"""Offline integration regressions for inventory and explicit-scope safety."""
from __future__ import annotations

import json
import math
import os
import tempfile
import unittest
from unittest.mock import Mock

from pipeline.adapters import RemoteInventoryIncomplete
from pipeline.candidates import CandidateSelector, SubmissionBudget
from pipeline.executor import PipelineRunner
from pipeline.models import EpisodeKey, EpisodeStatus, ReviewReason, Subscription
from pipeline.services import DownloadService, ReviewService
from pipeline.state import StateStore


MEDIA_ID = "111"
QUARTER = "2026-10"
BACKFILL_E01_E05 = {
    "season_quarter": QUARTER,
    "backfill": {
        "media_source": "tmdb",
        "media_id": MEDIA_ID,
        "season": 1,
        "start": 1,
        "end": 5,
    },
}


class FakeCloud:
    """Task lookup sees only tasks submitted through this in-memory adapter."""

    def __init__(self, *, submit_failure: Exception | None = None) -> None:
        self.tasks: dict[str, dict[str, str]] = {}
        self.find_calls: list[str] = []
        self.submit_calls: list[str] = []
        self.submit_failure = submit_failure

    def find_task(self, infohash: str):
        key = infohash.strip().lower()
        self.find_calls.append(key)
        return self.tasks.get(key)

    def submit(self, uri: str) -> str:
        self.submit_calls.append(uri)
        if self.submit_failure is not None:
            raise self.submit_failure
        marker = "magnet:?xt=urn:btih:"
        if not uri.startswith(marker):
            raise AssertionError("DownloadService submitted a noncanonical magnet")
        infohash = uri[len(marker):].lower()
        task_id = f"fake-task-{len(self.tasks) + 1}"
        self.tasks[infohash] = {"task_id": task_id, "infohash": infohash}
        return task_id


class FixedLock:
    def __init__(self, acquired: bool) -> None:
        self.acquired = acquired
        self.acquire_calls = 0
        self.release_calls = 0

    def acquire(self) -> bool:
        self.acquire_calls += 1
        return self.acquired

    def release(self) -> None:
        self.release_calls += 1


class InventoryScopeIntegrationTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="inventory-scope-")
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))
        self.store.get_subscription_season_air_date = lambda _episode_id: "2026-10-03"

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def episode(
        self,
        *,
        source: str = "tmdb",
        media_id: str = MEDIA_ID,
        season: int = 1,
        episode: int = 1,
        status: str = EpisodeStatus.DOWNLOAD_PENDING.value,
        infohash: str | None = None,
    ) -> int:
        ident = self.store.get_or_create_episode({
            "key": EpisodeKey(source, media_id, season, episode),
            "status": status,
            "canonical_work": "Fixture Show",
            "subscription_id": "fixture-subscription",
        })
        if infohash is None:
            infohash = f"{episode:040x}"
        candidate_id = self.store.add_candidate(
            ident,
            {
                "infohash": infohash,
                "title": f"Fixture Show S{season:02d}E{episode:02d} 1080p",
                "provider": "fixture",
                "source_id": infohash[:8],
                "season": season,
                "episode": episode,
            },
        )
        self.store.select_candidate(candidate_id)
        return ident

    def test_present_inventory_repeatedly_blocks_submit_without_claiming_ready(self) -> None:
        ident = self.episode()
        cloud = FakeCloud()
        inventory = Mock(return_value={"state": "present"})

        for _ in range(2):
            budget = SubmissionBudget(global_limit=1, per_subscription=1)
            service = DownloadService(
                self.store,
                cloud,
                budget=budget,
                pre_submit_inventory=inventory,
            )
            result = service.advance(ident)

            self.assertEqual(result.detail["reason"], "pre_submit_inventory_present")
            self.assertEqual(result.status, EpisodeStatus.DOWNLOAD_PENDING.value)
            self.assertEqual(budget.total, 0)

        row = self.store.get_episode(ident)
        self.assertEqual(row["status"], EpisodeStatus.DOWNLOAD_PENDING.value)
        self.assertEqual(row["submit_count"], 0)
        self.assertNotEqual(row["status"], EpisodeStatus.LIBRARY_READY.value)
        self.assertEqual(cloud.submit_calls, [])
        self.assertEqual(len(cloud.find_calls), 2)
        self.assertEqual(inventory.call_count, 2)

    def test_explicit_backfill_advances_only_tmdb_season_one_episodes_one_to_five(self) -> None:
        allowed = [
            self.episode(episode=number, infohash=f"{number:040x}")
            for number in range(1, 6)
        ]
        outside = [
            self.episode(episode=6, infohash=f"{6:040x}"),
            self.episode(media_id="222", episode=1, infohash=f"{11:040x}"),
            self.episode(season=2, episode=1, infohash=f"{12:040x}"),
            self.episode(source="legacy", episode=1, infohash=f"{13:040x}"),
        ]
        before = {
            ident: (self.store.get_episode(ident), self.store.history(ident))
            for ident in outside
        }
        cloud = FakeCloud()
        budget = SubmissionBudget(global_limit=10, per_subscription=10)
        service = DownloadService(self.store, cloud, budget=budget)
        runner = PipelineRunner(self.store, services={"download": service})

        runner.run(window=BACKFILL_E01_E05)

        self.assertEqual(len(cloud.submit_calls), 5)
        self.assertEqual(budget.total, 5)
        for ident in allowed:
            row = self.store.get_episode(ident)
            self.assertEqual(row["status"], EpisodeStatus.SUBMITTED.value)
            self.assertEqual(row["submit_count"], 1)
        for ident in outside:
            self.assertEqual(self.store.get_episode(ident), before[ident][0])
            self.assertEqual(self.store.history(ident), before[ident][1])

    def test_runner_lock_failure_skips_discovery_and_all_submission_calls(self) -> None:
        self.episode()
        cloud = FakeCloud()
        service = DownloadService(
            self.store,
            cloud,
            budget=SubmissionBudget(global_limit=2, per_subscription=2),
        )
        discovery = Mock()
        lock = FixedLock(acquired=False)
        runner = PipelineRunner(
            self.store,
            services={"download": service},
            discovery=discovery,
            lock=lock,
        )

        summary = runner.run(discover=True, window={"season_quarter": QUARTER})

        self.assertTrue(summary["skipped_lock"])
        discovery.prepare.assert_not_called()
        self.assertEqual(cloud.find_calls, [])
        self.assertEqual(cloud.submit_calls, [])
        self.assertEqual(lock.acquire_calls, 1)
        self.assertEqual(lock.release_calls, 0)

    def test_each_in_scope_episode_advances_one_stage_in_one_runner_turn(self) -> None:
        episode_ids = [
            self.episode(
                episode=number,
                status=EpisodeStatus.STRM_PENDING.value,
                infohash=f"{number:040x}",
            )
            for number in range(1, 6)
        ]
        strm = Mock()
        strm.advance.return_value = {"status": EpisodeStatus.STRM_READY.value}
        scrape = Mock()
        runner = PipelineRunner(
            self.store,
            services={"strm": strm, "scrape": scrape},
        )

        summary = runner.run(window=BACKFILL_E01_E05)

        self.assertEqual(summary["advanced"], 5)
        self.assertCountEqual(
            [call.args[0]["id"] for call in strm.advance.call_args_list],
            episode_ids,
        )
        self.assertEqual(strm.advance.call_count, 5)
        scrape.advance.assert_not_called()
        for ident in episode_ids:
            self.assertEqual(
                self.store.get_episode(ident)["status"],
                EpisodeStatus.STRM_READY.value,
            )

    def test_shared_hash_reconciles_second_episode_without_second_submit(self) -> None:
        shared_hash = "a" * 40
        first = self.episode(episode=1, infohash=shared_hash)
        second = self.episode(episode=2, infohash=shared_hash)
        cloud = FakeCloud()
        budget = SubmissionBudget(global_limit=5, per_subscription=5)
        service = DownloadService(self.store, cloud, budget=budget)
        runner = PipelineRunner(self.store, services={"download": service})
        window = {
            "season_quarter": QUARTER,
            "backfill": {"media_id": MEDIA_ID, "season": 1, "start": 1, "end": 2},
        }

        summary = runner.run(window=window)

        self.assertEqual(len(cloud.submit_calls), 1)
        self.assertEqual(budget.total, 1)
        self.assertEqual(cloud.find_calls, [shared_hash, shared_hash])
        self.assertEqual(summary["new_submissions"], 1)
        self.assertEqual(summary["reconciled"], 1)
        task_ids = {
            self.store.get_episode(ident)["remote_task_id"]
            for ident in (first, second)
        }
        self.assertEqual(len(task_ids), 1)
        self.assertTrue(all(
            self.store.get_episode(ident)["status"] == EpisodeStatus.SUBMITTED.value
            for ident in (first, second)
        ))

    def test_durable_submit_intent_without_receipt_never_submits_again(self) -> None:
        ident = self.episode()
        self.store.transition(
            ident,
            EpisodeStatus.DOWNLOAD_PENDING.value,
            {"reason": "download_submit_intent"},
        )
        self.store.transition(
            ident,
            EpisodeStatus.DOWNLOAD_PENDING.value,
            {"reason": "download_submit_failed"},
        )
        cloud = FakeCloud()
        service = DownloadService(
            self.store,
            cloud,
            budget=SubmissionBudget(global_limit=2, per_subscription=2),
        )

        result = service.advance(ident)

        self.assertEqual(result.status, EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(result.detail["reason"], "download_submission_uncertain")
        self.assertEqual(cloud.submit_calls, [])
        self.assertEqual(self.store.get_episode(ident)["submit_count"], 0)
        self.assertEqual(self.store.get_episode(ident)["review_reason"], ReviewReason.UNEXPECTED_REMOTE_STATE.value)

    def test_unknown_submit_receipt_is_reviewed_and_cannot_be_retried(self) -> None:
        ident = self.episode()
        cloud = FakeCloud(submit_failure=RemoteInventoryIncomplete("fixture receipt unknown"))
        budget = SubmissionBudget(global_limit=2, per_subscription=2)
        first_service = DownloadService(self.store, cloud, budget=budget)

        first = first_service.advance(ident)
        second_service = DownloadService(self.store, cloud, budget=budget)
        second = second_service.advance(ident)

        self.assertEqual(first.status, EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(second.status, EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(len(cloud.submit_calls), 1)
        self.assertEqual(self.store.get_episode(ident)["submit_count"], 1)
        self.assertEqual(self.store.get_episode(ident)["review_reason"], ReviewReason.UNEXPECTED_REMOTE_STATE.value)
        history_details = self.store.conn.execute(
            "SELECT detail_json FROM stage_history WHERE episode_id=?", (ident,),
        ).fetchall()
        self.assertTrue(any(
            json.loads(row[0]).get("reason") == "download_submit_intent"
            for row in history_details
        ))

    def test_eight_equal_candidates_open_review_with_exact_safe_detail(self) -> None:
        ident = self.store.get_or_create_episode({
            "key": EpisodeKey("tmdb", MEDIA_ID, 1, 1),
            "status": EpisodeStatus.DISCOVERED.value,
            "canonical_work": "Fixture Show",
            "subscription_id": "fixture-subscription",
        })
        candidate_ids = []
        for number in range(8):
            infohash = f"{number + 1:040x}"
            candidate_ids.append(self.store.add_candidate(
                ident,
                {
                    "infohash": infohash,
                    "title": "Fixture Show S01E01 1080p",
                    "provider": "fixture",
                    "source_id": f"source-{number}",
                    "season": 1,
                    "episode": 1,
                },
            ))
        subscription = Subscription(
            "fixture-subscription",
            "tmdb",
            MEDIA_ID,
            "Fixture Show",
            aliases=("Fixture",),
            season=1,
        )

        result = CandidateSelector(self.store).select(ident, subscription)

        self.assertEqual(result.review_reason, ReviewReason.DUPLICATE_EPISODE)
        self.assertIsNone(result.candidate_id)
        self.assertEqual(self.store.selected_candidates(ident), [])
        self.assertEqual(self.store.get_episode(ident)["status"], EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0],
            0,
        )

        review = next(
            item for item in ReviewService(self.store).list_open()
            if item["review_id"] == ident
        )
        detail = review["review_detail"]
        self.assertEqual(review["review_reason"], ReviewReason.DUPLICATE_EPISODE.value)
        self.assertCountEqual(detail["candidate_ids"], candidate_ids)
        self.assertEqual(len(detail["candidate_ids"]), 8)
        self.assertEqual(detail["reason"], ReviewReason.DUPLICATE_EPISODE.value)
        self.assertIsInstance(detail["score"], (int, float))
        self.assertTrue(math.isfinite(detail["score"]))

        persisted = self.store.get_candidates(ident)
        scores = [candidate["score"] for candidate in persisted]
        self.assertTrue(all(score is not None for score in scores))
        self.assertTrue(all(math.isclose(score, scores[0], abs_tol=1e-9) for score in scores))
        self.assertTrue(math.isclose(detail["score"], scores[0], abs_tol=1e-9))


if __name__ == "__main__":
    unittest.main(verbosity=2)
