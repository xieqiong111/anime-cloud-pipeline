from __future__ import annotations

import json
import os
import tempfile
import unittest

from pipeline.candidates import SubmissionBudget
from pipeline.executor import PipelineRunner
from pipeline.models import EpisodeKey, EpisodeStatus
from pipeline.services import DownloadService
from pipeline.state import StateStore
from pipeline.video import VideoSelector

QUARTER_WINDOW = {"season_quarter": "2026-07"}


class StageFake:
    def __init__(self, name: str, *, fail: bool = False) -> None:
        self.name = name
        self.fail = fail
        self.calls: list[int] = []

    def advance(self, episode):
        episode_id = int(episode["id"])
        self.calls.append(episode_id)
        if self.fail:
            raise RuntimeError(f"{self.name}-secret failure")
        return {"status": self.name}


class OrganizerFake(StageFake):
    def reconcile_or_apply(self, episode):
        return self.advance(episode)


class CloudFake:
    def __init__(self) -> None:
        self.find_calls: list[str] = []
        self.submit_calls: list[str] = []
        self.list_calls: list[str] = []
        self.files: list[dict[str, object]] = []

    def find_task(self, infohash: str):
        self.find_calls.append(infohash)
        return None

    def submit(self, uri: str):
        self.submit_calls.append(uri)
        return "remote-1"

    def list_files(self, target_directory: str):
        self.list_calls.append(target_directory)
        return list(self.files)


class PipelineRunnerTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="task10-runner-")
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))
        # These stage-focused tests use direct episode fixtures, without a
        # MoviePilot subscription row. Keep their routing evidence explicit.
        self.store.get_subscription_season_air_date = lambda _episode_id: "2026-07-03"

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def episode(self, status: EpisodeStatus | str, *, subscription_id: str = "sub-1", infohash: str = "a" * 40) -> int:
        episode_id = self.store.get_or_create_episode(
            {
                "key": EpisodeKey("tmdb", str(len(self.store.all_episodes()) + 111), 1, 8),
                "subscription_id": subscription_id,
                "canonical_work": "Show",
                "title": "Show - S01E08",
                "status": status,
            }
        )
        self.store.add_candidate(episode_id, {"infohash": infohash, "title": "Show S01E08"})
        return episode_id

    def runner(self, *, cloud=None, fail_stage: str | None = None, budget=None):
        services = {
            "organize": OrganizerFake("organized", fail=fail_stage == "organize"),
            "strm": StageFake("strm_ready", fail=fail_stage == "strm"),
            "scrape": StageFake("metadata_ready", fail=fail_stage == "scrape"),
            "library": StageFake("library_ready", fail=fail_stage == "library"),
        }
        return PipelineRunner(
            self.store,
            cloud115=cloud,
            services=services,
            budget=budget or SubmissionBudget(10, 10),
            secrets=("handler-secret",),
        ), services

    def test_every_pending_status_has_one_handler_and_one_stage_per_run(self) -> None:
        episode_id = self.episode(EpisodeStatus.ORGANIZED)
        runner, services = self.runner()
        for status in (
            EpisodeStatus.DOWNLOAD_PENDING.value,
            EpisodeStatus.ORGANIZE_PENDING.value,
            EpisodeStatus.STRM_PENDING.value,
            EpisodeStatus.SCRAPE_PENDING.value,
            EpisodeStatus.LIBRARY_PENDING.value,
        ):
            self.assertIn(status, runner.handlers)

        self.store.transition(episode_id, EpisodeStatus.ORGANIZED)
        summary = runner.run(window=QUARTER_WINDOW, discover=False)
        self.assertEqual(summary["advanced"], 1)
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(services["strm"].calls, [])

    def test_handler_exception_is_sanitized_and_other_episode_continues(self) -> None:
        first = self.episode(EpisodeStatus.STRM_PENDING)
        second = self.episode(EpisodeStatus.SCRAPE_PENDING)
        runner, services = self.runner(fail_stage="strm")
        summary = runner.run(window=QUARTER_WINDOW, discover=False)
        self.assertEqual(summary["errors"], 1)
        self.assertEqual(services["scrape"].calls, [second])
        row = self.store.get_episode(first)
        self.assertEqual(row["status"], EpisodeStatus.STRM_PENDING.value)
        self.assertNotIn("handler-secret", row["last_error"])

    def test_read_only_has_no_store_writes_or_mutating_service_calls(self) -> None:
        episode_id = self.episode(EpisodeStatus.STRM_PENDING)
        runner, services = self.runner()
        before = self.store.get_episode(episode_id)
        history_before = self.store.history(episode_id)
        summary = runner.run(window=QUARTER_WINDOW, discover=False, read_only=True)
        self.assertEqual(summary["skipped_read_only"], 1)
        self.assertEqual(self.store.get_episode(episode_id), before)
        self.assertEqual(self.store.history(episode_id), history_before)
        self.assertEqual(services["strm"].calls, [])

    def test_budget_counts_only_new_download_submission(self) -> None:
        cloud = CloudFake()
        budget = SubmissionBudget(1, 1)
        first = self.episode(EpisodeStatus.DOWNLOAD_PENDING, subscription_id="sub-1")
        second = self.episode(EpisodeStatus.DOWNLOAD_PENDING, subscription_id="sub-1")
        runner, _ = self.runner(cloud=cloud, budget=budget)
        summary = runner.run(window=QUARTER_WINDOW, discover=False)
        self.assertEqual(len(cloud.submit_calls), 1)
        self.assertEqual(budget.total, 1)
        self.assertEqual(summary["submitted"], 1)
        self.assertEqual(summary["new_submissions"], 1)
        self.assertEqual(self.store.get_episode(first)["status"], EpisodeStatus.SUBMITTED.value)
        self.assertEqual(self.store.get_episode(second)["status"], EpisodeStatus.DOWNLOAD_PENDING.value)

    def test_download_pending_uses_dedicated_reconciliation_service(self) -> None:
        cloud = CloudFake()
        budget = SubmissionBudget(1, 1)
        episode_id = self.episode(EpisodeStatus.DOWNLOAD_PENDING)
        service = DownloadService(self.store, cloud, budget=budget)

        result = service.advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.SUBMITTED.value)
        self.assertEqual(len(cloud.submit_calls), 1)
        self.assertEqual(budget.total, 1)

    def test_reconciled_download_does_not_consume_submission_budget(self) -> None:
        class ExistingCloud(CloudFake):
            def find_task(self, infohash: str):
                self.find_calls.append(infohash)
                return {"task_id": "already-there", "infohash": infohash}

        cloud = ExistingCloud()
        budget = SubmissionBudget(1, 1)
        episode_id = self.episode(EpisodeStatus.DOWNLOAD_PENDING)
        runner, _ = self.runner(cloud=cloud, budget=budget)

        runner.run(window=QUARTER_WINDOW, discover=False)

        self.assertEqual(cloud.submit_calls, [])
        self.assertEqual(budget.total, 0)
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.SUBMITTED.value)

    def test_poll_persists_complete_inventory_and_reviews_multiple_videos(self) -> None:
        cloud = CloudFake()
        infohash = "b" * 40
        episode_id = self.episode(EpisodeStatus.SUBMITTED, infohash=infohash)
        self.store.select_candidate(self.store.get_candidates(episode_id)[0]["id"])
        cloud.files = [
            {
                "infohash": infohash,
                "remote_file_id": "file-a",
                "path": "/target/Show.S01E08.mkv",
                "name": "Show.S01E08.mkv",
                "size": 100,
                "duration": 60,
            },
            {
                "infohash": infohash,
                "remote_file_id": "file-b",
                "path": "/target/Show.S01E08-copy.mkv",
                "name": "Show.S01E08-copy.mkv",
                "size": 100,
                "duration": 60,
            },
        ]
        service = DownloadService(
            self.store,
            cloud,
            budget=SubmissionBudget(1, 1),
            video_selector=VideoSelector(),
        )

        result = service.poll(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(result.review_reason, "MULTIPLE_VIDEO_FILES")
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM download_files").fetchone()[0],
            2,
        )
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM episode_sources WHERE episode_id=?",
                (episode_id,),
            ).fetchone()[0],
            2,
        )
        self.assertEqual(
            self.store.selected_candidates(episode_id),
            [self.store.get_candidates(episode_id)[0]["id"]],
        )

    def test_poll_does_not_match_inventory_when_infohash_is_empty(self) -> None:
        cloud = CloudFake()
        episode_id = self.store.get_or_create_episode(
            {
                "key": EpisodeKey("tmdb", "999", 1, 1),
                "canonical_work": "Show",
                "title": "Show - S01E01",
                "status": EpisodeStatus.SUBMITTED,
            }
        )
        service = DownloadService(self.store, cloud, budget=SubmissionBudget(1, 1))

        result = service.poll(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.SUBMITTED.value)
        self.assertEqual(cloud.list_calls, [])
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM download_files").fetchone()[0],
            0,
        )
        self.assertGreater(self.store.get_episode(episode_id)["retry_count"], 0)

    def test_discovered_without_selected_candidate_does_not_enter_download(self) -> None:
        episode_id = self.episode(EpisodeStatus.DISCOVERED)
        runner, _ = self.runner()

        summary = runner.run(window=QUARTER_WINDOW, discover=False)

        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.DISCOVERED.value)
        self.assertEqual(summary["advanced"], 0)

    def test_discovered_with_selected_candidate_enters_download_without_selector(self) -> None:
        episode_id = self.episode(EpisodeStatus.DISCOVERED)
        candidate_id = self.store.get_candidates(episode_id)[0]["id"]
        self.store.select_candidate(candidate_id)
        runner, _ = self.runner()

        summary = runner.run(window=QUARTER_WINDOW, discover=False)

        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.DOWNLOAD_PENDING.value)
        self.assertEqual(summary["advanced"], 1)
        self.assertEqual(summary["skipped_selection"], 0)

    def test_discovered_calls_candidate_selector_and_requires_valid_result(self) -> None:
        episode_id = self.episode(EpisodeStatus.DISCOVERED)
        candidate_id = self.store.get_candidates(episode_id)[0]["id"]

        class Selector:
            def __init__(self):
                self.calls: list[int] = []

            def select_episode(self, selected_episode_id):
                self.calls.append(selected_episode_id)
                return {"candidate_id": candidate_id}

        selector = Selector()
        runner, _ = self.runner()
        runner.candidate_selector = selector

        summary = runner.run(window=QUARTER_WINDOW, discover=False)

        self.assertEqual(selector.calls, [episode_id])
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.DOWNLOAD_PENDING.value)
        self.assertEqual(self.store.selected_candidates(episode_id), [candidate_id])
        self.assertEqual(summary["advanced"], 1)

    def test_discovered_selector_exception_does_not_enter_download(self) -> None:
        class BrokenSelector:
            def select(self, _episode_id):
                raise RuntimeError("selector-secret")

        episode_id = self.episode(EpisodeStatus.DISCOVERED)
        runner, _ = self.runner()
        runner.candidate_selector = BrokenSelector()

        summary = runner.run(window=QUARTER_WINDOW, discover=False)

        row = self.store.get_episode(episode_id)
        self.assertEqual(row["status"], EpisodeStatus.DISCOVERED.value)
        self.assertEqual(summary["advanced"], 0)
        self.assertEqual(summary["errors"], 1)
        self.assertNotIn("selector-secret", row["last_error"])

    def test_budget_exhaustion_is_skipped_not_advanced(self) -> None:
        cloud = CloudFake()
        episode_id = self.episode(EpisodeStatus.DOWNLOAD_PENDING)
        budget = SubmissionBudget(0, 0)
        runner, _ = self.runner(cloud=cloud, budget=budget)

        summary = runner.run(window=QUARTER_WINDOW, discover=False)

        self.assertEqual(summary["advanced"], 0)
        self.assertEqual(summary["skipped_budget"], 1)
        self.assertEqual(summary["submitted"], 0)
        self.assertEqual(cloud.submit_calls, [])
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.DOWNLOAD_PENDING.value)

    def test_shared_infohash_poll_only_advances_legal_submitted_targets(self) -> None:
        cloud = CloudFake()
        infohash = "c" * 40
        submitted = self.episode(EpisodeStatus.SUBMITTED, infohash=infohash)
        discovered = self.episode(EpisodeStatus.DISCOVERED, infohash=infohash)
        reviewed = self.episode(EpisodeStatus.DISCOVERED, infohash=infohash)
        for episode_id in (submitted, discovered, reviewed):
            self.store.select_candidate(self.store.get_candidates(episode_id)[0]["id"])
        self.store.open_review(reviewed, "DUPLICATE_EPISODE", {"reason": "operator"})
        cloud.files = [{
            "infohash": infohash,
            "remote_file_id": "file-one",
            "path": "/target/Show.S01E08.mkv",
            "name": "Show.S01E08.mkv",
            "size": 100,
            "duration": 60,
        }]
        service = DownloadService(self.store, cloud, budget=SubmissionBudget(5, 5))

        result = service.poll(self.store.get_episode(submitted))

        self.assertEqual(result.status, EpisodeStatus.CLOUD_READY.value)
        self.assertEqual(self.store.get_episode(submitted)["status"], EpisodeStatus.CLOUD_READY.value)
        self.assertEqual(self.store.get_episode(discovered)["status"], EpisodeStatus.DISCOVERED.value)
        self.assertEqual(self.store.get_episode(reviewed)["status"], EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(self.store.history(discovered), [EpisodeStatus.DISCOVERED.value])
        self.assertEqual(self.store.history(reviewed)[-1], EpisodeStatus.NEEDS_REVIEW.value)

    def test_shared_infohash_poll_rereads_target_before_cas_transition(self) -> None:
        cloud = CloudFake()
        infohash = "d" * 40
        first = self.episode(EpisodeStatus.SUBMITTED, infohash=infohash)
        second = self.episode(EpisodeStatus.SUBMITTED, infohash=infohash)
        for episode_id in (first, second):
            self.store.select_candidate(self.store.get_candidates(episode_id)[0]["id"])
        cloud.files = [{
            "infohash": infohash,
            "remote_file_id": "file-one",
            "path": "/target/Show.S01E08.mkv",
            "name": "Show.S01E08.mkv",
            "size": 100,
            "duration": 60,
        }]

        class RacesToReview(VideoSelector):
            def select(self, inventory, episodes):
                self_store.open_review(second, "DUPLICATE_EPISODE", {"reason": "race"})
                return super().select(inventory, episodes)

        self_store = self.store
        service = DownloadService(
            self.store,
            cloud,
            budget=SubmissionBudget(5, 5),
            video_selector=RacesToReview(),
        )

        service.poll(self.store.get_episode(first))

        self.assertEqual(self.store.get_episode(first)["status"], EpisodeStatus.CLOUD_READY.value)
        self.assertEqual(self.store.get_episode(second)["status"], EpisodeStatus.NEEDS_REVIEW.value)

    def test_runner_rereads_shared_poll_rows_and_calls_external_stage_once(self) -> None:
        cloud = CloudFake()
        infohash = "e" * 40
        first = self.episode(EpisodeStatus.SUBMITTED, infohash=infohash)
        second = self.episode(EpisodeStatus.SUBMITTED, infohash=infohash)
        for episode_id in (first, second):
            self.store.select_candidate(self.store.get_candidates(episode_id)[0]["id"])
        cloud.files = [{
            "infohash": infohash,
            "remote_file_id": "file-one",
            "path": "/target/Show.S01E08.mkv",
            "name": "Show.S01E08.mkv",
            "size": 100,
            "duration": 60,
        }]
        runner, _ = self.runner(cloud=cloud)

        summary = runner.run(window=QUARTER_WINDOW, discover=False)

        self.assertEqual(cloud.list_calls, ["target"])
        self.assertEqual(self.store.history(first).count(EpisodeStatus.CLOUD_READY.value), 1)
        self.assertEqual(self.store.history(second).count(EpisodeStatus.CLOUD_READY.value), 1)
        self.assertEqual(summary["errors"], 0)


if __name__ == "__main__":
    unittest.main(verbosity=2)
