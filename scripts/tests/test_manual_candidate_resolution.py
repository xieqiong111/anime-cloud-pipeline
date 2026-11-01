"""Offline regressions for durable manual duplicate-candidate resolution."""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from pipeline.candidates import CandidateSelector
from pipeline.executor import PipelineRunner
from pipeline.models import EpisodeKey, EpisodeStatus, ReviewReason, Subscription
from pipeline.services import ReviewService
from pipeline.state import StateStore


MEDIA_ID = "111"
QUARTER = "2026-10"
BACKFILL = {
    "season_quarter": QUARTER,
    "backfill": {"media_id": MEDIA_ID, "season": 1, "start": 1, "end": 5},
}


class ManualCandidateResolutionTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="manual-candidate-resolution-")
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))
        self.store.get_subscription_season_air_date = lambda _episode_id: "2026-10-01"
        self.reviews = ReviewService(self.store)
        self.subscription = self.make_subscription()

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    @staticmethod
    def make_subscription(*, exclude: str = "") -> Subscription:
        return Subscription(
            "fixture-subscription",
            "tmdb",
            MEDIA_ID,
            "Example Show",
            aliases=("Example",),
            season=1,
            exclude=exclude,
        )

    def episode(self, number: int, *, status: str = EpisodeStatus.DISCOVERED.value) -> int:
        return self.store.get_or_create_episode({
            "key": EpisodeKey("tmdb", MEDIA_ID, 1, number),
            "status": status,
            "canonical_work": "Example Show",
            "subscription_id": self.subscription.subscription_id,
        })

    def add_eight_equal_candidates(self, episode_id: int) -> list[int]:
        result = []
        for index in range(8):
            infohash = f"{index + 1:040x}"
            result.append(self.store.add_candidate(
                episode_id,
                {
                    "infohash": infohash,
                    "title": "Example Show S01E01 1080p",
                    "provider": "fixture",
                    "source_id": f"source-{index}",
                    "season": 1,
                    "episode": 1,
                },
            ))
        return result

    def open_eight_candidate_review(self, episode_id: int, candidate_ids: list[int]) -> None:
        result = CandidateSelector(
            self.store,
            subscription=self.subscription,
        ).select(episode_id, self.subscription)
        self.assertEqual(result.review_reason, ReviewReason.DUPLICATE_EPISODE)
        self.assertEqual(self.store.selected_candidates(episode_id), [])
        review = self.store.get_review(episode_id)
        self.assertIsNotNone(review)
        detail = json.loads(review["review_detail"])
        self.assertCountEqual(detail["candidate_ids"], candidate_ids)

    def test_resolved_eight_way_tie_survives_bounded_runner_resume(self) -> None:
        target = self.episode(1)
        target_candidates = self.add_eight_equal_candidates(target)
        neighbor = self.episode(2)
        neighbor_candidate = self.store.add_candidate(
            neighbor,
            {
                "infohash": "f" * 40,
                "title": "Example Show S01E02 1080p",
                "provider": "fixture",
                "source_id": "neighbor-source",
                "season": 1,
                "episode": 2,
            },
        )
        self.store.select_candidate(neighbor_candidate)

        self.open_eight_candidate_review(target, target_candidates)
        selected = target_candidates[4]
        self.assertEqual(
            self.reviews.resolve(target, {"selected_candidate_id": selected}),
            EpisodeStatus.DISCOVERED.value,
        )
        resolved_row = self.store.get_episode(target)
        self.assertEqual(self.store.selected_candidates(target), [selected])
        self.assertEqual(
            json.loads(resolved_row["review_resolution"])["selected_candidate_id"],
            selected,
        )

        selector = CandidateSelector(self.store, subscription=self.subscription)
        runner = PipelineRunner(self.store, candidate_selector=selector)
        summary = runner.run(discover=False, window=BACKFILL)

        self.assertEqual(summary["errors"], 0)
        self.assertEqual(self.store.get_episode(target)["status"], EpisodeStatus.DOWNLOAD_PENDING.value)
        self.assertEqual(self.store.selected_candidates(target), [selected])
        self.assertIsNone(self.store.get_review(target))
        self.assertEqual(self.store.get_episode(neighbor)["status"], EpisodeStatus.DOWNLOAD_PENDING.value)
        self.assertEqual(self.store.selected_candidates(neighbor), [neighbor_candidate])
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0],
            0,
        )

    def test_preselected_tie_without_valid_resolved_duplicate_review_stays_in_review(self) -> None:
        episode_id = self.episode(1)
        candidate_ids = self.add_eight_equal_candidates(episode_id)
        self.store.select_candidate(candidate_ids[0])

        result = CandidateSelector(
            self.store,
            subscription=self.subscription,
        ).select(episode_id, self.subscription)

        self.assertEqual(result.review_reason, ReviewReason.DUPLICATE_EPISODE)
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(self.store.selected_candidates(episode_id), [])
        self.assertEqual(self.store.get_review(episode_id)["review_reason"], ReviewReason.DUPLICATE_EPISODE.value)

    def test_resolved_choice_must_match_the_current_selected_candidate(self) -> None:
        episode_id = self.episode(1)
        candidate_ids = self.add_eight_equal_candidates(episode_id)
        self.open_eight_candidate_review(episode_id, candidate_ids)
        self.reviews.resolve(episode_id, {"selected_candidate_id": candidate_ids[0]})
        # Simulate durable selection drift after the operator's resolution.
        self.store.select_candidate(candidate_ids[1])

        result = CandidateSelector(
            self.store,
            subscription=self.subscription,
        ).select(episode_id, self.subscription)

        self.assertEqual(result.review_reason, ReviewReason.DUPLICATE_EPISODE)
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(self.store.selected_candidates(episode_id), [])
        self.assertEqual(self.store.get_review(episode_id)["review_reason"], ReviewReason.DUPLICATE_EPISODE.value)

    def test_resolved_choice_excluded_by_current_subscription_stays_in_review(self) -> None:
        episode_id = self.episode(1)
        candidate_ids = self.add_eight_equal_candidates(episode_id)
        self.open_eight_candidate_review(episode_id, candidate_ids)
        self.reviews.resolve(episode_id, {"selected_candidate_id": candidate_ids[0]})
        excluded_subscription = self.make_subscription(exclude="1080p")

        result = CandidateSelector(
            self.store,
            subscription=excluded_subscription,
        ).select(episode_id, excluded_subscription)

        self.assertEqual(result.review_reason, ReviewReason.DUPLICATE_EPISODE)
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(self.store.selected_candidates(episode_id), [])
        self.assertEqual(self.store.get_review(episode_id)["review_reason"], ReviewReason.DUPLICATE_EPISODE.value)
        self.assertTrue(all(
            row["decision_reason"] != "selected"
            for row in self.store.get_candidates(episode_id)
        ))


if __name__ == "__main__":
    unittest.main(verbosity=2)
