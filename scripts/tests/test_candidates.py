from __future__ import annotations

import json
import os
import tempfile
import time
import unittest
from concurrent.futures import ThreadPoolExecutor

from pipeline.candidates import CandidateSelector, SubmissionBudget
from pipeline.models import EpisodeKey, ReviewReason, Subscription
from pipeline.parsers import is_non_episode_media
from pipeline.state import StateStore


def candidate(infohash: str, title: str, **fields: object) -> dict[str, object]:
    value: dict[str, object] = {
        "infohash": infohash,
        "title": title,
        "provider": "fixture",
        "source_id": infohash[:8],
        "episode": 8,
        "season": 1,
    }
    value.update(fields)
    return value


class CandidateSelectorTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))
        self.subscription = Subscription(
            "sub-1",
            "tmdb",
            "111",
            "Example Show",
            aliases=("Example",),
            season=1,
            include="简",
            exclude="NCOP",
        )
        self.episode = self.store.get_or_create_episode(
            {
                "key": EpisodeKey("tmdb", "111", 1, 8),
                "subscription_id": self.subscription.subscription_id,
                "canonical_work": self.subscription.work,
                "aliases": self.subscription.aliases,
            }
        )
        self.selector = CandidateSelector(
            self.store,
            scoring={
                "include_weight": 10,
                "language_weight": 5,
                "release_group_weight": 3,
                "resolution_weight": 2,
                "collection_penalty": 4,
            },
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_two_hashes_for_same_episode_select_exactly_one_higher_scoring_candidate(self):
        first = self.store.add_candidate(
            self.episode,
            candidate("a" * 40, "Example Show S01E08 1080p 简体", resolution="1080p"),
        )
        second = self.store.add_candidate(
            self.episode,
            candidate(
                "b" * 40,
                "Example Show S01E08 2160p 简繁内封",
                resolution="2160p",
                language="简繁",
            ),
        )

        result = self.selector.select(self.episode, self.subscription)

        self.assertEqual(result.candidate_id, second)
        self.assertEqual(self.store.selected_candidates(self.episode), [second])
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM episodes_v5 WHERE media_source='tmdb' AND media_id='111' AND season=1 AND episode=8"
            ).fetchone()[0],
            1,
        )
        rows = self.store.conn.execute(
            "SELECT id, score, decision_reason, traits_json FROM candidates WHERE episode_id=? ORDER BY id",
            (self.episode,),
        ).fetchall()
        self.assertEqual([row[0] for row in rows], [first, second])
        self.assertGreater(rows[1][1], rows[0][1])
        self.assertIn("score_components", json.loads(rows[1][3]))
        self.assertTrue(rows[1][2])

    def test_equal_top_candidates_require_review_and_clear_selection(self):
        self.store.add_candidate(
            self.episode,
            candidate("a" * 40, "Example Show S01E08 1080p", resolution="1080p"),
        )
        self.store.add_candidate(
            self.episode,
            candidate("b" * 40, "Example Show S01E08 1080p", resolution="1080p"),
        )

        result = self.selector.select(self.episode, self.subscription)

        self.assertEqual(result.review_reason, ReviewReason.DUPLICATE_EPISODE)
        self.assertIsNone(result.candidate_id)
        self.assertEqual(self.store.selected_candidates(self.episode), [])
        self.assertEqual(self.store.get_episode(self.episode)["review_reason"], ReviewReason.DUPLICATE_EPISODE.value)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0],
            0,
        )

    def test_excluded_and_non_episode_candidates_are_not_selectable(self):
        excluded = self.store.add_candidate(
            self.episode,
            candidate("a" * 40, "Example Show S01E08 NCOP 1080p", resolution="1080p"),
        )
        non_episode = self.store.add_candidate(
            self.episode,
            candidate("b" * 40, "Example Show S01E08 1080p", resolution="1080p"),
        )

        excluded_score = self.selector.score(
            self.store.get_candidate(excluded), self.subscription
        )
        self.assertTrue(excluded_score.excluded)
        self.assertEqual(excluded_score.total, 0)
        self.assertIn("non_episode", excluded_score.decision_reason)

        result = self.selector.select(self.episode, Subscription(
            "sub-1", "tmdb", "111", "Example Show", season=1, exclude="1080p"
        ))

        self.assertIsNone(result.candidate_id)
        self.assertIsNone(result.review_reason)
        self.assertEqual(self.store.selected_candidates(self.episode), [])
        reasons = [
            row[0]
            for row in self.store.conn.execute(
                "SELECT decision_reason FROM candidates WHERE episode_id=? ORDER BY id",
                (self.episode,),
            ).fetchall()
        ]
        self.assertTrue(all(reasons))
        self.assertEqual(len(reasons), 2)
        self.assertNotEqual(excluded, non_episode)

    def test_reselection_clears_old_selected_candidate_atomically(self):
        first = self.store.add_candidate(
            self.episode,
            candidate("a" * 40, "Example Show S01E08 720p", resolution="720p"),
        )
        second = self.store.add_candidate(
            self.episode,
            candidate("b" * 40, "Example Show S01E08 简体 2160p", resolution="2160p"),
        )
        self.store.select_candidate(first)

        result = self.selector.select(self.episode, self.subscription)

        self.assertEqual(result.candidate_id, second)
        self.assertEqual(self.store.selected_candidates(self.episode), [second])
        self.assertEqual(
            self.store.conn.execute(
                "SELECT selected FROM candidates WHERE id=?", (first,)
            ).fetchone()[0],
            0,
        )

    def test_numbered_non_episode_markers_are_rejected_without_operation_false_positive(self):
        for title in (
            "Example Show S01E08 NCOP1",
            "Example Show S01E08 NCED2",
            "Example Show S01E08 PV1",
            "Example Show S01E08 SP01",
        ):
            with self.subTest(title=title):
                self.assertTrue(is_non_episode_media(title))
        self.assertFalse(is_non_episode_media("Operation Example S01E08"))

    def test_malformed_decision_values_do_not_raise_and_false_collection_is_not_penalty(self):
        result = self.selector.score(
            {
                "infohash": "c" * 40,
                "title": "Example Show S01E08 1080p",
                "decision": ["eligible"],
                "collection": "false",
            },
            self.subscription,
        )

        self.assertFalse(result.excluded)
        self.assertEqual(result.components["collection_penalty"], 0)


class SubmissionBudgetConcurrencyTests(unittest.TestCase):
    def test_concurrent_reservations_never_exceed_global_or_subscription_limits(self):
        class YieldingCounts(dict[str, int]):
            def get(self, key, default=None):
                time.sleep(0.001)
                return super().get(key, default)

            def values(self):
                time.sleep(0.001)
                return super().values()

        budget = SubmissionBudget(global_limit=8, per_subscription=2)
        budget.counts = YieldingCounts()

        with ThreadPoolExecutor(max_workers=32) as pool:
            accepted = list(pool.map(lambda index: budget.reserve(str(index % 10)), range(160)))

        self.assertLessEqual(sum(accepted), 8)
        self.assertLessEqual(max(budget.counts.values(), default=0), 2)


if __name__ == "__main__":
    unittest.main(verbosity=2)
