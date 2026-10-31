from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest
from unittest.mock import patch

from pipeline.models import EpisodeKey, EpisodeStatus, ReviewReason
from pipeline.services import ReviewService
from pipeline.state import StateStore


class ReviewServiceTests(unittest.TestCase):
    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory(prefix="task10-reviews-")
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))
        self.episode_id = self.store.get_or_create_episode(
            {
                "key": EpisodeKey("tmdb", "111", 1, 8),
                "canonical_work": "Show",
                "title": "Show - S01E08",
                "status": EpisodeStatus.ORGANIZE_PENDING,
                "retry_count": 2,
                "next_retry_at": 9999999999.0,
                "last_error": "old-retry",
            }
        )
        self.reviews = ReviewService(self.store, secrets=("review-secret",))

    def map_review_files(self) -> tuple[int, int]:
        download_id = self.store.upsert_download("review-download")
        file_ids = self.store.record_download_files(
            download_id,
            [
                {"remote_file_id": "a", "name": "a.mkv"},
                {"remote_file_id": "b", "name": "b.mkv"},
            ],
        )
        self.store.map_episode_source(self.episode_id, file_ids[0], selected=False)
        self.store.map_episode_source(self.episode_id, file_ids[1], selected=False)
        return file_ids[0], file_ids[1]

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_review_persists_until_explicit_resolution_and_resets_retry(self) -> None:
        self.map_review_files()
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.MULTIPLE_VIDEO_FILES,
            {"file_ids": ["a", "b"], "note": "review-secret"},
            "organize_pending",
        )

        row = self.store.get_episode(self.episode_id)
        self.assertEqual(review_id, self.episode_id)
        self.assertEqual(row["status"], EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(row["resume_status"], EpisodeStatus.ORGANIZE_PENDING.value)
        self.assertNotIn("review-secret", row["review_detail"])
        self.assertEqual(self.reviews.list_open()[0]["review_id"], self.episode_id)

        self.reviews.resolve(review_id, {"selected_file_id": "b"})
        row = self.store.get_episode(self.episode_id)
        self.assertEqual(row["status"], EpisodeStatus.ORGANIZE_PENDING.value)
        self.assertEqual(row["retry_count"], 0)
        self.assertIsNone(row["next_retry_at"])
        self.assertEqual(row["last_error"], "")
        self.assertEqual(json.loads(row["review_resolution"])["selected_file_id"], "b")
        self.assertEqual(self.reviews.list_open(), [])

    def test_invalid_resolution_is_atomic_noop(self) -> None:
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.MULTIPLE_VIDEO_FILES,
            {"file_ids": ["a", "b"]},
        )
        before = self.store.get_episode(self.episode_id)
        history_before = self.store.history(self.episode_id)

        with self.assertRaises(ValueError):
            self.reviews.resolve(review_id, {"unexpected": "value"})

        after = self.store.get_episode(self.episode_id)
        self.assertEqual(after, before)
        self.assertEqual(self.store.history(self.episode_id), history_before)

    def test_concurrent_double_resolution_only_one_wins(self) -> None:
        self.map_review_files()
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.MULTIPLE_VIDEO_FILES,
            {"file_ids": ["a", "b"]},
        )
        other_store = StateStore(os.path.join(self.tmp.name, "state.db"))
        other_reviews = ReviewService(other_store)
        results: list[str] = []
        failures: list[type[BaseException]] = []

        def resolve(service: ReviewService) -> None:
            try:
                results.append(service.resolve(review_id, {"selected_file_id": "b"}))
            except BaseException as exc:  # pragma: no cover - thread assertion below
                failures.append(type(exc))

        first = threading.Thread(target=resolve, args=(self.reviews,))
        second = threading.Thread(target=resolve, args=(other_reviews,))
        first.start()
        second.start()
        first.join()
        second.join()

        self.assertEqual(len(results), 1)
        self.assertEqual(len(failures), 1)
        self.assertEqual(len(self.store.history(self.episode_id)), 3)
        other_store.close()

    def test_duplicate_resolution_selects_only_candidate_in_same_transaction(self) -> None:
        first = self.store.add_candidate(self.episode_id, {"infohash": "a" * 40})
        second = self.store.add_candidate(self.episode_id, {"infohash": "b" * 40})
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.DUPLICATE_EPISODE,
            {"candidate_ids": [first, second]},
            "download_pending",
        )

        self.reviews.resolve(review_id, {"selected_candidate_id": second})

        self.assertEqual(self.store.selected_candidates(self.episode_id), [second])
        self.assertEqual(self.store.get_episode(self.episode_id)["status"], EpisodeStatus.DOWNLOAD_PENDING.value)

    def test_duplicate_resolution_rejects_candidate_from_another_episode_atomically(self) -> None:
        other_episode = self.store.get_or_create_episode(
            {
                "key": EpisodeKey("tmdb", "222", 1, 1),
                "canonical_work": "Other",
                "title": "Other - S01E01",
                "status": EpisodeStatus.DOWNLOAD_PENDING,
            }
        )
        foreign = self.store.add_candidate(other_episode, {"infohash": "c" * 40})
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.DUPLICATE_EPISODE,
            {"candidate_ids": [foreign]},
            "download_pending",
        )
        before = self.store.get_episode(self.episode_id)
        history_before = self.store.history(self.episode_id)

        with self.assertRaises(ValueError):
            self.reviews.resolve(review_id, {"selected_candidate_id": foreign})

        self.assertEqual(self.store.get_episode(self.episode_id), before)
        self.assertEqual(self.store.history(self.episode_id), history_before)
        self.assertEqual(self.store.selected_candidates(other_episode), [])

    def test_multiple_video_resolution_selects_mapped_file_as_primary(self) -> None:
        download_id = self.store.upsert_download("d" * 40)
        file_ids = self.store.record_download_files(
            download_id,
            [
                {"remote_file_id": "file-a", "name": "a.mkv"},
                {"remote_file_id": "file-b", "name": "b.mkv"},
            ],
        )
        self.store.map_episode_source(self.episode_id, file_ids[0], selected=False)
        self.store.map_episode_source(self.episode_id, file_ids[1], selected=False)
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.MULTIPLE_VIDEO_FILES,
            {"file_ids": ["file-a", "file-b"]},
            "organize_pending",
        )

        self.reviews.resolve(review_id, {"selected_file_id": "file-b"})

        selected = self.store.conn.execute(
            """SELECT df.remote_file_id FROM episode_sources es
               JOIN download_files df ON df.id=es.download_file_id
               WHERE es.episode_id=? AND es.selected_primary=1""",
            (self.episode_id,),
        ).fetchall()
        self.assertEqual([row[0] for row in selected], ["file-b"])
        self.assertEqual(self.store.get_episode(self.episode_id)["file_id"], "file-b")

    def test_multiple_video_resolution_db_row_alias_persists_canonical_remote_id(self) -> None:
        _first_file_id, second_file_id = self.map_review_files()
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.MULTIPLE_VIDEO_FILES,
            {"file_ids": ["a", "b"]},
            "organize_pending",
        )

        self.reviews.resolve(review_id, {"selected_file_id": second_file_id})

        row = self.store.get_episode(self.episode_id)
        selected = self.store.conn.execute(
            """SELECT df.remote_file_id FROM episode_sources es
               JOIN download_files df ON df.id=es.download_file_id
               WHERE es.episode_id=? AND es.selected_primary=1""",
            (self.episode_id,),
        ).fetchone()
        self.assertEqual(row["file_id"], "b")
        self.assertEqual(selected[0], "b")

    def test_legacy_identity_resolution_updates_identity_atomically(self) -> None:
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.LEGACY_IDENTITY_UNRESOLVED,
            {"reason": "legacy"},
            "download_pending",
        )

        self.reviews.resolve(
            review_id,
            {"media_source": "tmdb", "media_id": "222", "season": 2, "episode": 3},
        )

        row = self.store.get_episode(self.episode_id)
        self.assertEqual((row["media_source"], row["media_id"], row["season"], row["episode"]), ("tmdb", "222", 2, 3))

    def test_runtime_mapping_resolution_updates_task_identity_atomically(self) -> None:
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.RUNTIME_MAPPING_AMBIGUOUS,
            {"reason": "runtime"},
            "organize_pending",
        )

        self.reviews.resolve(review_id, {"task_id": "litepan-task-2"})

        self.assertEqual(
            json.loads(self.store.get_episode(self.episode_id)["runtime_identity_json"])["task_id"],
            "litepan-task-2",
        )

    def test_non_inventory_file_resolution_keeps_reference_without_claiming_source(self) -> None:
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.PATH_CONFLICT,
            {"reason": "path"},
            "organize_pending",
        )

        self.reviews.resolve(
            review_id,
            {"action": "keep_existing", "selected_file_id": "target-file"},
        )

        row = self.store.get_episode(self.episode_id)
        self.assertEqual(row["file_id"], "target-file")
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM episode_sources WHERE episode_id=? AND selected_primary=1",
                (self.episode_id,),
            ).fetchone()[0],
            0,
        )

    def test_legacy_choice_resolution_can_explicitly_decline_identity_update(self) -> None:
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.LEGACY_IDENTITY_UNRESOLVED,
            {"reason": "legacy"},
            "download_pending",
        )

        self.reviews.resolve(review_id, {"choice": "keep_legacy"})

        row = self.store.get_episode(self.episode_id)
        self.assertEqual(row["status"], EpisodeStatus.DOWNLOAD_PENDING.value)

    def test_stale_review_version_cannot_resolve_reopened_review(self) -> None:
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.MULTIPLE_VIDEO_FILES,
            {"file_ids": ["a", "b"]},
            "organize_pending",
        )
        stale = self.store.get_review(review_id)
        self.reviews.open(
            self.episode_id,
            ReviewReason.PATH_CONFLICT,
            {"reason": "reopened"},
            "organize_pending",
        )

        with self.assertRaises(ValueError):
            self.store.resolve_review(
                review_id,
                {"action": "keep_existing"},
                expected_review_created_at=stale["review_created_at"],
                expected_review_reason=stale["review_reason"],
                expected_review_version=stale["review_created_at"],
            )

        self.assertEqual(self.store.get_review(review_id)["review_reason"], ReviewReason.PATH_CONFLICT.value)

    def test_review_version_is_checked_even_when_created_at_matches(self) -> None:
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.PATH_CONFLICT,
            {"reason": "version"},
            "organize_pending",
        )
        current = self.store.get_review(review_id)

        with self.assertRaises(ValueError):
            self.store.resolve_review(
                review_id,
                {"action": "keep_existing"},
                expected_review_created_at=current["review_created_at"],
                expected_review_reason=current["review_reason"],
                expected_review_version=current["review_created_at"] + 1.0,
            )

        self.assertIsNotNone(self.store.get_review(review_id))

    def test_concurrent_stale_resolver_cannot_close_reopened_review(self) -> None:
        review_id = self.reviews.open(
            self.episode_id,
            ReviewReason.METADATA_CONFLICT,
            {"reason": "old"},
            "organize_pending",
        )
        stale = self.store.get_review(review_id)
        other_store = StateStore(os.path.join(self.tmp.name, "state.db"))
        ready = threading.Event()
        release = threading.Event()
        failures: list[str] = []

        def resolve_stale() -> None:
            ready.set()
            release.wait(timeout=5)
            try:
                other_store.resolve_review(
                    review_id,
                    {"action": "keep_existing", "choice": "old", "confirm": True},
                    expected_review_created_at=stale["review_created_at"],
                    expected_review_reason=stale["review_reason"],
                    expected_review_version=stale["review_created_at"],
                )
            except ValueError as exc:
                failures.append(str(exc))

        worker = threading.Thread(target=resolve_stale)
        worker.start()
        self.assertTrue(ready.wait(timeout=5))
        self.reviews.open(
            self.episode_id,
            ReviewReason.PATH_CONFLICT,
            {"reason": "reopened"},
            "organize_pending",
        )
        release.set()
        worker.join(timeout=5)
        other_store.close()

        self.assertFalse(worker.is_alive())
        self.assertEqual(len(failures), 1)
        self.assertEqual(
            self.store.get_review(review_id)["review_reason"],
            ReviewReason.PATH_CONFLICT.value,
        )

    def test_review_generation_is_monotonic_and_rejects_fixed_time_aba(self) -> None:
        with patch("pipeline.state.time.time", return_value=123.0):
            review_id = self.reviews.open(
                self.episode_id,
                ReviewReason.PATH_CONFLICT,
                {"reason": "first"},
                "organize_pending",
            )
            stale = self.store.get_review(review_id)
            self.reviews.open(
                self.episode_id,
                ReviewReason.PATH_CONFLICT,
                {"reason": "second"},
                "organize_pending",
            )
            current = self.store.get_review(review_id)

            self.assertEqual(stale["review_created_at"], current["review_created_at"])
            self.assertEqual(current["review_generation"], stale["review_generation"] + 1)
            with self.assertRaises(ValueError):
                self.store.resolve_review(
                    review_id,
                    {"action": "keep_existing"},
                    expected_review_generation=stale["review_generation"],
                    expected_review_reason=current["review_reason"],
                )

    def test_open_review_acquires_immediate_write_lock_before_reading_review(self) -> None:
        statements: list[str] = []
        self.store.conn.set_trace_callback(statements.append)
        try:
            self.store.open_review(
                self.episode_id,
                ReviewReason.PATH_CONFLICT,
                {"reason": "lock-order"},
            )
        finally:
            self.store.conn.set_trace_callback(None)

        begin = next(index for index, sql in enumerate(statements) if sql.startswith("BEGIN IMMEDIATE"))
        read = next(index for index, sql in enumerate(statements) if "SELECT * FROM episodes_v5" in sql)
        self.assertLess(begin, read)

    def test_duplicate_review_uses_the_same_immediate_lock_path(self) -> None:
        candidate_id = self.store.add_candidate(
            self.episode_id,
            {"infohash": "d" * 40},
        )
        self.store.select_candidate(candidate_id)
        statements: list[str] = []
        self.store.conn.set_trace_callback(statements.append)
        try:
            self.store.review_duplicate_candidates(
                self.episode_id,
                {"candidate_ids": [candidate_id]},
            )
        finally:
            self.store.conn.set_trace_callback(None)

        begin = next(index for index, sql in enumerate(statements) if sql.startswith("BEGIN IMMEDIATE"))
        read = next(index for index, sql in enumerate(statements) if "SELECT * FROM episodes_v5" in sql)
        self.assertLess(begin, read)
        self.assertEqual(self.store.selected_candidates(self.episode_id), [])

    def test_concurrent_reopens_have_distinct_generations_and_stale_same_reason_cannot_resolve(self) -> None:
        class CapturingStore(StateStore):
            def __init__(self, path: str) -> None:
                self.review_generations: list[int] = []
                super().__init__(path)

            def _update_episode(self, episode_id: int, updates: dict[str, object]) -> None:
                if "review_generation" in updates:
                    self.review_generations.append(int(updates["review_generation"]))
                super()._update_episode(episode_id, updates)

        db_path = os.path.join(self.tmp.name, "concurrent-reopen.db")
        seed = StateStore(db_path)
        episode_id = seed.get_or_create_episode(
            {
                "key": EpisodeKey("tmdb", "concurrent", 1, 1),
                "canonical_work": "Concurrent",
                "title": "Concurrent - S01E01",
                "status": EpisodeStatus.ORGANIZE_PENDING,
            }
        )
        seed.close()
        first = CapturingStore(db_path)
        second = CapturingStore(db_path)
        start = threading.Barrier(2)
        failures: list[BaseException] = []

        def reopen(store: CapturingStore, detail: str) -> None:
            try:
                start.wait(timeout=5)
                store.open_review(
                    episode_id,
                    ReviewReason.PATH_CONFLICT,
                    {"reason": detail},
                    resume_status=EpisodeStatus.ORGANIZE_PENDING,
                )
            except BaseException as exc:  # pragma: no cover - asserted below
                failures.append(exc)

        workers = [
            threading.Thread(target=reopen, args=(first, "first")),
            threading.Thread(target=reopen, args=(second, "second")),
        ]
        for worker in workers:
            worker.start()
        for worker in workers:
            worker.join(timeout=5)

        try:
            self.assertTrue(all(not worker.is_alive() for worker in workers))
            self.assertEqual(failures, [])
            self.assertEqual(
                sorted(first.review_generations + second.review_generations),
                [1, 2],
            )
            current = first.get_review(episode_id)
            self.assertEqual(current["review_generation"], 2)
            with self.assertRaises(ValueError):
                first.resolve_review(
                    episode_id,
                    {"action": "keep_existing"},
                    expected_review_generation=1,
                    expected_review_reason=ReviewReason.PATH_CONFLICT,
                )
        finally:
            first.close()
            second.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
