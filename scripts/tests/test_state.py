from __future__ import annotations

import json
import os
import sqlite3
import tempfile
import time
import threading
import unittest
from concurrent.futures import ThreadPoolExecutor
from unittest import mock

from pipeline.models import EpisodeKey, EpisodeStatus, ReviewReason, Subscription
from pipeline.state import ProcessLock, StateStore


def valid_episode(**overrides: object) -> dict[str, object]:
    result: dict[str, object] = {
        "key": EpisodeKey("tmdb", "111", 1, 8),
        "subscription_id": "sub-1",
        "canonical_work": "Example Show",
        "aliases": ("Example",),
        "status": EpisodeStatus.DISCOVERED,
    }
    result.update(overrides)
    return result


class ProcessLockTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "run.lock")

    def tearDown(self):
        self.tmp.cleanup()

    def write_pid(self, value):
        with open(self.path, "w") as handle:
            handle.write(value)

    def test_empty_or_invalid_pid_fails_closed(self):
        for value in ("", " ", "broken", "0", "-1", "123" * 30):
            self.write_pid(value)
            lock = ProcessLock(self.path)
            with mock.patch.object(lock, "_pid_alive") as alive:
                self.assertFalse(lock.acquire())
            alive.assert_not_called()
            with open(self.path) as handle:
                self.assertEqual(handle.read(), value)

    def test_contender_cannot_reap_creator_before_pid_write(self):
        owner = ProcessLock(self.path)
        contender = ProcessLock(self.path)
        real_write = os.write

        def during_write(fd, value):
            # O_EXCL created the file, but the creator has not written its PID.
            self.assertFalse(contender.acquire())
            return real_write(fd, value)

        with mock.patch("pipeline.state.os.write", side_effect=during_write):
            self.assertTrue(owner.acquire())
        self.assertFalse(contender.acquire())
        owner.release()
        self.assertFalse(os.path.exists(self.path))

    def test_only_one_stale_reaper_can_probe_and_remove(self):
        self.write_pid("99999999")
        entered, resume = threading.Event(), threading.Event()
        first, second = ProcessLock(self.path), ProcessLock(self.path)

        def probe(_pid):
            entered.set()
            if not resume.wait(2):
                raise AssertionError("reaper was not resumed")
            return False

        with mock.patch.object(first, "_pid_alive", side_effect=probe):
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(first.acquire)
                try:
                    self.assertTrue(entered.wait(2))
                    with mock.patch.object(second, "_pid_alive") as alive:
                        self.assertFalse(second.acquire())
                    alive.assert_not_called()
                finally:
                    resume.set()
                self.assertTrue(pending.result(timeout=2))
        self.assertFalse(second.acquire())
        first.release()

    def test_stale_reaper_rechecks_identity_and_pid_before_unlink(self):
        for replace_inode in (False, True):
            self.write_pid("99999999")
            lock = ProcessLock(self.path)

            def changed_while_probing(_pid):
                if replace_inode:
                    os.rename(self.path, self.path + ".old")
                self.write_pid(str(os.getpid()))
                return False

            with mock.patch.object(lock, "_pid_alive", side_effect=changed_while_probing):
                self.assertFalse(lock.acquire())
            with open(self.path) as handle:
                self.assertEqual(handle.read(), str(os.getpid()))

    def test_new_live_owner_between_reap_and_retry_is_preserved(self):
        self.write_pid("99999999")
        reaper, successor = ProcessLock(self.path), ProcessLock(self.path)
        unguard = reaper._unguard

        def handoff(fd):
            unguard(fd)
            if not os.path.exists(self.path):
                self.assertTrue(successor.acquire())

        with mock.patch.object(reaper, "_unguard", side_effect=handoff):
            self.assertFalse(reaper.acquire())
        self.assertIsNotNone(successor._owner)
        successor.release()

    def test_unknown_guard_is_preserved_and_blocks_even_without_lock(self):
        with open(self.path + ".reap", "w") as handle:
            handle.write("unknown")
        self.assertFalse(ProcessLock(self.path).acquire())
        self.assertFalse(os.path.exists(self.path))
        self.write_pid("99999999")
        self.assertFalse(ProcessLock(self.path).acquire())
        with open(self.path + ".reap") as handle:
            self.assertEqual(handle.read(), "unknown")

    def test_live_owner_can_release_while_reaper_guard_is_busy(self):
        owner = ProcessLock(self.path)
        self.assertTrue(owner.acquire())
        with open(self.path + ".reap", "w") as handle:
            handle.write("busy")
        owner.release()
        self.assertFalse(os.path.exists(self.path))
        with open(self.path + ".reap") as handle:
            self.assertEqual(handle.read(), "busy")

    def test_two_releases_cannot_remove_a_subsequent_owners_lock(self):
        owner, successor = ProcessLock(self.path), ProcessLock(self.path)
        self.assertTrue(owner.acquire())
        real_unlink = os.unlink

        def handoff(path, *args, **kwargs):
            real_unlink(path, *args, **kwargs)
            if path == self.path:
                self.assertTrue(successor.acquire())

        with mock.patch("pipeline.state.os.unlink", side_effect=handoff):
            with ThreadPoolExecutor(max_workers=2) as pool:
                list(pool.map(lambda _: owner.release(), range(2)))
        self.assertTrue(os.path.exists(self.path))
        self.assertIsNotNone(successor._owner)
        successor.release()

    def test_release_requires_same_object_pid_inode_and_content(self):
        owner, outsider = ProcessLock(self.path), ProcessLock(self.path)
        self.assertTrue(owner.acquire())
        outsider.release()
        self.assertTrue(os.path.exists(self.path))
        with mock.patch("pipeline.state.os.getpid", return_value=os.getpid() + 1):
            owner.release()
        self.assertTrue(os.path.exists(self.path))
        os.rename(self.path, self.path + ".old")
        self.write_pid(str(os.getpid()))
        owner.release()
        self.assertTrue(os.path.exists(self.path))
        os.unlink(self.path)
        owner = ProcessLock(self.path)
        self.assertTrue(owner.acquire())
        self.write_pid("99999999")
        owner.release()
        self.assertTrue(os.path.exists(self.path))

    def test_permission_or_unknown_liveness_error_does_not_reap(self):
        for error in (PermissionError(), OSError("unknown"), OverflowError(), ValueError()):
            self.write_pid("99999999")
            with mock.patch("pipeline.state.os.kill", side_effect=error):
                self.assertFalse(ProcessLock(self.path).acquire())
            self.assertTrue(os.path.exists(self.path))


class StateStoreTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def test_episode_business_key_is_unique_across_candidates(self):
        first = self.store.get_or_create_episode(valid_episode())
        second = self.store.get_or_create_episode(
            valid_episode(candidate={"infohash": "b" * 40})
        )

        self.assertEqual(first, second)
        self.store.add_candidate(first, {"infohash": "a" * 40, "title": "A"})
        self.store.add_candidate(second, {"infohash": "b" * 40, "title": "B"})
        self.assertEqual(
            self.store.conn.execute(
                "SELECT COUNT(*) FROM episodes_v5 WHERE media_source='tmdb' AND media_id='111' AND season=1 AND episode=8"
            ).fetchone()[0],
            1,
        )

    def test_partial_unique_selected_candidate_enforced(self):
        episode = self.store.get_or_create_episode(valid_episode())
        one = self.store.add_candidate(episode, {"infohash": "a" * 40})
        two = self.store.add_candidate(episode, {"infohash": "b" * 40})
        self.store.select_candidate(one)

        with self.assertRaises(sqlite3.IntegrityError):
            self.store.conn.execute(
                "UPDATE candidates SET selected=1 WHERE id=?", (two,)
            )

    def test_one_download_file_can_map_to_multiple_episodes(self):
        first = self.store.get_or_create_episode(valid_episode())
        second = self.store.get_or_create_episode(
            valid_episode(key=EpisodeKey("tmdb", "111", 1, 9))
        )
        self.store.add_candidate(first, {"infohash": "a" * 40})
        self.store.add_candidate(second, {"infohash": "a" * 40})
        file_ids = self.store.record_download_files(
            "a" * 40,
            [{"remote_file_id": "file-1", "name": "Example.S01E08.mkv", "size": 10}],
        )
        self.store.map_episode_source(first, file_ids[0], selected=True)
        self.store.map_episode_source(second, file_ids[0], selected=True)

        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM episode_sources").fetchone()[0],
            2,
        )

    def test_unknown_inventory_file_is_enriched_by_later_known_identity_idempotently(self):
        unknown = self.store.record_download_files(
            "A" * 40,
            [
                {
                    "remote_file_id": None,
                    "path": None,
                    "name": "Example.S01E08.mkv",
                    "size": 10,
                    "duration": 60,
                    "fingerprint": "file-fingerprint",
                }
            ],
        )
        known = self.store.record_download_files(
            "a" * 40,
            [
                {
                    "remote_file_id": "file-8",
                    "path": "/Example.S01E08.mkv",
                    "name": "Example.S01E08.mkv",
                    "size": 10,
                    "duration": 60,
                    "fingerprint": "file-fingerprint",
                }
            ],
        )
        repeated = self.store.record_download_files(
            "a" * 40,
            [
                {
                    "remote_file_id": "file-8",
                    "path": "/Example.S01E08.mkv",
                    "name": "Example.S01E08.mkv",
                    "size": 10,
                    "duration": 60,
                    "fingerprint": "file-fingerprint",
                }
            ],
        )

        self.assertEqual(unknown, known)
        self.assertEqual(known, repeated)
        row = self.store.conn.execute(
            "SELECT remote_file_id, path FROM download_files WHERE id=?", (unknown[0],)
        ).fetchone()
        self.assertEqual(tuple(row), ("file-8", "/Example.S01E08.mkv"))
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM download_files").fetchone()[0],
            1,
        )

    def test_redacted_paths_are_not_used_as_unique_identity(self):
        first = self.store.record_download_files(
            "b" * 40,
            [{"remote_file_id": None, "path": "/library?token=one", "name": "one.mkv"}],
        )
        second = self.store.record_download_files(
            "b" * 40,
            [{"remote_file_id": None, "path": "/library?token=two", "name": "two.mkv"}],
        )

        self.assertNotEqual(first, second)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM download_files").fetchone()[0],
            2,
        )
        paths = [
            row[0]
            for row in self.store.conn.execute(
                "SELECT path FROM download_files ORDER BY id"
            ).fetchall()
        ]
        self.assertEqual(paths, ["/library?token=[REDACTED]"] * 2)

    def test_unknown_reconcile_does_not_cross_parent_conflicts(self):
        first = self.store.record_download_files(
            "c" * 40,
            [
                {
                    "remote_file_id": None,
                    "path": None,
                    "parent_id": "parent-a",
                    "name": "same.mkv",
                    "size": 10,
                    "duration": 60,
                }
            ],
        )
        second = self.store.record_download_files(
            "c" * 40,
            [
                {
                    "remote_file_id": "file-b",
                    "path": "/parent-b/same.mkv",
                    "parent_id": "parent-b",
                    "name": "same.mkv",
                    "size": 10,
                    "duration": 60,
                }
            ],
        )

        self.assertNotEqual(first, second)
        self.assertEqual(
            self.store.conn.execute("SELECT COUNT(*) FROM download_files").fetchone()[0],
            2,
        )

    def test_transition_and_history_roll_back_together(self):
        episode = self.store.create_episode(valid_episode())

        with self.assertRaises(RuntimeError):
            self.store.transition(episode, EpisodeStatus.SUBMITTED, fail_after_episode_update=True)

        self.assertEqual(self.store.get_episode(episode)["status"], "discovered")
        self.assertEqual(self.store.history(episode), ["discovered"])

    def test_mark_pending_applies_configured_secrets_to_history_and_last_error(self):
        episode = self.store.create_episode(valid_episode(status=EpisodeStatus.CLOUD_READY))
        secret = "state-cookie-456"

        self.store.mark_pending(
            episode,
            EpisodeStatus.ORGANIZE_PENDING,
            {"error": f"remote failed token={secret}"},
            extra={"last_error": secret},
            secrets=(secret,),
        )

        self.assertNotIn(secret, str(self.store.last_history_detail(episode)))
        self.assertNotIn(secret, str(self.store.get_episode(episode)["last_error"]))

    def test_review_open_persist_and_resolve_returns_to_resume_status(self):
        episode = self.store.create_episode(valid_episode(status="download_pending"))

        self.store.open_review(
            episode,
            ReviewReason.AMBIGUOUS_EPISODE,
            {"Authorization": "Bearer top-secret", "note": "choose one"},
        )
        row = self.store.get_episode(episode)
        self.assertEqual(row["status"], "needs_review")
        self.assertEqual(row["resume_status"], "download_pending")
        self.assertNotIn("top-secret", row["review_detail"])

        self.store.resolve_review(episode, {"choice": "candidate-1"})
        row = self.store.get_episode(episode)
        self.assertEqual(row["status"], "download_pending")
        self.assertIsNotNone(row["review_resolved_at"])

    def test_library_ready_and_notification_enqueue_are_atomic(self):
        episode = self.store.create_episode(valid_episode(status="library_pending"))

        self.store.transition_library_ready_with_notification(
            episode, "ready-key", "New episode"
        )
        self.assertEqual(self.store.get_episode(episode)["status"], "library_ready")
        self.assertEqual(len(self.store.pending_notifications()), 1)
        self.store.transition_library_ready_with_notification(
            episode, "ready-key", "New episode"
        )
        self.assertEqual(len(self.store.pending_notifications()), 1)

    def test_library_ready_notification_rolls_back_with_episode(self):
        episode = self.store.create_episode(valid_episode(status="library_pending"))

        with self.assertRaises(RuntimeError):
            self.store.transition_library_ready_with_notification(
                episode, "ready-key", "New episode", fail_after_episode_update=True
            )

        self.assertEqual(self.store.get_episode(episode)["status"], "library_pending")
        self.assertEqual(self.store.pending_notifications(), [])

    def test_library_ready_transition_is_history_idempotent(self):
        episode = self.store.create_episode(valid_episode(status="library_pending"))
        before = len(self.store.history(episode))

        self.store.transition_library_ready_with_notification(
            episode, "ready-key", "New episode"
        )
        self.store.transition_library_ready_with_notification(
            episode, "ready-key", "New episode"
        )

        self.assertEqual(len(self.store.history(episode)), before + 1)
        self.assertEqual(len(self.store.pending_notifications()), 1)

    def test_subscription_upsert_and_download_status_are_persisted(self):
        subscription = Subscription(
            "sub-1", "tmdb", "111", "Example Show", aliases=("Example",), season=1
        )
        first = self.store.upsert_subscription(subscription)
        second = self.store.upsert_subscription(
            {"subscription_id": "sub-1", "media_source": "tmdb", "media_id": "111",
             "work": "Renamed Show", "season": 1}
        )

        self.assertEqual(first, second)
        self.assertEqual(
            self.store.conn.execute("SELECT work FROM subscriptions WHERE id=?", (first,)).fetchone()[0],
            "Renamed Show",
        )

    def test_foreign_keys_reject_invalid_relationships(self):
        with self.assertRaises(sqlite3.IntegrityError):
            self.store.conn.execute(
                "INSERT INTO stage_history(episode_id, from_status, to_status, stage, detail, detail_json, created_at) VALUES (999, NULL, 'x', 'x', '{}', '{}', 1)"
            )

    def test_legacy_compatibility_methods_still_work(self):
        episode = self.store.create_episode(
            infohash="c" * 40,
            source_id="source",
            source_url="https://source.invalid",
            title="Example S01E01",
            work="Example",
            season=1,
            episode=1,
        )
        self.assertEqual(self.store.get_by_infohash("c" * 40)["id"], episode)
        self.store.set_stage(episode, "submitted", {"Authorization": "Bearer secret"})
        self.assertEqual(self.store.all_episodes()[0]["stage"], "submitted")
        self.assertNotIn("secret", json.dumps(self.store.last_history_detail(episode)))

    def test_unknown_episode_hashes_share_unresolved_identity_and_candidates(self):
        first = self.store.create_episode(
            infohash="a" * 40, source_id="source-a", source_url="url-a",
            title="Example unknown A", work="Example", season=1,
        )
        second = self.store.create_episode(
            infohash="b" * 40, source_id="source-b", source_url="url-b",
            title="Example unknown B", work="Example", season=1,
        )

        self.assertEqual(first, second)
        row = self.store.get_episode(first)
        self.assertEqual(row["status"], "needs_review")
        self.assertNotIn("a" * 40, row["media_id"])
        self.assertNotIn("b" * 40, row["media_id"])
        candidates = self.store.conn.execute(
            "SELECT infohash FROM candidates WHERE episode_id=? ORDER BY infohash",
            (first,),
        ).fetchall()
        self.assertEqual([candidate[0] for candidate in candidates], ["a" * 40, "b" * 40])

    def test_unknown_episode_does_not_merge_real_episode_one(self):
        unresolved = self.store.create_episode(
            infohash="a" * 40, source_id="source-a", source_url="url-a",
            title="Example unknown", work="Example", season=1,
        )
        real = self.store.create_episode(
            infohash="c" * 40, source_id="source-c", source_url="url-c",
            title="Example S01E01", work="Example", season=1, episode=1,
        )

        self.assertNotEqual(unresolved, real)
        unresolved_row = self.store.get_episode(unresolved)
        real_row = self.store.get_episode(real)
        self.assertEqual(unresolved_row["status"], "needs_review")
        self.assertEqual(unresolved_row["episode"], 1)
        self.assertTrue(unresolved_row["media_id"].startswith("legacy-unresolved:"))
        self.assertEqual(real_row["media_id"], "legacy-work:Example")
        self.assertEqual(real_row["episode"], 1)
        self.assertNotEqual(real_row["status"], "needs_review")
        self.assertEqual(
            [row[0] for row in self.store.conn.execute(
                "SELECT infohash FROM candidates WHERE episode_id=?", (unresolved,)
            ).fetchall()],
            ["a" * 40],
        )

    def test_concurrent_get_or_create_episode_is_idempotent_and_writes_one_creation_history(self):
        barrier = threading.Barrier(2)

        class CoordinatedConnection(sqlite3.Connection):
            def execute(self, sql, parameters=()):
                if sql.strip().startswith("SELECT id FROM episodes_v5"):
                    probe = super().execute(sql, parameters)
                    if probe.fetchone() is None:
                        if not self.in_transaction:
                            barrier.wait(timeout=5)
                            return super().execute(sql, parameters)
                        deadline = time.monotonic() + 5
                        while time.monotonic() < deadline:
                            time.sleep(0.005)
                            if super().execute(sql, parameters).fetchone() is not None:
                                return super().execute(sql, parameters)
                    return super().execute(sql, parameters)
                return super().execute(sql, parameters)

        path = os.path.join(self.tmp.name, "concurrent.db")
        first_connection = sqlite3.connect(
            path, factory=CoordinatedConnection, check_same_thread=False
        )
        first = StateStore(first_connection)
        first.close()
        second_connection = sqlite3.connect(
            path, factory=CoordinatedConnection, check_same_thread=False
        )
        second = StateStore(second_connection)
        try:
            with ThreadPoolExecutor(max_workers=2) as pool:
                ids = list(pool.map(
                    lambda store: store.get_or_create_episode(valid_episode()),
                    (first, second),
                ))
            self.assertEqual(ids[0], ids[1])
            self.assertEqual(
                first.conn.execute(
                    "SELECT COUNT(*) FROM episodes_v5 WHERE media_source='tmdb' AND media_id='111' AND season=1 AND episode=8"
                ).fetchone()[0],
                1,
            )
            self.assertEqual(
                first.conn.execute(
                    "SELECT COUNT(*) FROM stage_history WHERE episode_id=? AND to_status='discovered'",
                    (ids[0],),
                ).fetchone()[0],
                1,
            )
        finally:
            first.conn.close()
            second.conn.close()

    def test_candidate_raw_and_unapproved_traits_are_not_persisted(self):
        episode = self.store.get_or_create_episode(valid_episode())
        self.store.add_candidate(
            episode,
            {
                "infohash": "d" * 40,
                "title": "Example Show S01E08",
                "raw": {"token": "candidate-secret", "url": "https://source.invalid"},
                "traits": {
                    "safe": "candidate-safe",
                    "raw": {"token": "candidate-secret-2"},
                },
            },
        )

        row = self.store.conn.execute(
            "SELECT traits_json FROM candidates WHERE episode_id=?", (episode,)
        ).fetchone()
        self.assertIsNotNone(row)
        traits = json.loads(row[0])
        self.assertNotIn("raw", traits)
        self.assertNotIn("safe", traits)
        self.assertNotIn("candidate-secret", "\n".join(self.store.conn.iterdump()))


if __name__ == "__main__":
    unittest.main()
