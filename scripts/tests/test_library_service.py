"""Focused Task 9 tests for recoverable Emby library reconciliation."""
from __future__ import annotations

import json
import os
import tempfile
import threading
import unittest

from pipeline.http import HttpError
from pipeline.models import EpisodeKey, EpisodeStatus
from pipeline.paths import PathMapper, ServiceRoots
from pipeline.services import LibraryService
from pipeline.state import StateStore


MAPPER = PathMapper(ServiceRoots(
    cloud_source_root="/cloud-source",
    cloud_library_root="/cloud",
    litepan_output_root="/lite",
    moviepilot_mount_root="/moviepilot",
    emby_mount_root="/emby",
))
LITE_PATH = "/lite/Show/Season 1/Show - S01E08.strm"
EXPECTED_PATH = MAPPER.litepan_to_emby(LITE_PATH)
OLD_PATH = "/emby/old/Show - S01E08.strm"


def episode_item(path=EXPECTED_PATH):
    return {
        "Id": "ep-1", "Path": path, "ProviderIds": {"Tmdb": "111"},
        "SeriesId": "series-1", "SeriesName": "Show",
        "ParentIndexNumber": 1, "IndexNumber": 8,
    }


class FakeEmby:
    def __init__(self, item=None, *, libraries=None, error=None):
        self.item = item
        self.libraries = libraries if libraries is not None else [{
            "Id": "lib-1", "Name": "Anime", "Locations": ["/emby"],
        }]
        self.error = error
        self.refresh_calls = []
        self.query_calls = []
        self.library_calls = []

    def find_library(self, name, root):
        self.library_calls.append((name, root))
        if self.error is not None:
            raise self.error
        matches = [lib for lib in self.libraries
                   if lib.get("Name") == name and root in lib.get("Locations", [])]
        if len(matches) != 1:
            raise RuntimeError("library inventory ambiguous")
        return matches[0]

    def find_episode(self, identity, expected_path, **kwargs):
        self.query_calls.append((identity, expected_path))
        if self.item is None:
            return None
        return self.item if self.item.get("Path") == expected_path else None

    def refresh_library(self, library_id):
        self.refresh_calls.append(library_id)
        return {"accepted": True}


class BarrierEmby(FakeEmby):
    def __init__(self, *args, barrier=None, crash=False, **kwargs):
        super().__init__(*args, **kwargs)
        self.barrier = barrier
        self.crash = crash
        self._refresh_lock = threading.Lock()

    def find_episode(self, identity, expected_path, **kwargs):
        result = super().find_episode(identity, expected_path, **kwargs)
        if self.barrier is not None:
            self.barrier.wait(timeout=5)
        return result

    def refresh_library(self, library_id):
        with self._refresh_lock:
            self.refresh_calls.append(library_id)
            if self.crash:
                self.crash = False
                raise KeyboardInterrupt("simulated crash after claim")
        return {"accepted": True}


class LibraryServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def create_episode(self, **overrides):
        values = {
            "key": EpisodeKey("tmdb", "111", 1, 8),
            "canonical_work": "Show", "aliases": ["Alias"],
            "title": "Show - S01E08", "status": EpisodeStatus.METADATA_READY,
            "cloud_path": "/cloud/Show/Season 1/Show - S01E08.mkv",
            "strm_path": LITE_PATH,
        }
        values.update(overrides)
        return self.store.get_or_create_episode(values)

    def service(self, emby, **kwargs):
        return LibraryService(
            self.store, emby, MAPPER, library_name="Anime", **kwargs
        )

    def test_query_before_refresh_and_exact_item_avoids_refresh(self):
        episode_id = self.create_episode()
        emby = FakeEmby(episode_item())

        result = self.service(emby).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.LIBRARY_READY.value)
        self.assertEqual(emby.refresh_calls, [])
        self.assertEqual(len(self.store.pending_notifications()), 1)

    def test_old_path_stays_pending_and_refreshes_once(self):
        episode_id = self.create_episode()
        emby = FakeEmby(episode_item(OLD_PATH))

        result = self.service(emby).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.LIBRARY_PENDING.value)
        self.assertEqual(self.store.episode_status(episode_id), "library_pending")
        self.assertEqual(emby.refresh_calls, ["lib-1"])
        self.assertEqual(len(self.store.pending_notifications()), 0)

    def test_absent_first_run_stays_pending_then_later_query_can_ready(self):
        episode_id = self.create_episode()
        emby = FakeEmby()
        clock = iter((100.0, 200.0))
        service = self.service(emby, now=lambda: next(clock), retry_base_seconds=10)

        first = service.advance(self.store.get_episode(episode_id))
        self.assertEqual(first.status, EpisodeStatus.LIBRARY_PENDING.value)
        self.assertEqual(emby.refresh_calls, ["lib-1"])
        self.assertEqual(self.store.get_episode(episode_id)["retry_count"], 1)
        self.assertEqual(self.store.get_episode(episode_id)["next_retry_at"], 110.0)

        emby.item = episode_item()
        second = service.advance(self.store.get_episode(episode_id))
        self.assertEqual(second.status, EpisodeStatus.LIBRARY_READY.value)
        self.assertEqual(emby.refresh_calls, ["lib-1"])

    def test_ready_transition_notification_is_idempotent_on_restart(self):
        episode_id = self.create_episode()
        emby = FakeEmby(episode_item())
        service = self.service(emby)

        service.advance(self.store.get_episode(episode_id))
        service.advance(self.store.get_episode(episode_id))

        self.assertEqual(self.store.episode_status(episode_id), "library_ready")
        self.assertEqual(len(self.store.pending_notifications()), 1)
        self.assertEqual(emby.refresh_calls, [])

    def test_library_errors_are_recoverable_and_secret_safe(self):
        episode_id = self.create_episode()
        secret = "emby-secret-123"
        error = HttpError(500, url=f"https://emby.invalid/Items?api_key={secret}")
        emby = FakeEmby(error=error)

        result = self.service(emby).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.LIBRARY_PENDING.value)
        row = self.store.get_episode(episode_id)
        self.assertNotIn(secret, row["last_error"])
        self.assertNotIn(secret, json.dumps(self.store.last_history_detail(episode_id)))
        self.assertEqual(emby.refresh_calls, [])

    def test_ambiguous_library_does_not_refresh(self):
        episode_id = self.create_episode()
        emby = FakeEmby(libraries=[
            {"Id": "lib-1", "Name": "Anime", "Locations": ["/emby"]},
            {"Id": "lib-2", "Name": "Anime", "Locations": ["/emby"]},
        ])

        result = self.service(emby).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.LIBRARY_PENDING.value)
        self.assertEqual(emby.refresh_calls, [])

    def test_concurrent_absent_queries_allow_only_one_refresh_claim(self):
        db_path = os.path.join(self.tmp.name, "concurrent.db")
        # Recreate the episode in a file-backed store so each worker has its
        # own SQLite connection and the claim is tested across transactions.
        self.store.close()
        self.store = StateStore(db_path)
        episode_id = self.create_episode()
        barrier = threading.Barrier(2)
        emby = BarrierEmby(barrier=barrier)
        errors = []

        def worker():
            worker_store = StateStore(db_path)
            try:
                worker_service = LibraryService(
                    worker_store, emby, MAPPER, library_name="Anime",
                    now=lambda: 100.0, retry_base_seconds=30,
                )
                worker_service.advance(worker_store.get_episode(episode_id))
            except BaseException as exc:  # pragma: no cover - assertion below
                errors.append(exc)
            finally:
                worker_store.close()

        threads = [threading.Thread(target=worker) for _ in range(2)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join(timeout=10)

        self.assertEqual(errors, [])
        self.assertEqual(emby.refresh_calls, ["lib-1"])
        row = self.store.get_episode(episode_id)
        self.assertEqual(row["retry_count"], 1)
        self.assertGreater(row["next_retry_at"], 0)

    def test_crash_after_refresh_claim_keeps_lease_and_blocks_duplicate(self):
        episode_id = self.create_episode()
        current_time = [100.0]
        clock = lambda: current_time[0]
        emby = BarrierEmby(crash=True)
        service = self.service(emby, now=clock, retry_base_seconds=30)

        with self.assertRaises(KeyboardInterrupt):
            service.advance(self.store.get_episode(episode_id))
        claimed = self.store.get_episode(episode_id)
        self.assertEqual(claimed["retry_count"], 1)
        self.assertGreater(claimed["next_retry_at"], 100.0)

        result = service.advance(self.store.get_episode(episode_id))
        self.assertEqual(result.status, EpisodeStatus.LIBRARY_PENDING.value)
        self.assertEqual(emby.refresh_calls, ["lib-1"])

        current_time[0] = 131.0
        service.advance(self.store.get_episode(episode_id))
        retried = self.store.get_episode(episode_id)
        self.assertEqual(emby.refresh_calls, ["lib-1", "lib-1"])
        self.assertEqual(retried["retry_count"], 2)
        self.assertEqual(retried["next_retry_at"], 191.0)

    def test_authoritative_path_is_required_and_strict(self):
        episode_id = self.create_episode()
        emby = FakeEmby(episode_item())
        service = self.service(emby)

        for overrides in (
            {"cloud_path": None, "strm_path": None,
             "emby_path": EXPECTED_PATH},
            {"cloud_path": None, "strm_path": "/emby/Show/episode.strm"},
            {"cloud_path": " /cloud/Show/Season 1/Show - S01E08.mkv"},
            {"cloud_path": "/cloud/Show/Season 1/Show%20-%20S01E08.mkv"},
            {"cloud_path": "/cloud/Show/Season 1/../Show - S01E08.mkv"},
            {"cloud_path": None, "strm_path": "/lite/Show/Season 1/Show - S01E08.txt"},
        ):
            with self.subTest(overrides=overrides):
                row = dict(self.store.get_episode(episode_id))
                row.update(overrides)
                result = service.advance(row)
                self.assertEqual(result.status, EpisodeStatus.LIBRARY_PENDING.value)
                self.assertEqual(emby.refresh_calls, [])

    def test_ready_clears_retry_metadata(self):
        episode_id = self.create_episode(retry_count=3, next_retry_at=9999,
                                         last_error="old error")
        result = self.service(FakeEmby(episode_item())).advance(
            self.store.get_episode(episode_id)
        )
        self.assertEqual(result.status, EpisodeStatus.LIBRARY_READY.value)
        row = self.store.get_episode(episode_id)
        self.assertEqual(row["retry_count"], 0)
        self.assertIsNone(row["next_retry_at"])
        self.assertEqual(row["last_error"], "")


if __name__ == "__main__":
    unittest.main()
