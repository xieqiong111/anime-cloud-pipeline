"""Regression contracts for the Task 13 architecture-review fixes."""
from __future__ import annotations

from pathlib import Path
import json
import sys
import tempfile
import unittest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.discovery import DiscoveryService
from pipeline.candidates import SubmissionBudget
from pipeline.models import EpisodeKey, EpisodeStatus
from pipeline.adapters import FakeMoviePilot
from pipeline.organizer import Organizer
from pipeline.paths import PathMapper, ServiceRoots
from pipeline.real_adapters import (
    KissSubFetcher,
    KissSubSourceProvider,
    P115CloudAdapter,
    RemoteInventoryIncomplete,
)
from pipeline.subscriptions import normalize_subscription
from pipeline.http import RetryPolicy
from pipeline.services import DownloadService, ReviewService
from pipeline.state import StateStore


class CompleteTreeClient:
    """Small local transport fake with explicit, complete directory envelopes."""

    def __init__(self, entries: dict[str, list[dict]], *, task_id: str = "task-1"):
        self.entries = {str(key): list(value) for key, value in entries.items()}
        self.task_id = task_id
        self.file_calls: list[str] = []
        self.submit_payloads: list[dict] = []
        self.mkdir_calls: list[tuple[dict, str]] = []

    def fs_files(self, payload: dict):
        request = dict(payload)
        token = str(request["cid"])
        offset = int(request.get("offset", 0))
        limit = int(request.get("limit", 30))
        self.file_calls.append(token)
        values = self.entries.get(token, [])
        return {
            "state": True,
            "cid": token,
            "data": list(values[offset:offset + limit]),
            "count": len(values),
            "offset": offset,
        }

    def clouddownload_task_add_bt(self, *args, **kwargs):
        raise AssertionError("physical add_bt must never be called")

    def clouddownload_task_add_url(self, payload, *, type):
        if type != "ssp":
            raise AssertionError("unexpected submission profile")
        self.submit_payloads.append(dict(payload))
        prefix = "magnet:?xt=urn:btih:"
        if set(payload) != {"url", "wp_path_id"} or not payload["url"].startswith(prefix):
            raise AssertionError("unexpected magnet payload")
        return {"state": True, "data": {"task_id": self.task_id,
                                         "info_hash": payload["url"][len(prefix):]}}

    def fs_mkdir(self, payload, *, pid):
        self.mkdir_calls.append((dict(payload), str(pid)))
        child_id = f"created-{len(self.mkdir_calls)}"
        name = str(payload["cname"])
        self.entries.setdefault(str(pid), []).append({
            "cid": child_id, "n": name, "type": "folder",
        })
        self.entries[child_id] = []
        return {"state": True, "data": {"cid": child_id, "name": name}}


class P115RootSeparationTests(unittest.TestCase):
    def test_common_root_resolves_source_target_and_path_ops_use_common_root(self):
        client = CompleteTreeClient({
            "0": [{"cid": "common", "n": "cloud", "type": "folder"}],
            "common": [
                {"cid": "source", "n": "source", "type": "folder"},
                {"cid": "library", "n": "library", "type": "folder"},
            ],
            "source": [],
            "library": [],
        })
        cloud = P115CloudAdapter(
            None,
            root_directory_id="common",
            client=client,
            root_directory_path="/cloud",
        )

        source_id = cloud.resolve_directory_path("/cloud/source")
        cloud.set_source_directory_id(source_id)
        self.assertEqual(source_id, "source")
        self.assertEqual(cloud.root_directory_id, "common")
        self.assertEqual(cloud.target_directory_id, "source")
        self.assertEqual(cloud.submit("magnet:?xt=urn:btih:" + "a" * 40), "task-1")
        self.assertEqual(client.submit_payloads, [{"url": "magnet:?xt=urn:btih:" + "a" * 40,
                                                  "wp_path_id": "source"}])

        cloud.mkdir("/cloud/library/Season 1")
        self.assertIn("0", client.file_calls)
        self.assertIn("common", client.file_calls)
        self.assertNotIn("source", client.file_calls[:2])
        self.assertEqual(client.mkdir_calls[-1][1], "library")

    def test_incomplete_or_ambiguous_source_chain_blocks_submit_without_mutation(self):
        client = CompleteTreeClient({
            "0": [{"cid": "common", "n": "cloud", "type": "folder"}],
            "common": [
                {"cid": "source-1", "n": "source", "type": "folder"},
                {"cid": "source-2", "n": "source", "type": "folder"},
            ],
            "source-1": [],
            "source-2": [],
        })
        cloud = P115CloudAdapter(
            None,
            root_directory_id="common",
            client=client,
            root_directory_path="/cloud",
        )
        with self.assertRaises(RemoteInventoryIncomplete):
            cloud.resolve_directory_path("/cloud/source")
        self.assertEqual(client.submit_payloads, [])
        self.assertEqual(client.mkdir_calls, [])


class DiscoveryWindowTests(unittest.TestCase):
    def test_single_generic_tmdb_metadata_requires_matching_season_evidence(self):
        base = {"id": "sub", "name": "Show", "tmdbid": "111", "season": 2}
        for metadata in ({"air_date": "2026-10-03"},
                         {"season_number": 1, "air_date": "2026-10-03"}):
            with self.subTest(metadata=metadata):
                self.assertEqual(normalize_subscription({**base, "tmdb_metadata": metadata}).season_air_date, "")
        self.assertEqual(normalize_subscription({
            **base, "tmdb_metadata": {"2": {"air_date": "2026-10-03"}},
        }).season_air_date, "2026-10-03")

    def _store(self):
        temporary = tempfile.TemporaryDirectory(prefix="task13-window-")
        return temporary, StateStore(Path(temporary.name) / "state.db")

    def test_date_window_is_timezone_strict_and_filters_each_subscription(self):
        subscriptions = [
            {"id": "sub-a", "name": "Show A", "tmdbid": "111", "season": 1,
             "tmdb_seasons": [{"season_number": 1, "air_date": "2026-09-03"}]},
            {"id": "sub-b", "name": "Show B", "tmdbid": "222", "season": 1,
             "tmdb_seasons": [{"season_number": 1, "air_date": "2026-09-03"}]},
        ]
        results = {
            "sub-a": [
                {"title": "Show A S01E01", "infohash": "a" * 40,
                 "published": "Sat, 19 Sep 2026 15:59:59 GMT"},
                {"title": "Show A S01E02", "infohash": "b" * 40,
                 "published": "Sat, 19 Sep 2026 16:00:00 GMT"},
            ],
            "sub-b": [
                {"title": "Show B S01E03", "infohash": "c" * 40,
                 "published": "Sat, 19 Sep 2026 16:00:00 GMT"},
            ],
        }

        class Provider:
            name = "kisssub"

            def discover(self, subscription, _window):
                return results[subscription.subscription_id]

        temporary, store = self._store()
        try:
            plan = DiscoveryService(
                store,
                FakeMoviePilot(subscriptions=subscriptions),
                [Provider()],
            ).prepare({
                "published_date": "2026-09-20",
                "timezone": "Asia/Shanghai",
                "season_quarter": "2026-07",
            }, False)
            self.assertEqual(
                [(item.subscription_id, item.episode) for item in plan.decisions],
                [("sub-a", 2), ("sub-b", 3)],
            )
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 2)
        finally:
            store.close()
            temporary.cleanup()

    def test_backfill_requires_identity_season_and_episode_range(self):
        subscriptions = [
            {"id": "sub-a", "name": "Show A", "tmdbid": "111", "season": 1,
             "tmdb_seasons": [{"season_number": 1, "air_date": "2026-10-03"}]},
            {"id": "sub-b", "name": "Show B", "tmdbid": "222", "season": 1,
             "tmdb_seasons": [{"season_number": 1, "air_date": "2026-10-03"}]},
        ]
        results = {
            "sub-a": [
                {"title": "Show A S01E02", "infohash": "a" * 40,
                 "media_source": "tmdb", "media_id": "111", "season": 1, "episode": 2},
                {"title": "Show A S01E04", "infohash": "b" * 40,
                 "media_source": "tmdb", "media_id": "111", "season": 1, "episode": 4},
                {"title": "Show A S02E02", "infohash": "c" * 40,
                 "media_source": "tmdb", "media_id": "111", "season": 2, "episode": 2},
                {"title": "Show A S01E02", "infohash": "d" * 40,
                 "media_source": "tmdb", "media_id": "999", "season": 1, "episode": 2},
            ],
            "sub-b": [{
                "title": "Show B S01E02", "infohash": "e" * 40,
                "media_source": "tmdb", "media_id": "222", "season": 1, "episode": 2,
            }],
        }

        class Provider:
            name = "kisssub"

            def discover(self, subscription, _window):
                return results[subscription.subscription_id]

        temporary, store = self._store()
        try:
            plan = DiscoveryService(
                store,
                FakeMoviePilot(subscriptions=subscriptions),
                [Provider()],
            ).prepare({
                "season_quarter": "2026-10",
                "backfill": {"media_id": "111", "season": 1, "start": 1, "end": 3},
            }, False)
            self.assertEqual([(item.subscription_id, item.episode) for item in plan.decisions], [("sub-a", 2)])
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 1)
        finally:
            store.close()
            temporary.cleanup()

    def test_backfill_rejects_overlapping_collection_without_persisting_out_of_range_episode(self):
        subscription = {
            "id": "sub-a", "name": "Show A", "tmdbid": "111", "season": 1,
            "tmdb_seasons": [{"season_number": 1, "air_date": "2026-10-03"}],
        }

        class Provider:
            name = "kisssub"

            def discover(self, _subscription, _window):
                return [{
                    "title": "Show A S01E01-E12",
                    "infohash": "a" * 40,
                    "media_source": "tmdb", "media_id": "111",
                    "season": 1, "episode": 1, "episode_end": 12,
                }]

        temporary, store = self._store()
        try:
            plan = DiscoveryService(
                store, FakeMoviePilot(subscriptions=[subscription]), [Provider()],
            ).prepare({
                "season_quarter": "2026-10",
                "backfill": {"media_id": "111", "season": 1, "start": 5, "end": 5},
            }, False)
            self.assertEqual(plan.decisions, ())
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM episodes_v5").fetchone()[0],
                0,
            )
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0],
                0,
            )
        finally:
            store.close()
            temporary.cleanup()

    def test_verified_tmdb_season_metadata_is_persisted_as_episode_routing_evidence(self):
        subscription = {
            "id": "sub-a", "name": "Show A", "tmdbid": "111", "season": 1,
            "tmdb_seasons": [{"season_number": 1, "air_date": "2026-10-03"}],
        }

        class Provider:
            name = "kisssub"

            def discover(self, _subscription, _window):
                return [{"title": "Show A S01E01", "infohash": "b" * 40}]

        temporary, store = self._store()
        try:
            plan = DiscoveryService(
                store, FakeMoviePilot(subscriptions=[subscription]), [Provider()],
            ).prepare({"season_quarter": "2026-10"}, False)
            self.assertEqual(len(plan.decisions), 1)
            episode = store.get_episode_by_key(EpisodeKey("tmdb", "111", 1, 1))
            self.assertIsNotNone(episode)
            evidence = json.loads(episode["runtime_identity_json"])
            self.assertEqual(evidence["_season_air_date"], "2026-10-03")
            self.assertEqual(evidence["season_air_date"], "2026-10-03")
            self.assertEqual(plan.subscriptions[0].season_air_date, "2026-10-03")
            snapshot = json.loads(store.subscriptions()[0]["snapshot_json"])
            self.assertEqual(snapshot["season_air_date"], "2026-10-03")
        finally:
            store.close()
            temporary.cleanup()

    def test_fully_contained_collection_is_written_for_each_matching_subscription(self):
        subscriptions = [
            {"id": "sub-a", "name": "Show A", "tmdbid": "111", "season": 1,
             "tmdb_seasons": [{"season_number": 1, "air_date": "2026-10-03"}]},
            {"id": "sub-b", "name": "Show B", "tmdbid": "111", "season": 1,
             "tmdb_seasons": [{"season_number": 1, "air_date": "2026-10-03"}]},
        ]

        class Provider:
            name = "kisssub"

            def discover(self, subscription, _window):
                return [{
                    "title": f"{subscription.work} S01E02-E04",
                    "infohash": ("c" if subscription.subscription_id == "sub-a" else "d") * 40,
                    "media_source": "tmdb", "media_id": "111", "season": 1,
                    "episode": 2, "episode_end": 4,
                }]

        temporary, store = self._store()
        try:
            plan = DiscoveryService(
                store, FakeMoviePilot(subscriptions=subscriptions), [Provider()],
            ).prepare({
                "season_quarter": "2026-10",
                "backfill": {"media_id": "111", "season": 1, "start": 1, "end": 5},
            }, False)
            self.assertEqual(
                [item.subscription_id for item in plan.decisions], ["sub-a", "sub-b"]
            )
            persisted_episodes = [
                int(row[0]) for row in store.conn.execute(
                    "SELECT episode FROM episodes_v5 ORDER BY id"
                ).fetchall()
            ]
            self.assertEqual(persisted_episodes, [2])
            self.assertNotIn(1, persisted_episodes)
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 2
            )
        finally:
            store.close()
            temporary.cleanup()

    def test_kisssub_provider_applies_date_window_before_returning_items(self):
        class Fetcher:
            def feed_for_keyword(self, keyword):
                self.keyword = keyword
                return "https://source.invalid/feed.xml"

            def fetch_feed(self, _url):
                return """<?xml version="1.0"?><rss><channel>
                <item><title>Show A S01E01</title>
                <link>https://source.invalid/show-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.html</link>
                <pubDate>Sat, 19 Sep 2026 15:59:59 GMT</pubDate></item>
                <item><title>Show A S01E02</title>
                <link>https://source.invalid/show-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb.html</link>
                <pubDate>Sat, 19 Sep 2026 16:00:00 GMT</pubDate></item>
                </channel></rss>"""

        subscription = normalize_subscription({
            "id": "sub-a", "name": "Show A", "tmdbid": "111", "season": 1,
        })
        results = KissSubSourceProvider(Fetcher()).discover(
            subscription,
            {"published_date": "2026-09-20", "timezone": "Asia/Shanghai"},
        )
        self.assertEqual([item.episode for item in results], [2])


class KissSubNetworkPolicyTests(unittest.TestCase):
    def test_fetcher_uses_configured_timeout_and_shared_retry_policy(self):
        policy = RetryPolicy(
            max_attempts=4,
            backoff_seconds=0.25,
            backoff_max_seconds=3,
            jitter_seconds=0.5,
            max_read_retries=3,
        )
        fetcher = KissSubFetcher(
            "https://source.invalid",
            timeout=17,
            retry_policy=policy,
        )
        self.assertEqual(fetcher.timeout, 17)
        self.assertIs(fetcher.retry_policy, policy)
        self.assertIs(fetcher._http.policy, policy)
        self.assertEqual(fetcher._http.timeout, 17)


class P115OrganizerAcceptanceTests(unittest.TestCase):
    def test_recursive_listing_emits_common_root_paths_and_parent_evidence(self):
        infohash = "c" * 40
        client = CompleteTreeClient({
            "0": [{"cid": "common", "n": "cloud", "type": "folder"}],
            "common": [{"cid": "source", "n": "source", "type": "folder"}],
            "source": [{
                "fid": "file-1", "n": "Show.S01E01.mkv", "type": "file",
                "size": 123, "sha1": "fingerprint-1",
            }],
        })

        class TaskClient(CompleteTreeClient):
            def clouddownload_task_list(self, _payload):
                return {"state": True, "data": {"items": [{
                    "info_hash": infohash, "task_id": "task-1", "cid": "source",
                }], "total": 1, "page": 1, "page_size": 30, "has_more": False}}

        client = TaskClient(client.entries)
        cloud = P115CloudAdapter(
            None, root_directory_id="common", client=client,
            root_directory_path="/cloud",
        )
        cloud.resolve_source_directory("/cloud/source")

        files = cloud.list_files("source")
        self.assertEqual(len(files), 1)
        self.assertEqual(files[0]["name"], "Show.S01E01.mkv")
        self.assertEqual(files[0]["filename"], "Show.S01E01.mkv")
        self.assertEqual(files[0]["parent_path"], "/cloud/source")
        self.assertEqual(files[0]["path"], "/cloud/source/Show.S01E01.mkv")
        self.assertEqual(files[0]["fingerprint"], "fingerprint-1")

    def test_real_p115_download_state_and_organizer_consume_persisted_file_and_date(self):
        self._download_organizer_chain(False)

    def test_multiple_video_review_resolution_projects_selected_inventory_for_organizer(self):
        self._download_organizer_chain(True)

    def _download_organizer_chain(self, multiple):
        infohash = "d" * 40

        class MovingClient(CompleteTreeClient):
            def clouddownload_task_list(self, _payload):
                return {"state": True, "data": {"items": [{
                    "info_hash": infohash, "task_id": "task-1", "cid": "source",
                }], "total": 1, "page": 1, "page_size": 30, "has_more": False}}

            def fs_move(self, file_id, *, pid):
                for parent, values in self.entries.items():
                    for index, item in enumerate(values):
                        if str(item.get("fid", "")) == str(file_id):
                            self.entries.setdefault(str(pid), []).append(values.pop(index))
                            return {"state": True, "data": {"fid": str(file_id)}}
                raise AssertionError("selected source file was not found")

            def fs_rename_no_replace(self, payload):
                file_id = str(payload["fid"])
                for values in self.entries.values():
                    for item in values:
                        if str(item.get("fid", "")) == file_id:
                            item["n"] = str(payload["name"])
                            return {"state": True, "data": {"fid": file_id}}
                raise AssertionError("moved source file was not found")

        client = MovingClient({
            "0": [{"cid": "common", "n": "cloud", "type": "folder"}],
            "common": [
                {"cid": "source", "n": "source", "type": "folder"},
                {"cid": "library", "n": "library", "type": "folder"},
            ],
            "source": [{
                "fid": "file-1", "n": "Show.S01E01.mkv", "type": "file",
                "size": 123, "sha1": "fingerprint-1",
            }],
            "library": [],
        })
        cloud = P115CloudAdapter(
            None, root_directory_id="common", client=client,
            root_directory_path="/cloud",
        )
        if multiple:
            client.entries["source"].append({
                "fid": "file-2", "n": "Show.S01E01.mp4", "type": "file",
                "size": 123, "sha1": "fingerprint-2",
            })
        source_id = cloud.resolve_source_directory("/cloud/source")

        class Provider:
            name = "kisssub"

            def discover(self, _subscription, _window):
                return [{"title": "Show A S01E01", "infohash": infohash}]

        temporary = tempfile.TemporaryDirectory(prefix="task13-chain-")
        store = StateStore(Path(temporary.name) / "state.db")
        try:
            plan = DiscoveryService(
                store,
                FakeMoviePilot(subscriptions=[{
                    "id": "sub-a", "name": "Show A", "tmdbid": "111", "season": 1,
                    "tmdb_seasons": [{"season_number": 1, "air_date": "2026-10-03"}],
                }]),
                [Provider()],
            ).prepare({"season_quarter": "2026-10"}, False)
            self.assertFalse(plan.errors)
            episode_id = store.get_episode_by_key(EpisodeKey("tmdb", "111", 1, 1))["id"]
            store.select_candidate(store.get_candidates(episode_id)[0]["id"])
            store.transition(
                episode_id, EpisodeStatus.SUBMITTED,
                extra={"remote_task_id": "task-1"},
            )

            download = DownloadService(
                store, cloud, budget=SubmissionBudget(1, 1),
                target_directory=source_id,
            )
            poll = download.poll(store.get_episode(episode_id))
            if multiple:
                self.assertEqual(poll.status, EpisodeStatus.NEEDS_REVIEW.value, poll.detail)
                self.assertEqual(store.get_episode(episode_id)["review_reason"], "MULTIPLE_VIDEO_FILES")
                file_row = store.conn.execute(
                    "SELECT id FROM download_files WHERE remote_file_id='file-1'"
                ).fetchone()[0]
                before = store.get_episode(episode_id)
                sources_before = [tuple(row) for row in store.conn.execute("SELECT * FROM episode_sources")]
                with self.assertRaisesRegex(RuntimeError, "injected failure"):
                    store.resolve_review(episode_id, selected_file_id=file_row,
                                         fail_after_episode_update=True)
                self.assertEqual(store.get_episode(episode_id), before)
                self.assertEqual([tuple(row) for row in store.conn.execute("SELECT * FROM episode_sources")], sources_before)
                ReviewService(store).resolve(episode_id, {"selected_file_id": file_row})
                self.assertEqual(store.get_episode(episode_id)["file_id"], "file-1")
            else:
                self.assertEqual(poll.status, EpisodeStatus.CLOUD_READY.value, poll.detail)
            row = store.get_episode(episode_id)
            evidence = json.loads(row["runtime_identity_json"])
            self.assertEqual(evidence["source_path"], "/cloud/source/Show.S01E01.mkv")
            self.assertEqual(evidence["remote_file_id"], "file-1")
            self.assertEqual(evidence["size"], 123)
            self.assertEqual(evidence["fingerprint"], "fingerprint-1")
            self.assertEqual(evidence["_season_air_date"], "2026-10-03")

            mapper = PathMapper(ServiceRoots(
                cloud_source_root="/cloud/source",
                cloud_library_root="/cloud/library",
                litepan_output_root="/litepan/output",
                moviepilot_mount_root="/moviepilot/media",
                emby_mount_root="/emby/media",
            ))
            result = Organizer(store, cloud, mapper).reconcile_or_apply(episode_id)
            self.assertEqual(result.status, EpisodeStatus.ORGANIZED.value, result.detail)
            self.assertEqual(result.move_count, 1)
            self.assertEqual(result.rename_count, 0)
            self.assertEqual(result.cloud_path.rsplit("/", 1)[-1], "Show.S01E01.mkv")
            self.assertTrue(result.cloud_path.startswith("/cloud/library/"))
            self.assertEqual(store.get_episode(episode_id)["status"], EpisodeStatus.ORGANIZED.value)
            self.assertEqual(client.submit_payloads, [])
        finally:
            store.close()
            temporary.cleanup()

    def test_missing_verified_season_date_is_excluded_before_p115_access(self):
        infohash = "e" * 40

        class MovingClient(CompleteTreeClient):
            def clouddownload_task_list(self, _payload):
                return {"state": True, "data": {"items": [{
                    "info_hash": infohash, "task_id": "task-1", "cid": "source",
                }], "total": 1, "page": 1, "page_size": 30, "has_more": False}}

            def fs_move(self, *_args, **_kwargs):
                raise AssertionError("move must not be called")

            def fs_rename_no_replace(self, *_args, **_kwargs):
                raise AssertionError("rename must not be called")

        client = MovingClient({
            "0": [{"cid": "common", "n": "cloud", "type": "folder"}],
            "common": [
                {"cid": "source", "n": "source", "type": "folder"},
                {"cid": "library", "n": "library", "type": "folder"},
            ],
            "source": [{
                "fid": "file-1", "n": "Show.S01E01.mkv", "type": "file",
                "size": 123, "sha1": "fingerprint-1",
            }],
            "library": [],
        })
        cloud = P115CloudAdapter(
            None, root_directory_id="common", client=client,
            root_directory_path="/cloud",
        )
        temporary = tempfile.TemporaryDirectory(prefix="task13-date-")
        store = StateStore(Path(temporary.name) / "state.db")
        try:
            provider_calls = []

            class Provider:
                def discover(self, _subscription, _window):
                    provider_calls.append(True)
                    return [{"title": "Show A S01E01", "infohash": infohash}]

            plan = DiscoveryService(
                store,
                FakeMoviePilot(subscriptions=[{
                    "id": "sub-a", "name": "Show A", "tmdbid": "111", "season": 1,
                    "tmdb_metadata": {"air_date": "2026-10-03"},
                }]),
                [Provider()],
            ).prepare({"season_quarter": "2026-10"}, False)
            self.assertEqual(plan.subscriptions, ())
            self.assertEqual([item.reason for item in plan.exclusions], ["season_date_missing"])
            self.assertEqual(plan.errors, ())
            self.assertEqual(provider_calls, [])
            self.assertEqual(store.subscriptions(), [])
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM episodes_v5").fetchone()[0], 0
            )
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 0
            )
            self.assertEqual(client.file_calls, [])
            self.assertEqual(client.submit_payloads, [])
            self.assertEqual(client.mkdir_calls, [])
        finally:
            store.close()
            temporary.cleanup()

    def test_real_adapter_source_payload_is_accepted_by_organizer(self):
        class MovingClient(CompleteTreeClient):
            def fs_move(self, file_id, *, pid):
                source_id = str(file_id)
                target_id = str(pid)
                for parent, values in self.entries.items():
                    for index, item in enumerate(values):
                        if str(item.get("fid", "")) != source_id:
                            continue
                        moved = dict(values.pop(index))
                        self.entries.setdefault(target_id, []).append(moved)
                        return {"state": True}
                raise AssertionError("selected source file was not found")

            def fs_rename_no_replace(self, payload):
                file_id = str(payload["fid"])
                for values in self.entries.values():
                    for item in values:
                        if str(item.get("fid", "")) == file_id:
                            item["n"] = str(payload["name"])
                            return {"state": True}
                raise AssertionError("moved source file was not found")

        client = MovingClient({
            "0": [{"cid": "common", "n": "cloud", "type": "folder"}],
            "common": [
                {"cid": "source", "n": "source", "type": "folder"},
                {"cid": "library", "n": "library", "type": "folder"},
            ],
            "source": [{"fid": "file-1", "n": "incoming.mkv", "type": "file", "size": 100}],
            "library": [],
        })
        cloud = P115CloudAdapter(
            None,
            root_directory_id="common",
            client=client,
            root_directory_path="/cloud",
        )
        cloud.resolve_source_directory("/cloud/source")

        mapper = PathMapper(ServiceRoots(
            cloud_source_root="/cloud/source",
            cloud_library_root="/cloud/library",
            litepan_output_root="/litepan/output",
            moviepilot_mount_root="/moviepilot/media",
            emby_mount_root="/emby/media",
        ))
        temporary = tempfile.TemporaryDirectory(prefix="task13-organizer-")
        store = StateStore(Path(temporary.name) / "state.db")
        try:
            episode_id = store.get_or_create_episode({
                "media_source": "tmdb", "media_id": "111", "season": 1,
                "episode": 8, "canonical_work": "Show", "status": EpisodeStatus.CLOUD_READY,
            })
            result = Organizer(store, cloud, mapper).reconcile_or_apply({
                "id": episode_id,
                "media_source": "tmdb", "media_id": "111", "season": 1,
                "episode": 8, "canonical_work": "Show",
                "season_air_date": "2026-10-03",
                "source_path": "/cloud/source/incoming.mkv",
                "selected_file": {
                    "remote_file_id": "file-1", "filename": "incoming.mkv",
                    "path": "/cloud/source/incoming.mkv", "size": 100,
                },
                "file_id": "file-1", "remote_file_id": "file-1",
                "file_size": 100, "status": EpisodeStatus.CLOUD_READY.value,
            })
            self.assertEqual(result.status, EpisodeStatus.ORGANIZED.value, result.detail)
            self.assertEqual(result.move_count, 1)
            self.assertEqual(result.rename_count, 0)
            self.assertEqual(result.cloud_path.rsplit("/", 1)[-1], "incoming.mkv")
            self.assertTrue(result.cloud_path.startswith("/cloud/library/"))
            self.assertIsNotNone(cloud.get_file(result.cloud_path))
            self.assertEqual(store.get_episode(episode_id)["status"], EpisodeStatus.ORGANIZED.value)
        finally:
            store.close()
            temporary.cleanup()


if __name__ == "__main__":
    unittest.main()
