from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import patch

from pipeline.adapters import RemoteDirectoryMissing, RemoteInventoryIncomplete
from pipeline.real_adapters import MoviePilotClient, P115CloudAdapter


def runtime_config(subscription_sources=()):
    return {
        "services": {
            "moviepilot": {"subscription_sources": tuple(subscription_sources)},
        },
        "limits": {
            "network_timeout_seconds": 7,
            "max_read_retries": 2,
            "max_task_pages": 4,
            "max_recursive_depth": 3,
            "max_recursive_files": 30,
        },
        "retry": {
            "max_attempts": 2,
            "backoff_seconds": 0,
            "backoff_max_seconds": 1,
            "jitter_seconds": 0,
        },
    }


def detail_payload():
    return {
        "success": True,
        "data": {
            "season_info": [{"season_number": 1, "air_date": "2026-07-03"}],
            "genre_ids": [16],
        },
    }


class MoviePilotEnrichmentTests(unittest.TestCase):
    def test_allowlist_skips_details_for_unlisted_missing_and_explicitly_excluded(self):
        raw = [
            {"id": "allowed", "username": "Bangumi", "media_source": "tmdb", "media_id": "101"},
            {"id": "unlisted", "username": "other", "media_source": "tmdb", "media_id": "202"},
            {"id": "no-username", "media_source": "tmdb", "media_id": "303"},
            {"id": "disabled", "username": "admin", "enabled": False,
             "media_source": "tmdb", "media_id": "404"},
            {"id": "same-media", "username": "admin", "media_source": "tmdb", "media_id": "101"},
        ]
        client = MoviePilotClient(
            "https://moviepilot.invalid", token="test-token",
            runtime_config=runtime_config(("Bangumi", "admin")),
        )
        detail_ids = []

        def fake_call(method, path, params=None, body=None, timeout=None):
            if path == "/api/v1/subscribe/":
                return 200, raw
            detail_ids.append(path.rsplit("/", 1)[-1])
            return 200, detail_payload()

        client._call = fake_call
        result = client.list_subscriptions()

        self.assertEqual(client.subscription_sources, ("Bangumi", "admin"))
        self.assertEqual(detail_ids, ["101"])
        self.assertEqual(len(result), len(raw))
        self.assertEqual(result[0]["tmdb_genre_ids"], [16])
        for index in (1, 2, 3):
            self.assertEqual(result[index], raw[index])
        self.assertEqual(result[4]["tmdb_seasons"], result[0]["tmdb_seasons"])

    def test_empty_allowlist_preserves_enrichment_behavior(self):
        raw = [
            {"id": "disabled", "username": "other", "enabled": False,
             "media_source": "tmdb", "media_id": "101"},
            {"id": "missing-user", "media_source": "tmdb", "media_id": "202"},
            {"id": "same-id", "media_source": "tmdb", "media_id": "101"},
        ]
        client = MoviePilotClient(
            "https://moviepilot.invalid", token="test-token",
            runtime_config=runtime_config(),
        )
        detail_ids = []

        def fake_call(method, path, params=None, body=None, timeout=None):
            if path == "/api/v1/subscribe/":
                return 200, raw
            detail_ids.append(path.rsplit("/", 1)[-1])
            return 200, detail_payload()

        client._call = fake_call
        result = client.list_subscriptions()

        self.assertEqual(client.subscription_sources, ())
        self.assertEqual(detail_ids, ["101", "202"])
        self.assertTrue(all("tmdb_seasons" in item for item in result))


class _DirectoryClient:
    def __init__(self, *, fail_ids=()):
        self.fail_ids = {str(value) for value in fail_ids}
        self.entries = {
            "0": [{"cid": "root", "n": "Root", "type": "folder"}],
            "root": [],
        }

    def fs_files(self, payload):
        directory_id = str(payload["cid"])
        if directory_id in self.fail_ids:
            return {"state": False, "data": []}
        entries = self.entries.get(directory_id, [])
        offset = int(payload.get("offset", 0))
        limit = int(payload.get("limit", 30))
        return {
            "state": True,
            "cid": directory_id,
            "data": entries[offset:offset + limit],
            "count": len(entries),
            "offset": offset,
        }


class P115AdapterTests(unittest.TestCase):
    def test_complete_parent_inventory_distinguishes_missing_directory_from_failure(self):
        missing = P115CloudAdapter(
            None, "root", client=_DirectoryClient(), root_directory_path="/Root",
        )
        with self.assertRaises(RemoteDirectoryMissing):
            missing.resolve_directory_path("/Root/Absent")

        unavailable = P115CloudAdapter(
            None, "root", client=_DirectoryClient(fail_ids={"root"}),
            root_directory_path="/Root",
        )
        with self.assertRaises(RemoteInventoryIncomplete) as caught:
            unavailable.resolve_directory_path("/Root/Absent")
        self.assertNotIsInstance(caught.exception, RemoteDirectoryMissing)

    def test_sdk_request_receives_configured_timeout_without_network(self):
        class FakeP115Client:
            def __init__(self, credential):
                self.credential = credential
                self.request_calls = []

            def request(self, *args, **kwargs):
                self.request_calls.append((args, dict(kwargs)))
                return {"ok": True}

        module = types.ModuleType("p115client")
        module.P115Client = FakeP115Client
        config = runtime_config()
        config["limits"]["network_timeout_seconds"] = 13
        with patch.dict(sys.modules, {"p115client": module}):
            adapter = P115CloudAdapter(
                None, "root", credential="mock-credential", runtime_config=config,
            )
            result = adapter._client.request(url="https://unused.invalid")

        self.assertEqual(result, {"ok": True})
        self.assertEqual(adapter._client.request_calls[0][1]["timeout"], 13)

    def test_sdk_request_uses_30_second_default_without_runtime_config(self):
        class FakeP115Client:
            def __init__(self, credential):
                self.credential = credential
                self.request_calls = []

            def request(self, *args, **kwargs):
                self.request_calls.append((args, dict(kwargs)))
                return {"ok": True}

        module = types.ModuleType("p115client")
        module.P115Client = FakeP115Client
        with patch.dict(sys.modules, {"p115client": module}):
            adapter = P115CloudAdapter(
                None, "root", credential="mock-credential", max_task_pages=1,
                max_recursive_depth=1, max_recursive_files=1,
            )
            adapter._client.request(url="https://unused.invalid")

        self.assertEqual(adapter._client.request_calls[0][1]["timeout"], 30)


if __name__ == "__main__":
    unittest.main(verbosity=2)
