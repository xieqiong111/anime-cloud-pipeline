from __future__ import annotations

import os
from types import SimpleNamespace
import unittest
from unittest.mock import patch

from pipeline.resource_commands import qualify_current_subscriptions


def config():
    service = SimpleNamespace(
        base_url="http://invalid.local", secret_reference="QUALIFICATION_TEST_SECRET",
        auth_mode="x-api-key", subscription_sources=("other",),
    )
    return SimpleNamespace(
        services=SimpleNamespace(moviepilot=service),
        limits=SimpleNamespace(max_read_retries=2, network_timeout_seconds=10),
        retry=SimpleNamespace(max_attempts=2, backoff_seconds=0,
                              backoff_max_seconds=1, jitter_seconds=0),
    )


def row(username="admin"):
    return {
        "id": 1, "username": username, "type": "电视剧", "state": "R",
        "media_source": "themoviedb", "media_id": 123, "season": 2,
        "name": "private-name",
    }


def detail():
    return {"success": True, "data": {
        "genre_ids": [16],
        "season_info": [{"season_number": 1, "air_date": "2020-01-01"},
                        {"season_number": 2, "air_date": "2026-10-03"}],
    }}


class ResourceQualificationCommandTests(unittest.TestCase):
    def call(self, response, media=None):
        calls = []

        def request(client, method, path, **kwargs):
            calls.append((method, path))
            if path == "/api/v1/subscribe/":
                if isinstance(response, Exception):
                    raise response
                return response
            if path.startswith("/api/v1/media/"):
                return (200, detail() if media is None else media)
            self.fail("qualification made an unexpected service request")

        with patch.dict(os.environ, {"QUALIFICATION_TEST_SECRET": "test-secret"}), \
             patch("pipeline.resource_commands._current_quarter", return_value="2026-10"), \
             patch("pipeline.real_adapters.MoviePilotClient._call", new=request):
            result = qualify_current_subscriptions(config())
        self.assertNotIn("private-name", repr(result))
        self.assertNotIn("test-secret", repr(result))
        self.assertNotIn("invalid.local", repr(result))
        return result, calls

    def test_actual_adapter_enriches_fixed_sources_and_corresponding_season(self):
        for username in ("admin", "Bangumi订阅"):
            with self.subTest(username=username):
                result, calls = self.call((200, [row(username)]))
                self.assertEqual(result["status"], "ok")
                self.assertEqual(result["counts"]["eligible"], 1)
                self.assertEqual(result["eligible"][0]["season_air_date"], "2026-10-03")
                self.assertEqual(len(calls), 2)

    def test_known_zero_reads_only_subscription_endpoint(self):
        for rows in ([], [row("other")]):
            result, calls = self.call((200, rows))
            self.assertEqual(result["status"], "ok")
            self.assertEqual(result["counts"]["eligible"], 0)
            self.assertEqual(calls, [("GET", "/api/v1/subscribe/")])

    def test_failed_or_incomplete_snapshots_never_report_zero(self):
        cases = [
            (401, []), (200, {"success": False, "data": []}),
            (200, {"code": 401, "data": []}),
            (200, {"data": [], "total": 1}),
            (200, {"data": [], "page": 1}),
            (200, {"data": [], "total": 0, "page": 2}),
            (200, {"data": [], "total": True}),
            (200, {"data": [], "total": "0"}),
            (200, {"data": [row()], "total": 1, "page_size": 0}),
            RuntimeError("private failure"),
        ]
        for index, response in enumerate(cases):
            with self.subTest(case=index):
                result, calls = self.call(response)
                self.assertEqual(result["status"], "blocked")
                self.assertIsNone(result["counts"]["eligible"])
                self.assertEqual(len(calls), 1)

    def test_complete_envelope_is_accepted(self):
        result, _ = self.call((200, {"data": [], "total": 0, "page": 1, "page_size": 10}))
        self.assertEqual(result["status"], "ok")
        self.assertEqual(result["counts"]["eligible"], 0)

    def test_detail_failure_missing_genre_or_season_is_blocked(self):
        cases = [
            {"success": "false", "data": detail()["data"]},
            {"code": 401, "data": detail()["data"]},
            {"success": True, "data": {"season_info": []}},
            {"success": True, "data": {"genre_ids": [16]}},
        ]
        for media in cases:
            result, _ = self.call((200, [row()]), media=media)
            self.assertEqual(result["status"], "blocked")

    def test_missing_secret_blocks_before_client_construction(self):
        with patch.dict(os.environ, {}, clear=True), \
             patch("pipeline.real_adapters.MoviePilotClient.__init__") as constructor:
            result = qualify_current_subscriptions(config())
        self.assertEqual(result["reason"], "moviepilot_secret_missing")
        constructor.assert_not_called()


if __name__ == "__main__":
    unittest.main(verbosity=2)
