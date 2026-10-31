from __future__ import annotations

import json
import socket
import unittest
from datetime import datetime, timezone
from email.utils import format_datetime
from urllib.parse import parse_qsl, urlsplit
from unittest.mock import MagicMock, patch

from pipeline.http import HttpError, JsonHttpClient, RetryPolicy
from pipeline.real_adapters import EmbyClient, MoviePilotClient, _http_json


class ScriptedTransport:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, method, url, body=None, headers=None, timeout=None):
        self.calls.append({
            "method": method,
            "url": url,
            "body": body,
            "headers": headers,
            "timeout": timeout,
        })
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


POLICY = RetryPolicy(
    max_attempts=3,
    backoff_seconds=1.0,
    backoff_max_seconds=10.0,
    jitter_seconds=0.0,
)


class JsonHttpClientTests(unittest.TestCase):
    def test_scrape_json_content_type_reaches_real_urllib_request(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = 200
        response.headers = {"Content-Type": "application/json"}
        response.read.return_value = b'{"success":true}'
        with patch("pipeline.http.urllib.request.urlopen", return_value=response) as opened:
            result = MoviePilotClient("https://moviepilot.invalid", token="test-key").scrape_in_place(
                "/published/Show/Season 1/Show - S01E08.strm", "themoviedb", "111")
        request = opened.call_args.args[0]
        self.assertTrue(result.ok)
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(urlsplit(request.full_url).path, "/api/v1/media/scrape/local")
        self.assertEqual(json.loads(request.data), {
            "storage": "local", "path": "/published/Show", "name": "Show", "type": "dir"})

    def test_emby_refresh_is_bodyless_post_at_real_urllib_request(self):
        response = MagicMock()
        response.__enter__.return_value = response
        response.status = 204
        response.headers = {}
        response.read.return_value = b""
        with patch("pipeline.http.urllib.request.urlopen", return_value=response) as opened:
            result = EmbyClient("https://emby.invalid", api_key="test-key").refresh_library("lib-1")
        request = opened.call_args.args[0]
        self.assertTrue(result["accepted"])
        self.assertEqual(request.get_method(), "POST")
        self.assertIsNone(request.data)
        self.assertEqual(urlsplit(request.full_url).path, "/emby/Items/lib-1/Refresh")
        self.assertEqual(dict(parse_qsl(urlsplit(request.full_url).query))["Recursive"], "true")

    def test_transient_read_uses_exponential_backoff_with_cap(self):
        transport = ScriptedTransport([
            TimeoutError("socket timeout"),
            HttpError(500, "upstream failed"),
            (200, {"ok": True}),
        ])
        sleeps = []
        client = JsonHttpClient(
            POLICY,
            transport=transport,
            sleep=sleeps.append,
            jitter=lambda: 0,
        )

        self.assertEqual(
            client.request("GET", "http://service/items"),
            {"ok": True},
        )
        self.assertEqual(sleeps, [1.0, 2.0])
        self.assertEqual([call["method"] for call in transport.calls], ["GET"] * 3)

    def test_query_parameters_are_merged_and_terminal_errors_are_secret_safe(self):
        transport = ScriptedTransport([socket.timeout("connection failed")])
        client = JsonHttpClient(
            RetryPolicy(max_attempts=1, backoff_seconds=1, backoff_max_seconds=1),
            transport=transport,
        )

        with self.assertRaises(HttpError) as caught:
            client.request(
                "GET",
                "https://service/items?existing=keep",
                params={"q": "a value", "token": "top-secret"},
            )

        query = dict(parse_qsl(urlsplit(transport.calls[0]["url"]).query))
        self.assertEqual(query, {"existing": "keep", "q": "a value", "token": "top-secret"})
        self.assertNotIn("top-secret", str(caught.exception))

    def test_post_is_not_retried_by_generic_client(self):
        transport = ScriptedTransport([TimeoutError("submit timeout"), (200, {"ok": True})])
        sleeps = []
        client = JsonHttpClient(
            POLICY,
            transport=transport,
            sleep=sleeps.append,
            jitter=lambda: 0,
        )

        with self.assertRaises(HttpError):
            client.request("POST", "https://service/submit", body={"intent": "persisted"})
        self.assertEqual(len(transport.calls), 1)
        self.assertEqual(sleeps, [])

    def test_retryable_429_stops_at_attempt_cap(self):
        transport = ScriptedTransport([HttpError(429, "token=top-secret")] * 3)
        sleeps = []
        client = JsonHttpClient(
            POLICY,
            transport=transport,
            sleep=sleeps.append,
            jitter=lambda: 0,
        )

        with self.assertRaises(HttpError) as caught:
            client.request("GET", "https://service/items?token=top-secret")
        self.assertEqual(len(transport.calls), 3)
        self.assertEqual(sleeps, [1.0, 2.0])
        self.assertNotIn("top-secret", str(caught.exception))

    def test_retry_after_seconds_is_honored_and_clamped_to_backoff_cap(self):
        transport = ScriptedTransport([
            (429, {"token": "top-secret"}, {"Retry-After": "99"}),
            (200, {"ok": True}),
        ])
        sleeps = []
        client = JsonHttpClient(
            RetryPolicy(max_attempts=2, backoff_seconds=1, backoff_max_seconds=5),
            transport=transport,
            sleep=sleeps.append,
            jitter=lambda: 0,
        )

        self.assertEqual(client.request("GET", "https://service/items"), {"ok": True})
        self.assertEqual(sleeps, [5.0])

    def test_retry_after_http_date_uses_injected_clock(self):
        retry_at = datetime.fromtimestamp(1010, tz=timezone.utc)
        transport = ScriptedTransport([
            (429, None, {"Retry-After": format_datetime(retry_at, usegmt=True)}),
            (200, {"ok": True}),
        ])
        sleeps = []
        client = JsonHttpClient(
            RetryPolicy(max_attempts=2, backoff_seconds=1, backoff_max_seconds=20),
            transport=transport,
            sleep=sleeps.append,
            jitter=lambda: 0,
            now=lambda: 1000.0,
        )

        self.assertEqual(client.request("GET", "https://service/items"), {"ok": True})
        self.assertEqual(sleeps, [10.0])

    def test_malformed_error_url_and_structured_reason_are_safe(self):
        error = HttpError(
            500,
            {"token": "top-secret", "cookie": "session-secret"},
            url="https://user:pass@[bad-host]/?token=top-secret",
        )

        self.assertEqual(error.url, "<safe-endpoint>")
        self.assertEqual(error.reason, "structured-error")
        self.assertNotIn("top-secret", str(error))
        self.assertNotIn("session-secret", str(error))

    def test_non_2xx_adapter_contract_drops_remote_body(self):
        with patch("pipeline.real_adapters.JsonHttpClient") as client_type:
            client_type.return_value.request_with_status.return_value = (
                500,
                {"token": "top-secret", "message": "cookie=session-secret"},
            )

            status, payload = _http_json("GET", "https://service/items")

        self.assertEqual(status, 500)
        self.assertIsNone(payload)

    def test_storage_and_scrape_errors_do_not_echo_remote_body(self):
        client = MoviePilotClient("https://moviepilot", token="top-secret")
        payload = {"token": "top-secret", "cookie": "session-secret"}

        with patch("pipeline.real_adapters._http_json", return_value=(500, payload)):
            with self.assertRaises(RuntimeError) as storage_error:
                client.storage_list("local", "/unsafe")
            result = client.scrape_in_place("/tmp/a.strm", "tmdb", "1")
            with self.assertRaises(RuntimeError) as plugin_error:
                client.update_plugin_conf("plugin", payload)

        for message in (str(storage_error.exception), str(result.detail), str(plugin_error.exception)):
            self.assertNotIn("top-secret", message)
            self.assertNotIn("session-secret", message)

    def test_runtime_config_injects_retry_and_read_limits(self):
        runtime = {
            "retry": {
                "max_attempts": 7,
                "backoff_seconds": 2,
                "backoff_max_seconds": 11,
                "jitter_seconds": 0.25,
            },
            "limits": {
                "network_timeout_seconds": 17,
                "max_read_retries": 5,
            },
        }

        client = MoviePilotClient("https://moviepilot", token="top-secret", runtime_config=runtime)

        self.assertEqual(client.retry_policy.max_attempts, 7)
        self.assertEqual(client.retry_policy.max_read_retries, 5)
        self.assertEqual(client.retry_policy.backoff_seconds, 2.0)
        self.assertEqual(client.retry_policy.backoff_max_seconds, 11.0)
        self.assertEqual(client.retry_policy.jitter_seconds, 0.25)
        self.assertEqual(client.max_read_retries, 5)
        self.assertEqual(client.timeout, 17)


if __name__ == "__main__":
    unittest.main(verbosity=2)
