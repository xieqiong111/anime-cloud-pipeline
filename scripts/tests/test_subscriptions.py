import unittest
from unittest.mock import patch

from pipeline.adapters import FakeMoviePilot, MockMoviePilot
from pipeline.real_adapters import MoviePilotClient, MoviePilotResponseError
from pipeline.subscriptions import SubscriptionError, exclusion_reason, normalize_subscription


class SubscriptionNormalizationTests(unittest.TestCase):
    def test_non_tv_signal_overrides_tv_in_any_content_type_field(self):
        fields = ("type", "media_type", "subscribe_type", "category", "kind")
        for field in fields:
            for non_tv in ("movie", "电影", "ova"):
                with self.subTest(field=field, non_tv=non_tv):
                    payload = dict.fromkeys(fields, "tv")
                    payload[field] = non_tv
                    self.assertEqual(exclusion_reason(payload), "non_tv")

    def test_absent_content_type_and_namespace_do_not_conflict_with_tv(self):
        for value in (None, "", "tmdb"):
            with self.subTest(value=value):
                self.assertIsNone(exclusion_reason({"type": "tv", "media_type": value}))

    def test_moviepilot_payload_becomes_subscription(self):
        sub = normalize_subscription({
            "id": 7,
            "name": "Show A",
            "tmdbid": 111,
            "season": 2,
            "include": "简",
            "exclude": "合集",
            "names": ["Alias A"],
        })

        self.assertEqual(sub.subscription_id, "7")
        self.assertEqual(sub.media_source, "tmdb")
        self.assertEqual(sub.media_id, "111")
        self.assertEqual(sub.work, "Show A")
        self.assertEqual(sub.aliases, ("Alias A",))
        self.assertEqual(sub.include, "简")
        self.assertEqual(sub.exclude, "合集")
        self.assertEqual(sub.season, 2)

    def test_invalid_subscription_is_reported_not_silently_skipped(self):
        with self.assertRaisesRegex(SubscriptionError, r"media_id"):
            normalize_subscription({"id": 7, "name": "Show A", "season": 1})

    def test_aliases_are_immutable_and_deduplicated(self):
        sub = normalize_subscription({
            "id": "s-1",
            "title": "Show A",
            "media_source": "tmdb",
            "media_id": "111",
            "season": "01",
            "aliases": ["Alias", None, " Alias ", "", "Show A", "Other"],
        })
        self.assertEqual(sub.aliases, ("Alias", "Other"))
        with self.assertRaises(TypeError):
            sub.aliases[0] = "changed"

    def test_two_subscriptions_keep_separate_identity(self):
        first = normalize_subscription({"id": 1, "name": "A", "tmdbid": 111, "season": 1})
        second = normalize_subscription({"id": 2, "name": "B", "tmdbid": 222, "season": 2})
        self.assertNotEqual(first.subscription_id, second.subscription_id)
        self.assertNotEqual(first.media_id, second.media_id)

    def test_media_type_tv_is_content_type_and_tmdbid_selects_namespace(self):
        sub = normalize_subscription({
            "id": "tv-1", "name": "Show A", "tmdbid": 111,
            "media_type": "tv", "season": 1,
        })
        self.assertEqual(sub.media_source, "tmdb")

    def test_media_type_namespace_is_not_treated_as_non_tv(self):
        payload = {
            "id": "tmdb-1", "name": "Show A", "tmdbid": 111,
            "media_type": "tmdb", "season": 1,
        }
        sub = normalize_subscription(payload)
        self.assertEqual(sub.media_source, "tmdb")
        self.assertIsNone(exclusion_reason(payload))


class MoviePilotSubscriptionApiTests(unittest.TestCase):
    def test_list_and_supported_wrapper_shapes_are_returned(self):
        client = MoviePilotClient("http://mp", token="secret")
        payload = [{"id": 7, "name": "Show A", "tmdbid": 111, "season": 1}]
        with patch("pipeline.real_adapters._http_json", return_value=(200, payload)):
            direct = client.list_subscriptions()
        with patch("pipeline.real_adapters._http_json", return_value=(200, {"data": payload})):
            wrapped = client.list_subscriptions()
        self.assertEqual(direct, wrapped)

    def test_malformed_response_shape_raises_structured_safe_error(self):
        client = MoviePilotClient("http://mp", token="secret-token")
        with patch("pipeline.real_adapters._http_json", return_value=(200, {"unexpected": []})):
            with self.assertRaises(MoviePilotResponseError) as caught:
                client.list_subscriptions()
        self.assertIn("shape", str(caught.exception).lower())
        self.assertNotIn("secret-token", str(caught.exception))

    def test_unsuccessful_response_envelope_is_not_empty_success(self):
        client = MoviePilotClient("http://mp", token="secret-token")
        for payload in (
            {"data": [], "success": False, "message": "token=secret-token"},
            {"data": [], "success": "error", "message": "token=secret-token"},
            {"data": [], "success": "failure", "message": "token=secret-token"},
            {"data": [], "code": 500, "message": "request failed"},
            {"data": [], "code": "code-secret", "message": "request failed"},
        ):
            with self.subTest(payload=payload):
                with patch("pipeline.real_adapters._http_json", return_value=(200, payload)):
                    with self.assertRaises(MoviePilotResponseError) as caught:
                        client.list_subscriptions()
                self.assertNotIn("secret-token", str(caught.exception))
                if payload.get("code") == "code-secret":
                    self.assertNotIn("code-secret", str(caught.exception))

    def test_malformed_object_never_echoes_remote_keys_or_values(self):
        client = MoviePilotClient("http://mp", token="secret-token")
        for payload in (
            {"secret-token": "private-value"},
            {"data": [], "unconfigured-credential": "private-value"},
        ):
            with self.subTest(payload=payload):
                with patch("pipeline.real_adapters._http_json", return_value=(200, payload)):
                    with self.assertRaises(MoviePilotResponseError) as caught:
                        client.list_subscriptions()
                error = str(caught.exception)
                self.assertIn("shape", error)
                for remote_text in ("secret-token", "unconfigured-credential", "private-value"):
                    self.assertNotIn(remote_text, error)

    def test_fake_moviepilot_is_subscription_source(self):
        payload = [{"id": 1, "name": "A", "tmdbid": 11, "season": 1}]
        adapter = FakeMoviePilot(subscriptions=payload)
        self.assertEqual(adapter.list_subscriptions(), payload)
        self.assertEqual(adapter.list_subscriptions_calls, 1)

    def test_legacy_moviepilot_mock_still_supports_scrape(self):
        # The legacy acceptance suite constructs this mock without subscriptions.
        self.assertTrue(hasattr(MockMoviePilot(), "scrape_in_place"))


if __name__ == "__main__":
    unittest.main(verbosity=2)
