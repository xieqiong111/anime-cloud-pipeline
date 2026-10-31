"""Focused Task 9 tests for Emby inventory and identity matching."""
from __future__ import annotations

import unittest
from urllib.parse import parse_qs, urlsplit

from pipeline.http import JsonHttpClient, RetryPolicy
from pipeline.models import EpisodeKey
from pipeline.paths import PathMapper, ServiceRoots
from pipeline.real_adapters import EmbyClient
from pipeline.adapters import EmbyInventoryError, MockEmby


PATHS = ServiceRoots(
    cloud_source_root="/cloud-source",
    cloud_library_root="/cloud",
    litepan_output_root="/lite",
    moviepilot_mount_root="/moviepilot",
    emby_mount_root="/emby",
)
MAPPER = PathMapper(PATHS)
LITE_PATH = "/lite/Show/Season 1/Show - S01E08.strm"
EXPECTED_PATH = MAPPER.litepan_to_emby(LITE_PATH)
OLD_PATH = "/emby/old/Show - S01E08.strm"


def item(*, tmdb="111", series="Show", path=EXPECTED_PATH, item_id="ep-1"):
    return {
        "Id": item_id,
        "Path": path,
        "ProviderIds": {"Tmdb": tmdb} if tmdb is not None else {},
        "SeriesId": "series-1",
        "SeriesName": series,
        "ParentIndexNumber": 1,
        "IndexNumber": 8,
    }


class PageTransport:
    def __init__(self, pages):
        self.pages = pages
        self.calls = []

    def request(self, method, url, *, body=None, headers=None, timeout=None):
        self.calls.append((method, url))
        query = parse_qs(urlsplit(url).query)
        start = int(query["StartIndex"][0])
        return 200, self.pages[start]


class EmbyAdapterTests(unittest.TestCase):
    def inventory_client(self, episodes, series):
        class InventoryTransport:
            def request(self, method, url, **kwargs):
                kind = parse_qs(urlsplit(url).query)["IncludeItemTypes"][0]
                return 200, episodes if kind == "Episode" else series
        return EmbyClient("https://emby.example.invalid", api_key="test-key",
                          transport=InventoryTransport(), library_id="lib-1")

    def test_complete_empty_library_is_proven_absent(self):
        empty = {"Items": [], "TotalRecordCount": 0}
        client = self.inventory_client(empty, empty)
        self.assertIsNone(client.find_episode(
            {"media_id": "111", "season": 1, "episode": 8}, EXPECTED_PATH))

    def test_episode_without_series_identity_is_unknown(self):
        episodes = {"Items": [item()], "TotalRecordCount": 1}
        for series in (
            {"Items": [], "TotalRecordCount": 0},
            {"Items": [{"Id": "series-1", "ProviderIds": {}}], "TotalRecordCount": 1},
            {"Items": [{"Id": "other-series", "ProviderIds": {"Tmdb": "111"}}],
             "TotalRecordCount": 1},
        ):
            with self.subTest(series=series), self.assertRaises(EmbyInventoryError):
                self.inventory_client(episodes, series).find_episode(
                    {"media_id": "111", "season": 1, "episode": 8}, EXPECTED_PATH)

    def test_empty_episode_inventory_does_not_hide_invalid_series_inventory(self):
        client = self.inventory_client(
            {"Items": [], "TotalRecordCount": 0},
            {"Items": [{"Id": "series-1", "ProviderIds": {}}], "TotalRecordCount": 1})
        with self.assertRaises(EmbyInventoryError):
            client.find_episode({"media_id": "111", "season": 1, "episode": 8}, EXPECTED_PATH)

    def test_series_identity_and_canonical_path_are_still_required(self):
        episodes = {"Items": [item(tmdb="episode-id")], "TotalRecordCount": 1}
        identity = {"media_id": "111", "canonical_work": "Show", "season": 1, "episode": 8}
        for work_id, path, expected in (
            ("111", EXPECTED_PATH, "ep-1"),
            ("222", EXPECTED_PATH, None),
            ("111", OLD_PATH, None),
        ):
            with self.subTest(work_id=work_id, path=path):
                client = self.inventory_client(episodes, {
                    "Items": [{"Id": "series-1", "ProviderIds": {"Tmdb": work_id}}],
                    "TotalRecordCount": 1})
                result = client.find_episode(identity, path)
                self.assertEqual(result["Id"] if result else None, expected)

    def client(self, pages, **kwargs):
        transport = PageTransport(pages)
        http = JsonHttpClient(
            RetryPolicy(max_attempts=1), transport=transport, timeout=1
        )
        client = EmbyClient(
            "https://emby.example.invalid",
            api_key="secret-key",
            http_client=http,
            page_size=500,
            **kwargs,
        )
        client._test_transport = transport
        return client

    def test_episode_on_second_page_is_found(self):
        pages = {
            0: {
                "Items": [item(item_id=f"ep-{index}", tmdb="999") for index in range(500)],
                "TotalRecordCount": 501,
            },
            500: {"Items": [item(item_id="ep-501")], "TotalRecordCount": 501},
        }
        client = self.client(pages)

        match = client.find_episode(
            {
                "media_id": "111",
                "canonical_work": "Show",
                "aliases": ["Alias"],
                "season": 1,
                "episode": 8,
            },
            EXPECTED_PATH,
            mapper=MAPPER,
        )

        self.assertEqual(match["Id"], "ep-501")
        starts = [int(parse_qs(urlsplit(url).query)["StartIndex"][0])
                  for _method, url in client._test_transport.calls]
        self.assertEqual(starts, [0, 500])

    def test_missing_or_truncated_total_fails_closed(self):
        for payload in (
            {"Items": [item()], "TotalRecordCount": None},
            {"Items": [item()], "TotalRecordCount": 2},
        ):
            with self.subTest(payload=payload):
                client = self.client({0: payload})
                with self.assertRaises(RuntimeError):
                    list(client.iter_episodes("lib-1"))

    def test_current_path_wins_and_old_path_is_rejected(self):
        emby = MockEmby([
            item(item_id="old", path=OLD_PATH),
            item(item_id="current", path=EXPECTED_PATH),
        ])
        identity = EpisodeKey("tmdb", "111", 1, 8)

        match = emby.find_episode(
            {"key": identity, "canonical_work": "Show"}, EXPECTED_PATH,
            mapper=MAPPER,
        )

        self.assertEqual(match["Id"], "current")

    def test_tmdb_provider_match_precedes_alias_and_conflict_is_rejected(self):
        emby = MockEmby([
            item(item_id="wrong-provider", tmdb="222", series="Alias"),
            item(item_id="tmdb-match", tmdb="111", series="Different title"),
        ])
        identity = {
            "media_id": "111", "canonical_work": "Show", "aliases": ["Alias"],
            "season": 1, "episode": 8,
        }

        match = emby.find_episode(identity, EXPECTED_PATH, mapper=MAPPER)

        self.assertEqual(match["Id"], "tmdb-match")

    def test_all_tmdb_provider_aliases_must_normalize_to_one_value(self):
        agreeing = item(item_id="agreeing")
        agreeing["ProviderIds"] = {
            "Tmdb": " 111 ", "TMDB": "111", "tmdb_id": "111",
        }
        self.assertEqual(
            MockEmby([agreeing]).find_episode(
                {"media_id": "111", "canonical_work": "Show", "season": 1, "episode": 8},
                EXPECTED_PATH,
                mapper=MAPPER,
            )["Id"],
            "agreeing",
        )

        for provider_ids in (
            {"Tmdb": "111", "TMDB": "222"},
            {"Tmdb": "111", "TMDB": ""},
            {"Tmdb": ""},
        ):
            conflicting = item(item_id="conflicting")
            conflicting["ProviderIds"] = provider_ids
            with self.subTest(provider_ids=provider_ids):
                with self.assertRaises(EmbyInventoryError):
                    MockEmby([conflicting]).find_episode(
                        {"media_id": "111", "canonical_work": "Show",
                         "season": 1, "episode": 8},
                        EXPECTED_PATH,
                        mapper=MAPPER,
                    )

    def test_real_inventory_requires_series_and_provider_contract_fields(self):
        malformed = item(item_id="malformed")
        malformed.pop("SeriesName")
        client = self.client({0: {"Items": [malformed], "TotalRecordCount": 1}})
        with self.assertRaises(EmbyInventoryError):
            list(client.iter_episodes("lib-1"))

    def test_real_inventory_rejects_non_json_contract_types(self):
        malformed_cases = [
            ("Id", 123), ("Id", True), ("Id", ["ep-1"]), ("Id", {"id": "ep-1"}),
            ("SeriesId", 123), ("SeriesId", False), ("SeriesId", ["series-1"]),
            ("SeriesId", {"id": "series-1"}),
            ("SeriesName", 123), ("SeriesName", ["Show"]),
            ("SeriesName", {"name": "Show"}),
            ("Path", 123), ("Path", [EXPECTED_PATH]),
            ("Path", {"path": EXPECTED_PATH}),
            ("ParentIndexNumber", "1"), ("ParentIndexNumber", 1.0),
            ("ParentIndexNumber", 1.25),
            ("ParentIndexNumber", True), ("IndexNumber", "8"),
            ("IndexNumber", 8.0), ("IndexNumber", 8.5), ("IndexNumber", False),
            ("ProviderIds", {"Tmdb": 111}),
            ("ProviderIds", {"Tmdb": True}),
            ("ProviderIds", {"Tmdb": ["111"]}),
            ("ProviderIds", {"Tmdb": {"id": "111"}}),
            ("ProviderIds", {"Tmdb": ""}),
            ("ProviderIds", {"Tmdb": "  "}),
            ("ProviderIds", {"Imdb": 123}),
            ("ProviderIds", {"Imdb": ["tt1"]}),
            ("ProviderIds", {"Imdb": {"id": "tt1"}}),
            ("ProviderIds", {"Imdb": "  "}),
        ]
        for field, value in malformed_cases:
            with self.subTest(field=field, value=value):
                malformed = item(item_id="malformed")
                malformed[field] = value
                client = self.client({0: {
                    "Items": [malformed], "TotalRecordCount": 1,
                }})
                with self.assertRaises(EmbyInventoryError):
                    list(client.iter_episodes("lib-1"))

    def test_malformed_real_item_never_becomes_absence(self):
        malformed = item(item_id=["ep-1"])
        client = self.client({0: {"Items": [malformed], "TotalRecordCount": 1}})
        with self.assertRaises(EmbyInventoryError):
            client.find_episode(
                {"media_id": "111", "canonical_work": "Show",
                 "season": 1, "episode": 8},
                EXPECTED_PATH,
                mapper=MAPPER,
            )

    def test_library_can_have_multiple_configured_locations(self):
        client = EmbyClient("https://emby.example.invalid", api_key="secret-key")
        client._request = lambda *_args, **_kwargs: (200, {
            "Items": [{
                "Id": "lib-1", "Name": "Anime",
                "Locations": ["/emby", "/other"],
            }],
        })
        self.assertEqual(client.find_library("Anime", "/emby")["Id"], "lib-1")

    def test_ambiguous_equal_rank_items_fail_closed(self):
        one = item(item_id="one", tmdb=None)
        two = item(item_id="two", tmdb=None)
        one["ProviderIds"] = {"Imdb": "tt0000001"}
        two["ProviderIds"] = {"Imdb": "tt0000002"}
        emby = MockEmby([one, two])
        with self.assertRaises(RuntimeError):
            emby.find_episode(
                {"media_id": "111", "canonical_work": "Show", "season": 1, "episode": 8},
                EXPECTED_PATH,
                mapper=MAPPER,
            )


if __name__ == "__main__":
    unittest.main()
