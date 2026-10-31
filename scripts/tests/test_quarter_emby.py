from __future__ import annotations

import copy
import json
import unittest
from unittest.mock import patch
from urllib.parse import parse_qs, urlsplit

from pipeline.quarter_emby import QuarterEmbyClient, QuarterEmbyError


SPEC = {"name": "2026年10月新番", "path": "/published/2026-10"}
AVAILABLE = {
    "TypeOptions": [{"Type": t, "MetadataFetchers": [{"Name": "TMDB"}],
                     "ImageFetchers": [{"Name": "TMDB"}]}
                    for t in ("Series", "Season", "Episode")],
    "MetadataReaders": [{"Name": "Nfo"}], "SubtitleFetchers": [{"Name": "Sub"}],
    "LyricsFetchers": [],
}


def quiet_options():
    return {
        "ContentType": "tvshows", "PathInfos": [{"Path": SPEC["path"]}],
        "EnableRealtimeMonitor": False, "EnableMarkerDetection": False,
        "EnableMarkerDetectionDuringLibraryScan": False,
        "EnableChapterImageExtraction": False, "ExtractChapterImagesDuringLibraryScan": False,
        "DownloadImagesInAdvance": False, "SaveLocalMetadata": False,
        "SaveLocalThumbnailSets": False, "AutoGenerateChapters": False,
        "ImportCollections": False, "ImportPlaylists": False,
        "AutomaticRefreshIntervalDays": 0, "PlaceholderMetadataRefreshIntervalDays": 0,
        "MetadataSavers": [], "SubtitleDownloadLanguages": [], "LyricsDownloadLanguages": [],
        "TypeOptions": [{"Type": t, "MetadataFetchers": [], "ImageFetchers": []}
                        for t in ("Series", "Season", "Episode")],
        "DisabledLocalMetadataReaders": ["Nfo"], "DisabledSubtitleFetchers": ["Sub"],
        "DisabledLyricsFetchers": [],
    }


def library(**values):
    return dict({"ItemId": "1", "Name": SPEC["name"], "Locations": [SPEC["path"]],
                 "CollectionType": "tvshows"}, **values)


class Transport:
    def __init__(self, rows=None):
        self.rows = rows if rows is not None else []
        self.calls = []
        self.version = "4.9.5.0"
        self.available = copy.deepcopy(AVAILABLE)
        self.post_error = None
        self.readback_mutation = None

    def request(self, method, url, body=None, **_kw):
        self.calls.append((method, url, body))
        endpoint = urlsplit(url).path
        if method == "POST":
            if self.post_error:
                raise self.post_error
            row = library(LibraryOptions=copy.deepcopy(body["LibraryOptions"]))
            if self.readback_mutation:
                self.readback_mutation(row)
            self.rows = [row]
            return 204, None
        return 200, {
            "/emby/Library/VirtualFolders": self.rows,
            "/emby/System/Info/Public": {"Version": self.version},
            "/emby/Users/Query": {"Items": []},
            "/emby/Libraries/AvailableOptions": self.available,
        }[endpoint]


class QuarterEmbyTests(unittest.TestCase):
    def test_local_nfo_reader_can_remain_enabled_after_authorized_ingestion(self):
        options = quiet_options()
        options['DisabledLocalMetadataReaders'] = []
        transport = Transport([library(LibraryOptions=options)])
        self.assertEqual(self.client(transport).inventory(SPEC)['state'], 'reuse')
        options['TypeOptions'][0]['MetadataFetchers'] = ['TMDB']
        self.assertEqual(self.client(transport).inventory(SPEC)['state'], 'conflict')

    def test_nfo_allowance_does_not_enable_other_local_readers(self):
        options = quiet_options()
        options['DisabledLocalMetadataReaders'] = []
        transport = Transport([library(LibraryOptions=options)])
        transport.available['MetadataReaders'].append({'Name': 'Other'})
        self.assertEqual(self.client(transport).inventory(SPEC)['state'], 'conflict')

    def test_json_options_reach_actual_urllib_request(self):
        class Response:
            status = 204
            headers = {}

            def __enter__(self):
                return self

            def __exit__(self, *_):
                return False

            def read(self):
                return b""

        client = QuarterEmbyClient("http://emby", "secret-test-key")
        payload = {"LibraryOptions": quiet_options()}
        with patch("pipeline.http.urllib.request.urlopen", return_value=Response()) as opened:
            client._request("POST", "/Library/VirtualFolders", body=payload)
        request = opened.call_args.args[0]
        self.assertEqual(request.get_method(), "POST")
        self.assertEqual(request.get_header("Content-type"), "application/json")
        self.assertEqual(json.loads(request.data), payload)

    def client(self, transport):
        return QuarterEmbyClient("http://emby", "secret-test-key", transport)

    def test_inventory_exact_tv_mapping_reused(self):
        self.assertEqual(self.client(Transport([library(LibraryOptions=quiet_options())])).inventory(SPEC)["state"], "reuse")

    def test_identity_without_explicit_quiet_options_is_unknown(self):
        for options in (None, {}, {"EnableRealtimeMonitor": False}):
            with self.subTest(options=options):
                row = library(LibraryOptions=options)
                self.assertEqual(self.client(Transport([row])).inventory(SPEC)["state"], "unknown")

    def test_explicit_unsafe_scan_refresh_and_provider_options_conflict(self):
        mutations = (
            lambda o: o.update(EnableRealtimeMonitor=True),
            lambda o: o.update(AutomaticRefreshIntervalDays=30),
            lambda o: o.update(EnableMarkerDetectionDuringLibraryScan=True),
            lambda o: o["TypeOptions"][0].update(MetadataFetchers=["TMDB"]),
            lambda o: o["TypeOptions"][1].update(ImageFetchers=["TMDB"]),
            lambda o: o.update(DisabledLocalMetadataReaders=["Unexpected"]),
        )
        for mutate in mutations:
            options = quiet_options()
            mutate(options)
            self.assertEqual(self.client(Transport([library(LibraryOptions=options)])).inventory(SPEC)["state"], "conflict")

    def test_missing_nested_provider_array_is_unknown(self):
        options = quiet_options()
        options["TypeOptions"][0].pop("MetadataFetchers")
        self.assertEqual(self.client(Transport([library(LibraryOptions=options)])).inventory(SPEC)["state"], "unknown")

    def test_extra_option_keys_and_type_order_do_not_prevent_quiet_reuse(self):
        options = quiet_options()
        options["IgnoreHiddenFiles"] = True
        options["TypeOptions"].reverse()
        options["TypeOptions"][0]["ImageOptions"] = []
        self.assertEqual(self.client(Transport([library(LibraryOptions=options)])).inventory(SPEC)["state"], "reuse")

    def test_inventory_absent_is_create(self):
        self.assertEqual(self.client(Transport()).inventory(SPEC)["state"], "create")

    def test_name_path_type_overlap_and_duplicates_conflict(self):
        for rows in ([library(Locations=["/other"])], [library(Name="other")],
                     [library(CollectionType="movies")],
                     [library(Name="other", Locations=["/published"])],
                     [library(Name="other", Locations=["/"])],
                     [library(), library(ItemId="2")]):
            with self.subTest(rows=rows):
                self.assertEqual(self.client(Transport(rows)).inventory(SPEC)["state"], "conflict")

    def test_malformed_inventory_unknown_never_create(self):
        for rows in ({"Items": []}, [{}], [library(Locations=None)],
                     [library(), library()]):
            self.assertEqual(self.client(Transport(rows)).inventory(SPEC)["state"], "unknown")

    def test_invalid_spec_has_no_requests(self):
        t = Transport()
        self.assertEqual(self.client(t).inventory(dict(SPEC, path="/a/../b"))["state"], "unknown")
        self.assertEqual(t.calls, [])

    def test_empty_directory_verification_does_not_guess_unsupported_get(self):
        t = Transport()
        result = self.client(t).verify_empty_directory(SPEC)
        self.assertEqual(result, {"verified": False,
                                  "reason": "emby_directory_get_unverified"})
        self.assertEqual(t.calls, [])

    def test_empty_directory_invalid_spec_is_not_verified(self):
        t = Transport()
        result = self.client(t).verify_empty_directory(dict(SPEC, path="relative"))
        self.assertEqual(result, {"verified": False, "reason": "emby_spec_invalid"})
        self.assertEqual(t.calls, [])

    def test_creation_matches_installed_client_and_readback(self):
        t = Transport()
        self.assertEqual(self.client(t).create_library(SPEC)["reason"], "emby_creation_verified")
        posts = [c for c in t.calls if c[0] == "POST"]
        self.assertEqual(len(posts), 1)
        query = parse_qs(urlsplit(posts[0][1]).query)
        self.assertEqual(query, {"name": [SPEC["name"]], "collectionType": ["tvshows"],
                                 "refreshLibrary": ["false"]})
        opts = posts[0][2]["LibraryOptions"]
        self.assertIsInstance(opts, dict)
        self.assertIs(opts["EnableRealtimeMonitor"], False)
        self.assertEqual(opts["AutomaticRefreshIntervalDays"], 0)
        self.assertEqual(opts["DisabledLocalMetadataReaders"], ["Nfo"])
        for row in opts["TypeOptions"]:
            self.assertEqual(row["MetadataFetchers"], [])
            self.assertEqual(row["ImageFetchers"], [])
        self.assertTrue(all("/Refresh" not in c[1] for c in t.calls))

    def test_unknown_version_or_missing_type_options_blocks_before_post(self):
        for version, available in (("4.10", AVAILABLE), ("4.9.5.0", {})):
            t = Transport()
            t.version, t.available = version, available
            with self.assertRaises(QuarterEmbyError):
                self.client(t).create_library(SPEC)
            self.assertTrue(all(c[0] == "GET" for c in t.calls))

    def test_omitted_provider_readback_cannot_mean_disabled(self):
        t = Transport()
        t.readback_mutation = lambda row: row["LibraryOptions"].pop("TypeOptions")
        with self.assertRaisesRegex(QuarterEmbyError, "emby_creation_options_unverified"):
            self.client(t).create_library(SPEC)
        self.assertEqual(self.client(t).inventory(SPEC)["state"], "unknown")

    def test_failed_unsafe_creation_cannot_recover_as_prepared(self):
        t = Transport()
        t.readback_mutation = lambda row: row["LibraryOptions"].update(EnableRealtimeMonitor=True)
        with self.assertRaisesRegex(QuarterEmbyError, "emby_creation_options_unverified"):
            self.client(t).create_library(SPEC)
        self.assertEqual(self.client(t).inventory(SPEC)["state"], "conflict")

    def test_timeout_has_one_post_and_fixed_secret_safe_error(self):
        t = Transport()
        t.post_error = TimeoutError("secret-test-key private response")
        with self.assertRaisesRegex(QuarterEmbyError, "^emby_request_failed$"):
            self.client(t).create_library(SPEC)
        self.assertEqual(sum(c[0] == "POST" for c in t.calls), 1)

    def test_mapping_readback_conflict_cannot_be_success(self):
        t = Transport()
        t.readback_mutation = lambda row: row.update(Locations=["/other"])
        with self.assertRaisesRegex(QuarterEmbyError, "emby_creation_readback_failed"):
            self.client(t).create_library(SPEC)

    def test_reuse_does_not_write_or_change_existing_options(self):
        t = Transport([library(LibraryOptions=quiet_options())])
        self.client(t).create_library(SPEC)
        self.assertEqual([c[0] for c in t.calls], ["GET", "GET"])


if __name__ == "__main__":
    unittest.main()
