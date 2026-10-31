from __future__ import annotations

import unittest

from pipeline.config import PathConfig
from pipeline.paths import (
    PathMapper,
    PathMappingError,
    SeasonRoutingError,
    season_label_from_air_date,
)


PATHS = PathConfig(
    cloud_source_root="/影视库/下载",
    cloud_library_root="/影视库/动画",
    litepan_output_root="/lite/out",
    moviepilot_mount_root="/media/mp",
    emby_mount_root="/media/emby",
)

# Published STRM must be able to reproduce the legacy v3 layout
# ``<published-root>/<quarter>/整理/<work>/Season N/<file>.strm`` while the raw
# LitePan output root stays untouched.
PATHS_PUBLISH = PathConfig(
    cloud_source_root="/影视库/下载",
    cloud_library_root="/影视库/动画",
    litepan_output_root="/lite/out",
    moviepilot_mount_root="/media/mp",
    emby_mount_root="/media/emby",
    litepan_raw_output_root="/lite/raw",
    litepan_publish_subdir="整理",
)

# V5 deploys against the real v3 cloud tree, where organized media already
# lives under ``<quarter>/整理/<work>/...``.  The fix keeps a single ``整理``
# layer in the *cloud* path (via cloud_library_subdir) and projects both the
# published and raw STRM trees one-to-one from it (publish_subdir empty).
PATHS_LIBRARY_SUBDIR = PathConfig(
    cloud_source_root="/影视库/动画/未整理",
    cloud_library_root="/影视库/动画",
    litepan_output_root="/lite/out",
    moviepilot_mount_root="/media/mp",
    emby_mount_root="/media/emby",
    litepan_raw_output_root="/lite/raw",
    cloud_library_subdir="整理",
)


class PathMapperTests(unittest.TestCase):
    def setUp(self) -> None:
        self.mapper = PathMapper(PATHS)

    def test_publish_subdir_is_absent_by_default(self):
        self.assertEqual(getattr(self.mapper, "publish_subdir", ""), "")

    def test_publish_subdir_reproduces_v3_layout_without_touching_raw(self):
        mapper = PathMapper(PATHS_PUBLISH)
        cloud = "/影视库/动画/2026-10月新番/Show/Season 1/Show - S01E08.mkv"

        lite = mapper.cloud_to_litepan(cloud)

        self.assertEqual(
            lite,
            "/lite/out/2026-10月新番/整理/Show/Season 1/Show - S01E08.strm",
        )
        self.assertEqual(
            mapper.litepan_to_moviepilot(lite),
            "/media/mp/2026-10月新番/整理/Show/Season 1/Show - S01E08.strm",
        )
        self.assertEqual(
            mapper.litepan_to_emby(lite),
            "/media/emby/2026-10月新番/整理/Show/Season 1/Show - S01E08.strm",
        )
        # The dedicated raw STRM root must never inherit the publish subdir.
        self.assertEqual(
            mapper.cloud_to_raw_litepan(cloud, "/lite/raw"),
            "/lite/raw/2026-10月新番/Show/Season 1/Show - S01E08.strm",
        )

    def test_publish_subdir_rejects_path_separators(self):
        with self.assertRaises(PathMappingError):
            PathMapper({
                "cloud_source_root": "/cloud/source",
                "cloud_library_root": "/cloud/library",
                "litepan_output_root": "/lite/out",
                "moviepilot_mount_root": "/media/mp",
                "emby_mount_root": "/media/emby",
                "litepan_publish_subdir": "a/b",
            })

    def test_maps_distinct_litepan_moviepilot_and_emby_roots(self):
        cloud = "/影视库/动画/2026-10月新番/Show/Season 1/Show - S01E08.mkv"

        lite = self.mapper.cloud_to_litepan(cloud)

        self.assertEqual(
            lite,
            "/lite/out/2026-10月新番/Show/Season 1/Show - S01E08.strm",
        )
        self.assertEqual(
            self.mapper.litepan_to_moviepilot(lite),
            "/media/mp/2026-10月新番/Show/Season 1/Show - S01E08.strm",
        )
        self.assertEqual(
            self.mapper.litepan_to_emby(lite),
            "/media/emby/2026-10月新番/Show/Season 1/Show - S01E08.strm",
        )

    def test_mapping_rejects_traversal_and_prefix_confusion(self):
        for path in (
            "/影视库/动画/../private/file.mkv",
            "/影视库/动画-old/2026-10月新番/file.mkv",
            "/影视库/动画/%2e%2e/private/file.mkv",
            "/影视库/动画/%252e%252e/private/file.mkv",
        ):
            with self.subTest(path=path):
                with self.assertRaises(PathMappingError):
                    self.mapper.cloud_to_litepan(path)

    def test_mapping_rejects_windows_separators_drives_unc_and_absolute_substitution(self):
        for path in (
            r"\影视库\动画\2026-10月新番\Show.mkv",
            "/影视库/动画/C:/private/file.mkv",
            "C:/影视库/动画/file.mkv",
            "//server/share/影视库/动画/file.mkv",
            "/影视库/动画/%2fprivate/file.mkv",
        ):
            with self.subTest(path=path):
                with self.assertRaises(PathMappingError):
                    self.mapper.cloud_to_litepan(path)

    def test_malformed_roots_are_rejected_before_mapping(self):
        malformed = {
            "cloud_source_root": "/cloud/source",
            "cloud_library_root": "/cloud/library",
            "litepan_output_root": "/lite/out",
            "moviepilot_mount_root": "/media/mp",
            "emby_mount_root": "C:/media/emby",
        }
        with self.assertRaises(PathMappingError):
            PathMapper(malformed)

    def test_tmdb_season_air_date_drives_quarter_label(self):
        self.assertEqual(season_label_from_air_date("2026-10-03"), "2026-10月新番")
        with self.assertRaises(SeasonRoutingError):
            season_label_from_air_date("")
        for value in (
            "2026-1-03",
            "2026-10-3",
            "2026-02-30",
            "2026-10-03T00:00:00",
            "２０２６-１０-０３",
            "٢٠٢٦-١٠-٠٣",
        ):
            with self.subTest(value=value), self.assertRaises(SeasonRoutingError):
                season_label_from_air_date(value)

    def test_canonical_routing_uses_explicit_tmdb_season_air_date_only(self):
        with self.assertRaises(SeasonRoutingError):
            self.mapper.canonical_for_episode({
                "canonical_work": "Show",
                "season": 1,
                "episode": 8,
                "air_date": "2026-10-03",
                "extension": ".mkv",
            })

        path = self.mapper.canonical_for_episode({
            "canonical_work": "Show",
            "season": 1,
            "episode": 8,
            "air_date": "2026-01-03",
            "season_air_date": "2026-10-03",
            "extension": ".mkv",
        })
        self.assertIn("/2026-10月新番/", path)

    def test_legacy_persisted_generic_air_date_cannot_route_a_new_target(self):
        with self.assertRaises(SeasonRoutingError):
            self.mapper.canonical_for_episode({
                "canonical_work": "Show",
                "season": 1,
                "episode": 8,
                "extension": ".mkv",
                "runtime_identity_json": {
                    "_air_date": "2026-10-03",
                },
            })

    def test_drive_like_component_is_rejected_even_when_not_a_full_drive_path(self):
        with self.assertRaises(PathMappingError):
            self.mapper.cloud_to_litepan(
                "/影视库/动画/2026-10月新番/C:bar/Season 1/C:bar - S01E01.mkv"
            )
        with self.assertRaises(PathMappingError):
            self.mapper.build_canonical_cloud_path(
                work="C:bar",
                season=1,
                episode=1,
                extension=".mkv",
                air_date="2026-10-03",
            )

    def test_uppercase_strm_is_normalized_to_lowercase(self):
        cloud = "/影视库/动画/2026-10月新番/Show/Season 1/Show - S01E08.STRM"
        lite = self.mapper.cloud_to_litepan(cloud)
        self.assertEqual(
            lite,
            "/lite/out/2026-10月新番/Show/Season 1/Show - S01E08.strm",
        )
        uppercase_lite = lite[:-5] + ".STRM"
        self.assertEqual(
            self.mapper.litepan_to_moviepilot(uppercase_lite),
            "/media/mp/2026-10月新番/Show/Season 1/Show - S01E08.strm",
        )
        self.assertEqual(
            self.mapper.litepan_to_emby(uppercase_lite),
            "/media/emby/2026-10月新番/Show/Season 1/Show - S01E08.strm",
        )

    def test_canonical_builder_preserves_media_suffix_until_cloud_boundary(self):
        cloud = self.mapper.build_canonical_cloud_path(
            work="Show",
            season=1,
            episode=8,
            extension=".mkv",
            air_date="2026-10-03",
        )

        self.assertEqual(
            cloud,
            "/影视库/动画/2026-10月新番/Show/Season 1/Show - S01E08.mkv",
        )
        self.assertEqual(
            self.mapper.cloud_to_litepan(cloud),
            "/lite/out/2026-10月新番/Show/Season 1/Show - S01E08.strm",
        )

    def test_canonical_builder_rejects_unsafe_work_component(self):
        with self.assertRaises(PathMappingError):
            self.mapper.build_canonical_cloud_path(
                work="../private",
                season=1,
                episode=8,
                extension=".mkv",
                air_date="2026-10-03",
            )


    def test_library_subdir_reproduces_v3_layout_for_legacy_rows(self):
        # Invariant (1): a migrated v3 row whose cloud_path already contains
        # ``整理`` must map to the *existing* v3 published path with no doubled
        # segment, and its raw LitePan path must equal the real scan output.
        mapper = PathMapper(PATHS_LIBRARY_SUBDIR)
        cloud = "/影视库/动画/2026-07月新番/整理/示例动画/Season 1/示例动画 - S01E10.mkv"

        self.assertEqual(
            mapper.cloud_to_litepan(cloud),
            "/lite/out/2026-07月新番/整理/示例动画/Season 1/示例动画 - S01E10.strm",
        )
        self.assertEqual(
            mapper.cloud_to_raw_litepan(cloud, "/lite/raw"),
            "/lite/raw/2026-07月新番/整理/示例动画/Season 1/示例动画 - S01E10.strm",
        )

    def test_new_download_cloud_target_places_library_subdir_after_quarter(self):
        # Invariant (2): generated cloud targets keep the v3 cloud layout and
        # the published/raw STRM trees stay isomorphic to them.
        mapper = PathMapper(PATHS_LIBRARY_SUBDIR)
        cloud = mapper.build_canonical_cloud_path(
            work="Show",
            season=1,
            episode=8,
            extension=".mkv",
            air_date="2026-07-05",
        )

        self.assertEqual(
            cloud,
            "/影视库/动画/2026-07月新番/整理/Show/Season 1/Show - S01E08.mkv",
        )
        self.assertEqual(
            mapper.cloud_to_litepan(cloud),
            "/lite/out/2026-07月新番/整理/Show/Season 1/Show - S01E08.strm",
        )
        self.assertEqual(
            mapper.cloud_to_raw_litepan(cloud, "/lite/raw"),
            "/lite/raw/2026-07月新番/整理/Show/Season 1/Show - S01E08.strm",
        )

        isolated = mapper.build_isolated_cloud_path(
            work="Show",
            season=1,
            remote_file_id=12345,
            original_basename="Show - S01E08.mkv",
            air_date="2026-07-05",
        )
        self.assertTrue(
            isolated.startswith("/影视库/动画/2026-07月新番/整理/Show/Season 1/fid-"),
            isolated,
        )

    def test_library_subdir_defaults_empty_and_rejects_separators(self):
        self.assertEqual(getattr(PathMapper(PATHS), "cloud_library_subdir", ""), "")
        with self.assertRaises(PathMappingError):
            PathMapper({
                "cloud_source_root": "/cloud/source",
                "cloud_library_root": "/cloud/library",
                "litepan_output_root": "/lite/out",
                "moviepilot_mount_root": "/media/mp",
                "emby_mount_root": "/media/emby",
                "cloud_library_subdir": "a/b",
            })


if __name__ == "__main__":
    unittest.main(verbosity=2)
