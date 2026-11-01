"""Offline ownership regressions for the submission media inventory."""
from __future__ import annotations

import os
from pathlib import Path
import sys
import tempfile
import unittest
from types import SimpleNamespace as NS
from unittest.mock import patch

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline.evidence import SidecarEvidence, StrmEvidence
from pipeline.media_inventory import _HostInventory, prove_submission_inventory
from pipeline.models import EpisodeKey
from pipeline.paths import PathMapper
from pipeline.state import StateStore


LABEL = "2026-10月新番"
MEDIA_ID = "12345"
SEASON = 1


class FakeCloud:
    def __init__(self) -> None:
        self.files: list[dict[str, str]] = []

    def resolve_directory_path(self, path: str) -> str:
        return "quarter-directory"

    def list_files(self, directory_id: str, *, max_depth: int, max_files: int):
        return list(self.files)


class FakeEmby:
    def find_library(self, name: str, root: str):
        return {"Id": "quarter-library"}

    def iter_episodes(self, library_id: str):
        return iter(())

    def _series_provider_map(self, library_id: str):
        return {}


class InventoryOwnershipTests(unittest.TestCase):
    def setUp(self) -> None:
        # macOS's default .../T/<name>/ resembles LitePan's /t/<token>/.
        # Keep synthetic filesystem paths separate from that redaction grammar.
        self.temporary = tempfile.TemporaryDirectory(dir='/tmp')
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name).resolve()

        self.raw_base = self.root / "litepan-raw"
        self.published_base = self.root / "litepan-published"
        self.emby_base = self.root / "emby"
        self.raw_dir = self.raw_base / LABEL
        self.published_dir = self.published_base / LABEL
        self.raw_dir.mkdir(parents=True)
        self.published_dir.mkdir(parents=True)

        self.config = NS(
            limits=NS(max_recursive_depth=8, max_recursive_files=100),
            paths=NS(
                cloud_source_root="/cloud/source",
                cloud_library_root="/cloud/organized",
                litepan_raw_output_root=str(self.raw_base),
                litepan_output_root=str(self.published_base),
                moviepilot_mount_root=str(self.root / "moviepilot"),
                emby_mount_root=str(self.emby_base),
                litepan_publish_subdir=None,
                cloud_library_subdir=None,
            ),
            runtime=NS(
                litepan_task_source_root=f"/cloud/organized/{LABEL}",
                litepan_task_output_root=str(self.raw_dir),
                emby_library_root=str(self.emby_base / LABEL),
                emby_library_name=LABEL,
            ),
        )
        self.mapper = PathMapper(self.config.paths)
        self.store = StateStore(self.root / "state.db")
        self.addCleanup(self.store.close)
        self.store.upsert_subscription(
            subscription_id="inventory-subscription",
            media_source="tmdb",
            media_id=MEDIA_ID,
            season=SEASON,
            work="Show",
            snapshot={"season_air_date": "2026-10-05"},
        )
        self.cloud = FakeCloud()
        self.emby = FakeEmby()
        self.episodes: dict[int, dict[str, object]] = {}

    def add_episode(self, number: int, *, selected: bool = True) -> dict[str, object]:
        cloud_path = (
            f"/cloud/organized/{LABEL}/Show/Season {SEASON}/"
            f"Show - S{SEASON:02d}E{number:02d}.mkv"
        )
        identity = {
            "media_source": "tmdb",
            "media_id": MEDIA_ID,
            "season": SEASON,
            "episode": number,
        }
        published_path = self.mapper.cloud_to_published_litepan(cloud_path, identity)
        episode_id = self.store.get_or_create_episode(
            {
                "key": EpisodeKey("tmdb", MEDIA_ID, SEASON, number),
                "subscription_id": "inventory-subscription",
                "canonical_work": "Show",
                "cloud_path": cloud_path,
                "strm_path": published_path,
            }
        )
        row = self.store.get_episode(episode_id)
        assert row is not None
        if selected:
            remote_file_id = f"remote-file-{number}"
            file_id = self.store.record_download_files(
                "a" * 40,
                [{
                    "remote_file_id": remote_file_id,
                    "path": cloud_path,
                    "name": Path(cloud_path).name,
                }],
            )[0]
            self.store.map_episode_source(episode_id, file_id, selected_primary=True)
            self.cloud.files.append({"remote_file_id": remote_file_id, "path": cloud_path})
            row = self.store.get_episode(episode_id)
            assert row is not None
        self.episodes[number] = row
        return row

    def prove(self, row: dict[str, object] | None = None) -> dict[str, object]:
        if row is None:
            row = self.episodes[4]
        return prove_submission_inventory(
            row, config=self.config, cloud=self.cloud, emby=self.emby, store=self.store
        )

    def raw_path(self, row: dict[str, object]) -> str:
        return self.mapper.cloud_to_raw_litepan(
            str(row["cloud_path"]), self.config.paths.litepan_raw_output_root
        )

    def published_path(self, row: dict[str, object]) -> str:
        return self.mapper.cloud_to_published_litepan(str(row["cloud_path"]), row)

    @staticmethod
    def write(path: str | Path, content: bytes | str) -> None:
        target = Path(path)
        target.parent.mkdir(parents=True, exist_ok=True)
        if isinstance(content, bytes):
            target.write_bytes(content)
        else:
            target.write_text(content, encoding="utf-8")

    def write_strm_pair(
        self,
        row: dict[str, object],
        *,
        raw_url: str = "https://media.example.invalid/play/episode-4",
        published_url: str | None = None,
        raw_newline: bytes = b"",
        include_raw: bool = True,
    ) -> tuple[str, str]:
        raw_path = self.raw_path(row)
        published_path = self.published_path(row)
        if include_raw:
            self.write(raw_path, raw_url.encode("utf-8") + raw_newline)
        self.write(published_path, (published_url or raw_url))
        return raw_path, published_path

    def write_sidecars(
        self,
        row: dict[str, object],
        *,
        episode_season: int = SEASON,
        show_id: str = MEDIA_ID,
        include_episode_image: bool = True,
        include_show_images: bool = True,
        include_season_images: bool = True,
    ) -> dict[str, str]:
        strm = self.published_path(row)
        stem = strm[:-5]
        season_dir = str(Path(strm).parent)
        work_dir = str(Path(season_dir).parent)
        paths = {
            "episode_nfo": stem + ".nfo",
            "episode_image": stem + ".jpg",
            "tvshow_nfo": f"{work_dir}/tvshow.nfo",
            "tvshow_poster": f"{work_dir}/poster.jpg",
            "tvshow_fanart": f"{work_dir}/fanart.jpg",
            "tvshow_backdrop": f"{work_dir}/backdrop.jpg",
            "season_nfo": f"{season_dir}/season.nfo",
            "season_poster": f"{season_dir}/poster.jpg",
            "season_root_poster": f"{work_dir}/season01-poster.jpg",
        }
        self.write(
            paths["episode_nfo"],
            "<episodedetails>"
            f"<season>{episode_season}</season>"
            f"<episode>{row['episode']}</episode>"
            "<tmdbid>90004</tmdbid></episodedetails>",
        )
        if include_episode_image:
            self.write(paths["episode_image"], b"episode-image")
        self.write(paths["tvshow_nfo"], f"<tvshow><tmdbid>{show_id}</tmdbid></tvshow>")
        if include_show_images:
            self.write(paths["tvshow_poster"], b"show-poster")
            self.write(paths["tvshow_fanart"], b"show-fanart")
            self.write(paths["tvshow_backdrop"], b"show-backdrop")
            self.write(paths["season_root_poster"], b"season-root-poster")
        self.write(
            paths["season_nfo"],
            f"<seasonDetails><seasonnumber>{SEASON}</seasonnumber></seasonDetails>",
        )
        if include_season_images:
            self.write(paths["season_poster"], b"season-poster")
        return paths

    def rewrite_nfo_same_size_restore_mtime(self, path: str | Path) -> None:
        target = Path(path)
        original_stat = target.stat()
        original = target.read_bytes()
        rewritten = original.replace(
            b"<tmdbid>90004</tmdbid>", b"<tmdbid>90005</tmdbid>", 1
        )
        self.assertEqual(len(rewritten), len(original))
        self.assertNotEqual(rewritten, original)
        target.write_bytes(rewritten)
        os.utime(
            target,
            ns=(original_stat.st_atime_ns, original_stat.st_mtime_ns),
        )

    def test_complete_sidecars_and_shared_images_are_not_unknown(self):
        row = self.add_episode(4)
        raw_path, published_path = self.write_strm_pair(
            row, raw_newline=b"\r\n"
        )
        self.write_sidecars(row)

        self.assertEqual(raw_path, self.mapper.cloud_to_raw_litepan(
            str(row["cloud_path"]), self.config.paths.litepan_raw_output_root
        ))
        self.assertEqual(published_path, row["strm_path"])
        self.assertEqual(self.prove(row)["state"], "present")

    def test_isolated_fid_path_maps_raw_colon_name_to_canonical_published_episode(self):
        row = self.add_episode(4)
        remote_file_id = "remote-file-4"
        isolated_cloud_path = self.mapper.build_isolated_cloud_path(
            "Show", SEASON, remote_file_id, "Original: episode 4.mkv", "2026-10-05"
        )
        self.store.conn.execute(
            "UPDATE episodes_v5 SET cloud_path=? WHERE id=?",
            (isolated_cloud_path, row["id"]),
        )
        self.store.conn.execute(
            "UPDATE download_files SET path=? WHERE remote_file_id=?",
            (isolated_cloud_path, remote_file_id),
        )
        self.cloud.files[0]["path"] = isolated_cloud_path
        row = self.store.get_episode(int(row["id"]))
        assert row is not None
        row["season_air_date"] = "2026-10-05"

        published_path = self.mapper.cloud_to_published_litepan(isolated_cloud_path, row)
        raw_path = self.mapper.cloud_to_raw_litepan(
            isolated_cloud_path, self.config.paths.litepan_raw_output_root
        )
        self.assertEqual(published_path, row["strm_path"])
        self.assertNotIn("fid-", published_path)
        self.assertIn("fid-", raw_path)
        self.assertIn("Original_ episode 4.strm", raw_path)
        url = "https://media.example.invalid/play/isolated-episode-4"
        self.write(raw_path, url)
        self.write(published_path, url)
        self.write_sidecars(row)

        self.assertEqual(self.prove(row)["state"], "present")

    def test_same_show_season_sidecars_shared_by_multiple_episodes_do_not_conflict(self):
        first = self.add_episode(4)
        second = self.add_episode(5)
        for row in (first, second):
            self.write_strm_pair(row, raw_url=f"https://media.example.invalid/e{row['episode']}")
            self.write_sidecars(row)

        self.assertEqual(self.prove(first)["state"], "present")

    def test_hardlinked_standard_season_posters_keep_verified_shared_ownership(self):
        row = self.add_episode(4)
        self.write_strm_pair(row)
        paths = self.write_sidecars(row)
        root_poster = Path(paths['season_root_poster'])
        root_poster.unlink()
        root_poster.hardlink_to(paths['season_poster'])
        self.assertEqual(root_poster.stat().st_nlink, 2)
        self.assertEqual(self.prove(row)['state'], 'present')

    def test_existing_sidecar_classification_reports_episode_and_shared_paths(self):
        row = self.add_episode(4, selected=False)
        paths = self.write_sidecars(row)
        result = SidecarEvidence(str(self.root)).classify_existing(row)

        self.assertTrue(result.ok)
        self.assertIn("paths_by_role", result.detail)
        classified = set()
        for role_paths in result.detail["paths_by_role"].values():
            if isinstance(role_paths, str):
                classified.add(role_paths)
            else:
                classified.update(role_paths)
        # SidecarEvidence classifies its six canonical scrape slots. The
        # inventory layer additionally checks standard shared artwork slots.
        canonical_roles = {'episode_nfo', 'episode_image', 'tvshow_nfo',
                           'tvshow_poster', 'season_nfo', 'season_poster'}
        self.assertEqual({paths[name] for name in canonical_roles}, classified)

    def test_raw_and_published_strm_urls_must_match(self):
        row = self.add_episode(4)
        self.write_strm_pair(
            row,
            raw_url="https://media.example.invalid/play/raw",
            published_url="https://media.example.invalid/play/other",
        )

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_wrong_raw_path_with_matching_name_is_not_inferred_by_filename(self):
        row = self.add_episode(4)
        expected_raw, published = self.write_strm_pair(row, include_raw=False)
        wrong_raw = Path(self.raw_dir) / "Misfiled" / "Season 1" / Path(expected_raw).name
        self.write(wrong_raw, b"https://media.example.invalid/play/episode-4\r\n")

        self.assertNotEqual(str(wrong_raw), expected_raw)
        self.assertTrue(Path(published).is_file())
        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_raw_strm_without_selected_cloud_owner_is_unknown(self):
        row = self.add_episode(4, selected=False)
        self.cloud.files.append({
            "remote_file_id": "unselected-file",
            "path": str(row["cloud_path"]),
        })
        self.write_strm_pair(row)

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_canonical_published_strm_can_be_present_without_raw_strm(self):
        row = self.add_episode(4)
        self.cloud.files.clear()
        raw_path, published_path = self.write_strm_pair(row, include_raw=False)

        self.assertFalse(Path(raw_path).exists())
        self.assertEqual(published_path, row["strm_path"])
        self.assertEqual(self.prove(row)["state"], "present")

    def test_same_remote_file_id_collision_blocks_related_and_unrelated_targets(self):
        related = self.add_episode(4)
        second = self.add_episode(5)
        unrelated_missing = self.add_episode(6, selected=False)
        shared_remote_file_id = "remote-file-4"
        second_file_id = self.store.record_download_files(
            "b" * 40,
            [{
                "remote_file_id": shared_remote_file_id,
                "path": str(second["cloud_path"]),
                "name": Path(str(second["cloud_path"])).name,
            }],
        )[0]
        self.store.map_episode_source(
            int(second["id"]), second_file_id, selected_primary=True
        )
        self.cloud.files.clear()
        self.write(
            self.published_path(related),
            "https://media.example.invalid/play/episode-4",
        )

        self.assertEqual(self.cloud.files, [])
        self.assertEqual(self.prove(related)["state"], "unknown")
        self.assertEqual(self.prove(unrelated_missing)["state"], "unknown")

    def test_persisted_published_path_must_match_canonical_mapping(self):
        row = self.add_episode(4)
        self.write_strm_pair(row, include_raw=False)
        wrong = str(Path(str(row["strm_path"])).with_name("renamed.strm"))
        self.store.conn.execute(
            "UPDATE episodes_v5 SET strm_path=? WHERE id=?", (wrong, row["id"])
        )
        self.write(wrong, "https://media.example.invalid/play/episode-4")
        row = self.store.get_episode(int(row["id"]))
        assert row is not None

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_wrong_season_episode_nfo_is_unknown(self):
        row = self.add_episode(4)
        self.write_strm_pair(row, include_raw=False)
        self.write_sidecars(row, episode_season=2)

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_wrong_show_nfo_identity_is_unknown(self):
        row = self.add_episode(4)
        self.write_strm_pair(row, include_raw=False)
        self.write_sidecars(row, show_id="99999")

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_unrecognized_extra_file_is_unknown(self):
        row = self.add_episode(4)
        self.write_strm_pair(row, include_raw=False)
        self.write(Path(self.published_dir) / "Show" / "mystery.bin", b"extra")

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_symlink_is_unknown(self):
        row = self.add_episode(4)
        raw_path, published_path = self.write_strm_pair(row, include_raw=False)
        target = self.root / "outside.strm"
        self.write(target, "https://media.example.invalid/play/episode-4")
        link = Path(raw_path)
        link.parent.mkdir(parents=True, exist_ok=True)
        link.symlink_to(target)
        self.assertTrue(Path(published_path).is_file())

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_cloud_path_outside_library_root_is_unknown(self):
        row = self.add_episode(4)
        self.write_strm_pair(row)
        outside = str(row["cloud_path"]).replace(
            "/cloud/organized/", "/cloud/organized-old/", 1
        )
        self.store.conn.execute(
            "UPDATE episodes_v5 SET cloud_path=? WHERE id=?", (outside, row["id"])
        )
        self.store.conn.execute(
            "UPDATE download_files SET path=? WHERE remote_file_id=?",
            (outside, "remote-file-4"),
        )
        self.cloud.files = [{"remote_file_id": "remote-file-4", "path": outside}]
        row = self.store.get_episode(int(row["id"]))
        assert row is not None

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_file_cap_is_unknown(self):
        row = self.add_episode(4)
        self.write_strm_pair(row, include_raw=False)
        self.config.limits.max_recursive_files = 2

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_same_size_nfo_rewrite_with_restored_mtime_is_unknown_during_read(self):
        row = self.add_episode(4)
        _, published = self.write_strm_pair(row, include_raw=False)
        sidecars = self.write_sidecars(row)
        nfo_path = sidecars["episode_nfo"]
        original_validate = StrmEvidence.validate
        rewritten = False

        def validate_then_rewrite(evidence, path):
            nonlocal rewritten
            result = original_validate(evidence, path)
            if path == published and not rewritten:
                self.rewrite_nfo_same_size_restore_mtime(nfo_path)
                rewritten = True
            return result

        with patch.object(StrmEvidence, "validate", new=validate_then_rewrite):
            result = self.prove(row)

        self.assertTrue(rewritten)
        self.assertEqual(result["state"], "unknown")

    def test_same_size_nfo_rewrite_with_restored_mtime_is_unknown_at_final_check(self):
        row = self.add_episode(4)
        self.write_strm_pair(row, include_raw=False)
        sidecars = self.write_sidecars(row)
        nfo_path = sidecars["episode_nfo"]
        original_unchanged = _HostInventory.unchanged
        rewritten = False

        def rewrite_before_final_check(inventory):
            nonlocal rewritten
            if Path(inventory.root) == self.published_dir and not rewritten:
                self.rewrite_nfo_same_size_restore_mtime(nfo_path)
                rewritten = True
            return original_unchanged(inventory)

        with patch.object(_HostInventory, "unchanged", new=rewrite_before_final_check):
            result = self.prove(row)

        self.assertTrue(rewritten)
        self.assertEqual(result["state"], "unknown")

    def test_oversized_nfo_is_unknown(self):
        row = self.add_episode(4)
        self.write_strm_pair(row, include_raw=False)
        sidecars = self.write_sidecars(row)
        oversized_nfo = (
            b"<episodedetails><season>1</season><episode>4</episode>"
            b"<tmdbid>90004</tmdbid><plot>"
            + b"x" * (1024 * 1024 + 1)
            + b"</plot></episodedetails>"
        )
        self.write(sidecars["episode_nfo"], oversized_nfo)

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_oversized_strm_is_unknown(self):
        row = self.add_episode(4)
        self.write_strm_pair(row, include_raw=False)
        oversized = b"https://media.example.invalid/" + b"x" * (1024 * 1024)
        self.write(self.published_path(row), oversized)

        self.assertEqual(self.prove(row)["state"], "unknown")

    def test_valid_partial_sidecars_are_classified_but_do_not_prove_episode_present(self):
        row = self.add_episode(4)
        self.cloud.files.clear()
        paths = self.write_sidecars(
            row,
            include_episode_image=True,
            include_show_images=False,
            include_season_images=False,
        )
        self.assertTrue(Path(paths["episode_nfo"]).is_file())
        self.assertFalse(Path(paths["tvshow_poster"]).exists())
        self.assertFalse(Path(self.raw_path(row)).exists())
        self.assertFalse(Path(self.published_path(row)).exists())

        result = self.prove(row)
        self.assertEqual(result["state"], "missing")


if __name__ == "__main__":
    unittest.main()
