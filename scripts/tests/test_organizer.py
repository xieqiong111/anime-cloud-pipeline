from __future__ import annotations

import os
import tempfile
import unittest

from pipeline.config import PathConfig
from pipeline.models import EpisodeStatus, ReviewReason
from pipeline.organizer import Organizer
from pipeline.paths import PathMapper
from pipeline.real_adapters import P115CloudAdapter
from pipeline.state import StateStore


PATHS = PathConfig(
    cloud_source_root="/影视库/下载",
    cloud_library_root="/影视库/动画",
    litepan_output_root="/lite/out",
    moviepilot_mount_root="/media/mp",
    emby_mount_root="/media/emby",
)
MAPPER = PathMapper(PATHS)


def identity(file_id: str, *, size: int = 100, fingerprint: str = "fp-1") -> dict[str, object]:
    return {
        "remote_file_id": file_id,
        "file_id": file_id,
        "size": size,
        "fingerprint": fingerprint,
    }


class FakeCloud:
    def __init__(self, source: dict[str, object] | None, target: dict[str, object] | None):
        self.files: dict[str, dict[str, object]] = {}
        self.source_path = "/影视库/下载/incoming.mkv"
        self.target_path = "/影视库/动画/2026-10月新番/Show/Season 1/Show - S01E08.mkv"
        if source is not None:
            self.files[self.source_path] = {"path": self.source_path, "name": "incoming.mkv", **source}
        if target is not None:
            self.files[self.target_path] = {"path": self.target_path, "name": os.path.basename(self.target_path), **target}
        self.mkdir_calls: list[str] = []
        self.directory_reads: list[str] = []
        self.move_calls: list[tuple[str, str]] = []
        self.rename_calls: list[tuple[str, str]] = []
        self.fail_mkdir = False
        self.fail_move = False
        self.move_error = "move failed access_token=secret"
        self.fail_rename = False
        self.fail_directory_read = False
        self.race_target_on_atomic_rename = False
        self.race_target: dict[str, object] | None = None

    def get_file(self, path: str):
        value = self.files.get(path)
        return None if value is None else dict(value)

    def mkdir(self, path: str):
        self.mkdir_calls.append(path)
        if self.fail_mkdir:
            raise RuntimeError("mkdir failed Authorization: Bearer secret")
        return {"directory_id": path, "path": path}

    def get_directory(self, directory_id: str):
        self.directory_reads.append(directory_id)
        if self.fail_directory_read:
            return None
        return {"directory_id": directory_id, "path": directory_id}

    def move_file(self, file_id: str, directory_id: str):
        self.move_calls.append((file_id, directory_id))
        if self.fail_move:
            raise RuntimeError(self.move_error)
        source_path, source = next(
            (path, value) for path, value in self.files.items()
            if value.get("remote_file_id") == file_id
        )
        del self.files[source_path]
        source = dict(source)
        source["path"] = f"{directory_id}/{source['name']}"
        self.files[source["path"]] = source
        return {"state": True}

    def rename_file_no_replace(
        self,
        file_id: str,
        name: str,
        *,
        expected_identity=None,
        target_path: str | None = None,
    ):
        self.rename_calls.append((file_id, name))
        if self.fail_rename:
            raise RuntimeError("rename failed cookies=secret")
        if self.race_target_on_atomic_rename and target_path:
            self.files[target_path] = {
                "path": target_path,
                "name": name,
                **(self.race_target or identity("racing-target")),
            }
            return {"state": False, "reason": "PATH_CONFLICT"}
        if target_path and target_path in self.files:
            return {"state": False, "reason": "PATH_CONFLICT"}
        source_path, source = next(
            (path, value) for path, value in self.files.items()
            if value.get("remote_file_id") == file_id
        )
        del self.files[source_path]
        source = dict(source)
        source["name"] = name
        parent = source_path.rsplit("/", 1)[0]
        source["path"] = f"{parent}/{name}"
        self.files[source["path"]] = source
        return {"state": True}

    def rename_file(self, file_id: str, name: str):
        return self.rename_file_no_replace(file_id, name)


def episode(**overrides: object) -> dict[str, object]:
    value: dict[str, object] = {
        "id": 1,
        "media_source": "tmdb",
        "media_id": "111",
        "canonical_work": "Show",
        "season": 1,
        "episode": 8,
        "season_air_date": "2026-10-03",
        "source_path": "/影视库/下载/incoming.mkv",
        "file_id": "source-1",
        "file_size": 100,
        "fingerprint": "fp-1",
        "status": EpisodeStatus.CLOUD_READY.value,
    }
    value.update(overrides)
    return value


class OrganizerTests(unittest.TestCase):
    def test_partial_root_chain_leaves_pending_without_any_cloud_write(self):
        from unittest.mock import Mock

        for bad_id in ("0", "media"):
            with self.subTest(id=bad_id):
                class PartialBackend:
                    def fs_files(self, payload):
                        request = dict(payload)
                        directory_id = str(request["cid"])
                        entries = {
                            "0": [{"cid": "media", "n": "影视库"}],
                            "media": [
                                {"cid": "downloads", "n": "下载", "type": "folder"},
                                {"cid": "anime", "n": "动画", "type": "folder"},
                            ],
                            "downloads": [{
                                "fid": "source-1", "n": "incoming.mkv", "type": "file",
                                "size": 100, "sha1": "fp-1",
                            }],
                            "anime": [],
                        }[directory_id]
                        return {
                            "state": True,
                            "cid": directory_id,
                            "data": entries,
                            "count": len(entries),
                            "offset": 1 if directory_id == bad_id else request.get("offset", 0),
                        }

                backend = PartialBackend()
                backend.fs_mkdir = Mock()
                backend.fs_move = Mock()
                backend.fs_rename_no_replace = Mock()
                cloud = P115CloudAdapter(None, "media", client=backend,
                    root_directory_path="/影视库")
                result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
                    episode(
                        id=self.episode_id,
                        remote_file_id="source-1",
                        selected_file={
                            "remote_file_id": "source-1",
                            "filename": "incoming.mkv",
                            "path": "/影视库/下载/incoming.mkv",
                            "size": 100,
                            "fingerprint": "fp-1",
                        },
                    )
                )
                self.assertEqual(result.status, "organize_pending")
                backend.fs_mkdir.assert_not_called()
                backend.fs_move.assert_not_called()
                backend.fs_rename_no_replace.assert_not_called()

    def setUp(self) -> None:
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))
        self.episode_id = self.store.get_or_create_episode(
            {
                "media_source": "tmdb",
                "media_id": "111",
                "season": 1,
                "episode": 8,
                "canonical_work": "Show",
                "status": EpisodeStatus.CLOUD_READY,
            }
        )

    def tearDown(self) -> None:
        self.store.close()
        self.tmp.cleanup()

    def test_existing_same_target_reconciles_without_move(self):
        cloud = FakeCloud(source=identity("source-1"), target=identity("source-1"))
        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.ORGANIZED.value)
        self.assertIsNone(result.review_reason)
        self.assertEqual(cloud.move_calls, [])
        self.assertEqual(cloud.rename_calls, [])
        self.assertEqual(self.store.get_episode(self.episode_id)["status"], "organized")

    def test_existing_different_target_needs_review_without_overwrite(self):
        cloud = FakeCloud(source=identity("source-1"), target=identity("different"))
        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id)
        )

        self.assertEqual(result.review_reason, ReviewReason.PATH_CONFLICT)
        self.assertEqual(cloud.move_calls, [])
        self.assertEqual(cloud.rename_calls, [])
        self.assertEqual(self.store.get_episode(self.episode_id)["status"], "needs_review")

    def test_restart_from_organize_pending_reconciles_source_missing_target_identity(self):
        self.store.mark_pending(
            self.episode_id,
            EpisodeStatus.ORGANIZE_PENDING,
            extra={
                "runtime_identity_json": {
                    **identity("source-1"),
                    "_season_air_date": "2026-10-03",
                    "_work": "Show",
                    "_season": 1,
                    "_episode": 8,
                    "_extension": ".mkv",
                },
                "cloud_path": "/影视库/动画/2026-10月新番/Show/Season 1/Show - S01E08.mkv",
            },
        )
        cloud = FakeCloud(source=None, target=identity("source-1"))

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(self.episode_id)

        self.assertEqual(result.status, EpisodeStatus.ORGANIZED.value)
        self.assertEqual(cloud.move_calls, [])
        self.assertEqual(self.store.get_episode(self.episode_id)["status"], "organized")

    def test_restart_after_move_before_rename_reuses_remote_identity_without_second_move(self):
        self.store.mark_pending(
            self.episode_id,
            EpisodeStatus.ORGANIZE_PENDING,
            extra={
                "runtime_identity_json": {
                    **identity("source-1"),
                    "_season_air_date": "2026-10-03",
                    "_work": "Show",
                    "_season": 1,
                    "_episode": 8,
                    "_extension": ".mkv",
                },
            },
        )
        cloud = FakeCloud(source=identity("source-1"), target=None)
        cloud.files["/影视库/动画/2026-10月新番/Show/Season 1/incoming.mkv"] = {
            "path": "/影视库/动画/2026-10月新番/Show/Season 1/incoming.mkv",
            "name": "incoming.mkv",
            **identity("source-1"),
        }
        del cloud.files[cloud.source_path]

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(self.episode_id)

        self.assertEqual(result.status, EpisodeStatus.ORGANIZED.value)
        self.assertEqual(cloud.move_calls, [])
        self.assertEqual(len(cloud.rename_calls), 1)

    def test_missing_air_date_opens_review_before_cloud_writes(self):
        cloud = FakeCloud(source=identity("source-1"), target=None)

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id, season_air_date="", air_date="2026-10-03")
        )

        self.assertEqual(result.review_reason, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS)
        self.assertEqual(cloud.mkdir_calls, [])
        self.assertEqual(cloud.move_calls, [])
        self.assertEqual(cloud.rename_calls, [])
        self.assertEqual(self.store.get_episode(self.episode_id)["status"], "needs_review")

    def test_legacy_persisted_air_date_cannot_write_without_season_air_date(self):
        cloud = FakeCloud(source=identity("source-1"), target=None)

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(
                id=self.episode_id,
                season_air_date="",
                air_date="2026-10-03",
                runtime_identity_json={"_air_date": "2026-10-03"},
            )
        )

        self.assertEqual(result.review_reason, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS)
        self.assertEqual(cloud.mkdir_calls, [])
        self.assertEqual(cloud.move_calls, [])
        self.assertEqual(cloud.rename_calls, [])

    def test_size_only_target_is_not_proven_same(self):
        cloud = FakeCloud(source={"size": 100}, target={"size": 100})
        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(
                id=self.episode_id,
                file_id="",
                remote_file_id="",
                fingerprint="",
                file_size=100,
            )
        )
        self.assertEqual(result.review_reason, ReviewReason.PATH_CONFLICT)
        self.assertEqual(cloud.move_calls, [])

    def test_restart_prefers_persisted_canonical_path_when_metadata_drifts(self):
        persisted_path = "/影视库/动画/2026-10月新番/Show/Season 1/Show - S01E08.mkv"
        self.store.mark_pending(
            self.episode_id,
            EpisodeStatus.ORGANIZE_PENDING,
            extra={
                "cloud_path": persisted_path,
                "runtime_identity_json": {
                    **identity("source-1"),
                    "_season_air_date": "2026-10-03",
                    "_work": "Show",
                    "_season": 1,
                    "_episode": 8,
                    "_extension": ".mkv",
                },
            },
        )
        cloud = FakeCloud(source=None, target=identity("source-1"))
        row = episode(
            id=self.episode_id,
            status=EpisodeStatus.ORGANIZE_PENDING.value,
            cloud_path=persisted_path,
            runtime_identity_json={
                **identity("source-1"),
                "_season_air_date": "2026-10-03",
                "_work": "Show",
                "_season": 1,
                "_episode": 8,
                "_extension": ".mkv",
            },
            season_air_date="2027-01-03",
            canonical_work="Renamed Show",
        )

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(row)

        self.assertEqual(result.status, EpisodeStatus.ORGANIZED.value)
        self.assertEqual(result.cloud_path, persisted_path)
        self.assertEqual(cloud.move_calls, [])

    def test_directory_is_re_read_before_move(self):
        cloud = FakeCloud(source=identity("source-1"), target=None)

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.ORGANIZED.value)
        self.assertEqual(len(cloud.directory_reads), 1)
        self.assertEqual(len(cloud.move_calls), 1)

    def test_directory_disappearing_after_mkdir_stays_pending_without_move(self):
        cloud = FakeCloud(source=identity("source-1"), target=None)
        cloud.fail_directory_read = True

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.ORGANIZE_PENDING.value)
        self.assertEqual(cloud.move_calls, [])

    def test_atomic_rename_race_returns_path_conflict_without_overwrite(self):
        cloud = FakeCloud(source=identity("source-1"), target=None)
        cloud.race_target_on_atomic_rename = True
        cloud.race_target = identity("racing-target", fingerprint="different")

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id)
        )

        self.assertEqual(result.review_reason, ReviewReason.PATH_CONFLICT)
        self.assertEqual(len(cloud.move_calls), 1)
        self.assertEqual(len(cloud.rename_calls), 1)
        self.assertEqual(
            cloud.files[result.cloud_path]["remote_file_id"], "racing-target"
        )

    def test_real_adapter_conflict_maps_to_path_review_in_organizer(self):
        class ConflictClient:
            def __init__(self):
                self.rename_calls = []

            def fs_rename_no_replace(self, payload):
                self.rename_calls.append(payload)
                return {"success": False, "reason": "target_exists", "code": "EEXIST"}

        backend = ConflictClient()
        real_cloud = P115CloudAdapter(None, "root", client=backend)

        class AdapterBackedCloud(FakeCloud):
            def rename_file_no_replace(
                self,
                file_id: str,
                name: str,
                *,
                expected_identity=None,
                target_path=None,
            ):
                self.rename_calls.append((file_id, name))
                return real_cloud.rename_file_no_replace(
                    file_id,
                    name,
                    expected_identity=expected_identity,
                    target_path=target_path,
                )

        cloud = AdapterBackedCloud(source=identity("source-1"), target=None)
        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id)
        )

        self.assertEqual(result.review_reason, ReviewReason.PATH_CONFLICT)
        self.assertEqual(result.status, EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(len(backend.rename_calls), 1)
        self.assertEqual(len(cloud.rename_calls), 1)
        self.assertNotIn(result.cloud_path, cloud.files)

    def test_directory_listing_contract_is_not_called_with_a_path(self):
        class DirectoryOnlyCloud:
            def __init__(self):
                self.calls: list[str] = []

            def list_files(self, directory_id: str):
                self.calls.append(directory_id)
                return []

        cloud = DirectoryOnlyCloud()
        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id)
        )
        self.assertEqual(result.status, EpisodeStatus.ORGANIZE_PENDING.value)
        self.assertEqual(cloud.calls, [])

    def test_configured_secret_is_absent_from_result_last_error_and_history(self):
        secret = "organizer-cookie-123"
        cloud = FakeCloud(source=identity("source-1"), target=None)
        cloud.fail_move = True
        cloud.move_error = secret

        result = Organizer(self.store, cloud, MAPPER, secrets=(secret,)).reconcile_or_apply(
            episode(id=self.episode_id)
        )

        row = self.store.get_episode(self.episode_id)
        self.assertNotIn(secret, str(result.detail))
        self.assertNotIn(secret, str(row.get("last_error")))
        self.assertNotIn(secret, str(self.store.last_history_detail(self.episode_id)))

    def test_source_path_escape_is_reviewed_without_remote_reads_or_writes(self):
        cloud = FakeCloud(source=identity("source-1"), target=None)

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id, source_path="/影视库/下载/../private/incoming.mkv")
        )

        self.assertEqual(result.review_reason, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS)
        self.assertEqual(cloud.mkdir_calls, [])
        self.assertEqual(cloud.move_calls, [])
        self.assertEqual(cloud.rename_calls, [])

    def test_same_remote_id_with_different_size_is_a_path_conflict(self):
        cloud = FakeCloud(
            source=identity("source-1", size=100),
            target=identity("source-1", size=101),
        )

        result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
            episode(id=self.episode_id)
        )

        self.assertEqual(result.review_reason, ReviewReason.PATH_CONFLICT)
        self.assertEqual(cloud.move_calls, [])

    def test_mkdir_move_and_rename_failures_remain_pending_with_sanitized_detail(self):
        for attr in ("fail_mkdir", "fail_move", "fail_rename"):
            with self.subTest(operation=attr):
                self.store.transition(self.episode_id, EpisodeStatus.CLOUD_READY)
                cloud = FakeCloud(source=identity("source-1"), target=None)
                setattr(cloud, attr, True)
                result = Organizer(self.store, cloud, MAPPER).reconcile_or_apply(
                    episode(id=self.episode_id)
                )
                self.assertEqual(result.status, EpisodeStatus.ORGANIZE_PENDING.value)
                self.assertIsNone(result.review_reason)
                self.assertNotIn("secret", str(result.detail))
                self.assertEqual(
                    self.store.get_episode(self.episode_id)["status"],
                    EpisodeStatus.ORGANIZE_PENDING.value,
                )


if __name__ == "__main__":
    unittest.main(verbosity=2)
