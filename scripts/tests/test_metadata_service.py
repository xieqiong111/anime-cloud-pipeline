"""Focused Task 8 tests for sidecar identity and recoverable scraping."""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from pipeline.adapters import ScrapeResult
from pipeline.evidence import SidecarEvidence
from pipeline.http import HttpError
from pipeline.models import EpisodeKey, EpisodeStatus, ReviewReason
from pipeline.paths import PathMapper, ServiceRoots
from pipeline.services import MetadataService
from pipeline.state import StateStore


STRM = "/lite/Show/Season 1/Show - S01E08.strm"
EP_NFO = "/lite/Show/Season 1/Show - S01E08.nfo"
EP_IMAGE = "/lite/Show/Season 1/Show - S01E08.jpg"
TVSHOW_NFO = "/lite/Show/tvshow.nfo"
TVSHOW_POSTER = "/lite/Show/poster.jpg"
SEASON_NFO = "/lite/Show/Season 1/season.nfo"
SEASON_POSTER = "/lite/Show/Season 1/poster.jpg"


class FakeFiles:
    def __init__(self, values=None):
        self.values = dict(values or {})
        self.read_calls: list[str] = []
        self.symlinks: set[str] = set()

    def exists(self, path: str) -> bool:
        return path in self.values

    def read(self, path: str) -> object:
        self.read_calls.append(path)
        if path not in self.values:
            raise FileNotFoundError(path)
        return self.values[path]

    def is_symlink(self, path: str) -> bool:
        return path in self.symlinks


class ExplodingFiles(FakeFiles):
    def __init__(self, message: str):
        super().__init__({})
        self.values = None
        self.message = message

    def exists(self, path: str) -> bool:
        raise RuntimeError(self.message)


def complete_files(*, season: int = 1, episode: int = 8) -> dict[str, object]:
    return {
        STRM: "https://media.example/play/1/t/redacted/n/file",
        EP_NFO: (
            "<episodedetails><season>%d</season><episode>%d</episode>"
            "<tmdbid>111</tmdbid></episodedetails>" % (season, episode)
        ),
        EP_IMAGE: b"episode-image",
        TVSHOW_NFO: "<tvshow><tmdbid>111</tmdbid></tvshow>",
        TVSHOW_POSTER: b"show-poster",
        SEASON_NFO: "<seasondetails><seasonnumber>%d</seasonnumber></seasondetails>" % season,
        SEASON_POSTER: b"season-poster",
    }


class FakeMoviePilot:
    def __init__(self, *, error=None, files=None):
        self.error = error
        self.files = files
        self.scrape_calls: list[tuple[str, str, str]] = []

    def scrape_in_place(self, strm_path: str, media_source: str, media_id: str, type_name="电视剧"):
        self.scrape_calls.append((strm_path, media_source, media_id))
        if self.error is not None:
            if isinstance(self.error, BaseException):
                raise self.error
            raise RuntimeError(str(self.error))
        return ScrapeResult(ok=True, nfo_path=EP_NFO)


class MetadataServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def episode(self, **overrides):
        values = {
            "key": EpisodeKey("tmdb", "111", 1, 8),
            "canonical_work": "Show",
            "title": "Show - S01E08",
            "status": EpisodeStatus.STRM_READY,
            "cloud_path": "/cloud/Show/Season 1/Show - S01E08.mkv",
            "strm_path": STRM,
        }
        values.update(overrides)
        return self.store.get_or_create_episode(values)

    @property
    def mapper(self):
        return PathMapper(ServiceRoots(
            cloud_source_root="/cloud-source",
            cloud_library_root="/cloud",
            litepan_output_root="/lite",
            moviepilot_mount_root="/moviepilot",
            emby_mount_root="/emby",
        ))

    def service(self, moviepilot, files, **kwargs):
        return MetadataService(
            self.store,
            moviepilot,
            SidecarEvidence(files),
            mapper=kwargs.pop("mapper", self.mapper),
            poll_attempts=kwargs.pop("poll_attempts", 2),
            poll_interval=0,
            **kwargs,
        )

    def test_http_500_returns_to_recoverable_scrape_pending(self):
        episode_id = self.episode()
        secret_url = "https://media.example/api/scrape?token=secret-token"
        moviepilot = FakeMoviePilot(error=HttpError(500, url=secret_url))

        self.service(moviepilot, FakeFiles({})).advance(self.store.get_episode(episode_id))

        row = self.store.get_episode(episode_id)
        self.assertEqual(row["status"], EpisodeStatus.SCRAPE_PENDING.value)
        self.assertGreater(row["retry_count"], 0)
        self.assertIsNotNone(row["next_retry_at"])
        self.assertNotIn("secret-token", row["last_error"])
        self.assertNotIn("secret-token", json.dumps(self.store.last_history_detail(episode_id)))

    def test_restart_reconciles_existing_sidecars_before_rescrape(self):
        episode_id = self.episode(status=EpisodeStatus.SCRAPE_PENDING)
        moviepilot = FakeMoviePilot()

        self.service(moviepilot, FakeFiles(complete_files())).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.METADATA_READY.value)
        self.assertEqual(moviepilot.scrape_calls, [])

    def test_direct_evidence_rejects_conflicting_strm_aliases(self):
        for alias in ("litepan_path", "moviepilot_path", "emby_path"):
            with self.subTest(alias=alias):
                result = SidecarEvidence(FakeFiles(complete_files())).validate({
                    "strm_path": STRM, alias: "/other/Show/Season 1/other.strm",
                    "media_id": "111", "season": 1, "episode": 8,
                })
                self.assertFalse(result.ok)

    def test_mismatched_episode_nfo_opens_metadata_review(self):
        result = SidecarEvidence(FakeFiles(complete_files(season=1, episode=7))).validate(
            {"strm_path": STRM, "media_id": "111", "season": 1, "episode": 8}
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.review_reason, ReviewReason.METADATA_CONFLICT)

    def test_missing_any_required_sidecar_stays_pending_after_scrape(self):
        episode_id = self.episode()
        moviepilot = FakeMoviePilot()

        self.service(moviepilot, FakeFiles({STRM: "https://media.example/play"})).advance(
            self.store.get_episode(episode_id)
        )

        row = self.store.get_episode(episode_id)
        self.assertEqual(row["status"], EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(len(moviepilot.scrape_calls), 1)
        self.assertIsNotNone(row["next_retry_at"])

    def test_one_invocation_has_bounded_deterministic_validation_calls(self):
        episode_id = self.episode()

        class CountingEvidence:
            def __init__(self):
                self.calls = 0

            def validate(self, episode):
                self.calls += 1
                from pipeline.evidence import EvidenceResult
                return EvidenceResult(False, detail={"reason": "missing"})

        evidence = CountingEvidence()
        service = MetadataService(
            self.store,
            FakeMoviePilot(),
            evidence,
            mapper=self.mapper,
            poll_attempts=2,
            poll_interval=0,
        )

        service.advance(self.store.get_episode(episode_id))

        self.assertEqual(evidence.calls, 3)  # pre-scrape plus two bounded polls

    def test_strm_ready_without_canonical_strm_path_does_not_scrape(self):
        episode_id = self.episode(strm_path="")
        moviepilot = FakeMoviePilot()

        result = self.service(moviepilot, FakeFiles({})).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(moviepilot.scrape_calls, [])
        row = self.store.get_episode(episode_id)
        self.assertIsNotNone(row["next_retry_at"])

    def test_reader_exception_is_recoverable_and_secret_safe(self):
        episode_id = self.episode()
        secret_url = "https://media.example/scrape?token=secret-reader"
        moviepilot = FakeMoviePilot()

        result = self.service(
            moviepilot,
            ExplodingFiles(f"reader failed for {secret_url}"),
        ).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(moviepilot.scrape_calls, [])
        row = self.store.get_episode(episode_id)
        self.assertNotIn("secret-reader", row["last_error"])
        self.assertNotIn("secret-reader", json.dumps(self.store.last_history_detail(episode_id)))

    def test_backoff_reconciles_complete_sidecars_before_scrape_skip(self):
        episode_id = self.episode(
            status=EpisodeStatus.SCRAPE_PENDING,
            retry_count=3,
            next_retry_at=9999999999,
        )
        moviepilot = FakeMoviePilot()

        result = self.service(
            moviepilot,
            FakeFiles(complete_files()),
            now=lambda: 100,
        ).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.METADATA_READY.value)
        self.assertEqual(moviepilot.scrape_calls, [])
        row = self.store.get_episode(episode_id)
        self.assertEqual(row["retry_count"], 0)
        self.assertIsNone(row["next_retry_at"])

    def test_xml_doctype_and_entity_are_rejected(self):
        files = complete_files()
        files[EP_NFO] = (
            "<!DOCTYPE episodedetails [<!ENTITY secret '111'>]>"
            "<episodedetails><season>1</season><episode>&secret;</episode>"
            "<tmdbid>111</tmdbid></episodedetails>"
        )

        result = SidecarEvidence(FakeFiles(files)).validate(
            {"strm_path": STRM, "media_id": "111", "season": 1, "episode": 8}
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.review_reason, ReviewReason.METADATA_CONFLICT)
        self.assertNotIn("111", json.dumps(result.detail))

    def test_duplicate_episode_identity_fields_must_agree(self):
        files = complete_files()
        files[EP_NFO] = (
            "<episodedetails><season>1</season><season>2</season>"
            "<episode>8</episode><tmdbid>111</tmdbid></episodedetails>"
        )

        result = SidecarEvidence(FakeFiles(files)).validate(
            {"strm_path": STRM, "media_id": "111", "season": 1, "episode": 8}
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.review_reason, ReviewReason.METADATA_CONFLICT)

    def test_sidecar_alias_paths_are_not_treated_as_complete(self):
        files = complete_files()
        files[TVSHOW_POSTER] = files[EP_IMAGE]

        result = SidecarEvidence(FakeFiles(files)).validate({
            "strm_path": STRM,
            "episode_nfo_path": EP_NFO,
            "episode_image_path": TVSHOW_POSTER,
            "tvshow_nfo_path": TVSHOW_NFO,
            "tvshow_poster_path": TVSHOW_POSTER,
            "season_nfo_path": SEASON_NFO,
            "season_poster_path": SEASON_POSTER,
            "media_id": "111",
            "season": 1,
            "episode": 8,
        })

        self.assertFalse(result.ok)
        self.assertEqual(result.detail["reason"], "sidecar_path_collision")

    def test_wrong_root_or_foo_only_nfo_is_not_complete(self):
        files = complete_files()
        files[EP_NFO] = "<foo><title>Show</title></foo>"

        result = SidecarEvidence(FakeFiles(files)).validate(
            {"strm_path": STRM, "media_id": "111", "season": 1, "episode": 8}
        )

        self.assertFalse(result.ok)
        self.assertEqual(result.review_reason, ReviewReason.METADATA_CONFLICT)

    def test_sidecar_identity_is_required_even_when_nfo_contains_identity(self):
        result = SidecarEvidence(FakeFiles(complete_files())).validate({
            "strm_path": STRM,
            "media_id": "111",
        })

        self.assertFalse(result.ok)
        self.assertEqual(result.review_reason, ReviewReason.METADATA_CONFLICT)

    def test_sidecar_symlink_is_pending_without_moviepilot_scrape(self):
        episode_id = self.episode()
        files = FakeFiles(complete_files())
        files.symlinks.add(EP_NFO)
        moviepilot = FakeMoviePilot()

        result = self.service(moviepilot, files).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(result.evidence.detail["reason"], "symlink")
        self.assertEqual(moviepilot.scrape_calls, [])

    def test_canonical_strm_symlink_is_pending_without_moviepilot_scrape(self):
        episode_id = self.episode()
        files = FakeFiles({STRM: "https://media.example/play"})
        files.symlinks.add(STRM)
        moviepilot = FakeMoviePilot()

        result = self.service(moviepilot, files).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(result.evidence.detail["reason"], "symlink")
        self.assertEqual(moviepilot.scrape_calls, [])

    def test_unsafe_sidecar_alias_is_pending_without_moviepilot_scrape(self):
        episode_id = self.episode()
        moviepilot = FakeMoviePilot()

        class UnsafeSidecarEvidence:
            def validate(self, episode):
                values = dict(episode)
                values["episode_nfo_path"] = "/tmp/not-canonical.nfo?escape"
                return SidecarEvidence(FakeFiles(complete_files())).validate(values)

        service = MetadataService(
            self.store,
            moviepilot,
            UnsafeSidecarEvidence(),
            mapper=self.mapper,
            poll_attempts=2,
            poll_interval=0,
        )

        result = service.advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(result.evidence.detail["reason"], "unsafe_path")
        self.assertEqual(moviepilot.scrape_calls, [])

    def test_sidecar_path_collision_is_pending_without_moviepilot_scrape(self):
        episode_id = self.episode()
        moviepilot = FakeMoviePilot()

        class CollidingSidecarEvidence:
            def validate(self, episode):
                values = dict(episode)
                values["episode_image_path"] = TVSHOW_POSTER
                values["tvshow_poster_path"] = TVSHOW_POSTER
                return SidecarEvidence(FakeFiles(complete_files())).validate(values)

        service = MetadataService(
            self.store,
            moviepilot,
            CollidingSidecarEvidence(),
            mapper=self.mapper,
            poll_attempts=2,
            poll_interval=0,
        )

        result = service.advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(result.evidence.detail["reason"], "sidecar_path_collision")
        self.assertEqual(moviepilot.scrape_calls, [])

    def test_second_sidecar_alias_unsafe_value_cannot_be_hidden_by_canonical_first(self):
        episode_id = self.episode()
        moviepilot = FakeMoviePilot()
        files = FakeFiles({STRM: "https://media.example/play"})

        class UnsafeAliasEvidence:
            def validate(self, episode):
                values = dict(episode)
                values["episode_nfo_path"] = EP_NFO
                values["nfo_path"] = "https://media.example/nfo?token=secret"
                return SidecarEvidence(files).validate(values)

        service = MetadataService(
            self.store,
            moviepilot,
            UnsafeAliasEvidence(),
            mapper=self.mapper,
            poll_attempts=2,
            poll_interval=0,
            secrets=("secret",),
        )

        result = service.advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(moviepilot.scrape_calls, [])
        self.assertNotIn("secret", json.dumps(result.detail))

    def test_second_sidecar_alias_conflict_cannot_be_hidden_by_canonical_first(self):
        episode_id = self.episode()
        moviepilot = FakeMoviePilot()
        files = FakeFiles({STRM: "https://media.example/play"})

        class ConflictingAliasEvidence:
            def validate(self, episode):
                values = dict(episode)
                values["episode_nfo_path"] = EP_NFO
                values["nfo_path"] = "/lite/Other/other.nfo"
                return SidecarEvidence(files).validate(values)

        service = MetadataService(
            self.store,
            moviepilot,
            ConflictingAliasEvidence(),
            mapper=self.mapper,
            poll_attempts=2,
            poll_interval=0,
        )

        result = service.advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.SCRAPE_PENDING.value)
        self.assertEqual(moviepilot.scrape_calls, [])


if __name__ == "__main__":
    unittest.main(verbosity=2)
