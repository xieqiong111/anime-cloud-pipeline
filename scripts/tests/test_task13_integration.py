"""Task 13 executable integration contracts for the V5 runtime boundary."""
from __future__ import annotations

import os
import json
from datetime import datetime
from pathlib import Path
import tempfile
import unittest
from unittest import mock
import sys
from zoneinfo import ZoneInfo

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.candidates import SubmissionBudget
from pipeline.adapters import ScrapeResult
from pipeline.evidence import SidecarEvidence
from pipeline.executor import PipelineRunner
from pipeline.http import RetryPolicy
from pipeline.models import EpisodeKey, EpisodeStatus, ReviewReason
from pipeline.paths import PathMapper, ServiceRoots
from pipeline.services import (
    DownloadService,
    LibraryService,
    MetadataService,
    ServiceResult,
    StrmService,
)
from pipeline.state import StateStore

import run_pipeline
import anime_pipeline


def _current_quarter() -> str:
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    quarter_month = ((now.month - 1) // 3) * 3 + 1
    return f"{now.year:04d}-{quarter_month:02d}"


def _stage_store(path: Path) -> StateStore:
    """Give stage-only fixtures a verified season date without remote discovery."""
    store = StateStore(path)
    store.get_subscription_season_air_date = lambda _episode_id: _current_quarter() + "-03"
    return store


def _config(state_directory: str) -> dict:
    quarter_label = _current_quarter() + "月新番"
    return {
        "schema_version": 5,
        "services": {
            "moviepilot": {
                "name": "moviepilot",
                "base_url": "https://moviepilot.example.invalid",
                "token_env": "MOVIEPILOT_TOKEN",
            },
            "cloud": {
                "name": "cloud",
                "credential_env": "CLOUD_COOKIE",
                "root_directory_path": "/cloud",
            },
            "litepan": {
                "name": "litepan",
                "base_url": "https://litepan.example.invalid",
                "api_key_env": "LITEPAN_API_KEY",
                "task_name": "anime-strm",
            },
            "emby": {
                "name": "emby",
                "base_url": "https://emby.example.invalid",
                "api_key_env": "EMBY_API_KEY",
                "library_name": "Anime",
            },
        },
        "source": {
            "name": "kisssub",
            "kind": "kisssub",
            "base_url": "https://source.example.invalid",
        },
        "paths": {
            "cloud_source_root": "/cloud/source",
            "cloud_library_root": "/cloud/library",
            "litepan_output_root": "/litepan/output",
            "litepan_raw_output_root": "/litepan/raw",
            "moviepilot_mount_root": "/moviepilot/media",
            "emby_mount_root": "/emby/media",
            "state_directory": state_directory,
        },
        "scoring": {
            "include_weight": 10,
            "language_weight": 5,
            "release_group_weight": 3,
            "resolution_weight": 2,
            "collection_penalty": 4,
        },
        "limits": {
            "network_timeout_seconds": 30,
            "max_read_retries": 3,
            "max_task_pages": 20,
            "max_recursive_depth": 8,
            "max_recursive_files": 1000,
            "max_new_tasks_per_run": 4,
            "max_new_tasks_per_subscription": 2,
            "max_review_batch": 50,
            "max_reconcile_batch": 50,
        },
        "retry": {
            "max_attempts": 2,
            "backoff_seconds": 2,
            "backoff_max_seconds": 5,
            "jitter_seconds": 0.5,
        },
        "runtime": {
            "litepan_task_name": "anime-strm",
            "litepan_task_source_root": f"/cloud/library/{quarter_label}",
            "litepan_task_output_root": f"/litepan/raw/{quarter_label}",
            "litepan_task_db_path": "/test-fixtures/litepan.db",
            "litepan_strm_host_root": "/litepan",
            "emby_library_name": "Anime",
            "emby_library_root": f"/emby/media/{quarter_label}",
            "reconcile_batch": 50,
        },
        "schedule": {"timezone": "Asia/Shanghai"},
    }


def _fixture_state_directory(temporary: str) -> str:
    """Return a semantic absolute config root for a disposable fixture."""

    return f"/test-fixtures/{Path(temporary).name}/state"


class _PortFake:
    """A local-only port fake with the methods required by V5 services."""

    def __init__(self, *_args, **kwargs):
        self.target_directory_id = kwargs.get("target_directory_id", "root")
        self.root_directory_path = kwargs.get("root_directory_path", "")
        self.library_name = "Anime"

    def list_subscriptions(self):
        return []

    def scrape_in_place(self, *_args):
        return {"ok": True}

    def find_task(self, *_args):
        return None

    def submit(self, *_args):
        return "task"

    def list_files(self, *_args):
        return []

    def get_file(self, *_args):
        return None

    def mkdir(self, *_args):
        return {"directory_id": "dir"}

    def get_directory(self, *_args):
        return {"directory_id": "dir"}

    def move_file(self, *_args):
        return {"ok": True}

    def rename_file_no_replace(self, *_args, **_kwargs):
        return {"ok": True}

    def list_tasks(self):
        return []

    def list_runtime_tasks(self):
        return []

    def trigger_event(self, *_args):
        return {"status": 200, "accepted": True}

    def find_library(self, *_args):
        return {"Id": "library", "Locations": ["/emby/media"]}

    def iter_episodes(self, *_args):
        return iter(())

    def find_episode(self, *_args):
        return None

    def refresh_library(self, *_args):
        return {"accepted": True}


class _Files:
    def __init__(self, values=None):
        self.values = dict(values or {})
        self.read_calls = []

    def exists(self, path):
        return path in self.values

    def read(self, path):
        self.read_calls.append(path)
        if path not in self.values:
            raise FileNotFoundError(path)
        return self.values[path]

    def is_symlink(self, _path):
        return False


class _FailingCloud:
    def __init__(self):
        self.find_calls = 0

    def find_task(self, _infohash):
        self.find_calls += 1
        raise TimeoutError("local cloud timeout")


class _FailingLitePan:
    def __init__(self):
        self.trigger_calls = 0

    def list_tasks(self):
        return [{
            "id": "task-1", "name": "anime-strm", "active": True,
            "source_root": "/cloud/library", "output_root": "/litepan/output",
        }]

    def trigger_event(self, *_args):
        self.trigger_calls += 1
        raise TimeoutError("local LitePan timeout")


class _FailingMoviePilot:
    def __init__(self):
        self.scrape_calls = 0

    def scrape_in_place(self, *_args):
        self.scrape_calls += 1
        raise TimeoutError("local MoviePilot timeout")


class _FailingEmby:
    def __init__(self):
        self.library_calls = 0

    def find_library(self, *_args):
        self.library_calls += 1
        raise TimeoutError("local Emby timeout")


class _RefreshFailingEmby:
    def __init__(self):
        self.find_library_calls = 0
        self.find_episode_calls = 0
        self.refresh_calls = 0

    def find_library(self, *_args):
        self.find_library_calls += 1
        return {"Id": "library", "Locations": ["/emby/media"]}

    def find_episode(self, *_args):
        self.find_episode_calls += 1
        return None

    def refresh_library(self, *_args):
        self.refresh_calls += 1
        raise TimeoutError("local Emby refresh timeout")


def _mapper():
    return PathMapper(ServiceRoots(
        cloud_source_root="/cloud/source",
        cloud_library_root="/cloud/library",
        litepan_output_root="/litepan/output",
        moviepilot_mount_root="/moviepilot/media",
        emby_mount_root="/emby/media",
    ))


def _episode(store, status, *, media_id="1",
             strm_path="/litepan/output/Show/Season 1/Show - S01E01.strm"):
    return store.get_or_create_episode({
        "key": EpisodeKey("tmdb", media_id, 1, 1),
        "canonical_work": "Show",
        "title": "Show - S01E01",
        "status": status,
        "cloud_path": "/cloud/library/Show/Season 1/Show - S01E01.mkv",
        "strm_path": strm_path,
    })


class RuntimeIntegrationTests(unittest.TestCase):
    def test_build_runtime_wires_every_v5_stage_and_runner_boundary(self):
        with tempfile.TemporaryDirectory(prefix="task13-runtime-") as temporary:
            raw = _config(_fixture_state_directory(temporary))
            config = run_pipeline.ConfigSchema.from_mapping(raw)
            environment = {
                "MOVIEPILOT_TOKEN": "mp-token",
                "CLOUD_COOKIE": "cloud-cookie",
                "LITEPAN_API_KEY": "litepan-key",
                "EMBY_API_KEY": "emby-key",
                "P115_ROOT_DIRECTORY_ID": "root-id",
            }
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
                "pipeline.real_adapters.MoviePilotClient", _PortFake
            ), mock.patch("pipeline.real_adapters.P115CloudAdapter", _PortFake), mock.patch(
                "pipeline.real_adapters.LitePanClient", _PortFake
            ), mock.patch("pipeline.real_adapters.EmbyClient", _PortFake):
                runtime = run_pipeline.build_runtime(config, writable=True)

            expected = {
                "discovery", "candidate_selector", "download", "organize",
                "strm", "scrape", "library",
            }
            self.assertTrue(expected.issubset(runtime.services))
            self.assertEqual(runtime.services["discovery"].store, None)
            for name in ("download", "strm", "scrape", "library"):
                self.assertEqual(runtime.services[name].retry_max_attempts, config.retry.max_attempts)
                self.assertEqual(runtime.services[name].retry_base_seconds, config.retry.backoff_seconds)
                self.assertEqual(runtime.services[name].retry_max_seconds, config.retry.backoff_max_seconds)
                self.assertEqual(runtime.services[name].retry_jitter_seconds, config.retry.jitter_seconds)
            store = _stage_store(Path(temporary) / "state.db")
            try:
                runner = PipelineRunner(store, services=runtime.services, config=config)
                self.assertIs(runner.discovery, runtime.services["discovery"])
                self.assertTrue(all(runner._service(name) is not None for name in expected))
                episode_id = store.get_or_create_episode({
                    "media_source": "tmdb", "media_id": "1", "season": 1,
                    "episode": 1, "canonical_work": "Show", "status": EpisodeStatus.ORGANIZED,
                })
                pending_id = store.get_or_create_episode({
                    "media_source": "tmdb", "media_id": "2", "season": 1,
                    "episode": 1, "canonical_work": "Show 2", "status": EpisodeStatus.STRM_PENDING,
                })
                summary = runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                self.assertEqual(summary["errors"], 0)
                self.assertEqual(
                    store.get_episode(episode_id)["status"],
                    EpisodeStatus.STRM_PENDING.value,
                )
                self.assertEqual(
                    store.get_episode(pending_id)["status"],
                    EpisodeStatus.STRM_PENDING.value,
                )
                self.assertEqual(summary["handler_counts"].get("strm_pending"), 1)
            finally:
                store.close()

    def test_runner_auto_services_preserve_configured_retry_policy(self):
        with tempfile.TemporaryDirectory(prefix="task13-runner-retry-") as temporary:
            config = run_pipeline.ConfigSchema.from_mapping(
                _config(_fixture_state_directory(temporary))
            )
            store = StateStore(Path(temporary) / "state.db")
            try:
                runner = PipelineRunner(
                    store,
                    cloud=_PortFake(),
                    emby=_PortFake(),
                    mapper=_mapper(),
                    config=config,
                )
                for name in ("download", "library"):
                    service = runner._service(name)
                    self.assertEqual(service.retry_max_attempts, config.retry.max_attempts)
                    self.assertEqual(service.retry_base_seconds, config.retry.backoff_seconds)
                    self.assertEqual(service.retry_max_seconds, config.retry.backoff_max_seconds)
                    self.assertEqual(service.retry_jitter_seconds, config.retry.jitter_seconds)
            finally:
                store.close()

    def test_missing_cloud_runtime_binding_fails_before_cloud_adapter(self):
        with tempfile.TemporaryDirectory(prefix="task13-missing-") as temporary:
            config = run_pipeline.ConfigSchema.from_mapping(
                _config(_fixture_state_directory(temporary))
            )
            environment = {
                "MOVIEPILOT_TOKEN": "mp-token",
                "CLOUD_COOKIE": "cloud-cookie",
                "LITEPAN_API_KEY": "litepan-key",
                "EMBY_API_KEY": "emby-key",
            }
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
                "pipeline.real_adapters.P115CloudAdapter"
            ) as cloud:
                with self.assertRaises(run_pipeline.CliError):
                    run_pipeline.build_runtime(config, writable=True)
                cloud.assert_not_called()

    def test_missing_stage_service_fails_before_state_open(self):
        with tempfile.TemporaryDirectory(prefix="task13-stage-missing-") as temporary:
            config_path = Path(temporary) / "config.json"
            config_path.write_text(
                json.dumps(_config(_fixture_state_directory(temporary))),
                encoding="utf-8",
            )
            runtime = run_pipeline.RuntimeBundle(moviepilot=_PortFake(), services={})
            with mock.patch.object(run_pipeline, "build_runtime", return_value=runtime), mock.patch.object(
                run_pipeline, "_open_writable"
            ) as open_store:
                code = run_pipeline.main(["run", "--config", str(config_path)])
            self.assertEqual(code, 2)
            open_store.assert_not_called()

    def test_cmd_run_rebinds_the_graph_to_v5_state_store(self):
        with tempfile.TemporaryDirectory(prefix="task13-run-") as temporary:
            config_path = Path(temporary) / "config.json"
            state_directory = _fixture_state_directory(temporary)
            config_path.write_text(json.dumps(_config(state_directory)), encoding="utf-8")
            config = run_pipeline.ConfigSchema.from_mapping(_config(state_directory))
            environment = {
                "MOVIEPILOT_TOKEN": "mp-token",
                "CLOUD_COOKIE": "cloud-cookie",
                "LITEPAN_API_KEY": "litepan-key",
                "EMBY_API_KEY": "emby-key",
                "P115_ROOT_DIRECTORY_ID": "root-id",
            }
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
                "pipeline.real_adapters.MoviePilotClient", _PortFake
            ), mock.patch("pipeline.real_adapters.P115CloudAdapter", _PortFake), mock.patch(
                "pipeline.real_adapters.LitePanClient", _PortFake
            ), mock.patch("pipeline.real_adapters.EmbyClient", _PortFake
            ):
                runtime = run_pipeline.build_runtime(config, writable=True)
                with mock.patch.object(run_pipeline, "build_runtime", return_value=runtime):
                    native_state = Path(temporary) / "state" / "state.db"
                    with mock.patch.object(
                        run_pipeline, "_state_path", return_value=str(native_state)
                    ):
                        code = run_pipeline.main(["run", "--config", str(config_path)])
            state_path = Path(temporary) / "state" / "state.db"
            self.assertEqual(code, 0)
            self.assertTrue(state_path.exists())

    def test_configured_retry_policy_controls_timestamp_and_exhaustion_review(self):
        with tempfile.TemporaryDirectory(prefix="task13-retry-") as temporary:
            store = StateStore(Path(temporary) / "state.db")
            try:
                episode_id = store.get_or_create_episode({
                    "media_source": "tmdb", "media_id": "1", "season": 1,
                    "episode": 1, "canonical_work": "Show", "status": EpisodeStatus.DOWNLOAD_PENDING,
                })
                store.add_candidate(episode_id, {"infohash": "a" * 40, "title": "Show"})

                class FailingCloud:
                    def find_task(self, _infohash):
                        raise TimeoutError("local fake timeout")

                service = DownloadService(
                    store,
                    FailingCloud(),
                    budget=SubmissionBudget(1, 1),
                    now=lambda: 100.0,
                    retry_policy=RetryPolicy(
                        max_attempts=2,
                        backoff_seconds=2,
                        backoff_max_seconds=5,
                        jitter_seconds=0.5,
                    ),
                    retry_random=lambda: 1.0,
                )
                first = service.advance(store.get_episode(episode_id))
                self.assertEqual(first.status, EpisodeStatus.DOWNLOAD_PENDING.value)
                self.assertEqual(store.get_episode(episode_id)["next_retry_at"], 102.5)

                row = store.get_episode(episode_id)
                service.now = lambda: 200.0
                second = service.advance(row)
                self.assertEqual(second.status, EpisodeStatus.NEEDS_REVIEW.value)
                review = store.get_review(episode_id)
                self.assertIsNotNone(review)
                self.assertEqual(review["review_reason"], "UNEXPECTED_REMOTE_STATE")
                self.assertIsNone(review["next_retry_at"])
            finally:
                store.close()

    def test_dry_runtime_does_not_construct_mutating_clients(self):
        with tempfile.TemporaryDirectory(prefix="task13-dry-") as temporary:
            config = run_pipeline.ConfigSchema.from_mapping(
                _config(_fixture_state_directory(temporary))
            )
            with mock.patch.dict(os.environ, {"MOVIEPILOT_TOKEN": "mp-token"}, clear=False), mock.patch(
                "pipeline.real_adapters.MoviePilotClient", _PortFake
            ), mock.patch(
                "pipeline.real_adapters.P115CloudAdapter", side_effect=AssertionError("cloud constructed")
            ), mock.patch(
                "pipeline.real_adapters.LitePanClient", side_effect=AssertionError("litepan constructed")
            ), mock.patch(
                "pipeline.real_adapters.EmbyClient", side_effect=AssertionError("emby constructed")
            ):
                runtime = run_pipeline.build_runtime(config, writable=False)
            self.assertEqual(runtime.services, {})
            self.assertFalse(Path(temporary, "state.db").exists())

    def test_compatibility_wrapper_uses_the_direct_v5_entrypoint(self):
        self.assertIs(anime_pipeline.main, run_pipeline.main)
        self.assertIs(anime_pipeline.build_executor, run_pipeline.build_executor)

    def test_runner_retry_exhaustion_persists_review_and_blocks_followup_calls(self):
        cases = []
        policy = RetryPolicy(
            max_attempts=1,
            backoff_seconds=2,
            backoff_max_seconds=5,
            jitter_seconds=0,
        )
        with tempfile.TemporaryDirectory(prefix="task13-exhaustion-") as temporary:
            # Each service uses the real StateStore and service implementation;
            # only the remote boundary is a counting local transport fake.
            store = _stage_store(Path(temporary) / "state.db")
            try:
                episode_id = _episode(store, EpisodeStatus.DOWNLOAD_PENDING)
                store.add_candidate(episode_id, {
                    "infohash": "a" * 40, "title": "Show S01E01",
                })
                cloud = _FailingCloud()
                cases.append((
                    "download",
                    DownloadService(
                        store, cloud, budget=SubmissionBudget(1, 1),
                        retry_policy=policy, now=lambda: 100.0,
                    ),
                    cloud,
                    "find_calls",
                    episode_id,
                ))
                runner = PipelineRunner(
                    store,
                    services={"download": cases[-1][1]},
                    config={"retry": policy},
                    now=lambda: 100.0,
                )
                first = runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                self.assertEqual(first["errors"], 0)
                self.assertEqual(
                    store.get_episode(episode_id)["status"],
                    EpisodeStatus.NEEDS_REVIEW.value,
                )
                self.assertEqual(store.get_episode(episode_id)["review_reason"],
                                 ReviewReason.UNEXPECTED_REMOTE_STATE.value)
                runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                self.assertEqual(cloud.find_calls, 1)
            finally:
                store.close()

        # The remaining stages are independent StateStores so the same
        # max_attempts=1 contract is exercised through each runner handler.
        stage_cases = [
            ("strm", EpisodeStatus.STRM_PENDING, _FailingLitePan(), lambda store, remote: StrmService(
                 store, remote, files=_Files({}), mapper=_mapper(), task_name="anime-strm",
                source_root="/cloud/library", output_root="/litepan/output",
                retry_policy=policy, now=lambda: 100.0,
            ), "trigger_calls"),
            ("scrape", EpisodeStatus.SCRAPE_PENDING, _FailingMoviePilot(), lambda store, remote: MetadataService(
                store, remote, SidecarEvidence(_Files({
                    "/litepan/output/Show/Season 1/Show - S01E01.strm":
                    "https://media.example/show/1",
                })), mapper=_mapper(), retry_policy=policy, now=lambda: 100.0,
            ), "scrape_calls"),
            ("library", EpisodeStatus.LIBRARY_PENDING, _FailingEmby(), lambda store, remote: LibraryService(
                store, remote, _mapper(), library_name="Anime", library_root="/emby/media",
                retry_policy=policy, now=lambda: 100.0,
            ), "library_calls"),
        ]
        for stage, status, remote, factory, call_attr in stage_cases:
            with self.subTest(stage=stage), tempfile.TemporaryDirectory(
                prefix=f"task13-{stage}-exhaustion-"
            ) as temporary:
                store = _stage_store(Path(temporary) / "state.db")
                try:
                    episode_id = _episode(store, status)
                    service = factory(store, remote)
                    runner = PipelineRunner(
                        store,
                        services={stage: service},
                        config={"retry": policy},
                        now=lambda: 100.0,
                    )
                    runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                    row = store.get_episode(episode_id)
                    self.assertEqual(row["status"], EpisodeStatus.NEEDS_REVIEW.value)
                    self.assertEqual(row["review_reason"], ReviewReason.UNEXPECTED_REMOTE_STATE.value)
                    runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                    self.assertEqual(getattr(remote, call_attr), 1)
                finally:
                    store.close()

    def test_runner_never_regresses_review_when_handler_returns_stale_pending(self):
        with tempfile.TemporaryDirectory(prefix="task13-runner-cas-") as temporary:
            store = _stage_store(Path(temporary) / "state.db")
            calls = {"count": 0}
            try:
                episode_id = _episode(store, EpisodeStatus.DOWNLOAD_PENDING)

                class ReviewThenStalePending:
                    def advance(self, row):
                        calls["count"] += 1
                        store.open_review(
                            int(row["id"]),
                            ReviewReason.UNEXPECTED_REMOTE_STATE,
                            {"reason": "handler_reviewed"},
                            resume_status=EpisodeStatus.DOWNLOAD_PENDING.value,
                        )
                        return ServiceResult(
                            EpisodeStatus.DOWNLOAD_PENDING.value,
                            detail={"reason": "stale_handler_result"},
                        )

                runner = PipelineRunner(
                    store,
                    services={"download": ReviewThenStalePending()},
                    now=lambda: 100.0,
                )
                runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                self.assertEqual(
                    store.get_episode(episode_id)["status"],
                    EpisodeStatus.NEEDS_REVIEW.value,
                )
                runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                self.assertEqual(calls["count"], 1)
            finally:
                store.close()

    def test_retry_delay_jitter_is_clamped_by_cap_for_service_and_library_claim(self):
        policy = RetryPolicy(
            max_attempts=3,
            backoff_seconds=5,
            backoff_max_seconds=5,
            jitter_seconds=100,
        )
        with tempfile.TemporaryDirectory(prefix="task13-cap-") as temporary:
            store = StateStore(Path(temporary) / "state.db")
            try:
                episode_id = _episode(store, EpisodeStatus.DOWNLOAD_PENDING)
                store.add_candidate(episode_id, {"infohash": "b" * 40, "title": "Show"})
                service = DownloadService(
                    store, _FailingCloud(), budget=SubmissionBudget(1, 1),
                    retry_policy=policy, retry_random=lambda: 1.0,
                    now=lambda: 100.0,
                )
                service.advance(store.get_episode(episode_id))
                self.assertEqual(store.get_episode(episode_id)["next_retry_at"], 105.0)

                library_id = _episode(store, EpisodeStatus.LIBRARY_PENDING, media_id="2")
                claim = store.claim_library_refresh(
                    library_id, {"reason": "refresh"}, now=200.0, base=5, cap=5,
                    jitter_seconds=100, random_source=lambda: 1.0,
                )
                self.assertEqual(claim["next_retry_at"], 205.0)
            finally:
                store.close()

    def test_library_refresh_exhaustion_is_review_before_next_external_call(self):
        policy = RetryPolicy(
            max_attempts=1,
            backoff_seconds=2,
            backoff_max_seconds=5,
            jitter_seconds=0,
        )
        with tempfile.TemporaryDirectory(prefix="task13-library-refresh-") as temporary:
            store = _stage_store(Path(temporary) / "state.db")
            remote = _RefreshFailingEmby()
            try:
                episode_id = _episode(store, EpisodeStatus.LIBRARY_PENDING)
                service = LibraryService(
                    store, remote, _mapper(), library_name="Anime", library_root="/emby/media",
                    retry_policy=policy, now=lambda: 100.0,
                )
                runner = PipelineRunner(
                    store, services={"library": service}, config={"retry": policy},
                    now=lambda: 100.0,
                )
                runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                self.assertEqual(
                    store.get_episode(episode_id)["status"],
                    EpisodeStatus.NEEDS_REVIEW.value,
                )
                self.assertEqual(remote.refresh_calls, 1)
                runner.run(window={"season_quarter": _current_quarter()}, discover=False)
                self.assertEqual(remote.refresh_calls, 1)
                self.assertEqual(remote.find_library_calls, 1)
            finally:
                store.close()

    def test_runtime_wires_read_only_source_provider_and_discovers_candidates(self):
        class SubscriptionMoviePilot(_PortFake):
            def list_subscriptions(self):
                return [{
                    "id": "sub-1", "name": "Show", "tmdbid": "1", "season": 1,
                    "tmdb_season_air_date": _current_quarter() + "-03",
                }]

        rss = """<?xml version="1.0"?><rss><channel><item>
        <title>[Group] Show S01E01 1080p</title>
        <link>https://source.invalid/show-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa.html</link>
        <enclosure url="https://source.invalid/show.torrent" />
        </item></channel></rss>"""
        with tempfile.TemporaryDirectory(prefix="task13-source-") as temporary:
            config = run_pipeline.ConfigSchema.from_mapping(
                _config(_fixture_state_directory(temporary))
            )
            environment = {
                "MOVIEPILOT_TOKEN": "mp-token",
                "CLOUD_COOKIE": "cloud-cookie",
                "LITEPAN_API_KEY": "litepan-key",
                "EMBY_API_KEY": "emby-key",
                "P115_ROOT_DIRECTORY_ID": "root-id",
            }
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
                "pipeline.real_adapters.MoviePilotClient", SubscriptionMoviePilot
            ), mock.patch("pipeline.real_adapters.P115CloudAdapter", _PortFake), mock.patch(
                "pipeline.real_adapters.LitePanClient", _PortFake
            ), mock.patch("pipeline.real_adapters.EmbyClient", _PortFake), mock.patch(
                "pipeline.real_adapters.KissSubFetcher._fetch", return_value=rss
            ):
                runtime = run_pipeline.build_runtime(config, writable=True)
                self.assertTrue(runtime.providers)
                self.assertIs(runtime.services["discovery"].providers[0], runtime.providers[0])
                store = StateStore(Path(temporary) / "state.db")
                try:
                    runner = PipelineRunner(store, services=runtime.services, config=config)
                    summary = runner.run(window={"season_quarter": _current_quarter()}, discover=True, read_only=False)
                    self.assertEqual(summary["discovery"]["decision_count"], 1)
                    self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 1)
                finally:
                    store.close()

    def test_source_configuration_and_adapter_capability_fail_closed(self):
        with tempfile.TemporaryDirectory(prefix="task13-source-missing-") as temporary:
            raw = _config(_fixture_state_directory(temporary))
            raw.pop("source")
            config = run_pipeline.ConfigSchema.from_mapping(raw)
            environment = {
                "MOVIEPILOT_TOKEN": "mp-token",
                "CLOUD_COOKIE": "cloud-cookie",
                "LITEPAN_API_KEY": "litepan-key",
                "EMBY_API_KEY": "emby-key",
                "P115_ROOT_DIRECTORY_ID": "root-id",
            }
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
                "pipeline.real_adapters.P115CloudAdapter"
            ) as cloud:
                with self.assertRaises(run_pipeline.CliError):
                    run_pipeline.build_runtime(config, writable=True)
                cloud.assert_not_called()

    def test_p115_root_path_is_explicit_and_missing_remote_inventory_blocks_preflight(self):
        with tempfile.TemporaryDirectory(prefix="task13-roots-") as temporary:
            raw = _config(_fixture_state_directory(temporary))
            config = run_pipeline.ConfigSchema.from_mapping(raw)
            environment = {
                "MOVIEPILOT_TOKEN": "mp-token",
                "CLOUD_COOKIE": "cloud-cookie",
                "LITEPAN_API_KEY": "litepan-key",
                "EMBY_API_KEY": "emby-key",
                "P115_ROOT_DIRECTORY_ID": "root-id",
            }
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
                "pipeline.real_adapters.MoviePilotClient", _PortFake
            ), mock.patch("pipeline.real_adapters.P115CloudAdapter", _PortFake), mock.patch(
                "pipeline.real_adapters.LitePanClient", _PortFake
            ), mock.patch("pipeline.real_adapters.EmbyClient", _PortFake):
                runtime = run_pipeline.build_runtime(config, writable=True)
            self.assertEqual(runtime.cloud.root_directory_path, "/cloud")
            self.assertEqual(runtime.services["strm"].source_root,
                             raw["runtime"]["litepan_task_source_root"])

            config_path = Path(temporary) / "config.json"
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
                "run_pipeline._json_output"
            ) as output:
                self.assertEqual(
                    run_pipeline.main(["preflight", "--config", str(config_path)]),
                    2,
                )
            result = output.call_args.args[0]
            self.assertEqual(result["status"], "blocked")
            self.assertIn("runtime.litepan_task_inventory", result["missing_capabilities"])
            self.assertIn("services.cloud.source_inventory", result["missing_capabilities"])
            self.assertIn("runtime.emby_quarter_library", result["missing_capabilities"])

    def test_capability_validation_rejects_missing_later_adapter_before_state_open(self):
        with tempfile.TemporaryDirectory(prefix="task13-capability-") as temporary:
            config = run_pipeline.ConfigSchema.from_mapping(
                _config(_fixture_state_directory(temporary))
            )
            environment = {
                "MOVIEPILOT_TOKEN": "mp-token",
                "CLOUD_COOKIE": "cloud-cookie",
                "LITEPAN_API_KEY": "litepan-key",
                "EMBY_API_KEY": "emby-key",
                "P115_ROOT_DIRECTORY_ID": "root-id",
            }
            with mock.patch.dict(os.environ, environment, clear=False), mock.patch(
                "pipeline.real_adapters.MoviePilotClient", _PortFake
            ), mock.patch("pipeline.real_adapters.P115CloudAdapter", _PortFake), mock.patch(
                "pipeline.real_adapters.LitePanClient", _PortFake
            ), mock.patch("pipeline.real_adapters.EmbyClient", _PortFake):
                runtime = run_pipeline.build_runtime(config, writable=True)
            runtime.services["library"].emby = object()
            with self.assertRaises(run_pipeline.CliError):
                run_pipeline._validate_stage_services(runtime.services)

            config_path = Path(temporary) / "config.json"
            config_path.write_text(
                json.dumps(_config(_fixture_state_directory(temporary))),
                encoding="utf-8",
            )
            with mock.patch.object(run_pipeline, "build_runtime", return_value=runtime), mock.patch.object(
                run_pipeline, "_open_writable"
            ) as open_store:
                code = run_pipeline.main(["run", "--config", str(config_path)])
            self.assertEqual(code, 2)
            open_store.assert_not_called()

    def test_underlying_p115_missing_capabilities_fail_before_state_or_mutations(self):
        from pipeline.real_adapters import P115CloudAdapter
        config = run_pipeline.ConfigSchema.from_mapping(
            _config("/test-fixtures/capability-test/state")
        )
        for missing in ("all", "fs_files", "fs_mkdir", "fs_move",
                        "clouddownload_task_add_url", "clouddownload_task_list"):
            with self.subTest(missing=missing):
                client = object() if missing == "all" else mock.Mock(spec=[
                    name for name in ("fs_files", "fs_mkdir", "fs_move", "fs_rename_no_replace",
                                      "clouddownload_task_add_url", "clouddownload_task_list")
                    if name != missing
                ])
                cloud = P115CloudAdapter(None, root_directory_id="root", target_directory_id="source", client=client,
                                         root_directory_path="/cloud")
                services = run_pipeline._build_stage_services(
                    config, moviepilot=_PortFake(), cloud=cloud, litepan=_PortFake(),
                    emby=_PortFake(), mapper=_mapper(), providers=[mock.Mock()], secrets=(),
                )
                with self.assertRaises(run_pipeline.CliError):
                    run_pipeline._validate_stage_services(services)
                runtime = run_pipeline.RuntimeBundle(moviepilot=_PortFake(), services=services)
                with tempfile.TemporaryDirectory(prefix="task13-client-gate-") as directory:
                    config_path = Path(directory, "config.json")
                    config_path.write_text(
                        json.dumps(_config("/test-fixtures/capability-test/state")),
                        encoding="utf-8",
                    )
                    with mock.patch.object(run_pipeline, "build_runtime", return_value=runtime), mock.patch.object(
                        run_pipeline, "_open_writable"
                    ) as writable:
                        self.assertEqual(run_pipeline.main(["run", "--config", str(config_path)]), 2)
                    writable.assert_not_called()
                if missing != "all":
                    self.assertEqual(client.mock_calls, [])


if __name__ == "__main__":
    unittest.main()
