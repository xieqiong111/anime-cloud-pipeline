#!/usr/bin/env python3
"""The single V5 command-line entrypoint.

The command boundary is deliberately small. Configuration is parsed and
validated before a client, writable SQLite store, or external operation is
created. Dry-run, status, review listing, and audit use read-only paths; only
the run, migrate, and review-resolution commands open writable state.
"""
from __future__ import annotations

import argparse
from dataclasses import dataclass, field
from pathlib import Path
import datetime as dt
import hashlib
import json
import os
import re
import sqlite3
import stat
import sys
import tempfile
from typing import Any, Mapping, Sequence
from zoneinfo import ZoneInfo

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)

from pipeline.config import (  # noqa: E402
    ConfigError,
    ConfigSchema,
    migrate_legacy_config,
)
from pipeline.anime_guard import DiscoveryGuardError  # noqa: E402
from pipeline.discovery import DiscoveryService  # noqa: E402
from pipeline.diagnostics import ReadOnlyDeadline, read_only_command, stage  # noqa: E402
from pipeline.executor import PipelineRunner  # noqa: E402
from pipeline.migrations import LATEST_SCHEMA_VERSION, MigrationRunner  # noqa: E402
from pipeline.paths import PathMapper  # noqa: E402
from pipeline.security import sanitize_text  # noqa: E402
from pipeline.services import ReviewService  # noqa: E402
from pipeline.state import StateStore  # noqa: E402
from pipeline.throttle import CloudThrottleState  # noqa: E402


class CliError(ValueError):
    """A deterministic operator-facing input/configuration error."""


class _SafeArgumentParser(argparse.ArgumentParser):
    """Argument parser whose diagnostics never include raw argv values."""

    def error(self, _message: str) -> None:
        raise CliError("invalid command-line arguments")


def load_config(path: str) -> ConfigSchema:
    """Compatibility loader returning the strictly validated V5 schema."""

    return _config_mapping(path)[1]


@dataclass
class RuntimeBundle:
    """Adapters and ports used by discovery and the bounded runner."""

    moviepilot: Any
    providers: tuple[Any, ...] = ()
    services: dict[str, Any] = field(default_factory=dict)
    litepan: Any = None
    emby: Any = None
    mapper: Any = None
    candidate_selector: Any = None
    cloud: Any = None


def build_executor(config: Mapping[str, Any]) -> Any:
    """Compatibility guard for callers that used the pre-V5 helper.

    The old executor graph is intentionally not constructed anymore.  Keep
    its one safety validation at the boundary so older diagnostics fail closed
    before a P115 client could be created, then direct operators to ``run``.
    """

    p115 = config.get("p115") if isinstance(config, Mapping) else None
    p115 = p115 if isinstance(p115, Mapping) else {}
    root_id = p115.get("root_directory_id") or p115.get("target_directory_id")
    credential = p115.get("credential_file") or p115.get("credential_env")
    if credential and root_id and not (p115.get("root_directory_path") or p115.get("root_path")):
        raise ValueError("p115.root_directory_path is required with root_directory_id")
    raise CliError("legacy executor removed; invoke the V5 run command")


class _ReadOnlyStore:
    """Small read-only repository projection for CLI reports.

    ``StateStore`` intentionally owns migrations and enables WAL. Using it for
    status/review listing would therefore turn a read command into a write.
    """

    def __init__(self, path: str):
        absolute = os.path.abspath(path)
        if not os.path.isfile(absolute):
            raise FileNotFoundError(absolute)
        self.conn = _readonly_connection(absolute)
        self.conn.row_factory = sqlite3.Row

    def close(self) -> None:
        self.conn.close()

    def _rows(self, query: str, parameters: Sequence[Any] = ()) -> list[dict[str, Any]]:
        try:
            return [dict(row) for row in self.conn.execute(query, tuple(parameters)).fetchall()]
        except sqlite3.OperationalError:
            return []

    def all_episodes(self, stage: str | None = None) -> list[dict[str, Any]]:
        if stage:
            return self._rows("SELECT * FROM episodes_v5 WHERE status=? ORDER BY id", (stage,))
        return self._rows("SELECT * FROM episodes_v5 ORDER BY id")

    def list_open_reviews(self) -> list[dict[str, Any]]:
        return self._rows(
            """SELECT * FROM episodes_v5
               WHERE status='needs_review' AND review_resolved_at IS NULL
               ORDER BY COALESCE(review_created_at, created_at), id"""
        )

    def get_review(self, review_id: int) -> dict[str, Any] | None:
        rows = self._rows(
            """SELECT * FROM episodes_v5
               WHERE id=? AND status='needs_review' AND review_resolved_at IS NULL""",
            (review_id,),
        )
        return rows[0] if rows else None

    def audit(self) -> dict[str, Any]:
        counts: dict[str, int] = {}
        for row in self._rows("SELECT status, COUNT(*) AS count FROM episodes_v5 GROUP BY status"):
            counts[str(row.get("status") or "unknown")] = int(row.get("count") or 0)
        history = self._rows("SELECT COUNT(*) AS count FROM stage_history")
        notifications = self._rows("SELECT COUNT(*) AS count FROM notifications")
        open_reviews = self._rows(
            "SELECT COUNT(*) AS count FROM episodes_v5 WHERE status='needs_review' AND review_resolved_at IS NULL"
        )
        return {
            "schema_version": self._schema_version(),
            "episode_counts": counts,
            "open_reviews": int((open_reviews[0] if open_reviews else {}).get("count") or 0),
            "history_rows": int((history[0] if history else {}).get("count") or 0),
            "notification_rows": int((notifications[0] if notifications else {}).get("count") or 0),
        }

    def _schema_version(self) -> int:
        try:
            return int(self.conn.execute("PRAGMA user_version").fetchone()[0])
        except (sqlite3.Error, TypeError, ValueError):
            return 0


def _config_mapping(path: str) -> tuple[dict[str, Any], ConfigSchema]:
    """Read and strictly validate a V5 config without side effects."""

    try:
        with open(path, encoding="utf-8") as handle:
            raw = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise CliError(f"config could not be read ({type(exc).__name__})") from None
    try:
        config = ConfigSchema.from_mapping(raw, os.environ, sys.platform)
    except ConfigError as exc:
        raise CliError(_safe_config_error(exc)) from None
    return raw, config


def _safe_config_error(exc: BaseException) -> str:
    text = sanitize_text(str(exc))
    text = re.sub(r"https?://[^\s,;]+", "<url>", text, flags=re.I)
    return text[:600]


def _state_path(config: ConfigSchema) -> str:
    return os.path.join(os.path.abspath(config.paths.state_directory), "state.db")


def _readonly_connection(path: str) -> sqlite3.Connection:
    """Read a stable private copy, including committed WAL, without source writes.

    SQLite can create or update SHM even through mode=ro. Never attach SQLite
    to the production files: collect two equal images, recover the copy in a
    secure temporary directory, then back it up into a query-only connection.
    A changing source fails closed after three bounded attempts.
    """
    source = Path(path).absolute()

    def collect() -> dict[str, bytes]:
        images = {"state.db": source.read_bytes()}
        for suffix in ("-wal", "-shm"):
            try:
                images["state.db" + suffix] = Path(str(source) + suffix).read_bytes()
            except FileNotFoundError:
                pass
        return images

    for _attempt in range(3):
        images = collect()
        if images != collect():
            continue
        with tempfile.TemporaryDirectory(prefix="pipeline-readonly-") as directory:
            for name, data in images.items():
                Path(directory, name).write_bytes(data)
            snapshot = sqlite3.connect(":memory:", check_same_thread=False)
            copied = sqlite3.connect(str(Path(directory, "state.db")))
            try:
                copied.backup(snapshot)
                snapshot.execute("PRAGMA query_only=ON")
            except Exception:
                snapshot.close()
                raise
            finally:
                copied.close()
        return snapshot
    raise CliError("state changed while taking read-only snapshot; retry when writes are idle")


def _secret_values(config: ConfigSchema, *, required: bool = False) -> tuple[str, ...]:
    values: list[str] = []
    for service in (
        config.services.moviepilot,
        config.services.cloud,
        config.services.litepan,
        config.services.emby,
    ):
        reference = service.secret_reference
        value = os.environ.get(reference or "", "")
        if required and not value:
            raise CliError(f"services.{service.name}.secret_env is not available")
        if value:
            values.append(value)
    return tuple(values)


def _env_enabled(name: str) -> bool:
    value = os.environ.get(name, "")
    return str(value).strip().casefold() in {"1", "true", "yes", "on", "enforce"}


def _raw_flag(raw: Any, *path: str) -> bool:
    node: Any = raw
    for key in path:
        if not isinstance(node, Mapping):
            return False
        node = node.get(key)
    return node in (True, 1, "1", "true", "on", "yes", "enforce")


def build_discovery_guard(raw_config: Any = None) -> Any:
    """Return a configured safety guard, or ``None`` when it is disabled.

    The guard is enabled by ``V5_ANIME_GUARD=1`` (set by the reviewed V5
    deployment wrapper) or by an explicit ``discovery.anime_guard.enabled``
    config flag.  It stays disabled by default so the historical full-discovery
    behaviour and the shipped unit-suite are unchanged until an operator turns
    it on.
    """

    if not _env_enabled("V5_ANIME_GUARD") and not _raw_flag(
        raw_config, "discovery", "anime_guard", "enabled"
    ):
        return None
    from pipeline.anime_guard import DiscoveryGuard

    return DiscoveryGuard()


def _service_secret(service: Any) -> str:
    reference = service.secret_reference
    value = os.environ.get(reference or "", "")
    if not value:
        raise CliError(f"services.{service.name}.secret_env is not available")
    return value


def build_runtime(
    config: ConfigSchema,
    *,
    writable: bool = False,
    retry_random: Any = None,
) -> RuntimeBundle:
    """Construct adapters after strict config validation.

    Dry-run needs only the read-only MoviePilot subscription port. Write mode
    constructs every V5 adapter and stage service, but does not open SQLite;
    ``PipelineRunner`` binds the returned service objects to the store after
    the command has passed this boundary.  The 115 root identity is supplied
    through a deployment environment reference rather than guessed from a
    path or copied into the validated configuration.
    """

    from pipeline.real_adapters import (
        EmbyClient,
        KissSubFetcher,
        KissSubSourceProvider,
        LitePanClient,
        MoviePilotClient,
        P115CloudAdapter,
    )

    providers = _build_source_providers(config, KissSubFetcher, KissSubSourceProvider)
    moviepilot_cfg = config.services.moviepilot
    moviepilot = MoviePilotClient(
        moviepilot_cfg.base_url,
        token=_service_secret(moviepilot_cfg),
        auth_mode=moviepilot_cfg.auth_mode,
        runtime_config=config,
    )
    if not writable:
        return RuntimeBundle(moviepilot=moviepilot, providers=providers)

    cloud_cfg = config.services.cloud
    litepan_cfg = config.services.litepan
    emby_cfg = config.services.emby
    root_id = _cloud_root_directory_id()
    root_path = _cloud_root_directory_path(config)
    _validate_cloud_remote_roots(config, root_path)
    cloud = P115CloudAdapter(
        None,
        root_directory_id=root_id,
        credential=_service_secret(cloud_cfg),
        runtime_config=config,
        root_directory_path=root_path,
    )
    validate_cloud = getattr(cloud, "validate_runtime_capabilities", None)
    if callable(validate_cloud):
        try:
            validate_cloud()
        except ValueError as exc:
            raise CliError(str(exc)) from None
    resolve_source = getattr(cloud, "resolve_source_directory", None)
    if callable(resolve_source):
        try:
            resolve_source(config.paths.cloud_source_root)
        except Exception:
            # Directory-chain proof is a pre-mutation gate.  Do not construct
            # a writable state graph when the source target is missing,
            # ambiguous, or backed by an incomplete remote listing.
            raise CliError(
                "services.cloud: cloud_source_root could not be verified"
            ) from None
    elif not getattr(cloud, "target_directory_id", None):
        raise CliError("services.cloud: source directory capability is unavailable")
    litepan = LitePanClient(
        litepan_cfg.base_url,
        api_key=_service_secret(litepan_cfg),
        task_db_path=config.runtime.litepan_task_db_path,
        strm_host_root=config.runtime.litepan_strm_host_root,
        runtime_config=config,
    )
    emby = EmbyClient(
        emby_cfg.base_url,
        api_key=_service_secret(emby_cfg),
        runtime_config=config,
    )
    mapper = PathMapper(config.paths)
    services = _build_stage_services(
        config,
        moviepilot=moviepilot,
        cloud=cloud,
        litepan=litepan,
        emby=emby,
        mapper=mapper,
        providers=providers,
        secrets=_secret_values(config),
        retry_random=retry_random,
    )
    _validate_stage_services(services)
    return RuntimeBundle(
        moviepilot=moviepilot,
        providers=providers,
        services=services,
        cloud=cloud,
        litepan=litepan,
        emby=emby,
        mapper=mapper,
        candidate_selector=services["candidate_selector"],
    )


def _cloud_root_directory_id() -> str:
    """Return the deployment-bound 115 root identity without persisting it."""

    for name in ("P115_ROOT_DIRECTORY_ID", "P115_ROOT_ID"):
        value = os.environ.get(name, "").strip()
        if value:
            return value
    raise CliError(
        "services.cloud: P115_ROOT_DIRECTORY_ID is required for write mode"
    )


def _cloud_root_directory_path(config: ConfigSchema) -> str:
    configured = getattr(config.services.cloud, "root_directory_path", None)
    for value in (configured, os.environ.get("P115_ROOT_DIRECTORY_PATH"), os.environ.get("P115_ROOT_PATH")):
        if isinstance(value, str) and value.strip():
            path = value.strip()
            if not path.startswith("/") or path.startswith("//") or "\\" in path:
                raise CliError("services.cloud.root_directory_path must be an absolute POSIX path")
            if any(part in {"", ".", ".."} for part in path.split("/")[1:]):
                raise CliError("services.cloud.root_directory_path contains unsafe segments")
            return path.rstrip("/") or "/"
    raise CliError(
        "services.cloud.root_directory_path or P115_ROOT_DIRECTORY_PATH is required for write mode"
    )


def _remote_path_contained(root: str, child: str) -> bool:
    root_value = str(root or "").rstrip("/") or "/"
    child_value = str(child or "").rstrip("/") or "/"
    if root_value == "/":
        return child_value.startswith("/")
    return child_value == root_value or child_value.startswith(root_value + "/")


def _validate_cloud_remote_roots(config: ConfigSchema, root_path: str) -> None:
    for field, value in (
        ("paths.cloud_source_root", config.paths.cloud_source_root),
        ("paths.cloud_library_root", config.paths.cloud_library_root),
    ):
        if not _remote_path_contained(root_path, value):
            raise CliError(
                f"{field} must be contained by services.cloud.root_directory_path"
            )


def _static_capability_descriptors() -> tuple[tuple[str, Any, tuple[str, ...]], ...]:
    """Return class-level contracts used by preflight without client creation."""

    from pipeline.candidates import CandidateSelector
    from pipeline.discovery import DiscoveryService
    from pipeline.organizer import Organizer
    from pipeline.real_adapters import (
        EmbyClient,
        KissSubSourceProvider,
        LitePanClient,
        MoviePilotClient,
        P115CloudAdapter,
    )
    from pipeline.services import DownloadService, LibraryService, MetadataService, StrmService

    return (
        ("source.provider", KissSubSourceProvider, ("discover",)),
        ("discovery", DiscoveryService, ("prepare",)),
        ("discovery.moviepilot", MoviePilotClient, ("list_subscriptions",)),
        ("candidate_selector", CandidateSelector, ("select",)),
        ("download", DownloadService, ("advance", "poll")),
        ("download.cloud", P115CloudAdapter, ("find_task", "submit", "list_files")),
        ("cloud.root", P115CloudAdapter, ("resolve_source_directory", "find_file_by_identity")),
        ("organize", Organizer, ("reconcile_or_apply",)),
        ("organize.cloud", P115CloudAdapter, ("get_file", "mkdir", "get_directory", "move_file")),
        ("strm", StrmService, ("advance",)),
        ("strm.litepan", LitePanClient, ("trigger_event", "list_tasks")),
        ("scrape", MetadataService, ("advance",)),
        ("scrape.moviepilot", MoviePilotClient, ("scrape_in_place",)),
        ("library", LibraryService, ("advance",)),
        ("library.emby", EmbyClient, ("find_library", "find_episode", "refresh_library")),
    )


def _static_capability_missing() -> list[str]:
    missing: list[str] = []
    for label, owner, methods in _static_capability_descriptors():
        absent = [method for method in methods if not callable(getattr(owner, method, None))]
        if absent:
            missing.append(f"{label}.{','.join(absent)}")
    return missing


def _build_source_providers(config: ConfigSchema, fetcher_type: Any, provider_type: Any) -> tuple[Any, ...]:
    source = getattr(config, "source", None)
    if source is None:
        raise CliError("source: explicit read-only source provider configuration is required")
    if str(source.kind).casefold() != "kisssub":
        raise CliError(f"source.kind {source.kind!r} is not supported by the V5 adapter")
    fetcher = fetcher_type(
        source.base_url,
        runtime_config=config,
        retry_policy=config.retry,
        timeout=config.limits.network_timeout_seconds,
    )
    if not callable(getattr(fetcher, "fetch_feed", None)) or not callable(
        getattr(fetcher, "feed_for_keyword", None)
    ):
        raise CliError("source provider lacks read-only feed methods")
    provider = provider_type(fetcher)
    if not callable(getattr(provider, "discover", None)):
        raise CliError("source provider lacks discover capability")
    return (provider,)


def _build_stage_services(
    config: ConfigSchema,
    *,
    moviepilot: Any,
    cloud: Any,
    litepan: Any,
    emby: Any,
    mapper: Any,
    providers: tuple[Any, ...] = (),
    store: Any = None,
    secrets: tuple[str, ...] = (),
    retry_random: Any = None,
) -> dict[str, Any]:
    """Create the concrete V5 service graph at one validated boundary."""

    from pipeline.media_inventory import prove_submission_inventory
    from pipeline.candidates import CandidateSelector, SubmissionBudget
    from pipeline.discovery import DiscoveryService
    from pipeline.evidence import SidecarEvidence
    from pipeline.organizer import Organizer
    from pipeline.services import (
        DownloadService,
        LibraryService,
        MetadataService,
        StrmService,
    )

    limits = config.limits
    budget = SubmissionBudget(limits=limits)
    retry = config.retry
    # One account-level throttle state per run: every 115-reading stage shares
    # the cooldown, and the budget window rotates with the cron period so a
    # wide backlog cannot starve its tail.
    throttle_state = CloudThrottleState()
    rotation_seed = int(dt.datetime.now(dt.timezone.utc).timestamp()) // 600
    selector = CandidateSelector(store, scoring=config.scoring)
    target_directory = str(
        getattr(cloud, "target_directory_id", "") or _cloud_root_directory_id()
    )
    raw_output_root = config.paths.litepan_raw_output_root
    if callable(getattr(cloud, "supports_isolated_move", None)) and cloud.supports_isolated_move():
        if not raw_output_root:
            raise ValueError(
                "paths.litepan_raw_output_root is required for isolated 115 organization"
            )
    raw_output_root = raw_output_root or config.paths.litepan_output_root
    task_output_root = config.runtime.litepan_task_output_root or raw_output_root
    files = config.paths.litepan_output_root
    return {
        "discovery": DiscoveryService(
            store,
            moviepilot,
            providers,
            config=config,
            secrets=secrets,
        ),
        "candidate_selector": selector,
        "download": DownloadService(
            store,
            cloud,
            budget=budget,
            target_directory=target_directory,
            secrets=secrets,
            retry_policy=retry,
            retry_random=retry_random,
            cloud_check_budget=config.limits.max_cloud_checks_per_run,
            throttle_state=throttle_state,
            rotation_seed=rotation_seed,
            pre_submit_inventory=lambda row: prove_submission_inventory(
                row, config=config, cloud=cloud, emby=emby, store=store,
            ),
        ),
        "organize": Organizer(
            store,
            cloud,
            mapper,
            secrets=secrets,
            throttle_state=throttle_state,
            jitter_seconds=getattr(retry, "jitter_seconds", 0.0) or 0.0,
        ),
        "strm": StrmService(
            store,
            litepan,
            files=files,
            mapper=mapper,
            task_name=config.runtime.litepan_task_name,
            source_root=config.runtime.litepan_task_source_root or config.paths.cloud_library_root,
            output_root=task_output_root,
            raw_output_root=raw_output_root,
            event=config.runtime.litepan_event_name,
            source=config.runtime.litepan_event_source,
            secrets=secrets,
            retry_policy=retry,
            retry_random=retry_random,
        ),
        "scrape": MetadataService(
            store,
            moviepilot,
            SidecarEvidence(files, secrets=secrets),
            mapper=mapper,
            media_source=config.services.moviepilot.media_source,
            secrets=secrets,
            retry_policy=retry,
            retry_random=retry_random,
        ),
        "library": LibraryService(
            store,
            emby,
            mapper,
            library_name=config.runtime.emby_library_name,
            library_root=config.runtime.emby_library_root or config.paths.emby_mount_root,
            runtime_config=config,
            secrets=secrets,
            retry_policy=retry,
            retry_random=retry_random,
        ),
    }


def _validate_stage_services(services: Mapping[str, Any]) -> None:
    """Fail closed before a runner can persist a partial stage transition."""

    if not isinstance(services, Mapping):
        raise CliError("runtime V5 services unavailable before mutation")
    required = {
        "candidate_selector": ("select", "select_episode"),
        "download": ("advance", "poll"),
        "organize": ("reconcile_or_apply", "advance"),
        "strm": ("advance",),
        "scrape": ("advance",),
        "library": ("advance",),
        "discovery": ("prepare",),
    }
    missing: list[str] = []
    alternatives = {"candidate_selector", "organize"}
    for name, methods in required.items():
        service = services.get(name)
        if service is None:
            missing.append(f"{name} ({' or '.join(methods)})")
            continue
        if name in alternatives:
            if not any(callable(getattr(service, method, None)) for method in methods):
                missing.append(f"{name} ({' or '.join(methods)})")
        else:
            absent = [method for method in methods if not callable(getattr(service, method, None))]
            for method in absent:
                missing.append(f"{name}.{method}")

    def require_methods(label: str, adapter: Any, methods: tuple[str, ...]) -> None:
        absent = [method for method in methods if not callable(getattr(adapter, method, None))]
        if absent:
            missing.append(f"{label}.{','.join(absent)}")

    discovery = services.get("discovery")
    if discovery is not None:
        require_methods("discovery.moviepilot", getattr(discovery, "moviepilot", None), ("list_subscriptions",))
        providers = tuple(getattr(discovery, "providers", ()) or ())
        if not providers:
            missing.append("discovery.providers")
        for index, provider in enumerate(providers):
            require_methods(f"discovery.providers[{index}]", provider, ("discover",))

    download = services.get("download")
    if download is not None:
        cloud = getattr(download, "cloud", None)
        require_methods("download.cloud", cloud, ("find_task", "submit", "list_files"))
        if hasattr(cloud, "common_root_directory_id"):
            require_methods("download.cloud", cloud, ("resolve_source_directory",))
        validate_cloud = getattr(cloud, "validate_runtime_capabilities", None)
        if callable(validate_cloud):
            try:
                validate_cloud()
            except ValueError as exc:
                missing.append(str(exc))

    organize = services.get("organize")
    if organize is not None:
        require_methods(
            "organize.cloud",
            getattr(organize, "cloud", None),
            ("get_file", "mkdir", "get_directory", "move_file"),
        )
        cloud = getattr(organize, "cloud", None)
        validate_cloud = getattr(cloud, "validate_runtime_capabilities", None)
        if callable(validate_cloud):
            try:
                validate_cloud()
            except ValueError as exc:
                missing.append(str(exc))
        if not callable(getattr(cloud, "rename_file_no_replace", None)) and not callable(
            getattr(cloud, "rename_file_if_identity", None)
        ):
            missing.append("organize.cloud.rename_file_no_replace|rename_file_if_identity")
        if getattr(organize, "mapper", None) is None:
            missing.append("organize.mapper")

    strm = services.get("strm")
    if strm is not None:
        require_methods("strm.litepan", getattr(strm, "litepan", None), ("trigger_event",))
        litepan = getattr(strm, "litepan", None)
        if not callable(getattr(litepan, "list_runtime_tasks", None)) and not callable(
            getattr(litepan, "list_tasks", None)
        ):
            missing.append("strm.litepan.list_runtime_tasks|list_tasks")
        if getattr(strm, "mapper", None) is None:
            missing.append("strm.mapper")
        files = getattr(strm, "files", None)
        if not isinstance(files, (str, bytes, os.PathLike)):
            require_methods("strm.files", files, ("exists", "read", "is_symlink"))

    scrape = services.get("scrape")
    if scrape is not None:
        require_methods("scrape.moviepilot", getattr(scrape, "moviepilot", None), ("scrape_in_place",))
        if getattr(scrape, "mapper", None) is None:
            missing.append("scrape.mapper")
        evidence = getattr(scrape, "evidence", None)
        files = getattr(evidence, "files", None)
        if files is not None and not isinstance(files, (str, bytes, os.PathLike)):
            require_methods("scrape.evidence.files", files, ("exists", "read", "is_symlink"))

    library = services.get("library")
    if library is not None:
        require_methods(
            "library.emby",
            getattr(library, "emby", None),
            ("find_library", "find_episode", "refresh_library"),
        )
        if getattr(library, "mapper", None) is None:
            missing.append("library.mapper")
    if missing:
        raise CliError(
            "runtime V5 capabilities unavailable before mutation: " + ", ".join(missing)
        )


def _open_readonly(config: ConfigSchema) -> _ReadOnlyStore | None:
    path = _state_path(config)
    if not os.path.exists(path):
        return None
    try:
        return _ReadOnlyStore(path)
    except (OSError, sqlite3.Error):
        raise CliError("state database is not readable") from None


def _open_writable(config: ConfigSchema) -> StateStore:
    path = _state_path(config)
    try:
        os.makedirs(os.path.dirname(path), exist_ok=True)
        return StateStore(path)
    except (OSError, sqlite3.Error) as exc:
        raise CliError(f"state database could not be opened ({type(exc).__name__})") from None


def _close_store(store: Any) -> None:
    if store is None:
        return
    close = getattr(store, "close", None)
    if callable(close):
        close()
    else:
        connection = getattr(store, "conn", None)
        if connection is not None:
            connection.close()


def _safe_prefix(value: Any, length: int = 8) -> str:
    text = str(value or "")
    return text[:length] if text else "-"


def _json_output(value: Mapping[str, Any]) -> None:
    print(json.dumps(value, ensure_ascii=False, sort_keys=True, separators=(",", ":")))


def _summary_projection(summary: Mapping[str, Any]) -> dict[str, Any]:
    discovery = summary.get("discovery")
    discovery_summary: dict[str, Any] = {}
    if isinstance(discovery, Mapping):
        for key in ("subscription_count", "decision_count", "error_count", "excluded_count"):
            if key in discovery:
                try:
                    discovery_summary[key] = int(discovery[key])
                except (TypeError, ValueError):
                    discovery_summary[key] = 0
    output: dict[str, Any] = {}
    for key in (
        "discovered", "advanced", "submitted", "new_submissions", "reconciled",
        "polled_not_ready", "cloud_checks", "cloud_check_deferred",
        "skipped", "skipped_review", "skipped_terminal", "skipped_retry",
        "skipped_budget", "skipped_selection", "skipped_read_only",
        "skipped_state_changed", "skipped_pacing", "skipped_quarter", "skipped_scope", "errors",
    ):
        if key in summary:
            try:
                output[key] = int(summary[key])
            except (TypeError, ValueError):
                output[key] = 0
    if "skipped_lock" in summary:
        output["skipped_lock"] = bool(summary["skipped_lock"])
    if discovery_summary:
        output["discovery"] = discovery_summary
    if summary.get("stop_reason"):
        reason = summary["stop_reason"]
        output["stop_reason"] = reason if reason in {
            "pre_submit_inventory_present", "pre_submit_inventory_unknown",
            "pre_submit_inventory_error",
        } else "media_stage_blocked"
    return output


def _previous_day(timezone: str) -> dt.date:
    try:
        zone = ZoneInfo(timezone)
    except Exception:
        raise CliError("schedule.timezone is not a valid IANA timezone") from None
    return dt.datetime.now(dt.timezone.utc).astimezone(zone).date() - dt.timedelta(days=1)


def _parse_backfill(value: str | None) -> tuple[str, int, int, int] | None:
    if not value:
        return None
    match = re.fullmatch(r"([^:]+):(\d+):(\d+)-(\d+)", value)
    if not match:
        raise CliError("--backfill must use MEDIA_ID:SEASON:START-END")
    media_id, season, start, end = match.groups()
    start_i, end_i = int(start), int(end)
    if start_i > end_i:
        raise CliError("--backfill START must not exceed END")
    return media_id, int(season), start_i, end_i


def _window(args: argparse.Namespace, config: ConfigSchema) -> dict[str, Any]:
    today = dt.datetime.now(dt.timezone.utc).astimezone(
        ZoneInfo(config.schedule.timezone)
    ).date()
    quarter_month = ((today.month - 1) // 3) * 3 + 1
    window: dict[str, Any] = {"season_quarter": f"{today.year:04d}-{quarter_month:02d}"}
    if getattr(args, "previous_day", False):
        window["published_date"] = _previous_day(config.schedule.timezone).isoformat()
        window["timezone"] = config.schedule.timezone
    backfill = _parse_backfill(getattr(args, "backfill", None))
    if backfill is not None:
        media_id, season, start, end = backfill
        window["backfill"] = {
            "media_id": media_id,
            "season": season,
            "start": start,
            "end": end,
        }
    return window


def _quarter_resource_mismatches(config: ConfigSchema, window: Mapping[str, Any]) -> list[str]:
    """Reject a broad scan or a stale quarter mapping before a writable run."""

    label = str(window["season_quarter"]) + "月新番"
    expected_cloud = config.paths.cloud_library_root.rstrip("/") + "/" + label
    expected_emby = config.paths.emby_mount_root.rstrip("/") + "/" + label
    mismatches = []
    if config.runtime.litepan_task_source_root != expected_cloud:
        mismatches.append("runtime.litepan_task_source_root")
    if config.runtime.emby_library_root != expected_emby:
        mismatches.append("runtime.emby_library_root")
    return mismatches


def _capability_reason(exc: Exception) -> str:
    """Classify without printing remote exception text or request data."""
    from pipeline.adapters import RemoteDirectoryMissing, RemoteInventoryIncomplete, RemoteRateLimited
    from pipeline.http import HttpError

    if "115 authentication expired" in str(exc):
        return "authentication_expired"
    if isinstance(exc, RemoteDirectoryMissing):
        return "not_found_in_complete_inventory"
    if isinstance(exc, RemoteRateLimited):
        return "remote_rate_limited_or_unavailable"
    if isinstance(exc, RemoteInventoryIncomplete):
        return "inventory_unverified"
    if isinstance(exc, HttpError) and exc.status in {401, 403}:
        return "authentication_rejected"
    if isinstance(exc, (TimeoutError, ConnectionError, OSError, HttpError)):
        return "query_failed"
    return "adapter_error"


def cmd_preflight(args: argparse.Namespace) -> int:
    _raw, config = _config_mapping(args.config)
    missing = []
    for service in (
        config.services.moviepilot,
        config.services.cloud,
        config.services.litepan,
        config.services.emby,
    ):
        if not os.environ.get(service.secret_reference or ""):
            missing.append(service.name)
    missing_capabilities: list[str] = []
    diagnostics: list[dict[str, str]] = []
    window = _window(args, config)
    for capability in _quarter_resource_mismatches(config, window):
        missing_capabilities.append(capability)
        value = getattr(config.runtime, capability.split(".", 1)[1])
        diagnostics.append({"capability": capability, "stage": "quarter_mapping",
                            "reason": "quarter_mapping_mismatch" if value else "not_configured",
                            "expected_quarter": str(window["season_quarter"])})
    if not any(os.environ.get(name, "").strip() for name in ("P115_ROOT_DIRECTORY_ID", "P115_ROOT_ID")):
        missing_capabilities.append("services.cloud.P115_ROOT_DIRECTORY_ID")
    if not config.paths.litepan_raw_output_root:
        missing_capabilities.append("paths.litepan_raw_output_root")
    if not config.runtime.litepan_task_db_path:
        missing_capabilities.append("runtime.litepan_task_db_path")
    if not config.runtime.litepan_strm_host_root:
        missing_capabilities.append("runtime.litepan_strm_host_root")
    if config.runtime.litepan_task_db_path and config.runtime.litepan_strm_host_root:
        try:
            from pipeline.real_adapters import LitePanClient
            from pipeline.services import _task_matches

            client = LitePanClient(
                config.services.litepan.base_url,
                task_db_path=config.runtime.litepan_task_db_path,
                strm_host_root=config.runtime.litepan_strm_host_root,
            )
            with stage("litepan.tasks"):
                tasks = client.list_tasks()
            matches = [
                task for task in tasks
                if _task_matches(
                    task, name=config.runtime.litepan_task_name,
                    source_root=config.runtime.litepan_task_source_root or config.paths.cloud_library_root,
                    output_root=config.runtime.litepan_task_output_root or
                    config.paths.litepan_raw_output_root,
                ) and task.get("recursive") is True
            ]
            if len(matches) != 1:
                missing_capabilities.append("runtime.litepan_task_mapping")
                diagnostics.append({"capability": "runtime.litepan_task_mapping",
                                    "stage": "litepan.tasks", "reason": "mapping_not_unique"})
            else:
                try:
                    client.assert_event_targets_task(
                        matches[0]["id"], config.runtime.litepan_event_name,
                        config.runtime.litepan_event_source,
                    )
                except RuntimeError:
                    missing_capabilities.append("runtime.litepan_event_rule")
                    diagnostics.append({"capability": "runtime.litepan_event_rule",
                                        "stage": "litepan.tasks", "reason": "event_mapping_unverified"})
        except Exception as exc:
            missing_capabilities.append("runtime.litepan_task_inventory")
            diagnostics.append({"capability": "runtime.litepan_task_inventory",
                                "stage": "litepan.tasks", "reason": _capability_reason(exc)})
    if config.source is None:
        missing_capabilities.append("source.read_only_provider")
    elif str(config.source.kind).casefold() != "kisssub":
        missing_capabilities.append(f"source.kind={config.source.kind}")
    try:
        root_path = _cloud_root_directory_path(config)
    except CliError:
        missing_capabilities.append("services.cloud.root_directory_path")
    else:
        for field, value in (
            ("paths.cloud_source_root", config.paths.cloud_source_root),
            ("paths.cloud_library_root", config.paths.cloud_library_root),
        ):
            if not _remote_path_contained(root_path, value):
                missing_capabilities.append(f"{field} not contained by services.cloud.root_directory_path")
        if (any(os.environ.get(name, "").strip() for name in ("P115_ROOT_DIRECTORY_ID", "P115_ROOT_ID"))
                and os.environ.get(config.services.cloud.secret_reference or "")):
            cloud_stage = "115.initialize"
            try:
                from pipeline.real_adapters import P115CloudAdapter

                with stage(cloud_stage):
                    cloud = P115CloudAdapter(
                        None, root_directory_id=_cloud_root_directory_id(),
                        credential=_service_secret(config.services.cloud),
                        runtime_config=config, root_directory_path=root_path,
                    )
                cloud_stage = "115.capabilities"
                with stage(cloud_stage):
                    cloud.validate_runtime_capabilities()
                cloud_stage = "115.source_root"
                with stage(cloud_stage):
                    cloud.resolve_source_directory(config.paths.cloud_source_root)
                cloud_stage = "115.quarter_library_root"
                with stage(cloud_stage):
                    cloud.resolve_source_directory(
                        config.paths.cloud_library_root.rstrip("/") + "/"
                        + str(window["season_quarter"]) + "月新番"
                    )
            except Exception as exc:
                reason = _capability_reason(exc)
                capability = ("services.cloud.authentication_expired" if reason == "authentication_expired"
                              else "services.cloud.source_inventory")
                missing_capabilities.append(capability)
                diagnostics.append({"capability": capability, "stage": cloud_stage,
                                    "reason": ("client_capabilities_unavailable"
                                               if cloud_stage == "115.capabilities" else reason)})
    if os.environ.get(config.services.emby.secret_reference or ""):
        try:
            from pipeline.real_adapters import EmbyClient

            emby = EmbyClient(
                config.services.emby.base_url,
                api_key=_service_secret(config.services.emby),
                runtime_config=config,
            )
            with stage("emby.library"):
                library = emby.find_library(
                    config.runtime.emby_library_name,
                    config.runtime.emby_library_root or "",
                )
            if library is None:
                missing_capabilities.append("runtime.emby_quarter_library")
                diagnostics.append({"capability": "runtime.emby_quarter_library",
                                    "stage": "emby.library", "reason": "library_mapping_not_found"})
        except Exception as exc:
            missing_capabilities.append("runtime.emby_quarter_library")
            diagnostics.append({"capability": "runtime.emby_quarter_library",
                                "stage": "emby.library", "reason": _capability_reason(exc)})
    missing_capabilities.extend(_static_capability_missing())
    try:
        PathMapper(config.paths)
    except Exception:
        # ConfigSchema normally catches invalid roots; retain a stable
        # capability diagnostic if a custom loader supplies an invalid object.
        missing_capabilities.append("paths.mapping")
    if missing or missing_capabilities:
        _json_output({
            "command": "preflight",
            "status": "blocked",
            "missing_secrets": missing,
            "missing_capabilities": missing_capabilities,
            "capability_diagnostics": diagnostics,
        })
        return 2
    _json_output({"command": "preflight", "status": "ok"})
    return 0


def _destination_identity(path: str) -> tuple[int, int, int] | None:
    """Validate an output path and return its inode identity when present."""

    try:
        info = os.lstat(path)
    except FileNotFoundError:
        return None
    except OSError:
        raise CliError("output path could not be inspected") from None
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or int(getattr(info, "st_nlink", 1)) != 1
    ):
        raise CliError("output path is not a safe regular file")
    return (int(info.st_dev), int(info.st_ino), int(info.st_nlink))


def _same_path(left: str, right: str) -> bool:
    left_key = os.path.normcase(os.path.realpath(os.path.abspath(left)))
    right_key = os.path.normcase(os.path.realpath(os.path.abspath(right)))
    if left_key == right_key:
        return True
    try:
        return os.path.exists(left) and os.path.exists(right) and os.path.samefile(left, right)
    except OSError:
        return False


def _validate_config_migrate_paths(legacy: str, output: str, report: str) -> None:
    """Reject destination aliases before either migration artifact is written."""

    if _same_path(output, report):
        raise CliError("output and report paths must differ")
    if _same_path(legacy, output) or _same_path(legacy, report):
        raise CliError("migration destinations must differ from the legacy input")
    _destination_identity(os.path.abspath(output))
    _destination_identity(os.path.abspath(report))


def _fsync_directory(path: str) -> None:
    if os.name == "nt":
        return
    flags = os.O_RDONLY | getattr(os, "O_DIRECTORY", 0)
    try:
        fd = os.open(path, flags)
    except OSError:
        return
    try:
        os.fsync(fd)
    except OSError:
        pass
    finally:
        os.close(fd)


def _write_json_atomic(path: str, payload: Mapping[str, Any]) -> None:
    """Write JSON via a same-directory exclusive temporary file and replace."""

    target = os.path.abspath(path)
    parent = os.path.dirname(target) or "."
    os.makedirs(parent, exist_ok=True)
    if os.path.islink(parent) or not os.path.isdir(parent):
        raise CliError("output directory is not safe")
    original_identity = _destination_identity(target)
    descriptor, temporary = tempfile.mkstemp(
        prefix="." + os.path.basename(target) + ".",
        suffix=".tmp",
        dir=parent,
        text=False,
    )
    temporary_identity = _destination_identity(temporary)
    descriptor_open = True
    try:
        with os.fdopen(descriptor, "w", encoding="utf-8", newline="\n") as handle:
            descriptor_open = False
            json.dump(payload, handle, ensure_ascii=False, indent=2)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        if temporary_identity is None or _destination_identity(temporary) != temporary_identity:
            raise CliError("temporary output changed during atomic write")
        if _destination_identity(target) != original_identity:
            raise CliError("output path changed during atomic write")
        os.replace(temporary, target)
        temporary = ""
        if _destination_identity(target) != temporary_identity:
            raise CliError("atomic output verification failed")
        _fsync_directory(parent)
    finally:
        if descriptor_open:
            try:
                os.close(descriptor)
            except OSError:
                pass
        if temporary:
            try:
                if _destination_identity(temporary) == temporary_identity:
                    os.remove(temporary)
            except OSError:
                pass


def cmd_config_migrate(args: argparse.Namespace) -> int:
    _validate_config_migrate_paths(args.legacy, args.output, args.report)
    try:
        with open(args.legacy, encoding="utf-8") as handle:
            legacy = json.load(handle)
    except (OSError, json.JSONDecodeError) as exc:
        raise CliError(f"legacy config could not be read ({type(exc).__name__})") from None
    try:
        migrated, report = migrate_legacy_config(legacy)
    except ConfigError as exc:
        raise CliError(_safe_config_error(exc)) from None

    _write_json_atomic(args.report, report.to_mapping())
    if report.blocking_unmapped_fields:
        if not os.path.exists(args.output):
            _write_json_atomic(args.output, migrated)
        fields = ",".join(report.blocking_unmapped_fields[:8])
        print(f"config-migrate blocked: {fields}", file=sys.stderr)
        return 2
    try:
        ConfigSchema.from_mapping(migrated, os.environ, sys.platform)
    except ConfigError as exc:
        if not os.path.exists(args.output):
            migrated["migration_blocked"] = True
            migrated["migration_blocking_fields"] = ["post_migration_validation"]
            _write_json_atomic(args.output, migrated)
        print(f"config-migrate blocked: {_safe_config_error(exc)}", file=sys.stderr)
        return 2
    _write_json_atomic(args.output, migrated)
    _json_output({
        "command": "config-migrate",
        "status": "ok",
        "subscriptions_to_verify": len(report.moviepilot_subscriptions_to_verify),
        "dropped_fields": len(report.dropped_fields),
        "deprecated_fields": len(report.deprecated_fields),
    })
    return 0


def cmd_migrate(args: argparse.Namespace) -> int:
    _raw, config = _config_mapping(args.config)
    path = _state_path(config)
    if args.check_only:
        if not os.path.exists(path):
            _json_output({"command": "migrate", "status": "not_initialized", "schema_version": 0})
            return 0
        try:
            connection = _readonly_connection(path)
            try:
                version = int(connection.execute("PRAGMA user_version").fetchone()[0])
            finally:
                connection.close()
        except sqlite3.Error:
            raise CliError("state database could not be inspected") from None
        _json_output({
            "command": "migrate",
            "status": "ok" if version == LATEST_SCHEMA_VERSION else "pending",
            "schema_version": version,
            "target_version": LATEST_SCHEMA_VERSION,
        })
        return 0 if version <= LATEST_SCHEMA_VERSION else 2

    os.makedirs(os.path.dirname(path), exist_ok=True)
    runner = MigrationRunner(path)
    try:
        report = runner.migrate()
    except Exception as exc:
        raise CliError(f"migration failed ({type(exc).__name__})") from None
    finally:
        runner.close()
    _json_output({
        "command": "migrate",
        "status": "ok" if report.success else "failed",
        "schema_version": report.version,
        "applied_versions": list(report.applied_versions),
        "detected_legacy": bool(report.detected_legacy),
    })
    return 0 if report.success else 1


def _dry_run_details(plan: Any, secrets: tuple[str, ...]) -> dict[str, Any]:
    def text(value: Any) -> Any:
        if not isinstance(value, str):
            return value
        value = sanitize_text(value, secrets=secrets)
        return re.sub(r'https?://[^\s<>"\x27]+', '[URL_REDACTED]', value)[:300]

    subscriptions = [{key: text(getattr(sub, key)) for key in (
        "subscription_id", "media_source", "media_id", "work", "season", "season_air_date",
    )} for sub in plan.subscriptions]
    decisions = []
    for decision in plan.decisions:
        row = decision.to_mapping()
        item = {key: text(row.get(key)) for key in (
            "subscription_id", "media_source", "media_id", "work", "season", "episode",
            "episode_end", "provider", "title", "language", "release_group", "resolution",
            "collection", "decision", "reason", "season_air_date",
        )}
        item["infohash_sha256"] = hashlib.sha256(str(row.get("infohash") or "").encode()).hexdigest()
        decisions.append(item)
    return {"subscription_details": subscriptions, "decision_details": decisions}


def cmd_dry_run(args: argparse.Namespace) -> int:
    with stage("config"):
        _raw, config = _config_mapping(args.config)
    window = _window(args, config)
    with stage("runtime"):
        runtime = build_runtime(config, writable=False)
    guard = build_discovery_guard(_raw)
    discovery = DiscoveryService(
        None,
        runtime.moviepilot,
        runtime.providers,
        config=config,
        secrets=_secret_values(config),
        guard=guard,
    )
    if guard is not None:
        with stage("state.snapshot"):
            store = _open_readonly(config)
            try:
                guard.bind(store.all_episodes() if store is not None else [])
            finally:
                _close_store(store)
    with stage("discovery"):
        plan = discovery.prepare(window, read_only=True)
    summary = dict(plan.summary)
    report = {
        "command": "dry-run",
        "status": "ok" if not plan.errors else "completed_with_errors",
        "subscriptions": int(summary.get("subscription_count", 0)),
        "decisions": int(summary.get("decision_count", 0)),
        "errors": int(summary.get("error_count", 0)),
        "excluded": int(summary.get("excluded_count", 0)),
        "guard": guard.describe() if guard is not None else None,
    }
    if getattr(args, "details", False):
        report.update(_dry_run_details(plan, _secret_values(config)))
    _json_output(report)
    return 0 if not plan.errors else 1


def cmd_qualify(args: argparse.Namespace) -> int:
    with stage("config"):
        _raw, config = _config_mapping(args.config)
    from pipeline.resource_commands import qualify_current_subscriptions

    with stage("qualification"):
        report = qualify_current_subscriptions(config)
    _json_output({"command": "qualify", **report})
    return 0 if report.get("status") == "ok" else 2


_RESOURCE_REPORT_KEYS = (
    "cloud_quarter", "cloud_source", "cloud_organized", "raw_dir",
    "published_dir", "litepan_task", "litepan_rule", "emby_library",
)
_RESOURCE_STATES = {"reuse", "create", "conflict", "unknown"}


def _safe_resource_report(value: Any) -> dict[str, Any]:
    """Project only fixed coordinator fields into CLI output."""

    if not isinstance(value, Mapping):
        return _blocked_resource_report("resource_result_invalid")
    status = value.get("status")
    if not isinstance(status, str) or status not in {"plan_only", "prepared", "noop", "blocked"}:
        return _blocked_resource_report("resource_result_invalid")
    counts = value.get("counts")
    resources = value.get("resources")
    blockers = value.get("blockers", [])
    if not isinstance(counts, Mapping) or not isinstance(resources, Mapping):
        return _blocked_resource_report("resource_result_invalid")
    if any(
        isinstance(counts.get(name), bool)
        or not isinstance(counts.get(name), int)
        or counts[name] < 0
        for name in ("created", "reused")
    ):
        return _blocked_resource_report("resource_result_invalid")
    if any(
        not isinstance(resources.get(key), str)
        or resources[key] not in _RESOURCE_STATES
        for key in _RESOURCE_REPORT_KEYS
    ):
        return _blocked_resource_report("resource_result_invalid")
    if not isinstance(blockers, list) or any(
        not isinstance(item, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", item)
        for item in blockers
    ):
        return _blocked_resource_report("resource_result_invalid")
    reason = value.get("reason")
    if reason is not None and (
        not isinstance(reason, str)
        or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", reason)
    ):
        return _blocked_resource_report("resource_result_invalid")
    projected = {
        "status": status,
        "reason": reason,
        "partial": value.get("partial") is True,
        "counts": {name: counts[name] for name in ("created", "reused")},
        "resources": {key: resources[key] for key in _RESOURCE_REPORT_KEYS},
        "blockers": list(blockers),
    }
    # Resource plans expose only derived targets and the seven allowed mapping
    # fields, never the raw configuration, service credentials or account IDs.
    def safe_value(item: Any) -> bool:
        return (isinstance(item, str) and bool(item.strip()) and "://" not in item
                and not any(ord(char) < 32 or ord(char) == 127 for char in item))

    if "config_revision" in value:
        revision = value["config_revision"]
        if revision is not None and (not isinstance(revision, str)
                                     or not re.fullmatch(r"[0-9a-f]{64}", revision)):
            return _blocked_resource_report("resource_result_invalid")
        projected["config_revision"] = revision
    if "quarter" in value:
        quarter = value["quarter"]
        if not isinstance(quarter, str) or not re.fullmatch(r"[0-9]{4}-(01|04|07|10)", quarter):
            return _blocked_resource_report("resource_result_invalid")
        projected["quarter"] = quarter
    if "targets" in value:
        targets = value["targets"]
        if (not isinstance(targets, Mapping) or not set(targets).issubset(_RESOURCE_REPORT_KEYS)
                or any(not safe_value(item) for item in targets.values())):
            return _blocked_resource_report("resource_result_invalid")
        projected["targets"] = dict(targets)
    if "config_changes" in value:
        from pipeline.resource_engine import CONFIG_FIELDS
        changes = value["config_changes"]
        if not isinstance(changes, list) or len(changes) > 7:
            return _blocked_resource_report("resource_result_invalid")
        seen = set()
        for change in changes:
            if (not isinstance(change, Mapping) or set(change) != {"field", "before", "after"}
                    or change["field"] not in CONFIG_FIELDS or change["field"] in seen
                    or not safe_value(change["after"])
                    or (change["before"] is not None and not safe_value(change["before"]))):
                return _blocked_resource_report("resource_result_invalid")
            seen.add(change["field"])
        projected["config_changes"] = [dict(change) for change in changes]
    if "qualification_counts" in value:
        from pipeline.resource_commands import _COUNT_FIELDS
        qualification = value["qualification_counts"]
        if (not isinstance(qualification, Mapping) or not set(qualification).issubset(_COUNT_FIELDS)
                or any(item is not None and (type(item) is not int or item < 0)
                       for item in qualification.values())):
            return _blocked_resource_report("resource_result_invalid")
        projected["qualification_counts"] = dict(qualification)
    if "resource_reasons" in value:
        reasons = value["resource_reasons"]
        if (not isinstance(reasons, Mapping) or not set(reasons).issubset(_RESOURCE_REPORT_KEYS)
                or any(not isinstance(item, str) or not re.fullmatch(r"[a-z][a-z0-9_]{0,63}", item)
                       for item in reasons.values())):
            return _blocked_resource_report("resource_result_invalid")
        projected["resource_reasons"] = dict(reasons)
    return projected


def _blocked_resource_report(reason: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "reason": reason,
        "partial": False,
        "counts": {"created": 0, "reused": 0},
        "resources": {key: "unknown" for key in _RESOURCE_REPORT_KEYS},
        "blockers": [],
    }


def _cmd_resource_preparation(args: argparse.Namespace, *, apply: bool, command: str) -> int:
    with stage("config"):
        raw_config, config = _config_mapping(args.config)
    try:
        # Import the write-capable graph only at the resource-command boundary.
        from pipeline.resource_runtime import run_resource_preparation

        with stage("resources.inventory"):
            report = run_resource_preparation(
                raw_config,
                config,
                config_path=args.config,
                apply=apply,
            )
    except Exception:
        report = _blocked_resource_report("resource_preparation_failed")
    report = _safe_resource_report(report)
    _json_output({"command": command, **report})
    expected = {"prepared", "noop"} if apply else {"plan_only", "noop"}
    return 0 if report["status"] in expected else 2


def cmd_plan_resources(args: argparse.Namespace) -> int:
    return _cmd_resource_preparation(args, apply=False, command="plan-resources")


def cmd_prepare_resources(args: argparse.Namespace) -> int:
    return _cmd_resource_preparation(args, apply=True, command="prepare-resources")


def cmd_run(args: argparse.Namespace) -> int:
    _raw, config = _config_mapping(args.config)
    window = _window(args, config)
    mismatches = _quarter_resource_mismatches(config, window)
    if mismatches:
        raise CliError("current-quarter resources are not configured: " + ", ".join(mismatches))
    _json_output_guard: Any = None
    runtime = build_runtime(config, writable=True)
    _validate_stage_services(runtime.services)
    store = _open_writable(config)
    try:
        if getattr(runtime, "cloud", None) is not None:
            runtime.services = _build_stage_services(
                config,
                moviepilot=runtime.moviepilot,
                cloud=runtime.cloud,
                litepan=runtime.litepan,
                emby=runtime.emby,
                mapper=runtime.mapper,
                providers=runtime.providers,
                store=store,
                secrets=_secret_values(config),
            )
        _validate_stage_services(runtime.services)
        runtime.candidate_selector = runtime.services["candidate_selector"]
        discovery = runtime.services.get("discovery")
        if discovery is None:
            discovery = DiscoveryService(
                store,
                runtime.moviepilot,
                runtime.providers,
                config=config,
                secrets=_secret_values(config),
            )
        guard = build_discovery_guard(_raw)
        if guard is not None:
            discovery.guard = guard
            _json_output_guard = guard.describe()
        else:
            _json_output_guard = None
        runner = PipelineRunner(
            store,
            discovery=discovery,
            services=runtime.services,
            config=config,
            lock_path=os.path.join(os.path.dirname(_state_path(config)), "run.lock"),
            secrets=_secret_values(config),
        )
        summary = runner.run(discover=not args.resume_only, read_only=False, window=window)
    finally:
        _close_store(store)
    projection = _summary_projection(summary)
    blocked = bool(projection.get("stop_reason"))
    _json_output({
        "command": "run",
        "status": "blocked" if blocked else "ok",
        "guard": _json_output_guard,
        **projection,
    })
    if blocked:
        return 2
    return 0 if int(summary.get("errors", 0)) == 0 else 1


def cmd_status(args: argparse.Namespace) -> int:
    _raw, config = _config_mapping(args.config)
    store = _open_readonly(config)
    if store is None:
        _json_output({"command": "status", "status": "empty", "episodes": []})
        return 0
    try:
        episodes = []
        for row in store.all_episodes():
            episodes.append({
                "id": int(row.get("id") or 0),
                "status": str(row.get("status") or ""),
                "season": row.get("season"),
                "episode": row.get("episode"),
                "hash_prefix": _safe_prefix(row.get("infohash")),
                "submit_count": int(row.get("submit_count") or 0),
            })
    finally:
        _close_store(store)
    _json_output({"command": "status", "status": "ok", "episodes": episodes})
    return 0


def _resolution_mapping(value: str) -> dict[str, Any]:
    try:
        parsed = json.loads(value)
    except (TypeError, json.JSONDecodeError):
        raise CliError("--resolution must be a JSON object") from None
    if not isinstance(parsed, dict):
        raise CliError("--resolution must be a JSON object")
    return parsed


def cmd_reviews_list(args: argparse.Namespace) -> int:
    _raw, config = _config_mapping(args.config)
    store = _open_readonly(config)
    if store is None:
        _json_output({"command": "reviews list", "status": "empty", "reviews": []})
        return 0
    try:
        rows = ReviewService(store).list_open()
        reviews = [
            {
                "review_id": int(row.get("review_id") or 0),
                "review_reason": str(row.get("review_reason") or ""),
                "review_generation": int(row.get("review_generation") or 0),
                "status": "open",
            }
            for row in rows
        ]
    finally:
        _close_store(store)
    _json_output({"command": "reviews list", "status": "ok", "reviews": reviews})
    return 0


def cmd_reviews_resolve(args: argparse.Namespace) -> int:
    _raw, config = _config_mapping(args.config)
    try:
        review_id = int(args.id)
    except (TypeError, ValueError):
        raise CliError("--id must be an integer") from None
    if review_id <= 0:
        raise CliError("--id must be positive")
    resolution = _resolution_mapping(args.resolution)

    readonly = _open_readonly(config)
    if readonly is None:
        raise CliError(f"review {review_id} is not open")
    try:
        row = readonly.get_review(review_id)
        if row is None:
            raise CliError(f"review {review_id} is not open")
        from pipeline.models import ReviewReason  # noqa: PLC0415
        from pipeline.services import _validate_review_resolution  # noqa: PLC0415

        try:
            reason = ReviewReason(str(row.get("review_reason")))
            _validate_review_resolution(reason, resolution, ())
        except ValueError as exc:
            message = str(exc)
            if not resolution:
                message += "; required fields include selected_file_id/file_id where applicable"
            raise CliError(message) from None
        expected_generation = row.get("review_generation")
        if args.generation is not None:
            try:
                if int(args.generation) != int(expected_generation or 0):
                    raise CliError("review generation is stale")
            except ValueError:
                raise CliError("--generation must be an integer") from None
    finally:
        _close_store(readonly)

    writable = _open_writable(config)
    try:
        current = writable.get_review(review_id)
        if current is None:
            raise CliError(f"review {review_id} is not open")
        if args.generation is not None and int(current.get("review_generation") or 0) != int(args.generation):
            raise CliError("review generation is stale")
        result = ReviewService(writable).resolve(review_id, resolution)
    finally:
        _close_store(writable)
    _json_output({"command": "reviews resolve", "status": "ok", "review_id": review_id, "resume_status": result})
    return 0


def cmd_audit(args: argparse.Namespace) -> int:
    _raw, config = _config_mapping(args.config)
    store = _open_readonly(config)
    if store is None:
        _json_output({
            "command": "audit",
            "status": "empty",
            "schema_version": 0,
            "episode_counts": {},
            "open_reviews": 0,
            "history_rows": 0,
            "notification_rows": 0,
        })
        return 0
    try:
        result = store.audit()
    finally:
        _close_store(store)
    _json_output({"command": "audit", "status": "ok", **result})
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(description="Anime Cloud Pipeline V5 CLI")
    sub = parser.add_subparsers(
        dest="command", required=True, parser_class=_SafeArgumentParser
    )

    preflight = sub.add_parser("preflight", help="validate config and local credential references")
    preflight.add_argument("--config", required=True)
    preflight.set_defaults(handler=cmd_preflight)

    migrate_config = sub.add_parser("config-migrate", help="migrate a legacy JSON config")
    migrate_config.add_argument("--legacy", required=True)
    migrate_config.add_argument("--output", required=True)
    migrate_config.add_argument("--report", required=True)
    migrate_config.set_defaults(handler=cmd_config_migrate)

    migrate = sub.add_parser("migrate", help="apply SQLite V5 migrations")
    migrate.add_argument("--config", required=True)
    migrate.add_argument("--check-only", action="store_true")
    migrate.set_defaults(handler=cmd_migrate)

    def add_run_options(command: argparse.ArgumentParser) -> None:
        command.add_argument("--config", required=True)
        command.add_argument("--previous-day", action="store_true")
        command.add_argument("--backfill")

    dry = sub.add_parser("dry-run", help="read-only discovery preview")
    add_run_options(dry)
    dry.add_argument("--details", action="store_true")
    dry.set_defaults(handler=cmd_dry_run)

    qualify = sub.add_parser("qualify", help="read-only subscription qualification")
    qualify.add_argument("--config", required=True)
    qualify.set_defaults(handler=cmd_qualify)

    plan_resources = sub.add_parser(
        "plan-resources", help="read-only current-quarter resource plan"
    )
    plan_resources.add_argument("--config", required=True)
    plan_resources.set_defaults(handler=cmd_plan_resources)

    prepare_resources = sub.add_parser(
        "prepare-resources", help="prepare current-quarter resources"
    )
    prepare_resources.add_argument("--config", required=True)
    prepare_resources.add_argument("--apply", action="store_true", required=True)
    prepare_resources.set_defaults(handler=cmd_prepare_resources)

    run = sub.add_parser("run", help="one bounded V5 runner invocation")
    add_run_options(run)
    run.add_argument("--resume-only", action="store_true")
    run.set_defaults(handler=cmd_run)

    status = sub.add_parser("status", help="read-only episode summary")
    status.add_argument("--config", required=True)
    status.set_defaults(handler=cmd_status)

    reviews = sub.add_parser("reviews", help="review lifecycle commands")
    review_sub = reviews.add_subparsers(
        dest="reviews_command", required=True, parser_class=_SafeArgumentParser
    )
    reviews_list = review_sub.add_parser("list")
    reviews_list.add_argument("--config", required=True)
    reviews_list.set_defaults(handler=cmd_reviews_list)
    reviews_resolve = review_sub.add_parser("resolve")
    reviews_resolve.add_argument("--config", required=True)
    reviews_resolve.add_argument("--id", required=True)
    reviews_resolve.add_argument("--resolution", required=True)
    reviews_resolve.add_argument("--generation")
    reviews_resolve.set_defaults(handler=cmd_reviews_resolve)

    audit = sub.add_parser("audit", help="read-only aggregate audit summary")
    audit.add_argument("--config", required=True)
    audit.set_defaults(handler=cmd_audit)
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    try:
        args = _parser().parse_args(list(argv) if argv is not None else None)
        if args.command in {"preflight", "dry-run", "qualify", "plan-resources"}:
            with read_only_command(args.command):
                return int(args.handler(args))
        return int(args.handler(args))
    except ReadOnlyDeadline:
        _json_output({"command": args.command, "status": "blocked",
                      "reason": "read_only_deadline", "incomplete": True})
        return 2
    except CliError as exc:
        print(f"error: {_safe_config_error(exc)}", file=sys.stderr)
        return 2
    except ConfigError as exc:
        print(f"error: {_safe_config_error(exc)}", file=sys.stderr)
        return 2
    except DiscoveryGuardError as exc:
        print(f"error: {_safe_config_error(exc)}", file=sys.stderr)
        return 3
    except (OSError, sqlite3.Error) as exc:
        print(f"error: operation failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    except Exception as exc:
        # Do not echo arbitrary adapter/remote exception text at the CLI
        # boundary. The logger and persisted state have their own redaction
        # policy; summaries need only a stable error class.
        print(f"error: operation failed ({type(exc).__name__})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
