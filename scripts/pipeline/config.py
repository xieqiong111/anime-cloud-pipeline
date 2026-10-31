"""Strict V5 configuration loading and pure legacy normalization.

V5 configuration describes service connections, path boundaries, scoring, and
bounded retry/scan policy.  It intentionally contains no subscription
catalogue or service-generated IDs: MoviePilot is the sole subscription source
of truth and runtime IDs are resolved by name during later stages.
"""
from __future__ import annotations

from copy import deepcopy
from dataclasses import asdict, dataclass
import json
import math
import posixpath
import re
from typing import Any, Iterable, Mapping, Optional
from urllib.parse import urlparse

from .http import RetryPolicy


_ENV_NAME = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_RUNTIME_ID_KEYS = {
    "strm_task_id",
    "library_id",
    "target_directory_id",
    "remote_task_id",
    "task_id",
    "file_id",
}
_SCORING_SECTION_ALIASES = ("scoring", "candidate_scoring")
_LIMIT_SECTION_ALIASES = ("limits", "submission_limits", "scan_limits", "submission", "scan")
_RETRY_SECTION_ALIASES = ("retry", "network_retry", "network")


class ConfigError(ValueError):
    """Raised when a configuration cannot safely be used by V5."""

    def __init__(self, message: str, *, errors: Iterable[str] | None = None):
        super().__init__(message)
        self.errors = tuple(errors or (message,))


@dataclass(frozen=True, slots=True)
class ServiceConfig:
    """A validated service connection with an environment-backed secret."""

    name: str
    base_url: str = ""
    token_env: Optional[str] = None
    api_key_env: Optional[str] = None
    credential_env: Optional[str] = None
    secret_env: Optional[str] = None
    auth_mode: str = "x-api-key"
    task_name: Optional[str] = None
    library_name: Optional[str] = None
    media_source: str = "tmdb"
    root_directory_path: Optional[str] = None
    # Optional, opt-in allow-list of MoviePilot subscription-source usernames.
    # Empty means "no source filtering"; discovery keeps its historical
    # behaviour until an operator names the allowed ``username`` values.
    subscription_sources: tuple[str, ...] = ()

    @property
    def secret_reference(self) -> Optional[str]:
        return self.token_env or self.api_key_env or self.credential_env or self.secret_env


@dataclass(frozen=True, slots=True)
class ServicesConfig:
    moviepilot: ServiceConfig
    cloud: ServiceConfig
    litepan: ServiceConfig
    emby: ServiceConfig

    @property
    def p115(self) -> ServiceConfig:
        """The V5 neutral cloud service under its legacy adapter name."""
        return self.cloud


@dataclass(frozen=True, slots=True)
class PathConfig:
    cloud_source_root: str
    cloud_library_root: str
    litepan_output_root: str
    moviepilot_mount_root: str
    emby_mount_root: str
    state_directory: str = "/var/lib/anime-cloud-pipeline"
    litepan_raw_output_root: str | None = None
    litepan_publish_subdir: str | None = None
    cloud_library_subdir: str | None = None


@dataclass(frozen=True, slots=True)
class ScoringConfig:
    include_weight: int
    language_weight: int
    release_group_weight: int
    resolution_weight: int
    collection_penalty: int

    @property
    def weights(self) -> dict[str, int]:
        return {
            "include_weight": self.include_weight,
            "language_weight": self.language_weight,
            "release_group_weight": self.release_group_weight,
            "resolution_weight": self.resolution_weight,
            "collection_penalty": self.collection_penalty,
        }


@dataclass(frozen=True, slots=True)
class LimitsConfig:
    network_timeout_seconds: int
    max_read_retries: int
    max_task_pages: int
    max_recursive_depth: int
    max_recursive_files: int
    max_new_tasks_per_run: int
    max_new_tasks_per_subscription: int
    max_review_batch: int
    max_reconcile_batch: int
    max_cloud_checks_per_run: int

    @property
    def global_submission_limit(self) -> int:
        return self.max_new_tasks_per_run

    @property
    def per_subscription_submission_limit(self) -> int:
        return self.max_new_tasks_per_subscription


# Configuration, HTTP adapters, and stage services all consume this one
# policy type.  Keep the historical import name for callers of config.py.
RetryConfig = RetryPolicy


@dataclass(frozen=True, slots=True)
class SourceConfig:
    """Read-only external source provider configuration."""

    name: str
    kind: str
    base_url: str


@dataclass(frozen=True, slots=True)
class RuntimeConfig:
    litepan_task_name: str
    emby_library_name: str
    reconcile_batch: int
    litepan_task_source_root: str | None = None
    litepan_task_output_root: str | None = None
    litepan_task_db_path: str | None = None
    litepan_strm_host_root: str | None = None
    litepan_event_name: str = "anime_ready"
    litepan_event_source: str = "hermes"
    emby_library_root: str | None = None


@dataclass(frozen=True, slots=True)
class ScheduleConfig:
    timezone: str
    cron: str = ""


@dataclass(frozen=True, slots=True)
class ConfigMigrationReport:
    """Machine-readable diagnostics produced by :func:`migrate_legacy_config`."""

    dropped_fields: tuple[str, ...] = ()
    deprecated_fields: tuple[str, ...] = ()
    moviepilot_subscriptions_to_verify: tuple[str, ...] = ()
    blocking_unmapped_fields: tuple[str, ...] = ()

    @property
    def subscriptions_to_verify(self) -> tuple[str, ...]:
        return self.moviepilot_subscriptions_to_verify

    @property
    def dropped(self) -> tuple[str, ...]:
        return self.dropped_fields

    @property
    def deprecated(self) -> tuple[str, ...]:
        return self.deprecated_fields

    def to_mapping(self) -> dict[str, Any]:
        return {
            "dropped_fields": list(self.dropped_fields),
            "deprecated_fields": list(self.deprecated_fields),
            "moviepilot_subscriptions_to_verify": list(self.moviepilot_subscriptions_to_verify),
            "blocking_unmapped_fields": list(self.blocking_unmapped_fields),
        }

    def as_dict(self) -> dict[str, Any]:
        return self.to_mapping()


@dataclass(frozen=True, slots=True)
class ConfigSchema:
    """Fully validated, immutable V5 configuration."""

    schema_version: int
    services: ServicesConfig
    paths: PathConfig
    scoring: ScoringConfig
    limits: LimitsConfig
    retry: RetryConfig
    runtime: RuntimeConfig
    schedule: ScheduleConfig
    timezone: str
    source: SourceConfig | None = None

    @property
    def moviepilot(self) -> ServiceConfig:
        return self.services.moviepilot

    @property
    def p115(self) -> ServiceConfig:
        return self.services.cloud

    @property
    def cloud(self) -> ServiceConfig:
        return self.services.cloud

    @property
    def litepan(self) -> ServiceConfig:
        return self.services.litepan

    @property
    def emby(self) -> ServiceConfig:
        return self.services.emby

    @classmethod
    def from_mapping(
        cls,
        raw: Mapping[str, Any],
        environ: Mapping[str, str] | None = None,
        platform: str | None = None,
    ) -> "ConfigSchema":
        """Normalize and validate a V5 mapping without mutating ``raw``.

        ``environ`` is accepted so callers can perform their own secret
        availability checks at the boundary.  V5 validates references here,
        but never requires the secret value to be present while parsing a
        committed configuration file.
        """
        del environ, platform  # reserved for runtime-specific policy checks
        if not isinstance(raw, Mapping):
            raise ConfigError("config: must be an object; fix: provide a JSON object")
        subscriptions_path = _first_subscription_catalogue(raw)
        if subscriptions_path is not None:
            raise ConfigError(
                f"{subscriptions_path}: deprecated local catalogue; "
                "fix: remove it and use MoviePilot as the source of truth"
            )
        if raw.get("migration_blocked"):
            fields = raw.get("migration_blocking_fields") or "required legacy fields"
            raise ConfigError(
                f"migration_blocked: {fields}; fix: complete the legacy mapping before starting V5"
            )
        runtime_id = _first_runtime_id(raw)
        if runtime_id is not None:
            field, key = runtime_id
            raise ConfigError(
                f"{field}: runtime ID is not allowed; fix: remove it and resolve the runtime object by name"
            )

        version = raw.get("schema_version")
        if version != 5:
            raise ConfigError(
                "schema_version: must be 5; fix: migrate this file with config-migrate before starting V5"
            )

        services_raw = _service_mapping(raw)
        services = ServicesConfig(
            moviepilot=_parse_service("moviepilot", services_raw.get("moviepilot"), require_url=True,
                                      require_secret=True),
            cloud=_parse_service("cloud", services_raw.get("cloud"), require_url=False,
                                 require_secret=True),
            litepan=_parse_service("litepan", services_raw.get("litepan"), require_url=True,
                                   require_secret=True),
            emby=_parse_service("emby", services_raw.get("emby"), require_url=True,
                                require_secret=True),
        )
        source_container = raw.get("services")
        source = _parse_source(
            raw,
            source_container if isinstance(source_container, Mapping) else {},
        )

        paths_raw = _section(raw, "paths")
        paths = PathConfig(
            cloud_source_root=_path(paths_raw, "cloud_source_root"),
            cloud_library_root=_path(paths_raw, "cloud_library_root"),
            litepan_output_root=_path(paths_raw, "litepan_output_root"),
            moviepilot_mount_root=_path(paths_raw, "moviepilot_mount_root"),
            emby_mount_root=_path(paths_raw, "emby_mount_root"),
            state_directory=_path(paths_raw, "state_directory", default="/var/lib/anime-cloud-pipeline"),
            litepan_raw_output_root=_path(paths_raw, "litepan_raw_output_root")
            if "litepan_raw_output_root" in paths_raw else None,
            litepan_publish_subdir=_component(paths_raw, "litepan_publish_subdir")
            if "litepan_publish_subdir" in paths_raw else None,
            cloud_library_subdir=_component(paths_raw, "cloud_library_subdir")
            if "cloud_library_subdir" in paths_raw else None,
        )
        _validate_distinct_roots(paths)

        scoring_raw = _merge_alias_sections(raw, _SCORING_SECTION_ALIASES, "scoring")
        scoring = ScoringConfig(
            include_weight=_positive(scoring_raw, "include_weight", "scoring"),
            language_weight=_positive(scoring_raw, "language_weight", "scoring"),
            release_group_weight=_positive(scoring_raw, "release_group_weight", "scoring"),
            resolution_weight=_positive(scoring_raw, "resolution_weight", "scoring"),
            collection_penalty=_positive(scoring_raw, "collection_penalty", "scoring"),
        )

        limits_raw = _merge_alias_sections(raw, _LIMIT_SECTION_ALIASES, "limits")
        limits = LimitsConfig(
            network_timeout_seconds=_positive(limits_raw, "network_timeout_seconds", "limits"),
            max_read_retries=_positive(limits_raw, "max_read_retries", "limits"),
            max_task_pages=_positive(limits_raw, "max_task_pages", "limits"),
            max_recursive_depth=_positive_alias(
                limits_raw, ("max_recursive_depth", "recursive_scan_max_depth"), "limits.max_recursive_depth"
            ),
            max_recursive_files=_positive_alias(
                limits_raw, ("max_recursive_files", "recursive_scan_max_files"), "limits.max_recursive_files"
            ),
            max_new_tasks_per_run=_positive_alias(
                limits_raw,
                ("max_new_tasks_per_run", "global_submission_limit", "max_global_submissions",
                 "max_new_submissions_per_run"),
                "limits.max_new_tasks_per_run",
            ),
            max_new_tasks_per_subscription=_positive_alias(
                limits_raw,
                ("max_new_tasks_per_subscription", "per_subscription_submission_limit",
                 "max_submissions_per_subscription", "max_new_submissions_per_subscription"),
                "limits.max_new_tasks_per_subscription",
            ),
            max_review_batch=_positive_alias(limits_raw, ("max_review_batch",), "limits.max_review_batch"),
            max_reconcile_batch=_positive_alias(
                limits_raw, ("max_reconcile_batch",), "limits.max_reconcile_batch"
            ),
            # Pacing default: a backlog wider than this issues its remaining
            # cloud reads on later runs instead of one burst the account-level
            # 115 rate limit answers with RemoteInventoryIncomplete.
            max_cloud_checks_per_run=_positive_alias(
                limits_raw, ("max_cloud_checks_per_run",), "limits.max_cloud_checks_per_run",
                default=8,
            ),
        )

        retry_raw = _merge_alias_sections(raw, _RETRY_SECTION_ALIASES, "retry")
        retry_max_read_retries = _positive_alias(
            retry_raw, ("max_read_retries",), "retry.max_read_retries",
            default=limits.max_read_retries,
        )
        if retry_max_read_retries != limits.max_read_retries:
            raise ConfigError(
                "retry.max_read_retries: must match limits.max_read_retries; "
                "fix: configure one read retry limit"
            )
        retry = RetryConfig(
            max_attempts=_positive_alias(retry_raw, ("max_attempts", "max_retries"), "retry.max_attempts"),
            backoff_seconds=_positive_float_alias(
                retry_raw, ("backoff_seconds", "backoff_initial_seconds", "base_delay_seconds"),
                "retry.backoff_seconds", allow_zero=True
            ),
            backoff_max_seconds=_positive_float_alias(
                retry_raw, ("backoff_max_seconds", "backoff_cap_seconds", "max_delay_seconds"),
                "retry.backoff_max_seconds",
                allow_zero=False,
            ),
            jitter_seconds=_positive_float_alias(
                retry_raw, ("jitter_seconds", "backoff_jitter_seconds"), "retry.jitter_seconds", allow_zero=True,
                default=0.0,
            ),
            max_read_retries=retry_max_read_retries,
        )
        if retry.backoff_max_seconds < retry.backoff_seconds:
            raise ConfigError(
                "retry.backoff_max_seconds: must not be less than backoff_seconds; "
                "fix: increase the retry backoff cap"
            )

        runtime_raw = _section(raw, "runtime", default={})
        task_name = _first_nonempty(
            runtime_raw.get("litepan_task_name"), services.litepan.task_name,
        )
        library_name = _first_nonempty(
            runtime_raw.get("emby_library_name"), services.emby.library_name,
        )
        if not task_name:
            raise ConfigError(
                "runtime.litepan_task_name: required; fix: provide the LitePan task name used for runtime lookup"
            )
        if not library_name:
            raise ConfigError(
                "runtime.emby_library_name: required; fix: provide the Emby library name used for runtime lookup"
            )
        reconcile_batch = _positive_alias(
            runtime_raw, ("reconcile_batch",), "runtime.reconcile_batch", default=limits.max_reconcile_batch
        )
        runtime = RuntimeConfig(
            str(task_name), str(library_name), reconcile_batch,
            *(
                _path(runtime_raw, key) if key in runtime_raw else None
                for key in (
                    "litepan_task_source_root", "litepan_task_output_root",
                    "litepan_task_db_path", "litepan_strm_host_root",
                )
            ),
            _nonempty_runtime_token(runtime_raw, "litepan_event_name", "anime_ready"),
            _nonempty_runtime_token(runtime_raw, "litepan_event_source", "hermes"),
            _path(runtime_raw, "emby_library_root") if "emby_library_root" in runtime_raw else None,
        )
        _validate_litepan_geometry(paths, runtime)

        schedule_raw = _section(raw, "schedule", default={})
        timezone = str(schedule_raw.get("timezone") or raw.get("timezone") or "").strip()
        if not timezone:
            raise ConfigError("schedule.timezone: required; fix: set an IANA timezone name")
        cron = str(schedule_raw.get("cron") or "").strip()
        schedule = ScheduleConfig(timezone=timezone, cron=cron)
        return cls(5, services, paths, scoring, limits, retry, runtime, schedule, timezone, source)

    def to_mapping(self) -> dict[str, Any]:
        """Return a JSON-compatible canonical mapping without secret values."""
        service_values: dict[str, Any] = {}
        for name, service in (
            ("moviepilot", self.services.moviepilot),
            ("cloud", self.services.cloud),
            ("litepan", self.services.litepan),
            ("emby", self.services.emby),
        ):
            service_values[name] = {
                key: value
                for key, value in {
                    "name": service.name,
                    "base_url": service.base_url,
                    "token_env": service.token_env,
                    "api_key_env": service.api_key_env,
                    "credential_env": service.credential_env,
                    "secret_env": service.secret_env,
                    "auth_mode": service.auth_mode,
                    "task_name": service.task_name,
                    "library_name": service.library_name,
                    "media_source": service.media_source,
                    "root_directory_path": service.root_directory_path,
                }.items()
                if value not in (None, "")
            }
            if service.subscription_sources:
                service_values[name]["subscription_sources"] = list(
                    service.subscription_sources
                )
        result = {
            "schema_version": self.schema_version,
            "services": service_values,
            "paths": {
                "cloud_source_root": self.paths.cloud_source_root,
                "cloud_library_root": self.paths.cloud_library_root,
                "litepan_output_root": self.paths.litepan_output_root,
                **({"litepan_raw_output_root": self.paths.litepan_raw_output_root}
                   if self.paths.litepan_raw_output_root else {}),
                **({"litepan_publish_subdir": self.paths.litepan_publish_subdir}
                   if self.paths.litepan_publish_subdir else {}),
                **({"cloud_library_subdir": self.paths.cloud_library_subdir}
                   if self.paths.cloud_library_subdir else {}),
                "moviepilot_mount_root": self.paths.moviepilot_mount_root,
                "emby_mount_root": self.paths.emby_mount_root,
                "state_directory": self.paths.state_directory,
            },
            "scoring": self.scoring.weights,
            "limits": {
                "network_timeout_seconds": self.limits.network_timeout_seconds,
                "max_read_retries": self.limits.max_read_retries,
                "max_task_pages": self.limits.max_task_pages,
                "max_recursive_depth": self.limits.max_recursive_depth,
                "max_recursive_files": self.limits.max_recursive_files,
                "max_new_tasks_per_run": self.limits.max_new_tasks_per_run,
                "max_new_tasks_per_subscription": self.limits.max_new_tasks_per_subscription,
                "max_review_batch": self.limits.max_review_batch,
                "max_reconcile_batch": self.limits.max_reconcile_batch,
            },
            "retry": {
                "max_attempts": self.retry.max_attempts,
                "max_read_retries": self.retry.max_read_retries,
                "backoff_seconds": self.retry.backoff_seconds,
                "backoff_max_seconds": self.retry.backoff_max_seconds,
                "jitter_seconds": self.retry.jitter_seconds,
            },
            "runtime": {
                "litepan_task_name": self.runtime.litepan_task_name,
                "emby_library_name": self.runtime.emby_library_name,
                "reconcile_batch": self.runtime.reconcile_batch,
                "litepan_event_name": self.runtime.litepan_event_name,
                "litepan_event_source": self.runtime.litepan_event_source,
                **{
                    key: value for key, value in (
                        ("litepan_task_source_root", self.runtime.litepan_task_source_root),
                        ("litepan_task_output_root", self.runtime.litepan_task_output_root),
                        ("litepan_task_db_path", self.runtime.litepan_task_db_path),
                        ("litepan_strm_host_root", self.runtime.litepan_strm_host_root),
                        ("emby_library_root", self.runtime.emby_library_root),
                    ) if value is not None
                },
            },
            "schedule": {"timezone": self.schedule.timezone, "cron": self.schedule.cron},
        }
        if self.source is not None:
            result["source"] = {
                "name": self.source.name,
                "kind": self.source.kind,
                "base_url": self.source.base_url,
            }
        return result


def migrate_legacy_config(raw: Mapping[str, Any]) -> tuple[dict[str, Any], ConfigMigrationReport]:
    """Return a V5-safe copy of a legacy mapping and diagnostics.

    The input is never mutated.  The local subscription catalogue is removed,
    but each identity is retained in the report so an operator can verify it
    against MoviePilot.  Runtime-generated IDs are dropped rather than copied
    into the new configuration.
    """
    if not isinstance(raw, Mapping):
        raise ConfigError("config: must be an object; fix: provide a JSON object")
    migrated = deepcopy(dict(raw))
    dropped: list[str] = []
    deprecated: list[str] = []
    identities: list[str] = []

    # Legacy releases stored the same local catalogue at the root, under a
    # MoviePilot section, or inside a services wrapper.  Remove every copy and
    # retain each identity in one verification report.
    _remove_subscription_catalogues(migrated, (), dropped, deprecated, identities)

    # Remove generated IDs at every nesting level while preserving all other
    # recognized service, path, limit, scoring, and schedule values.
    _drop_runtime_ids(migrated, (), dropped, deprecated)

    # Give the new loader a canonical service view while retaining legacy
    # top-level sections for a human-readable, loss-minimizing migration file.
    if "services" not in migrated:
        services: dict[str, Any] = {}
        for canonical, aliases in {
            "moviepilot": ("moviepilot",),
            "cloud": ("cloud", "p115", "storage"),
            "litepan": ("litepan",),
            "emby": ("emby",),
        }.items():
            for alias in aliases:
                section = migrated.get(alias)
                if isinstance(section, Mapping):
                    services[canonical] = deepcopy(dict(section))
                    break
        if services:
            migrated["services"] = services
    _ensure_service_names(migrated)

    blocking = _migration_blockers(migrated)
    if blocking:
        # Preserve the legacy version and mark the result explicitly blocked;
        # a file with missing semantics must never look runnable as V5.
        migrated["schema_version"] = raw.get("schema_version", 0)
        migrated["migration_blocked"] = True
        migrated["migration_blocking_fields"] = list(blocking)
    else:
        canonical = _canonicalize_migration_mapping(migrated)
        for key in ("services", "scoring", "limits", "retry"):
            if key in canonical:
                migrated[key] = canonical[key]
        migrated["schema_version"] = 5

    report = ConfigMigrationReport(
        dropped_fields=tuple(dropped),
        deprecated_fields=tuple(deprecated),
        moviepilot_subscriptions_to_verify=tuple(identities),
        blocking_unmapped_fields=tuple(blocking),
    )
    return migrated, report


def load_config(path: str, environ: Mapping[str, str] | None = None,
                platform: str | None = None) -> ConfigSchema:
    """Load and validate a JSON V5 file."""
    with open(path, encoding="utf-8") as handle:
        return ConfigSchema.from_mapping(json.load(handle), environ, platform)


def _service_mapping(raw: Mapping[str, Any]) -> dict[str, Any]:
    services = raw.get("services")
    if "services" in raw and not isinstance(services, Mapping):
        raise ConfigError("services: must be an object; fix: provide service mappings")
    result: dict[str, Any] = {}
    # Top-level legacy names and nested service names are aliases for one
    # semantic service.  Every present alias is checked; a malformed or
    # conflicting alias cannot be silently shadowed by another copy.
    for canonical, aliases in {
        "moviepilot": ("moviepilot",),
        "cloud": ("cloud", "p115", "storage"),
        "litepan": ("litepan",),
        "emby": ("emby",),
    }.items():
        candidates: list[tuple[str, Mapping[str, Any]]] = []
        if isinstance(services, Mapping):
            for alias in aliases:
                if alias in services:
                    value = services[alias]
                    field = f"services.{alias}"
                    if not isinstance(value, Mapping):
                        raise ConfigError(
                            f"{field}: must be an object; fix: provide the {canonical} service mapping"
                        )
                    candidates.append((field, value))
        for alias in aliases:
            if alias in raw:
                value = raw[alias]
                if not isinstance(value, Mapping):
                    raise ConfigError(
                        f"{alias}: must be an object; fix: provide the {canonical} service mapping"
                    )
                candidates.append((alias, value))
        if not candidates:
            continue
        normalized = [
            (field, _parse_service(canonical, value, require_url=canonical != "cloud",
                                   require_secret=True))
            for field, value in candidates
        ]
        first_field, first_value = normalized[0]
        for field, value in normalized[1:]:
            if value != first_value:
                raise ConfigError(
                    f"{field}: conflicts with {first_field}; fix: keep one matching {canonical} service mapping"
                )
        result[canonical] = asdict(first_value)
    return result


def _parse_service(name: str, value: Any, *, require_url: bool, require_secret: bool) -> ServiceConfig:
    field = f"services.{name}"
    if not isinstance(value, Mapping):
        raise ConfigError(f"{field}: required; fix: define the {name} service object with a name")
    service = dict(value)
    service_name = str(service.get("name") or "").strip()
    if not service_name:
        raise ConfigError(f"{field}.name: required; fix: set a stable service name")
    base_url = str(service.get("base_url") or "").strip().rstrip("/")
    if require_url:
        if not base_url:
            raise ConfigError(f"{field}.base_url: required; fix: set an http(s) service URL")
        parsed = urlparse(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc:
            raise ConfigError(f"{field}.base_url: must be an http(s) URL; fix: set a valid service URL")

    for key in _RUNTIME_ID_KEYS:
        if key in service:
            raise ConfigError(
                f"{field}.{key}: runtime ID is not allowed; fix: remove it and resolve the runtime object by name"
            )
    secret_keys = ("token_env", "api_key_env", "credential_env", "secret_env")
    refs = [service.get(key) for key in secret_keys if service.get(key) not in (None, "")]
    if require_secret and not refs:
        raise ConfigError(
            f"{field}.secret_env: required; fix: reference the service secret with an environment variable name"
        )
    for key, ref in zip(secret_keys, (service.get(key) for key in secret_keys)):
        if ref in (None, ""):
            continue
        if not isinstance(ref, str) or not _ENV_NAME.fullmatch(ref):
            raise ConfigError(
                f"{field}.{key}: must be an environment variable reference; "
                "fix: use a name such as MOVIEPILOT_TOKEN"
            )

    task_name = _optional_name(service, "task_name", "strm_task_name", field)
    library_name = _optional_name(service, "library_name", "library", field)
    media_source = str(service.get("media_source") or "tmdb").strip()
    if not media_source:
        raise ConfigError(f"{field}.media_source: required; fix: set a media identifier namespace")
    return ServiceConfig(
        name=service_name,
        base_url=base_url,
        token_env=_optional_str(service.get("token_env")),
        api_key_env=_optional_str(service.get("api_key_env")),
        credential_env=_optional_str(service.get("credential_env")),
        secret_env=_optional_str(service.get("secret_env")),
        auth_mode=str(service.get("auth_mode") or "x-api-key"),
        task_name=task_name,
        library_name=library_name,
        media_source=media_source,
        root_directory_path=_optional_service_path(service, "root_directory_path", field),
        subscription_sources=_optional_source_list(service, "subscription_sources", field),
    )


def _parse_source(raw: Mapping[str, Any], services: Mapping[str, Any]) -> SourceConfig | None:
    """Parse an explicit read-only source provider without constructing it."""

    source = raw.get("source")
    if source is None:
        source = raw.get("sources")
    if source is None and isinstance(services, Mapping):
        source = services.get("source")
    if source is None:
        return None
    if not isinstance(source, Mapping):
        raise ConfigError("source: must be an object; fix: provide a read-only source mapping")
    values = dict(source)
    name = str(values.get("name") or values.get("type") or "").strip()
    kind = str(values.get("kind") or values.get("type") or name).strip().casefold()
    base_url = str(values.get("base_url") or values.get("site_url") or "").strip().rstrip("/")
    if not name:
        raise ConfigError("source.name: required; fix: set the source provider name")
    if not kind:
        raise ConfigError("source.kind: required; fix: set the source provider kind")
    parsed = urlparse(base_url)
    if parsed.scheme not in {"http", "https"} or not parsed.netloc:
        raise ConfigError("source.base_url: must be an http(s) URL; fix: set a read-only source URL")
    return SourceConfig(name=name, kind=kind, base_url=base_url)


def _optional_name(service: Mapping[str, Any], key: str, alias: str | None = None,
                   field: str = "service") -> Optional[str]:
    values = (service.get(key), service.get(alias)) if alias else (service.get(key),)
    names = [str(value).strip() for value in values if value not in (None, "")]
    if len(set(names)) > 1:
        raise ConfigError(
            f"{field}.{key}: conflicts with {alias}; fix: keep one matching value for {key}"
        )
    for value in values:
        if value not in (None, ""):
            text = str(value).strip()
            if not text:
                break
            return text
    return None


def _optional_str(value: Any) -> Optional[str]:
    if value in (None, ""):
        return None
    return str(value)


def _optional_source_list(
    service: Mapping[str, Any], key: str, field: str,
) -> tuple[str, ...]:
    """Parse an opt-in allow-list of subscription-source usernames.

    Absent/empty means "no source filtering" so the historical behaviour is
    unchanged.  A present value must be a list (or the internal tuple form
    produced by ``asdict``) of non-blank strings; anything else fails closed.
    """

    value = service.get(key)
    if value in (None, ""):
        return ()
    if isinstance(value, str) or not isinstance(value, (list, tuple)):
        raise ConfigError(
            f"{field}.{key}: must be a list of subscription-source usernames; "
            "fix: use a JSON array such as [\"Bangumi订阅\", \"admin\"]"
        )
    names: list[str] = []
    for item in value:
        if not isinstance(item, str) or not item.strip():
            raise ConfigError(
                f"{field}.{key}: entries must be non-blank strings; "
                "fix: list each allowed subscription-source username"
            )
        names.append(item.strip())
    return tuple(names)


def _optional_service_path(
    service: Mapping[str, Any], key: str, field: str,
) -> Optional[str]:
    value = service.get(key)
    if value in (None, ""):
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"{field}.{key}: must be an absolute POSIX path")
    value = value.strip()
    if not value.startswith("/") or value.startswith("//") or "\\" in value or "\x00" in value:
        raise ConfigError(f"{field}.{key}: must be an absolute POSIX path")
    if any(part in {".", "..", ""} for part in value.split("/")[1:]):
        raise ConfigError(f"{field}.{key}: must not contain traversal or empty segments")
    return posixpath.normpath(value)


def _section(raw: Mapping[str, Any], key: str, *, default: Mapping[str, Any] | None = None) -> dict[str, Any]:
    value = raw.get(key, default)
    if value is None:
        return {}
    if not isinstance(value, Mapping):
        raise ConfigError(f"{key}: must be an object; fix: provide the {key} settings object")
    return dict(value)


_SCORING_KEYS = {
    "include_weight",
    "language_weight",
    "release_group_weight",
    "resolution_weight",
    "collection_penalty",
}
_LIMIT_KEYS = {
    "network_timeout_seconds": "network_timeout_seconds",
    "max_read_retries": "max_read_retries",
    "max_task_pages": "max_task_pages",
    "max_recursive_depth": "max_recursive_depth",
    "recursive_scan_max_depth": "max_recursive_depth",
    "max_recursive_files": "max_recursive_files",
    "recursive_scan_max_files": "max_recursive_files",
    "max_new_tasks_per_run": "max_new_tasks_per_run",
    "global_submission_limit": "max_new_tasks_per_run",
    "max_global_submissions": "max_new_tasks_per_run",
    "max_new_submissions_per_run": "max_new_tasks_per_run",
    "max_new_tasks_per_subscription": "max_new_tasks_per_subscription",
    "per_subscription_submission_limit": "max_new_tasks_per_subscription",
    "max_submissions_per_subscription": "max_new_tasks_per_subscription",
    "max_new_submissions_per_subscription": "max_new_tasks_per_subscription",
    "max_review_batch": "max_review_batch",
    "max_reconcile_batch": "max_reconcile_batch",
}
_RETRY_INT_KEYS = {
    "max_attempts": "max_attempts",
    "max_retries": "max_attempts",
    "max_read_retries": "max_read_retries",
}
_RETRY_FLOAT_KEYS = {
    "backoff_seconds": ("backoff_seconds", True),
    "backoff_initial_seconds": ("backoff_seconds", True),
    "base_delay_seconds": ("backoff_seconds", True),
    "backoff_max_seconds": ("backoff_max_seconds", False),
    "backoff_cap_seconds": ("backoff_max_seconds", False),
    "max_delay_seconds": ("backoff_max_seconds", False),
    "jitter_seconds": ("jitter_seconds", True),
    "backoff_jitter_seconds": ("jitter_seconds", True),
}


def _merge_alias_sections(
    raw: Mapping[str, Any], aliases: tuple[str, ...], canonical: str
) -> dict[str, Any]:
    """Normalize one semantic section while validating every present alias.

    The loader and migration path share this function so aliases cannot hide a
    malformed canonical section.  Aliases may split a section's fields, but a
    field supplied by more than one alias must have the same value.
    """
    merged: dict[str, Any] = {}
    origins: dict[str, str] = {}
    for alias in aliases:
        if alias not in raw:
            continue
        value = raw[alias]
        if not isinstance(value, Mapping):
            raise ConfigError(
                f"{alias}: must be an object; fix: provide the {canonical} settings object"
            )
        normalized = _normalize_alias_section(alias, canonical, value)
        for key, item in normalized.items():
            if key in merged and merged[key] != item:
                raise ConfigError(
                    f"{canonical}.{key}: conflicts with {origins[key]}; "
                    f"fix: keep one matching value for {canonical}.{key}"
                )
            merged[key] = item
            origins[key] = f"{alias}.{key}"
    return merged


def _normalize_alias_section(
    alias: str, canonical: str, value: Mapping[str, Any]
) -> dict[str, Any]:
    """Validate present values in one alias and return loader-shaped fields."""
    section = dict(value)
    items: list[tuple[str, Any]]
    if canonical == "scoring" and "weights" in section:
        weights = section["weights"]
        if not isinstance(weights, Mapping):
            raise ConfigError(
                f"{alias}.weights: must be an object; fix: provide scoring weights"
            )
        # Validate both representations before applying the loader's nested
        # weight precedence.  An invalid top-level value must not be hidden by
        # a valid value with the same key inside ``weights``.
        for key, item in section.items():
            if key != "weights":
                _validate_alias_value(canonical, key, item)
        for key, item in weights.items():
            _validate_alias_value(canonical, key, item)
        # Preserve the historical nested-weight precedence only for the
        # unambiguous case; differing values for one semantic field are a
        # migration conflict rather than a value to choose silently.
        items = [(key, item) for key, item in section.items() if key != "weights"]
        items.extend(weights.items())
    else:
        items = list(section.items())

    normalized: dict[str, Any] = {}
    origins: dict[str, str] = {}
    for key, item in items:
        _validate_alias_value(canonical, key, item)
        target = _canonical_alias_key(canonical, key)
        if target in normalized and normalized[target] != item:
            raise ConfigError(
                f"{canonical}.{target}: conflicts with {alias}.{origins[target]}; "
                f"fix: keep one matching value for {canonical}.{target}"
            )
        normalized[target] = item
        origins[target] = key
    return normalized


def _canonical_alias_key(canonical: str, key: str) -> str:
    if canonical == "limits":
        return _LIMIT_KEYS.get(key, key)
    if canonical == "retry":
        if key in _RETRY_INT_KEYS:
            return _RETRY_INT_KEYS[key]
        if key in _RETRY_FLOAT_KEYS:
            return _RETRY_FLOAT_KEYS[key][0]
    return key


def _validate_alias_value(canonical: str, key: str, value: Any) -> None:
    if canonical == "scoring" and key in _SCORING_KEYS:
        _require_alias_positive_int(f"scoring.{key}", value)
        return

    if canonical == "limits" and key in _LIMIT_KEYS:
        _require_alias_positive_int(f"limits.{_LIMIT_KEYS[key]}", value)
        return

    if canonical == "retry":
        if key in _RETRY_INT_KEYS:
            _require_alias_positive_int(f"retry.{_RETRY_INT_KEYS[key]}", value)
            return
        if key in _RETRY_FLOAT_KEYS:
            target, allow_zero = _RETRY_FLOAT_KEYS[key]
            _require_alias_float(f"retry.{target}", value, allow_zero=allow_zero)


def _require_alias_positive_int(field: str, value: Any) -> None:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{field}: must be positive; fix: set an integer greater than zero")


def _require_alias_float(field: str, value: Any, *, allow_zero: bool) -> None:
    if isinstance(value, bool):
        raise ConfigError(f"{field}: must be a finite number; fix: set a valid retry duration")
    try:
        number = float(value)
    except (TypeError, ValueError):
        raise ConfigError(f"{field}: must be a finite number; fix: set a valid retry duration")
    if not math.isfinite(number):
        raise ConfigError(f"{field}: must be finite; fix: use a finite retry duration")
    if number < 0 or (number == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "positive"
        raise ConfigError(f"{field}: must be {qualifier}; fix: set a valid retry duration")


def _nonempty_runtime_token(values: Mapping[str, Any], key: str, default: str) -> str:
    value = values.get(key, default)
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9_-]{1,80}", value):
        raise ConfigError(f"runtime.{key}: must be a short event token")
    return value


def _path(values: Mapping[str, Any], key: str, *, default: str | None = None) -> str:
    if key not in values and default is None:
        raise ConfigError(f"paths.{key}: required; fix: add an absolute POSIX path root")
    value = values.get(key, default)
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"paths.{key}: required; fix: add an absolute POSIX path root")
    value = value.strip()
    if not value.startswith("/") or "\\" in value or "\x00" in value:
        raise ConfigError(f"paths.{key}: must be an absolute POSIX path; fix: use a path beginning with /")
    if any(part == ".." for part in value.split("/")):
        raise ConfigError(f"paths.{key}: traversal is not allowed; fix: remove .. path segments")
    return posixpath.normpath(value)


def _component(values: Mapping[str, Any], key: str) -> str | None:
    """Parse one optional single path component below ``paths``.

    Used for ``litepan_publish_subdir``: the legacy v3 published layout inserts
    a directory (``整理``) between the quarter label and the work.  Also used
    for ``cloud_library_subdir``, which inserts the same kind of directory into
    every generated *cloud* target.  The value must be exactly one safe
    component so it can never escape or restructure a configured root.
    """

    value = values.get(key)
    if value is None:
        return None
    if not isinstance(value, str) or not value.strip():
        raise ConfigError(f"paths.{key}: must be a non-empty path component")
    value = value.strip()
    if (
        "/" in value
        or "\\" in value
        or "\x00" in value
        or "?" in value
        or "#" in value
        or "://" in value
        or value in {".", ".."}
        or ":" in value
    ):
        raise ConfigError(
            f"paths.{key}: must be a single safe path component; fix: remove separators"
        )
    return value


def _validate_distinct_roots(paths: PathConfig) -> None:
    if paths.cloud_source_root == paths.cloud_library_root:
        raise ConfigError("paths.cloud_library_root: must differ from cloud_source_root")
    if paths.litepan_raw_output_root:
        raw = paths.litepan_raw_output_root.rstrip("/")
        published = paths.litepan_output_root.rstrip("/")
        if raw == published or raw.startswith(published + "/") or published.startswith(raw + "/"):
            raise ConfigError(
                "paths.litepan_raw_output_root: must not overlap litepan_output_root"
            )


def _validate_litepan_geometry(paths: PathConfig, runtime: RuntimeConfig) -> None:
    if not paths.litepan_raw_output_root:
        return
    source = runtime.litepan_task_source_root or paths.cloud_library_root
    output = runtime.litepan_task_output_root or paths.litepan_raw_output_root
    library = paths.cloud_library_root
    if source == library or library.startswith(source.rstrip("/") + "/"):
        relative = library[len(source):].lstrip("/")
        expected = posixpath.normpath(posixpath.join(output, relative))
        if expected != paths.litepan_raw_output_root:
            raise ConfigError(
                "paths.litepan_raw_output_root: must map cloud_library_root inside LitePan task output"
            )
        return
    prefix = library.rstrip("/") + "/"
    label = source[len(prefix):] if source.startswith(prefix) else ""
    if label != "<quarter-label>" and not re.fullmatch(r"[0-9]{4}-(?:01|04|07|10)月新番", label):
        raise ConfigError("runtime.litepan_task_source_root: must select one quarter below cloud_library_root")
    if posixpath.normpath(posixpath.join(paths.litepan_raw_output_root, label)) != output:
        raise ConfigError("runtime.litepan_task_output_root: must map the selected quarter inside litepan_raw_output_root")


def _positive(values: Mapping[str, Any], key: str, section: str) -> int:
    if key not in values:
        raise ConfigError(f"{section}.{key}: required; fix: set a positive integer")
    value = values[key]
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ConfigError(f"{section}.{key}: must be positive; fix: set an integer greater than zero")
    return value


def _positive_alias(values: Mapping[str, Any], keys: tuple[str, ...], field: str,
                    *, default: int | None = None) -> int:
    for key in keys:
        if key in values:
            return _positive(values, key, field.rsplit(".", 1)[0])
    if default is not None:
        return default
    raise ConfigError(f"{field}: required; fix: set a positive integer")


def _positive_float_alias(values: Mapping[str, Any], keys: tuple[str, ...], field: str,
                         *, allow_zero: bool, default: float | None = None) -> float:
    for key in keys:
        if key in values:
            value = values[key]
            if isinstance(value, bool):
                raise ConfigError(
                    f"{field}: must be a finite number; fix: set a valid retry duration"
                )
            try:
                number = float(value)
            except (TypeError, ValueError):
                raise ConfigError(
                    f"{field}: must be a finite number; fix: set a valid retry duration"
                )
            if not math.isfinite(number):
                raise ConfigError(
                    f"{field}: must be finite; fix: use a finite retry duration"
                )
            if number < 0 or (number == 0 and not allow_zero):
                qualifier = "non-negative" if allow_zero else "positive"
                raise ConfigError(
                    f"{field}: must be {qualifier}; fix: set a valid retry duration"
                )
            return number
    if default is not None:
        return default
    qualifier = "non-negative" if allow_zero else "positive"
    raise ConfigError(f"{field}: must be {qualifier}; fix: set a valid retry duration")


def _first_nonempty(*values: Any) -> Any:
    for value in values:
        if value not in (None, ""):
            text = str(value).strip()
            if text:
                return value
    return None


def _remove_subscription_catalogues(
    value: Any,
    path: tuple[str, ...],
    dropped: list[str],
    deprecated: list[str],
    identities: list[str],
) -> None:
    if isinstance(value, dict):
        for key in list(value):
            current = path + (str(key),)
            if key == "subscriptions":
                catalogue = value.pop(key)
                if isinstance(catalogue, list):
                    for index, subscription in enumerate(catalogue):
                        label = _indexed_path(current, index)
                        dropped.append(label)
                        deprecated.append(label)
                        identity = _subscription_identity(subscription)
                        if identity and identity not in identities:
                            identities.append(identity)
                else:
                    label = ".".join(current)
                    dropped.append(label)
                    deprecated.append(label)
                continue
            _remove_subscription_catalogues(value[key], current, dropped, deprecated, identities)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _remove_subscription_catalogues(item, path + (f"[{index}]",), dropped, deprecated, identities)


def _indexed_path(path: tuple[str, ...], index: int) -> str:
    return ".".join(path[:-1] + (f"{path[-1]}[{index}]",))


def _subscription_identity(subscription: Any) -> str | None:
    if not isinstance(subscription, Mapping):
        return None
    media_id = _first_nonempty(
        subscription.get("media_id"), subscription.get("tmdbid"), subscription.get("tmdb_id"),
        subscription.get("douban_id"), subscription.get("id"),
    )
    if media_id is None:
        return None
    season = subscription.get("season", subscription.get("season_number", 1))
    try:
        season_text = str(int(season))
    except (TypeError, ValueError):
        season_text = str(season).strip() or "?"
    return f"{str(media_id).strip()}:{season_text}"


def _migration_blockers(mapping: Mapping[str, Any]) -> tuple[str, ...]:
    """Validate the canonicalized candidate with the real V5 loader."""
    try:
        candidate = _canonicalize_migration_mapping(mapping)
        ConfigSchema.from_mapping(candidate, {}, "linux")
    except ConfigError as error:
        fields = re.findall(
            r"(?m)^\s*([A-Za-z_][A-Za-z0-9_.]*(?:\[\d+\])?):",
            str(error),
        )
        return tuple(dict.fromkeys(fields or ("config",)))
    return ()


def _canonicalize_migration_mapping(mapping: Mapping[str, Any]) -> dict[str, Any]:
    """Build the exact shape consumed by ``ConfigSchema.from_mapping``.

    Legacy aliases are merged before validation so migration diagnostics cannot
    disagree with the loader about which sections are present.
    """
    candidate = deepcopy(dict(mapping))
    candidate["schema_version"] = 5
    candidate.pop("migration_blocked", None)
    candidate.pop("migration_blocking_fields", None)

    services = _service_mapping(candidate)
    if services:
        candidate["services"] = {
            name: deepcopy(services[name])
            for name in ("moviepilot", "cloud", "litepan", "emby")
            if isinstance(services.get(name), Mapping)
        }

    if any(key in candidate for key in _SCORING_SECTION_ALIASES):
        candidate["scoring"] = _merge_alias_sections(
            candidate, _SCORING_SECTION_ALIASES, "scoring"
        )
    if any(key in candidate for key in _LIMIT_SECTION_ALIASES):
        candidate["limits"] = _merge_alias_sections(
            candidate, _LIMIT_SECTION_ALIASES, "limits"
        )
    if any(key in candidate for key in _RETRY_SECTION_ALIASES):
        candidate["retry"] = _merge_alias_sections(
            candidate, _RETRY_SECTION_ALIASES, "retry"
        )
    return candidate


def _drop_runtime_ids(value: Any, path: tuple[str, ...], dropped: list[str], deprecated: list[str]) -> None:
    if isinstance(value, dict):
        for key in list(value):
            current = path + (str(key),)
            if key in _RUNTIME_ID_KEYS:
                dotted = ".".join(current)
                dropped.append(dotted)
                deprecated.append(dotted)
                del value[key]
                continue
            _drop_runtime_ids(value[key], current, dropped, deprecated)
    elif isinstance(value, list):
        for index, item in enumerate(value):
            _drop_runtime_ids(item, path + (f"[{index}]",), dropped, deprecated)


def _first_runtime_id(value: Any, path: tuple[str, ...] = ()) -> tuple[str, str] | None:
    """Find the first generated ID in a mapping for strict config checks."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            current = path + (str(key),)
            if key in _RUNTIME_ID_KEYS:
                return ".".join(current), str(key)
            found = _first_runtime_id(item, current)
            if found is not None:
                return found
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found = _first_runtime_id(item, path + (f"[{index}]",))
            if found is not None:
                return found
    return None


def _first_subscription_catalogue(value: Any, path: tuple[str, ...] = ()) -> str | None:
    """Return the first nested local subscription catalogue path."""
    if isinstance(value, Mapping):
        for key, item in value.items():
            current = path + (str(key),)
            if key == "subscriptions":
                return ".".join(current)
            found = _first_subscription_catalogue(item, current)
            if found is not None:
                return found
    elif isinstance(value, list):
        for index, item in enumerate(value):
            found = _first_subscription_catalogue(item, path + (f"[{index}]",))
            if found is not None:
                return found
    return None


def _ensure_service_names(migrated: dict[str, Any]) -> None:
    services = migrated.get("services")
    defaults = {"moviepilot": "moviepilot", "cloud": "p115", "p115": "p115",
                "storage": "p115", "litepan": "litepan", "emby": "emby"}
    # Apply migration-only defaults to every alias before semantic comparison.
    # Never replace a malformed services wrapper with a synthesized mapping.
    containers = [migrated]
    if isinstance(services, dict):
        containers.append(services)
    for container in containers:
        for key, default in defaults.items():
            value = container.get(key)
            if isinstance(value, dict) and not value.get("name"):
                value["name"] = default


__all__ = [
    "ConfigError",
    "ConfigMigrationReport",
    "ConfigSchema",
    "ServiceConfig",
    "ServicesConfig",
    "PathConfig",
    "ScoringConfig",
    "LimitsConfig",
    "RetryConfig",
    "SourceConfig",
    "RuntimeConfig",
    "ScheduleConfig",
    "load_config",
    "migrate_legacy_config",
]
