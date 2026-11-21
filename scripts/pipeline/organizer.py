"""Crash-safe, idempotent organization of selected 115 files."""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
import inspect
import json
import posixpath
import time
from typing import Any, Iterable, Optional

from .adapters import RemotePathConflict, RemoteRateLimited
from .models import EpisodeStatus, ReviewReason
from .paths import PathMapper, PathMappingError, SeasonRoutingError
from .safe_diagnostics import safe_exception_diagnostics
from .security import sanitize_text
from .throttle import (
    CloudCooldownStateError,
    CloudThrottleState,
    ThrottleDecision,
    throttle_decision,
)


@dataclass(frozen=True, slots=True)
class OrganizeResult:
    """Safe evidence returned by one bounded organizer invocation."""

    status: str
    cloud_path: str = ""
    litepan_path: str = ""
    moviepilot_path: str = ""
    emby_path: str = ""
    review_reason: ReviewReason | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)
    mkdir_count: int = 0
    move_count: int = 0
    rename_count: int = 0

    @property
    def writes(self) -> int:
        """Number of remote write calls made during this invocation."""

        return self.mkdir_count + self.move_count + self.rename_count

    @property
    def write_count(self) -> int:
        return self.writes

    @property
    def cloud_write_count(self) -> int:
        return self.writes

    @property
    def target_path(self) -> str:
        return self.cloud_path

    @property
    def strm_path(self) -> str:
        return self.litepan_path

    @property
    def reason(self) -> ReviewReason | None:
        return self.review_reason

    @property
    def organized(self) -> bool:
        return self.status == EpisodeStatus.ORGANIZED.value


class Organizer:
    """Reconcile a selected remote file with its canonical cloud target.

    The class only depends on the small business-level methods exposed by the
    cloud adapter.  Tests and future adapters can provide ``get_file`` plus
    ``mkdir``/``move_file``/``rename_file``; read-only inspection is attempted
    before every remote write so an interrupted process can safely resume.
    """

    def __init__(self, store: Any, cloud: Any, mapper: PathMapper, *, secrets: Iterable[Any] = (),
                 throttle_state: CloudThrottleState | None = None, jitter_seconds: float = 0.0, config: Any = None):
        self.store = store
        self.cloud = cloud
        self.mapper = mapper
        self.secrets = tuple(secrets)
        self.throttle_state = throttle_state or CloudThrottleState()
        self.jitter_seconds = jitter_seconds
        self.config = config

    def reconcile_or_apply(self, episode: Any, *, _stage_only: bool = False) -> OrganizeResult:
        # Account-level 115 cooldown: defer the whole organize walk (reads and
        # writes alike) without consuming the episode's retry attempts.
        if self.throttle_state.active(time.time()):
            return OrganizeResult(
                status=EpisodeStatus.ORGANIZE_PENDING.value,
                detail={"reason": "cloud_check_cooldown"},
            )
        row = self._episode_row(episode)
        if not _stage_only and callable(getattr(self.cloud, "supports_flat_season_move", None)):
            if not self.cloud.supports_flat_season_move():
                return OrganizeResult(status=EpisodeStatus.ORGANIZE_PENDING.value,
                                      detail={"reason": "flat_move_capability_missing"})
            return self._reconcile_flat_layout(row)
        episode_id = _episode_id(row)
        source = _selected_source(row)
        if not source:
            selected_reader = getattr(self.store, "get_selected_download_file", None)
            if callable(selected_reader):
                source = selected_reader(episode_id) or source
        if not _routing_evidence(row, source).get("_season_air_date"):
            season_date_reader = getattr(self.store, "get_subscription_season_air_date", None)
            if callable(season_date_reader):
                season_date = season_date_reader(episode_id)
                if season_date:
                    row = dict(row)
                    row["season_air_date"] = season_date
        source_identity = _identity(row, source)
        source_path = _source_path(row, source)
        routing_evidence = _routing_evidence(row, source)
        name_intent: dict[str, Any] | None = None
        from_cloud_path = ""

        def pending_identity() -> dict[str, Any]:
            value = _runtime_identity_mapping(row)
            value.update(source_identity)
            value.update(routing_evidence)
            existing_dir_id = _runtime_identity_mapping(row).get("_target_directory_id")
            if existing_dir_id not in (None, ""):
                value["_target_directory_id"] = str(existing_dir_id)
            if name_intent is not None:
                value["_cloud_name_intent"] = dict(name_intent)
            return value

        try:
            self._validate_source_path(source_path)
        except PathMappingError as exc:
            detail = self._safe_detail({"reason": "SOURCE_PATH_INVALID", "error": exc})
            self._mark_pending(episode_id, pending_identity(), source_path, "", detail)
            self._open_review(episode_id, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS, detail)
            return OrganizeResult(
                status=EpisodeStatus.NEEDS_REVIEW.value,
                review_reason=ReviewReason.RUNTIME_MAPPING_AMBIGUOUS,
                detail=detail,
            )

        try:
            isolated_move = callable(getattr(self.cloud, "supports_isolated_move", None)) and bool(
                self.cloud.supports_isolated_move()
            )
            if isolated_move:
                name_paths = self.mapper.isolated_name_paths_for_episode(row, source)
                from_cloud_path = name_paths["from_cloud_path"]
                cloud_path = name_paths["to_cloud_path"]
                runtime = _runtime_identity_mapping(row)
                prior_intent = runtime.get("_cloud_name_intent")
                current_status = row.get("status")
                current_status = getattr(current_status, "value", current_status)
                if prior_intent is not None:
                    if not isinstance(prior_intent, Mapping):
                        raise PathMappingError("cloud_name_intent: malformed persisted value")
                    name_intent = dict(prior_intent)
                    intent_source = name_intent.get("source_path")
                    if intent_source not in (None, ""):
                        source_path = str(intent_source)
                else:
                    name_intent = {
                        "version": 1,
                        "from_cloud_path": from_cloud_path,
                        "to_cloud_path": cloud_path,
                        "source_path": source_path,
                        "original_status": str(current_status or ""),
                        "rename_verified": False,
                    }
                    if source_identity.get("remote_file_id") not in (None, ""):
                        name_intent["file_id"] = source_identity["remote_file_id"]
                    if source_identity.get("size") not in (None, ""):
                        normalized_size = _positive_size_value(source_identity["size"])
                        if normalized_size is None:
                            raise PathMappingError(
                                "cloud_name_intent: size must be a positive integer"
                            )
                        name_intent["size"] = normalized_size
                    if source_identity.get("fingerprint") not in (None, ""):
                        name_intent["fingerprint"] = source_identity["fingerprint"]
                    existing_dir_id = runtime.get("_target_directory_id")
                    if existing_dir_id not in (None, ""):
                        name_intent["directory_id"] = str(existing_dir_id)
                    published_hash = runtime.get("published_sha256_before")
                    if published_hash not in (None, ""):
                        name_intent["published_sha256_before"] = published_hash
                saved_size = name_intent.get("size")
                if saved_size not in (None, ""):
                    normalized_size = _positive_size_value(saved_size)
                    if normalized_size is None:
                        raise PathMappingError(
                            "cloud_name_intent: size must be a positive integer"
                        )
                    name_intent["size"] = normalized_size
                if not name_intent.get("source_path") and source_path:
                    name_intent["source_path"] = source_path
                if not name_intent.get("file_id") and source_identity.get("remote_file_id"):
                    name_intent["file_id"] = source_identity["remote_file_id"]
                if name_intent.get("size") in (None, "") and source_identity.get("size") not in (None, ""):
                    normalized_size = _positive_size_value(source_identity["size"])
                    if normalized_size is None:
                        raise PathMappingError(
                            "cloud_name_intent: size must be a positive integer"
                        )
                    name_intent["size"] = normalized_size
                if (
                    name_intent.get("from_cloud_path") != from_cloud_path
                    or name_intent.get("to_cloud_path") != cloud_path
                ):
                    raise PathMappingError("cloud_name_intent: persisted paths changed")
                intent_dir_id = name_intent.get("directory_id")
                runtime_dir_id = runtime.get("_target_directory_id")
                if (
                    intent_dir_id not in (None, "")
                    and runtime_dir_id not in (None, "")
                    and str(intent_dir_id) != str(runtime_dir_id)
                ):
                    raise PathMappingError("cloud_name_intent: directory identity mismatch")
                self._validate_source_path(source_path)
            else:
                cloud_path = self.mapper.canonical_for_episode(row, source)
            litepan_path = (
                self.mapper.cloud_to_published_litepan(cloud_path, row)
                if isolated_move and posixpath.basename(posixpath.dirname(cloud_path)).startswith("fid-")
                else self.mapper.cloud_to_litepan(cloud_path)
            )
            moviepilot_path = self.mapper.litepan_to_moviepilot(litepan_path)
            emby_path = self.mapper.litepan_to_emby(litepan_path)
        except (PathMappingError, SeasonRoutingError, ValueError, TypeError) as exc:
            detail = self._safe_detail({"reason": "RUNTIME_MAPPING_INVALID", "error": exc})
            self._mark_pending(episode_id, pending_identity(), source_path, "", detail)
            self._open_review(episode_id, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS, detail)
            return OrganizeResult(
                status=EpisodeStatus.NEEDS_REVIEW.value,
                review_reason=ReviewReason.RUNTIME_MAPPING_AMBIGUOUS,
                detail=detail,
            )

        # Persist the exact canonical target before any mkdir/move/rename write.
        self._mark_pending(episode_id, pending_identity(), source_path, cloud_path)

        if isolated_move and not posixpath.basename(posixpath.dirname(cloud_path)).startswith("Season "):
            return self._reconcile_isolated(
                episode_id, row, source_path, cloud_path, litepan_path,
                moviepilot_path, emby_path, source_identity, pending_identity(),
                from_cloud_path=from_cloud_path,
                name_intent=name_intent,
            )

        try:
            target = self._inspect(cloud_path, expected_identity=None)
            source_file = self._inspect(source_path, expected_identity=source_identity)
        except RemoteRateLimited as exc:
            decision = self._throttled_pending(
                episode_id, pending_identity(), source_path, cloud_path, exc
            )
            if decision.escalated:
                return OrganizeResult(
                    status=EpisodeStatus.NEEDS_REVIEW.value,
                    cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                    litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                    moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                    emby_path=sanitize_text(emby_path, secrets=self.secrets),
                    review_reason=ReviewReason.UNEXPECTED_REMOTE_STATE,
                    detail=decision.detail,
                )
            return self._pending_result(
                cloud_path, litepan_path, moviepilot_path, emby_path, decision.detail
            )
        except CloudCooldownStateError:
            raise
        except Exception as exc:
            detail = self._safe_detail({
                "reason": "ORGANIZE_READ_FAILED",
                "error": exc,
                "cloud_path": cloud_path,
            })
            self._mark_pending(episode_id, pending_identity(), source_path, cloud_path, detail)
            return self._pending_result(
                cloud_path, litepan_path, moviepilot_path, emby_path, detail
            )
        target_identity = _identity({}, target)

        if target is not None:
            if _same_identity(source_identity, target_identity, source_path, cloud_path):
                return self._organized(
                    episode_id,
                    cloud_path,
                    litepan_path,
                    moviepilot_path,
                    emby_path,
                    source_identity,
                    {"reconciled": True, "move": "skipped"},
                )
            # A target exists but cannot be proven to be the selected source.
            # Never delete or overwrite it: leave an explicit operator review.
            detail = self._safe_detail({
                "reason": ReviewReason.PATH_CONFLICT.value,
                "target": target_identity,
                "source": source_identity,
                "cloud_path": cloud_path,
            })
            self._open_review(episode_id, ReviewReason.PATH_CONFLICT, detail)
            return OrganizeResult(
                status=EpisodeStatus.NEEDS_REVIEW.value,
                cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                emby_path=sanitize_text(emby_path, secrets=self.secrets),
                review_reason=ReviewReason.PATH_CONFLICT,
                detail=detail,
            )

        # A process can crash after move and before rename/verification.  On
        # restart the source is gone, the canonical target is still absent,
        # and the remote file may be sitting in the canonical directory under
        # its old basename.  Reconcile that identity before treating the
        # source as missing; this path never issues a second move.
        if target is None and source_file is None and _identity_has_evidence(source_identity):
            parent_path = posixpath.dirname(cloud_path)
            try:
                moved = self._inspect_by_identity(source_identity, parent_path)
            except RemoteRateLimited as exc:
                decision = self._throttled_pending(
                    episode_id, pending_identity(), source_path, cloud_path, exc
                )
                if decision.escalated:
                    return OrganizeResult(
                        status=EpisodeStatus.NEEDS_REVIEW.value,
                        cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                        litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                        moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                        emby_path=sanitize_text(emby_path, secrets=self.secrets),
                        review_reason=ReviewReason.UNEXPECTED_REMOTE_STATE,
                        detail=decision.detail,
                    )
                return self._pending_result(
                    cloud_path, litepan_path, moviepilot_path, emby_path, decision.detail
                )
            except CloudCooldownStateError:
                raise
            except Exception as exc:
                detail = self._safe_detail({
                    "reason": "ORGANIZE_READ_FAILED",
                    "error": exc,
                    "cloud_path": cloud_path,
                })
                self._mark_pending(episode_id, pending_identity(), source_path, cloud_path, detail)
                return self._pending_result(
                    cloud_path, litepan_path, moviepilot_path, emby_path, detail
                )
            if moved is not None:
                counts = {"mkdir": 0, "move": 0, "rename": 0}
                try:
                    moved_name = _field(moved, "name", "filename")
                    expected_name = posixpath.basename(cloud_path)
                    moved_id = _field(moved, "remote_file_id", "file_id", "id")
                    if moved_name and str(moved_name) != expected_name:
                        if not moved_id:
                            raise RuntimeError("moved remote source has no stable file identity")
                        counts["rename"] += 1
                        self._rename(
                            moved_id,
                            expected_name,
                            expected_identity=source_identity,
                            target_path=cloud_path,
                        )
                    verified = self._inspect(cloud_path, expected_identity=source_identity)
                    if verified is None or not _same_identity(
                        source_identity, _identity({}, verified), "", cloud_path
                    ):
                        raise RuntimeError("reconciled target identity could not be verified")
                    return self._organized(
                        episode_id,
                        cloud_path,
                        litepan_path,
                        moviepilot_path,
                        emby_path,
                        source_identity,
                        {"reconciled": True, "move": "already_applied"},
                        counts,
                    )
                except RemotePathConflict as exc:
                    detail = self._safe_detail({
                        "reason": ReviewReason.PATH_CONFLICT.value,
                        "error": exc,
                        "cloud_path": cloud_path,
                    })
                    self._open_review(episode_id, ReviewReason.PATH_CONFLICT, detail)
                    return OrganizeResult(
                        status=EpisodeStatus.NEEDS_REVIEW.value,
                        cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                        litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                        moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                        emby_path=sanitize_text(emby_path, secrets=self.secrets),
                        review_reason=ReviewReason.PATH_CONFLICT,
                        detail=detail,
                        mkdir_count=counts["mkdir"],
                        move_count=counts["move"],
                        rename_count=counts["rename"],
                    )
                except RemoteRateLimited as exc:
                    decision = self._throttled_pending(
                        episode_id, pending_identity(), source_path, cloud_path, exc
                    )
                    if decision.escalated:
                        return OrganizeResult(
                            status=EpisodeStatus.NEEDS_REVIEW.value,
                            cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                            litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                            moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                            emby_path=sanitize_text(emby_path, secrets=self.secrets),
                            review_reason=ReviewReason.UNEXPECTED_REMOTE_STATE,
                            detail=decision.detail,
                            mkdir_count=counts["mkdir"],
                            move_count=counts["move"],
                            rename_count=counts["rename"],
                        )
                    return OrganizeResult(
                        status=EpisodeStatus.ORGANIZE_PENDING.value,
                        cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                        litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                        moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                        emby_path=sanitize_text(emby_path, secrets=self.secrets),
                        detail=decision.detail,
                        mkdir_count=counts["mkdir"],
                        move_count=counts["move"],
                        rename_count=counts["rename"],
                    )
                except CloudCooldownStateError:
                    raise
                except Exception as exc:
                    detail = self._safe_detail({
                        "reason": "ORGANIZE_RECONCILE_FAILED",
                        "error": exc,
                        "cloud_path": cloud_path,
                    })
                    self._mark_pending(episode_id, pending_identity(), source_path, cloud_path, detail)
                    return OrganizeResult(
                        status=EpisodeStatus.ORGANIZE_PENDING.value,
                        cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                        litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                        moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                        emby_path=sanitize_text(emby_path, secrets=self.secrets),
                        detail=detail,
                        mkdir_count=counts["mkdir"],
                        move_count=counts["move"],
                        rename_count=counts["rename"],
                    )

        # A restart may find the target but not the source.  The target branch
        # above reconciles it when persisted file identity proves equivalence;
        # absence of both sides remains recoverable rather than terminal.
        if source_file is None:
            detail = self._safe_detail({
                "reason": "SOURCE_NOT_FOUND",
                "source_path": source_path,
                "cloud_path": cloud_path,
            })
            self._mark_pending(episode_id, pending_identity(), source_path, cloud_path, detail)
            return self._pending_result(
                cloud_path, litepan_path, moviepilot_path, emby_path, detail
            )

        actual_source_identity = _identity(source_file, source_file)
        if not _identity_has_evidence(source_identity):
            source_identity = actual_source_identity
            self._mark_pending(episode_id, pending_identity(), source_path, cloud_path)
        elif not _same_identity(source_identity, actual_source_identity, source_path, source_path):
            detail = self._safe_detail({
                "reason": "SOURCE_IDENTITY_CHANGED",
                "expected": source_identity,
                "actual": actual_source_identity,
            })
            self._mark_pending(episode_id, pending_identity(), source_path, cloud_path, detail)
            return self._pending_result(
                cloud_path, litepan_path, moviepilot_path, emby_path, detail
            )

        counts = {"mkdir": 0, "move": 0, "rename": 0}
        try:
            parent_path = posixpath.dirname(cloud_path)
            counts["mkdir"] += int(self._has_directory_writer())
            directory_id = self._ensure_directory(parent_path)
            race_target = self._inspect(cloud_path, expected_identity=None)
            if race_target is not None and not _same_identity(
                source_identity, _identity({}, race_target), "", cloud_path
            ):
                raise RemotePathConflict("remote target appeared before move")
            source_ref = source_identity.get("remote_file_id") or _field(source_file, "remote_file_id", "file_id", "id")
            if not source_ref:
                raise RuntimeError("selected remote source has no stable file identity")
            counts["move"] += 1
            self._move(source_ref, directory_id)

            # Re-read after move.  Some remote APIs retain the old basename in
            # the new directory, so locate the moved file by identity before a
            # rename.  This read is mandatory evidence, not a best-effort log.
            moved = self._inspect(cloud_path, expected_identity=None)
            if moved is not None and not _same_identity(
                source_identity, _identity({}, moved), "", cloud_path
            ):
                raise RemotePathConflict("remote target appeared during organize")
            if moved is None:
                moved = self._inspect_by_identity(source_identity, parent_path)
            if moved is None:
                raise RuntimeError("remote move completed without readable target evidence")

            moved_name = _field(moved, "name", "filename")
            expected_name = posixpath.basename(cloud_path)
            moved_id = _field(moved, "remote_file_id", "file_id", "id") or source_ref
            if moved_name and str(moved_name) != expected_name:
                counts["rename"] += 1
                self._rename(
                    moved_id,
                    expected_name,
                    expected_identity=source_identity,
                    target_path=cloud_path,
                )

            verified = self._inspect(cloud_path, expected_identity=source_identity)
            if verified is None:
                raise RuntimeError("remote rename completed without readable target evidence")
            verified_identity = _identity({}, verified)
            if not _same_identity(source_identity, verified_identity, source_path, cloud_path):
                detail = self._safe_detail({
                    "reason": ReviewReason.PATH_CONFLICT.value,
                    "source": source_identity,
                    "target": verified_identity,
                    "cloud_path": cloud_path,
                })
                self._open_review(episode_id, ReviewReason.PATH_CONFLICT, detail)
                return OrganizeResult(
                    status=EpisodeStatus.NEEDS_REVIEW.value,
                    cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                    litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                    moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                    emby_path=sanitize_text(emby_path, secrets=self.secrets),
                    review_reason=ReviewReason.PATH_CONFLICT,
                    detail=detail,
                    mkdir_count=counts["mkdir"],
                    move_count=counts["move"],
                    rename_count=counts["rename"],
                )
        except RemotePathConflict as exc:
            detail = self._safe_detail({
                "reason": ReviewReason.PATH_CONFLICT.value,
                "error": exc,
                "cloud_path": cloud_path,
            })
            self._open_review(episode_id, ReviewReason.PATH_CONFLICT, detail)
            return OrganizeResult(
                status=EpisodeStatus.NEEDS_REVIEW.value,
                cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                emby_path=sanitize_text(emby_path, secrets=self.secrets),
                review_reason=ReviewReason.PATH_CONFLICT,
                detail=detail,
                mkdir_count=counts["mkdir"],
                move_count=counts["move"],
                rename_count=counts["rename"],
            )
        except RemoteRateLimited as exc:
            # Transport-level throttle: back off on the shared account ladder
            # instead of the episode's contradiction review budget.
            decision = self._throttled_pending(
                episode_id, pending_identity(), source_path, cloud_path, exc
            )
            if decision.escalated:
                return OrganizeResult(
                    status=EpisodeStatus.NEEDS_REVIEW.value,
                    cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                    litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                    moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                    emby_path=sanitize_text(emby_path, secrets=self.secrets),
                    review_reason=ReviewReason.UNEXPECTED_REMOTE_STATE,
                    detail=decision.detail,
                    mkdir_count=counts["mkdir"],
                    move_count=counts["move"],
                    rename_count=counts["rename"],
                )
            return OrganizeResult(
                status=EpisodeStatus.ORGANIZE_PENDING.value,
                cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                emby_path=sanitize_text(emby_path, secrets=self.secrets),
                detail=decision.detail,
                mkdir_count=counts["mkdir"],
                move_count=counts["move"],
                rename_count=counts["rename"],
            )
        except CloudCooldownStateError:
            raise
        except Exception as exc:
            detail = self._safe_detail({
                "reason": "ORGANIZE_WRITE_FAILED",
                "error": exc,
                "cloud_path": cloud_path,
            })
            self._mark_pending(episode_id, pending_identity(), source_path, cloud_path, detail)
            return OrganizeResult(
                status=EpisodeStatus.ORGANIZE_PENDING.value,
                cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
                litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                emby_path=sanitize_text(emby_path, secrets=self.secrets),
                detail=detail,
                mkdir_count=counts["mkdir"],
                move_count=counts["move"],
                rename_count=counts["rename"],
            )

        return self._organized(
            episode_id,
            cloud_path,
            litepan_path,
            moviepilot_path,
            emby_path,
            source_identity,
            {"reconciled": False, "writes": counts},
            counts,
        )

    def _reconcile_flat_layout(self, episode: Mapping[str, Any]) -> OrganizeResult:
        """Use isolated rename as staging; seal only a direct Season video."""
        from .cloud_layout import build_season_owners, prepare_layout_runtime
        from .cloud_naming import _identity as strict_identity
        from .paths import _remote_file_id_token

        row = self._episode_row(episode)
        episode_id = _episode_id(row)
        if str(getattr(row.get("status"), "value", row.get("status"))) == EpisodeStatus.NEEDS_REVIEW.value:
            return OrganizeResult(status=EpisodeStatus.NEEDS_REVIEW.value,
                                  detail={"reason": "review_requires_explicit_resolution"})
        if self.config is None:
            return OrganizeResult(status=EpisodeStatus.ORGANIZE_PENDING.value,
                                  detail={"reason": "flat_layout_config_missing"})
        selected_reader = getattr(self.store, "get_selected_download_file", None)
        selected = selected_reader(episode_id) if callable(selected_reader) else None
        if not isinstance(selected, Mapping):
            return OrganizeResult(status=EpisodeStatus.ORGANIZE_PENDING.value,
                                  detail={"reason": "flat_layout_selection_missing"})
        source_path = str(selected.get("path") or "")
        counts = {"mkdir": 0, "move": 0, "rename": 0}
        runtime = _runtime_identity_mapping(row)
        layout: dict[str, Any] = {}
        final_path = ""
        stage_path = ""
        litepan_path = ""
        moviepilot_path = ""
        emby_path = ""

        def persist(reason: str, *, cloud_path: str | None = None) -> None:
            runtime["_cloud_layout_intent"] = dict(layout)
            current_path = cloud_path or str(row.get("cloud_path") or stage_path)
            detail = {"reason": reason, "source_path": source_path,
                      "cloud_path": current_path, "identity": runtime}
            self._mark_pending(episode_id, runtime, source_path, current_path, detail)

        def pending(reason: str, error: BaseException | None = None) -> OrganizeResult:
            if layout:
                persist(reason, cloud_path=final_path if layout.get("verified") is True else stage_path)
            detail: dict[str, Any] = {"reason": reason}
            if error is not None:
                detail["error"] = error
            return OrganizeResult(
                status=EpisodeStatus.ORGANIZE_PENDING.value,
                cloud_path=sanitize_text(final_path, secrets=self.secrets),
                litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                emby_path=sanitize_text(emby_path, secrets=self.secrets),
                detail=self._safe_detail(detail), mkdir_count=counts["mkdir"],
                move_count=counts["move"], rename_count=counts["rename"],
            )

        try:
            date_reader = getattr(self.store, "get_subscription_season_air_date", None)
            season_date = date_reader(episode_id) if callable(date_reader) else ""
            season_date = season_date or _routing_evidence(row, selected).get("_season_air_date")
            row = {**row, "season_air_date": season_date}
            runtime = prepare_layout_runtime(row, selected, self.mapper, season_date)
            layout = dict(runtime["_cloud_layout_intent"])
            expected = strict_identity(selected)
            final_path = self.mapper.flat_for_episode(row, selected)
            stage_path = str(layout.get("from_cloud_path") or "")
            parent = posixpath.dirname(final_path)
            if (layout.get("to_cloud_path") != final_path
                    or layout.get("source_path") != source_path
                    or str(layout.get("file_id")) != expected["remote_file_id"]
                    or posixpath.dirname(posixpath.dirname(stage_path)) != parent
                    or posixpath.basename(posixpath.dirname(stage_path))
                        != _remote_file_id_token(expected["remote_file_id"])
                    or posixpath.basename(stage_path) != posixpath.basename(final_path)):
                raise PathMappingError("flat_layout: persisted target or selection changed")
            self._validate_source_path(source_path)
            litepan_path = self.mapper.cloud_to_litepan(final_path)
            moviepilot_path = self.mapper.litepan_to_moviepilot(litepan_path)
            emby_path = self.mapper.litepan_to_emby(litepan_path)
            # This durable final-target intent protects a crash after the old
            # staging Organizer emits its verified name-stage history.
            persist("flat_layout_intent", cloud_path=str(row.get("cloud_path") or stage_path))
            if layout.get("to_directory_id") not in (None, ""):
                season_id = str(self.cloud.resolve_directory_path(parent))
                if season_id != str(layout["to_directory_id"]):
                    raise RemotePathConflict("flat target directory identity changed")
            else:
                counts["mkdir"] += 1
                season_id = str(self._ensure_directory(parent))
                layout["to_directory_id"] = season_id
                persist("flat_season_directory_verified")

            current_final = self.cloud.find_file_by_identity(
                expected, parent, directory_id=season_id,
            )
            if current_final is None:
                staged = self.reconcile_or_apply(self.store.get_episode(episode_id), _stage_only=True)
                counts["mkdir"] += staged.mkdir_count
                counts["move"] += staged.move_count
                counts["rename"] += staged.rename_count
                if not staged.organized:
                    return staged
                row = self.store.get_episode(episode_id)
                runtime = _runtime_identity_mapping(row)
                layout = dict(runtime["_cloud_layout_intent"])
            elif current_final.get("path") != final_path or strict_identity(current_final) != expected:
                raise RemotePathConflict("flat target file identity changed")

            name_intent = runtime.get("_cloud_name_intent")
            if (not isinstance(name_intent, Mapping)
                    or type(name_intent.get("version")) is not int
                    or name_intent.get("version") != 1
                    or name_intent.get("rename_verified") is not True
                    or name_intent.get("to_cloud_path") != stage_path
                    or name_intent.get("source_path") != source_path
                    or str(name_intent.get("file_id")) != expected["remote_file_id"]
                    or name_intent.get("size") != expected["size"]
                    or name_intent.get("fingerprint") != expected["fingerprint"]):
                return pending("flat_name_stage_not_closed")
            source_directory_id = str(name_intent.get("directory_id") or "")
            if not source_directory_id:
                return pending("flat_staging_directory_identity_missing")
            if layout.get("from_directory_id") not in (None, "", source_directory_id):
                raise RemotePathConflict("flat staging directory identity changed")
            layout.update({"name_intent": dict(name_intent),
                           "from_directory_id": source_directory_id,
                           "to_directory_id": season_id})
            owners = build_season_owners(self.store, self.mapper, self.config, parent,
                                         include_quarter=True)
            persist("flat_move_pending", cloud_path=final_path if current_final is not None else stage_path)
            earlier_delete = layout.get("staging_archive_state") == "deleted"
            if current_final is not None:
                # The final file already sits in Season, so the staging source
                # (and its source directory) may be gone on purpose. Listing a
                # deleted 115 directory id is not a valid read: 115 answers with
                # the account root listing, which fails the identity proof.
                moved = current_final
            else:
                moved = self.cloud.move_staged_file_to_season(
                    expected, stage_path, final_path,
                    source_directory_id=source_directory_id,
                    target_directory_id=season_id, owners=owners,
                )
            if (not isinstance(moved, Mapping) or strict_identity(moved) != expected
                    or moved.get("path") != final_path
                    or str(moved.get("parent_id")) != season_id):
                return pending("flat_move_readback_incomplete")
            counts["move"] += int(moved.get("_mutation_performed") is True)
            layout["verified"] = True
            runtime["_target_directory_id"] = season_id
            archive_path = posixpath.join(
                self.mapper.layout_archive_root_for_episode(row, selected),
                _remote_file_id_token(expected["remote_file_id"]),
            )
            if layout.get("staging_archive_path") not in (None, "", archive_path):
                raise RemotePathConflict("flat staging archive route changed")
            if layout.get("staging_archive_state") not in (None, "", "deleted"):
                raise RemotePathConflict("flat staging archive state changed")
            layout["staging_archive_path"] = archive_path
            persist("flat_video_verified", cloud_path=final_path)
            deleted = (
                {"directory_id": source_directory_id, "deleted": True,
                 "already_applied": True, "_mutation_performed": False}
                if earlier_delete else
                self.cloud.delete_empty_staging_directory(
                    source_directory_id, parent,
                    file_id=expected["remote_file_id"], owners=owners,
                )
            )
            if (not isinstance(deleted, Mapping)
                    or str(deleted.get("directory_id")) != source_directory_id
                    or deleted.get("deleted") is not True):
                return pending("flat_staging_delete_not_closed")
            counts["move"] += int(deleted.get("_mutation_performed") is True)
            layout["staging_archive_state"] = "deleted"
            final = self.cloud.find_file_by_identity(expected, parent, directory_id=season_id)
            if (final is None or final.get("path") != final_path
                    or strict_identity(final) != expected
                    or str(final.get("parent_id")) != season_id):
                return pending("flat_final_identity_not_closed")
            layout["staging_archive_verified"] = True
            runtime["_cloud_layout_intent"] = dict(layout)
            runtime["_target_directory_id"] = season_id
            persist("flat_layout_verified", cloud_path=final_path)
            return self._organized(
                episode_id, final_path, litepan_path, moviepilot_path, emby_path,
                runtime, {"reason": "flat_layout_verified", "source_path": source_path,
                          "cloud_path": final_path, "identity": runtime}, counts,
            )
        except RemotePathConflict as exc:
            detail = self._safe_detail({"reason": "FLAT_LAYOUT_CONFLICT", "error": exc,
                                        "source_path": source_path,
                                        "cloud_path": final_path, "identity": runtime})
            self._open_review(episode_id, ReviewReason.PATH_CONFLICT, detail,
                              resume_status=EpisodeStatus.ORGANIZE_PENDING.value)
            return OrganizeResult(status=EpisodeStatus.NEEDS_REVIEW.value,
                                  review_reason=ReviewReason.PATH_CONFLICT, detail=detail)
        except RemoteRateLimited as exc:
            decision = self._throttled_pending(episode_id, runtime, source_path,
                                              final_path or stage_path, exc)
            return OrganizeResult(
                status=EpisodeStatus.NEEDS_REVIEW.value if decision.escalated
                       else EpisodeStatus.ORGANIZE_PENDING.value,
                detail=decision.detail,
            )
        except CloudCooldownStateError:
            raise
        except Exception as exc:
            return pending("flat_layout_incomplete", exc)

    def _reconcile_isolated(
        self,
        episode_id: int,
        row: Mapping[str, Any],
        source_path: str,
        cloud_path: str,
        litepan_path: str,
        moviepilot_path: str,
        emby_path: str,
        source_identity: Mapping[str, Any],
        pending_identity: Mapping[str, Any],
        *,
        from_cloud_path: str = "",
        name_intent: Mapping[str, Any] | None = None,
    ) -> OrganizeResult:
        """Move and rename one selected file inside its verified fid directory."""

        parent_path = posixpath.dirname(cloud_path)
        runtime = _runtime_identity_mapping(row)
        intent = dict(name_intent or runtime.get("_cloud_name_intent") or {})
        if not intent:
            # Keep the private helper's old direct-call surface usable by
            # focused callers; production enters with the persisted V1 intent.
            from_cloud_path = from_cloud_path or cloud_path
            intent = {
                "version": 1,
                "from_cloud_path": from_cloud_path,
                "to_cloud_path": cloud_path,
                "source_path": source_path,
                "original_status": str(
                    getattr(row.get("status"), "value", row.get("status")) or ""
                ),
                "rename_verified": False,
            }
            if source_identity.get("remote_file_id") not in (None, ""):
                intent["file_id"] = source_identity["remote_file_id"]
            for key in ("size", "fingerprint"):
                if source_identity.get(key) not in (None, ""):
                    intent[key] = source_identity[key]
        from_cloud_path = from_cloud_path or str(intent.get("from_cloud_path") or "")
        to_cloud_path = str(intent.get("to_cloud_path") or cloud_path)
        identity = dict(pending_identity)
        identity["_cloud_name_intent"] = intent
        counts = {"mkdir": 0, "move": 0, "rename": 0}

        def persist_intent() -> None:
            identity["_cloud_name_intent"] = dict(intent)
            if intent.get("directory_id") not in (None, ""):
                identity["_target_directory_id"] = str(intent["directory_id"])
            self._mark_pending(episode_id, identity, source_path, to_cloud_path)

        def pending(reason: str, error: Any = None) -> OrganizeResult:
            detail = {
                "reason": reason,
                "cloud_path": to_cloud_path,
                "from_cloud_path": from_cloud_path,
            }
            if error is not None:
                detail["error"] = error
            safe = self._safe_detail(detail)
            persist_intent()
            return OrganizeResult(
                status=EpisodeStatus.ORGANIZE_PENDING.value,
                cloud_path=sanitize_text(to_cloud_path, secrets=self.secrets),
                litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                emby_path=sanitize_text(emby_path, secrets=self.secrets),
                detail=safe,
                mkdir_count=counts["mkdir"], move_count=counts["move"],
                rename_count=counts["rename"],
            )

        def conflict(reason: str) -> OrganizeResult:
            detail = self._safe_detail({
                "reason": reason,
                "cloud_path": to_cloud_path,
                "from_cloud_path": from_cloud_path,
                "remote_file_id": intent.get("file_id"),
                "directory_id": intent.get("directory_id"),
            })
            self._open_review(episode_id, ReviewReason.PATH_CONFLICT, detail)
            return OrganizeResult(
                status=EpisodeStatus.NEEDS_REVIEW.value,
                cloud_path=sanitize_text(to_cloud_path, secrets=self.secrets),
                litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                emby_path=sanitize_text(emby_path, secrets=self.secrets),
                review_reason=ReviewReason.PATH_CONFLICT, detail=detail,
                mkdir_count=counts["mkdir"], move_count=counts["move"],
                rename_count=counts["rename"],
            )

        def throttled(exc: Any) -> OrganizeResult:
            decision = self._throttled_pending(
                episode_id, identity, source_path, to_cloud_path, exc
            )
            if decision.escalated:
                return OrganizeResult(
                    status=EpisodeStatus.NEEDS_REVIEW.value,
                    cloud_path=sanitize_text(to_cloud_path, secrets=self.secrets),
                    litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                    moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                    emby_path=sanitize_text(emby_path, secrets=self.secrets),
                    review_reason=ReviewReason.UNEXPECTED_REMOTE_STATE,
                    detail=decision.detail,
                    mkdir_count=counts["mkdir"], move_count=counts["move"],
                    rename_count=counts["rename"],
                )
            return OrganizeResult(
                status=EpisodeStatus.ORGANIZE_PENDING.value,
                cloud_path=sanitize_text(to_cloud_path, secrets=self.secrets),
                litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
                moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
                emby_path=sanitize_text(emby_path, secrets=self.secrets),
                detail=decision.detail,
                mkdir_count=counts["mkdir"], move_count=counts["move"],
                rename_count=counts["rename"],
            )

        try:
            # This read stays first so account cooldown and transport failures
            # escape into the existing bounded retry behavior before any write.
            target = self._inspect(to_cloud_path, expected_identity=None)
            if (
                type(intent.get("version")) is not int
                or intent.get("version") != 1
                or intent.get("from_cloud_path") != from_cloud_path
                or intent.get("to_cloud_path") != to_cloud_path
                or not source_path
                or not from_cloud_path
                or posixpath.dirname(from_cloud_path) != parent_path
                or posixpath.dirname(to_cloud_path) != parent_path
            ):
                return pending("CLOUD_NAME_INTENT_INVALID")
            try:
                self.mapper.relative_to(
                    parent_path, self.mapper.roots.cloud_library_root,
                    field="cloud_name_intent.parent_path",
                )
            except (AttributeError, PathMappingError):
                return pending("CLOUD_NAME_INTENT_INVALID")

            entries_reader = getattr(self.cloud, "list_directory_entries", None)
            resolver = getattr(self.cloud, "resolve_directory_path", None)
            if not callable(entries_reader) or not callable(resolver):
                return pending("ISOLATED_DIRECTORY_INVENTORY_UNAVAILABLE")

            expected = dict(source_identity)
            intent_file_id = intent.get("file_id")
            if intent_file_id not in (None, ""):
                if (
                    expected.get("remote_file_id") not in (None, "")
                    and str(expected["remote_file_id"]) != str(intent_file_id)
                ):
                    return conflict("CLOUD_NAME_INTENT_FILE_ID_CHANGED")
                expected["remote_file_id"] = str(intent_file_id)
            if expected.get("remote_file_id") in (None, ""):
                return pending("SOURCE_FILE_ID_MISSING")

            prior_dir_id = intent.get("directory_id")
            runtime_dir_id = identity.get("_target_directory_id")
            if (
                prior_dir_id not in (None, "")
                and runtime_dir_id not in (None, "")
                and str(prior_dir_id) != str(runtime_dir_id)
            ):
                return conflict("ISOLATED_DIRECTORY_ID_CHANGED")
            if prior_dir_id in (None, "") and runtime_dir_id not in (None, ""):
                prior_dir_id = str(runtime_dir_id)
                intent["directory_id"] = prior_dir_id

            def observe(item: Any, path: str) -> bool:
                if item is None:
                    return False
                actual = _identity({}, item)
                actual_id = actual.get("remote_file_id")
                if actual_id in (None, "") or str(actual_id) != str(expected["remote_file_id"]):
                    return False
                actual_path = _field(item, "path", "remote_path")
                if actual_path not in (None, "") and str(actual_path) != path:
                    return False
                for key in ("size", "fingerprint"):
                    actual_value = actual.get(key)
                    expected_value = expected.get(key)
                    if expected_value not in (None, "") and actual_value not in (None, ""):
                        if str(expected_value) != str(actual_value):
                            return False
                    elif expected_value in (None, "") and actual_value not in (None, ""):
                        expected[key] = actual_value
                return True

            def complete_identity_matches(item: Any) -> bool:
                actual = _identity({}, item)
                return all(
                    expected.get(key) not in (None, "")
                    and actual.get(key) not in (None, "")
                    and str(expected[key]) == str(actual[key])
                    for key in ("remote_file_id", "size", "fingerprint")
                )

            source_item = None
            from_item = None
            to_item = target
            for path in dict.fromkeys((source_path, from_cloud_path, to_cloud_path)):
                item = target if path == to_cloud_path else self._inspect(
                    path, expected_identity=None
                )
                if item is None:
                    continue
                if not observe(item, path):
                    return conflict("ISOLATED_FILE_IDENTITY_CONFLICT")
                if path == source_path:
                    source_item = item
                if path == from_cloud_path:
                    from_item = item
                if path == to_cloud_path:
                    to_item = item

            if source_item is not None and (from_item is not None or to_item is not None):
                if source_path not in (from_cloud_path, to_cloud_path):
                    return conflict("ISOLATED_FILE_PRESENT_AT_MULTIPLE_PATHS")

            if expected.get("size") in (None, "") or expected.get("fingerprint") in (None, ""):
                return pending("SOURCE_IDENTITY_INCOMPLETE")
            for item in (source_item, from_item, to_item):
                if item is not None and not complete_identity_matches(item):
                    return pending("SOURCE_IDENTITY_INCOMPLETE")
            for intent_key, identity_key in (
                ("file_id", "remote_file_id"),
                ("size", "size"),
                ("fingerprint", "fingerprint"),
            ):
                saved = intent.get(intent_key)
                actual = expected.get(identity_key)
                if saved not in (None, "") and str(saved) != str(actual):
                    return conflict("CLOUD_NAME_INTENT_IDENTITY_CHANGED")
                if identity_key == "size":
                    normalized_size = _positive_size_value(actual)
                    if normalized_size is None:
                        return pending("SOURCE_IDENTITY_SIZE_INVALID")
                    intent[intent_key] = normalized_size
                else:
                    intent[intent_key] = actual
            identity.update(expected)
            identity["_cloud_name_intent"] = intent

            if prior_dir_id not in (None, ""):
                directory_id = str(prior_dir_id)
                if str(resolver(parent_path)) != directory_id:
                    return conflict("ISOLATED_DIRECTORY_ID_CHANGED")
            elif from_item is not None or to_item is not None:
                directory_id = str(resolver(parent_path))
                intent["directory_id"] = directory_id
            elif source_item is not None:
                # mkdir is idempotent for the ID-token directory: it reads and
                # reuses an existing matching directory, otherwise creates it.
                persist_intent()
                counts["mkdir"] += 1
                directory_id = str(self._ensure_directory(parent_path))
                intent["directory_id"] = directory_id
            else:
                try:
                    directory_id = str(resolver(parent_path))
                    intent["directory_id"] = directory_id
                except RemoteRateLimited as exc:
                    return throttled(exc)
                except CloudCooldownStateError:
                    raise
                except Exception as exc:
                    return pending("SOURCE_NOT_FOUND", exc)

            identity["_target_directory_id"] = directory_id
            intent["directory_id"] = directory_id
            persist_intent()

            entries = entries_reader(directory_id)
            if not isinstance(entries, list):
                return pending("ISOLATED_DIRECTORY_INVENTORY_INCOMPLETE")
            if len(entries) > 1:
                return conflict("ISOLATED_DIRECTORY_CONTENT_CONFLICT")

            moved = None
            moved_path = ""
            if entries:
                item = entries[0]
                if not isinstance(item, Mapping) or _field(item, "is_dir", "is_directory"):
                    return conflict("ISOLATED_DIRECTORY_CONTENT_CONFLICT")
                item_identity = _identity({}, item)
                if (
                    item_identity.get("remote_file_id") in (None, "")
                    or str(item_identity.get("remote_file_id"))
                    != str(expected["remote_file_id"])
                    or not complete_identity_matches(item)
                    or not _same_identity(expected, item_identity, "", "")
                ):
                    return conflict("ISOLATED_DIRECTORY_CONTENT_CONFLICT")
                moved = item
                moved_path = _field(item, "path", "remote_path") or posixpath.join(
                    parent_path, str(_field(item, "n", "name", "file_name", "filename") or "")
                )
                if moved_path not in (from_cloud_path, to_cloud_path):
                    return conflict("ISOLATED_MOVE_PATH_MISMATCH")
            elif from_item is None and to_item is None:
                if source_item is None:
                    return pending("SOURCE_NOT_FOUND")
                # Re-read the origin immediately before the move; a stale path
                # read is not enough to authorize a remote file-ID mutation.
                source_item = self._inspect(source_path, expected_identity=None)
                if source_item is None or not observe(source_item, source_path):
                    return conflict("SOURCE_IDENTITY_CHANGED")
                persist_intent()
                counts["move"] += 1
                self._move(expected["remote_file_id"], directory_id)
                entries = entries_reader(directory_id)
                if not isinstance(entries, list):
                    return pending("MOVE_INVENTORY_INCOMPLETE")
                if len(entries) != 1:
                    return conflict("ISOLATED_MOVE_CONTENT_CONFLICT")
                item = entries[0]
                if not isinstance(item, Mapping):
                    return conflict("ISOLATED_MOVE_IDENTITY_MISSING")
                item_identity = _identity({}, item)
                if (
                    item_identity.get("remote_file_id") in (None, "")
                    or str(item_identity.get("remote_file_id"))
                    != str(expected["remote_file_id"])
                    or not complete_identity_matches(item)
                    or not _same_identity(expected, item_identity, "", "")
                ):
                    return conflict("ISOLATED_MOVE_IDENTITY_CHANGED")
                moved = item
                moved_path = _field(item, "path", "remote_path") or posixpath.join(
                    parent_path, str(_field(item, "n", "name", "file_name", "filename") or "")
                )
                if moved_path not in (from_cloud_path, to_cloud_path):
                    return conflict("ISOLATED_MOVE_PATH_MISMATCH")

            if moved is None:
                return pending("ISOLATED_MOVE_NOT_VISIBLE")
            if moved_path == to_cloud_path:
                if intent.get("rename_verified") is True and to_item is None:
                    return conflict("CLOUD_NAME_RENAME_EVIDENCE_CONFLICT")
            elif moved_path == from_cloud_path:
                if intent.get("rename_verified") is True:
                    return conflict("CLOUD_NAME_RENAME_EVIDENCE_CONFLICT")
                source_basename = posixpath.basename(from_cloud_path)
                target_basename = posixpath.basename(to_cloud_path)
                if source_basename != target_basename:
                    rename = getattr(self.cloud, "rename_isolated_file", None)
                    if not callable(rename):
                        return pending("ISOLATED_RENAME_UNAVAILABLE")
                    persist_intent()
                    counts["rename"] += 1
                    _call_write(
                        rename,
                        expected,
                        parent_path,
                        directory_id=directory_id,
                        source_basename=source_basename,
                        target_basename=target_basename,
                    )
                    # The endpoint is not atomic. A successful reply alone is
                    # not evidence; the next complete read must prove it.
                    resolver_id = str(resolver(parent_path))
                    if resolver_id != directory_id:
                        return conflict("ISOLATED_DIRECTORY_ID_CHANGED")
                    entries = entries_reader(directory_id)
                    if not isinstance(entries, list) or len(entries) != 1:
                        return conflict("ISOLATED_RENAME_CONTENT_CONFLICT")
                    moved = entries[0]
                    item_identity = _identity({}, moved)
                    moved_path = _field(moved, "path", "remote_path") or posixpath.join(
                        parent_path, str(_field(moved, "n", "name", "file_name", "filename") or "")
                    )
                    if (
                        not isinstance(moved, Mapping)
                        or moved_path != to_cloud_path
                        or item_identity.get("remote_file_id") in (None, "")
                        or str(item_identity.get("remote_file_id"))
                        != str(expected["remote_file_id"])
                        or not complete_identity_matches(moved)
                        or not _same_identity(expected, item_identity, "", "")
                    ):
                        return conflict("ISOLATED_RENAME_IDENTITY_CONFLICT")
                else:
                    # The remote name was already canonical at source; no write
                    # is needed, but the same complete identity read is required.
                    if not _same_identity(expected, _identity({}, moved), "", ""):
                        return conflict("ISOLATED_CANONICAL_IDENTITY_CONFLICT")
            else:
                return conflict("ISOLATED_MOVE_PATH_MISMATCH")

            intent["rename_verified"] = True
            source_parent = posixpath.dirname(source_path)
            if posixpath.basename(posixpath.dirname(source_parent)) == "未整理":
                cleanup = self.cloud.delete_empty_source_directory(source_parent)
                if not isinstance(cleanup, Mapping):
                    return pending("SOURCE_DIRECTORY_DELETE_NOT_CLOSED")
                if cleanup.get("deleted") is True:
                    runtime["_download_source_cleanup"] = "deleted"
                elif cleanup.get("skipped") != "not_empty":
                    return pending("SOURCE_DIRECTORY_DELETE_NOT_CLOSED")
                else:
                    runtime["_download_source_cleanup"] = "kept_not_empty"
            identity.update(expected)
            identity["_target_directory_id"] = directory_id
            identity["_cloud_name_intent"] = intent
            persist_intent()
            return self._organized(
                episode_id, to_cloud_path, litepan_path, moviepilot_path, emby_path,
                identity,
                {
                    "reconciled": moved_path == to_cloud_path,
                    "move": "verified",
                    "rename_verified": True,
                    "source_path": source_path,
                    "cloud_path": to_cloud_path,
                    "identity": identity,
                },
                counts,
            )
        except RemotePathConflict:
            return conflict("ISOLATED_NAME_CONFLICT")
        except RemoteRateLimited as exc:
            return throttled(exc)
        except CloudCooldownStateError:
            raise
        except Exception as exc:
            return pending("ISOLATED_ORGANIZE_FAILED", exc)

    def _safe_detail(self, value: Any) -> dict[str, Any]:
        return _safe_detail(value, secrets=self.secrets)

    # ---------- state transitions ----------
    def _throttled_pending(
        self,
        episode_id: int,
        identity: Mapping[str, Any],
        source_path: str,
        cloud_path: str,
        error: Any,
    ) -> ThrottleDecision:
        """Handle one transport-level 115 throttle on the shared account ladder.

        ``throttle_decision`` owns the escalation: a sustained failure opens
        the operator review, anything earlier parks the episode as pending
        with the decision's ``next_retry_at`` persisted so a restart (and the
        runner) continue the durable account-wide backoff instead of burning
        the episode's retry budget.
        """

        detail = self._safe_detail({
            "reason": "organize_rate_limited",
            "error": error,
            "source_path": source_path,
            "cloud_path": cloud_path,
            "identity": identity,
        })
        decision = throttle_decision(
            detail,
            state=self.throttle_state,
            timestamp=time.time(),
            jitter_seconds=self.jitter_seconds,
        )
        if decision.escalated:
            self._open_review(
                episode_id, ReviewReason.UNEXPECTED_REMOTE_STATE, decision.detail,
                resume_status=EpisodeStatus.ORGANIZE_PENDING.value,
            )
            return decision
        self._mark_pending(
            episode_id, identity, source_path, cloud_path, detail,
            extra={"next_retry_at": decision.next_retry_at},
        )
        return decision

    def _mark_pending(
        self,
        episode_id: int,
        identity: Mapping[str, Any],
        source_path: str,
        cloud_path: str,
        detail: Any = None,
        extra: Mapping[str, Any] | None = None,
    ) -> None:
        safe_detail = self._safe_detail(detail or {
            "source_path": source_path,
            "cloud_path": cloud_path,
            "identity": identity,
        })
        safe_identity = _safe_mapping(identity, secrets=self.secrets)
        merged_extra = {
            "cloud_path": sanitize_text(cloud_path or "", secrets=self.secrets) or None,
            "runtime_identity_json": safe_identity,
            "last_error": sanitize_text(
                safe_detail.get("error", "") if isinstance(safe_detail, Mapping) else "",
                secrets=self.secrets,
            ),
        }
        if extra:
            merged_extra.update(extra)
        if hasattr(self.store, "mark_pending"):
            _call_state(
                self.store.mark_pending,
                episode_id,
                EpisodeStatus.ORGANIZE_PENDING,
                safe_detail,
                extra=merged_extra,
                secrets=self.secrets,
            )
        elif hasattr(self.store, "transition"):
            _call_state(
                self.store.transition,
                episode_id,
                EpisodeStatus.ORGANIZE_PENDING,
                safe_detail,
                extra=merged_extra,
                secrets=self.secrets,
            )
        elif hasattr(self.store, "set_stage"):
            _call_state(
                self.store.set_stage,
                episode_id,
                EpisodeStatus.ORGANIZE_PENDING.value,
                safe_detail,
                extra=merged_extra,
            )
        else:
            raise TypeError("state store has no pending transition method")

    def _open_review(self, episode_id: int, reason: ReviewReason, detail: Mapping[str, Any],
                     resume_status: EpisodeStatus | None = None) -> None:
        safe_detail = self._safe_detail(detail)
        if hasattr(self.store, "open_review"):
            try:
                _call_state(
                    self.store.open_review,
                    episode_id,
                    reason,
                    safe_detail,
                    resume_status=resume_status,
                    secrets=self.secrets,
                )
                return
            except TypeError as exc:
                if "resume_status" not in str(exc):
                    raise
                _call_state(
                    self.store.open_review,
                    episode_id,
                    reason,
                    safe_detail,
                    secrets=self.secrets,
                )
                return
        if hasattr(self.store, "review"):
            _call_state(
                self.store.review,
                episode_id,
                reason,
                safe_detail,
                secrets=self.secrets,
            )
            return
        raise TypeError("state store has no review method")

    def _organized(
        self,
        episode_id: int,
        cloud_path: str,
        litepan_path: str,
        moviepilot_path: str,
        emby_path: str,
        identity: Mapping[str, Any],
        detail: Mapping[str, Any],
        counts: Mapping[str, int] | None = None,
    ) -> OrganizeResult:
        safe_detail = self._safe_detail(detail)
        safe_identity = _safe_mapping(identity, secrets=self.secrets)
        extra = {
            "cloud_path": sanitize_text(cloud_path, secrets=self.secrets),
            "strm_path": sanitize_text(litepan_path, secrets=self.secrets),
            "runtime_identity_json": safe_identity,
            "last_error": "",
            "next_retry_at": None,
        }
        if hasattr(self.store, "transition"):
            _call_state(
                self.store.transition,
                episode_id,
                EpisodeStatus.ORGANIZED,
                safe_detail,
                extra=extra,
                secrets=self.secrets,
            )
        elif hasattr(self.store, "set_stage"):
            _call_state(
                self.store.set_stage,
                episode_id,
                EpisodeStatus.ORGANIZED.value,
                safe_detail,
                extra=extra,
                secrets=self.secrets,
            )
        else:
            raise TypeError("state store has no transition method")
        values = counts or {}
        # A completed organize proves 115 writes succeeded: the account-level
        # throttle streak ends here.
        self.throttle_state.reset()
        return OrganizeResult(
            status=EpisodeStatus.ORGANIZED.value,
            cloud_path=sanitize_text(cloud_path, secrets=self.secrets),
            litepan_path=sanitize_text(litepan_path, secrets=self.secrets),
            moviepilot_path=sanitize_text(moviepilot_path, secrets=self.secrets),
            emby_path=sanitize_text(emby_path, secrets=self.secrets),
            detail=safe_detail,
            mkdir_count=int(values.get("mkdir", 0)),
            move_count=int(values.get("move", 0)),
            rename_count=int(values.get("rename", 0)),
        )

    def _pending_result(self, cloud: str, litepan: str, moviepilot: str, emby: str, detail: Mapping[str, Any]) -> OrganizeResult:
        return OrganizeResult(
            status=EpisodeStatus.ORGANIZE_PENDING.value,
            cloud_path=sanitize_text(cloud, secrets=self.secrets),
            litepan_path=sanitize_text(litepan, secrets=self.secrets),
            moviepilot_path=sanitize_text(moviepilot, secrets=self.secrets),
            emby_path=sanitize_text(emby, secrets=self.secrets),
            detail=self._safe_detail(detail),
        )

    # ---------- cloud reads/writes ----------
    def _inspect(self, path: str, *, expected_identity: Mapping[str, Any] | None) -> Any:
        if not path:
            return None
        for name in ("get_file", "find_file", "stat_file", "inspect_file", "file_at", "stat"):
            method = getattr(self.cloud, name, None)
            if not callable(method):
                continue
            try:
                result = _call_read(method, path)
            except (NotImplementedError, KeyError, LookupError):
                continue
            normalized = _unwrap_file(result)
            if normalized is not None:
                return normalized
        files = getattr(self.cloud, "files", None)
        if isinstance(files, Mapping):
            value = files.get(path)
            if value is not None:
                return value
        return None

    def _inspect_by_identity(
        self, identity: Mapping[str, Any], parent_path: str, *, directory_id: Any = None
    ) -> Any:
        files = getattr(self.cloud, "files", None)
        if isinstance(files, Mapping):
            for item in files.values():
                if _path_parent(item) == parent_path and _same_identity(identity, _identity({}, item), "", ""):
                    return item
        finder = getattr(self.cloud, "find_file_by_identity", None)
        if callable(finder):
            if directory_id is None:
                result = _call_write(finder, identity, parent_path)
            else:
                result = _call_write(finder, identity, parent_path, directory_id=directory_id)
            for item in _iter_files(result):
                if _same_identity(identity, _identity({}, item), "", ""):
                    return item
        return None

    def _ensure_directory(self, path: str) -> Any:
        # Validate the directory path against the canonical library root before
        # passing it to a remote adapter.
        self.mapper.relative_to(path, self.mapper.roots.cloud_library_root, field="directory_path")
        for name in ("mkdir", "make_directory", "ensure_directory", "create_directory"):
            method = getattr(self.cloud, name, None)
            if callable(method):
                result = _call_write(method, path)
                _assert_write_success(result, "mkdir")
                if isinstance(result, Mapping):
                    directory_id = result.get("directory_id") or result.get("id") or result.get("path")
                    if not directory_id:
                        raise RuntimeError("mkdir returned no stable directory identity")
                    self._confirm_directory(directory_id, path)
                    return directory_id
                if isinstance(result, bool):
                    if not result:
                        raise RuntimeError("mkdir returned false")
                    self._confirm_directory(path, path)
                    return path
                directory_id = result or path
                self._confirm_directory(directory_id, path)
                return directory_id
        raise RuntimeError("cloud adapter has no directory-create method")

    def _has_directory_writer(self) -> bool:
        return any(
            callable(getattr(self.cloud, name, None))
            for name in ("mkdir", "make_directory", "ensure_directory", "create_directory")
        )

    def _confirm_directory(self, directory_id: Any, path: str) -> None:
        """Re-read the created directory before allowing a move into it."""

        for name in ("get_directory", "stat_directory", "directory_exists", "confirm_directory"):
            method = getattr(self.cloud, name, None)
            if not callable(method):
                continue
            result = _call_read(method, directory_id)
            if result is None or result is False:
                raise RuntimeError(f"directory disappeared after mkdir: {path}")
            return
        raise RuntimeError("cloud adapter has no directory confirmation method")

    def _move(self, file_id: Any, directory_id: Any) -> None:
        for name in ("move_file", "move"):
            method = getattr(self.cloud, name, None)
            if callable(method):
                result = _call_write(method, file_id, directory_id)
                _assert_write_success(result, "move")
                return
        raise RuntimeError("cloud adapter has no move method")

    def _rename(
        self,
        file_id: Any,
        name: str,
        *,
        expected_identity: Mapping[str, Any] | None = None,
        target_path: str | None = None,
    ) -> None:
        for method_name in ("rename_file_no_replace", "rename_file_if_identity"):
            method = getattr(self.cloud, method_name, None)
            if callable(method):
                try:
                    result = _call_write(
                        method,
                        file_id,
                        name,
                        expected_identity=expected_identity,
                        target_path=target_path,
                    )
                except CloudCooldownStateError:
                    raise
                except Exception as exc:
                    if _exception_is_path_conflict(exc):
                        raise RemotePathConflict(str(exc)) from exc
                    raise
                if _is_path_conflict(result):
                    raise RemotePathConflict("remote rename destination conflict")
                _assert_write_success(result, "rename")
                return
        raise RuntimeError("cloud adapter has no atomic no-replace rename method")

    def _episode_row(self, episode: Any) -> Mapping[str, Any]:
        if isinstance(episode, int):
            row = self.store.get_episode(episode)
        elif isinstance(episode, Mapping):
            row = episode
        else:
            row = _object_mapping(episode)
        if not isinstance(row, Mapping):
            raise KeyError("episode row not found")
        return row

    def _validate_source_path(self, path: str) -> None:
        if not path:
            return
        errors: list[Exception] = []
        for root in (
            self.mapper.roots.cloud_source_root,
            self.mapper.roots.cloud_library_root,
        ):
            try:
                self.mapper.relative_to(path, root, field="source_path")
                return
            except PathMappingError as exc:
                errors.append(exc)
        raise errors[-1] if errors else PathMappingError("source_path: invalid")


def _episode_id(row: Mapping[str, Any]) -> int:
    value = row.get("id", row.get("episode_id"))
    if value is None:
        raise KeyError("episode row has no id")
    try:
        return int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("episode id must be an integer") from exc


def _selected_source(row: Mapping[str, Any]) -> Any:
    for key in ("selected_video", "selected_file", "remote_file", "source_file", "file"):
        if row.get(key) is not None:
            return row[key]
    # A normalized episode row's integer ``id`` is the database identity, not
    # a remote file identity.  Keep the source empty here so _identity can use
    # only explicit episode file fields as its row-level fallback.
    return {}


def _source_path(row: Mapping[str, Any], source: Any) -> str:
    value = _first_value(source, "path", "remote_path", "source_path", "cloud_source_path")
    if not value:
        value = _first_value(row, "source_path", "cloud_source_path", "remote_path", "cloud_path")
    persisted = _runtime_identity_mapping(row)
    if not value:
        value = _first_value(
            persisted, "source_path", "cloud_source_path", "remote_path", "path"
        )
    if not value:
        parent = _first_value(source, "parent_path")
        name = _first_value(source, "filename", "name")
        if not parent:
            parent = _first_value(persisted, "source_parent_path", "parent_path")
        if not name:
            name = _first_value(persisted, "source_filename", "filename", "name")
        if parent and name:
            value = posixpath.join(str(parent), str(name))
    return str(value or "")


def _identity(row: Any, source: Any) -> dict[str, Any]:
    result: dict[str, Any] = {}
    persisted = _runtime_identity_mapping(row) if isinstance(row, Mapping) else {}
    for output, source_keys, row_keys in (
        ("remote_file_id", ("remote_file_id", "file_id", "fid", "id"), ("remote_file_id", "file_id", "fid")),
        ("size", ("size", "file_size", "bytes", "s"), ("size", "file_size", "bytes", "s")),
        ("fingerprint", ("fingerprint", "content_fingerprint", "hash", "sha1", "sha", "md5", "etag"), ("fingerprint", "content_fingerprint", "hash", "sha1", "sha", "md5", "etag")),
    ):
        value = _first_value(source, *source_keys)
        if value is None or value == "":
            value = _first_value(row, *row_keys)
        if value is None or value == "":
            value = _first_value(
                persisted,
                *( {
                    "remote_file_id": ("remote_file_id",),
                    "size": ("size", "file_size", "bytes"),
                    "fingerprint": ("fingerprint", "content_fingerprint", "hash", "etag"),
                }[output] ),
            )
        if value is not None and value != "":
            result[output] = sanitize_text(value)
    return result


def _positive_size_value(value: Any) -> int | None:
    """Normalize remote size evidence without changing legacy runtime values."""

    if type(value) is int:
        return value if value > 0 else None
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        normalized = int(value)
        return normalized if normalized > 0 else None
    return None


def _identity_has_evidence(identity: Mapping[str, Any]) -> bool:
    # Size is useful corroboration, but it cannot prove that two remote files
    # are the same.  Restart reconciliation and same-target decisions require
    # a stable remote ID or content fingerprint.
    return any(identity.get(key) not in (None, "") for key in ("remote_file_id", "fingerprint"))


def _routing_evidence(row: Mapping[str, Any], source: Any = None) -> dict[str, Any]:
    """Keep the TMDB season routing inputs with a pending intent.

    The normalized V5 schema intentionally does not make TMDB response fields
    part of business identity.  Persisting these small, non-secret values in
    ``runtime_identity_json`` lets a crash restart reconstruct the same target
    without falling back to the current date.
    """

    result: dict[str, Any] = {}
    persisted = _runtime_identity_mapping(row)
    for output, keys in (
        ("_season_air_date", ("season_air_date", "tmdb_season_air_date", "target_season_air_date")),
        ("_work", ("canonical_work", "work", "title")),
        ("_season", ("season",)),
        ("_episode", ("episode",)),
    ):
        value = _first_present(row, *keys)
        if value is None:
            value = _first_present(source, *keys)
        if value is None:
            value = _first_present(persisted, *keys)
        if value is None and output == "_season_air_date":
            value = _first_present(
                persisted, "_season_air_date", "season_air_date", "tmdb_season_air_date"
            )
        if value not in (None, ""):
            result[output] = sanitize_text(value)
    source_name = _first_value(source, "filename", "name", "path", "source_path")
    if source_name in (None, ""):
        source_name = _first_value(row, "filename", "name", "path", "source_path", "cloud_source_path")
    if isinstance(source_name, str):
        suffix = posixpath.splitext(posixpath.basename(source_name.replace("\\", "/")))[1]
        if suffix:
            result["_extension"] = sanitize_text(suffix)
    return result


def _runtime_identity_mapping(row: Any) -> dict[str, Any]:
    if not isinstance(row, Mapping):
        return {}
    value = row.get("runtime_identity") or row.get("runtime_identity_json") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            return {}
    return dict(value) if isinstance(value, Mapping) else {}


def _same_identity(
    source: Mapping[str, Any],
    target: Mapping[str, Any],
    source_path: str,
    target_path: str,
) -> bool:
    stable_compared = 0
    for key in ("remote_file_id", "fingerprint"):
        left = source.get(key)
        right = target.get(key)
        if left in (None, "") or right in (None, ""):
            continue
        stable_compared += 1
        if str(left) != str(right):
            return False
    if not stable_compared:
        return False
    source_size = source.get("size")
    target_size = target.get("size")
    if source_size not in (None, "") and target_size not in (None, ""):
        if str(source_size) != str(target_size):
            return False
    return True


def _field(value: Any, *keys: str) -> Any:
    if isinstance(value, Mapping):
        for key in keys:
            candidate = value.get(key)
            if candidate is not None and candidate != "":
                return candidate
        return None
    for key in keys:
        candidate = getattr(value, key, None)
        if candidate is not None and candidate != "":
            return candidate
    return None


def _first_value(value: Any, *keys: str) -> Any:
    for key in keys:
        candidate = _field(value, key)
        if candidate is not None and candidate != "":
            return candidate
    return None


def _first_present(value: Any, *keys: str) -> Any:
    """Return the first explicitly present field, including an empty value."""

    if isinstance(value, Mapping):
        for key in keys:
            if key in value:
                return value[key]
        return None
    for key in keys:
        if hasattr(value, key):
            return getattr(value, key)
    return None


def _object_mapping(value: Any) -> dict[str, Any]:
    names = (
        "id", "episode_id", "media_source", "media_id", "canonical_work", "work", "title",
        "season", "episode", "air_date", "season_air_date", "tmdb_season_air_date",
        "target_season_air_date", "source_path", "cloud_source_path", "remote_path",
        "cloud_path", "file_id", "remote_file_id", "file_size", "size", "fingerprint",
        "extension", "suffix",
        "selected_video", "selected_file", "remote_file", "source_file", "status",
    )
    return {name: getattr(value, name) for name in names if hasattr(value, name)}


def _path_matches(item: Any, path: str) -> bool:
    value = _field(item, "path", "full_path", "remote_path")
    return value == path


def _path_parent(item: Any) -> str:
    path = _field(item, "path", "full_path", "remote_path")
    return posixpath.dirname(str(path)) if path else str(_field(item, "parent_path") or "")


def _iter_files(result: Any) -> list[Any]:
    if result is None:
        return []
    if isinstance(result, Mapping):
        for key in ("files", "items", "data", "list"):
            value = result.get(key)
            if isinstance(value, (list, tuple)):
                return list(value)
        return [result]
    if isinstance(result, (list, tuple, set)):
        return list(result)
    return []


def _unwrap_file(result: Any) -> Any:
    if result is None or result is False:
        return None
    if isinstance(result, Mapping):
        for key in ("file", "item", "result"):
            nested = result.get(key)
            if isinstance(nested, Mapping):
                return nested
        if result.get("ok") is False or result.get("success") is False or result.get("state") is False:
            return None
        return result
    if isinstance(result, (list, tuple)):
        return result[0] if result else None
    return result


def _call_read(method: Any, path: str) -> Any:
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(path)
    positional = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
    ]
    if not positional and any(parameter.kind == parameter.KEYWORD_ONLY for parameter in signature.parameters.values()):
        return method(path=path)
    return method(path)


def _call_write(method: Any, *args: Any, **kwargs: Any) -> Any:
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(*args, **kwargs)
    positional = [
        parameter
        for parameter in signature.parameters.values()
        if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
    ]
    if len(positional) < len(args):
        original_args = args
        keyword_only = [
            parameter.name
            for parameter in signature.parameters.values()
            if parameter.kind == parameter.KEYWORD_ONLY
        ]
        if len(keyword_only) >= len(args):
            args = ()
            kwargs = {**dict(zip(keyword_only, original_args)), **kwargs}
    accepted = {
        key: value
        for key, value in kwargs.items()
        if any(parameter.kind == parameter.VAR_KEYWORD for parameter in signature.parameters.values())
        or key in signature.parameters
    }
    return method(*args, **accepted)


def _call_state(method: Any, *args: Any, **kwargs: Any) -> Any:
    """Call a state fake or the full StateStore without broad TypeError retry."""

    if not kwargs:
        return method(*args)
    try:
        signature = inspect.signature(method)
    except (TypeError, ValueError):
        return method(*args, **kwargs)
    if not any(
        parameter.kind == parameter.VAR_POSITIONAL
        for parameter in signature.parameters.values()
    ):
        positional = [
            parameter
            for parameter in signature.parameters.values()
            if parameter.kind in (parameter.POSITIONAL_ONLY, parameter.POSITIONAL_OR_KEYWORD)
        ]
        args = args[:len(positional)]
    accepts_kwargs = any(
        parameter.kind == parameter.VAR_KEYWORD
        for parameter in signature.parameters.values()
    )
    accepted = {
        key: value
        for key, value in kwargs.items()
        if accepts_kwargs or key in signature.parameters
    }
    return method(*args, **accepted)


def _assert_write_success(result: Any, operation: str) -> None:
    if isinstance(result, Mapping):
        for key in ("ok", "success", "accepted", "state"):
            if key in result and result[key] in (False, 0, "0", "false", "False"):
                raise RuntimeError(f"remote {operation} rejected")


def _is_path_conflict(result: Any) -> bool:
    if not isinstance(result, Mapping):
        return False
    values = (
        result.get("reason"),
        result.get("code"),
        result.get("error"),
        result.get("status"),
    )
    return any(
        isinstance(value, str)
        and value.replace("-", "_").replace(" ", "_").casefold()
        in {"path_conflict", "target_exists", "already_exists", "file_exists", "eexist"}
        for value in values
    )


def _exception_is_path_conflict(value: BaseException) -> bool:
    if isinstance(value, (RemotePathConflict, FileExistsError)):
        return True
    text = str(value).replace("-", "_").replace(" ", "_").casefold()
    return any(token in text for token in ("path_conflict", "target_exists", "file_exists", "eexist"))


def _safe_detail(value: Any, *, secrets: Iterable[Any] = ()) -> dict[str, Any]:
    if isinstance(value, Mapping):
        result: dict[str, Any] = {}
        exception: BaseException | None = None
        for key, item in value.items():
            if key == "error" and isinstance(item, BaseException):
                exception = item
                continue
            result[sanitize_text(key, secrets=secrets)] = _safe_value(item, secrets=secrets, _name_intent=key in {"_cloud_name_intent", "_cloud_layout_intent"})
        if exception is not None:
            # Exception messages may contain credentials, paths, or response
            # bodies. Persist only the fixed diagnostic projection, never the
            # exception object or its string representation.
            result.update(safe_exception_diagnostics(exception))
    else:
        result = {"error": sanitize_text(value, secrets=secrets)}
    try:
        # Round-trip through JSON gives deterministic, JSON-safe evidence and
        # applies redaction to exception/string representations once more.
        text = sanitize_text(
            json.dumps(result, ensure_ascii=False, sort_keys=True, default=str),
            secrets=secrets,
        )
        parsed = json.loads(text)
        return parsed if isinstance(parsed, dict) else {"detail": text}
    except (TypeError, ValueError):
        return {"detail": sanitize_text(result, secrets=secrets)}


def _safe_value(value: Any, *, secrets: Iterable[Any] = (), _name_intent: bool = False) -> Any:
    if isinstance(value, Mapping):
        return {
            sanitize_text(key, secrets=secrets): _safe_value(
                item, secrets=secrets,
                _name_intent=_name_intent or key in {"_cloud_name_intent", "_cloud_layout_intent"},
            )
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple, set)):
        return [_safe_value(item, secrets=secrets, _name_intent=_name_intent) for item in value]
    if _name_intent and (value is None or type(value) in (bool, int)):
        return value
    return sanitize_text(value, secrets=secrets)


def _safe_mapping(value: Any, *, secrets: Iterable[Any] = ()) -> dict[str, Any]:
    if not isinstance(value, Mapping):
        return {}
    return _safe_value(value, secrets=secrets)


__all__ = ["OrganizeResult", "Organizer"]
