"""Bounded Task 8 stage services.

The services in this module are intentionally small orchestration boundaries:
they reconcile durable evidence first, persist a pending intent before an
external write, and leave retry metadata when the write or evidence check is
not complete.  They do not retry indefinitely and never place playback URLs
in state, logs, or exception details.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import json
import math
import os
import posixpath
import random
import stat
import tempfile
import time
from collections.abc import Mapping
from typing import Any, Callable, Iterable

from .adapters import RemoteInventoryIncomplete, RemoteRateLimited
from .evidence import EvidenceResult, SidecarEvidence, StrmEvidence
from .http import retry_delay
from .models import EpisodeKey, EpisodeStatus, ReviewReason
from .paths import PathMapper, PathMappingError
from .security import REDACTED, sanitize_text
from .throttle import CloudThrottleState, throttle_decision
from .video import VideoSelector


_UNSAFE_SIDECAR_REASONS = frozenset({
    "symlink",
    "unsafe_path",
    "invalid_path",
    "invalid_extension",
    "invalid_strm_path",
    "noncanonical_sidecar_path",
    "sidecar_path_collision",
    "path_collision",
    "strm_path_missing",
})


@dataclass(frozen=True, slots=True)
class _RetrySettings:
    """One bounded retry policy shared by all pending-stage services.

    ``max_attempts`` is optional for the compatibility ports used by the
    earlier task tests.  The CLI always supplies the validated V5 policy, so
    production stage retries are finite and can escalate to a durable review.
    """

    max_attempts: int | None
    backoff_seconds: float
    backoff_max_seconds: float
    jitter_seconds: float
    random_source: Callable[[], float]


def _retry_settings(
    retry_policy: Any = None,
    *,
    max_attempts: int | None = None,
    backoff_seconds: float = 30.0,
    backoff_max_seconds: float = 3600.0,
    jitter_seconds: float = 0.0,
    random_source: Callable[[], float] | None = None,
) -> _RetrySettings:
    """Normalize HTTP-style and service-style retry policy objects."""

    if retry_policy is not None:
        def read(name: str, default: Any) -> Any:
            if isinstance(retry_policy, Mapping):
                return retry_policy.get(name, default)
            return getattr(retry_policy, name, default)

        max_attempts = read("max_attempts", max_attempts)
        backoff_seconds = read("backoff_seconds", backoff_seconds)
        backoff_max_seconds = read("backoff_max_seconds", backoff_max_seconds)
        jitter_seconds = read("jitter_seconds", jitter_seconds)
    if max_attempts is not None:
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int) or max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
    base = _nonnegative_delay(backoff_seconds, "retry_base_seconds")
    cap = _positive_delay(backoff_max_seconds, "retry_max_seconds")
    if cap < base:
        cap = base
    jitter = _nonnegative_delay(jitter_seconds, "retry_jitter_seconds")
    return _RetrySettings(
        max_attempts=max_attempts,
        backoff_seconds=base,
        backoff_max_seconds=cap,
        jitter_seconds=jitter,
        random_source=random_source or random.random,
    )


def _with_retry_settings(row: Mapping[str, Any], settings: _RetrySettings) -> dict[str, Any]:
    """Attach non-persisted retry settings to one service row."""

    value = dict(row)
    value["_retry_settings"] = settings
    return value


@dataclass(frozen=True, slots=True)
class ServiceResult:
    """Safe summary returned by one bounded service invocation."""

    status: str
    evidence: EvidenceResult | None = None
    detail: Mapping[str, Any] = field(default_factory=dict)
    review_reason: ReviewReason | None = None
    trigger_calls: int = 0
    scrape_calls: int = 0
    poll_calls: int = 0

    @property
    def ok(self) -> bool:
        return self.status in {
            EpisodeStatus.STRM_READY.value,
            EpisodeStatus.METADATA_READY.value,
            EpisodeStatus.LIBRARY_READY.value,
        }

    @property
    def reason(self) -> str:
        if self.review_reason is not None:
            return self.review_reason.value
        if self.evidence is not None:
            return self.evidence.reason
        return str(self.detail.get("reason") or self.status)


def normalize_backfill_scope(window: Any) -> dict[str, Any] | None:
    """Validate an invocation scope before discovery or any state mutation."""
    if not isinstance(window, Mapping) or "backfill" not in window:
        return None
    scope = window["backfill"]
    if not isinstance(scope, Mapping):
        raise ValueError("invalid_backfill_scope")
    source = str(scope.get("media_source", "tmdb")).strip().casefold()
    media_id = str(scope.get("media_id") or "").strip()
    if source not in {"tmdb", "themoviedb"} or not media_id:
        raise ValueError("invalid_backfill_scope")
    numbers = {}
    for field in ("season", "start", "end"):
        value = scope.get(field)
        if isinstance(value, bool) or not isinstance(value, (str, int)):
            raise ValueError("invalid_backfill_scope")
        text = str(value)
        if not text.isascii() or not text.isdigit() or int(text) <= 0:
            raise ValueError("invalid_backfill_scope")
        numbers[field] = int(text)
    if numbers["start"] > numbers["end"]:
        raise ValueError("invalid_backfill_scope")
    return {"media_source": "tmdb", "media_id": media_id, **numbers}


def episode_in_backfill_scope(row: Any, scope: Mapping[str, Any] | None) -> bool:
    if scope is None:
        return True
    def field(name: str, default: Any = None) -> Any:
        return row.get(name, default) if isinstance(row, Mapping) else getattr(row, name, default)
    source = str(field("media_source", "")).strip().casefold()
    if source not in {"tmdb", "themoviedb"} or str(field("media_id", "")) != scope["media_id"]:
        return False
    try:
        values = [field("season"), field("episode"), field("episode_end", field("episode"))]
        if values[2] is None:
            values[2] = values[1]
        if any(isinstance(value, bool) or not isinstance(value, (int, str)) or not str(value).isascii() or not str(value).isdigit() for value in values):
            return False
        season, episode, end = map(int, values)
    except (ValueError, TypeError):
        return False
    return season == scope["season"] and scope["start"] <= episode <= end <= scope["end"]


class ReviewService:
    """Durable, reason-specific review lifecycle.

    Reviews are represented by the episode row in V5 (the episode id is the
    stable review id).  Validation happens before entering the repository
    transaction; the repository then performs a compare-and-set resolution so
    an invalid payload or a concurrent second resolver cannot alter state,
    retry metadata, or history.
    """

    _ALLOWED_KEYS = frozenset({
        "selected_file_id", "file_id", "selected_candidate_id", "candidate_id",
        "season", "episode", "media_source", "media_id", "action", "choice",
        "task_id", "task_name", "source_root", "output_root", "library_id",
        "note", "confirm", "force",
    })
    _REQUIRED = {
        ReviewReason.MULTIPLE_VIDEO_FILES: (
            ("selected_file_id", "file_id"),
        ),
        ReviewReason.AMBIGUOUS_EPISODE: (
            ("selected_file_id", "file_id", "choice"),
            ("season", "episode"),
        ),
        ReviewReason.NO_VALID_VIDEO: (
            ("selected_file_id", "file_id", "choice", "action"),
        ),
        ReviewReason.PATH_CONFLICT: (("action", "selected_file_id", "file_id"),),
        ReviewReason.METADATA_CONFLICT: (("action", "choice", "confirm"),),
        ReviewReason.DUPLICATE_EPISODE: (("selected_candidate_id", "candidate_id"),),
        ReviewReason.UNEXPECTED_REMOTE_STATE: (("action", "choice", "task_id"),),
        ReviewReason.LEGACY_IDENTITY_UNRESOLVED: (
            ("choice",),
            ("media_id", "season", "episode"),
        ),
        ReviewReason.RUNTIME_MAPPING_AMBIGUOUS: (("task_id",),),
    }

    def __init__(self, store: Any, *, secrets: Iterable[Any] = ()) -> None:
        self.store = store
        self.secrets = tuple(secrets)

    def open(
        self,
        episode_id: int,
        reason: ReviewReason | str,
        detail: Mapping[str, Any] | str | None = None,
        resume_status: str | EpisodeStatus | None = None,
    ) -> int:
        """Open a review and return its stable id (the episode id)."""

        reason_value = _review_reason(reason)
        safe_detail = _sanitize_review_value(detail or {}, self.secrets)
        opener = getattr(self.store, "open_review", None)
        if not callable(opener):
            raise TypeError("state store lacks review lifecycle support")
        try:
            return int(opener(
                episode_id,
                reason_value,
                safe_detail,
                resume_status=resume_status,
                secrets=self.secrets,
            ))
        except TypeError as exc:
            # Small repository fakes from earlier tasks expose the original
            # three-argument port.  The real StateStore uses the richer call.
            if "resume_status" not in str(exc):
                raise
            return int(opener(episode_id, reason_value, safe_detail))

    def list_open(self) -> list[dict[str, Any]]:
        """Return sanitized active reviews without changing their state."""

        getter = getattr(self.store, "list_open_reviews", None)
        if not callable(getter):
            raise TypeError("state store lacks review listing support")
        return [_review_projection(row, self.secrets) for row in getter()]

    def resolve(self, review_id: int, resolution: Mapping[str, Any] | str) -> str:
        """Validate and atomically resolve one active review."""

        getter = getattr(self.store, "get_review", None)
        resolver = getattr(self.store, "resolve_review", None)
        if not callable(getter) or not callable(resolver):
            raise TypeError("state store lacks review resolution support")
        row = getter(review_id)
        if row is None:
            raise ValueError(f"review {review_id} is not open")
        reason = _review_reason(row.get("review_reason"))
        safe_resolution = _validate_review_resolution(
            reason, resolution, self.secrets,
        )
        selected_file = _one_resolution_alias(
            safe_resolution, "selected_file_id", "file_id",
        )
        selected_candidate = _one_resolution_alias(
            safe_resolution, "selected_candidate_id", "candidate_id",
        )
        extra: dict[str, Any] = {}
        if selected_file not in (None, "") and reason is not ReviewReason.MULTIPLE_VIDEO_FILES:
            # Non-inventory resolutions (for example PATH_CONFLICT) still
            # retain the operator's durable file reference, but only a
            # MULTIPLE_VIDEO_FILES resolution is allowed to claim a mapped
            # download source and set its primary flag below.
            extra["file_id"] = selected_file
        selected_file_for_state = (
            selected_file
            if reason is ReviewReason.MULTIPLE_VIDEO_FILES
            else None
        )
        selected_candidate_for_state = (
            selected_candidate
            if reason is ReviewReason.DUPLICATE_EPISODE
            else None
        )
        identity = None
        if reason is ReviewReason.LEGACY_IDENTITY_UNRESOLVED:
            identity_values = {
                key: safe_resolution[key]
                for key in ("media_source", "media_id", "season", "episode")
                if key in safe_resolution
            }
            identity = identity_values or None
        runtime_identity = None
        if reason is ReviewReason.RUNTIME_MAPPING_AMBIGUOUS:
            runtime_identity = {
                key: safe_resolution[key]
                for key in ("task_id", "task_name", "source_root", "output_root")
                if key in safe_resolution
            }
        expected_reason = row.get("review_reason")
        expected_generation = row.get("review_generation")
        try:
            return resolver(
                review_id,
                safe_resolution,
                selected_file_id=selected_file_for_state,
                selected_candidate_id=selected_candidate_for_state,
                identity=identity,
                runtime_identity=runtime_identity,
                extra=extra,
                expected_review_reason=expected_reason,
                expected_review_generation=expected_generation,
                secrets=self.secrets,
            )
        except TypeError as exc:
            if "expected_review_generation" not in str(exc):
                raise
            # Compatibility fakes may expose the pre-CAS repository port; the
            # real StateStore path above is the transactional contract.
            return resolver(review_id, safe_resolution)


class DownloadService:
    """Reconcile one download intent and submit only genuinely new work."""

    def __init__(
        self,
        store: Any,
        cloud: Any,
        *,
        budget: Any,
        target_directory: str = "target",
        secrets: Iterable[Any] = (),
        now: Callable[[], float] | None = None,
        retry_base_seconds: float = 30.0,
        retry_max_seconds: float = 3600.0,
        retry_policy: Any = None,
        retry_max_attempts: int | None = None,
        retry_jitter_seconds: float = 0.0,
        retry_random: Callable[[], float] | None = None,
        video_selector: Any = None,
        target_episodes: Any = None,
        cloud_check_budget: int | None = None,
        throttle_state: CloudThrottleState | None = None,
        rotation_seed: int = 0,
        pre_submit_inventory: Callable[[Mapping[str, Any]], Any] | None = None,
    ) -> None:
        if budget is None:
            raise ValueError("download budget is required")
        self.store = store
        self.cloud = cloud
        self.budget = budget
        self.target_directory = target_directory
        self.secrets = tuple(secrets)
        self.now = now or time.time
        self.cloud_check_budget = cloud_check_budget
        self.throttle_state = throttle_state or CloudThrottleState()
        self.rotation_seed = rotation_seed
        self.pre_submit_inventory = pre_submit_inventory
        self.backfill_scope = None
        # Cloud reads this run actually issued (passed the budget gate) —
        # surfaced in the run summary so pacing is auditable.
        self.cloud_checks_used = 0
        self.retry = _retry_settings(
            retry_policy,
            max_attempts=retry_max_attempts,
            backoff_seconds=retry_base_seconds,
            backoff_max_seconds=retry_max_seconds,
            jitter_seconds=retry_jitter_seconds,
            random_source=retry_random,
        )
        self.retry_base_seconds = self.retry.backoff_seconds
        self.retry_max_seconds = self.retry.backoff_max_seconds
        self.retry_max_attempts = self.retry.max_attempts
        self.retry_jitter_seconds = self.retry.jitter_seconds
        self.video_selector = video_selector or VideoSelector()
        self.target_episodes = target_episodes

    def _cloud_check_gate(self, pending_status: str, episode_id: int) -> Any | None:
        """Reserve one per-run 115 read, or return a no-retry pending result.

        The budget caps how many reconcile/inventory reads one run issues so a
        large backlog cannot turn a single run into a request burst that trips
        the account-level 115 rate limit.  Admission is hashed per episode:
        ``(episode_id + rotation_seed) % (budget * 8)`` below the budget wins
        the window, so about one eighth of the backlog earns a read on every
        run at any queue depth — not just the first entries.  The seed rotates
        per cron cycle, so successive runs cover different subsets, and the
        same (episode_id, seed) pair always yields the same decision without
        knowing the queue length.  Two no-retry outcomes exist:
        ``cloud_check_cooldown`` while the shared account cooldown is active,
        and ``cloud_check_deferred`` for episodes outside this run's budget
        window — waiting is pacing, not failure, so retry attempts and backoff
        stay untouched either way.
        """

        if self.backfill_scope is not None:
            # An explicitly bounded invocation has no background queue to
            # rotate through. Keep account pacing and the hard read budget.
            if self.throttle_state.active(float(self.now())):
                return ServiceResult(
                    pending_status, detail={"reason": "cloud_check_cooldown"}
                )
            if self.cloud_check_budget is not None and self.cloud_checks_used >= self.cloud_check_budget:
                return ServiceResult(
                    pending_status, detail={"reason": "cloud_check_budget_exhausted"}
                )
            self.cloud_checks_used += 1
            return None
        if self.cloud_check_budget is None:
            return None
        now_value = float(self.now())
        if self.throttle_state.active(now_value):
            return ServiceResult(
                pending_status, detail={"reason": "cloud_check_cooldown"}
            )
        window = max(1, self.cloud_check_budget)
        spread = window * 8
        slot = (int(episode_id) + self.rotation_seed) % spread
        if slot >= window:
            return ServiceResult(
                pending_status, detail={"reason": "cloud_check_deferred"}
            )
        self.cloud_checks_used += 1
        return None

    def _throttle_pending(
        self,
        store: Any,
        episode_id: int,
        status: str,
        row: Mapping[str, Any],
        detail: Mapping[str, Any],
    ) -> str:
        """Back one rate-limited 115 read off without burning review budget.

        Escalation is driven by the shared account-level consecutive-throttle
        counter (reset by every successful 115 stage), never by this row's
        cumulative retry count, so only genuinely sustained throttling —
        hours with the default ladder — opens the summary review.
        """

        decision = throttle_decision(
            detail,
            state=self.throttle_state,
            timestamp=float(self.now()),
            jitter_seconds=self.retry_jitter_seconds,
            random_source=self.retry.random_source,
        )
        extra = {
            "retry_count": decision.consecutive,
            "next_retry_at": decision.next_retry_at,
        }
        if decision.escalated:
            _open_review(
                store,
                episode_id,
                ReviewReason.UNEXPECTED_REMOTE_STATE,
                decision.detail,
                resume_status=status,
                extra=extra,
                secrets=self.secrets,
            )
            return EpisodeStatus.NEEDS_REVIEW.value
        _transition(store, episode_id, status, detail, extra=extra, secrets=self.secrets)
        return status

    def set_scope(self, window: Any) -> None:
        self.backfill_scope = normalize_backfill_scope(window)

    def _submission_attempted(self, episode_id: int, row: Mapping[str, Any]) -> bool:
        if int(row.get("submit_count") or 0) > 0:
            return True
        # Older releases recorded the intent before incrementing submit_count.
        # Inspect every durable event: a retry event may hide the crash marker
        # from last_history_detail().
        connection = getattr(self.store, "conn", None)
        if connection is not None:
            events = connection.execute(
                "SELECT detail_json FROM stage_history WHERE episode_id=?", (episode_id,)
            ).fetchall()
            return any(
                isinstance(detail := json.loads(event[0]), Mapping)
                and detail.get("reason") == "download_submit_intent"
                for event in events
            )
        detail = getattr(self.store, "last_history_detail", lambda _: {})(episode_id)
        return detail.get("reason") == "download_submit_intent"

    def _submission_uncertain(self, episode_id: int) -> ServiceResult:
        detail = {"reason": "download_submission_uncertain"}
        _open_review(
            self.store, episode_id, ReviewReason.UNEXPECTED_REMOTE_STATE, detail,
            resume_status=EpisodeStatus.DOWNLOAD_PENDING.value,
            extra={"last_error": "download_submission_uncertain", "next_retry_at": None},
            secrets=self.secrets,
        )
        return ServiceResult(
            EpisodeStatus.NEEDS_REVIEW.value, detail=detail,
            review_reason=ReviewReason.UNEXPECTED_REMOTE_STATE,
        )

    def advance(self, episode: Any) -> ServiceResult:
        row = _with_retry_settings(_episode_row(self.store, episode), self.retry)
        episode_id = _episode_id(row)
        current = self.store.get_episode(episode_id)
        if current is not None:
            row = _with_retry_settings(current, self.retry)
            if _status(row) != EpisodeStatus.DOWNLOAD_PENDING.value:
                return ServiceResult(
                    _status(row), detail={"reason": "download_already_reconciled"}
                )
        attempted = self._submission_attempted(episode_id, row)
        infohash = _download_infohash(self.store, row)
        if not infohash:
            detail = {"reason": "download_identity_missing"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.DOWNLOAD_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)

        gated = self._cloud_check_gate(
            EpisodeStatus.DOWNLOAD_PENDING.value, episode_id
        )
        if gated is not None:
            return gated

        finder = getattr(self.cloud, "find_task", None)
        try:
            if not callable(finder):
                raise RemoteInventoryIncomplete("download task inventory unavailable")
            task = finder(infohash)
            # The reconcile read completed, so it counts as a successful 115
            # read even when it found no finished task (task is None: the
            # remote work is simply not done yet).  Reset the shared
            # consecutive-throttle streak here so intermittent throttling
            # never accumulates into the sustained "consecutive 20" review.
            self.throttle_state.reset()
        except RemoteRateLimited as exc:
            detail = {"reason": "download_reconcile_failed", "error": _safe_error(exc)}
            status_value = self._throttle_pending(
                self.store, episode_id, EpisodeStatus.DOWNLOAD_PENDING.value, row, detail
            )
            return ServiceResult(status_value, detail=detail)
        except Exception as exc:
            detail = {"reason": "download_reconcile_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.DOWNLOAD_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)

        if task is not None:
            task_id = _task_value(task, "task_id", "remote_task_id", "id")
            detail = {"reason": "download_reconciled"}
            # A proven 115 read ends any account-level throttle streak.
            self.throttle_state.reset()
            _transition(
                self.store, episode_id, EpisodeStatus.SUBMITTED.value, detail,
                extra={
                    "remote_task_id": task_id, "last_error": "", "next_retry_at": None,
                    "retry_count": 0,
                },
                secrets=self.secrets,
            )
            return ServiceResult(EpisodeStatus.SUBMITTED.value, detail=detail)

        if attempted:
            return self._submission_uncertain(episode_id)

        if self.pre_submit_inventory is not None:
            reason = "pre_submit_inventory_unknown"
            try:
                inventory = self.pre_submit_inventory(dict(row)) if callable(finder) else None
                state = inventory.get("state") if isinstance(inventory, Mapping) else None
                if state == "present":
                    reason = "pre_submit_inventory_present"
                elif state == "missing":
                    reason = ""
            except Exception:
                reason = "pre_submit_inventory_error"
            if reason:
                detail = {"reason": reason}
                _transition(self.store, episode_id, EpisodeStatus.DOWNLOAD_PENDING.value,
                            detail, secrets=self.secrets)
                return ServiceResult(EpisodeStatus.DOWNLOAD_PENDING.value, detail=detail)

        subscription_id = str(row.get("subscription_id") or "episode")
        # Submitting is a 115 write: defer it while the account cooldown is
        # active so the burst that tripped the limit cannot continue.
        if self.throttle_state.active(float(self.now())):
            return ServiceResult(
                EpisodeStatus.DOWNLOAD_PENDING.value,
                detail={"reason": "cloud_check_cooldown"},
            )
        submit = getattr(self.cloud, "submit", None)
        if not callable(submit):
            raise RuntimeError("cloud adapter lacks submit")
        if not self.budget.reserve(subscription_id):
            detail = {"reason": "submission_budget_exhausted"}
            return ServiceResult(EpisodeStatus.DOWNLOAD_PENDING.value, detail=detail)
        # Persist the external-attempt intent before calling the non-idempotent
        # submit port.  A crash or timeout is therefore recoverable through
        # the same pending row and history rather than an implicit retry.
        _transition(
            self.store,
            episode_id,
            EpisodeStatus.DOWNLOAD_PENDING.value,
            {"reason": "download_submit_intent"},
            extra={"last_error": "", "next_retry_at": None,
                   "submit_count": int(row.get("submit_count") or 0) + 1},
            secrets=self.secrets,
        )
        try:
            task_value = submit(f"magnet:?xt=urn:btih:{infohash}")
        except Exception:
            return self._submission_uncertain(episode_id)
        task_id = _task_value(task_value, "task_id", "remote_task_id", "id") or task_value
        detail = {"reason": "download_submitted"}
        self.throttle_state.reset()
        _transition(
            self.store, episode_id, EpisodeStatus.SUBMITTED.value, detail,
            extra={
                "remote_task_id": task_id,
                "submit_count": int(row.get("submit_count") or 0) + 1,
                "last_error": "",
                "next_retry_at": None,
                "retry_count": 0,
            }, secrets=self.secrets,
        )
        return ServiceResult(EpisodeStatus.SUBMITTED.value, detail=detail)

    def poll(self, episode: Any) -> ServiceResult:
        """Persist and select one complete submitted-task inventory.

        Inventory rows are persisted through Task 6's ``VideoSelectionResult``
        boundary before any Episode transition.  A tied video selection is a
        durable manual review; no arbitrary first file is ever selected.
        """

        row = _with_retry_settings(_episode_row(self.store, episode), self.retry)
        episode_id = _episode_id(row)
        current_row = self.store.get_episode(episode_id)
        if current_row is not None and _status(current_row) != EpisodeStatus.SUBMITTED.value:
            return ServiceResult(
                _status(current_row),
                detail={"reason": "download_already_reconciled"},
            )
        infohash = _download_infohash(self.store, row)
        if not infohash:
            detail = {"reason": "download_identity_missing"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SUBMITTED.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)

        gated = self._cloud_check_gate(EpisodeStatus.SUBMITTED.value, episode_id)
        if gated is not None:
            return gated

        lister = getattr(self.cloud, "list_files", None)
        if not callable(lister):
            raise RuntimeError("cloud adapter lacks file inventory")
        try:
            files = lister(self.target_directory) or []
            # The inventory read completed — even when the target file has not
            # appeared yet — so it counts as a successful 115 read.  Reset the
            # shared consecutive-throttle streak here so intermittent
            # throttling never accumulates into the sustained "consecutive 20"
            # review.
            self.throttle_state.reset()
        except RemoteRateLimited as exc:
            detail = {"reason": "download_inventory_failed", "error": _safe_error(exc)}
            status_value = self._throttle_pending(
                self.store, episode_id, EpisodeStatus.SUBMITTED.value, row, detail
            )
            return ServiceResult(status_value, detail=detail)
        except Exception as exc:
            detail = {"reason": "download_inventory_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SUBMITTED.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)

        mine = [
            item for item in files
            if str(_task_value(item, "infohash", "info_hash", "hash") or "").strip().lower() == infohash
        ]
        if not mine:
            return ServiceResult(EpisodeStatus.SUBMITTED.value, detail={"reason": "download_not_ready"})

        try:
            targets, episode_ids = self._targets(row, infohash)
        except ValueError:
            latest = self.store.get_episode(episode_id) or row
            if _status(latest) != EpisodeStatus.SUBMITTED.value:
                return ServiceResult(
                    _status(latest),
                    detail={"reason": "download_already_reconciled"},
                )
            detail = {"reason": "download_target_selection_missing"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SUBMITTED.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)
        selector = self.video_selector
        method = getattr(selector, "select", None)
        if not callable(method):
            detail = {"reason": "video_selector_unavailable"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SUBMITTED.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)
        try:
            selection = method(mine, [key for key, _ in targets])
            inventory = tuple(getattr(selection, "inventory", ()) or ())
            if not inventory:
                inventory = tuple(mine)
            persist = getattr(selection, "persist", None)
            if callable(persist):
                file_ids = list(persist(
                    self.store, infohash, episode_ids=episode_ids,
                ))
            else:
                file_ids = list(self.store.record_download_files(
                    infohash, [_inventory_mapping(item) for item in inventory],
                ))
        except RemoteInventoryIncomplete:
            detail = {"reason": "download_inventory_incomplete"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SUBMITTED.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)
        except Exception as exc:
            detail = {"reason": "video_selection_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SUBMITTED.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)

        file_ids_by_item: dict[Any, Any] = {}
        for item, file_id in zip(inventory, file_ids):
            try:
                file_ids_by_item[item] = file_id
            except TypeError:
                pass
            file_ids_by_item[_inventory_identity(item)] = file_id

        reviews = getattr(selection, "reviews", None)
        if reviews is None:
            reviews = getattr(selection, "review_reasons", {}) or {}
        mappings = getattr(selection, "mapping", None)
        if mappings is None:
            mappings = getattr(selection, "selected", {}) or {}
        details = getattr(selection, "details", {}) or {}
        current_review: ReviewReason | None = None
        for target_key, target_id in targets:
            # Shared infohashes are one remote task but many logical episodes.
            # Re-read each target after inventory selection and only promote a
            # still-submitted row with the same selected candidate.  The CAS
            # transition below closes the final race with a review resolver.
            target_row = self.store.get_episode(target_id)
            if target_row is None or not episode_in_backfill_scope(target_row, self.backfill_scope) or not _eligible_download_target(
                self.store, target_row, infohash,
            ):
                continue
            review_value = reviews.get(target_key) if isinstance(reviews, Mapping) else None
            if review_value is not None:
                reason = _review_reason(review_value)
                detail = details.get(target_key, {"reason": reason.value})
                _open_review(
                    self.store, target_id, reason, detail,
                    resume_status=EpisodeStatus.ORGANIZE_PENDING.value,
                    secrets=self.secrets,
                )
                if target_id == episode_id:
                    current_review = reason
                continue

            selected = mappings.get(target_key) if isinstance(mappings, Mapping) else None
            remote_file = _task_value(selected, "remote_file")
            if remote_file is None and selected is not None:
                remote_file = getattr(selected, "remote_file", None)
            try:
                file_id = file_ids_by_item.get(remote_file)
            except TypeError:
                file_id = None
            if file_id is None and remote_file is not None:
                file_id = file_ids_by_item.get(_inventory_identity(remote_file))
            if file_id is None:
                file_id = _task_value(remote_file, "remote_file_id", "file_id", "id")
            if file_id in (None, ""):
                reason = ReviewReason.NO_VALID_VIDEO
                _open_review(
                    self.store, target_id, reason,
                    {"reason": reason.value},
                    resume_status=EpisodeStatus.ORGANIZE_PENDING.value,
                    secrets=self.secrets,
                )
                if target_id == episode_id:
                    current_review = reason
                continue
            runtime_identity = _download_file_runtime_identity(
                target_row, remote_file, infohash, file_id,
            )
            transition_extra = {
                "file_id": _task_value(remote_file, "remote_file_id", "file_id", "id") or file_id,
                "last_error": "",
                "next_retry_at": None,
                "retry_count": 0,
            }
            if runtime_identity:
                transition_extra["runtime_identity_json"] = runtime_identity
            promoted = _transition_if_current(
                self.store, target_id, EpisodeStatus.SUBMITTED.value,
                EpisodeStatus.CLOUD_READY.value,
                {"reason": "download_files_ready", "files": len(inventory)},
                extra=transition_extra, secrets=self.secrets,
            )
            if promoted:
                self.throttle_state.reset()
            if not promoted:
                continue

        latest = self.store.get_episode(episode_id) or {}
        if current_review is not None:
            return ServiceResult(
                EpisodeStatus.NEEDS_REVIEW.value,
                review_reason=current_review,
                detail={"reason": current_review.value},
            )
        return ServiceResult(
            _status(latest),
            detail={"reason": "download_files_ready", "files": len(inventory)},
        )

    def _targets(
        self,
        row: Mapping[str, Any],
        infohash: str,
    ) -> tuple[list[tuple[EpisodeKey, int]], dict[EpisodeKey, int]]:
        source = self.target_episodes
        if callable(source):
            source = source(row)
        values: list[Any]
        if isinstance(source, Mapping):
            values = [(key, value) for key, value in source.items()]
        elif source is None:
            values = []
            for candidate_row in self.store.all_episodes():
                candidate_id = _episode_id(candidate_row)
                if candidate_id == _episode_id(row) or _download_infohash(self.store, candidate_row) == infohash:
                    values.append(candidate_row)
        else:
            values = list(source)
        if not values:
            values = [row]
        result: list[tuple[EpisodeKey, int]] = []
        episode_ids: dict[EpisodeKey, int] = {}
        for value in values:
            candidate_id = None
            if (
                isinstance(value, tuple)
                and len(value) == 2
                and _episode_key(value[0]) is not None
            ):
                value, candidate_id = value
            elif isinstance(value, Mapping):
                candidate_id = value.get("id", value.get("episode_id"))
            key = _episode_key(value)
            if key is None or not episode_in_backfill_scope(key, self.backfill_scope):
                continue
            if candidate_id is None:
                found = self.store.get_episode_by_key(key)
                candidate_id = found.get("id") if found else None
            if candidate_id is None:
                continue
            try:
                candidate_id = int(candidate_id)
            except (TypeError, ValueError):
                continue
            fresh = self.store.get_episode(candidate_id)
            if fresh is None or not episode_in_backfill_scope(fresh, self.backfill_scope) or not _eligible_download_target(
                self.store, fresh, infohash,
            ):
                continue
            key = _episode_key(fresh) or key
            key = (key, candidate_id)
            if key[0] in episode_ids:
                continue
            episode_ids[key[0]] = key[1]
            result.append(key)
        if not result:
            raise ValueError("download target episode identity unavailable")
        return result, episode_ids


class StrmService:
    """Resolve one LitePan task and prove one exact STRM path."""

    def __init__(
        self,
        store: Any,
        litepan: Any,
        files: Any = None,
        mapper: PathMapper | None = None,
        *,
        task_name: str | None = None,
        expected_task_name: str | None = None,
        source_root: str | None = None,
        expected_source_root: str | None = None,
        output_root: str | None = None,
        expected_output_root: str | None = None,
        raw_output_root: str | None = None,
        event: str = "anime_ready",
        source: str = "hermes",
        secrets: Iterable[Any] = (),
        now: Callable[[], float] | None = None,
        retry_base_seconds: float = 30.0,
        retry_max_seconds: float = 3600.0,
        retry_policy: Any = None,
        retry_max_attempts: int | None = None,
        retry_jitter_seconds: float = 0.0,
        retry_random: Callable[[], float] | None = None,
    ) -> None:
        self.store = store
        self.litepan = litepan
        self.files = litepan if files is None else files
        self.mapper = mapper
        self.task_name = _runtime_config_value(
            task_name if task_name is not None else expected_task_name,
            "task_name",
        )
        self.source_root = _runtime_config_value(
            source_root if source_root is not None else expected_source_root,
            "source_root",
        )
        self.output_root = _runtime_config_value(
            output_root if output_root is not None else expected_output_root,
            "output_root",
        )
        self.raw_output_root = _runtime_config_value(
            raw_output_root if raw_output_root is not None else self.output_root,
            "raw_output_root",
        )
        self.event = event
        self.source = source
        self.secrets = tuple(secrets)
        self.now = now or time.time
        self.retry = _retry_settings(
            retry_policy,
            max_attempts=retry_max_attempts,
            backoff_seconds=retry_base_seconds,
            backoff_max_seconds=retry_max_seconds,
            jitter_seconds=retry_jitter_seconds,
            random_source=retry_random,
        )
        self.retry_base_seconds = self.retry.backoff_seconds
        self.retry_max_seconds = self.retry.backoff_max_seconds
        self.retry_max_attempts = self.retry.max_attempts
        self.retry_jitter_seconds = self.retry.jitter_seconds

    def advance(self, episode: Any) -> ServiceResult:
        row = _with_retry_settings(_episode_row(self.store, episode), self.retry)
        episode_id = _episode_id(row)
        strm_path = self._strm_path(row)
        if not strm_path:
            detail = {"reason": "canonical_strm_path_missing"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.STRM_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": ""},
            )
            return ServiceResult(pending_status, detail=detail)

        evidence = StrmEvidence(self.files, secrets=self.secrets).validate(strm_path)
        if evidence.reason in {"reader_error", "symlink"}:
            detail = {"reason": "strm_evidence_unreadable", **dict(evidence.detail)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.STRM_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": strm_path},
            )
            return ServiceResult(pending_status, evidence=evidence, detail=detail)

        persisted_identity = _persisted_task_identity(row, self.secrets)
        try:
            task, candidates = self._find_task(persisted_identity)
        except Exception as exc:
            detail = {"reason": "runtime_task_lookup_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.STRM_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)

        if task is None:
            detail = {
                "reason": "runtime_task_mapping_ambiguous",
                "candidate_count": len(candidates),
            }
            _open_review(
                self.store, episode_id, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS, detail,
                secrets=self.secrets,
            )
            return ServiceResult(
                EpisodeStatus.NEEDS_REVIEW.value,
                detail=detail,
                review_reason=ReviewReason.RUNTIME_MAPPING_AMBIGUOUS,
            )

        try:
            evidence = self._refresh_published_strm(row, strm_path, evidence)
        except Exception as exc:
            detail = {"reason": "strm_publish_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.STRM_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": strm_path},
            )
            return ServiceResult(pending_status, detail=detail)

        identity = _runtime_identity(row)
        identity.update(_task_identity(task, self.secrets))
        identity["strm_path"] = sanitize_text(strm_path, secrets=self.secrets)
        if _not_due(row, self.now()):
            if evidence.ok:
                _transition_ready(
                    self.store, episode_id, EpisodeStatus.STRM_READY.value,
                    evidence.detail, extra={
                        "strm_path": strm_path,
                        "runtime_identity_json": identity,
                        "retry_count": 0,
                        "next_retry_at": None,
                        "last_error": "",
                    }, secrets=self.secrets,
                )
                return ServiceResult(EpisodeStatus.STRM_READY.value, evidence=evidence,
                                     detail=evidence.detail)
            return ServiceResult(
                EpisodeStatus.STRM_PENDING.value,
                evidence=evidence,
                detail={"reason": "retry_not_due", **dict(evidence.detail)},
            )
        if evidence.ok:
            _transition_ready(
                self.store, episode_id, EpisodeStatus.STRM_READY.value,
                evidence.detail, extra={
                    "strm_path": strm_path,
                    "runtime_identity_json": identity,
                    "retry_count": 0,
                    "next_retry_at": None,
                    "last_error": "",
                }, secrets=self.secrets,
            )
            return ServiceResult(EpisodeStatus.STRM_READY.value, evidence=evidence,
                                 detail=evidence.detail)

        # Persist both the exact target and the resolved runtime identity
        # before triggering an external task.  A restart can now reconcile the
        # same target without trusting a stale configuration ID.
        _transition_pending(
            self.store, episode_id, EpisodeStatus.STRM_PENDING.value,
            {"reason": "strm_evidence_missing", **dict(evidence.detail)},
            extra={
                "strm_path": strm_path,
                "runtime_identity_json": identity,
                "next_retry_at": None,
                "last_error": "",
            }, secrets=self.secrets,
        )
        try:
            verify_event = getattr(self.litepan, "assert_event_targets_task", None)
            if callable(verify_event):
                verify_event(_normalized_task_fields(task)["id"], self.event, self.source)
            response = self.litepan.trigger_event(
                self.event,
                self.source,
                f"episode {episode_id} ready",
            )
        except Exception as exc:
            detail = {"reason": "litepan_trigger_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.STRM_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": strm_path, "runtime_identity_json": identity},
            )
            return ServiceResult(pending_status, evidence=evidence,
                                 detail=detail, trigger_calls=1)

        if not _accepted(response):
            detail = {"reason": "litepan_trigger_rejected"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.STRM_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": strm_path, "runtime_identity_json": identity},
            )
            return ServiceResult(pending_status, evidence=evidence,
                                 detail=detail, trigger_calls=1)

        # HTTP 200/accepted is only a trigger acknowledgement.  Re-read the
        # exact path once in this bounded invocation; a later run can retry.
        after = StrmEvidence(self.files, secrets=self.secrets).validate(strm_path)
        try:
            after = self._refresh_published_strm(row, strm_path, after)
        except Exception as exc:
            detail = {"reason": "strm_publish_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.STRM_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": strm_path, "runtime_identity_json": identity},
            )
            return ServiceResult(pending_status, evidence=after,
                                 detail=detail, trigger_calls=1)
        if after.ok:
            _transition_ready(
                self.store, episode_id, EpisodeStatus.STRM_READY.value,
                after.detail, extra={
                    "strm_path": strm_path,
                    "runtime_identity_json": identity,
                    "retry_count": 0,
                    "next_retry_at": None,
                    "last_error": "",
                }, secrets=self.secrets,
            )
            return ServiceResult(EpisodeStatus.STRM_READY.value, evidence=after,
                                 detail=after.detail, trigger_calls=1)

        detail = {"reason": "strm_evidence_missing_after_trigger", **dict(after.detail)}
        pending_status = _retry_pending(
            self.store, episode_id, EpisodeStatus.STRM_PENDING.value, row,
            detail, now=self.now, base=self.retry_base_seconds,
            cap=self.retry_max_seconds, secrets=self.secrets,
            extra={"strm_path": strm_path, "runtime_identity_json": identity},
        )
        return ServiceResult(pending_status, evidence=after,
                             detail=detail, trigger_calls=1)

    def _strm_path(self, row: Mapping[str, Any]) -> str:
        return _canonical_strm_path(row, self.mapper)

    def _refresh_published_strm(
        self, row: Mapping[str, Any], target: str, evidence: EvidenceResult
    ) -> EvidenceResult:
        """Project LitePan's isolated raw STRM into the canonical season path."""

        if self.mapper is None:
            return evidence
        cloud_path = row.get("cloud_path")
        if not isinstance(cloud_path, str) or not cloud_path:
            return evidence
        raw_path = self.mapper.cloud_to_raw_litepan(cloud_path, self.raw_output_root)
        if raw_path == target:
            return evidence
        raw_evidence = StrmEvidence(self.files, secrets=self.secrets).validate(raw_path)
        if not raw_evidence.ok:
            return EvidenceResult(False, detail={"reason": "raw_strm_missing"})
        if not isinstance(self.files, str):
            return EvidenceResult(False, detail={"reason": "strm_publisher_unavailable"})
        _publish_local_strm(
            raw_path, target, self.mapper.roots.litepan_output_root, self.raw_output_root
        )
        return StrmEvidence(self.files, secrets=self.secrets).validate(target)

    def _find_unique_task(self) -> tuple[Mapping[str, Any] | Any | None, list[Any]]:
        tasks = _list_runtime_tasks(self.litepan)
        candidates = [task for task in tasks if _task_matches(
            task,
            name=self.task_name,
            source_root=self.source_root,
            output_root=self.output_root,
        ) and _normalized_task_fields(task).get("id") not in (None, "")]
        if len(candidates) != 1:
            return None, candidates
        return candidates[0], candidates

    def _find_task(
        self,
        persisted_identity: Mapping[str, Any],
    ) -> tuple[Mapping[str, Any] | Any | None, list[Any]]:
        selected_id = persisted_identity.get("task_id") or persisted_identity.get("litepan_task_id")
        if selected_id in (None, ""):
            return self._find_unique_task()
        tasks = _list_runtime_tasks(self.litepan)
        candidates: list[Any] = []
        for task in tasks:
            fields = _normalized_task_fields(task)
            if str(fields.get("id") or "") != str(selected_id):
                continue
            if not _task_matches(
                task,
                name=self.task_name,
                source_root=self.source_root,
                output_root=self.output_root,
            ):
                continue
            if persisted_identity.get("task_name") not in (None, "") and str(
                fields.get("name") or ""
            ) != str(persisted_identity["task_name"]):
                continue
            if persisted_identity.get("source_root") not in (None, "") and not _same_root(
                fields.get("source_root"), persisted_identity["source_root"]
            ):
                continue
            if persisted_identity.get("output_root") not in (None, "") and not _same_root(
                fields.get("output_root"), persisted_identity["output_root"]
            ):
                continue
            candidates.append(task)
        if len(candidates) != 1:
            return None, candidates
        return candidates[0], candidates


class MetadataService:
    """Reconcile sidecars and perform one bounded MoviePilot scrape."""

    def __init__(
        self,
        store: Any,
        moviepilot: Any,
        evidence: SidecarEvidence | Any,
        *,
        mapper: PathMapper | None = None,
        media_source: str = "tmdb",
        type_name: str = "电视剧",
        poll_attempts: int = 2,
        max_polls: int | None = None,
        poll_interval: float = 0.0,
        sleep: Callable[[float], Any] | None = None,
        now: Callable[[], float] | None = None,
        retry_base_seconds: float = 30.0,
        retry_max_seconds: float = 3600.0,
        retry_policy: Any = None,
        retry_max_attempts: int | None = None,
        retry_jitter_seconds: float = 0.0,
        retry_random: Callable[[], float] | None = None,
        secrets: Iterable[Any] = (),
    ) -> None:
        if max_polls is not None:
            poll_attempts = max_polls
        if isinstance(poll_attempts, bool) or not isinstance(poll_attempts, int) or poll_attempts < 0:
            raise ValueError("poll_attempts must be a non-negative integer")
        self.store = store
        self.moviepilot = moviepilot
        self.evidence = evidence
        self.mapper = mapper
        self.media_source = str(media_source or "tmdb")
        self.type_name = type_name
        self.poll_attempts = poll_attempts
        self.poll_interval = _nonnegative_delay(poll_interval, "poll_interval")
        self.sleep = sleep or time.sleep
        self.now = now or time.time
        self.retry = _retry_settings(
            retry_policy,
            max_attempts=retry_max_attempts,
            backoff_seconds=retry_base_seconds,
            backoff_max_seconds=retry_max_seconds,
            jitter_seconds=retry_jitter_seconds,
            random_source=retry_random,
        )
        self.retry_base_seconds = self.retry.backoff_seconds
        self.retry_max_seconds = self.retry.backoff_max_seconds
        self.retry_max_attempts = self.retry.max_attempts
        self.retry_jitter_seconds = self.retry.jitter_seconds
        self.secrets = tuple(secrets)

    def advance(self, episode: Any) -> ServiceResult:
        row = _with_retry_settings(_episode_row(self.store, episode), self.retry)
        episode_id = _episode_id(row)
        strm_path = _canonical_strm_path(row, self.mapper, require_persisted=True)
        if not strm_path:
            detail = {"reason": "canonical_strm_path_missing"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": ""},
            )
            return ServiceResult(pending_status, detail=detail)

        evidence_row = dict(row)
        evidence_row["strm_path"] = strm_path
        # The mapper has verified each service mount alias; evidence reads the
        # canonical LitePan mount and requires all of its aliases to agree.
        for alias in ("litepan_path", "moviepilot_path", "emby_path"):
            if alias in evidence_row:
                evidence_row[alias] = strm_path
        try:
            initial = self.evidence.validate(evidence_row)
        except Exception:
            initial = EvidenceResult(False, detail={"reason": "reader_error"})
        if initial.reason == "reader_error":
            detail = {"reason": "evidence_reader_error"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": strm_path},
            )
            return ServiceResult(pending_status, evidence=initial, detail=detail)
        if _unsafe_sidecar_evidence(initial):
            detail = _unsafe_sidecar_detail(initial, self.secrets)
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"strm_path": strm_path},
            )
            return ServiceResult(pending_status, evidence=initial, detail=detail)
        if initial.review_reason is not None:
            _open_review(
                self.store, episode_id, initial.review_reason, initial.detail,
                secrets=self.secrets,
            )
            return ServiceResult(
                EpisodeStatus.NEEDS_REVIEW.value,
                evidence=initial,
                detail=initial.detail,
                review_reason=initial.review_reason,
            )
        if initial.ok:
            _transition_ready(
                self.store, episode_id, EpisodeStatus.METADATA_READY.value,
                initial.detail,
                extra={
                    "retry_count": 0,
                    "next_retry_at": None,
                    "last_error": "",
                    "strm_path": strm_path,
                },
                secrets=self.secrets,
            )
            return ServiceResult(EpisodeStatus.METADATA_READY.value,
                                 evidence=initial, detail=initial.detail)

        if _not_due(row, self.now()):
            return ServiceResult(
                EpisodeStatus.SCRAPE_PENDING.value,
                evidence=initial,
                detail={"reason": "retry_not_due", **dict(initial.detail)},
            )

        media_id = str(_episode_value(row, "media_id", "tmdb_id") or "")
        _transition_pending(
            self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value,
            {"reason": "scrape_intent", **dict(initial.detail)},
            extra={"strm_path": strm_path} if strm_path else None,
            secrets=self.secrets,
        )
        scrape_calls = 0
        try:
            moviepilot_path = (
                self.mapper.litepan_to_moviepilot(strm_path)
                if self.mapper is not None else strm_path
            )
            result = self.moviepilot.scrape_in_place(
                moviepilot_path,
                self.media_source,
                media_id,
                self.type_name,
            )
            scrape_calls = 1
        except Exception as exc:
            detail = {"reason": "scrape_exception", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, evidence=initial,
                                 detail=detail, scrape_calls=scrape_calls)

        if not _scrape_ok(result):
            detail = {"reason": "scrape_rejected"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, evidence=initial,
                                 detail=detail, scrape_calls=scrape_calls)

        latest = initial
        polls = 0
        while polls < self.poll_attempts:
            if self.poll_interval:
                self.sleep(self.poll_interval)
            try:
                latest = self.evidence.validate(evidence_row)
            except Exception:
                latest = EvidenceResult(False, detail={"reason": "reader_error"})
            polls += 1
            if latest.reason == "reader_error":
                detail = {"reason": "evidence_reader_error"}
                pending_status = _retry_pending(
                    self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value, row,
                    detail, now=self.now, base=self.retry_base_seconds,
                    cap=self.retry_max_seconds, secrets=self.secrets,
                    extra={"strm_path": strm_path},
                )
                return ServiceResult(
                    pending_status,
                    evidence=latest,
                    detail=detail,
                    scrape_calls=scrape_calls,
                    poll_calls=polls,
                )
            if _unsafe_sidecar_evidence(latest):
                detail = _unsafe_sidecar_detail(latest, self.secrets)
                pending_status = _retry_pending(
                    self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value, row,
                    detail, now=self.now, base=self.retry_base_seconds,
                    cap=self.retry_max_seconds, secrets=self.secrets,
                    extra={"strm_path": strm_path},
                )
                return ServiceResult(
                    pending_status,
                    evidence=latest,
                    detail=detail,
                    scrape_calls=scrape_calls,
                    poll_calls=polls,
                )
            if latest.review_reason is not None:
                _open_review(
                    self.store, episode_id, latest.review_reason, latest.detail,
                    secrets=self.secrets,
                )
                return ServiceResult(
                    EpisodeStatus.NEEDS_REVIEW.value,
                    evidence=latest,
                    detail=latest.detail,
                    review_reason=latest.review_reason,
                    scrape_calls=scrape_calls,
                    poll_calls=polls,
                )
            if latest.ok:
                _transition_ready(
                    self.store, episode_id, EpisodeStatus.METADATA_READY.value,
                    latest.detail,
                    extra={
                        "retry_count": 0,
                        "next_retry_at": None,
                        "last_error": "",
                    }, secrets=self.secrets,
                )
                return ServiceResult(EpisodeStatus.METADATA_READY.value,
                                     evidence=latest, detail=latest.detail,
                                     scrape_calls=scrape_calls, poll_calls=polls)

        detail = {
            "reason": "sidecar_evidence_incomplete",
            **dict(latest.detail),
            "poll_limit": self.poll_attempts,
        }
        pending_status = _retry_pending(
            self.store, episode_id, EpisodeStatus.SCRAPE_PENDING.value, row,
            detail, now=self.now, base=self.retry_base_seconds,
            cap=self.retry_max_seconds, secrets=self.secrets,
        )
        return ServiceResult(pending_status, evidence=latest,
                             detail=detail, scrape_calls=scrape_calls,
                             poll_calls=polls)


class LibraryService:
    """Reconcile one exact Emby Episode and its durable ready notification."""

    def __init__(
        self,
        store: Any,
        emby: Any,
        mapper: PathMapper | None = None,
        *,
        library_name: str | None = None,
        library_root: str | None = None,
        runtime_config: Any = None,
        now: Callable[[], float] | None = None,
        retry_base_seconds: float = 30.0,
        retry_max_seconds: float = 3600.0,
        retry_policy: Any = None,
        retry_max_attempts: int | None = None,
        retry_jitter_seconds: float = 0.0,
        retry_random: Callable[[], float] | None = None,
        secrets: Iterable[Any] = (),
    ) -> None:
        self.store = store
        self.emby = emby
        self.mapper = mapper
        configured_name = library_name
        if configured_name in (None, ""):
            configured_name = _runtime_config_section_value(
                runtime_config, "emby_library_name"
            )
        if configured_name in (None, ""):
            configured_name = getattr(emby, "library_name", None)
        self.library_name = str(configured_name or "").strip()
        configured_root = library_root
        if configured_root in (None, "") and mapper is not None:
            configured_root = mapper.roots.emby_mount_root
        self.library_root = str(configured_root or "").strip()
        self.now = now or time.time
        self.retry = _retry_settings(
            retry_policy,
            max_attempts=retry_max_attempts,
            backoff_seconds=retry_base_seconds,
            backoff_max_seconds=retry_max_seconds,
            jitter_seconds=retry_jitter_seconds,
            random_source=retry_random,
        )
        self.retry_base_seconds = self.retry.backoff_seconds
        self.retry_max_seconds = self.retry.backoff_max_seconds
        self.retry_max_attempts = self.retry.max_attempts
        self.retry_jitter_seconds = self.retry.jitter_seconds
        self.secrets = tuple(secrets)

    def advance(self, episode: Any) -> ServiceResult:
        row = _with_retry_settings(_episode_row(self.store, episode), self.retry)
        episode_id = _episode_id(row)
        current_status = _status(row)
        if current_status in {
            EpisodeStatus.LIBRARY_READY.value,
            EpisodeStatus.NOTIFIED.value,
        }:
            return ServiceResult(current_status, detail={"reason": "already_ready"})

        expected_path = _canonical_emby_path(row, self.mapper)
        if not expected_path:
            detail = {"reason": "canonical_emby_path_missing"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.LIBRARY_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)

        try:
            library = self._resolve_library(row)
        except Exception as exc:
            detail = {"reason": "emby_library_lookup_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.LIBRARY_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)
        if not library:
            detail = {"reason": "emby_library_missing"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.LIBRARY_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)
        library_id = _library_id(library)
        if not library_id:
            detail = {"reason": "emby_library_id_missing"}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.LIBRARY_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
            )
            return ServiceResult(pending_status, detail=detail)

        identity = _emby_identity(row)
        identity["library_id"] = library_id
        try:
            item = self._find_episode(identity, expected_path)
        except Exception as exc:
            detail = {"reason": "emby_query_failed", "error": _safe_error(exc)}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.LIBRARY_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"library_id": library_id, "library_path": self.library_root},
            )
            return ServiceResult(pending_status, detail=detail)

        if item:
            detail = {
                "reason": "emby_exact_episode",
                "library_id": library_id,
                "item_id": _item_id(item),
                "path": expected_path,
            }
            notification_key = _library_notification_key(row, episode_id, self.secrets)
            message = _library_notification_message(row, self.secrets)
            self._transition_ready(
                episode_id,
                notification_key,
                message,
                detail,
                library_id=library_id,
                library_path=self.library_root,
                emby_path=expected_path,
            )
            return ServiceResult(EpisodeStatus.LIBRARY_READY.value, detail=detail)

        absent = {"reason": "emby_exact_episode_absent", "library_id": library_id}
        refresh_now = float(self.now())
        if _not_due(row, refresh_now):
            return ServiceResult(
                EpisodeStatus.LIBRARY_PENDING.value,
                detail={"reason": "retry_not_due", **absent},
            )
        if (
            self.retry.max_attempts is not None
            and int(row.get("retry_count") or 0) >= self.retry.max_attempts
        ):
            detail = {
                "reason": "retry_exhausted",
                "attempts": int(row.get("retry_count") or 0),
                "last_reason": absent["reason"],
                **absent,
            }
            _open_review(
                self.store,
                episode_id,
                ReviewReason.UNEXPECTED_REMOTE_STATE,
                detail,
                resume_status=EpisodeStatus.LIBRARY_PENDING.value,
                extra={
                    "retry_count": int(row.get("retry_count") or 0),
                    "next_retry_at": None,
                    "last_error": _safe_error_text(detail, self.secrets),
                },
                secrets=self.secrets,
            )
            return ServiceResult(
                EpisodeStatus.NEEDS_REVIEW.value,
                detail=detail,
                review_reason=ReviewReason.UNEXPECTED_REMOTE_STATE,
            )
        try:
            claim = _claim_library_refresh(
                self.store,
                episode_id,
                {**absent, "intent": "refresh_library"},
                now=refresh_now,
                base=self.retry_base_seconds,
                cap=self.retry_max_seconds,
                extra={"library_id": library_id, "library_path": self.library_root},
                retry_policy=self.retry,
                secrets=self.secrets,
            )
        except Exception as exc:
            detail = {"reason": "emby_refresh_claim_failed", "error": _safe_error(exc), **absent}
            pending_status = _retry_pending(
                self.store, episode_id, EpisodeStatus.LIBRARY_PENDING.value, row,
                detail, now=self.now, base=self.retry_base_seconds,
                cap=self.retry_max_seconds, secrets=self.secrets,
                extra={"library_id": library_id, "library_path": self.library_root},
            )
            return ServiceResult(pending_status, detail=detail)
        if not claim:
            return ServiceResult(
                EpisodeStatus.LIBRARY_PENDING.value,
                detail={"reason": "refresh_claim_not_won", **absent},
            )
        claim_extra = {
            "library_id": library_id,
            "library_path": self.library_root,
            "retry_count": claim["retry_count"],
            "next_retry_at": claim["next_retry_at"],
            "last_error": "",
        }
        try:
            self.emby.refresh_library(library_id)
        except Exception as exc:
            detail = {"reason": "emby_refresh_failed", "error": _safe_error(exc), **absent}
            claim_extra["last_error"] = _safe_error_text(detail, self.secrets)
            if (
                self.retry.max_attempts is not None
                and int(claim.get("retry_count") or 0) >= self.retry.max_attempts
            ):
                exhausted = {
                    **detail,
                    "reason": "retry_exhausted",
                    "attempts": int(claim.get("retry_count") or 0),
                    "last_reason": detail.get("reason") or "retryable_error",
                }
                claim_extra["next_retry_at"] = None
                _open_review(
                    self.store,
                    episode_id,
                    ReviewReason.UNEXPECTED_REMOTE_STATE,
                    exhausted,
                    resume_status=EpisodeStatus.LIBRARY_PENDING.value,
                    extra=claim_extra,
                    secrets=self.secrets,
                )
                return ServiceResult(
                    EpisodeStatus.NEEDS_REVIEW.value,
                    detail=exhausted,
                    review_reason=ReviewReason.UNEXPECTED_REMOTE_STATE,
                )
            _transition_pending(
                self.store, episode_id, EpisodeStatus.LIBRARY_PENDING.value,
                detail, extra=claim_extra, secrets=self.secrets,
            )
            return ServiceResult(EpisodeStatus.LIBRARY_PENDING.value, detail=detail)
        detail = {"reason": "emby_refresh_accepted", **absent}
        _transition_pending(
            self.store, episode_id, EpisodeStatus.LIBRARY_PENDING.value,
            detail, extra=claim_extra, secrets=self.secrets,
        )
        return ServiceResult(EpisodeStatus.LIBRARY_PENDING.value, detail=detail)

    def _resolve_library(self, row: Mapping[str, Any]) -> Mapping[str, Any] | None:
        finder = getattr(self.emby, "find_library", None)
        if callable(finder):
            if not self.library_name or not self.library_root:
                raise RuntimeError("Emby library name and root are required")
            try:
                library = finder(self.library_name, self.library_root)
            except NotImplementedError:
                library = None
            if library is not None:
                if not isinstance(library, Mapping):
                    raise RuntimeError("Emby library result is malformed")
                return library
            if callable(getattr(self.emby, "iter_episodes", None)):
                return None
        library_id = _episode_value(row, "library_id") or getattr(self.emby, "library_id", None)
        if library_id in (None, ""):
            return None
        return {"Id": str(library_id), "Locations": [self.library_root]}

    def _find_episode(self, identity: Mapping[str, Any], expected_path: str) -> Any:
        finder = getattr(self.emby, "find_episode", None)
        if not callable(finder):
            raise RuntimeError("Emby episode query is unavailable")
        # V5 adapters receive one identity and the exact current mounted path.
        # The legacy fallback is intentionally limited to adapters that expose
        # the old five-argument port; it cannot authorize a path-less match.
        try:
            return finder(identity, expected_path)
        except TypeError as exc:
            message = str(exc)
            if "positional" not in message and "argument" not in message:
                raise
            return finder(
                identity.get("library_id"),
                identity.get("canonical_work", identity.get("work", "")),
                identity.get("season"),
                identity.get("episode"),
                expected_path,
            )

    def _transition_ready(
        self,
        episode_id: int,
        notification_key: str,
        message: str,
        detail: Mapping[str, Any],
        *,
        library_id: str,
        library_path: str,
        emby_path: str,
    ) -> None:
        transition = getattr(self.store, "transition_library_ready_with_notification", None)
        if not callable(transition):
            raise RuntimeError("state store lacks atomic library-ready transition")
        extra = {
            "library_id": library_id,
            "library_path": library_path,
            "retry_count": 0,
            "next_retry_at": None,
            "last_error": "",
        }
        # ``emby_path`` is durable evidence, while the V5 schema uses strm_path
        # for the LitePan canonical path.  Keep the exact Emby path in history
        # and library_path without introducing a second schema alias.
        safe_detail = {**dict(detail), "emby_path": emby_path}
        try:
            transition(
                episode_id,
                notification_key,
                message,
                safe_detail,
                extra={**extra, "library_path": library_path},
                secrets=self.secrets,
            )
        except TypeError as exc:
            if "argument" not in str(exc) and "positional" not in str(exc):
                raise
            try:
                transition(
                    episode_id,
                    notification_key,
                    message,
                    safe_detail,
                    extra={**extra, "library_path": library_path},
                )
            except TypeError as fallback_exc:
                if "argument" not in str(fallback_exc) and "positional" not in str(fallback_exc):
                    raise
                transition(episode_id, notification_key, message, safe_detail)


def _episode_row(store: Any, episode: Any) -> Mapping[str, Any]:
    if isinstance(episode, Mapping):
        return episode
    if isinstance(episode, int):
        getter = getattr(store, "get_episode", None) or getattr(store, "get_by_id", None)
        if callable(getter):
            row = getter(episode)
            if row is not None:
                return row
    if hasattr(episode, "__dict__"):
        return vars(episode)
    raise TypeError("episode must be a mapping or persisted episode ID")


def _episode_id(row: Mapping[str, Any]) -> int:
    value = row.get("id", row.get("episode_id"))
    if value is None:
        raise ValueError("episode id is required")
    return int(value)


def _status(row: Mapping[str, Any]) -> str:
    value = row.get("status", row.get("stage", ""))
    return value.value if isinstance(value, EpisodeStatus) else str(value or "")


def _episode_value(row: Mapping[str, Any], *names: str) -> Any:
    for name in names:
        value = row.get(name)
        if value not in (None, ""):
            return value
    return None


def _runtime_config_section_value(runtime_config: Any, name: str) -> Any:
    if runtime_config is None:
        return None
    if isinstance(runtime_config, Mapping):
        if name in runtime_config:
            return runtime_config.get(name)
        nested = runtime_config.get("runtime")
        if isinstance(nested, Mapping):
            return nested.get(name)
    value = getattr(runtime_config, name, None)
    if value not in (None, ""):
        return value
    nested = getattr(runtime_config, "runtime", None)
    return getattr(nested, name, None) if nested is not None else None


def _canonical_emby_path(row: Mapping[str, Any], mapper: PathMapper | None) -> str:
    if mapper is None:
        return ""
    persisted_emby = _episode_value(row, "emby_path")
    strm_path = _episode_value(row, "strm_path")
    cloud_path = _episode_value(row, "cloud_path")
    for value in (cloud_path, strm_path, persisted_emby):
        if value not in (None, "") and not _strict_service_path(value):
            return ""
    try:
        if cloud_path:
            litepan_path = mapper.cloud_to_published_litepan(str(cloud_path), row)
            if strm_path not in (None, "") and str(strm_path) != litepan_path:
                return ""
        elif strm_path:
            mapper.relative_to(str(strm_path), mapper.roots.litepan_output_root,
                               field="strm_path")
            litepan_path = str(strm_path)
        else:
            return ""
        if not litepan_path.casefold().endswith(".strm"):
            return ""
        expected = mapper.litepan_to_emby(litepan_path)
        if not expected.casefold().endswith(".strm"):
            return ""
        expected = _safe_emby_path(expected, mapper=mapper)
        if not expected:
            return ""
        if persisted_emby not in (None, "") and _safe_emby_path(persisted_emby, mapper=mapper) != expected:
            return ""
        return expected
    except (PathMappingError, TypeError, ValueError):
        return ""


def _safe_emby_path(value: Any, mapper: PathMapper | None = None) -> str:
    if value in (None, ""):
        return ""
    text = str(value)
    if (text != text.strip() or "\\" in text or "%" in text
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in text)
            or not text.startswith("/") or text.startswith("//") or "://" in text
            or "?" in text or "#" in text):
        return ""
    if any(part in {".", "..", ""} for part in text.split("/")[1:]):
        return ""
    normalized = posixpath.normpath(text)
    if normalized == "." or any(part in {".", "..", ""} for part in normalized.split("/")[1:]):
        return ""
    if mapper is not None:
        try:
            root = mapper.roots.emby_mount_root
            relative = mapper.relative_to(normalized, root, field="emby_path")
            normalized = root.rstrip("/") + ("/" + relative if relative else "")
        except Exception:
            return ""
    return normalized


def _strict_service_path(value: Any) -> bool:
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    if ("%" in value or "\\" in value or "://" in value
            or "?" in value or "#" in value
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in value)):
        return False
    if not value.startswith("/") or value.startswith("//"):
        return False
    parts = value.split("/")[1:]
    return not any(part in {"", ".", ".."} for part in parts)


def _emby_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    aliases = row.get("aliases", row.get("aliases_json", []))
    if isinstance(aliases, str):
        try:
            aliases = json.loads(aliases)
        except (TypeError, ValueError):
            aliases = [aliases]
    if not isinstance(aliases, (list, tuple, set)):
        aliases = []
    return {
        "media_source": _episode_value(row, "media_source") or "tmdb",
        "media_id": _episode_value(row, "media_id", "tmdb_id") or "",
        "canonical_work": _episode_value(row, "canonical_work", "work", "title") or "",
        "aliases": [str(alias) for alias in aliases if str(alias).strip()],
        "season": _episode_value(row, "season"),
        "episode": _episode_value(row, "episode"),
        "series_id": _episode_value(row, "series_id", "known_series_id") or "",
    }


def _library_id(library: Mapping[str, Any]) -> str:
    value = library.get("Id", library.get("id", library.get("library_id")))
    return str(value).strip() if value not in (None, "") else ""


def _item_id(item: Any) -> str:
    if not isinstance(item, Mapping):
        return ""
    value = item.get("Id", item.get("id"))
    return str(value).strip() if value not in (None, "") else ""


def _library_notification_key(
    row: Mapping[str, Any], episode_id: int, secrets: Iterable[Any] = ()
) -> str:
    source = _episode_value(row, "media_source") or "tmdb"
    media_id = _episode_value(row, "media_id", "tmdb_id") or str(episode_id)
    season = _episode_value(row, "season") or 0
    episode = _episode_value(row, "episode") or episode_id
    return sanitize_text(
        f"library-ready:{source}:{media_id}:S{season}:E{episode}", secrets=secrets
    )


def _library_notification_message(
    row: Mapping[str, Any], secrets: Iterable[Any] = ()
) -> str:
    work = _episode_value(row, "canonical_work", "work", "title") or "episode"
    season = _episode_value(row, "season") or 0
    episode = _episode_value(row, "episode") or 0
    return sanitize_text(f"新集入库: {work} S{season}E{episode}", secrets=secrets)


def _canonical_strm_path(
    row: Mapping[str, Any],
    mapper: PathMapper | None,
    *,
    require_persisted: bool = False,
) -> str:
    """Derive the one LitePan path from authoritative cloud evidence.

    Persisted service-path aliases are never used as a source of truth.  They
    are accepted only when they exactly equal the mapped target (or the
    corresponding mount mapping), which prevents a stale/escaped path from
    steering a trigger or a MoviePilot scrape.
    """

    if mapper is None:
        return ""
    cloud_path = row.get("cloud_path")
    if not isinstance(cloud_path, str) or not cloud_path:
        return ""
    try:
        canonical = mapper.cloud_to_published_litepan(cloud_path, row)
    except (PathMappingError, TypeError, ValueError):
        return ""
    if not isinstance(canonical, str) or not canonical.casefold().endswith(".strm"):
        return ""

    persisted = {
        "strm_path": canonical,
        "litepan_path": canonical,
    }
    try:
        persisted["moviepilot_path"] = mapper.litepan_to_moviepilot(canonical)
        persisted["emby_path"] = mapper.litepan_to_emby(canonical)
    except (PathMappingError, TypeError, ValueError):
        return ""
    if require_persisted:
        for key, expected in persisted.items():
            if key in row and row.get(key) not in (None, ""):
                value = row.get(key)
                if not isinstance(value, str) or value != expected:
                    return ""
        value = row.get("strm_path")
        if not isinstance(value, str) or value != canonical:
            return ""
    # Touch every persisted alias so a later refactor cannot accidentally
    # reintroduce one as an alternate target.  Mismatches are deliberately
    # ignored; the canonical path remains the only returned value.
    for key, expected in persisted.items():
        value = row.get(key)
        if value not in (None, "") and (not isinstance(value, str) or value != expected):
            continue
    return canonical


def _publish_local_strm(
    source: str, target: str, output_root: str, raw_output_root: str
) -> None:
    """Publish a validated raw STRM under its canonical name without replace."""

    root = os.path.abspath(output_root)
    raw_root = os.path.abspath(raw_output_root)
    source = os.path.abspath(source)
    target = os.path.abspath(target)
    if (
        os.path.commonpath((root, target)) != root
        or os.path.commonpath((raw_root, source)) != raw_root
    ):
        raise ValueError("STRM projection escaped the configured output root")
    if (
        source == target or not os.path.isdir(root) or os.path.islink(root)
        or not os.path.isdir(raw_root) or os.path.islink(raw_root)
    ):
        raise ValueError("STRM projection root is invalid")
    root_real = os.path.realpath(root)
    raw_root_real = os.path.realpath(raw_root)
    if os.path.commonpath((raw_root_real, os.path.realpath(source))) != raw_root_real:
        raise ValueError("raw STRM resolves outside output root")

    source_fd = os.open(source, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    try:
        if not stat.S_ISREG(os.fstat(source_fd).st_mode):
            raise ValueError("raw STRM is not a regular file")
        with os.fdopen(source_fd, "rb", closefd=False) as handle:
            content = handle.read(65537)
        if not content or len(content) > 65536:
            raise ValueError("raw STRM size is invalid")
        # The published artifact is exactly the playback URL: LitePan's
        # trailing POSIX newline is not carried over.  Size stays within the
        # bound above because rstrip only ever shortens the content.
        content = content.rstrip(b"\r\n")
        if not content:
            raise ValueError("raw STRM content is empty")
    finally:
        os.close(source_fd)

    parent = os.path.dirname(target)
    if os.path.commonpath((root_real, os.path.realpath(parent))) != root_real:
        raise ValueError("STRM target parent resolves outside output root")
    os.makedirs(parent, exist_ok=True)
    if os.path.commonpath((root_real, os.path.realpath(parent))) != root_real:
        raise ValueError("STRM target parent changed during publication")

    temp_path = ""
    try:
        fd, temp_path = tempfile.mkstemp(prefix=".anime-strm-", dir=parent)
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
            handle.flush()
            os.fsync(handle.fileno())
        try:
            os.link(temp_path, target)
        except FileExistsError:
            target_fd = os.open(target, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
            try:
                if not stat.S_ISREG(os.fstat(target_fd).st_mode):
                    raise ValueError("published STRM is not a regular file")
                with os.fdopen(target_fd, "rb", closefd=False) as handle:
                    existing = handle.read(65537)
                # A migrated v3 published STRM still carries LitePan's trailing
                # newline, which this projection deliberately strips.  Compare
                # the normalized existing bytes so republishing identical
                # content stays idempotent while a genuinely different URL is
                # still rejected (no-replace semantics preserved).
                if existing.rstrip(b"\r\n") != content:
                    raise ValueError("published STRM has different content")
            finally:
                os.close(target_fd)
    finally:
        if temp_path:
            os.unlink(temp_path)


def _runtime_config_value(value: Any, name: str) -> str:
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{name} must be explicit and non-empty")
    text = value.strip()
    if text.casefold() in {"*", "any", "all", ".*"}:
        raise ValueError(f"{name} must not be a wildcard")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in text):
        raise ValueError(f"{name} contains control characters")
    if name.endswith("root"):
        if not text.startswith("/") or text.startswith("//"):
            raise ValueError(f"{name} must be an absolute POSIX path")
        if "\\" in text or "://" in text or "?" in text or "#" in text:
            raise ValueError(f"{name} contains unsafe path syntax")
        if any(part in {".", ".."} for part in text.split("/")):
            raise ValueError(f"{name} contains traversal")
    return text


def _runtime_identity(row: Mapping[str, Any]) -> dict[str, Any]:
    value = row.get("runtime_identity") or row.get("runtime_identity_json") or {}
    if isinstance(value, str):
        try:
            value = json.loads(value)
        except (TypeError, ValueError):
            value = {}
    return dict(value) if isinstance(value, Mapping) else {}


def _download_file_runtime_identity(
    row: Mapping[str, Any], remote_file: Any, infohash: str, file_id: Any,
) -> dict[str, Any]:
    """Persist the selected inventory evidence on the episode row.

    ``download_files`` remains the normalized inventory table, while this
    compact projection is the organizer's crash-safe input when it receives
    a plain StateStore episode ID on the next runner pass.
    """

    result = _runtime_identity(row)
    remote_id = _task_value(remote_file, "remote_file_id", "file_id", "id") or file_id
    if remote_id not in (None, ""):
        result["remote_file_id"] = remote_id
    for key in ("size", "fingerprint", "parent_id", "parent_path"):
        value = _task_value(remote_file, key)
        if value not in (None, ""):
            result[key] = value
    name = _task_value(remote_file, "filename", "name")
    path = _task_value(remote_file, "path", "remote_path", "source_path", "cloud_source_path")
    parent_path = _task_value(remote_file, "parent_path")
    if not path and parent_path and name:
        path = posixpath.join(str(parent_path), str(name))
    if path not in (None, ""):
        result["source_path"] = str(path)
    if parent_path not in (None, ""):
        result["source_parent_path"] = str(parent_path)
    parent_id = _task_value(remote_file, "parent_id")
    if parent_id not in (None, ""):
        result["source_parent_id"] = parent_id
    if name not in (None, ""):
        result["source_filename"] = str(name)
        suffix = posixpath.splitext(posixpath.basename(str(name).replace("\\", "/")))[1]
        if suffix:
            result["_extension"] = suffix
    if infohash:
        result["infohash"] = infohash
    # Preserve explicitly verified date evidence when a caller supplies a
    # compatibility row rather than one freshly loaded from StateStore.
    for key in ("_season_air_date", "season_air_date", "tmdb_season_air_date"):
        value = row.get(key)
        if value not in (None, ""):
            result.setdefault("_season_air_date", value)
            result.setdefault("season_air_date", value)
    return result


def _persisted_task_identity(row: Mapping[str, Any], secrets: Iterable[Any]) -> dict[str, Any]:
    value = _runtime_identity(row)
    allowed = ("task_id", "litepan_task_id", "task_name", "source_root", "output_root")
    return {
        key: sanitize_text(value[key], secrets=secrets)
        for key in allowed
        if value.get(key) not in (None, "")
    }


def _not_due(row: Mapping[str, Any], now: float) -> bool:
    value = row.get("next_retry_at")
    try:
        return value not in (None, "") and float(value) > now
    except (TypeError, ValueError):
        return False


def _transition_pending(store: Any, episode_id: int, status: str, detail: Mapping[str, Any], *, extra: Mapping[str, Any] | None = None, secrets: Iterable[Any] = ()) -> None:
    _transition(store, episode_id, status, detail, extra=extra, secrets=secrets)


def _claim_library_refresh(
    store: Any,
    episode_id: int,
    detail: Mapping[str, Any],
    *,
    now: float,
    base: float,
    cap: float,
    extra: Mapping[str, Any],
    secrets: Iterable[Any] = (),
    retry_policy: _RetrySettings | None = None,
) -> Mapping[str, Any] | None:
    method = getattr(store, "claim_library_refresh", None)
    if callable(method):
        return method(
            episode_id, detail, now=now, base=base, cap=cap,
            extra=extra,
            jitter_seconds=(retry_policy.jitter_seconds if retry_policy else 0.0),
            random_source=(retry_policy.random_source if retry_policy else None),
            secrets=secrets,
        )
    # Compatibility fakes from earlier stages do not expose the atomic port.
    # They retain the old bounded behavior, while the real StateStore path is
    # always the claim/lease implementation above.
    _transition_pending(store, episode_id, EpisodeStatus.LIBRARY_PENDING.value,
                        detail, extra=extra, secrets=secrets)
    return {"retry_count": 1, "next_retry_at": now + base}


def _transition_ready(store: Any, episode_id: int, status: str, detail: Mapping[str, Any], *, extra: Mapping[str, Any] | None = None, secrets: Iterable[Any] = ()) -> None:
    _transition(store, episode_id, status, detail, extra=extra, secrets=secrets)


def _transition(store: Any, episode_id: int, status: str, detail: Mapping[str, Any], *, extra: Mapping[str, Any] | None = None, secrets: Iterable[Any] = ()) -> None:
    method = getattr(store, "transition", None)
    if not callable(method):
        method = getattr(store, "set_stage", None)
    if not callable(method):
        raise TypeError("state store has no transition method")
    safe_detail = _safe_detail(detail, secrets)
    safe_extra = dict(extra or {})
    for key in ("cloud_path", "strm_path", "last_error"):
        if key in safe_extra:
            safe_extra[key] = sanitize_text(safe_extra[key], secrets=secrets)
    if "runtime_identity_json" in safe_extra:
        safe_extra["runtime_identity_json"] = _safe_detail(
            safe_extra["runtime_identity_json"], secrets
        )
    try:
        method(episode_id, status, safe_detail, extra=safe_extra, secrets=secrets)
    except TypeError:
        # Small local fakes commonly expose only the three positional state
        # arguments.  The full StateStore path above remains the canonical one.
        method(episode_id, status, safe_detail)


def _transition_if_current(
    store: Any,
    episode_id: int,
    expected_status: str,
    status: str,
    detail: Mapping[str, Any],
    *,
    extra: Mapping[str, Any] | None = None,
    secrets: Iterable[Any] = (),
) -> bool:
    method = getattr(store, "transition_if_current", None)
    safe_detail = _safe_detail(detail, secrets)
    safe_extra = dict(extra or {})
    for key in ("cloud_path", "strm_path", "last_error"):
        if key in safe_extra:
            safe_extra[key] = sanitize_text(safe_extra[key], secrets=secrets)
    if callable(method):
        return bool(method(
            episode_id,
            expected_status,
            status,
            safe_detail,
            extra=safe_extra,
            secrets=secrets,
        ))
    # Compatibility fakes do not expose the atomic port.  They still get a
    # fresh status guard; the real StateStore always takes the CAS path.
    current = getattr(store, "get_episode", lambda _id: None)(episode_id)
    if current is None or _status(current) != expected_status:
        return False
    _transition(store, episode_id, status, safe_detail, extra=safe_extra, secrets=secrets)
    return True


def _open_review(
    store: Any,
    episode_id: int,
    reason: ReviewReason,
    detail: Mapping[str, Any],
    *,
    resume_status: str | EpisodeStatus | None = None,
    extra: Mapping[str, Any] | None = None,
    secrets: Iterable[Any] = (),
) -> None:
    method = getattr(store, "open_review", None)
    if not callable(method):
        _transition(
            store, episode_id, EpisodeStatus.NEEDS_REVIEW.value, detail, secrets=secrets
        )
        return
    safe_detail = _safe_detail(detail, secrets)
    try:
        method(
            episode_id, reason, safe_detail,
            resume_status=resume_status, extra=dict(extra or {}), secrets=secrets,
        )
    except TypeError as exc:
        if "extra" not in str(exc) and "resume_status" not in str(exc):
            raise
        try:
            method(episode_id, reason, safe_detail, resume_status=resume_status, secrets=secrets)
        except TypeError:
            method(episode_id, reason, safe_detail)


def _retry_pending(
    store: Any,
    episode_id: int,
    status: str,
    row: Mapping[str, Any],
    detail: Mapping[str, Any],
    *,
    now: Callable[[], float],
    base: float,
    cap: float,
    secrets: Iterable[Any] = (),
    extra: Mapping[str, Any] | None = None,
    retry_policy: _RetrySettings | None = None,
) -> str:
    try:
        previous = int(row.get("retry_count") or 0)
    except (TypeError, ValueError):
        previous = 0
    count = previous + 1
    settings = retry_policy or row.get("_retry_settings")
    if settings is not None:
        base = settings.backoff_seconds
        cap = settings.backoff_max_seconds
    delay = retry_delay(
        settings or {
            "backoff_seconds": base,
            "backoff_max_seconds": cap,
            "jitter_seconds": 0.0,
        },
        count,
        settings.random_source if settings is not None else None,
    )
    timestamp = float(now())
    values = dict(extra or {})
    values.update({
        "retry_count": count,
        "last_error": _safe_error_text(detail, secrets),
    })
    exhausted = (
        settings is not None
        and settings.max_attempts is not None
        and count >= settings.max_attempts
    )
    if exhausted:
        values["next_retry_at"] = None
        exhausted_detail = dict(detail)
        exhausted_detail.update({
            "reason": "retry_exhausted",
            "attempts": count,
            "last_reason": detail.get("reason") or "retryable_error",
        })
        _open_review(
            store,
            episode_id,
            ReviewReason.UNEXPECTED_REMOTE_STATE,
            exhausted_detail,
            resume_status=status,
            extra=values,
            secrets=secrets,
        )
        return EpisodeStatus.NEEDS_REVIEW.value
    values["next_retry_at"] = timestamp + delay
    _transition(store, episode_id, status, detail, extra=values, secrets=secrets)
    return status


def _safe_error_text(detail: Mapping[str, Any], secrets: Iterable[Any]) -> str:
    reason = str(detail.get("reason") or "retryable_error")
    error = detail.get("error")
    if error:
        reason += f": {_safe_error(error)}"
    return sanitize_text(reason, secrets=secrets)


def _safe_error(exc: BaseException) -> str:
    status = getattr(exc, "status", None)
    if status is not None:
        try:
            return f"http_{int(status)}"
        except (TypeError, ValueError):
            return "http_error"
    return type(exc).__name__


def _task_value(value: Any, *names: str) -> Any:
    if isinstance(value, Mapping):
        for name in names:
            item = value.get(name)
            if item not in (None, ""):
                return item
        return None
    for name in names:
        item = getattr(value, name, None)
        if item not in (None, ""):
            return item
    return None


def _download_infohash(store: Any, row: Mapping[str, Any]) -> str:
    value = row.get("infohash") or row.get("info_hash")
    if value:
        return str(value).strip().lower()
    getter = getattr(store, "get_candidates", None)
    if callable(getter) and row.get("id") is not None:
        candidates = getter(int(row["id"])) or []
        for candidate in candidates:
            if candidate.get("selected"):
                return str(candidate.get("infohash") or "").strip().lower()
        if candidates:
            return str(candidates[0].get("infohash") or "").strip().lower()
    return ""


def _selected_candidate_infohash(store: Any, row: Mapping[str, Any]) -> str:
    getter = getattr(store, "get_candidates", None)
    if not callable(getter) or row.get("id") is None:
        return ""
    selected = [
        candidate for candidate in (getter(int(row["id"])) or [])
        if bool(candidate.get("selected"))
    ]
    if len(selected) != 1:
        return ""
    return str(selected[0].get("infohash") or "").strip().lower()


def _eligible_download_target(
    store: Any,
    row: Mapping[str, Any],
    infohash: str,
) -> bool:
    if _status(row) != EpisodeStatus.SUBMITTED.value:
        return False
    # A stale submitted label must not bypass an unresolved review.
    if row.get("review_reason") and row.get("review_resolved_at") in (None, ""):
        return False
    return _selected_candidate_infohash(store, row) == infohash


def _episode_key(value: Any) -> EpisodeKey | None:
    if isinstance(value, EpisodeKey):
        return value
    if isinstance(value, Mapping):
        nested = value.get("key") or value.get("episode_key")
        if nested is not None:
            return _episode_key(nested)
        source = value.get("media_source")
        media_id = value.get("media_id")
        season = value.get("season")
        episode = value.get("episode")
    elif isinstance(value, (tuple, list)) and len(value) == 4:
        source, media_id, season, episode = value
    else:
        source = getattr(value, "media_source", None)
        media_id = getattr(value, "media_id", None)
        season = getattr(value, "season", None)
        episode = getattr(value, "episode", None)
    try:
        if isinstance(season, bool) or isinstance(episode, bool):
            return None
        source_text = str(source or "").strip()
        media_text = str(media_id or "").strip()
        if not source_text or not media_text:
            return None
        return EpisodeKey(source_text, media_text, int(season), int(episode))
    except (TypeError, ValueError):
        return None


def _inventory_identity(value: Any) -> tuple[str, str, str, str]:
    return (
        str(_task_value(value, "infohash", "info_hash", "hash") or "").strip().lower(),
        str(_task_value(value, "remote_file_id", "file_id", "id") or ""),
        str(_task_value(value, "path", "parent_path") or ""),
        str(_task_value(value, "name", "filename") or ""),
    )


def _inventory_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    converter = getattr(value, "to_mapping", None)
    if callable(converter):
        return dict(converter())
    return {
        key: getattr(value, key)
        for key in (
            "infohash", "remote_file_id", "parent_id", "parent_path", "path",
            "filename", "name", "size", "duration", "fingerprint", "is_dir",
        )
        if hasattr(value, key)
    }


def _is_video_inventory(value: Any) -> bool:
    if isinstance(value, Mapping) and bool(value.get("is_dir", False)):
        return False
    name = _task_value(value, "name", "filename", "path")
    if not name:
        return False
    return os.path.splitext(str(name))[1].casefold() in {
        ".mkv", ".mp4", ".m4v", ".avi", ".mov", ".ts", ".webm",
    }


def _safe_detail(detail: Any, secrets: Iterable[Any] = ()) -> dict[str, Any]:
    if isinstance(detail, Mapping):
        result: dict[str, Any] = {}
        for key, value in detail.items():
            if isinstance(value, Mapping):
                result[str(key)] = _safe_detail(value, secrets)
            elif isinstance(value, (list, tuple)):
                result[str(key)] = [
                    _safe_detail(item, secrets) if isinstance(item, Mapping)
                    else sanitize_text(item, secrets=secrets)
                    for item in value
                ]
            else:
                result[str(key)] = sanitize_text(value, secrets=secrets)
        return result
    return {"value": sanitize_text(detail, secrets=secrets)}


def _review_reason(value: Any) -> ReviewReason:
    if isinstance(value, ReviewReason):
        return value
    try:
        return ReviewReason(str(value or "").strip())
    except ValueError as exc:
        raise ValueError("unknown review reason") from exc


def _sanitize_review_value(value: Any, secrets: Iterable[Any]) -> Any:
    """Sanitize JSON-compatible review evidence without retaining raw input."""

    if isinstance(value, Mapping):
        return {
            sanitize_text(key, secrets=secrets): _sanitize_review_value(item, secrets)
            for key, item in value.items()
        }
    if isinstance(value, (list, tuple)):
        return [_sanitize_review_value(item, secrets) for item in value]
    if isinstance(value, (str, int, float, bool)) or value is None:
        return sanitize_text(value, secrets=secrets) if isinstance(value, str) else value
    return sanitize_text(value, secrets=secrets)


def _decode_review_mapping(value: Mapping[str, Any] | str) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if not isinstance(value, str):
        raise ValueError("resolution must be a JSON object")
    try:
        parsed = json.loads(value)
    except (TypeError, ValueError) as exc:
        raise ValueError("resolution must be a JSON object") from exc
    if not isinstance(parsed, Mapping):
        raise ValueError("resolution must be a JSON object")
    return dict(parsed)


def _validate_review_resolution(
    reason: ReviewReason,
    resolution: Mapping[str, Any] | str,
    secrets: Iterable[Any],
) -> dict[str, Any]:
    raw = _decode_review_mapping(resolution)
    unknown = {
        str(key) for key in raw
        if str(key) not in ReviewService._ALLOWED_KEYS
    }
    if unknown:
        raise ValueError("resolution contains unknown fields")
    safe = _sanitize_review_value(raw, secrets)
    if not isinstance(safe, dict) or not safe:
        raise ValueError(f"resolution for {reason.value} must not be empty")

    required_groups = ReviewService._REQUIRED[reason]
    if reason is ReviewReason.AMBIGUOUS_EPISODE:
        has_file_choice = any(
            safe.get(key) not in (None, "", [], {})
            for key in required_groups[0]
        )
        has_episode_choice = all(
            safe.get(key) not in (None, "", [], {})
            for key in required_groups[1]
        )
        if not (has_file_choice or has_episode_choice):
            raise ValueError(
                "resolution for AMBIGUOUS_EPISODE requires a file/choice or season and episode"
            )
    elif reason is ReviewReason.LEGACY_IDENTITY_UNRESOLVED:
        has_choice = any(
            safe.get(key) not in (None, "", [], {})
            for key in required_groups[0]
        )
        has_identity = all(
            safe.get(key) not in (None, "", [], {})
            for key in required_groups[1]
        )
        if not (has_choice or has_identity):
            raise ValueError(
                "resolution for LEGACY_IDENTITY_UNRESOLVED requires choice or identity fields"
            )
    else:
        for group in required_groups:
            if all(key not in safe or safe.get(key) in (None, "", [], {}) for key in group):
                raise ValueError(
                    f"resolution for {reason.value} requires one of: {', '.join(group)}"
                )
    if reason is ReviewReason.MULTIPLE_VIDEO_FILES:
        _strict_file_resolution_id(
            _one_resolution_alias(safe, "selected_file_id", "file_id"),
            "selected_file_id",
        )
    elif reason is ReviewReason.DUPLICATE_EPISODE:
        _strict_resolution_int(
            _one_resolution_alias(safe, "selected_candidate_id", "candidate_id"),
            "selected_candidate_id",
        )
    elif reason is ReviewReason.LEGACY_IDENTITY_UNRESOLVED:
        identity_keys = {"media_id", "season", "episode"}
        present_identity = identity_keys & set(safe)
        if present_identity and present_identity != identity_keys:
            raise ValueError(
                "LEGACY_IDENTITY_UNRESOLVED identity requires media_id, season, and episode"
            )
        if present_identity:
            _strict_resolution_id(safe["media_id"], "media_id")
            _strict_resolution_int(safe["season"], "season", minimum=0)
            _strict_resolution_int(safe["episode"], "episode", minimum=1)
            if "media_source" in safe:
                _strict_resolution_id(safe["media_source"], "media_source")
    elif reason is ReviewReason.RUNTIME_MAPPING_AMBIGUOUS:
        _strict_resolution_id(safe.get("task_id"), "task_id")
        for key in ("task_name", "source_root", "output_root"):
            if key in safe:
                _strict_resolution_id(safe[key], key)
    return safe


def _one_resolution_alias(values: Mapping[str, Any], *keys: str) -> Any:
    present = [values[key] for key in keys if key in values]
    if not present:
        return None
    first = present[0]
    if any(value != first for value in present[1:]):
        raise ValueError(f"resolution aliases disagree: {', '.join(keys)}")
    return first


def _strict_resolution_id(value: Any, field: str) -> str:
    if (
        type(value) is not str
        or not value.strip()
        or value != value.strip()
        or REDACTED in value
    ):
        raise ValueError(f"{field} must be a non-empty trimmed string")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        raise ValueError(f"{field} contains control characters")
    return value


def _strict_file_resolution_id(value: Any, field: str) -> str | int:
    if type(value) is int:
        if value <= 0:
            raise ValueError(f"{field} must be a positive integer")
        return value
    return _strict_resolution_id(value, field)


def _strict_resolution_int(value: Any, field: str, *, minimum: int | None = None) -> int:
    if type(value) is not int:
        raise ValueError(f"{field} must be an integer")
    if minimum is not None and value < minimum:
        raise ValueError(f"{field} is outside the valid range")
    return value


def _parse_review_json(value: Any) -> Any:
    if isinstance(value, (dict, list)):
        return value
    try:
        return json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}


def _review_projection(row: Mapping[str, Any], secrets: Iterable[Any]) -> dict[str, Any]:
    reason = sanitize_text(row.get("review_reason") or "", secrets=secrets)
    detail = _sanitize_review_value(_parse_review_json(row.get("review_detail")), secrets)
    resolution = _sanitize_review_value(_parse_review_json(row.get("review_resolution")), secrets)
    episode_id = row.get("id")
    return {
        "review_id": int(episode_id) if episode_id is not None else None,
        "episode_id": int(episode_id) if episode_id is not None else None,
        "id": int(episode_id) if episode_id is not None else None,
        "status": sanitize_text(row.get("status") or "", secrets=secrets),
        "review_reason": reason,
        "reason": reason,
        "review_detail": detail if isinstance(detail, dict) else {},
        "detail": detail if isinstance(detail, dict) else {},
        "review_created_at": row.get("review_created_at"),
        "review_generation": row.get("review_generation"),
        "review_version": row.get("review_generation"),
        "review_resolved_at": row.get("review_resolved_at"),
        "review_resolution": resolution if isinstance(resolution, dict) else {},
        "resolution": resolution if isinstance(resolution, dict) else {},
        "resume_status": sanitize_text(row.get("resume_status") or "", secrets=secrets),
    }


def _unsafe_sidecar_evidence(result: EvidenceResult) -> bool:
    return result.reason in _UNSAFE_SIDECAR_REASONS


def _unsafe_sidecar_detail(result: EvidenceResult, secrets: Iterable[Any]) -> dict[str, Any]:
    return {
        "reason": "unsafe_sidecar_evidence",
        "evidence_reason": sanitize_text(result.reason, secrets=secrets),
    }


_TASK_ALIAS_GROUPS: dict[str, tuple[str, ...]] = {
    "id": ("id", "task_id", "taskId", "tid"),
    "active": ("active", "enabled", "is_active", "status"),
    "name": ("name", "task_name", "title"),
    "source_root": (
        "source_root", "source_path", "source", "scan_root", "scan_path"
    ),
    "output_root": (
        "output_root", "output_path", "output", "target_root", "target_path"
    ),
}


def _task_alias_values(task: Any, names: tuple[str, ...]) -> list[Any]:
    values: list[Any] = []
    for name in names:
        if isinstance(task, Mapping):
            if name not in task:
                continue
            value = task[name]
        else:
            value = getattr(task, name, None)
        if value not in (None, ""):
            values.append(value)
    return values


def _normalize_task_alias(field: str, value: Any) -> Any:
    if field == "active":
        if isinstance(value, bool):
            return value
        if isinstance(value, (int, float)) and value in (0, 1):
            return bool(value)
        text = str(value).strip().casefold()
        if text in {"active", "enabled", "running", "ready", "on", "true", "1"}:
            return True
        if text in {"inactive", "disabled", "stopped", "paused", "off", "false", "0"}:
            return False
        raise RuntimeError("LitePan task inventory entry has malformed active state")

    text = str(value).strip()
    if not text:
        raise RuntimeError(f"LitePan task inventory entry lacks {field}")
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in text):
        raise RuntimeError("LitePan task inventory entry contains unsafe text")
    if field in {"source_root", "output_root"} and text != "/":
        text = text.rstrip("/")
    return text


def _normalized_task_fields(task: Mapping[str, Any]) -> dict[str, Any]:
    fields: dict[str, Any] = {}
    for field, aliases in _TASK_ALIAS_GROUPS.items():
        values = _task_alias_values(task, aliases)
        if not values:
            continue
        normalized = [_normalize_task_alias(field, value) for value in values]
        if any(value != normalized[0] for value in normalized[1:]):
            raise RuntimeError(
                f"LitePan task inventory entry has conflicting {field} aliases"
            )
        fields[field] = normalized[0]
    return fields


def _task_active(task: Any) -> bool:
    return _normalized_task_fields(task).get("active") is True


def _same_root(actual: Any, expected: str | None) -> bool:
    return str(actual or "").rstrip("/") == str(expected).rstrip("/")


def _task_matches(task: Any, *, name: str | None, source_root: str | None, output_root: str | None) -> bool:
    fields = _normalized_task_fields(task)
    if not _task_active(task):
        return False
    if isinstance(task, Mapping) and task.get("recursive") is False:
        return False
    actual_name = fields.get("name")
    if name not in (None, "") and str(actual_name or "") != str(name):
        return False
    actual_source = fields.get("source_root")
    actual_output = fields.get("output_root")
    return _same_root(actual_source, source_root) and _same_root(actual_output, output_root)


def _list_runtime_tasks(adapter: Any) -> list[Any]:
    cls = type(adapter)
    mro = getattr(cls, "__mro__", (cls,))
    methods: list[tuple[int, int, Callable[[], Any]]] = []
    for priority, name in enumerate(("list_runtime_tasks", "list_tasks")):
        method = getattr(adapter, name, None)
        if not callable(method):
            continue
        owner_index = next(
            (index for index, base in enumerate(mro) if name in getattr(base, "__dict__", {})),
            len(mro),
        )
        methods.append((owner_index, priority, method))

    # Prefer a method defined by the concrete adapter.  This prevents a base
    # adapter's NotImplementedError stub from hiding a subclass's real
    # list_runtime_tasks implementation.  If both concrete methods exist, the
    # runtime spelling wins; a malformed response is never masked by fallback.
    if methods:
        minimum_owner = min(item[0] for item in methods)
        for _owner, _priority, method in sorted(
            (item for item in methods if item[0] == minimum_owner),
            key=lambda item: item[1],
        ):
            try:
                return _as_task_list(method())
            except NotImplementedError:
                continue

    tasks = getattr(adapter, "tasks", None)
    if tasks is not None and not callable(tasks):
        return _as_task_list(tasks)

    # A non-standard adapter may provide only an inherited but usable method;
    # invoke those methods only after concrete methods and the explicit tasks
    # inventory have been considered.
    for _owner, _priority, method in sorted(methods, key=lambda item: item[1]):
        try:
            return _as_task_list(method())
        except NotImplementedError:
            continue
    finder = getattr(adapter, "find_task", None)
    if callable(finder):
        value = finder()
        return _as_task_list(value)
    raise RuntimeError("LitePan runtime task listing is unavailable")


def _as_task_list(value: Any) -> list[Any]:
    if value is None:
        raise RuntimeError("LitePan task inventory is incomplete")
    if isinstance(value, (list, tuple)):
        tasks = list(value)
    elif isinstance(value, Mapping):
        _reject_inventory_failure(value)
        tasks = _unwrap_task_envelope(value)
    else:
        raise RuntimeError("LitePan task inventory shape is unsupported")
    if any(not isinstance(task, Mapping) for task in tasks):
        raise RuntimeError("LitePan task inventory contains malformed entries")
    for task in tasks:
        _validate_task_entry(task)
    return tasks


def _unwrap_task_envelope(value: Mapping[str, Any]) -> list[Any]:
    """Collect every envelope layer before flattening any task evidence."""
    containers = _inventory_containers(value)
    inventories = []
    for container in containers:
        if any(key in container for key in ("id", "task_id", "taskId", "tid")):
            inventories.append([container])
        for key in _TASK_LIST_KEYS:
            if isinstance(container.get(key), list):
                inventories.append(container[key])
    if len(inventories) != 1:
        raise RuntimeError("LitePan task inventory envelope is ambiguous or unsupported")
    tasks = inventories[0]
    _validate_pagination_metadata(value, len(tasks))
    return tasks


def _reject_inventory_failure(value: Mapping[str, Any]) -> None:
    for key in ("error", "errors", "failure", "failed"):
        if value.get(key) not in (None, "", [], {}, False):
            raise RuntimeError("LitePan task inventory reported failure")
    for key in ("success", "ok"):
        if key in value and value.get(key) is not True:
            raise RuntimeError("LitePan task inventory reported failure")
    for key in ("state",):
        if key in value and value.get(key) in (False, 0, "false", "failed", "error"):
            raise RuntimeError("LitePan task inventory reported failure")
    for key in ("status", "code"):
        status = value.get(key)
        if status is None:
            continue
        try:
            if int(status) >= 400:
                raise RuntimeError("LitePan task inventory reported failure")
        except (TypeError, ValueError):
            if str(status).casefold() in {"error", "failed", "failure"}:
                raise RuntimeError("LitePan task inventory reported failure")


def _optional_int(value: Mapping[str, Any], *names: str) -> int | None:
    values = _present_pagination_aliases(value, names)
    if not values:
        return None
    normalized: list[int] = []
    for item in values:
        if isinstance(item, bool):
            raise RuntimeError("LitePan task inventory pagination is malformed")
        try:
            number = int(item)
        except (TypeError, ValueError):
            raise RuntimeError("LitePan task inventory pagination is malformed")
        if isinstance(item, float) and number != item:
            raise RuntimeError("LitePan task inventory pagination is malformed")
        if isinstance(item, str) and str(number) != item.strip().lstrip("+"):
            raise RuntimeError("LitePan task inventory pagination is malformed")
        normalized.append(number)
    if any(item != normalized[0] for item in normalized[1:]):
        raise RuntimeError("LitePan task inventory pagination aliases conflict")
    return normalized[0]


_PAGINATION_CONTAINER_KEYS = ("pagination", "meta", "page_info", "pageInfo", "root", "data", "result")
_TASK_LIST_KEYS = ("tasks", "items", "results", "data")
_NEXT_KEYS = ("next", "next_page", "nextPage", "next_cursor", "nextCursor", "cursor",
              "continuation", "continuation_token", "continuationToken")
_PAGINATION_KEYS = set(_NEXT_KEYS) | {
    "total", "total_count", "totalCount", "count", "page", "page_no", "pageNo",
    "current_page", "currentPage", "pages", "page_count", "pageCount", "total_pages",
    "totalPages", "offset", "start", "start_index", "startIndex", "limit", "page_size",
    "pageSize", "size", "has_more", "hasMore", "more",
}


def _inventory_containers(value: Mapping[str, Any]) -> list[Mapping[str, Any]]:
    """Bounded traversal; unknown metadata can never hide a continuation."""
    result = []
    seen: set[int] = set()
    pending = [(value, 0)]
    allowed = _PAGINATION_KEYS | set(_PAGINATION_CONTAINER_KEYS) | set(_TASK_LIST_KEYS) | {
        "error", "errors", "failure", "failed", "success", "ok", "state", "status", "code", "message",
    }
    while pending:
        container, depth = pending.pop()
        if id(container) in seen or depth > 16 or len(result) >= 128:
            raise RuntimeError("LitePan task inventory envelope is cyclic or too deep")
        seen.add(id(container))
        _reject_inventory_failure(container)
        result.append(container)
        # Task settings have a separate schema, but known nested envelope
        # fields still need traversal even on a singleton task response.
        is_task = any(key in container for key in ("id", "task_id", "taskId", "tid"))
        if not is_task and set(container) - allowed:
            raise RuntimeError("LitePan task inventory envelope has unknown metadata")
        for key in set(_PAGINATION_CONTAINER_KEYS) | set(_TASK_LIST_KEYS):
            if key not in container:
                continue
            nested = container[key]
            if key in _TASK_LIST_KEYS and isinstance(nested, list):
                continue
            if isinstance(nested, Mapping) and key in _PAGINATION_CONTAINER_KEYS:
                pending.append((nested, depth + 1))
            else:
                raise RuntimeError("LitePan task inventory envelope is malformed")
    return result


def _present_pagination_aliases(value: Mapping[str, Any], names: tuple[str, ...]) -> list[Any]:
    return [
        value[name]
        for name in names
        if name in value
    ]


def _optional_bool(value: Mapping[str, Any], *names: str) -> bool | None:
    values = _present_pagination_aliases(value, names)
    if not values:
        return None
    normalized: list[bool] = []
    for item in values:
        if isinstance(item, bool):
            normalized.append(item)
            continue
        if isinstance(item, (int, float)) and item in (0, 1):
            normalized.append(bool(item))
            continue
        text = str(item).strip().casefold()
        if text in {"true", "1", "yes", "on"}:
            normalized.append(True)
        elif text in {"false", "0", "no", "off"}:
            normalized.append(False)
        else:
            raise RuntimeError("LitePan task inventory pagination is malformed")
    if any(item != normalized[0] for item in normalized[1:]):
        raise RuntimeError("LitePan task inventory pagination aliases conflict")
    return normalized[0]


def _optional_marker(value: Mapping[str, Any], *names: str) -> str | None:
    values = [value[name] for name in names if name in value]
    if not values:
        return None
    normalized: list[str | None] = []
    for item in values:
        if item is None or item is False or item == 0 or item == "":
            normalized.append("")
        elif isinstance(item, bool):
            normalized.append("true")
        else:
            text = str(item).strip()
            normalized.append(text)
    if any(item != normalized[0] for item in normalized[1:]):
        raise RuntimeError("LitePan task inventory pagination aliases conflict")
    return normalized[0]


def _pagination_info(value: Mapping[str, Any]) -> dict[str, Any]:
    return {
        "total": _optional_int(value, "total", "total_count", "totalCount", "count"),
        "page": _optional_int(value, "page", "page_no", "pageNo", "current_page", "currentPage"),
        "pages": _optional_int(value, "pages", "page_count", "pageCount", "total_pages", "totalPages"),
        "offset": _optional_int(value, "offset", "start", "start_index", "startIndex"),
        "limit": _optional_int(value, "limit", "page_size", "pageSize", "size"),
        "has_more": _optional_bool(value, "has_more", "hasMore", "more"),
        "next": _optional_marker(
            value,
            "next",
            "next_page",
            "nextPage",
            "next_cursor",
            "nextCursor",
            "cursor",
            "continuation",
            "continuation_token",
            "continuationToken",
        ),
    }


def _merged_pagination_info(value: Mapping[str, Any]) -> dict[str, Any]:
    merged: dict[str, Any] = {}
    containers = _inventory_containers(value)
    for container in containers:
        for field, item in _pagination_info(container).items():
            if item is None:
                continue
            if field in merged and merged[field] != item:
                raise RuntimeError("LitePan task inventory pagination aliases conflict")
            merged[field] = item
    return merged


def _validate_pagination_metadata(value: Mapping[str, Any], entry_count: int | None) -> None:
    info = _merged_pagination_info(value)
    total = info.get("total")
    page = info.get("page")
    pages = info.get("pages")
    offset = info.get("offset")
    limit = info.get("limit")
    has_more = info.get("has_more")
    marker = info.get("next")

    if total is not None and total < 0:
        raise RuntimeError("LitePan task inventory pagination total is invalid")
    if page is not None and page < 1:
        raise RuntimeError("LitePan task inventory pagination page is invalid")
    if pages is not None and pages < 1:
        raise RuntimeError("LitePan task inventory pagination pages is invalid")
    if page is None and pages is not None:
        raise RuntimeError("LitePan task inventory pagination page is missing")
    if page is not None and pages is not None and page > pages:
        raise RuntimeError("LitePan task inventory pagination page is invalid")
    if offset is not None and offset < 0:
        raise RuntimeError("LitePan task inventory pagination offset is invalid")
    if limit is not None and limit < 1:
        raise RuntimeError("LitePan task inventory pagination limit is invalid")
    if marker not in (None, ""):
        if has_more is False:
            raise RuntimeError("LitePan task inventory pagination aliases conflict")
        raise RuntimeError("LitePan task inventory pagination is incomplete")
    if has_more is True:
        raise RuntimeError("LitePan task inventory pagination is incomplete")

    if pages is not None and pages != 1:
        raise RuntimeError("LitePan task inventory pagination is incomplete")
    if page is not None and page != 1:
        raise RuntimeError("LitePan task inventory pagination is incomplete")
    if offset not in (None, 0):
        raise RuntimeError("LitePan task inventory pagination is incomplete")
    if page is not None and offset is not None and limit is not None:
        if offset != (page - 1) * limit:
            raise RuntimeError("LitePan task inventory pagination is inconsistent")

    if entry_count is None:
        return
    if total is not None:
        if entry_count > total or entry_count != total:
            raise RuntimeError("LitePan task inventory pagination is incomplete")
    if limit is not None and entry_count > limit:
        raise RuntimeError("LitePan task inventory pagination is inconsistent")
    if total is None and has_more is not False and marker != "":
        raise RuntimeError("LitePan task inventory completeness is unproven")


def _validate_task_entry(task: Mapping[str, Any]) -> None:
    fields = _normalized_task_fields(task)
    if fields.get("id") in (None, ""):
        raise RuntimeError("LitePan task inventory entry lacks an explicit ID")
    active = fields.get("active")
    if active in (None, ""):
        raise RuntimeError("LitePan task inventory entry lacks explicit active state")
    if not isinstance(active, bool):
        raise RuntimeError("LitePan task inventory entry has malformed active state")
    name = fields.get("name")
    source = fields.get("source_root")
    output = fields.get("output_root")
    for field, item in (("name", name), ("source_root", source), ("output_root", output)):
        if not isinstance(item, str) or not item.strip():
            raise RuntimeError(f"LitePan task inventory entry lacks {field}")
    for root in (source, output):
        if (
            not root.startswith("/")
            or root.startswith("//")
            or "\\" in root
            or "://" in root
            or "?" in root
            or "#" in root
            or any(part in {".", ".."} for part in root.split("/"))
        ):
            raise RuntimeError("LitePan task inventory entry contains unsafe root")


def _task_identity(task: Any, secrets: Iterable[Any]) -> dict[str, Any]:
    fields = _normalized_task_fields(task)
    task_id = fields.get("id")
    name = fields.get("name")
    source = fields.get("source_root")
    output = fields.get("output_root")
    return {
        "task_id": sanitize_text(task_id, secrets=secrets),
        "litepan_task_id": sanitize_text(task_id, secrets=secrets),
        "task_name": sanitize_text(name, secrets=secrets),
        "source_root": sanitize_text(source, secrets=secrets),
        "output_root": sanitize_text(output, secrets=secrets),
    }


def _accepted(response: Any) -> bool:
    if isinstance(response, Mapping):
        status = response.get("status", response.get("code"))
        if status is not None:
            try:
                return int(status) in {200, 201, 202, 204}
            except (TypeError, ValueError):
                return str(status).casefold() in {"ok", "success", "accepted"}
        return response.get("accepted") is True or response.get("success") is True
    if isinstance(response, bool):
        return response
    return False


def _scrape_ok(result: Any) -> bool:
    if isinstance(result, Mapping):
        return result.get("ok") is True or result.get("success") is True
    return bool(getattr(result, "ok", False))


def _positive_delay(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be positive") from exc
    if number <= 0:
        raise ValueError(f"{name} must be positive")
    return number


def _nonnegative_delay(value: Any, name: str) -> float:
    try:
        number = float(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{name} must be non-negative") from exc
    if number < 0:
        raise ValueError(f"{name} must be non-negative")
    return number


__all__ = [
    "DownloadService",
    "MetadataService",
    "ReviewService",
    "ServiceResult",
    "SidecarEvidence",
    "StrmService",
    "LibraryService",
]
