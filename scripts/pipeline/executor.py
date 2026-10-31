"""单次执行器：一轮完成去重、提交、STRM、刮削、刷库、通知的推进。

约束：
- 每轮有界（max_new_tasks_per_run），下载未完成留到下轮。
- 提交前先持久化 submitting 状态；超时后对账远端任务，不盲目重交。
- 上游未完成绝不触发下游（LitePan 未完成不刮削，MP 失败不刷 Emby）。
- 日志不包含凭据；标题/链接只当数据处理，绝不拼接进 shell。
"""
from __future__ import annotations

import logging
import math
import os
import random
import tempfile
import datetime as dt
import time
from collections.abc import Mapping
from typing import Any, Callable, Dict, List, Optional

from .adapters import (Cloud115Adapter, EmbyAdapter, LitePanAdapter,
                       MockLitePan, MoviePilotAdapter, NotifyAdapter,
                       expected_strm_exists, has_video_file, parse_episode_nfo)
from .candidates import SubmissionBudget
from .models import EpisodeStatus, ReviewReason
from .parsers import (FeedFormatError, SourceItem, SubRule, match_rule,
                      parse_feed, published_on)
from .paths import SeasonRoutingError, season_label_from_air_date
from .services import (DownloadService, LibraryService, normalize_backfill_scope,
                       episode_in_backfill_scope)
from .http import retry_delay
from .state import LockHeld, ProcessLock, StateStore

log = logging.getLogger("pipeline")


class RedactingFilter(logging.Filter):
    """日志脱敏：凭据形 secret 一律遮蔽（含 %-format 参数）。"""

    def __init__(self, secrets: Optional[List[str]] = None):
        super().__init__()
        self.secrets = [s for s in (secrets or []) if s]

    def filter(self, record: logging.LogRecord) -> bool:
        if not self.secrets:
            return True
        try:
            message = record.getMessage()
        except Exception:
            return True
        redacted = message
        for s in self.secrets:
            if s in redacted:
                redacted = redacted.replace(s, "<redacted>")
        if redacted != message:
            record.msg = redacted
            record.args = None
        return True


class RunSummary(dict):
    """Mapping-compatible summary for one bounded V5 runner invocation."""

    _DEFAULTS = {
        "discovered": 0,
        "advanced": 0,
        "submitted": 0,
        "new_submissions": 0,
        "reconciled": 0,
        "polled_not_ready": 0,
        "cloud_checks": 0,
        "cloud_check_deferred": 0,
        "skipped": 0,
        "skipped_review": 0,
        "skipped_terminal": 0,
        "skipped_retry": 0,
        "skipped_budget": 0,
        "skipped_selection": 0,
        "skipped_read_only": 0,
        "skipped_state_changed": 0,
        "skipped_pacing": 0,
        "skipped_quarter": 0,
        "errors": 0,
        "skipped_lock": False,
        "stage_counts": {},
        "handler_counts": {},
        "discovery": {},
    }

    def __init__(self, **values: Any) -> None:
        payload = {
            key: value.copy() if isinstance(value, dict) else value
            for key, value in self._DEFAULTS.items()
        }
        payload.update(values)
        super().__init__(payload)

    def __getattr__(self, name: str) -> Any:
        try:
            return self[name]
        except KeyError as exc:
            raise AttributeError(name) from exc

    def add_stage(self, status: str, count: int = 1) -> None:
        stages = self["stage_counts"]
        stages[status] = int(stages.get(status, 0)) + count

    def add_handler(self, handler: str, count: int = 1) -> None:
        handlers = self["handler_counts"]
        handlers[handler] = int(handlers.get(handler, 0)) + count


class PipelineRunner:
    """Bounded V5 state dispatcher.

    Stable states only persist the next pending intent.  A pending handler is
    then allowed one service invocation in the next runner turn.  This keeps
    external writes bounded and makes every failure independently recoverable.
    """

    _PENDING = {
        EpisodeStatus.DOWNLOAD_PENDING.value,
        EpisodeStatus.ORGANIZE_PENDING.value,
        EpisodeStatus.STRM_PENDING.value,
        EpisodeStatus.SCRAPE_PENDING.value,
        EpisodeStatus.LIBRARY_PENDING.value,
    }
    _TERMINAL = {
        EpisodeStatus.NEEDS_REVIEW.value,
        EpisodeStatus.LIBRARY_READY.value,
        EpisodeStatus.NOTIFIED.value,
        "failed_permanent",
    }
    _STABLE_TO_PENDING = {
        EpisodeStatus.DISCOVERED.value: EpisodeStatus.DOWNLOAD_PENDING.value,
        EpisodeStatus.CLOUD_READY.value: EpisodeStatus.ORGANIZE_PENDING.value,
        EpisodeStatus.ORGANIZED.value: EpisodeStatus.STRM_PENDING.value,
        EpisodeStatus.STRM_READY.value: EpisodeStatus.SCRAPE_PENDING.value,
        EpisodeStatus.METADATA_READY.value: EpisodeStatus.LIBRARY_PENDING.value,
    }

    def __init__(
        self,
        store: StateStore,
        *args: Any,
        services: Any = None,
        stage_services: Any = None,
        discovery: Any = None,
        discovery_service: Any = None,
        cloud115: Any = None,
        cloud: Any = None,
        download: Any = None,
        download_service: Any = None,
        organizer: Any = None,
        strm_service: Any = None,
        metadata_service: Any = None,
        library_service: Any = None,
        emby: Any = None,
        mapper: Any = None,
        library_name: str | None = None,
        library_root: str | None = None,
        runtime_config: Any = None,
        budget: SubmissionBudget | None = None,
        submission_budget: SubmissionBudget | None = None,
        config: Any = None,
        lock_path: str | None = None,
        lock: Any = None,
        secrets: Any = (),
        now: Callable[[], float] | None = None,
        candidate_selector: Any = None,
        video_selector: Any = None,
        retry_random: Callable[[], float] | None = None,
    ) -> None:
        # Accept the compact positional form used by early V5 orchestration
        # tests: PipelineRunner(store, {"organize": ..., ...}).
        if args:
            if services is None and stage_services is None:
                services = args[0]
            if len(args) > 1 and budget is None and submission_budget is None:
                budget = args[1]
        self.store = store
        self.config = config
        self.secrets = tuple(secrets or ())
        self.now = now or time.time
        self.retry_random = retry_random
        self.retry_policy = _runner_retry_settings(config, retry_random)
        self.discovery = discovery or discovery_service
        self.cloud115 = cloud115 or cloud
        self.candidate_selector = candidate_selector
        self.video_selector = video_selector
        self.lock = lock
        if self.lock is None and lock_path:
            self.lock = ProcessLock(lock_path)

        provided = stage_services if stage_services is not None else services
        if self.discovery is None:
            if isinstance(provided, Mapping):
                self.discovery = provided.get("discovery")
            elif provided is not None:
                self.discovery = getattr(provided, "discovery", None)
        self.stage_services: dict[str, Any] = {}
        if download is not None or download_service is not None:
            self.stage_services["download"] = download or download_service
        if isinstance(provided, Mapping):
            self.stage_services.update({str(key): value for key, value in provided.items()})
        elif provided is not None:
            self.stage_services["services"] = provided
            for name in (
                "download", "organize", "organizer", "strm", "strm_service",
                "scrape", "metadata", "metadata_service", "library", "library_service",
                "candidate_selector", "selector",
            ):
                value = getattr(provided, name, None)
                if value is not None:
                    self.stage_services[name] = value

        # Explicit keyword ports win over a mapping/object service bundle.
        self.stage_services.update({
            key: value for key, value in {
                "organize": organizer,
                "strm": strm_service,
                "scrape": metadata_service,
                "library": library_service,
            }.items() if value is not None
        })
        if self.stage_services.get("library") is None and emby is not None:
            # The Task 9 LibraryService is the only library path used by V5;
            # this prevents a runner caller from accidentally reintroducing
            # the legacy refresh-first flow.
            self.stage_services["library"] = LibraryService(
                self.store,
                emby,
                mapper,
                library_name=library_name,
                library_root=library_root,
                runtime_config=runtime_config,
                retry_policy=self.retry_policy,
                retry_random=self.retry_random,
                secrets=self.secrets,
            )
        if self.candidate_selector is None:
            self.candidate_selector = (
                self.stage_services.get("candidate_selector")
                or self.stage_services.get("selector")
            )
        self.stage_services.update({
            key: value for key, value in {
                "organize": self.stage_services.get("organize")
                or self.stage_services.get("organizer"),
                "strm": self.stage_services.get("strm")
                or self.stage_services.get("strm_service"),
                "scrape": self.stage_services.get("scrape")
                or self.stage_services.get("metadata")
                or self.stage_services.get("metadata_service"),
                "library": self.stage_services.get("library")
                or self.stage_services.get("library_service"),
            }.items() if value is not None
        })
        # ``run_pipeline.build_runtime`` deliberately constructs the complete
        # service graph before opening SQLite so missing capabilities fail
        # closed without a state side effect.  Bind those concrete services to
        # the command's already-open store here; injected test services that
        # already own a store are left untouched.
        for service in self.stage_services.values():
            if service is None or not hasattr(service, "store"):
                continue
            if getattr(service, "store", object()) is None:
                try:
                    service.store = self.store
                except (AttributeError, TypeError):
                    pass
        self.budget = budget or submission_budget or self._budget_from_config(config)
        if self.cloud115 is not None and self.stage_services.get("download") is None:
            self.stage_services["download"] = DownloadService(
                self.store,
                self.cloud115,
                budget=self.budget,
                target_directory=str(self._config_value(
                    "target_directory_id", "target_directory", default="target"
                )),
                secrets=self.secrets,
                now=self.now,
                retry_policy=self.retry_policy,
                retry_random=self.retry_random,
                video_selector=self.video_selector or self.stage_services.get("video_selector"),
            )
        self.handlers: dict[str, Callable[[Mapping[str, Any], RunSummary], Any]] = {
            EpisodeStatus.DISCOVERED.value: self._handle_discovered,
            EpisodeStatus.DOWNLOAD_PENDING.value: self._handle_download_pending,
            EpisodeStatus.SUBMITTED.value: self._handle_submitted,
            "submitting": self._handle_download_pending,
            "uncertain": self._handle_download_pending,
            "downloading": self._handle_submitted,
            EpisodeStatus.CLOUD_READY.value: self._handle_cloud_ready,
            EpisodeStatus.ORGANIZE_PENDING.value: self._handle_organize_pending,
            EpisodeStatus.ORGANIZED.value: self._handle_organized,
            EpisodeStatus.STRM_PENDING.value: self._handle_strm_pending,
            EpisodeStatus.STRM_READY.value: self._handle_strm_ready,
            EpisodeStatus.SCRAPE_PENDING.value: self._handle_scrape_pending,
            "scraping": self._handle_scrape_pending,
            EpisodeStatus.METADATA_READY.value: self._handle_metadata_ready,
            EpisodeStatus.LIBRARY_PENDING.value: self._handle_library_pending,
        }

    @staticmethod
    def _budget_from_config(config: Any) -> SubmissionBudget:
        limits = getattr(config, "limits", None)
        if limits is None and isinstance(config, Mapping):
            limits = config.get("limits") or config.get("submission_limits")
        if limits is None:
            # A runner without validated configuration remains bounded by the
            # caller's episode snapshot; no implicit low limit should prevent
            # reconciliation in local fakes.
            return SubmissionBudget(10**9, 10**9)
        def read(*names: str, default: int = 10**9) -> int:
            for name in names:
                value = limits.get(name) if isinstance(limits, Mapping) else getattr(limits, name, None)
                if value is not None:
                    try:
                        return int(value)
                    except (TypeError, ValueError):
                        break
            return default
        return SubmissionBudget(
            read("max_new_tasks_per_run", "global_submission_limit"),
            read("max_new_tasks_per_subscription", "per_subscription_submission_limit"),
        )

    def run(
        self,
        discover: bool = False,
        read_only: bool = False,
        *,
        window: Any = None,
    ) -> RunSummary:
        """Run one bounded dispatcher turn.

        Discovery receives the same read-only flag.  Existing Episode rows are
        snapshotted once; a row is handled at most once in that snapshot, so a
        successful service transition never cascades into a second external
        stage during this invocation.
        """

        if not read_only and (
            not isinstance(window, Mapping) or not window.get("season_quarter")
        ):
            raise ValueError("writable runner requires a current-quarter window")

        backfill_scope = normalize_backfill_scope(window)
        download = self.stage_services.get("download")
        scope_setter = getattr(download, "set_scope", None)
        if callable(scope_setter):
            scope_setter(window)
        elif backfill_scope is not None and download is not None:
            raise ValueError("download_service_scope_unsupported")

        summary = RunSummary()
        acquired = False
        # Read-only inspection must not create or mutate a process-lock file;
        # it has no external writes to serialize.
        if self.lock is not None and not read_only:
            acquired = bool(self.lock.acquire())
            if not acquired:
                summary["skipped_lock"] = True
                return summary
        try:
            if discover and self.discovery is not None:
                plan = self.discovery.prepare(window, read_only=read_only)
                summary["discovery"] = dict(getattr(plan, "summary", {}) or {})
                summary["discovered"] = int(summary["discovery"].get("decision_count", 0))
                if summary["discovery"].get("error_count"):
                    summary["errors"] += int(summary["discovery"]["error_count"])

            episodes = list(self.store.all_episodes())
            processed_ids: set[int] = set()
            for episode in episodes:
                snapshot = dict(episode)
                if not episode_in_backfill_scope(snapshot, backfill_scope):
                    summary["skipped"] += 1
                    continue
                try:
                    episode_id = int(snapshot["id"])
                except (KeyError, TypeError, ValueError):
                    summary["skipped"] += 1
                    continue
                if episode_id in processed_ids:
                    continue
                processed_ids.add(episode_id)
                getter = getattr(self.store, "get_episode", None)
                current = getter(episode_id) if callable(getter) else snapshot
                if current is None:
                    summary["skipped"] += 1
                    continue
                if not episode_in_backfill_scope(dict(current), backfill_scope):
                    summary["skipped"] += 1
                    continue
                compatibility_getter = getattr(self.store, "get_by_id", None)
                if callable(compatibility_getter):
                    try:
                        compatible = compatibility_getter(episode_id)
                    except (KeyError, TypeError, ValueError):
                        compatible = None
                    if compatible is not None:
                        current = compatible
                row = dict(current)
                if not episode_in_backfill_scope(row, backfill_scope):
                    summary["skipped"] += 1
                    continue
                quarter = window.get("season_quarter") if isinstance(window, Mapping) else None
                if quarter:
                    date_reader = getattr(self.store, "get_subscription_season_air_date", None)
                    season_date = date_reader(episode_id) if callable(date_reader) else ""
                    try:
                        label = season_label_from_air_date(season_date)
                    except SeasonRoutingError:
                        label = ""
                    if not label.startswith(str(quarter) + "月"):
                        summary["skipped"] += 1
                        summary["skipped_quarter"] += 1
                        continue
                snapshot_status = _status_text(
                    snapshot.get("status") or snapshot.get("stage")
                )
                status = _status_text(row.get("status") or row.get("stage"))
                # A shared remote task can promote a later snapshot row while
                # an earlier row is being polled.  Do not call that row's
                # handler again in this bounded turn.
                if status != snapshot_status:
                    summary["skipped"] += 1
                    summary["skipped_state_changed"] += 1
                    continue
                if status in self._TERMINAL:
                    summary["skipped"] += 1
                    if status == EpisodeStatus.NEEDS_REVIEW.value:
                        summary["skipped_review"] += 1
                    else:
                        summary["skipped_terminal"] += 1
                    continue
                if _retry_not_due(row, self.now()):
                    summary["skipped"] += 1
                    summary["skipped_retry"] += 1
                    continue
                handler = self.handlers.get(status)
                if handler is None:
                    summary["skipped"] += 1
                    continue
                if read_only:
                    summary["skipped"] += 1
                    summary["skipped_read_only"] += 1
                    summary.add_handler(status + ":read_only")
                    continue
                try:
                    result = handler(row, summary)
                    reason = _result_reason(result)
                    if reason == "submission_budget_exhausted":
                        summary["skipped"] += 1
                        summary["skipped_budget"] += 1
                    elif reason == "selection_required":
                        summary["skipped"] += 1
                        summary["skipped_selection"] += 1
                    elif reason in {
                        "cloud_check_budget_exhausted",
                        "cloud_check_cooldown",
                        "cloud_check_deferred",
                        "retry_not_due",
                        "pre_submit_inventory_present",
                        "pre_submit_inventory_unknown",
                        "pre_submit_inventory_error",
                    }:
                        # No external stage advanced; reconciliation or
                        # inventory reads must not inflate the counter.
                        summary["skipped"] += 1
                        summary["skipped_pacing"] += 1
                    else:
                        summary["advanced"] += 1
                    summary.add_handler(status)
                    latest = self.store.get_episode(int(row["id"])) or {}
                    summary.add_stage(_status_text(latest.get("status") or status))
                    if reason in {"pre_submit_inventory_present",
                                  "pre_submit_inventory_unknown",
                                  "pre_submit_inventory_error"}:
                        summary["stop_reason"] = reason
                        break
                except Exception as exc:
                    summary["errors"] += 1
                    try:
                        self._persist_handler_error(row, status, exc)
                    except Exception as persist_exc:
                        # A malformed/closed store must not abort the rest of
                        # this bounded snapshot; the original stage exception
                        # remains the primary evidence in the summary.
                        log.error(
                            "failed to persist retry evidence id=%s stage=%s: %s",
                            row.get("id"), status, _safe_runner_error(persist_exc),
                        )
                    summary.add_handler(status + ":error")
        finally:
            if acquired:
                self.lock.release()
        return summary

    # ---------- stable state boundaries ----------
    def _handle_discovered(self, row: Mapping[str, Any], summary: RunSummary) -> Any:
        selector = self.candidate_selector
        if selector is None:
            if not _has_selected_candidate(self.store, int(row["id"])):
                return {"status": EpisodeStatus.DISCOVERED.value,
                        "detail": {"reason": "selection_required"}}
        else:
            method = getattr(selector, "select", None) or getattr(selector, "select_episode", None)
            if not callable(method):
                raise RuntimeError("candidate selector has no select method")
            result = method(int(row["id"]))
            review_reason = getattr(result, "review_reason", None)
            candidate_id = getattr(
                result,
                "candidate_id",
                getattr(result, "selected_candidate_id", None),
            )
            if isinstance(result, Mapping):
                review_reason = result.get("review_reason") or review_reason
                candidate_id = result.get("candidate_id", result.get("selected_candidate_id", candidate_id))
            elif type(result) is int:
                candidate_id = result
            if (
                review_reason is not None
                or not _select_exact_candidate(
                    self.store, int(row["id"]), candidate_id,
                )
            ):
                return {"status": EpisodeStatus.DISCOVERED.value,
                        "detail": {"reason": "selection_required"}}
        _runner_transition(
            self.store,
            int(row["id"]),
            EpisodeStatus.DOWNLOAD_PENDING.value,
            {"reason": "download_intent"},
            secrets=self.secrets,
        )

    def _handle_cloud_ready(self, row: Mapping[str, Any], summary: RunSummary) -> Any:
        # Organizer owns remote mkdir/move/rename reconciliation.  A missing
        # organizer is treated as a durable intent, not as an inline write.
        organizer = self._service("organize")
        if organizer is None:
            _runner_transition(
                self.store, int(row["id"]), EpisodeStatus.ORGANIZE_PENDING.value,
                {"reason": "organize_intent"}, secrets=self.secrets,
            )
            return
        return self._invoke_service(organizer, row, ("reconcile_or_apply", "advance"), summary)

    def _handle_organized(self, row: Mapping[str, Any], summary: RunSummary) -> None:
        _runner_transition(
            self.store, int(row["id"]), EpisodeStatus.STRM_PENDING.value,
            {"reason": "strm_intent"}, secrets=self.secrets,
        )

    def _handle_strm_ready(self, row: Mapping[str, Any], summary: RunSummary) -> None:
        _runner_transition(
            self.store, int(row["id"]), EpisodeStatus.SCRAPE_PENDING.value,
            {"reason": "scrape_intent"}, secrets=self.secrets,
        )

    def _handle_metadata_ready(self, row: Mapping[str, Any], summary: RunSummary) -> None:
        _runner_transition(
            self.store, int(row["id"]), EpisodeStatus.LIBRARY_PENDING.value,
            {"reason": "library_intent"}, secrets=self.secrets,
        )

    # ---------- external stage handlers ----------
    def _handle_download_pending(self, row: Mapping[str, Any], summary: RunSummary) -> Any:
        service = self._service("download")
        if service is None:
            raise RuntimeError("download service unavailable")
        return self._invoke_service(service, row, ("advance", "reconcile_or_apply", "submit"), summary)

    def _handle_submitted(self, row: Mapping[str, Any], summary: RunSummary) -> Any:
        service = self._service("download")
        if service is None or not callable(getattr(service, "poll", None)):
            raise RuntimeError("download service lacks bounded poll method")
        return self._invoke_service(service, row, ("poll",), summary)

    def _handle_organize_pending(self, row: Mapping[str, Any], summary: RunSummary) -> Any:
        service = self._service("organize")
        if service is None:
            raise RuntimeError("organize service unavailable")
        return self._invoke_service(service, row, ("reconcile_or_apply", "advance"), summary)

    def _handle_strm_pending(self, row: Mapping[str, Any], summary: RunSummary) -> Any:
        service = self._service("strm")
        if service is None:
            raise RuntimeError("strm service unavailable")
        return self._invoke_service(service, row, ("advance",), summary)

    def _handle_scrape_pending(self, row: Mapping[str, Any], summary: RunSummary) -> Any:
        service = self._service("scrape")
        if service is None:
            raise RuntimeError("scrape service unavailable")
        return self._invoke_service(service, row, ("advance",), summary)

    def _handle_library_pending(self, row: Mapping[str, Any], summary: RunSummary) -> Any:
        service = self._service("library")
        if service is None:
            raise RuntimeError("library service unavailable")
        return self._invoke_service(service, row, ("advance",), summary)

    def _service(self, name: str) -> Any:
        service = self.stage_services.get(name)
        if service is not None:
            return service
        bundle = self.stage_services.get("services")
        if bundle is not None:
            return getattr(bundle, name, None)
        return None

    def _invoke_service(
        self,
        service: Any,
        row: Mapping[str, Any],
        names: tuple[str, ...],
        summary: RunSummary,
    ) -> Any:
        method = next((getattr(service, name, None) for name in names if callable(getattr(service, name, None))), None)
        if method is None and callable(service):
            method = service
        if method is None:
            raise RuntimeError("stage service has no bounded advance method")
        result = method(row)
        status = _result_status(result)
        attempts = getattr(service, "cloud_checks_used", None)
        if attempts is not None:
            # This counter only covers download-stage budget-gate pass-throughs;
            # 115 calls made by the Organizer and startup validation are excluded.
            summary["cloud_checks"] = attempts
        if status == EpisodeStatus.SUBMITTED.value:
            reason = str(_result_detail(result).get("reason") or "")
            if reason == "download_reconciled":
                summary["reconciled"] += 1
            elif reason == "download_not_ready":
                # The 115 task exists but the download is still running: this
                # is a wait, not a submission, and must not inflate the
                # submitted counter.
                summary["polled_not_ready"] += 1
            elif reason.startswith("cloud_check_"):
                summary["cloud_check_deferred"] += 1
            elif reason == "download_submitted":
                summary["submitted"] += 1
                summary["new_submissions"] += 1
            # Any other reason (manifest failure, selection failure, unknown)
            # must not count toward any submission counter.
        latest = self.store.get_episode(int(row["id"])) or {}
        latest_status = _status_text(latest.get("status") or latest.get("stage"))
        if status and status != latest_status:
            # A service is allowed to persist a durable review/terminal result
            # while it is handling the external call.  Never let a stale
            # handler result move that row back to a pending stage.  For a
            # still-current pending row, promote through the repository CAS
            # port so a concurrent worker cannot win and then be overwritten.
            if latest_status not in self._TERMINAL:
                _runner_transition_if_current(
                    self.store,
                    int(row["id"]),
                    _status_text(row.get("status") or row.get("stage")),
                    status,
                    {"reason": "stage_service_result"},
                    secrets=self.secrets,
                )
        elif (
            status in self._PENDING
            and _runner_result_needs_retry(result)
            and _retry_count(latest) <= _retry_count(row)
        ):
            # Organizer returns a pending result after a failed remote read
            # without performing a state transition itself.  Apply the same
            # configured bounded retry policy used by handler exceptions so a
            # pending stage can never spin without durable backoff evidence.
            _runner_retry(
                self.store,
                int(row["id"]),
                row,
                _result_detail(result),
                self.secrets,
                target=status,
                now=self.now,
                retry_policy=self.retry_policy,
            )
        return result

    def _persist_handler_error(self, row: Mapping[str, Any], status: str, exc: BaseException) -> None:
        target = status if status == EpisodeStatus.DISCOVERED.value else self._STABLE_TO_PENDING.get(status, status)
        detail = {
            "reason": "handler_exception",
            "stage": status,
            "error": _safe_runner_error(exc),
        }
        _runner_retry(
            self.store, int(row["id"]), row, detail, self.secrets,
            target=target, now=self.now, retry_policy=self.retry_policy,
        )

    def _config_value(self, name: str, *aliases: str, default: Any = None) -> Any:
        config = self.config
        if isinstance(config, Mapping):
            for key in (name, *aliases):
                if config.get(key) is not None:
                    return config[key]
            return default
        for key in (name, *aliases):
            value = getattr(config, key, None) if config is not None else None
            if value is not None:
                return value
        return default


def _status_text(value: Any) -> str:
    return str(getattr(value, "value", value) or "").strip()


def _result_status(result: Any) -> str | None:
    if isinstance(result, Mapping):
        value = result.get("status") or result.get("stage")
    else:
        value = getattr(result, "status", None) or getattr(result, "stage", None)
    if value in (None, ""):
        return None
    return _status_text(value)


def _result_detail(result: Any) -> Mapping[str, Any]:
    if isinstance(result, Mapping):
        detail = result.get("detail")
    else:
        detail = getattr(result, "detail", None)
    return detail if isinstance(detail, Mapping) else {}


def _result_reason(result: Any) -> str:
    return str(_result_detail(result).get("reason") or "")


def _retry_count(row: Mapping[str, Any]) -> int:
    try:
        return max(0, int(row.get("retry_count") or 0))
    except (TypeError, ValueError):
        return 0


def _runner_result_needs_retry(result: Any) -> bool:
    """Return whether a pending service result represents retryable work."""

    return _result_reason(result) not in {
        "retry_not_due",
        "submission_budget_exhausted",
        "cloud_check_budget_exhausted",
        "cloud_check_cooldown",
        "cloud_check_deferred",
        # The Organizer rate-limit branch already persisted next_retry_at in
        # the service; a runner retry must not overwrite that durable backoff.
        "organize_rate_limited",
        "download_already_reconciled",
        "already_ready",
        "refresh_claim_not_won",
    }


def _has_selected_candidate(store: Any, episode_id: int) -> bool:
    getter = getattr(store, "selected_candidates", None)
    if callable(getter):
        return len(getter(episode_id) or []) == 1
    candidates = getattr(store, "get_candidates", lambda _id: [])(episode_id) or []
    return sum(
        1 for item in candidates
        if isinstance(item, Mapping) and bool(item.get("selected"))
    ) == 1


def _candidate_selection_result_is_valid(
    store: Any,
    episode_id: int,
    candidate_id: Any,
) -> bool:
    if type(candidate_id) is not int or candidate_id <= 0:
        return False
    getter = getattr(store, "get_candidates", None)
    if not callable(getter):
        return True
    rows = getter(episode_id) or []
    # CandidateSelector's result is authoritative for persistence, but a
    # repository that exposes candidates can still reject a foreign/stale ID
    # before the discovered episode is promoted.
    return any(
        isinstance(item, Mapping)
        and type(item.get("id")) is int
        and int(item["id"]) == candidate_id
        for item in rows
    )


def _select_exact_candidate(store: Any, episode_id: int, candidate_id: Any) -> bool:
    """Make selector output durable, then prove exactly one selected row."""

    if not _candidate_selection_result_is_valid(store, episode_id, candidate_id):
        return False
    candidate_id = int(candidate_id)
    selected_getter = getattr(store, "selected_candidates", None)
    selected = list(selected_getter(episode_id) or []) if callable(selected_getter) else []
    if selected != [candidate_id]:
        selector = getattr(store, "select_candidate", None)
        if not callable(selector):
            return False
        selector(candidate_id)
    selected = list(selected_getter(episode_id) or []) if callable(selected_getter) else []
    if selected == [candidate_id]:
        return True
    rows = getattr(store, "get_candidates", lambda _id: [])(episode_id) or []
    return [
        int(item["id"])
        for item in rows
        if isinstance(item, Mapping) and bool(item.get("selected"))
    ] == [candidate_id]


def _retry_not_due(row: Mapping[str, Any], now: float) -> bool:
    value = row.get("next_retry_at")
    try:
        return value not in (None, "") and float(value) > float(now)
    except (TypeError, ValueError):
        return False


def _safe_runner_error(exc: BaseException) -> str:
    status = getattr(exc, "status", None)
    if status is not None:
        try:
            return f"http_{int(status)}"
        except (TypeError, ValueError):
            return "http_error"
    return type(exc).__name__


def _runner_transition(
    store: Any,
    episode_id: int,
    status: str | EpisodeStatus,
    detail: Mapping[str, Any] | str,
    *,
    extra: Mapping[str, Any] | None = None,
    secrets: Any = (),
) -> None:
    method = getattr(store, "transition", None) or getattr(store, "set_stage", None)
    if not callable(method):
        raise TypeError("state store has no transition method")
    try:
        method(episode_id, status, detail, extra=dict(extra or {}), secrets=tuple(secrets or ()))
    except TypeError as exc:
        # Earlier fakes implement only the positional state port.  The real
        # StateStore path above remains the canonical atomic implementation.
        if "extra" not in str(exc) and "secrets" not in str(exc):
            raise
        try:
            method(episode_id, status, detail, extra=dict(extra or {}))
        except TypeError as fallback_exc:
            if "extra" not in str(fallback_exc) and "secrets" not in str(fallback_exc):
                raise
            method(episode_id, status, detail)


def _runner_transition_if_current(
    store: Any,
    episode_id: int,
    expected_status: str,
    status: str,
    detail: Mapping[str, Any] | str,
    *,
    extra: Mapping[str, Any] | None = None,
    secrets: Any = (),
) -> bool:
    """CAS a service result without regressing a concurrent terminal row."""

    method = getattr(store, "transition_if_current", None)
    if callable(method):
        return bool(method(
            episode_id,
            expected_status,
            status,
            detail,
            extra=dict(extra or {}),
            secrets=tuple(secrets or ()),
        ))
    getter = getattr(store, "get_episode", None)
    current = getter(episode_id) if callable(getter) else None
    if current is None or _status_text(current.get("status") or current.get("stage")) != expected_status:
        return False
    _runner_transition(
        store, episode_id, status, detail, extra=extra, secrets=secrets,
    )
    return True


def _runner_retry_settings(config: Any, random_source: Callable[[], float] | None) -> dict[str, Any]:
    retry = getattr(config, "retry", None) if config is not None else None
    if retry is None and isinstance(config, Mapping):
        retry = config.get("retry")
    if retry is None:
        return {
            "max_attempts": None,
            "base": 30.0,
            "cap": 3600.0,
            "jitter": 0.0,
            "random": random_source or random.random,
            "backoff_seconds": 30.0,
            "backoff_max_seconds": 3600.0,
            "jitter_seconds": 0.0,
            "random_source": random_source or random.random,
        }

    def read(name: str, default: Any) -> Any:
        if isinstance(retry, Mapping):
            return retry.get(name, default)
        return getattr(retry, name, default)

    max_attempts = read("max_attempts", None)
    if max_attempts is not None:
        max_attempts = int(max_attempts)
    base = float(read("backoff_seconds", 30.0))
    cap = max(base, float(read("backoff_max_seconds", 3600.0)))
    jitter = max(0.0, float(read("jitter_seconds", 0.0)))
    return {
        "max_attempts": max_attempts,
        "base": max(0.0, base),
        "cap": cap,
        "jitter": jitter,
        "random": random_source or random.random,
        "backoff_seconds": max(0.0, base),
        "backoff_max_seconds": cap,
        "jitter_seconds": jitter,
        "random_source": random_source or random.random,
    }


def _runner_open_review(
    store: Any,
    episode_id: int,
    detail: Mapping[str, Any],
    *,
    target: str | EpisodeStatus | None,
    retry_count: int,
    secrets: Any,
) -> None:
    from .models import ReviewReason

    method = getattr(store, "open_review", None)
    extra = {
        "retry_count": retry_count,
        "next_retry_at": None,
        "last_error": str(detail.get("reason") or "retry_exhausted"),
    }
    if callable(method):
        try:
            method(
                episode_id,
                ReviewReason.UNEXPECTED_REMOTE_STATE,
                detail,
                resume_status=target,
                extra=extra,
                secrets=tuple(secrets or ()),
            )
            return
        except TypeError:
            try:
                method(
                    episode_id,
                    ReviewReason.UNEXPECTED_REMOTE_STATE,
                    detail,
                    resume_status=target,
                    secrets=tuple(secrets or ()),
                )
                return
            except TypeError:
                method(episode_id, ReviewReason.UNEXPECTED_REMOTE_STATE, detail)
                return
    _runner_transition(
        store,
        episode_id,
        EpisodeStatus.NEEDS_REVIEW.value,
        detail,
        extra=extra,
        secrets=secrets,
    )


def _runner_retry(
    store: Any,
    episode_id: int,
    row: Mapping[str, Any],
    detail: Mapping[str, Any] | str,
    secrets: Any = (),
    *,
    target: str | EpisodeStatus | None = None,
    now: Callable[[], float] = time.time,
    retry_policy: Mapping[str, Any] | None = None,
) -> None:
    try:
        previous = max(0, int(row.get("retry_count") or 0))
    except (TypeError, ValueError):
        previous = 0
    count = previous + 1
    policy = retry_policy or {
        "max_attempts": None,
        "base": 30.0,
        "cap": 3600.0,
        "jitter": 0.0,
        "random": random.random,
    }
    delay = retry_delay(policy, count, policy.get("random"))
    if isinstance(detail, Mapping):
        safe_detail: Mapping[str, Any] = dict(detail)
        error = safe_detail.get("error")
    else:
        safe_detail = {"reason": str(detail)}
        error = None
    reason = str(safe_detail.get("reason") or "retryable_error")
    if error:
        reason = f"{reason}: {_safe_runner_error(RuntimeError(str(error)))}"
    from .security import sanitize_text
    reason = sanitize_text(reason, secrets=secrets)
    if policy.get("max_attempts") is not None and count >= int(policy["max_attempts"]):
        exhausted = dict(safe_detail)
        exhausted.update({
            "reason": "retry_exhausted",
            "attempts": count,
            "last_reason": safe_detail.get("reason") or "retryable_error",
        })
        _runner_open_review(
            store,
            episode_id,
            exhausted,
            target=target,
            retry_count=count,
            secrets=secrets,
        )
        return
    _runner_transition(
        store,
        episode_id,
        target or _status_text(row.get("status") or row.get("stage")),
        safe_detail,
        extra={
            "retry_count": count,
            "next_retry_at": float(now()) + delay,
            "last_error": reason,
        },
        secrets=secrets,
    )


class Executor:
    def __init__(self, store: StateStore,
                 cloud115: Cloud115Adapter,
                 litepan: LitePanAdapter,
                 moviepilot: MoviePilotAdapter,
                 emby: EmbyAdapter,
                 notify: NotifyAdapter,
                 fetch_feed: Callable[[str], str],
                 lock_path: str,
                 config: Optional[Dict[str, Any]] = None):
        self.store = store
        self.cloud115 = cloud115
        self.litepan = litepan
        self.moviepilot = moviepilot
        self.emby = emby
        self.notify = notify
        self.fetch_feed = fetch_feed
        self.lock = ProcessLock(lock_path)
        cfg = config or {}
        self.max_new_tasks = int(cfg.get("max_new_tasks_per_run", 3))
        self.strm_root = cfg.get("strm_root") or os.path.join(tempfile.gettempdir(), "pipeline-strm")
        self.library_id = cfg.get("library_id", "anime")
        self.target_dir = cfg.get("target_directory_id", "target")
        self.media_source = cfg.get("media_source", "tmdb")
        self.media_id = cfg.get("media_id", "")
        # STRM 证据模式：False=本地文件证据(执行器与 LitePan 同机)；True=经 MP storage API 证据
        self.evidence_via_mp = bool(cfg.get("evidence_via_moviepilot"))
        self.mp_strm_root = (cfg.get("moviepilot_strm_root") or "").rstrip("/")
        # 115 目标目录在 /影视库 下的相对路径:STRM 由 LitePan 按 115 结构镜像生成,
        # 预期 STRM 相对路径 = target_strm_prefix + "/<任务文件夹>/<文件>.strm"
        self.target_strm_prefix = (cfg.get("target_strm_prefix") or "").strip("/")
        self.work_aliases = cfg.get("work_aliases") or []
        os.makedirs(self.strm_root, exist_ok=True)

    # ---------- 入口 ----------
    def run(self, subscriptions: List[Dict[str, Any]],
            backfill_range: Optional[Dict[str, Any]] = None,
            force: bool = False,
            published_date: Optional[dt.date] = None,
            timezone: str = "Asia/Shanghai",
            discover: bool = True) -> Dict[str, Any]:
        """执行一轮。另一进程持锁时直接跳过（不等待、不重复提交）。"""
        summary: Dict[str, Any] = {"discovered": 0, "submitted": 0, "advanced": 0,
                                   "notified": 0, "polled_not_ready": 0,
                                   "cloud_checks": 0, "cloud_check_deferred": 0,
                                   "skipped_pacing": 0, "skipped_lock": False}
        if not self.lock.acquire():
            summary["skipped_lock"] = True
            return summary
        try:
            if discover:
                for sub in subscriptions:
                    rule = SubRule(**{k: sub.get(k) for k in
                                      ("work", "aliases", "release_groups", "language",
                                       "resolution", "season", "exclude_keywords")})
                    self._poll_feed(sub, rule, summary, backfill_range=backfill_range,
                                    published_date=published_date, timezone=timezone)
            self._advance_all(summary)
            summary["notified"] = self._flush_outbox()
        finally:
            self.lock.release()
        return summary

    # ---------- 阶段 1：发现 ----------
    def _poll_feed(self, sub: Dict[str, Any], rule: SubRule,
                   summary: Dict[str, Any],
                   backfill_range: Optional[Dict[str, Any]] = None,
                   published_date: Optional[dt.date] = None,
                   timezone: str = "Asia/Shanghai") -> None:
        feed_url = sub.get("feed_url") or ""
        if not feed_url:
            return
        try:
            xml_text = self.fetch_feed(feed_url)
        except Exception as e:
            # 网络失败：不覆盖任何基线，不删除媒体
            log.warning("feed fetch failed for %s: %s", feed_url, type(e).__name__)
            summary["discovered"] += 0
            return
        try:
            items = parse_feed(xml_text)
        except FeedFormatError as e:
            log.warning("feed format invalid: %s", e)
            return

        backfill = backfill_range or sub.get("backfill")  # None = 追更模式
        new_count = 0
        # 首轮判定在本轮开始前固化：feed 内逐条建基线时，
        # 不会把"本轮刚记录的上一条"误当作已有基线
        first_run_done: Dict[int, bool] = {}
        for item in items:
            if published_date is not None and not published_on(item, published_date, timezone):
                continue
            verdict = match_rule(item, rule)
            if verdict == "exclude":
                continue
            season = item.season if item.season is not None else (rule.season or 1)
            work_key = f"{sub.get('work')}"
            if self.store.get_by_infohash(item.infohash):
                continue  # 已存在（去重）
            if verdict == "needs_review":
                ep_id = self.store.create_episode(
                    infohash=item.infohash, source_id=item.link,
                    source_url=item.link, title=item.title,
                    work=work_key, season=item.season, episode=item.episode,
                    release_group=item.release_group, quality=item.resolution,
                    stage="needs_review")
                self.store.set_stage(ep_id, "needs_review", {"reason": "season/episode 未确定"})
                continue
            # 追更模式：不下载基线期历史集；显式补集仅限授权范围
            if backfill is None:
                if season not in first_run_done:
                    first_run_done[season] = (
                        self.store.baseline_max_episode(work_key, season) is not None
                    )
                if not first_run_done[season]:
                    # 首轮发现：当前 feed 全部记为基线，一集都不下载
                    self.store.record_baseline(work_key, season, item.episode, item.infohash)
                    continue
                baseline_top = self.store.baseline_max_episode(work_key, season)
                if item.episode <= baseline_top:
                    self.store.record_baseline(work_key, season, item.episode, item.infohash)
                    continue
            else:
                start, end = backfill.get("start"), backfill.get("end")
                if start is not None and item.episode < start:
                    continue
                if end is not None and item.episode > end:
                    continue
            if new_count >= self.max_new_tasks:
                break  # 有界：超出部分留给下一轮
            ep_id = self.store.create_episode(
                infohash=item.infohash, source_id=item.link, source_url=item.link,
                title=item.title, work=work_key, season=season, episode=item.episode,
                episode_end=item.episode_end, release_group=item.release_group,
                quality=item.resolution)
            new_count += 1
            summary["discovered"] += 1
        return

    # ---------- 阶段推进 ----------
    def _advance_all(self, summary: Dict[str, Any]) -> None:
        """每轮每条目只处理其快照阶段一次：一个条目一轮最多推进一个阶段。

        这样失败重试、事件触发都在下一轮进行，行为有界、可预测。
        """
        snapshot = self.store.all_episodes()
        by_stage: Dict[str, List[Dict[str, Any]]] = {}
        for ep in snapshot:
            by_stage.setdefault(ep["stage"], []).append(ep)
        for stage, handler in [
            ("failed_retryable", self._retry_failed),
            ("discovered", self._do_submit),
            ("submitting", self._reconcile_submit),
            ("uncertain", self._reconcile_submit),
            ("submitted", self._poll_download),
            ("downloading", self._poll_download),
            ("cloud_ready", self._do_strm),
            ("strm_pending", self._do_strm),
            ("strm_ready", self._do_scrape),
            ("metadata_ready", self._do_library),
            ("library_ready", self._do_notify),
        ]:
            for ep in by_stage.get(stage, []):
                try:
                    handler(ep)
                    summary["advanced"] += 1
                except Exception as e:  # 单条失败不阻塞其它条目
                    log.error("advance failed id=%s stage=%s: %s",
                              ep["id"], stage, type(e).__name__)

    def _retry_failed(self, ep: Dict[str, Any]) -> None:
        """failed_retryable：从最后记录的失败阶段恢复，不重做已完成阶段。"""
        detail = self.store.last_history_detail(ep["id"])
        phase = detail.get("stage")
        if phase == "scraping":
            self._do_scrape(ep)
        elif phase == "strm_pending":
            self._do_strm(ep)
        else:
            self._do_submit(ep)

    # ---- 115 提交 ----
    def _do_submit(self, ep: Dict[str, Any]) -> None:
        ep_id = ep["id"]
        # 先持久化提交意图（submitting + infohash 已有唯一约束）
        self.store.set_stage(ep_id, "submitting", {"plan": "offline_download"})
        uri = f"magnet:?xt=urn:btih:{ep['infohash']}"
        try:
            task_id = self.cloud115.submit(uri)
        except TimeoutError:
            # 提交结果未知 → uncertain，等对账；本次尝试计入 submit_count
            self.store.set_stage(ep_id, "uncertain",
                                 {"error": "submit timeout"},
                                 extra={"submit_count": ep.get("submit_count", 0) + 1})
            return
        except Exception as e:
            self.store.set_stage(ep_id, "failed_retryable", {"error": type(e).__name__})
            return
        self.store.set_stage(ep_id, "submitted",
                             {"task_id": task_id},
                             extra={"remote_task_id": task_id,
                                    "submit_count": ep.get("submit_count", 0) + 1})
        # 有视频文件才视为完成（mock 里提交即完成；真实适配器要轮询 list_files）
        files = self.cloud115.list_files(self.target_dir)
        mine = [f for f in files if f.get("infohash") == ep["infohash"]]
        if mine and has_video_file(mine):
            extra = {"file_id": mine[0].get("file_id")}
            if self.target_strm_prefix and self.mp_strm_root:
                extra["strm_path"] = (f"{self.mp_strm_root}/"
                                      f"{self.target_strm_prefix}/{mine[0]['name']}.strm")
            self.store.set_stage(ep_id, "cloud_ready", {"files": len(files)},
                                 extra=extra)

    # ---- 下载完成轮询：submitted/downloading → cloud_ready ----
    def _poll_download(self, ep: Dict[str, Any]) -> None:
        ep_id = ep["id"]
        files = self.cloud115.list_files(self.target_dir)
        mine = [f for f in files if f.get("infohash") == ep["infohash"]]
        if mine and has_video_file(mine):
            extra = {"file_id": mine[0].get("file_id")}
            if self.target_strm_prefix and self.mp_strm_root:
                extra["strm_path"] = (f"{self.mp_strm_root}/"
                                      f"{self.target_strm_prefix}/{mine[0]['name']}.strm")
            self.store.set_stage(ep_id, "cloud_ready",
                                 {"files": len(mine)},
                                 extra=extra)
        # 未完成：留在 submitted/downloading，下一轮再查（不阻塞脚本）

    # ---- 超时对账：先查远端任务，不盲目重交 ----
    def _reconcile_submit(self, ep: Dict[str, Any]) -> None:
        ep_id = ep["id"]
        for t in self.cloud115.list_tasks():
            if t.infohash == ep["infohash"]:
                self.store.set_stage(ep_id, "submitted", {"task_id": t.task_id},
                                     extra={"remote_task_id": t.task_id})
                files = self.cloud115.list_files(self.target_dir)
                if any(f.get("infohash") == ep["infohash"] for f in files) and has_video_file(files):
                    self.store.set_stage(ep_id, "cloud_ready", {"files": len(files)})
                return
        # 远端确实没有 → 重新提交（submit_count 由本方法之前的事务保证）
        self.store.set_stage(ep_id, "discovered", {"reason": "reconcile: no remote task"})

    # ---- STRM ----
    def _do_strm(self, ep: Dict[str, Any]) -> None:
        ep_id = ep["id"]
        self.store.set_stage(ep_id, "strm_pending", {"trigger": "anime_ready"})
        resp = self.litepan.trigger_event("anime_ready", "hermes",
                                          f"115 file ready {ep['infohash']}")
        if not resp or not isinstance(resp, dict) or resp.get("status") != 200:
            self.store.set_stage(ep_id, "failed_retryable", {"stage": "strm_pending"})
            return
        # 事件 200 ≠ 完成：必须确认本集 STRM 文件真实存在
        strm_path = (ep.get("strm_path")
                     or os.path.join(self.strm_root, self._strm_relative(ep)))
        if self.evidence_via_mp:
            relative = strm_path.replace(self.mp_strm_root, "", 1).lstrip("/").replace("\\", "/")
        else:
            relative = os.path.relpath(strm_path, self.strm_root).replace("\\", "/")
        if not self._strm_evidence(relative):
            # 未完成：留在 strm_pending（下一轮重试），不触发 MP
            return
        self.store.set_stage(ep_id, "strm_ready",
                             {"strm": relative},
                             extra={"strm_path": strm_path})

    def _strm_evidence(self, relative: str) -> bool:
        """STRM 完成证据：文件存在且内容为 URL。
        本地模式查文件系统；远端模式经 MoviePilot storage API 查同一相对路径。"""
        if not self.evidence_via_mp:
            return expected_strm_exists(self.strm_root, relative)
        if not self.mp_strm_root:
            log.warning("evidence_via_mp 已启用但未配置 moviepilot_strm_root")
            return False
        mp_dir = os.path.dirname(f"{self.mp_strm_root}/{relative}")
        fname = relative.rsplit("/", 1)[-1]
        try:
            items = self.moviepilot.storage_list("local", mp_dir.replace("\\", "/"))
            names = [it.get("name") for it in items
                     if isinstance(it, dict) and it.get("type") == "file"]
            if fname not in names:
                return False
            content = self.moviepilot.read_file("local", f"{mp_dir}/{fname}")
            if isinstance(content, bytes):
                head = content[:16].decode("utf-8", "replace")
            elif isinstance(content, dict):
                head = (content.get("_head") or "")[:16]
            else:
                head = str(content)[:16]
            return head.startswith(("http://", "https://"))
        except Exception as e:
            log.warning("STRM 证据检查失败(%s): %s", relative, type(e).__name__)
            return False

    def _strm_relative(self, ep: Dict[str, Any]) -> str:
        safe_title = "".join(ch for ch in ep["title"] if ch not in '\\/:*?"<>|')
        return os.path.join(str(ep["work"]), safe_title[:50] + ".strm").replace("\\", "/")

    # ---- 刮削 ----
    def _do_scrape(self, ep: Dict[str, Any]) -> None:
        ep_id = ep["id"]
        self.store.set_stage(ep_id, "scraping", {})
        strm_path = ep.get("strm_path") or ""
        if not strm_path:
            self.store.set_stage(ep_id, "failed_retryable", {"stage": "scraping", "error": "strm missing"})
            return
        if not self.evidence_via_mp and not os.path.isfile(strm_path):
            self.store.set_stage(ep_id, "failed_retryable", {"stage": "scraping", "error": "strm missing"})
            return
        result = self.moviepilot.scrape_in_place(
            strm_path, self.media_source, str(self.media_id))
        if not result.ok:
            # MP 失败：不刷 Emby
            self.store.set_stage(ep_id, "failed_retryable", {"stage": "scraping"})
            return
        # NFO 完成判定：可解析且季集正确
        try:
            nfo_season, nfo_episode, _tmdb = parse_episode_nfo(result.nfo_path)
        except Exception as e:
            self.store.set_stage(ep_id, "failed_retryable",
                                 {"stage": "scraping", "error": f"nfo: {type(e).__name__}"})
            return
        if nfo_season != ep.get("season") or nfo_episode != ep.get("episode"):
            # 旧 NFO / 错误季集 → 不得通过
            self.store.set_stage(ep_id, "failed_permanent",
                                 {"stage": "scraping", "nfo": [nfo_season, nfo_episode]})
            return
        self.store.set_stage(ep_id, "metadata_ready", {"nfo": result.nfo_path})

    # ---- Emby ----
    def _do_library(self, ep: Dict[str, Any]) -> None:
        ep_id = ep["id"]
        self.emby.refresh_library(self.library_id)
        # 刷新接受 ≠ 入库完成：必须查询到条目（作品名匹配支持别名）
        candidates = [ep["work"]] + [a for a in self.work_aliases if a]
        item = None
        for name in candidates:
            item = self.emby.find_episode(self.library_id, name,
                                          ep.get("season") or 1,
                                          ep.get("episode") or 0)
            if item:
                break
        if not item:
            return  # 留在 metadata_ready，下一轮再查
        self.store.set_stage(ep_id, "library_ready", {"item": item.get("id")})

    # ---- 通知（outbox）----
    def _do_notify(self, ep: Dict[str, Any]) -> None:
        message = f"新集入库: {ep['work']} S{ep.get('season')}E{ep.get('episode')}"
        self.store.enqueue_notification(ep["id"], message)
        self.store.set_stage(ep["id"], "notified", {"queued": True})

    def _flush_outbox(self) -> int:
        delivered = 0
        for n in self.store.pending_notifications():
            if n["state"] == "delivery_pending":
                try:
                    notify_id = self.notify.deliver(n["message"])
                except Exception:
                    self.store.mark_notification(n["id"], "delivery_pending")
                    continue
                if notify_id:
                    self.store.mark_notification(n["id"], "notified", notify_id)
                    delivered += 1
        return delivered

    # ---- 恢复 ----
    def recover(self, ep_id: int) -> Dict[str, Any]:
        """指定条目恢复：从中断阶段继续，不重做已完成阶段。"""
        ep = self.store.get_by_id(ep_id)
        if not ep:
            return {"error": "not found"}
        before = ep["stage"]
        handlers = {
            "discovered": self._do_submit,
            "cloud_ready": self._do_strm,
            "strm_ready": self._do_scrape,
            "metadata_ready": self._do_library,
            "library_ready": self._do_notify,
        }
        handler = handlers.get(before)
        if handler:
            handler(ep)
        after = (self.store.get_by_id(ep_id) or {}).get("stage")
        return {"id": ep_id, "before": before, "after": after}
