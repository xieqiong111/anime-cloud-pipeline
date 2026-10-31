"""V5 SQLite state store with legacy-compatible read/write helpers.

The normalized ``episodes_v5``/``candidates``/``downloads`` tables are the
authoritative state.  A small compatibility surface remains for the original
acceptance suite, but compatibility calls are translated into V5 rows rather
than recreating the old infohash-centered schema.
"""
from __future__ import annotations

import json
import math
import os
import posixpath
import random
import sqlite3
import stat
import threading
import time
from datetime import date
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .migrations import MigrationRunner
from .models import EpisodeKey, EpisodeStatus, ReviewReason, Subscription
from .http import retry_delay
from .security import REDACTED, sanitize_text


STAGES = [
    "discovered", "submitting", "submitted", "downloading", "cloud_ready",
    "strm_pending", "strm_ready", "scraping", "metadata_ready",
    "library_ready", "notified",
]
SPECIAL = {"uncertain", "needs_review", "blocked_auth", "failed_retryable", "failed_permanent"}

_SAFE_CANDIDATE_TRAITS = (
    "media_source", "media_id", "season", "episode", "episode_end",
    "release_group", "resolution", "quality", "language", "languages",
    "subtitle_language", "collection", "is_collection", "media_type",
    "content_type", "marker", "media_marker", "decision", "decision_reason",
    "reason", "published", "score_components", "score_matches", "score_total",
)
_SAFE_CANDIDATE_TRAIT_NAMES = frozenset(_SAFE_CANDIDATE_TRAITS)


class LockHeld(Exception):
    """另一个进程持有运行锁。"""


class ProcessLock:
    """跨进程互斥锁：锁文件 + PID 存活检测。"""

    def __init__(self, path: str):
        self.path = path
        self._owner: Optional[tuple[int, int, int]] = None
        self._ownership_lock = threading.Lock()

    def _snapshot(self) -> Optional[tuple[int, int, int]]:
        """Read a complete regular PID file without following links."""
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
            with os.fdopen(fd, "rb") as handle:
                info = os.fstat(handle.fileno())
                raw = handle.read(64).strip()
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or not raw or not raw.isdigit() or len(raw) > 20):
                    return None
                pid = int(raw)
                current = os.stat(self.path, follow_symlinks=False)
                if pid <= 0 or (info.st_dev, info.st_ino) != (current.st_dev, current.st_ino):
                    return None
                return pid, info.st_dev, info.st_ino
        except (OSError, ValueError):
            return None

    def _guard(self) -> Optional[int]:
        # Never reap an unknown guard: a crashed reaper requires inspection.
        try:
            return os.open(self.path + ".reap", os.O_CREAT | os.O_EXCL | os.O_WRONLY, 0o600)
        except OSError:
            return None

    def _unguard(self, fd: int) -> None:
        try:
            info = os.fstat(fd)
            current = os.stat(self.path + ".reap", follow_symlinks=False)
            if (info.st_dev, info.st_ino) == (current.st_dev, current.st_ino):
                os.unlink(self.path + ".reap")
        except OSError:
            pass
        finally:
            os.close(fd)

    def acquire(self) -> bool:
        with self._ownership_lock:
            return self._acquire()

    def _acquire(self) -> bool:
        if self._owner is not None or os.path.lexists(self.path + ".reap"):
            return False
        flags = os.O_CREAT | os.O_EXCL | os.O_WRONLY
        for _attempt in range(2):
            try:
                fd = os.open(self.path, flags)
            except FileExistsError:
                guard = self._guard()
                if guard is None:
                    return False
                try:
                    snapshot = self._snapshot()
                    if snapshot is None or self._pid_alive(snapshot[0]):
                        return False
                    # Re-read inside the reaper guard after probing liveness.
                    if self._snapshot() != snapshot:
                        return False
                    os.unlink(self.path)
                except OSError:
                    return False
                finally:
                    self._unguard(guard)
                continue
            try:
                pid = os.getpid()
                raw = str(pid).encode()
                if os.write(fd, raw) != len(raw):
                    raise OSError("incomplete process lock PID")
                info = os.fstat(fd)
                self._owner = (pid, info.st_dev, info.st_ino)
            finally:
                os.close(fd)
            return True
        return False

    def _lock_alive(self) -> bool:
        snapshot = self._snapshot()
        return snapshot is None or self._pid_alive(snapshot[0])

    @staticmethod
    def _pid_alive(recorded: int) -> bool:
        if recorded == os.getpid():
            return True
        if os.name == "nt":
            import ctypes  # noqa: PLC0415
            handle = ctypes.windll.kernel32.OpenProcess(0x1000, False, recorded)
            if handle:
                ctypes.windll.kernel32.CloseHandle(handle)
                return True
            return ctypes.windll.kernel32.GetLastError() != 87
        try:
            os.kill(recorded, 0)
            return True
        except ProcessLookupError:
            return False
        except (OSError, OverflowError, ValueError):
            return True

    def release(self) -> None:
        # Reapers cannot remove our live PID. Serialize this object's releases
        # locally, so a second release cannot unlink a subsequent owner's file.
        # A busy reaper guard must not strand a long-lived owner's run.lock.
        with self._ownership_lock:
            owner = self._owner
            if owner is None or owner[0] != os.getpid():
                return
            try:
                if self._snapshot() == owner:
                    os.unlink(self.path)
                self._owner = None
            except OSError:
                pass

    def __enter__(self):
        if not self.acquire():
            raise LockHeld("另一个流水线进程正在运行")
        return self

    def __exit__(self, *exc):
        self.release()


class StateStore:
    """Transactional V5 repository and legacy compatibility facade."""

    _EPISODE_UPDATE_COLUMNS = {
        "subscription_id", "media_source", "media_id", "canonical_work", "aliases_json", "season", "episode",
        "status", "library_id", "library_path", "cloud_path", "strm_path",
        "runtime_identity_json", "retry_count", "next_retry_at", "last_error",
        "review_reason", "review_detail", "review_created_at", "review_resolved_at",
        "review_resolution", "resume_status", "review_generation", "title", "source_id", "source_url",
        "episode_end", "release_group", "quality", "remote_task_id", "file_id",
        "submit_count", "notify_id", "updated_at",
    }

    def __init__(self, db_path: str | os.PathLike[str] | sqlite3.Connection):
        self._owns_connection = not isinstance(db_path, sqlite3.Connection)
        if self._owns_connection:
            path = str(db_path)
            os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
            # The default isolation level lets ``with self.conn`` wrap each
            # repository operation in a real transaction.  V5 requires the
            # episode update and its history row to commit or roll back as a
            # unit.
            # Review resolution and runner workers may share one repository in
            # a bounded process, while separate workers use their own
            # connections.  Allow both patterns and give a competing writer a
            # short busy window so BEGIN IMMEDIATE can perform its CAS check.
            self.conn = sqlite3.connect(path, check_same_thread=False, timeout=10.0)
        else:
            self.conn = db_path
        if self.conn.isolation_level is None:
            # Callers may hand us an autocommit connection.  Normalize it so
            # repository context managers still provide atomic state changes.
            self.conn.isolation_level = ""
        self.conn.row_factory = sqlite3.Row
        self.conn.execute("PRAGMA busy_timeout=10000")
        self.conn.execute("PRAGMA foreign_keys=ON")
        self.conn.execute("PRAGMA journal_mode=WAL")
        self._init_schema()

    def _init_schema(self) -> None:
        MigrationRunner(self.conn).migrate()

    # ---------- subscriptions ----------
    def upsert_subscription(
        self,
        subscription: Subscription | Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> int:
        values = _subscription_values(subscription, fields)
        now = time.time()
        aliases_json = _json_text(values.get("aliases") or values.get("aliases_json") or [])
        snapshot = values.get("snapshot") or values.get("snapshot_json") or {}
        snapshot_json = _json_text(snapshot)
        season = _int_or_zero(values.get("season"))
        key = (
            _required_text(values.get("media_source"), "media_source"),
            _required_text(values.get("media_id"), "media_id"),
            season,
            _required_text(values.get("subscription_id"), "subscription_id"),
        )
        with self.conn:
            self.conn.execute(
                """INSERT INTO subscriptions(
                    subscription_id, media_source, media_id, season, work,
                    aliases_json, include_text, exclude_text, snapshot_json,
                    last_seen_at, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(media_source, media_id, season, subscription_id) DO UPDATE SET
                    work=excluded.work, aliases_json=excluded.aliases_json,
                    include_text=excluded.include_text, exclude_text=excluded.exclude_text,
                    snapshot_json=excluded.snapshot_json,
                    last_seen_at=excluded.last_seen_at, updated_at=excluded.updated_at""",
                (
                    key[3], key[0], key[1], key[2],
                    _text(values.get("work") or values.get("canonical_work")),
                    aliases_json,
                    sanitize_text(values.get("include") or values.get("include_text") or ""),
                    sanitize_text(values.get("exclude") or values.get("exclude_text") or ""),
                    snapshot_json,
                    now,
                    now,
                    now,
                ),
            )
        row = self.conn.execute(
            """SELECT id FROM subscriptions
               WHERE media_source=? AND media_id=? AND season=? AND subscription_id=?""",
            key,
        ).fetchone()
        return int(row[0])

    def get_subscription(self, subscription_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute("SELECT * FROM subscriptions WHERE id=?", (subscription_id,)).fetchone()
        return dict(row) if row else None

    def subscriptions(self) -> list[Dict[str, Any]]:
        return [dict(row) for row in self.conn.execute("SELECT * FROM subscriptions ORDER BY id").fetchall()]

    # ---------- normalized episodes ----------
    def get_or_create_episode(
        self,
        episode_input: EpisodeKey | Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> int:
        values = _episode_values(episode_input, fields)
        key = _episode_key(values)
        now = time.time()
        status = _status_value(values.get("status") or EpisodeStatus.DISCOVERED)
        aliases_json = _json_text(values.get("aliases") or values.get("aliases_json") or [])
        with self.conn:
            cur = self.conn.execute(
                """INSERT INTO episodes_v5(
                    subscription_id, media_source, media_id, canonical_work,
                    aliases_json, season, episode, status, library_id, library_path,
                    cloud_path, strm_path, runtime_identity_json, retry_count,
                    next_retry_at, last_error, title, source_id, source_url,
                    episode_end, release_group, quality, remote_task_id, file_id,
                    submit_count, notify_id, legacy_evidence_json, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, '{}', ?, ?)
                ON CONFLICT(media_source, media_id, season, episode) DO NOTHING""",
                (
                    _text_or_none(values.get("subscription_id")),
                    key.media_source,
                    key.media_id,
                    _text(values.get("canonical_work") or values.get("work") or ""),
                    aliases_json,
                    key.season,
                    key.episode,
                    status,
                    _text_or_none(values.get("library_id")),
                    _text_or_none(values.get("library_path")),
                    _text_or_none(values.get("cloud_path")),
                    _text_or_none(values.get("strm_path")),
                    _json_text(values.get("runtime_identity") or values.get("runtime_identity_json") or {}),
                    _int_or_zero(values.get("retry_count", values.get("attempts", 0))),
                    values.get("next_retry_at"),
                    sanitize_text(values.get("last_error") or ""),
                    _text(values.get("title") or values.get("canonical_work") or values.get("work") or ""),
                    _text(values.get("source_id") or ""),
                    _text(values.get("source_url") or ""),
                    _int_or_none(values.get("episode_end")),
                    _text_or_none(values.get("release_group")),
                    _text_or_none(values.get("quality")),
                    _text_or_none(values.get("remote_task_id")),
                    _text_or_none(values.get("file_id")),
                    _int_or_zero(values.get("submit_count")),
                    _text_or_none(values.get("notify_id")),
                    now,
                    now,
                ),
            )
            row = self.conn.execute(
                """SELECT id FROM episodes_v5
                   WHERE media_source=? AND media_id=? AND season=? AND episode=?""",
                key.as_tuple(),
            ).fetchone()
            if row is None:
                raise RuntimeError("episode insert did not produce a row")
            episode_id = int(row[0])
            if cur.rowcount == 1:
                self._insert_history(episode_id, None, status, {"reason": "created"})
            else:
                # Discovery may learn verified TMDB season evidence after an
                # episode was first created by an earlier provider result.
                # Merge that evidence without manufacturing a second status
                # transition/history row.
                incoming_runtime = _parse_json_mapping(
                    values.get("runtime_identity") or values.get("runtime_identity_json")
                )
                if incoming_runtime:
                    current_runtime = _parse_json_mapping(
                        self.conn.execute(
                            "SELECT runtime_identity_json FROM episodes_v5 WHERE id=?",
                            (episode_id,),
                        ).fetchone()[0]
                    )
                    current_runtime.update(incoming_runtime)
                    self._update_episode(
                        episode_id,
                        {"runtime_identity_json": _json_text(current_runtime)},
                    )
        return episode_id

    def create_episode(self, episode_input: Mapping[str, Any] | None = None, **fields: Any) -> int:
        """Create an episode from V5 fields or the legacy infohash fields."""
        values: dict[str, Any] = {}
        if episode_input:
            values.update(episode_input)
        values.update(fields)
        if "key" in values or "media_id" in values:
            return self.get_or_create_episode(values)

        # Compatibility discovery records predate media identity.  Keep their
        # identity deterministic but make the hash a candidate, never an
        # Episode uniqueness key.
        work = _text(values.get("work") or values.get("title") or "")
        raw_infohash = _text(values.get("infohash") or "").lower()
        episode_number = _int_or_zero(values.get("episode"))
        if values.get("media_id"):
            media_id = _text(values["media_id"])
        elif episode_number <= 0:
            # Reserve a distinct media identity for unresolved episode numbers;
            # a real E01 may use the ordinary legacy-work identity.
            media_id = f"legacy-unresolved:{work or 'unknown'}"
        else:
            media_id = f"legacy-work:{work or 'unknown'}"
        season = _int_or_zero(values.get("season"))
        key = EpisodeKey("legacy", media_id, season, max(1, episode_number))
        values["key"] = key
        values.setdefault("canonical_work", work)
        values.setdefault("title", values.get("title") or work)
        if episode_number <= 0:
            # The compatibility stage is applied by the review transition
            # below so its resume_status is the recoverable discovered state.
            values.pop("status", None)
        elif "stage" in values and "status" not in values:
            values["status"] = values["stage"]
        episode_id = self.get_or_create_episode(values)
        if episode_number <= 0:
            current = self.get_episode(episode_id)
            if current and current.get("status") != EpisodeStatus.NEEDS_REVIEW.value:
                self.open_review(
                    episode_id,
                    ReviewReason.AMBIGUOUS_EPISODE,
                    {
                        "reason": "LEGACY_EPISODE_NUMBER_UNRESOLVED",
                        "work": work,
                        "season": season,
                    },
                )
        infohash = raw_infohash
        if infohash:
            self.add_candidate(
                episode_id,
                {
                    "infohash": infohash,
                    "source_id": values.get("source_id"),
                    "source_url": values.get("source_url"),
                    "title": values.get("title"),
                    "provider": values.get("provider") or "legacy",
                },
            )
        return episode_id

    def get_episode(self, episode_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute("SELECT * FROM episodes_v5 WHERE id=?", (episode_id,)).fetchone()
        return dict(row) if row else None

    def episode_status(self, episode_id: int) -> Optional[str]:
        """Return the current normalized status for focused stage services."""

        row = self.conn.execute(
            "SELECT status FROM episodes_v5 WHERE id=?", (episode_id,)
        ).fetchone()
        return str(row[0]) if row else None

    def get_episode_by_key(self, key: EpisodeKey) -> Optional[Dict[str, Any]]:
        """Return the one episode row for a stable business identity."""

        if not isinstance(key, EpisodeKey):
            raise TypeError("key must be an EpisodeKey")
        row = self.conn.execute(
            """SELECT * FROM episodes_v5
               WHERE media_source=? AND media_id=? AND season=? AND episode=?""",
            key.as_tuple(),
        ).fetchone()
        return dict(row) if row else None

    def get_by_id(self, ep_id: int) -> Optional[Dict[str, Any]]:
        row = self.get_episode(ep_id)
        return self._compat_episode(row) if row else None

    def get_by_infohash(self, infohash: str) -> Optional[Dict[str, Any]]:
        row = self.conn.execute(
            """SELECT e.* FROM episodes_v5 e
               JOIN candidates c ON c.episode_id=e.id
               WHERE c.infohash=? ORDER BY c.id LIMIT 1""",
            (_text(infohash),),
        ).fetchone()
        return self._compat_episode(dict(row)) if row else None

    def all_episodes(self, stage: Optional[str] = None) -> List[Dict[str, Any]]:
        if stage:
            rows = self.conn.execute(
                "SELECT * FROM episodes_v5 WHERE status=? ORDER BY id", (_status_value(stage),)
            ).fetchall()
        else:
            rows = self.conn.execute("SELECT * FROM episodes_v5 ORDER BY id").fetchall()
        return [self._compat_episode(dict(row)) for row in rows]

    def set_stage(
        self,
        ep_id: int,
        stage: str | EpisodeStatus,
        detail: Dict[str, Any] | None = None,
        extra: Dict[str, Any] | None = None,
    ) -> None:
        self.transition(ep_id, stage, detail, extra=extra)

    def mark_pending(
        self,
        ep_id: int,
        stage: str | EpisodeStatus,
        detail: Mapping[str, Any] | str | None = None,
        *,
        extra: Mapping[str, Any] | None = None,
        secrets: Iterable[Any] = (),
    ) -> None:
        """Persist a recoverable stage intent and its redacted evidence.

        Organizer and later pending-stage services call this immediately
        before an external write.  It is intentionally a small public alias
        over :meth:`transition` so fakes and stage services share one
        transaction/history contract.
        """

        self.transition(ep_id, stage, detail, extra=extra, secrets=secrets)

    def transition(
        self,
        ep_id: int,
        status: str | EpisodeStatus,
        detail: Mapping[str, Any] | str | None = None,
        *,
        fail_after_episode_update: bool = False,
        extra: Mapping[str, Any] | None = None,
        secrets: Iterable[Any] = (),
    ) -> None:
        target = _status_value(status)
        current = self.get_episode(ep_id)
        if current is None:
            raise KeyError(f"episode {ep_id} not found")
        updates: dict[str, Any] = {"status": target, "updated_at": time.time()}
        updates.update(_allowed_episode_updates(extra or {}, secrets=secrets))
        with self.conn:
            self._update_episode(ep_id, updates)
            if fail_after_episode_update:
                raise RuntimeError("injected failure after episode update")
            self._insert_history(ep_id, current.get("status"), target, detail or {}, secrets=secrets)

    def transition_if_current(
        self,
        ep_id: int,
        expected_status: str | EpisodeStatus,
        status: str | EpisodeStatus,
        detail: Mapping[str, Any] | str | None = None,
        *,
        extra: Mapping[str, Any] | None = None,
        secrets: Iterable[Any] = (),
    ) -> bool:
        """Compare-and-set one episode stage and its history row.

        Shared download reconciliation rereads every target immediately before
        promotion.  The status check and history insert must share the same
        write transaction so a concurrent review/open cannot be overwritten.
        """

        expected = _status_value(expected_status)
        target = _status_value(status)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT * FROM episodes_v5 WHERE id=?", (ep_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"episode {ep_id} not found")
            current = dict(row)
            if current.get("status") != expected:
                self.conn.commit()
                return False
            updates: dict[str, Any] = {"status": target, "updated_at": time.time()}
            updates.update(_allowed_episode_updates(extra or {}, secrets=secrets))
            self._update_episode(ep_id, updates)
            self._insert_history(
                ep_id, current.get("status"), target, detail or {}, secrets=secrets,
            )
            self.conn.commit()
            return True
        except Exception:
            self.conn.rollback()
            raise

    def claim_library_refresh(
        self,
        ep_id: int,
        detail: Mapping[str, Any] | str | None = None,
        *,
        now: float | None = None,
        base: float = 30.0,
        cap: float = 3600.0,
        jitter_seconds: float = 0.0,
        random_source: Any = None,
        extra: Mapping[str, Any] | None = None,
        secrets: Iterable[Any] = (),
    ) -> Optional[Dict[str, Any]]:
        """Atomically claim one bounded Emby refresh attempt.

        The lease is stored in ``next_retry_at`` before the external POST.
        A crashed worker therefore leaves a durable intent which prevents a
        second process from refreshing until the bounded backoff expires.
        """

        timestamp = time.time() if now is None else float(now)
        base_delay = float(base)
        max_delay = float(cap)
        if base_delay <= 0 or max_delay <= 0:
            raise ValueError("refresh retry delays must be positive")
        max_delay = max(base_delay, max_delay)
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT status, retry_count, next_retry_at FROM episodes_v5 WHERE id=?",
                (ep_id,),
            ).fetchone()
            if row is None:
                raise KeyError(f"episode {ep_id} not found")
            status = str(row["status"] or "")
            if status in {EpisodeStatus.LIBRARY_READY.value, EpisodeStatus.NOTIFIED.value}:
                self.conn.commit()
                return None
            next_retry_at = row["next_retry_at"]
            try:
                if next_retry_at not in (None, "") and float(next_retry_at) > timestamp:
                    self.conn.commit()
                    return None
            except (TypeError, ValueError):
                # A malformed old retry timestamp is not a lease; repair it
                # with the next atomic claim rather than treating it as proof
                # that an external refresh is in flight.
                pass
            try:
                previous_count = int(row["retry_count"] or 0)
            except (TypeError, ValueError):
                previous_count = 0
            previous_count = max(0, previous_count)
            count = previous_count + 1
            delay = retry_delay(
                {
                    "backoff_seconds": base_delay,
                    "backoff_max_seconds": max_delay,
                    "jitter_seconds": jitter_seconds,
                },
                count,
                random_source,
            )
            lease_until = timestamp + delay
            updates = {
                "status": EpisodeStatus.LIBRARY_PENDING.value,
                "retry_count": count,
                "next_retry_at": lease_until,
                "last_error": "",
                "updated_at": timestamp,
            }
            updates.update(_allowed_episode_updates(extra or {}, secrets=secrets))
            self._update_episode(ep_id, updates)
            claim_detail = detail or {
                "reason": "emby_refresh_claimed",
                "intent": "refresh_library",
            }
            self._insert_history(
                ep_id, status, EpisodeStatus.LIBRARY_PENDING.value,
                claim_detail, secrets=secrets,
            )
            self.conn.commit()
            return {"retry_count": count, "next_retry_at": lease_until}
        except Exception:
            self.conn.rollback()
            raise

    def history(self, ep_id: int) -> list[str]:
        rows = self.conn.execute(
            "SELECT to_status FROM stage_history WHERE episode_id=? ORDER BY id", (ep_id,)
        ).fetchall()
        return [str(row[0]) for row in rows]

    def last_history_detail(self, ep_id: int) -> Dict[str, Any]:
        row = self.conn.execute(
            "SELECT detail_json FROM stage_history WHERE episode_id=? ORDER BY id DESC LIMIT 1",
            (ep_id,),
        ).fetchone()
        if not row:
            return {}
        try:
            parsed = json.loads(row[0])
            return parsed if isinstance(parsed, dict) else {}
        except (TypeError, ValueError):
            return {}

    # ---------- candidates/downloads/files ----------
    def add_candidate(
        self,
        episode_id: int,
        candidate: Mapping[str, Any] | None = None,
        **fields: Any,
    ) -> int:
        values: dict[str, Any] = {}
        if candidate:
            values.update(candidate)
        values.update(fields)
        infohash = _required_text(values.get("infohash"), "infohash").lower()
        candidate_traits = _safe_candidate_traits(
            _parse_json_mapping(values.get("traits") or values.get("traits_json") or {})
        )
        # Keep normalized media traits alongside the legacy candidate columns
        # so scoring can be rerun without losing provider evidence.
        for trait in _SAFE_CANDIDATE_TRAITS:
            if trait in values:
                safe_value = _safe_candidate_trait_value(values[trait], trait)
                if safe_value is not _UNSAFE_TRAIT:
                    candidate_traits[trait] = safe_value
        now = time.time()
        with self.conn:
            self.conn.execute(
                """INSERT INTO candidates(
                    episode_id, provider, source_id, source_url, title, infohash,
                    score, selected, decision_reason, traits_json, discovered_at,
                    selected_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(episode_id, infohash) DO UPDATE SET
                    provider=excluded.provider, source_id=excluded.source_id,
                    source_url=excluded.source_url, title=excluded.title,
                    score=excluded.score, decision_reason=excluded.decision_reason,
                    traits_json=excluded.traits_json""",
                (
                    episode_id,
                    _text(values.get("provider") or values.get("source") or ""),
                    _text(values.get("source_id") or ""),
                    _text(values.get("source_url") or values.get("url") or ""),
                    _text(values.get("title") or ""),
                    infohash,
                    _float_or_none(values.get("score")),
                    0,
                    sanitize_text(values.get("reason") or values.get("decision_reason") or ""),
                    _json_text(candidate_traits),
                    now,
                    None,
                ),
            )
        row = self.conn.execute(
            "SELECT id FROM candidates WHERE episode_id=? AND infohash=?", (episode_id, infohash)
        ).fetchone()
        if values.get("selected"):
            self.select_candidate(int(row[0]))
        return int(row[0])

    def get_candidate(self, candidate_id: int) -> Optional[Dict[str, Any]]:
        """Return one persisted candidate as a plain mapping."""

        row = self.conn.execute(
            "SELECT * FROM candidates WHERE id=?", (candidate_id,)
        ).fetchone()
        return dict(row) if row else None

    def get_candidates(self, episode_id: int) -> list[Dict[str, Any]]:
        """Return all candidates for an episode in stable insertion order."""

        rows = self.conn.execute(
            "SELECT * FROM candidates WHERE episode_id=? ORDER BY id", (episode_id,)
        ).fetchall()
        return [dict(row) for row in rows]

    # Friendly aliases used by orchestration code and focused tests.
    candidates_for_episode = get_candidates
    list_candidates = get_candidates

    def update_candidate_score(
        self,
        candidate_id: int,
        score: float,
        components: Mapping[str, Any] | None = None,
        *,
        matches: Mapping[str, Any] | None = None,
        decision_reason: str | None = None,
    ) -> None:
        """Persist a score and its explainable components without a schema bump.

        The V5 candidates table already has ``score`` and ``traits_json``.
        Keeping the component breakdown in the latter preserves old databases
        and lets operators inspect why a candidate won or was excluded.
        """

        row = self.get_candidate(candidate_id)
        if row is None:
            raise KeyError(f"candidate {candidate_id} not found")
        traits = _parse_json_mapping(row.get("traits_json"))
        traits["score_components"] = dict(components or {})
        if matches is not None:
            traits["score_matches"] = dict(matches)
        traits["score_total"] = score
        safe_reason = sanitize_text(decision_reason or row.get("decision_reason") or "")
        traits["score_decision_reason"] = safe_reason
        with self.conn:
            self.conn.execute(
                """UPDATE candidates
                   SET score=?, decision_reason=?, traits_json=?
                   WHERE id=?""",
                (
                    float(score),
                    safe_reason,
                    _json_text(traits),
                    candidate_id,
                ),
            )

    def clear_candidate_selection(self, episode_id: int) -> None:
        """Clear every selected candidate for one episode in one transaction."""

        with self.conn:
            self.conn.execute(
                "UPDATE candidates SET selected=0, selected_at=NULL WHERE episode_id=?",
                (episode_id,),
            )

    def review_duplicate_candidates(
        self,
        episode_id: int,
        detail: Mapping[str, Any] | str | None = None,
    ) -> None:
        """Clear selection and open a duplicate review as one transaction."""

        self._open_review_transaction(
            episode_id,
            ReviewReason.DUPLICATE_EPISODE,
            detail,
            clear_candidate_selection=True,
        )

    def select_candidate(self, candidate_id: int, selected_candidate_id: int | None = None) -> int:
        if selected_candidate_id is not None:
            candidate_id = selected_candidate_id
        row = self.conn.execute("SELECT episode_id FROM candidates WHERE id=?", (candidate_id,)).fetchone()
        if row is None:
            raise KeyError(f"candidate {candidate_id} not found")
        episode_id = int(row[0])
        with self.conn:
            self.conn.execute("UPDATE candidates SET selected=0, selected_at=NULL WHERE episode_id=?", (episode_id,))
            self.conn.execute("UPDATE candidates SET selected=1, selected_at=? WHERE id=?", (time.time(), candidate_id))
        return candidate_id

    def selected_candidates(self, episode_id: int) -> list[int]:
        rows = self.conn.execute("SELECT id FROM candidates WHERE episode_id=? AND selected=1 ORDER BY id", (episode_id,)).fetchall()
        return [int(row[0]) for row in rows]

    def upsert_download(self, infohash: str, **fields: Any) -> int:
        """Create or update the download identity shared by many episodes."""
        normalized = _required_text(infohash, "infohash").lower()
        now = time.time()
        with self.conn:
            self.conn.execute(
                """INSERT INTO downloads(
                    infohash, remote_task_id, status, attempts, submitted_at,
                    completed_at, last_error, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                ON CONFLICT(infohash) DO UPDATE SET
                    remote_task_id=COALESCE(excluded.remote_task_id, downloads.remote_task_id),
                    status=excluded.status, attempts=excluded.attempts,
                    submitted_at=COALESCE(excluded.submitted_at, downloads.submitted_at),
                    completed_at=COALESCE(excluded.completed_at, downloads.completed_at),
                    last_error=excluded.last_error, updated_at=excluded.updated_at""",
                (
                    normalized,
                    _text_or_none(fields.get("remote_task_id")),
                    _text(fields.get("status") or "pending"),
                    _int_or_zero(fields.get("attempts")),
                    fields.get("submitted_at"), fields.get("completed_at"),
                    sanitize_text(fields.get("last_error") or ""), now, now,
                ),
            )
        return int(self.conn.execute("SELECT id FROM downloads WHERE infohash=?", (normalized,)).fetchone()[0])

    def get_download(self, download_id: int) -> Optional[Dict[str, Any]]:
        row = self.conn.execute("SELECT * FROM downloads WHERE id=?", (download_id,)).fetchone()
        return dict(row) if row else None

    def record_download_files(
        self,
        download: int | str,
        files: Iterable[Mapping[str, Any]],
        **download_fields: Any,
    ) -> list[int]:
        now = time.time()
        with self.conn:
            if isinstance(download, int):
                download_id = download
            else:
                # ``downloads.infohash`` is the canonical lower-case identity;
                # inventory reconciliation must not create a second download
                # merely because a remote task returned upper-case hex.
                infohash = _required_text(download, "infohash").lower()
                self.conn.execute(
                    """INSERT INTO downloads(
                        infohash, remote_task_id, status, attempts, submitted_at,
                        completed_at, last_error, created_at, updated_at
                    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)
                    ON CONFLICT(infohash) DO UPDATE SET
                        remote_task_id=COALESCE(excluded.remote_task_id, downloads.remote_task_id),
                        status=excluded.status, attempts=excluded.attempts,
                        completed_at=COALESCE(excluded.completed_at, downloads.completed_at),
                        last_error=excluded.last_error, updated_at=excluded.updated_at""",
                    (
                        infohash,
                        _text_or_none(download_fields.get("remote_task_id")),
                        _text(download_fields.get("status") or "inventory_ready"),
                        _int_or_zero(download_fields.get("attempts")),
                        download_fields.get("submitted_at"),
                        download_fields.get("completed_at", now),
                        sanitize_text(download_fields.get("last_error") or ""),
                        now,
                        now,
                    ),
                )
                download_id = int(self.conn.execute("SELECT id FROM downloads WHERE infohash=?", (infohash,)).fetchone()[0])

            result: list[int] = []
            for item in files:
                if isinstance(item, Mapping):
                    values = dict(item)
                elif hasattr(item, "to_mapping"):
                    values = dict(item.to_mapping())
                else:
                    values = dict(item)
                remote_file_id = _text_or_none(values.get("remote_file_id") or values.get("file_id"))
                path = _text_or_none(values.get("path"))
                parent_id = _text_or_none(values.get("parent_id"))
                parent_path = _text_or_none(values.get("parent_path"))
                name = _text(values.get("name") or values.get("filename") or "")
                size = _int_or_none(values.get("size"))
                duration = _float_or_none(values.get("duration"))
                fingerprint = _text_or_none(
                    values.get("fingerprint") or values.get("content_fingerprint")
                )
                inventoried_at = values.get("inventoried_at", now)
                redacted_remote_id = remote_file_id is not None and REDACTED in remote_file_id
                redacted_parent_id = parent_id is not None and REDACTED in parent_id
                redacted_parent_path = parent_path is not None and REDACTED in parent_path
                redacted_path = path is not None and REDACTED in path
                redacted_name = REDACTED in name

                # SQLite's UNIQUE constraint treats NULL as distinct.  Resolve
                # the identity explicitly so repeated inventories with a
                # missing remote ID/path remain idempotent.  When both are
                # absent, the remaining stable file evidence is the best
                # available identity and prevents indistinguishable rows from
                # multiplying without collapsing differently named files.
                if remote_file_id is not None or path is not None:
                    if not redacted_remote_id and not redacted_path:
                        row = self.conn.execute(
                            """SELECT id FROM download_files
                               WHERE download_id=? AND remote_file_id IS ? AND path IS ?
                               ORDER BY id LIMIT 1""",
                            (download_id, remote_file_id, path),
                        ).fetchone()
                    elif remote_file_id is not None and not redacted_remote_id:
                        # A redacted path is display-only; a non-redacted
                        # remote ID remains a safe identity boundary.
                        rows = self.conn.execute(
                            """SELECT id FROM download_files
                               WHERE download_id=? AND remote_file_id=?
                               ORDER BY id""",
                            (download_id, remote_file_id),
                        ).fetchall()
                        row = rows[0] if len(rows) == 1 else None
                    elif path is not None and not redacted_path:
                        row = self.conn.execute(
                            """SELECT id FROM download_files
                               WHERE download_id=? AND path=?
                               ORDER BY id LIMIT 1""",
                            (download_id, path),
                        ).fetchone()
                    else:
                        # Never make the visible redaction placeholder the
                        # unique key: two different raw paths may render the
                        # same safe value.
                        row = None
                    if row is None:
                        # A first inventory may have had neither remote
                        # identity. Reconcile it only when exactly one
                        # NULL/NULL row is compatible with later evidence;
                        # multiple matches stay separate rather than guessing.
                        unknown_rows = self.conn.execute(
                            """SELECT id, parent_id, parent_path, name, size, duration, fingerprint
                               FROM download_files
                               WHERE download_id=? AND remote_file_id IS NULL
                                 AND path IS NULL
                               ORDER BY id""",
                            (download_id,),
                        ).fetchall()

                        def compatible_unknown(candidate: sqlite3.Row) -> bool:
                            evidence = (
                                (
                                    None if redacted_parent_id else parent_id,
                                    candidate["parent_id"],
                                ),
                                (
                                    None if redacted_parent_path else parent_path,
                                    candidate["parent_path"],
                                ),
                                (None if redacted_name else name, candidate["name"]),
                                (size, candidate["size"]),
                                (duration, candidate["duration"]),
                                (fingerprint, candidate["fingerprint"]),
                            )
                            matched = False
                            for incoming, previous in evidence:
                                if incoming is None or incoming == "":
                                    continue
                                if previous is not None and incoming != previous:
                                    return False
                                if previous == incoming:
                                    matched = True
                            return matched

                        compatible = [
                            candidate
                            for candidate in unknown_rows
                            if compatible_unknown(candidate)
                        ]
                        if len(compatible) == 1:
                            candidate = compatible[0]
                            file_id = int(candidate["id"])
                            self.conn.execute(
                                """UPDATE download_files
                                   SET remote_file_id=?, parent_id=?, parent_path=?, path=?,
                                       name=?, size=?, duration=?, fingerprint=?, inventoried_at=?
                                   WHERE id=?""",
                                (
                                    remote_file_id, parent_id, parent_path, path,
                                    name, size, duration, fingerprint, inventoried_at,
                                    file_id,
                                ),
                            )
                            result.append(file_id)
                            continue
                else:
                    if redacted_name or redacted_parent_id or redacted_parent_path:
                        row = None
                    else:
                        row = self.conn.execute(
                            """SELECT id FROM download_files
                               WHERE download_id=? AND remote_file_id IS NULL AND path IS NULL
                                 AND parent_id IS ? AND parent_path IS ? AND name=?
                                 AND size IS ? AND duration IS ? AND fingerprint IS ?
                               ORDER BY id LIMIT 1""",
                            (
                                download_id, parent_id, parent_path, name, size,
                                duration, fingerprint,
                            ),
                        ).fetchone()
                if row is None:
                    cursor = self.conn.execute(
                        """INSERT INTO download_files(
                            download_id, remote_file_id, parent_id, parent_path, path,
                            name, size, duration, fingerprint, inventoried_at
                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                        (
                            download_id, remote_file_id, parent_id, parent_path, path,
                            name, size, duration, fingerprint, inventoried_at,
                        ),
                    )
                    file_id = int(cursor.lastrowid)
                else:
                    file_id = int(row[0])
                    self.conn.execute(
                        """UPDATE download_files
                           SET parent_id=?, parent_path=?, name=?, size=?, duration=?,
                               fingerprint=?, inventoried_at=?
                           WHERE id=?""",
                        (
                            parent_id, parent_path, name, size, duration,
                            fingerprint, inventoried_at, file_id,
                        ),
                    )
                result.append(file_id)
            return result

    def map_episode_source(
        self,
        episode_id: int,
        download_file_id: int,
        *,
        selected: bool = False,
        selected_primary: bool | None = None,
        evidence: Mapping[str, Any] | str | None = None,
        reason: str | None = None,
    ) -> int:
        primary = selected if selected_primary is None else selected_primary
        with self.conn:
            if primary:
                self.conn.execute("UPDATE episode_sources SET selected_primary=0 WHERE episode_id=?", (episode_id,))
            self.conn.execute(
                """INSERT INTO episode_sources(
                    episode_id, download_file_id, selected_primary, evidence, reason, created_at
                ) VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT(episode_id, download_file_id) DO UPDATE SET
                    selected_primary=excluded.selected_primary, evidence=excluded.evidence,
                    reason=excluded.reason""",
                (episode_id, download_file_id, 1 if primary else 0, _json_text(evidence or {}), sanitize_text(reason or "") or None, time.time()),
            )
        row = self.conn.execute("SELECT id FROM episode_sources WHERE episode_id=? AND download_file_id=?", (episode_id, download_file_id)).fetchone()
        return int(row[0])

    def clear_episode_source_selection(self, episode_id: int) -> None:
        """Clear primary-source flags before persisting an unresolved review."""

        with self.conn:
            self.conn.execute(
                "UPDATE episode_sources SET selected_primary=0 WHERE episode_id=?",
                (episode_id,),
            )

    add_episode_source = map_episode_source
    link_episode_source = map_episode_source

    def record_file(self, download_id: int, file: Mapping[str, Any]) -> int:
        return self.record_download_files(download_id, [file])[0]

    def get_selected_download_file(self, episode_id: int) -> Optional[Dict[str, Any]]:
        """Return the one persisted primary remote file for an episode.

        The episode's ``file_id`` is provider identity, while the selected
        inventory row also retains the original basename and parent path.
        Require exactly one primary mapping before using it to organize.
        """

        rows = self.conn.execute(
            """SELECT df.remote_file_id, df.parent_id, df.parent_path,
                      df.path, df.name, df.size, df.fingerprint
               FROM episode_sources AS es
               JOIN download_files AS df ON df.id=es.download_file_id
               WHERE es.episode_id=? AND es.selected_primary=1""",
            (episode_id,),
        ).fetchall()
        if len(rows) != 1:
            return None
        row = dict(rows[0])
        row["filename"] = row["name"]
        return row

    def get_subscription_season_air_date(self, episode_id: int) -> str:
        """Read the verified TMDB season date from the matching subscription."""

        row = self.conn.execute(
            """SELECT e.media_id, e.season, s.media_id, s.season,
                      s.snapshot_json
               FROM episodes_v5 AS e JOIN subscriptions AS s
                 ON s.subscription_id=e.subscription_id
               WHERE e.id=?""",
            (episode_id,),
        ).fetchone()
        if row is None or str(row[0]) != str(row[2]) or int(row[1]) != int(row[3]):
            return ""
        snapshot = _parse_json_mapping(row[4])
        candidate = snapshot.get("season_air_date")
        if not isinstance(candidate, str) or len(candidate) != 10:
            return ""
        try:
            parsed = date.fromisoformat(candidate)
        except ValueError:
            return ""
        return candidate if parsed.isoformat() == candidate else ""

    # ---------- transactional reviews and notifications ----------
    def _open_review_transaction(
        self,
        ep_id: int,
        reason: str | ReviewReason,
        detail: Mapping[str, Any] | str | None = None,
        *,
        resume_status: str | EpisodeStatus | None = None,
        extra: Mapping[str, Any] | None = None,
        clear_candidate_selection: bool = False,
        fail_after_episode_update: bool = False,
        secrets: Iterable[Any] = (),
    ) -> int:
        """Open a review after taking SQLite's writer lock.

        Reopening a review must serialize the read of the current review and
        the generation increment.  A deferred transaction lets two workers
        both read the same generation before either writer is serialized,
        which can create duplicate generation values.  The standalone public
        entry points own this transaction; callers that already own a write
        transaction are allowed to participate without nesting ``BEGIN``.
        """

        reason_value = _reason_value(reason)
        detail_text = _json_text(detail or {}, secrets=secrets)
        owns_transaction = not self.conn.in_transaction
        if owns_transaction:
            self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT * FROM episodes_v5 WHERE id=?", (ep_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"episode {ep_id} not found")
            current = dict(row)
            now = time.time()
            current_status = current.get("status") or EpisodeStatus.DISCOVERED.value
            try:
                previous_generation = int(current.get("review_generation") or 0)
            except (TypeError, ValueError):
                previous_generation = 0
            saved_resume = current.get("resume_status")
            target_resume = _status_value(
                resume_status
                if resume_status is not None
                else (
                    saved_resume
                    if current_status == EpisodeStatus.NEEDS_REVIEW.value and saved_resume
                    else current_status
                )
            )
            if clear_candidate_selection:
                self.conn.execute(
                    "UPDATE candidates SET selected=0, selected_at=NULL WHERE episode_id=?",
                    (ep_id,),
                )
            updates = {
                "status": EpisodeStatus.NEEDS_REVIEW.value,
                "review_reason": reason_value,
                "review_detail": detail_text,
                "review_created_at": now,
                "review_resolved_at": None,
                "review_resolution": None,
                "resume_status": target_resume,
                "review_generation": previous_generation + 1,
                "updated_at": now,
            }
            updates.update(_allowed_episode_updates(extra or {}, secrets=secrets))
            self._update_episode(ep_id, updates)
            if fail_after_episode_update:
                raise RuntimeError("injected failure after review update")
            self._insert_history(
                ep_id,
                current_status,
                EpisodeStatus.NEEDS_REVIEW.value,
                detail_text,
                secrets=secrets,
            )
            if owns_transaction:
                self.conn.commit()
        except BaseException:
            if owns_transaction and self.conn.in_transaction:
                self.conn.rollback()
            raise
        return int(ep_id)

    def open_review(
        self,
        ep_id: int,
        reason: str | ReviewReason,
        detail: Mapping[str, Any] | str | None = None,
        *,
        resume_status: str | EpisodeStatus | None = None,
        extra: Mapping[str, Any] | None = None,
        fail_after_episode_update: bool = False,
        secrets: Iterable[Any] = (),
    ) -> int:
        """Open or replace one durable review in one episode transaction.

        V5 keeps one active review per episode, so the episode primary key is
        also the stable review id.  Re-opening an already open review keeps
        its original resume status unless the caller supplies one explicitly;
        this prevents a second review pass from turning ``resume_status`` into
        ``needs_review``.
        """

        return self._open_review_transaction(
            ep_id,
            reason,
            detail,
            resume_status=resume_status,
            extra=extra,
            fail_after_episode_update=fail_after_episode_update,
            secrets=secrets,
        )

    def resolve_review(
        self,
        ep_id: int,
        resolution: Mapping[str, Any] | str | None = None,
        *,
        status: str | EpisodeStatus | None = None,
        extra: Mapping[str, Any] | None = None,
        selected_candidate_id: int | None = None,
        selected_file_id: Any = None,
        identity: Mapping[str, Any] | None = None,
        runtime_identity: Mapping[str, Any] | None = None,
        expected_review_created_at: float | None = None,
        expected_review_reason: str | ReviewReason | None = None,
        expected_review_generation: int | None = None,
        expected_review_version: int | float | None = None,
        fail_after_episode_update: bool = False,
        secrets: Iterable[Any] = (),
    ) -> str:
        """Resolve an open review atomically and return its resume status.

        ``BEGIN IMMEDIATE`` makes the open-review check and the resolution
        transition one compare-and-set operation.  Two workers resolving the
        same review therefore produce one transition/history row; the loser
        receives ``ValueError`` without modifying retry or review state.
        """

        resolution_text = _json_text(resolution or {}, secrets=secrets)
        now = time.time()
        self.conn.execute("BEGIN IMMEDIATE")
        try:
            row = self.conn.execute(
                "SELECT * FROM episodes_v5 WHERE id=?", (ep_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"episode {ep_id} not found")
            current = dict(row)
            if (
                current.get("status") != EpisodeStatus.NEEDS_REVIEW.value
                or current.get("review_resolved_at") is not None
            ):
                raise ValueError(f"review {ep_id} is not open")
            if (
                expected_review_created_at is not None
                and current.get("review_created_at") != expected_review_created_at
            ):
                raise ValueError(f"review {ep_id} has changed")
            if expected_review_version is not None:
                try:
                    current_generation = int(current.get("review_generation") or 0)
                except (TypeError, ValueError):
                    current_generation = 0
                # ``expected_review_version`` was the pre-generation public
                # port and used the timestamp.  Accept a generation value as
                # the forward-compatible spelling while retaining old callers.
                try:
                    version_is_generation = int(expected_review_version) == current_generation
                except (TypeError, ValueError):
                    version_is_generation = False
                if not version_is_generation and current.get("review_created_at") != expected_review_version:
                    raise ValueError(f"review {ep_id} has changed")
            if (
                expected_review_reason is not None
                and current.get("review_reason") != _reason_value(expected_review_reason)
            ):
                raise ValueError(f"review {ep_id} has changed")
            if expected_review_generation is not None:
                try:
                    current_generation = int(current.get("review_generation") or 0)
                except (TypeError, ValueError):
                    current_generation = 0
                if current_generation != int(expected_review_generation):
                    raise ValueError(f"review {ep_id} has changed")
            target = _status_value(
                status or current.get("resume_status") or EpisodeStatus.DISCOVERED
            )
            updates: dict[str, Any] = {
                "status": target,
                "review_resolved_at": now,
                "review_resolution": resolution_text,
                "retry_count": 0,
                "next_retry_at": None,
                "last_error": "",
                "updated_at": now,
            }
            if selected_candidate_id is not None:
                _select_candidate_in_transaction(
                    self.conn, ep_id, selected_candidate_id,
                )
            if selected_file_id is not None:
                selected_file = _select_episode_file_in_transaction(
                    self.conn, ep_id, selected_file_id,
                )
            if identity is not None:
                updates.update(_identity_updates(identity))
            if runtime_identity is not None:
                current_runtime = _parse_json_mapping(current.get("runtime_identity_json"))
                current_runtime.update(_runtime_identity_updates(runtime_identity))
                updates["runtime_identity_json"] = _json_text(
                    current_runtime, secrets=secrets,
                )
            updates.update(_allowed_episode_updates(extra or {}, secrets=secrets))
            # The transactional resolver owns the canonical mapped file ID;
            # operator aliases must never overwrite it with a database row ID.
            if selected_file_id is not None:
                updates["file_id"] = selected_file["remote_file_id"]
                current_runtime = _parse_json_mapping(current.get("runtime_identity_json"))
                # Inventory columns, not operator aliases or old projections,
                # own the complete identity of this newly selected source.
                name = selected_file["name"]
                path = selected_file["path"] or (
                    posixpath.join(selected_file["parent_path"], name)
                    if selected_file["parent_path"] and name else ""
                )
                current_runtime.update({
                    "remote_file_id": selected_file["remote_file_id"],
                    "source_path": path,
                    "source_filename": name,
                    "_extension": posixpath.splitext(name)[1],
                    "source_parent_id": selected_file["parent_id"],
                    "source_parent_path": selected_file["parent_path"],
                    "parent_id": selected_file["parent_id"],
                    "parent_path": selected_file["parent_path"],
                    "size": selected_file["size"],
                    "fingerprint": selected_file["fingerprint"],
                    "infohash": selected_file["infohash"],
                })
                updates["runtime_identity_json"] = _json_text(current_runtime, secrets=secrets)
            self._update_episode(ep_id, updates)
            if fail_after_episode_update:
                raise RuntimeError("injected failure after review resolution update")
            self._insert_history(
                ep_id, current.get("status"), target, resolution_text, secrets=secrets,
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise
        return target

    def list_open_reviews(self) -> list[Dict[str, Any]]:
        """Return active review rows in stable creation order."""

        rows = self.conn.execute(
            """SELECT * FROM episodes_v5
               WHERE status=? AND review_resolved_at IS NULL
               ORDER BY COALESCE(review_created_at, created_at), id""",
            (EpisodeStatus.NEEDS_REVIEW.value,),
        ).fetchall()
        return [dict(row) for row in rows]

    def get_review(self, review_id: int) -> Optional[Dict[str, Any]]:
        """Return one active review by its stable episode/review id."""

        row = self.conn.execute(
            """SELECT * FROM episodes_v5
               WHERE id=? AND status=? AND review_resolved_at IS NULL""",
            (review_id, EpisodeStatus.NEEDS_REVIEW.value),
        ).fetchone()
        return dict(row) if row else None

    def transition_library_ready_with_notification(
        self,
        ep_id: int,
        dedupe_key: str,
        message: str,
        detail: Mapping[str, Any] | str | None = None,
        *,
        fail_after_episode_update: bool = False,
        extra: Mapping[str, Any] | None = None,
        secrets: Iterable[Any] = (),
    ) -> int:
        now = time.time()
        safe_message = sanitize_text(message, secrets=secrets)
        safe_key = sanitize_text(dedupe_key, secrets=secrets)
        with self.conn:
            row = self.conn.execute(
                "SELECT * FROM episodes_v5 WHERE id=?", (ep_id,)
            ).fetchone()
            if row is None:
                raise KeyError(f"episode {ep_id} not found")
            current = dict(row)
            already_ready = current.get("status") in {
                EpisodeStatus.LIBRARY_READY.value,
                EpisodeStatus.NOTIFIED.value,
            }
            updates = {"status": EpisodeStatus.LIBRARY_READY.value, "updated_at": now}
            updates.update(_allowed_episode_updates(extra or {}, secrets=secrets))
            if not already_ready:
                self._update_episode(ep_id, updates)
                if fail_after_episode_update:
                    raise RuntimeError("injected failure after library-ready update")
                self._insert_history(
                    ep_id, current.get("status"), EpisodeStatus.LIBRARY_READY.value,
                    detail or {}, secrets=secrets,
                )
            elif len(updates) > 2:
                # A legacy ready row may still carry retry metadata; repair it
                # without emitting a duplicate stage-history event.
                repair_updates = {
                    key: value for key, value in updates.items()
                    if key not in {"status"}
                }
                self._update_episode(ep_id, repair_updates)
            self.conn.execute(
                """INSERT OR IGNORE INTO notifications(
                    episode_id, dedupe_key, message, delivery_state, state, attempts,
                    created_at, updated_at
                ) VALUES (?, ?, ?, 'delivery_pending', 'delivery_pending', 0, ?, ?)""",
                (ep_id, safe_key, safe_message, now, now),
            )
        row = self.conn.execute("SELECT id FROM notifications WHERE dedupe_key=?", (safe_key,)).fetchone()
        return int(row[0])

    # ---------- legacy baseline/outbox facade ----------
    def record_baseline(self, work: str, season: int, episode: int, infohash: str) -> None:
        with self.conn:
            self.conn.execute("""INSERT OR REPLACE INTO baseline(work, season, episode, infohash, recorded_at)
                               VALUES (?, ?, ?, ?, ?)""", (_text(work), season, episode, _text(infohash), time.time()))

    def baseline_max_episode(self, work: str, season: int) -> Optional[int]:
        row = self.conn.execute("SELECT MAX(episode) FROM baseline WHERE work=? AND season=?", (_text(work), season)).fetchone()
        return row[0] if row and row[0] is not None else None

    def in_baseline(self, work: str, season: int, episode: int, infohash: str) -> bool:
        row = self.conn.execute("""SELECT 1 FROM baseline WHERE work=? AND season=? AND episode=? AND infohash=?""", (_text(work), season, episode, _text(infohash))).fetchone()
        return row is not None

    def enqueue_notification(self, ep_id: int, message: str) -> int:
        key = f"episode:{ep_id}:message:{sanitize_text(message)}"
        now = time.time()
        with self.conn:
            self.conn.execute("""INSERT OR IGNORE INTO notifications(
                episode_id, dedupe_key, message, delivery_state, state, attempts,
                created_at, updated_at
            ) VALUES (?, ?, ?, 'delivery_pending', 'delivery_pending', 0, ?, ?)""", (ep_id, key, sanitize_text(message), now, now))
        row = self.conn.execute("SELECT id FROM notifications WHERE dedupe_key=?", (key,)).fetchone()
        return int(row[0])

    def pending_notifications(self) -> List[Dict[str, Any]]:
        rows = self.conn.execute("""SELECT id, episode_id, message, delivery_state, state, attempts,
                                          provider_id, notify_id FROM notifications
                                   WHERE delivery_state != 'notified' ORDER BY id""").fetchall()
        return [{"id": int(row[0]), "episode_id": int(row[1]), "message": row[2], "state": row[3] or row[4], "attempts": int(row[5]), "provider_id": row[6], "notify_id": row[7]} for row in rows]

    def mark_notification(self, outbox_id: int, state: str, notify_id: Optional[str] = None) -> None:
        safe_state = sanitize_text(state)
        with self.conn:
            self.conn.execute("""UPDATE notifications SET delivery_state=?, state=?, notify_id=?, provider_id=?,
                                      attempts=attempts+1, updated_at=? WHERE id=?""", (safe_state, safe_state, _text_or_none(notify_id), _text_or_none(notify_id), time.time(), outbox_id))

    def close(self) -> None:
        if self._owns_connection:
            self.conn.close()

    # ---------- internal helpers ----------
    def _update_episode(self, episode_id: int, updates: Mapping[str, Any]) -> None:
        safe_updates = {key: value for key, value in updates.items() if key in self._EPISODE_UPDATE_COLUMNS}
        if not safe_updates:
            return
        columns = ", ".join(f"{key}=?" for key in safe_updates)
        self.conn.execute(f"UPDATE episodes_v5 SET {columns} WHERE id=?", (*safe_updates.values(), episode_id))
        if self.conn.execute("SELECT changes()").fetchone()[0] != 1:
            raise KeyError(f"episode {episode_id} not found")

    def _insert_history(self, episode_id: int, from_status: str | None, to_status: str, detail: Mapping[str, Any] | str | None, *, secrets: Iterable[Any] = ()) -> None:
        safe_detail = _json_text(detail or {}, secrets=secrets)
        self.conn.execute("""INSERT INTO stage_history(
            episode_id, from_status, to_status, stage, detail, detail_json, created_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?)""", (episode_id, from_status, to_status, to_status, safe_detail, safe_detail, time.time()))

    def _compat_episode(self, row: Mapping[str, Any] | None) -> Optional[Dict[str, Any]]:
        if row is None:
            return None
        result = dict(row)
        candidate = self.conn.execute("""SELECT infohash, source_id, source_url, title
                                         FROM candidates WHERE episode_id=?
                                         ORDER BY selected DESC, id LIMIT 1""", (result["id"],)).fetchone()
        result["infohash"] = candidate[0] if candidate else ""
        result["source_id"] = result.get("source_id") or (candidate[1] if candidate else "")
        result["source_url"] = result.get("source_url") or (candidate[2] if candidate else "")
        result["title"] = result.get("title") or (candidate[3] if candidate else "")
        result["work"] = result.get("canonical_work") or ""
        result["stage"] = result.get("status")
        result["stage_updated_at"] = result.get("updated_at")
        return result


def _subscription_values(subscription: Subscription | Mapping[str, Any] | None, fields: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(subscription, Subscription):
        values = {"subscription_id": subscription.subscription_id, "media_source": subscription.media_source, "media_id": subscription.media_id, "work": subscription.work, "aliases": subscription.aliases, "season": subscription.season, "include": subscription.include, "exclude": subscription.exclude}
    elif isinstance(subscription, Mapping):
        values = dict(subscription)
    else:
        values = {}
    values.update(fields)
    return values


def _episode_values(episode: EpisodeKey | Mapping[str, Any] | None, fields: Mapping[str, Any]) -> dict[str, Any]:
    if isinstance(episode, EpisodeKey):
        values: dict[str, Any] = {"key": episode}
    elif isinstance(episode, Mapping):
        values = dict(episode)
    else:
        values = {}
    values.update(fields)
    return values


def _episode_key(values: Mapping[str, Any]) -> EpisodeKey:
    key = values.get("key") or values.get("episode_key")
    if isinstance(key, EpisodeKey):
        return key
    if isinstance(key, Mapping):
        values = {**values, **key}
    return EpisodeKey(_required_text(values.get("media_source") or "legacy", "media_source"), _required_text(values.get("media_id"), "media_id"), _int_or_zero(values.get("season")), max(1, _int_or_zero(values.get("episode"))))


def _status_value(value: Any) -> str:
    return value.value if isinstance(value, EpisodeStatus) else sanitize_text(value)


def _reason_value(value: Any) -> str:
    return value.value if isinstance(value, ReviewReason) else sanitize_text(value)


def _allowed_episode_updates(
    values: Mapping[str, Any], *, secrets: Iterable[Any] = ()
) -> dict[str, Any]:
    result = dict(values)
    if "stage" in result and "status" not in result:
        result["status"] = _status_value(result.pop("stage"))
    if "last_error" in result:
        result["last_error"] = sanitize_text(result["last_error"], secrets=secrets)
    for key in ("cloud_path", "strm_path"):
        if key in result:
            result[key] = sanitize_text(result[key], secrets=secrets)
    if "review_detail" in result:
        result["review_detail"] = _json_text(result["review_detail"], secrets=secrets)
    if "runtime_identity_json" in result:
        result["runtime_identity_json"] = _json_text(
            result["runtime_identity_json"], secrets=secrets
        )
    if "aliases_json" in result:
        result["aliases_json"] = _json_text(result["aliases_json"])
    return result


def _select_candidate_in_transaction(
    connection: sqlite3.Connection,
    episode_id: int,
    candidate_id: int,
) -> None:
    if type(candidate_id) is not int:
        raise ValueError("selected_candidate_id must be an integer")
    row = connection.execute(
        "SELECT episode_id FROM candidates WHERE id=?", (candidate_id,)
    ).fetchone()
    if row is None or int(row[0]) != int(episode_id):
        raise ValueError("selected candidate does not belong to the reviewed episode")
    connection.execute(
        "UPDATE candidates SET selected=0, selected_at=NULL WHERE episode_id=?",
        (episode_id,),
    )
    connection.execute(
        "UPDATE candidates SET selected=1, selected_at=? WHERE id=?",
        (time.time(), candidate_id),
    )


def _select_episode_file_in_transaction(
    connection: sqlite3.Connection,
    episode_id: int,
    selected_file_id: Any,
) -> dict[str, Any]:
    if isinstance(selected_file_id, bool) or selected_file_id in (None, ""):
        raise ValueError("selected_file_id must not be empty")
    selected_text = str(selected_file_id)
    rows = connection.execute(
        """SELECT df.*, d.infohash
           FROM episode_sources es
           JOIN download_files df ON df.id=es.download_file_id
           JOIN downloads d ON d.id=df.download_id
           WHERE es.episode_id=?
             AND (df.remote_file_id=? OR CAST(df.id AS TEXT)=?)
           ORDER BY df.id""",
        (episode_id, selected_text, selected_text),
    ).fetchall()
    if len(rows) != 1:
        raise ValueError("selected file is not mapped to the reviewed episode")
    connection.execute(
        "UPDATE episode_sources SET selected_primary=0 WHERE episode_id=?",
        (episode_id,),
    )
    connection.execute(
        "UPDATE episode_sources SET selected_primary=1 WHERE episode_id=? AND download_file_id=?",
        (episode_id, int(rows[0][0])),
    )
    return dict(rows[0])


def _identity_updates(values: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {"media_source", "media_id", "season", "episode"}
    unknown = set(values) - allowed
    if unknown:
        raise ValueError("identity contains unknown fields")
    updates: dict[str, Any] = {}
    for key in ("media_source", "media_id"):
        if key in values:
            value = values[key]
            if type(value) is not str or not value or value != value.strip():
                raise ValueError(f"{key} must be a non-empty trimmed string")
            if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
                raise ValueError(f"{key} contains control characters")
            updates[key] = value
    for key, minimum in (("season", 0), ("episode", 1)):
        if key in values:
            value = values[key]
            if type(value) is not int or value < minimum:
                raise ValueError(f"{key} is outside the valid range")
            updates[key] = value
    if set(updates) & {"media_id", "season", "episode"} != {"media_id", "season", "episode"}:
        raise ValueError("identity requires media_id, season, and episode")
    return updates


def _runtime_identity_updates(values: Mapping[str, Any]) -> dict[str, Any]:
    allowed = {"task_id", "task_name", "source_root", "output_root"}
    unknown = set(values) - allowed
    if unknown:
        raise ValueError("runtime identity contains unknown fields")
    updates: dict[str, Any] = {}
    for key, value in values.items():
        if type(value) is not str or not value or value != value.strip():
            raise ValueError(f"{key} must be a non-empty trimmed string")
        if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
            raise ValueError(f"{key} contains control characters")
        updates[key] = value
    if "task_id" not in updates:
        raise ValueError("runtime identity requires task_id")
    return updates


def _parse_json_mapping(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return dict(parsed) if isinstance(parsed, Mapping) else {}
    return {}


def _required_text(value: Any, field: str) -> str:
    text = _text(value)
    if not text:
        raise ValueError(f"{field} must not be empty")
    return text


def _text(value: Any) -> str:
    return sanitize_text("" if value is None else value)


def _text_or_none(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    return _int_or_none(value) or 0


def _float_or_none(value: Any) -> float | None:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


_UNSAFE_TRAIT = object()
_SAFE_SCORE_COMPONENTS = frozenset({
    "include", "language", "release_group", "resolution", "collection_penalty",
})
_SAFE_SCORE_MATCHES = frozenset({
    "include", "language", "release_group", "resolution", "collection",
})


def _safe_candidate_trait_value(value: Any, trait: str) -> Any:
    """Keep only scalar normalized traits and known score breakdowns."""

    if value is None or isinstance(value, (str, int, float, bool)):
        return value
    if trait == "languages" and isinstance(value, (list, tuple, set)):
        if all(isinstance(item, (str, int, float, bool)) for item in value):
            return list(value)
        return _UNSAFE_TRAIT
    if trait in {"score_components", "score_matches"} and isinstance(value, Mapping):
        allowed = (
            _SAFE_SCORE_COMPONENTS if trait == "score_components" else _SAFE_SCORE_MATCHES
        )
        result = {
            str(key): item
            for key, item in value.items()
            if str(key) in allowed and isinstance(item, (str, int, float, bool))
        }
        return result
    return _UNSAFE_TRAIT


def _safe_candidate_traits(values: Mapping[str, Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for trait, value in values.items():
        if trait not in _SAFE_CANDIDATE_TRAIT_NAMES:
            continue
        safe_value = _safe_candidate_trait_value(value, trait)
        if safe_value is not _UNSAFE_TRAIT:
            result[trait] = safe_value
    return result


def _json_text(value: Any, *, secrets: Iterable[Any] = ()) -> str:
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            text = json.dumps(str(value), ensure_ascii=False)
    return sanitize_text(text, secrets=secrets)


def _parse_json_mapping(value: Any) -> dict[str, Any]:
    """Decode a stored JSON object while tolerating legacy malformed traits."""

    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {}
    return dict(parsed) if isinstance(parsed, Mapping) else {}


__all__ = ["LockHeld", "ProcessLock", "SPECIAL", "STAGES", "StateStore"]
