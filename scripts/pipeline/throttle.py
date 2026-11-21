"""Account-level 115 throttle pacing shared by every cloud-reading stage.

115 rate limits are account-wide: one episode's transport failure must cool
down every other stage's reads too, and a rate-limited read must not spend
the episode's contradiction review budget.  State is shared by the stage services and, for the writable CLI, persisted
under the existing V5 run lock. Across runs the account cooldown is checked
before cloud startup reads; per-episode ``next_retry_at`` and the per-run
cloud-check budget continue to bound reconciliation.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
import json
import math
import os
from pathlib import Path
import stat
import tempfile
import time
from dataclasses import dataclass
from typing import Any

from .http import retry_delay

THROTTLE_BASE_SECONDS = 120.0
THROTTLE_CAP_SECONDS = 1800.0
THROTTLE_MAX_ATTEMPTS = 20


class CloudCooldownStateError(RuntimeError):
    """Unknown account cooldown state must stop the writable round."""


class CloudThrottleState:
    """Account cooldown optionally persisted under the existing V5 run lock."""

    def __init__(self, path: str | None = None) -> None:
        self.path = Path(path) if path is not None else None
        self.throttled_until: float | None = None
        self.consecutive: int = 0
        if self.path is not None:
            self._load()

    def _load(self) -> None:
        try:
            fd = os.open(self.path, os.O_RDONLY | os.O_NOFOLLOW | os.O_NONBLOCK)
        except FileNotFoundError:
            return
        except OSError:
            raise CloudCooldownStateError("cloud_cooldown_state_unreadable") from None
        try:
            with os.fdopen(fd, "rb") as handle:
                info = os.fstat(handle.fileno())
                if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                        or info.st_size > 4096 or info.st_mode & 0o022):
                    raise ValueError()
                value = json.loads(handle.read(4097))
            if (not isinstance(value, dict) or set(value) != {"version", "throttled_until", "consecutive"}
                    or type(value["version"]) is not int or value["version"] != 1
                    or type(value["consecutive"]) is not int or not 0 <= value["consecutive"] <= 1000000):
                raise ValueError()
            until = value["throttled_until"]
            if until is not None and (type(until) not in (int, float)
                    or not math.isfinite(until) or until < 0 or until > time.time() + THROTTLE_CAP_SECONDS * 2):
                raise ValueError()
            self.throttled_until, self.consecutive = until, value["consecutive"]
        except (OSError, ValueError, TypeError, KeyError):
            raise CloudCooldownStateError("cloud_cooldown_state_unreadable") from None

    def _save(self) -> None:
        if self.path is None:
            return
        temporary = None
        try:
            if self.path.is_symlink():
                raise OSError()
            if self.path.exists():
                info = self.path.stat(follow_symlinks=False)
                if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1 or info.st_mode & 0o022:
                    raise OSError()
            fd, temporary = tempfile.mkstemp(prefix=".cloud-cooldown-", dir=self.path.parent)
            with os.fdopen(fd, "w") as handle:
                os.fchmod(handle.fileno(), 0o600)
                json.dump({"version": 1, "throttled_until": self.throttled_until,
                           "consecutive": self.consecutive}, handle, allow_nan=False)
                handle.flush()
                os.fsync(handle.fileno())
            os.replace(temporary, self.path)
            temporary = None
            directory = os.open(self.path.parent, os.O_RDONLY | os.O_DIRECTORY)
            try:
                os.fsync(directory)
            finally:
                os.close(directory)
        except (OSError, ValueError, TypeError):
            raise CloudCooldownStateError("cloud_cooldown_state_unwritable") from None
        finally:
            if temporary is not None:
                try:
                    os.unlink(temporary)
                except OSError:
                    pass

    def active(self, now: float) -> bool:
        return self.throttled_until is not None and now < self.throttled_until

    def renew(self, until: float) -> None:
        self.throttled_until = max(self.throttled_until or 0.0, until)
        self.consecutive += 1
        self._save()

    def reset(self) -> None:
        changed = self.throttled_until is not None or self.consecutive != 0
        self.throttled_until = None
        self.consecutive = 0
        if changed:
            self._save()


@dataclass(frozen=True, slots=True)
class ThrottleDecision:
    """Outcome of one rate-limited read against the shared state."""

    consecutive: int
    next_retry_at: float
    escalated: bool
    detail: Mapping[str, Any]


def throttle_decision(
    detail: Mapping[str, Any],
    *,
    state: CloudThrottleState,
    timestamp: float,
    jitter_seconds: float = 0.0,
    base: float = THROTTLE_BASE_SECONDS,
    cap: float = THROTTLE_CAP_SECONDS,
    max_attempts: int = THROTTLE_MAX_ATTEMPTS,
    random_source: Callable[[], float] | None = None,
) -> ThrottleDecision:
    """Classify one rate-limited read and extend the account cooldown.

    Escalates to the sustained-rate review only after ``max_attempts``
    consecutive transport failures (hours with the default ladder); the
    caller performs the actual store transition or review with the returned
    enriched detail.
    """

    count = state.consecutive + 1
    delay = retry_delay(
        {"backoff_seconds": base, "backoff_max_seconds": cap, "jitter_seconds": jitter_seconds},
        count,
        random_source,
    )
    state.renew(timestamp + delay)
    if count >= max_attempts:
        escalated_detail = dict(detail)
        escalated_detail.update({
            "reason": "rate_limit_sustained",
            "attempts": count,
            "last_reason": detail.get("reason") or "rate_limited",
        })
        return ThrottleDecision(
            consecutive=count,
            next_retry_at=timestamp,
            escalated=True,
            detail=escalated_detail,
        )
    return ThrottleDecision(
        consecutive=count,
        next_retry_at=timestamp + delay,
        escalated=False,
        detail=detail,
    )


__all__ = [
    "CloudThrottleState",
    "CloudCooldownStateError",
    "ThrottleDecision",
    "THROTTLE_BASE_SECONDS",
    "THROTTLE_CAP_SECONDS",
    "THROTTLE_MAX_ATTEMPTS",
    "throttle_decision",
]
