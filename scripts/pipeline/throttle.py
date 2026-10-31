"""Account-level 115 throttle pacing shared by every cloud-reading stage.

115 rate limits are account-wide: one episode's transport failure must cool
down every other stage's reads too, and a rate-limited read must not spend
the episode's contradiction review budget.  State lives in one shared
:class:`CloudThrottleState` per runner process; across runs the per-episode
persisted ``next_retry_at`` continues the pacing and the per-run cloud-check
budget bounds how many reads can reach 115 before the state re-arms.
"""
from __future__ import annotations

from collections.abc import Callable, Mapping
from dataclasses import dataclass
from typing import Any

from .http import retry_delay

THROTTLE_BASE_SECONDS = 120.0
THROTTLE_CAP_SECONDS = 1800.0
THROTTLE_MAX_ATTEMPTS = 20


class CloudThrottleState:
    """Account-level cooldown shared by every 115-reading stage in one run.

    ``consecutive`` counts transport-level 115 failures since the last
    successful 115 stage; only this counter escalates to the sustained-rate
    review, so mixed histories can never fake "consecutive" throttling.
    """

    def __init__(self) -> None:
        self.throttled_until: float | None = None
        self.consecutive: int = 0

    def active(self, now: float) -> bool:
        return self.throttled_until is not None and now < self.throttled_until

    def renew(self, until: float) -> None:
        self.throttled_until = max(self.throttled_until or 0.0, until)
        self.consecutive += 1

    def reset(self) -> None:
        self.throttled_until = None
        self.consecutive = 0


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
    "ThrottleDecision",
    "THROTTLE_BASE_SECONDS",
    "THROTTLE_CAP_SECONDS",
    "THROTTLE_MAX_ATTEMPTS",
    "throttle_decision",
]
