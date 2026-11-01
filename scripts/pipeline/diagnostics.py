"""Read-only command timing with fixed labels and a finite wall-clock budget.

Never accept request URLs, parameters, response bodies or exception messages.
The context is enabled only at explicitly read-only CLI boundaries.
"""
from __future__ import annotations

from contextlib import contextmanager
from contextvars import ContextVar
from dataclasses import dataclass
import json
import math
import signal
import sys
import threading
import time


READ_ONLY_BUDGET_SECONDS = 150.0
LABELS = frozenset({
    "preflight", "dry-run", "qualify", "plan-resources", "config", "runtime", "state.snapshot", "discovery",
    "litepan.tasks", "115.initialize", "115.capabilities", "115.source_root",
    "115.quarter_library_root", "115.request", "emby.library",
    "moviepilot.subscriptions", "moviepilot.media_detail", "moviepilot.request", "moviepilot.season_episodes",
    "http.attempt", "source.feed", "source.history", "qualification", "resources.inventory",
})


class ReadOnlyDeadline(KeyboardInterrupt):
    """Bypass adapter exception handlers; never turn a deadline into empty data."""


@dataclass
class _Context:
    command: str
    started: float
    deadline: float
    current_stage: str


_active: ContextVar[_Context | None] = ContextVar("read_only_diagnostics", default=None)


def _emit(context: _Context, label: str, event: str, **facts: object) -> None:
    # Only numeric facts survive. No arbitrary adapter strings reach output.
    safe = {key: value for key, value in facts.items()
            if key in {"duration_s", "attempt", "timeout_s", "status"}
            and isinstance(value, (int, float)) and not isinstance(value, bool)
            and math.isfinite(value)}
    print(json.dumps({"diagnostic": "read_only", "command": context.command,
                      "stage": label, "event": event,
                      "elapsed_s": round(time.monotonic() - context.started, 3),
                      **safe}), file=sys.stderr, flush=True)


def request_timeout(configured: float | None) -> float | None:
    context = _active.get()
    if context is None:
        return configured
    remaining = context.deadline - time.monotonic()
    if remaining <= 0:
        raise ReadOnlyDeadline()
    return min(float(configured), remaining) if configured is not None else remaining


@contextmanager
def stage(label: str, **facts: object):
    if label not in LABELS:
        raise ValueError("unknown diagnostic stage")
    context = _active.get()
    if context is None:
        yield
        return
    request_timeout(None)
    previous = context.current_stage
    context.current_stage = label
    started = time.monotonic()
    _emit(context, label, "start", **facts)
    try:
        yield
    except ReadOnlyDeadline:
        _emit(context, label, "deadline", duration_s=round(time.monotonic()-started, 3))
        raise
    except BaseException:
        _emit(context, label, "error", duration_s=round(time.monotonic()-started, 3))
        raise
    else:
        _emit(context, label, "end", duration_s=round(time.monotonic()-started, 3))
    finally:
        context.current_stage = previous


@contextmanager
def read_only_command(command: str):
    if command not in {"preflight", "dry-run", "qualify", "plan-resources"}:
        raise ValueError("unsupported diagnostic command")
    started = time.monotonic()
    context = _Context(command, started, started + READ_ONLY_BUDGET_SECONDS, command)
    token = _active.set(context)
    use_alarm = (hasattr(signal, "setitimer")
                 and threading.current_thread() is threading.main_thread())
    old_handler = old_timer = None
    def expired(_signum, _frame):
        raise ReadOnlyDeadline()
    try:
        if use_alarm:
            old_handler = signal.getsignal(signal.SIGALRM)
            signal.signal(signal.SIGALRM, expired)
            old_timer = signal.setitimer(signal.ITIMER_REAL, READ_ONLY_BUDGET_SECONDS)
        with stage(command):
            yield
    finally:
        if use_alarm and old_timer is not None:
            signal.setitimer(signal.ITIMER_REAL, 0)
            signal.signal(signal.SIGALRM, old_handler)
            if old_timer[0] > 0:
                signal.setitimer(signal.ITIMER_REAL,
                                 max(0.001, old_timer[0] - (time.monotonic()-started)),
                                 old_timer[1])
        _active.reset(token)
