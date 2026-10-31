"""Bounded coordinator for V5 quarter-resource preparation.

All external reads and writes are supplied by ``ports`` and callbacks.  This
module does not perform network, filesystem, database, or lock operations.
"""
from __future__ import annotations

from collections.abc import Mapping
import math
import re
import time
import unicodedata
from typing import Any


RESOURCE_KEYS = (
    "cloud_quarter",
    "cloud_source",
    "cloud_organized",
    "raw_dir",
    "published_dir",
    "litepan_task",
    "litepan_rule",
    "emby_library",
)

# These are the only resource-derived config values approved by the current
# NAS contract.  In particular, base directories and retired publish fields
# must never be restored by this coordinator.
CONFIG_FIELDS = frozenset({
    "paths.cloud_source_root",
    "runtime.litepan_task_name",
    "runtime.litepan_task_source_root",
    "runtime.litepan_task_output_root",
    "runtime.litepan_event_name",
    "runtime.emby_library_name",
    "runtime.emby_library_root",
})

_STATE_VALUES = frozenset({"reuse", "create", "conflict", "unknown"})
_SAFE_CODE = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_QUARTER = re.compile(r"^[0-9]{4}-(01|04|07|10)$")
_MAX_CREATIONS = 8

_BLOCK_CODES = frozenset({
    "resource_blocked",
    "qualification_blocked",
    "qualification_invalid",
    "authorization_required",
    "request_invalid",
    "layout_invalid",
    "quarter_invalid",
    "budget_invalid",
    "budget_exhausted",
    "clock_failed",
    "inventory_failed",
    "inventory_shape_invalid",
    "inventory_unknown",
    "resource_conflict",
    "capability_check_failed",
    "capability_shape_invalid",
    "creation_capabilities_blocked",
    "port_unavailable",
    "journal_failed",
    "creation_limit_reached",
    "creation_uncertain",
    "inventory_incomplete_after_create",
    "resources_not_reused",
    "configuration_patch_failed",
    "configuration_patch_invalid",
    "configuration_commit_failed",
})


class ResourceBlocked(RuntimeError):
    """A safe, fixed-code resource coordinator failure."""

    def __init__(self, code: str):
        self.code = code if isinstance(code, str) and code in _BLOCK_CODES else "resource_blocked"
        super().__init__(self.code)


def coordinate_resources(
    *,
    layout: dict[str, Any],
    qualification: dict[str, Any],
    ports: Any,
    apply: bool,
    authorized: bool,
    journal: Any,
    commit_config: Any,
    clock=time.monotonic,
    max_seconds: float = 150,
) -> dict[str, Any]:
    """Plan or prepare one quarter's eight resources through injected ports.

    ``journal`` is called with one mapping containing only ``key``, ``action``
    and ``outcome``.  Resource events use a ``RESOURCE_KEYS`` value; the final
    preparation event uses the fixed sentinel key ``pipeline``.
    """

    resources = {key: "unknown" for key in RESOURCE_KEYS}
    resource_reasons: dict[str, str] = {}
    created_keys: set[str] = set()
    uncertain_create = False
    partial_effect = False
    capability_blockers: list[str] = []

    def result(status: str, reason: str | None = None) -> dict[str, Any]:
        reused = sum(
            1 for key, state in resources.items()
            if state == "reuse" and key not in created_keys
        )
        return {
            "status": status,
            "reason": reason,
            "partial": False if status in {"noop", "plan_only", "prepared"}
            else bool(partial_effect or created_keys or uncertain_create),
            "counts": {"created": len(created_keys), "reused": reused},
            "resources": dict(resources),
            "resource_reasons": dict(resource_reasons),
            "blockers": list(capability_blockers),
        }

    def blocked(code: str) -> dict[str, Any]:
        safe_code = code if code in _BLOCK_CODES else "resource_blocked"
        return result("blocked", safe_code)

    # These two gates have no observable port or journal calls by design.
    if not isinstance(qualification, Mapping) or qualification.get("status") != "ok":
        return blocked("qualification_blocked")
    qualification_counts = qualification.get("counts")
    eligible_count = (
        qualification_counts.get("eligible")
        if isinstance(qualification_counts, Mapping) else None
    )
    if isinstance(eligible_count, bool) or not isinstance(eligible_count, int) or eligible_count < 0:
        return blocked("qualification_invalid")
    if eligible_count == 0:
        return result("noop")

    if type(apply) is not bool or type(authorized) is not bool:
        return blocked("request_invalid")
    if apply and not authorized:
        return blocked("authorization_required")
    if not isinstance(layout, dict):
        return blocked("layout_invalid")
    quarter = layout.get("quarter")
    if not isinstance(quarter, str) or not _QUARTER.fullmatch(quarter):
        return blocked("quarter_invalid")
    try:
        year = int(quarter[:4])
        if year < 1:
            return blocked("quarter_invalid")
    except (TypeError, ValueError):
        return blocked("quarter_invalid")
    if isinstance(max_seconds, bool) or not isinstance(max_seconds, (int, float)):
        return blocked("budget_invalid")
    try:
        budget_limit = float(max_seconds)
    except (OverflowError, TypeError, ValueError):
        return blocked("budget_invalid")
    if not math.isfinite(budget_limit) or budget_limit <= 0:
        return blocked("budget_invalid")
    if not callable(clock):
        return blocked("clock_failed")
    if not callable(getattr(ports, "inventory", None)):
        return blocked("port_unavailable")
    if not callable(getattr(ports, "creation_capabilities", None)):
        return blocked("port_unavailable")
    if not callable(getattr(ports, "create", None)):
        return blocked("port_unavailable")
    if apply and not callable(getattr(ports, "config_patch", None)):
        return blocked("port_unavailable")
    if apply and (not callable(journal) or not callable(commit_config)):
        return blocked("request_invalid")

    try:
        started = float(clock())
    except Exception:
        return blocked("clock_failed")
    if not math.isfinite(started):
        return blocked("clock_failed")

    def budget_code() -> str | None:
        try:
            now = float(clock())
        except Exception:
            return "clock_failed"
        if not math.isfinite(now) or now < started:
            return "clock_failed"
        return "budget_exhausted" if now - started >= budget_limit else None

    def take_inventory() -> tuple[dict[str, str] | None, str | None]:
        code = budget_code()
        if code:
            return None, code
        try:
            snapshot = ports.inventory(layout)
        except ResourceBlocked as exc:
            resources.update({key: "unknown" for key in RESOURCE_KEYS})
            return None, exc.code
        except Exception:
            resources.update({key: "unknown" for key in RESOURCE_KEYS})
            return None, "inventory_failed"
        normalized = _normalize_inventory(snapshot)
        if normalized is None:
            resources.update({key: "unknown" for key in RESOURCE_KEYS})
            return None, "inventory_shape_invalid"
        resources.update(normalized)
        resource_reasons.clear()
        resource_reasons.update({key: row["reason"] for key, row in snapshot.items() if "reason" in row})
        code = budget_code()
        return normalized, code

    def take_capabilities() -> tuple[list[str] | None, str | None]:
        code = budget_code()
        if code:
            return None, code
        try:
            values = ports.creation_capabilities(layout)
        except ResourceBlocked as exc:
            return None, exc.code
        except Exception:
            return None, "capability_check_failed"
        normalized = _normalize_capabilities(values)
        if normalized is None:
            return None, "capability_shape_invalid"
        code = budget_code()
        return normalized, code

    snapshot, code = take_inventory()
    if code:
        return blocked(code)
    if snapshot is None:
        return blocked("inventory_shape_invalid")

    capability_blockers, code = take_capabilities()
    if code:
        return blocked(code)
    if capability_blockers is None:
        return blocked("capability_shape_invalid")

    code = _snapshot_block_code(snapshot)
    if code:
        return blocked(code)

    if not apply:
        needs_creation = any(state == "create" for state in snapshot.values())
        reason = (
            "creation_capabilities_blocked" if capability_blockers
            else "resources_need_creation" if needs_creation
            else None
        )
        return result("plan_only", reason)

    if capability_blockers:
        return blocked("creation_capabilities_blocked")

    for key in RESOURCE_KEYS:
        if snapshot[key] == "reuse":
            continue
        if snapshot[key] != "create":
            return blocked("inventory_unknown")

        # Refresh all eight rows immediately before each creation, so a new
        # conflict or incomplete inventory prevents the next write.
        snapshot, code = take_inventory()
        if code:
            return blocked(code)
        if snapshot is None:
            return blocked("inventory_shape_invalid")
        code = _snapshot_block_code(snapshot)
        if code:
            return blocked(code)
        if snapshot[key] == "reuse":
            continue
        if snapshot[key] != "create":
            return blocked("inventory_unknown")

        if len(created_keys) + int(uncertain_create) >= _MAX_CREATIONS:
            return blocked("creation_limit_reached")
        try:
            journal({"key": key, "action": "create", "outcome": "intent"})
        except Exception:
            return blocked("journal_failed")
        code = budget_code()
        if code:
            return blocked(code)

        create_failed = False
        try:
            ports.create(key, layout)
        except Exception:
            # A timeout may have created the resource.  Always read back before
            # deciding, and never retry this create call.
            create_failed = True

        uncertain_create = True
        partial_effect = True
        # The post-create inventory is mandatory even when the create call
        # timed out or raised; it is the only safe recovery signal.
        try:
            readback = ports.inventory(layout)
        except Exception:
            resources.update({resource_key: "unknown" for resource_key in RESOURCE_KEYS})
            try:
                journal({"key": key, "action": "create", "outcome": "uncertain"})
            except Exception:
                pass
            return blocked("inventory_incomplete_after_create")
        normalized = _normalize_inventory(readback)
        if normalized is None:
            resources.update({resource_key: "unknown" for resource_key in RESOURCE_KEYS})
            try:
                journal({"key": key, "action": "create", "outcome": "uncertain"})
            except Exception:
                pass
            return blocked("inventory_incomplete_after_create")
        resources.update(normalized)
        resource_reasons.clear()
        resource_reasons.update({key: row["reason"] for key, row in readback.items() if "reason" in row})
        code = _snapshot_block_code(normalized)
        if normalized[key] == "reuse":
            created_keys.add(key)
            uncertain_create = False
            outcome = "recovered" if create_failed else "verified"
        else:
            outcome = "uncertain"
        try:
            journal({"key": key, "action": "create", "outcome": outcome})
        except Exception:
            return blocked("journal_failed")
        code_after_readback = budget_code()
        if normalized[key] != "reuse":
            return blocked(code or "creation_uncertain")
        if code:
            return blocked(code)
        if code_after_readback:
            return blocked(code_after_readback)

    code = budget_code()
    if code:
        return blocked(code)
    final_snapshot, code = take_inventory()
    if code:
        return blocked(code)
    if final_snapshot is None:
        return blocked("inventory_shape_invalid")
    code = _snapshot_block_code(final_snapshot)
    if code:
        return blocked(code)
    if any(state != "reuse" for state in final_snapshot.values()):
        return blocked("resources_not_reused")

    code = budget_code()
    if code:
        return blocked(code)
    try:
        patch = ports.config_patch(layout)
    except ResourceBlocked as exc:
        return blocked(exc.code)
    except Exception:
        return blocked("configuration_patch_failed")
    code = budget_code()
    if code:
        return blocked(code)
    if not _valid_config_patch(patch):
        return blocked("configuration_patch_invalid")

    code = budget_code()
    if code:
        return blocked(code)
    try:
        commit_config(dict(patch))
    except Exception:
        partial_effect = True
        return blocked("configuration_commit_failed")
    partial_effect = True
    code = budget_code()
    if code:
        return blocked(code)
    try:
        journal({"key": "pipeline", "action": "complete", "outcome": "prepared"})
    except Exception:
        return blocked("journal_failed")
    code = budget_code()
    if code:
        return blocked(code)
    partial_effect = False
    return result("prepared")


def _normalize_inventory(value: Any) -> dict[str, str] | None:
    try:
        if not isinstance(value, Mapping) or set(value.keys()) != set(RESOURCE_KEYS):
            return None
        normalized: dict[str, str] = {}
        for key in RESOURCE_KEYS:
            row = value.get(key)
            if not isinstance(row, Mapping) or not set(row.keys()).issubset({"state", "reason"}):
                return None
            state = row.get("state")
            if not isinstance(state, str) or state not in _STATE_VALUES:
                return None
            if "reason" in row:
                reason = row["reason"]
                if not isinstance(reason, str) or not _SAFE_CODE.fullmatch(reason):
                    return None
            normalized[key] = state
        return normalized
    except Exception:
        return None


def _normalize_capabilities(value: Any) -> list[str] | None:
    if not isinstance(value, list):
        return None
    if any(not isinstance(item, str) or not _SAFE_CODE.fullmatch(item) for item in value):
        return None
    return sorted(set(value))


def _snapshot_block_code(snapshot: Mapping[str, str]) -> str | None:
    if any(snapshot.get(key) == "unknown" for key in RESOURCE_KEYS):
        return "inventory_unknown"
    if any(snapshot.get(key) == "conflict" for key in RESOURCE_KEYS):
        return "resource_conflict"
    return None


def _valid_config_patch(value: Any) -> bool:
    try:
        if not isinstance(value, Mapping) or not set(value.keys()).issubset(CONFIG_FIELDS):
            return False
        for key, item in value.items():
            if not isinstance(key, str) or type(item) is not str or not item.strip():
                return False
            if "://" in item or any(unicodedata.category(char) == "Cc" for char in item):
                return False
        return True
    except Exception:
        return False


__all__ = ["CONFIG_FIELDS", "RESOURCE_KEYS", "ResourceBlocked", "coordinate_resources"]
