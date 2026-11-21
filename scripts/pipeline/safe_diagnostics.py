"""Strict projections for inventory and remote-service diagnostics.

These helpers deliberately do not inspect exception messages.  Only fixed
reason tokens, enumerated labels, bounded numeric counts, and explicitly
typed transport metadata can cross into durable state or CLI summaries.
"""
from __future__ import annotations

from collections.abc import Mapping
import re
from typing import Any


_COUNT_FIELDS = (
    "cloud_files",
    "raw_files",
    "published_files",
    "emby_episodes",
    "emby_series",
    "owned_files",
    "owned_episode_metadata",
    "owned_shared_metadata",
)
_STAGES = frozenset({
    "scope", "state", "ownership", "cloud", "raw", "published", "emby",
    "task_page", "listing_request", "directory_page", "response_envelope",
    "pre_submit_inventory", "download_reconcile", "download_inventory", "startup",
})
_CATEGORIES = frozenset({
    "rate_limit", "transport", "authentication", "inventory_incomplete",
})
_KINDS = frozenset({"directories", "files", "tasks", "remote"})
_TYPE_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]{0,79}$", re.ASCII)
_CODE_RE = re.compile(r"^[0-9]{1,12}$", re.ASCII)
_INVENTORY_STAGES = frozenset({
    "scope", "state", "ownership", "cloud", "raw", "published", "emby",
})
_INVENTORY_REASONS = frozenset({
    "submission_inventory_present",
    "submission_inventory_missing",
    "submission_inventory_ownership_unknown",
    *(f"submission_inventory_{name}_unknown" for name in _INVENTORY_STAGES),
})
_BLOCKER_REASONS = frozenset({
    "pre_submit_inventory_present",
    "pre_submit_inventory_unknown",
    "pre_submit_inventory_error",
    "download_reconcile_failed",
    "download_inventory_failed",
    "download_inventory_incomplete",
})


def _enum(value: Any, allowed: frozenset[str]) -> str | None:
    return value if isinstance(value, str) and value in allowed else None


def _reason(value: Any, allowed: frozenset[str]) -> str | None:
    return value if isinstance(value, str) and value in allowed else None


def _exception_fields(exc: BaseException) -> dict[str, Any]:
    try:
        fields = vars(exc)
    except (TypeError, AttributeError):
        return {}
    return fields if isinstance(fields, dict) else {}


def _exception_value(exc: BaseException, fields: Mapping[str, Any], name: str) -> Any:
    if name in fields:
        return fields[name]
    # Read plain class constants without invoking descriptors or properties.
    for exception_class in type(exc).__mro__:
        value = exception_class.__dict__.get(name)
        if value is not None and not callable(value):
            return value
    return None


def _category(exc: BaseException, fields: Mapping[str, Any]) -> str | None:
    explicit = _enum(_exception_value(exc, fields, "category"), _CATEGORIES)
    if explicit is not None:
        return explicit
    # Compatibility for existing adapter exceptions that predate the explicit
    # diagnostic attributes.  New subclasses can override this with category.
    return {
        "RemoteServiceUnavailable": "transport",
        "RemoteAuthenticationError": "authentication",
        "RemoteRateLimited": "rate_limit",
        "RemoteInventoryIncomplete": "inventory_incomplete",
        "_Unknown": "inventory_incomplete",
        "TimeoutError": "transport",
        "ConnectionError": "transport",
    }.get(type(exc).__name__)


def _safe_http_status(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and 100 <= value <= 599:
        return value
    return None


def _safe_remote_code(value: Any) -> str | int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int) and 0 <= value <= 999_999:
        return value
    if isinstance(value, str) and _CODE_RE.fullmatch(value):
        return value
    return None


def safe_exception_diagnostics(
    exc: BaseException,
    *,
    stage: str | None = None,
) -> dict[str, Any]:
    """Return safe exception metadata without reading ``str(exc)`` or repr."""

    fields = _exception_fields(exc)
    result: dict[str, Any] = {}
    exception_type = type(exc).__name__
    if _TYPE_RE.fullmatch(exception_type):
        result["exception_type"] = exception_type
    category = _category(exc, fields)
    if category is not None:
        result["category"] = category
    safe_stage = _enum(_exception_value(exc, fields, "stage"), _STAGES) or _enum(stage, _STAGES)
    if safe_stage is not None:
        result["stage"] = safe_stage
    kind = _enum(_exception_value(exc, fields, "kind"), _KINDS)
    if kind is not None:
        result["kind"] = kind
    status = _safe_http_status(_exception_value(exc, fields, "http_status"))
    if status is None:
        status = _safe_http_status(_exception_value(exc, fields, "status"))
    if status is not None:
        result["http_status"] = status
    code = _safe_remote_code(_exception_value(exc, fields, "remote_code"))
    if code is not None:
        result["remote_code"] = code
    return result


def _safe_counts(value: Any) -> dict[str, int]:
    supplied = value if isinstance(value, Mapping) else {}
    result: dict[str, int] = {}
    for name in _COUNT_FIELDS:
        count = supplied.get(name)
        result[name] = (
            count if isinstance(count, int) and not isinstance(count, bool)
            and 0 <= count <= 2_147_483_647 else 0
        )
    return result


def _inventory_reason(value: Any, inventory_stage: str, state: str) -> str:
    candidate = _reason(value, _INVENTORY_REASONS)
    if candidate is not None:
        return candidate
    if state == "present":
        return "submission_inventory_present"
    if state == "missing":
        return "submission_inventory_missing"
    return f"submission_inventory_{inventory_stage}_unknown"


def safe_inventory_diagnostics(
    value: Any,
    *,
    stage: str = "scope",
) -> dict[str, Any]:
    """Project a submission inventory result onto fixed, non-sensitive fields."""

    detail = value if isinstance(value, Mapping) else {}
    state = _enum(detail.get("state"), frozenset({"present", "missing", "unknown"})) or "unknown"
    inventory_stage = (
        _enum(detail.get("inventory_stage"), _INVENTORY_STAGES)
        or _enum(stage, _INVENTORY_STAGES)
        or "scope"
    )
    result: dict[str, Any] = {
        "state": state,
        "reason": _inventory_reason(detail.get("reason"), inventory_stage, state),
        "inventory_stage": inventory_stage,
        "counts": _safe_counts(detail.get("counts")),
    }
    # Exception-stage labels are more specific than the inventory phase and
    # are kept separately from the fixed phase above.
    exception_stage = _enum(detail.get("stage"), _STAGES)
    if exception_stage is not None:
        result["stage"] = exception_stage
    category = _enum(detail.get("category"), _CATEGORIES)
    if category is not None:
        result["category"] = category
    exception_type = detail.get("exception_type")
    if isinstance(exception_type, str) and _TYPE_RE.fullmatch(exception_type):
        result["exception_type"] = exception_type
    kind = _enum(detail.get("kind"), _KINDS)
    if kind is not None:
        result["kind"] = kind
    status = _safe_http_status(detail.get("http_status"))
    if status is not None:
        result["http_status"] = status
    code = _safe_remote_code(detail.get("remote_code"))
    if code is not None:
        result["remote_code"] = code
    return result


def safe_blocker_details(detail: Any) -> dict[str, Any]:
    """Strictly project service detail for durable summaries and CLI output."""

    source = detail if isinstance(detail, Mapping) else {}
    result: dict[str, Any] = {}
    reason = _reason(source.get("reason"), _BLOCKER_REASONS | _INVENTORY_REASONS)
    if reason is not None:
        result["reason"] = reason
    subreason = _reason(source.get("subreason"), _INVENTORY_REASONS)
    if subreason is not None:
        result["subreason"] = subreason
    if isinstance(source.get("counts"), Mapping):
        result["counts"] = _safe_counts(source["counts"])
    category = _enum(source.get("category"), _CATEGORIES)
    if category is not None:
        result["category"] = category
    exception_type = source.get("exception_type")
    if isinstance(exception_type, str) and _TYPE_RE.fullmatch(exception_type):
        result["exception_type"] = exception_type
    for name in ("stage", "inventory_stage"):
        value = _enum(source.get(name), _STAGES if name == "stage" else _INVENTORY_STAGES)
        if value is not None:
            result[name] = value
    kind = _enum(source.get("kind"), _KINDS)
    if kind is not None:
        result["kind"] = kind
    status = _safe_http_status(source.get("http_status"))
    if status is not None:
        result["http_status"] = status
    code = _safe_remote_code(source.get("remote_code"))
    if code is not None:
        result["remote_code"] = code
    return result
