"""Fail-closed identities for one qualified current-quarter execution."""
from __future__ import annotations

from collections.abc import Mapping
from datetime import date
import re
from typing import Any


class QualifiedScopeError(ValueError):
    pass


def _positive(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str) and value.isascii() and value.isdecimal():
        number = int(value)
        return number if number > 0 else None
    return None


def canonical_identity(row: Mapping[str, Any]) -> tuple[str, str, int] | None:
    source = row.get("media_source")
    if not isinstance(source, str) or source.strip().lower() not in {"tmdb", "themoviedb"}:
        return None
    media_id, season = _positive(row.get("media_id")), _positive(row.get("season"))
    if media_id is None or season is None:
        return None
    return "tmdb", str(media_id), season


def _record(key: tuple[str, str, int]) -> dict[str, Any]:
    return dict(zip(("media_source", "media_id", "season"), key))


def build_scope(report: Mapping[str, Any], quarter: str, episodes: Any) -> dict[str, Any]:
    if not isinstance(quarter, str) or not re.fullmatch(r"[0-9]{4}-(01|04|07|10)", quarter):
        raise QualifiedScopeError("qualified_quarter_invalid")
    if report.get("status") != "ok" or not isinstance(report.get("eligible"), list):
        raise QualifiedScopeError("qualification_not_complete")
    counts = report.get("counts")
    if not isinstance(counts, Mapping) or type(counts.get("unknown")) is not int or counts["unknown"] != 0:
        raise QualifiedScopeError("qualification_identity_unknown")
    keys = set()
    for item in report["eligible"]:
        key = canonical_identity(item) if isinstance(item, Mapping) else None
        if key is None:
            raise QualifiedScopeError("qualified_identity_invalid")
        try:
            premiere = date.fromisoformat(item["season_air_date"])
        except (KeyError, TypeError, ValueError):
            raise QualifiedScopeError("qualified_season_date_invalid") from None
        label = f"{premiere.year:04d}-{((premiere.month - 1) // 3) * 3 + 1:02d}"
        if label != quarter or key in keys:
            raise QualifiedScopeError("qualification_scope_inconsistent")
        keys.add(key)
    if type(counts.get("eligible")) is not int or counts["eligible"] != len(keys):
        raise QualifiedScopeError("qualification_count_inconsistent")

    blocked, matched, ambiguous, unknown, outside = _classify_reviews(keys, episodes)
    runnable = keys - blocked if not unknown else set()
    status = "blocked" if keys and (unknown or not runnable) else "ok"
    return {
        "version": 1, "status": status, "quarter": quarter,
        "eligible": [_record(k) for k in sorted(keys)],
        "runnable": [_record(k) for k in sorted(runnable)],
        "blocked": [_record(k) for k in sorted(blocked)],
        "review_counts": {"matched": len(matched), "outside_declared_scope": outside,
                          "ambiguous": len(ambiguous), "identity_unknown": len(unknown)},
        "matched_review_ids": matched, "ambiguous_review_ids": ambiguous,
        "unknown_review_ids": unknown,
    }


def allowed_identities(window: Any) -> frozenset[tuple[str, str, int]] | None:
    if not isinstance(window, Mapping) or "qualified_scope" not in window:
        return None
    scope = window["qualified_scope"]
    if (not isinstance(scope, Mapping) or type(scope.get("version")) is not int
            or scope["version"] != 1 or scope.get("status") != "ok"):
        raise QualifiedScopeError("qualified_execution_scope_blocked")
    if scope.get("quarter") != window.get("season_quarter") or not isinstance(scope.get("runnable"), list):
        raise QualifiedScopeError("qualified_execution_scope_invalid")
    identities = [canonical_identity(row) if isinstance(row, Mapping) else None for row in scope["runnable"]]
    if any(key is None for key in identities):
        raise QualifiedScopeError("qualified_execution_identity_invalid")
    if not isinstance(scope.get("eligible"), list) or not isinstance(scope.get("blocked"), list):
        raise QualifiedScopeError("qualified_execution_scope_invalid")
    eligible = {canonical_identity(row) if isinstance(row, Mapping) else None for row in scope["eligible"]}
    blocked = {canonical_identity(row) if isinstance(row, Mapping) else None for row in scope["blocked"]}
    if (None in eligible or None in blocked or not set(identities).issubset(eligible)
            or set(identities).intersection(blocked) or scope.get("unknown_review_ids")):
        raise QualifiedScopeError("qualified_execution_scope_inconsistent")
    return frozenset(identities)


def identity_allowed(row: Mapping[str, Any], identities: Any) -> bool:
    return identities is None or canonical_identity(row) in identities


def _classify_reviews(keys, episodes):
    blocked, matched, ambiguous, unknown = set(), [], [], []
    outside = 0
    for row in episodes:
        if row.get("status") != "needs_review":
            continue
        review_id = row.get("id")
        key = canonical_identity(row)
        if key in keys:
            blocked.add(key)
            matched.append(review_id)
        elif key is not None:
            outside += 1
        elif row.get("media_source") == "legacy":
            media_id, season = _positive(row.get("media_id")), _positive(row.get("season"))
            if media_id is None or season is None:
                unknown.append(review_id)
                continue
            overlaps = {k for k in keys if k[1:] == (str(media_id), season)}
            if overlaps:
                blocked.update(overlaps)
                ambiguous.append(review_id)
            else:
                # This is an excluded declared identity, not proof of its quarter.
                # It cannot be dispatched by this scope; inventory checks still apply.
                outside += 1
        else:
            unknown.append(review_id)
    return blocked, matched, ambiguous, unknown, outside


def restrict_for_reviews(identities, episodes):
    """Reclassify fresh evidence without ever widening the original scope."""
    blocked, _matched, _ambiguous, unknown, _outside = _classify_reviews(identities, episodes)
    if unknown:
        raise QualifiedScopeError("qualified_review_identity_unknown")
    return frozenset(identities - blocked)
