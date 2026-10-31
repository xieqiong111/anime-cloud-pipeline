"""Pure, fail-closed qualification of MoviePilot subscriptions for V5 resources.

This module only evaluates an already-read API snapshot.  It performs no
network, state-store, discovery, or download work and deliberately returns no
subscription names or unfiltered API payloads.
"""
from __future__ import annotations

from collections import defaultdict
from collections.abc import Mapping
import datetime as dt
import re
from typing import Any

from .anime_guard import AnimeClassification, classify_anime
from .subscriptions import exclusion_reason, normalize_subscription


ALLOWED_USERNAMES = frozenset({"admin", "Bangumi订阅"})
_TMDB_SOURCES = frozenset({"tmdb", "themoviedb"})
_SOURCE_FIELDS = ("media_source", "source", "source_type")
_ID_FIELDS = ("tmdbid", "tmdb_id", "media_id", "mediaid")
_SEASON_FIELDS = ("season", "season_number")
_TV_FIELDS = ("type", "media_type", "subscribe_type", "category", "kind")
_TV_NAMESPACES = frozenset({"tmdb", "tvdb", "imdb", "douban", "mal", "anilist", "bangumi"})
_TV_VALUES = frozenset({
    "tv", "series", "show", "television", "tv series", "电视剧", "剧集", "番剧",
})
_ACTIVE_VALUES = frozenset({
    "true", "1", "enabled", "active", "running", "on", "启用", "运行中",
})
_ISO_DATE = re.compile(r"^[0-9]{4}-[0-9]{2}-[0-9]{2}$")


def qualify_subscriptions(
    rows: Any,
    *,
    quarter: str,
    allowed_usernames: tuple[str, ...],
) -> dict[str, Any]:
    """Return a privacy-minimal, fail-closed qualification report.

    ``quarter`` must be a calendar-quarter start month (YYYY-01/04/07/10).
    The allow-list is intentionally fixed to ``admin`` and ``Bangumi订阅``;
    the argument exists for an explicit caller contract, not policy widening.
    On MoviePilot's observed schema, state ``N``/``R`` proves active,
    ``S`` is paused, and ``P`` remains pending and therefore unknown.

    Counts are row counts at the named gate: ``source_pass`` counts exact
    allow-list matches; exclusion counts may overlap with ``unknown`` when a
    later duplicate conflict makes an otherwise classified identity unsafe.
    ``duplicates`` counts extra rows beyond the first for each canonical
    (tmdb, id, season) identity.  ``eligible`` counts deduplicated identities.
    """

    counts: dict[str, int | None] = {
        "api_total": len(rows) if isinstance(rows, list) else None,
        "source_pass": 0,
        "source_excluded": 0,
        "disabled": 0,
        "non_tv": 0,
        "non_anime": 0,
        "cross_quarter": 0,
        "unknown": 0,
        "duplicates": 0,
        "eligible": 0,
    }
    issue_codes: set[str] = set()

    def unknown(reason: str, record: dict[str, Any] | None = None) -> None:
        if record is None or not record["unknown"]:
            counts["unknown"] = int(counts["unknown"] or 0) + 1
        if record is not None:
            record["unknown"] = True
        issue_codes.add(reason)

    if not isinstance(rows, list):
        unknown("api_shape_invalid")
        return _result(counts, issue_codes, [])

    if (
        not isinstance(allowed_usernames, tuple)
        or len(allowed_usernames) != 2
        or any(not isinstance(value, str) for value in allowed_usernames)
        or frozenset(allowed_usernames) != ALLOWED_USERNAMES
    ):
        issue_codes.add("source_policy_invalid")
        return _result(counts, issue_codes, [])

    quarter_bounds = _quarter_bounds(quarter)
    if quarter_bounds is None:
        issue_codes.add("quarter_invalid")
        return _result(counts, issue_codes, [])

    start, end = quarter_bounds
    records: list[dict[str, Any]] = []

    for row in rows:
        if not isinstance(row, Mapping):
            unknown("api_row_invalid")
            continue

        # Explicit disablement wins even when source identity is absent.
        if _explicitly_disabled(row):
            counts["disabled"] = int(counts["disabled"] or 0) + 1
            issue_codes.add("subscription_disabled")
            continue

        username = row.get("username")
        if not isinstance(username, str) or username == "":
            unknown("source_username_unknown")
            continue
        if username not in ALLOWED_USERNAMES:
            counts["source_excluded"] = int(counts["source_excluded"] or 0) + 1
            issue_codes.add("source_not_allowed")
            continue
        counts["source_pass"] = int(counts["source_pass"] or 0) + 1

        if exclusion_reason(row) == "non_tv":
            counts["non_tv"] = int(counts["non_tv"] or 0) + 1
            issue_codes.add("subscription_non_tv")
            continue
        if not _has_explicit_tv_type(row):
            unknown("subscription_type_unknown")
            continue
        if not _has_explicit_active_state(row):
            unknown("subscription_status_unknown")
            continue

        identity = _normalized_identity(row)
        if identity is None:
            unknown("subscription_identity_unknown")
            continue
        key, normalized = identity
        genre = classify_anime(row)
        season_date, date_problem = _verified_season_date(row, normalized)
        record: dict[str, Any] = {
            "key": key,
            "genre": genre,
            "date": season_date,
            "decision": "candidate",
            "unknown": False,
        }
        records.append(record)

        if genre is AnimeClassification.NON_ANIME:
            counts["non_anime"] = int(counts["non_anime"] or 0) + 1
            record["decision"] = "excluded"
            issue_codes.add("subscription_non_anime")
        elif genre is AnimeClassification.UNKNOWN:
            unknown("anime_genre_unknown", record)

        if date_problem == "conflict":
            unknown("season_date_conflict", record)
        elif date_problem == "missing" and genre is not AnimeClassification.NON_ANIME:
            unknown("season_date_missing", record)

        if (
            genre is AnimeClassification.ANIME
            and not record["unknown"]
            and season_date is not None
            and not (start <= season_date < end)
        ):
            counts["cross_quarter"] = int(counts["cross_quarter"] or 0) + 1
            record["decision"] = "excluded"
            issue_codes.add("season_outside_quarter")

    groups: dict[tuple[str, str, int], list[dict[str, Any]]] = defaultdict(list)
    for record in records:
        groups[record["key"]].append(record)

    conflicted_keys: set[tuple[str, str, int]] = set()
    for key, group in groups.items():
        if len(group) < 2:
            continue
        counts["duplicates"] = int(counts["duplicates"] or 0) + len(group) - 1
        issue_codes.add("duplicate_identity")
        known_genres = {record["genre"] for record in group
                        if record["genre"] is not AnimeClassification.UNKNOWN}
        known_dates = {record["date"] for record in group if record["date"] is not None}
        if len(known_genres) > 1 or len(known_dates) > 1:
            conflicted_keys.add(key)
            issue_codes.add("duplicate_evidence_conflict")
            for record in group:
                unknown("duplicate_evidence_conflict", record)

    eligible_by_key: dict[tuple[str, str, int], dict[str, Any]] = {}
    for record in records:
        key = record["key"]
        if key in conflicted_keys or record["unknown"] or record["decision"] != "candidate":
            continue
        if key in eligible_by_key:
            continue
        season_date = record["date"]
        if season_date is None:
            continue
        eligible_by_key[key] = {
            "media_source": "tmdb",
            "media_id": key[1],
            "season": key[2],
            "season_air_date": season_date.isoformat(),
        }

    eligible = [eligible_by_key[key] for key in sorted(eligible_by_key)]
    counts["eligible"] = len(eligible)
    return _result(counts, issue_codes, eligible)


def _result(
    counts: dict[str, int | None], issue_codes: set[str], eligible: list[dict[str, Any]]
) -> dict[str, Any]:
    return {
        "status": "blocked" if issue_codes.intersection({
            "api_shape_invalid", "api_row_invalid", "source_policy_invalid",
            "quarter_invalid", "source_username_unknown", "subscription_type_unknown",
            "subscription_status_unknown", "subscription_identity_unknown",
            "anime_genre_unknown", "season_date_missing", "season_date_conflict",
            "duplicate_evidence_conflict",
        }) else "ok",
        "counts": counts,
        "eligible": eligible,
        "issues": [{"reason": code} for code in sorted(issue_codes)],
    }


def _quarter_bounds(value: Any) -> tuple[dt.date, dt.date] | None:
    if not isinstance(value, str) or not re.fullmatch(r"[0-9]{4}-(01|04|07|10)", value):
        return None
    year, month = (int(part) for part in value.split("-"))
    if year < 1:
        return None
    start = dt.date(year, month, 1)
    if month == 10:
        end = dt.date(year + 1, 1, 1) if year < 9999 else None
    else:
        end = dt.date(year, month + 3, 1)
    return (start, end) if end is not None else None


def _integer(value: Any) -> int | None:
    if isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value > 0 else None
    if isinstance(value, str):
        text = value.strip()
        if re.fullmatch(r"[0-9]+", text):
            number = int(text)
            return number if number > 0 else None
    return None


def _provided_values(row: Mapping[str, Any], names: tuple[str, ...]) -> list[Any]:
    return [row[name] for name in names if name in row and row[name] not in (None, "")]


def _normalized_identity(
    row: Mapping[str, Any],
) -> tuple[tuple[str, str, int], Any] | None:
    try:
        source_values = _provided_values(row, _SOURCE_FIELDS)
        tmdb_values = _provided_values(row, ("tmdbid", "tmdb_id"))
        id_values = _provided_values(row, _ID_FIELDS)
        season_values = _provided_values(row, _SEASON_FIELDS)
        if not id_values or not season_values:
            return None

        sources = {str(value).strip().lower() for value in source_values}
        if source_values and not sources.issubset(_TMDB_SOURCES):
            return None
        if not source_values and not tmdb_values:
            return None

        ids = {_integer(value) for value in id_values}
        seasons = {_integer(value) for value in season_values}
        if None in ids or len(ids) != 1 or None in seasons or len(seasons) != 1:
            return None
        media_id = str(next(iter(ids)))
        season = next(iter(seasons))

        # Reuse the repository normalizer for its identity and season parsing,
        # supplying an internal label only when the API row has no display name.
        normalization_payload = dict(row)
        if not any(
            isinstance(row.get(name), str) and row[name].strip()
            for name in ("work", "canonical_work", "name", "title", "show_name")
        ):
            normalization_payload["work"] = "qualification"
        normalized = normalize_subscription(normalization_payload)
        if normalized.media_source not in _TMDB_SOURCES:
            return None
        if _integer(normalized.media_id) != int(media_id) or normalized.season != season:
            return None
        return ("tmdb", media_id, season), normalized
    except Exception:
        # API values are untrusted.  Do not put exception text or raw values in
        # the report; callers receive a fixed reason code at the boundary.
        return None


def _has_explicit_tv_type(row: Mapping[str, Any]) -> bool:
    for field in _TV_FIELDS:
        value = row.get(field)
        if value is None or value == "":
            continue
        text = str(value).strip().casefold()
        if text in _TV_NAMESPACES:
            continue
        if text in _TV_VALUES:
            return True
    return False


def _active_value(value: Any) -> bool:
    if value is True or (isinstance(value, int) and not isinstance(value, bool) and value == 1):
        return True
    return isinstance(value, str) and value.strip().casefold() in _ACTIVE_VALUES


def _explicitly_disabled(row: Mapping[str, Any]) -> bool:
    if exclusion_reason(row) == "disabled":
        return True
    state = row.get("state")
    return isinstance(state, str) and state.strip().upper() == "S"


def _has_explicit_active_state(row: Mapping[str, Any]) -> bool:
    primary = [row[name] for name in ("enabled", "active")
               if name in row and row[name] is not None]
    status_values = [row["status"]] if row.get("status") is not None else []
    if status_values and not all(_active_value(value) for value in status_values):
        return False

    state_present = "state" in row and row["state"] is not None
    state_value = row.get("state")
    state_code = state_value.strip().upper() if isinstance(state_value, str) else ""
    if state_present:
        if state_code == "P":
            return False
        if state_code not in {"N", "R"} and not _active_value(state_value):
            return False

    if primary:
        return all(_active_value(value) for value in primary)
    if state_present:
        return state_code in {"N", "R"} or _active_value(state_value)
    if status_values:
        return all(_active_value(value) for value in status_values)
    return False


def _valid_date(value: Any) -> dt.date | None:
    if not isinstance(value, str):
        return None
    text = value.strip()
    if not _ISO_DATE.fullmatch(text):
        return None
    try:
        return dt.date.fromisoformat(text)
    except ValueError:
        return None


def _season_number(value: Mapping[str, Any]) -> int | None:
    for field in ("season_number", "seasonNumber", "season", "number"):
        if field in value and value[field] not in (None, ""):
            return _integer(value[field])
    return None


def _season_records(value: Any, target: int) -> list[Mapping[str, Any]]:
    if isinstance(value, (list, tuple)):
        return [entry for entry in value if isinstance(entry, Mapping)
                and _season_number(entry) == target]
    if not isinstance(value, Mapping):
        return []
    keyed = value.get(str(target), value.get(target))
    if isinstance(keyed, Mapping):
        entry = dict(keyed)
        entry.setdefault("season_number", target)
        return [entry] if _season_number(entry) == target else []
    if isinstance(keyed, (list, tuple)):
        return _season_records(keyed, target)
    if _season_number(value) == target:
        return [value]
    for field in ("seasons", "season", "results", "data", "items"):
        if field in value:
            return _season_records(value[field], target)
    return []


def _verified_season_date(row: Mapping[str, Any], normalized: Any) -> tuple[dt.date | None, str | None]:
    season = normalized.season
    # This is the explicit detail shape populated by MoviePilotClient from
    # /api/v1/media/... season_info.  Root-level normalized dates alone are
    # insufficient because they may describe an episode or release date.
    records = _season_records(row.get("tmdb_seasons"), season)
    if not records:
        return None, "missing"

    dates: list[dt.date] = []
    for entry in records:
        raw_dates = _provided_values(
            entry, ("air_date", "season_air_date", "tmdb_season_air_date"),
        )
        parsed_dates = [_valid_date(value) for value in raw_dates]
        if not parsed_dates or None in parsed_dates:
            return None, "missing"
        if len(set(parsed_dates)) != 1:
            return None, "conflict"
        dates.append(parsed_dates[0])
    if len(set(dates)) != 1:
        return None, "conflict"

    metadata_date = dates[0]
    normalized_date = _valid_date(normalized.season_air_date)
    if normalized_date is None:
        return None, "missing"
    root_dates = [_valid_date(value) for value in _provided_values(
        row, ("tmdb_season_air_date", "season_air_date"),
    )]
    if None in root_dates:
        return None, "missing"
    if normalized_date != metadata_date or any(value != metadata_date for value in root_dates):
        return None, "conflict"
    return metadata_date, None


__all__ = ["ALLOWED_USERNAMES", "qualify_subscriptions"]
