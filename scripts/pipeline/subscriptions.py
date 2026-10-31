"""MoviePilot subscription normalization.

MoviePilot is the only subscription source used by V5.  This module keeps the
HTTP representation at the adapter boundary and turns each active television
subscription into the immutable :class:`pipeline.models.Subscription` value
used by discovery and persistence.
"""
from __future__ import annotations

from collections.abc import Mapping
import datetime as dt
import re
from typing import Any, Iterable

from .models import Subscription


class SubscriptionError(ValueError):
    """A subscription payload is not safe to use for discovery.

    The exception carries the exact subscription identifier and field so a
    caller can report one bad entry without silently dropping the rest of the
    MoviePilot snapshot.
    """

    def __init__(
        self,
        message: str,
        *,
        subscription_id: str = "<unknown>",
        field: str = "",
    ) -> None:
        self.subscription_id = str(subscription_id or "<unknown>").strip() or "<unknown>"
        self.field = str(field or "").strip()
        self.reason = str(message).strip()
        detail = f"subscription {self.subscription_id}"
        if self.field:
            detail += f" field {self.field}"
        super().__init__(f"{detail}: {self.reason}")


_MISSING = object()


_ISO_DATE = re.compile(r"^\d{4}-\d{2}-\d{2}$")


def _verified_tmdb_season_air_date(payload: Mapping[str, Any], season: int) -> str:
    """Extract one season date from an explicit TMDB metadata namespace.

    MoviePilot versions expose this evidence under a few compatible shapes,
    but a generic ``air_date`` at the subscription root is intentionally not
    trusted: it may be an episode or release date.  The namespace and season
    number must identify the requested TMDB season before the date is kept.
    """

    for key in ("tmdb_season_air_date", "season_air_date"):
        value = payload.get(key)
        if _valid_iso_date(value):
            return str(value).strip()

    containers: list[Any] = []
    for key in (
        "tmdb_seasons", "tmdb_season", "tmdb_season_metadata", "tmdb_metadata",
    ):
        if key in payload:
            containers.append(payload[key])
    tmdb = payload.get("tmdb")
    if isinstance(tmdb, Mapping):
        containers.extend(
            tmdb.get(key) for key in ("seasons", "season", "season_metadata")
            if tmdb.get(key) is not None
        )
        if any(name in tmdb for name in ("air_date", "season_air_date", "tmdb_season_air_date")):
            containers.append(tmdb)

    for container in containers:
        entries = _season_metadata_entries(container, season)
        for entry in entries:
            if not isinstance(entry, Mapping):
                continue
            number = _season_number(entry)
            if number != season:
                continue
            value = next(
                (entry.get(key) for key in ("air_date", "season_air_date", "tmdb_season_air_date")
                 if entry.get(key) not in (None, "")),
                None,
            )
            if _valid_iso_date(value):
                return str(value).strip()
    return ""


def _season_metadata_entries(container: Any, season: int) -> list[Any]:
    if isinstance(container, (list, tuple)):
        return list(container)
    if not isinstance(container, Mapping):
        return []
    keyed = container.get(str(season))
    if keyed is None:
        keyed = container.get(season)
    if keyed is not None:
        if isinstance(keyed, Mapping):
            # The map key itself is explicit season evidence; preserve any
            # conflicting number so the caller rejects that record.
            if _season_number(keyed) is None:
                keyed = {**keyed, "season_number": season}
        return [keyed]
    for key in ("seasons", "season", "results", "data", "items"):
        nested = container.get(key)
        if nested is not None:
            return _season_metadata_entries(nested, season)
    return [container]


def _season_number(value: Mapping[str, Any]) -> int | None:
    for key in ("season_number", "seasonNumber", "season", "number"):
        if value.get(key) in (None, ""):
            continue
        try:
            return int(value[key])
        except (TypeError, ValueError):
            return None
    return None


def _valid_iso_date(value: Any) -> bool:
    if not isinstance(value, str) or not _ISO_DATE.fullmatch(value.strip()):
        return False
    try:
        dt.date.fromisoformat(value.strip())
    except ValueError:
        return False
    return True


def normalize_subscription(payload: Mapping[str, Any]) -> Subscription:
    """Normalize one MoviePilot subscription payload.

    Only identities present in the payload are used.  In particular, ``tmdb``
    is inferred only when MoviePilot supplied ``tmdbid`` and omitted the media
    source; a generic ``media_id`` never receives an invented namespace.
    """

    if not isinstance(payload, Mapping):
        raise SubscriptionError("expected an object", field="subscription")

    subscription_id = _required_text(
        payload, ("subscription_id", "id"), "subscription_id", payload
    )
    tmdb_id = _first_value(payload, ("tmdbid", "tmdb_id"))
    media_id = _first_value(payload, ("media_id", "mediaid"))
    source = _first_value(
        payload, ("media_source", "source", "source_type")
    )
    source_supplied = any(
        name in payload and payload[name] is not None
        for name in ("media_source", "source", "source_type")
    )
    if media_id is _MISSING and tmdb_id is not _MISSING:
        media_id = tmdb_id
    if media_id is _MISSING or not _as_text(media_id):
        raise SubscriptionError(
            "must be a nonblank identity; MoviePilot did not provide one",
            subscription_id=subscription_id,
            field="media_id",
        )
    if source is _MISSING or not _as_text(source):
        if not source_supplied and tmdb_id is not _MISSING and _as_text(tmdb_id):
            source = "tmdb"
        else:
            raise SubscriptionError(
                "must be explicit when tmdbid is absent",
                subscription_id=subscription_id,
                field="media_source",
            )

    work = _first_value(
        payload, ("work", "canonical_work", "name", "title", "show_name")
    )
    if work is _MISSING or not _as_text(work):
        raise SubscriptionError(
            "must be a nonblank canonical name",
            subscription_id=subscription_id,
            field="work",
        )

    season = _first_value(payload, ("season", "season_number"))
    if season is _MISSING:
        raise SubscriptionError(
            "must be a positive integer",
            subscription_id=subscription_id,
            field="season",
        )
    season_number = _positive_int(
        season,
        subscription_id=subscription_id,
        field="season",
    )
    season_air_date = _verified_tmdb_season_air_date(payload, season_number)

    aliases = _normalize_aliases(payload, _as_text(work))
    return Subscription(
        subscription_id=subscription_id,
        media_source=_as_text(source).lower(),
        media_id=_as_text(media_id),
        work=_as_text(work),
        aliases=aliases,
        season=season_number,
        include=_text_value(payload, ("include", "include_text")),
        exclude=_text_value(payload, ("exclude", "exclude_text")),
        season_air_date=season_air_date,
    )


def subscription_identifier(payload: Any) -> str:
    """Return a safe identifier for diagnostics before normalization."""

    if isinstance(payload, Mapping):
        value = _first_value(payload, ("subscription_id", "id"))
        if value is not _MISSING and _as_text(value):
            return _as_text(value)
    return "<unknown>"


def exclusion_reason(payload: Any) -> str | None:
    """Return an explicit reason for a non-discoverable subscription.

    Missing type/enabled fields are intentionally treated as active television
    subscriptions for compatibility with older MoviePilot payloads.  A field
    that explicitly says disabled or non-TV is never silently ignored.
    """

    if not isinstance(payload, Mapping):
        return None
    if _is_disabled(payload):
        return "disabled"
    if _is_non_tv(payload):
        return "non_tv"
    return None


def _required_text(
    payload: Mapping[str, Any],
    names: Iterable[str],
    field: str,
    original: Mapping[str, Any],
) -> str:
    value = _first_value(payload, names)
    if value is _MISSING or not _as_text(value):
        supplied_id = subscription_identifier(original)
        raise SubscriptionError(
            "must be a nonblank value",
            subscription_id=supplied_id,
            field=field,
        )
    return _as_text(value)


def _first_value(payload: Mapping[str, Any], names: Iterable[str]) -> Any:
    for name in names:
        if name in payload and payload[name] is not None:
            value = payload[name]
            if value != "":
                return value
    return _MISSING


def _as_text(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    return str(value).strip()


def _text_value(payload: Mapping[str, Any], names: Iterable[str]) -> str:
    value = _first_value(payload, names)
    if value is _MISSING:
        return ""
    if isinstance(value, (list, tuple, set)):
        return " ".join(_as_text(item) for item in value if _as_text(item))
    return _as_text(value)


def _positive_int(value: Any, *, subscription_id: str, field: str) -> int:
    if isinstance(value, bool):
        raise SubscriptionError(
            "must be a positive integer",
            subscription_id=subscription_id,
            field=field,
        )
    try:
        text = _as_text(value)
        if not text or not text.isdigit():
            raise ValueError
        number = int(text)
    except (TypeError, ValueError):
        raise SubscriptionError(
            "must be a positive integer",
            subscription_id=subscription_id,
            field=field,
        ) from None
    if number <= 0:
        raise SubscriptionError(
            "must be a positive integer",
            subscription_id=subscription_id,
            field=field,
        )
    return number


def _normalize_aliases(payload: Mapping[str, Any], canonical_work: str) -> tuple[str, ...]:
    values: list[Any] = []
    for field in ("aliases", "alias", "names", "name_list", "titles"):
        if field not in payload or payload[field] is None:
            continue
        raw = payload[field]
        if isinstance(raw, (list, tuple, set)):
            values.extend(raw)
        else:
            values.append(raw)

    result: list[str] = []
    seen: set[str] = set()
    canonical_key = canonical_work.casefold()
    for value in values:
        if value is None:
            continue
        text = _as_text(value)
        key = text.casefold()
        if not text or key == canonical_key or key in seen:
            continue
        seen.add(key)
        result.append(text)
    return tuple(result)


def _is_disabled(payload: Mapping[str, Any]) -> bool:
    for field in ("enabled", "active"):
        if field not in payload or payload[field] is None:
            continue
        value = payload[field]
        if value is False or value == 0:
            return True
        if isinstance(value, str) and value.strip().casefold() in {
            "false", "0", "disabled", "inactive", "off", "停用", "禁用", "关闭",
        }:
            return True
    value = _first_value(payload, ("status", "state"))
    if value is not _MISSING:
        normalized = _as_text(value).casefold()
        if normalized in {
            "disabled", "inactive", "off", "stopped", "false", "0", "停用", "禁用", "关闭",
        }:
            return True
    return False


def _is_non_tv(payload: Mapping[str, Any]) -> bool:
    fields = ("type", "media_type", "subscribe_type", "category", "kind")
    identifier_namespaces = {"tmdb", "tvdb", "imdb", "douban", "mal", "anilist", "bangumi"}
    values = [
        _as_text(payload[field]).casefold()
        for field in fields
        if field in payload
        and payload[field] is not None
        and _as_text(payload[field])
        and _as_text(payload[field]).casefold() not in identifier_namespaces
    ]
    if not values:
        return False
    tv_markers = ("tv", "series", "show", "television", "电视剧", "剧集", "番剧")
    movie_markers = ("movie", "film", "电影", "电影订阅")
    for value in values:
        if any(marker in value for marker in movie_markers):
            return True
        # Every supplied content type must be compatible with television.
        # A TV marker in one field cannot override another field's non-TV type.
        if not any(marker in value for marker in tv_markers):
            return True
    return False


__all__ = [
    "SubscriptionError",
    "exclusion_reason",
    "normalize_subscription",
    "subscription_identifier",
]
