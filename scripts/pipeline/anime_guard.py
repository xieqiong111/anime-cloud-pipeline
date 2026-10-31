"""V5 opt-in safety guard for full subscription discovery.

A *full* discovery (``run`` without ``--resume-only``) must not be able to
reach a download unless two independent risks are closed:

1. **Anime gate** -- a subscription is discoverable only when reliable TMDB
   evidence proves genre ``16`` (Animation).  A non-animation subscription is
   excluded; a subscription whose genre cannot be proven *fails closed*
   (``anime_genre_unknown``).  Names alone are never accepted as proof because
   they are localized and ambiguous.
2. **Identity dedup** -- the canonical episode identity is
   ``(tmdb_id, season, episode)``.  A migrated ``legacy`` row and a ``tmdb``
   discovery decision that describe the same episode collapse to one owner, so
   a discovery can never re-download an episode the migration already tracks.
   A legacy row may only be *claimed* when it carries TMDB verification
   evidence; an unverified legacy identity is never trusted and blocks the
   write instead of being silently promoted (fail closed).

The module is intentionally pure: it never touches the network, the clock, or
a store, so every decision is reproducible and unit-testable with fake
fixtures.  It is inert unless explicitly enabled (see ``run_pipeline``): the
default ``DiscoveryService`` behaviour is unchanged.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from enum import Enum
import json
from typing import Any, Optional

ANIMATION_GENRE_ID = 16
TMDB_MEDIA_SOURCES = frozenset({"tmdb", "themoviedb"})

# Exclusion / block reasons (stable strings used in reports and audits).
NON_ANIME = "non_anime"
GENRE_UNKNOWN = "anime_genre_unknown"
DUPLICATE_OWNED = "duplicate_owned_by_legacy"
UNVERIFIED_COLLISION = "unverified_legacy_identity"


class AnimeClassification(str, Enum):
    """Outcome of the TMDB genre gate."""

    ANIME = "anime"
    NON_ANIME = "non_anime"
    UNKNOWN = "unknown"


def _text(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip()


def _int_or_none(value: Any) -> Optional[int]:
    if isinstance(value, bool):
        return None
    try:
        number = int(str(value).strip())
    except (TypeError, ValueError):
        return None
    return number


def _field(obj: Any, name: str, default: Any = None) -> Any:
    if isinstance(obj, Mapping):
        return obj.get(name, default)
    return getattr(obj, name, default)


def _genre_id_numbers(payload: Any) -> tuple[int, ...]:
    """Read TMDB genre ids from a subscription payload, ids only.

    ``tmdb_genre_ids`` is the adapter-populated, verified field.  ``genre_ids``
    is accepted for adapter shapes that use the raw TMDB spelling.  Names are
    deliberately ignored here: only the numeric id proves animation.
    """

    raw = _field(payload, "tmdb_genre_ids")
    if raw is None:
        raw = _field(payload, "genre_ids")
    out: list[int] = []
    if isinstance(raw, (list, tuple, set)):
        for value in raw:
            number = _int_or_none(value)
            if number is not None and number not in out:
                out.append(number)
    return tuple(out)


def classify_anime(payload: Any) -> "AnimeClassification":
    """Classify a subscription using verified TMDB genre evidence only."""

    ids = _genre_id_numbers(payload)
    if ANIMATION_GENRE_ID in ids:
        return AnimeClassification.ANIME
    if ids:
        return AnimeClassification.NON_ANIME
    return AnimeClassification.UNKNOWN


def anime_exclusion_reason(payload: Any) -> Optional[str]:
    """Return a fail-closed exclusion reason, or ``None`` when provably anime."""

    verdict = classify_anime(payload)
    if verdict is AnimeClassification.ANIME:
        return None
    if verdict is AnimeClassification.NON_ANIME:
        return NON_ANIME
    return GENRE_UNKNOWN


def _evidence_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return {}
        return parsed if isinstance(parsed, Mapping) else {}
    return {}


def legacy_owned_identity(row: Any) -> Optional[tuple[str, int, int]]:
    """Return the *verified* ``(tmdb_id, season, episode)`` a row owns.

    A legacy row is only a trustworthy owner when the migration captured an
    explicit ``tmdb_id`` in its evidence.  A bare ``media_id`` is not proof.
    ``tmdb`` rows are trusted by construction.
    """

    source = _text(_field(row, "media_source")).casefold()
    season = _int_or_none(_field(row, "season"))
    episode = _int_or_none(_field(row, "episode"))
    if season is None or episode is None:
        return None
    if source in TMDB_MEDIA_SOURCES:
        media_id = _text(_field(row, "media_id"))
        return (media_id, season, episode) if media_id else None
    evidence = _evidence_mapping(_field(row, "legacy_evidence_json", {}))
    tmdb = _text(evidence.get("tmdb_id"))
    if not tmdb:
        return None
    return (tmdb, season, episode)


def legacy_unverified_season(row: Any) -> Optional[tuple[str, int]]:
    """Return ``(media_id, season)`` for a legacy row that cannot be verified."""

    if legacy_owned_identity(row) is not None:
        return None
    media_id = _text(_field(row, "media_id"))
    season = _int_or_none(_field(row, "season"))
    if not media_id or season is None:
        return None
    if media_id.startswith("legacy:"):
        return None
    return (media_id, season)


@dataclass
class GuardContext:
    """Index of the episode identities already owned by the local state."""

    owned: dict[tuple[str, int, int], tuple[Any, str]] = field(default_factory=dict)
    unverified: dict[tuple[str, int], tuple[Any, ...]] = field(default_factory=dict)
    verified_rows: int = 0
    unverified_rows: int = 0

    @property
    def total_rows(self) -> int:
        return self.verified_rows + self.unverified_rows


def build_context(existing_rows: Iterable[Any] = ()) -> GuardContext:
    context = GuardContext()
    for row in existing_rows or ():
        owned = legacy_owned_identity(row)
        row_id = _field(row, "id")
        source = _text(_field(row, "media_source")).casefold() or "legacy"
        if owned is not None:
            context.verified_rows += 1
            context.owned.setdefault(owned, (row_id, source))
            continue
        unverified = legacy_unverified_season(row)
        if unverified is not None:
            context.unverified_rows += 1
            context.unverified.setdefault(unverified, ())
            context.unverified[unverified] = context.unverified[unverified] + (row_id,)
    return context


def decision_identity(decision: Any) -> Optional[tuple[str, int, int]]:
    media_id = _text(_field(decision, "media_id"))
    season = _int_or_none(_field(decision, "season"))
    episode = _int_or_none(_field(decision, "episode"))
    if not media_id or season is None or episode is None:
        return None
    return (media_id, season, episode)


def decision_season_key(decision: Any) -> Optional[tuple[str, int]]:
    media_id = _text(_field(decision, "media_id"))
    season = _int_or_none(_field(decision, "season"))
    if not media_id or season is None:
        return None
    return (media_id, season)


@dataclass(frozen=True)
class WriteGuardVerdict:
    safe: bool
    blocked_reasons: tuple[str, ...] = ()
    audit: tuple[Mapping[str, Any], ...] = ()

    def to_mapping(self) -> dict[str, Any]:
        return {
            "safe": self.safe,
            "blocked_reasons": list(self.blocked_reasons),
            "audit": [dict(item) for item in self.audit],
        }


class DiscoveryGuardError(RuntimeError):
    """Raised by the write path when the guard cannot prove a run is safe."""

    def __init__(self, verdict: WriteGuardVerdict) -> None:
        self.verdict = verdict
        reasons = ", ".join(verdict.blocked_reasons) or "unspecified"
        super().__init__(f"full discovery blocked before download: {reasons}")


class DiscoveryGuard:
    """Compose the anime gate and the identity dedup gate."""

    name = "anime-guard"

    def __init__(self, *, require_anime: bool = True) -> None:
        self.require_anime = bool(require_anime)
        self.context = GuardContext()

    # ---- context ----
    def bind(self, existing_rows: Iterable[Any] = ()) -> GuardContext:
        """Recompute the local ownership index from the current state rows."""

        self.context = build_context(existing_rows)
        return self.context

    # ---- subscription gate ----
    def subscription_exclusion_reason(self, payload: Any) -> Optional[str]:
        if not self.require_anime:
            return None
        return anime_exclusion_reason(payload)

    # ---- decision dedup ----
    def decision_exclusion(self, decision: Any) -> Optional[str]:
        identity = decision_identity(decision)
        if identity is not None and identity in self.context.owned:
            return DUPLICATE_OWNED
        key = decision_season_key(decision)
        if key is not None and key in self.context.unverified:
            return UNVERIFIED_COLLISION
        return None

    # ---- write gate ----
    def preflight(self, subscriptions: Iterable[Any]) -> WriteGuardVerdict:
        reasons: list[str] = []
        for subscription in subscriptions or ():
            key = decision_season_key(subscription)
            if key is not None and key in self.context.unverified:
                reasons.append(f"{UNVERIFIED_COLLISION}:{key[0]}")
        deduped = tuple(dict.fromkeys(reasons))
        return WriteGuardVerdict(safe=not deduped, blocked_reasons=deduped)

    def assert_write_allowed(self, subscriptions: Iterable[Any]) -> WriteGuardVerdict:
        verdict = self.preflight(subscriptions)
        if not verdict.safe:
            raise DiscoveryGuardError(verdict)
        return verdict

    # ---- reporting ----
    def describe(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "require_anime": self.require_anime,
            "verified_rows": self.context.verified_rows,
            "unverified_rows": self.context.unverified_rows,
        }


__all__ = [
    "ANIMATION_GENRE_ID",
    "AnimeClassification",
    "DUPLICATE_OWNED",
    "DiscoveryGuard",
    "DiscoveryGuardError",
    "GENRE_UNKNOWN",
    "GuardContext",
    "NON_ANIME",
    "UNVERIFIED_COLLISION",
    "WriteGuardVerdict",
    "anime_exclusion_reason",
    "build_context",
    "classify_anime",
    "decision_identity",
    "decision_season_key",
    "legacy_owned_identity",
    "legacy_unverified_season",
]
