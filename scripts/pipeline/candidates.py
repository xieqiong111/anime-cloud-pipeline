"""Candidate scoring, selection, and per-run submission budgeting.

Discovery records source evidence; this module turns that evidence into an
explainable score and makes the one-candidate-per-episode decision.  No
external service is called here.  A separate runner may use
``SubmissionBudget`` after a candidate has been selected.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass
import datetime
from email.utils import parsedate_to_datetime
import json
import math
import re
import threading
from typing import Any, Optional

from .models import EpisodeKey, ReviewReason, Subscription
from .parsers import is_non_episode_media


_DEFAULT_WEIGHTS = {
    "include_weight": 10,
    "language_weight": 5,
    "release_group_weight": 3,
    "resolution_weight": 2,
    "collection_penalty": 4,
}
_EXPRESSION_SPLIT = re.compile(r"[,，;；|\s]+")
_TRUE_VALUES = {"1", "true", "yes", "on", "match", "matched", "preferred"}
_FALSE_VALUES = {"0", "false", "no", "off", "none", "null", ""}

# Deterministic duplicate tie-break evidence.  Only recorded release facts
# are compared, and every dimension is a pure function of the candidate
# title/publishing time, so the same candidates always produce the same
# winner.  Quality evidence ranks first; publish time may only settle a
# tie that survives it, and only when every tied candidate parses to a
# distinct timestamp.  Anything still indistinguishable stays a review
# instead of a guess.
_TITLE_VERSION = re.compile(r"(?<![0-9A-Za-z])V(?P<n>[23])(?![0-9A-Za-z])", re.IGNORECASE)
# 1920x1080 / 1920 X 1080：分辨率数字总是跟在宽度与分隔符之后
_TITLE_RESOLUTION_X = re.compile(r"[xX×]\s*(2160|1080|720|480)(?![0-9])")
# 1080p / 480i / 1080 P：显式扫描制式后缀
_TITLE_RESOLUTION_PI = re.compile(r"(?<![0-9A-Za-z])(2160|1080|720|480)\s*[pPiI](?![0-9A-Za-z])")
_TITLE_DEPTH_10 = re.compile(r"10-?bit|Hi10", re.IGNORECASE)
_TITLE_DEPTH_8 = re.compile(r"8-?bit", re.IGNORECASE)
_TITLE_CODEC_HEVC = re.compile(r"HEVC|H\.?265|X\.?265", re.IGNORECASE)
_TITLE_CODEC_AVC = re.compile(r"AVC|H\.?264|X\.?264", re.IGNORECASE)
_TITLE_SUB_BOTH = re.compile(r"简繁")
_TITLE_SUB_ANY = re.compile(r"简繁|简日|简体|简中|繁體|繁体|简|CHS|CHT|GB")
_TITLE_CONTAINER_MKV = re.compile(r"MKV", re.IGNORECASE)
_TITLE_CONTAINER_MP4 = re.compile(r"MP4", re.IGNORECASE)
# Anime web-source consensus ranking ( Crunchyroll > Bahamut > ABEMA );
# the source is declared release evidence, and this stable, documented
# ordering replaces an arbitrary pick when quality fields tie.
_TITLE_SOURCE_CR = re.compile(r"\bCR\b|CR-J|Crunchyroll", re.IGNORECASE)
_TITLE_SOURCE_BAHA = re.compile(r"\bBaha\b|Bahamut|B-Global|Baha", re.IGNORECASE)
_TITLE_SOURCE_ABEMA = re.compile(r"\bABEMA\b", re.IGNORECASE)
_TITLE_SEASON_CN = re.compile(r"第\s*(?P<c>[一二三四五六七八九十0-9]{1,3})\s*季")
_TITLE_SEASON_LATIN = re.compile(r"(?<![0-9A-Za-z])(?:Season\s*|S)(?P<n>[0-9]{1,2})(?![0-9])", re.IGNORECASE)
_CN_DIGITS = {"一": 1, "二": 2, "三": 3, "四": 4, "五": 5, "六": 6, "七": 7, "八": 8, "九": 9, "十": 10}


def _declared_season(title: str) -> int | None:
    """Return the season number a release title explicitly declares."""

    cn = _TITLE_SEASON_CN.search(title)
    if cn:
        raw = cn.group("c")
        if raw.isdigit():
            return int(raw)
        if raw in _CN_DIGITS:
            return _CN_DIGITS[raw]
        return None
    latin = _TITLE_SEASON_LATIN.search(title)
    if latin:
        return int(latin.group("n"))
    return None


def _source_rank(title: str) -> int:
    if _TITLE_SOURCE_CR.search(title):
        return 3
    if _TITLE_SOURCE_BAHA.search(title):
        return 2
    if _TITLE_SOURCE_ABEMA.search(title):
        return 1
    return 0


def _tiebreak_key(candidate: Any, expected_season: int | None = None) -> tuple[int, ...]:
    """Rank one equal-scored candidate by recorded release evidence.

    Higher tuples win.  Declared quality beats an undeclared value, and an
    undeclared value beats a declared worse one, so silent releases are
    neither punished nor rewarded against proven facts.  A title that
    explicitly declares a different season than the target episode marks a
    candidate whose content cannot be the wanted episode.  Publish time is
    not part of the key; it is only consulted by ``_duplicate_tiebreak_winner``
    after quality evidence ties.
    """

    values = _candidate_values(candidate)
    title = _text(values.get("title") or values.get("name")) or ""
    version = _TITLE_VERSION.search(title)
    resolution = _TITLE_RESOLUTION_X.search(title) or _TITLE_RESOLUTION_PI.search(title)
    if _TITLE_DEPTH_10.search(title):
        depth = 2
    elif _TITLE_DEPTH_8.search(title):
        depth = 0
    else:
        depth = 1
    if _TITLE_CODEC_HEVC.search(title):
        codec = 2
    elif _TITLE_CODEC_AVC.search(title):
        codec = 0
    else:
        codec = 1
    if _TITLE_SUB_BOTH.search(title):
        subtitle = 2
    elif _TITLE_SUB_ANY.search(title):
        subtitle = 1
    else:
        subtitle = 0
    if _TITLE_CONTAINER_MKV.search(title):
        container = 2
    elif _TITLE_CONTAINER_MP4.search(title):
        container = 0
    else:
        container = 1
    declared = _declared_season(title)
    if declared is None:
        season_match = 1
    elif expected_season is not None and declared == expected_season:
        season_match = 2
    elif expected_season is not None:
        season_match = 0
    else:
        season_match = 1
    return (
        season_match,
        int(version.group("n")) if version else 0,
        int(resolution.group(1)) if resolution else 0,
        depth,
        codec,
        subtitle,
        _source_rank(title),
        container,
    )


def _published_epoch(published: str) -> int | None:
    """Return the UTC epoch seconds of a publish timestamp, or ``None``.

    RFC 2822 timestamps are parsed first, then ISO 8601; a naive timestamp
    is read as UTC.  Anything unparseable returns ``None`` so an unknown or
    malformed time never imitates a proven one.
    """

    text = _text(published)
    if not text:
        return None
    parsed = None
    try:
        parsed = parsedate_to_datetime(text)
    except (TypeError, ValueError):
        parsed = None
    if parsed is None:
        try:
            parsed = datetime.datetime.fromisoformat(text)
        except (TypeError, ValueError):
            return None
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=datetime.timezone.utc)
    return int(parsed.timestamp())


def _duplicate_tiebreak_winner(
    tied: list[tuple[Any, Any]],
    expected_season: int | None = None,
) -> tuple[Any, Any] | None:
    """Return the single evidence-backed winner among equal candidates.

    Quality evidence decides first and never consults publish time.  Only
    when every tied candidate carries a distinct parseable publish
    timestamp does the newest one win.  Returns ``None`` when the evidence
    cannot distinguish them; the caller then keeps the durable review open
    instead of publishing an arbitrary pick as if it were a proven best
    version.
    """

    keyed = [
        (row, score, _tiebreak_key(row, expected_season))
        for row, score in tied
    ]
    best = max(key for _row, _score, key in keyed)
    winners = [(row, score) for row, score, key in keyed if key == best]
    if len(winners) == 1:
        return winners[0]
    epochs: list[int] = []
    for row, _score in winners:
        values = _candidate_values(row)
        published = _text(values.get("published") or values.get("pubdate") or "")
        epoch = _published_epoch(published)
        if epoch is None:
            return None
        epochs.append(epoch)
    if len(set(epochs)) != len(epochs):
        return None
    return winners[max(range(len(epochs)), key=lambda index: epochs[index])]


@dataclass(frozen=True, slots=True)
class CandidateInput:
    """Small immutable source candidate value accepted by the selector."""

    infohash: str
    title: str
    provider: str = ""
    source_id: str = ""
    source_url: str = ""
    published: str = ""
    season: Optional[int] = None
    episode: Optional[int] = None
    episode_end: Optional[int] = None
    release_group: str = ""
    resolution: str = ""
    language: str = ""
    collection: bool = False
    raw: Mapping[str, Any] | None = None


@dataclass(frozen=True, slots=True)
class CandidateScore:
    """Explainable score for one candidate.

    ``components`` contains weighted contributions.  ``matches`` retains the
    semantic booleans so a score remains auditable even if a weight changes.
    """

    total: float
    components: Mapping[str, float]
    matches: Mapping[str, bool]
    decision_reason: str = "eligible"
    excluded: bool = False

    @property
    def score(self) -> float:
        """Compatibility spelling for callers that use ``score``."""

        return self.total

    @property
    def reason(self) -> str:
        return self.decision_reason

    def to_mapping(self) -> dict[str, Any]:
        return {
            "total": self.total,
            "score": self.total,
            "components": dict(self.components),
            "matches": dict(self.matches),
            "decision_reason": self.decision_reason,
            "excluded": self.excluded,
        }


@dataclass(frozen=True, slots=True)
class SelectionResult:
    """Result of selecting candidates for one Episode identity."""

    candidate_id: Optional[int] = None
    review_reason: Optional[ReviewReason] = None
    scores: tuple[CandidateScore, ...] = ()
    decision_reason: str = ""

    @property
    def selected_candidate_id(self) -> Optional[int]:
        return self.candidate_id

    @property
    def reason(self) -> str:
        return self.decision_reason


class SubmissionBudget:
    """A global and per-subscription submission counter for one run.

    The object has no persistence side effects.  Creating or updating a
    candidate therefore never consumes capacity; callers reserve only when a
    genuinely new external submission is about to happen.
    """

    def __init__(
        self,
        global_limit: int | None = None,
        per_subscription: int | None = None,
        *,
        global_submission_limit: int | None = None,
        per_subscription_limit: int | None = None,
        per_subscription_submission_limit: int | None = None,
        max_new_tasks_per_run: int | None = None,
        max_new_tasks_per_subscription: int | None = None,
        limits: Any = None,
    ) -> None:
        if limits is not None:
            global_limit = global_limit if global_limit is not None else _read_limit(
                limits, "max_new_tasks_per_run", "global_submission_limit"
            )
            per_subscription = per_subscription if per_subscription is not None else _read_limit(
                limits,
                "max_new_tasks_per_subscription",
                "per_subscription_submission_limit",
            )
        if global_limit is None:
            global_limit = global_submission_limit
        if global_limit is None:
            global_limit = max_new_tasks_per_run
        if per_subscription is None:
            per_subscription = per_subscription_limit
        if per_subscription is None:
            per_subscription = per_subscription_submission_limit
        if per_subscription is None:
            per_subscription = max_new_tasks_per_subscription
        if global_limit is None or per_subscription is None:
            raise ValueError("global_limit and per_subscription are required")
        self.global_limit = _non_negative_int(global_limit, "global_limit")
        self.per_subscription = _non_negative_int(per_subscription, "per_subscription")
        self.counts: dict[str, int] = {}
        self._lock = threading.Lock()

    @property
    def total(self) -> int:
        with self._lock:
            return sum(self.counts.values())

    @property
    def remaining_global(self) -> int:
        return max(0, self.global_limit - self.total)

    def reserve(self, subscription_id: str) -> bool:
        """Reserve one new submission if both limits still have capacity."""

        if isinstance(subscription_id, Mapping):
            subscription_id = subscription_id.get("subscription_id") or subscription_id.get("id")
        elif hasattr(subscription_id, "subscription_id"):
            subscription_id = getattr(subscription_id, "subscription_id")
        key = str(subscription_id or "").strip()
        if not key:
            raise ValueError("subscription_id must not be empty")
        with self._lock:
            current = self.counts.get(key, 0)
            total = sum(self.counts.values())
            if total >= self.global_limit or current >= self.per_subscription:
                return False
            self.counts[key] = current + 1
            return True


class CandidateSelector:
    """Score and transactionally select candidates for one Episode."""

    def __init__(
        self,
        store: Any,
        scoring: Any = None,
        *,
        weights: Any = None,
        config: Any = None,
        scoring_config: Any = None,
        subscription: Any = None,
    ) -> None:
        self.store = store
        source = scoring
        if source is None:
            source = weights
        if source is None:
            source = scoring_config
        if source is None:
            source = config
        self.weights = _weights(source)
        self.subscription = subscription

    def score(self, candidate: Any, subscription: Any) -> CandidateScore:
        """Return an explainable score without mutating state."""

        values = _candidate_values(candidate)
        sub = _subscription_values(subscription)
        title = _text(values.get("title") or values.get("name"))

        excluded_reason = _hard_exclusion_reason(values, sub, title)
        zero_components = {
            "include": 0.0,
            "language": 0.0,
            "release_group": 0.0,
            "resolution": 0.0,
            "collection_penalty": 0.0,
        }
        if excluded_reason:
            return CandidateScore(
                total=0.0,
                components=zero_components,
                matches={
                    "include": False,
                    "language": False,
                    "release_group": False,
                    "resolution": False,
                    "collection": False,
                },
                decision_reason=excluded_reason,
                excluded=True,
            )

        matches = {
            "include": _include_match(values, sub, title),
            "language": _language_match(values, sub, title),
            "release_group": _release_group_match(values, sub),
            "resolution": _resolution_match(values, sub),
            "collection": _collection_match(values, title),
        }
        resolution_component = _resolution_contribution(
            values, sub, title, self.weights["resolution_weight"]
        )
        matches["resolution"] = resolution_component > 0
        components = {
            "include": float(self.weights["include_weight"] if matches["include"] else 0),
            "language": float(self.weights["language_weight"] if matches["language"] else 0),
            "release_group": float(
                self.weights["release_group_weight"] if matches["release_group"] else 0
            ),
            "resolution": resolution_component,
            "collection_penalty": float(
                -self.weights["collection_penalty"] if matches["collection"] else 0
            ),
        }
        return CandidateScore(
            total=sum(components.values()),
            components=components,
            matches=matches,
            decision_reason="eligible",
            excluded=False,
        )

    def select(
        self,
        episode_id: int | EpisodeKey | Mapping[str, Any],
        subscription: Any = None,
    ) -> SelectionResult:
        """Score all candidates and select one unique semantic winner.

        A valid resolved duplicate review retains its explicit choice after
        eligibility checks. Otherwise a semantic tie clears selections and
        opens a duplicate review in one state-store transaction.
        """

        resolved_id = self._episode_id(episode_id)
        episode = self.store.get_episode(resolved_id)
        if episode is None:
            raise KeyError(f"episode {resolved_id} not found")
        selected_subscription = subscription or self.subscription or self._episode_subscription(episode)
        rows = self.store.get_candidates(resolved_id)
        scores: list[CandidateScore] = []
        for row in rows:
            result = self.score(row, selected_subscription)
            scores.append(result)
            self.store.update_candidate_score(
                int(row["id"]),
                result.total,
                result.components,
                matches=result.matches,
                decision_reason=result.decision_reason,
            )

        eligible = [
            (row, score)
            for row, score in zip(rows, scores)
            if not score.excluded
        ]
        if episode.get("review_reason") == ReviewReason.DUPLICATE_EPISODE.value:
            # The review transaction already selected the operator's choice.
            # Re-scoring must validate it, not replace it with another tie.
            resolution = episode.get("review_resolution")
            if isinstance(resolution, str):
                try:
                    resolution = json.loads(resolution)
                except ValueError:
                    resolution = None
            choices = [resolution[key] for key in ("selected_candidate_id", "candidate_id")
                       if isinstance(resolution, Mapping) and key in resolution]
            choice = choices[0] if choices else None
            resolved_at = episode.get("review_resolved_at")
            selected = [int(row["id"]) for row in rows if row.get("selected")]
            chosen = next(((row, score) for row, score in eligible
                           if row["id"] == choice), None)
            if (choices and all(type(value) is int and value > 0 and value == choice
                                for value in choices)
                    and type(resolved_at) in (int, float)
                    and math.isfinite(resolved_at) and resolved_at > 0
                    and selected == [choice] and chosen is not None):
                return SelectionResult(
                    candidate_id=choice,
                    scores=tuple(scores),
                    decision_reason="selected_review_resolution",
                )
            self.store.review_duplicate_candidates(
                resolved_id,
                {"candidate_ids": [int(row["id"]) for row in rows],
                 "score": max((score.total for _, score in eligible), default=0.0),
                 "reason": ReviewReason.DUPLICATE_EPISODE.value},
            )
            return SelectionResult(
                review_reason=ReviewReason.DUPLICATE_EPISODE,
                scores=tuple(scores),
                decision_reason=ReviewReason.DUPLICATE_EPISODE.value,
            )
        if not eligible:
            self.store.clear_candidate_selection(resolved_id)
            return SelectionResult(
                candidate_id=None,
                scores=tuple(scores),
                decision_reason="no_eligible_candidates",
            )

        best_score = max(score.total for _, score in eligible)
        tied = [
            (row, score)
            for row, score in eligible
            if math.isclose(score.total, best_score, rel_tol=0.0, abs_tol=1e-9)
        ]
        if len(tied) > 1:
            expected_season = _int_or_none(episode["season"] if "season" in episode.keys() else None)
            winner_pair = _duplicate_tiebreak_winner(tied, expected_season=expected_season)
        else:
            winner_pair = None
        if len(tied) > 1 and winner_pair is not None:
            winner, winner_score = winner_pair
            winner_id = int(winner["id"])
            for row, score in tied:
                self.store.update_candidate_score(
                    int(row["id"]),
                    score.total,
                    score.components,
                    matches=score.matches,
                    decision_reason=(
                        "selected_duplicate_tiebreak"
                        if int(row["id"]) == winner_id
                        else "duplicate_tiebreak_loser"
                    ),
                )
            self.store.select_candidate(winner_id)
            return SelectionResult(
                candidate_id=winner_id,
                scores=tuple(scores),
                decision_reason="selected_duplicate_tiebreak",
            )
        if len(tied) > 1:
            tie_ids = [int(row["id"]) for row, _ in tied]
            for row, score in tied:
                self.store.update_candidate_score(
                    int(row["id"]),
                    score.total,
                    score.components,
                    matches=score.matches,
                    decision_reason=ReviewReason.DUPLICATE_EPISODE.value,
                )
            self.store.review_duplicate_candidates(
                resolved_id,
                {
                    "candidate_ids": tie_ids,
                    "score": best_score,
                    "reason": ReviewReason.DUPLICATE_EPISODE.value,
                },
            )
            return SelectionResult(
                candidate_id=None,
                review_reason=ReviewReason.DUPLICATE_EPISODE,
                scores=tuple(scores),
                decision_reason=ReviewReason.DUPLICATE_EPISODE.value,
            )

        winner, winner_score = tied[0]
        winner_id = int(winner["id"])
        self.store.update_candidate_score(
            winner_id,
            winner_score.total,
            winner_score.components,
            matches=winner_score.matches,
            decision_reason="selected",
        )
        self.store.select_candidate(winner_id)
        return SelectionResult(
            candidate_id=winner_id,
            scores=tuple(scores),
            decision_reason="selected",
        )

    # Naming used by early orchestration drafts and operator-facing tests.
    select_episode = select

    def _episode_id(self, value: int | EpisodeKey | Mapping[str, Any]) -> int:
        if isinstance(value, bool):
            raise TypeError("episode_id must be an integer or EpisodeKey")
        if isinstance(value, int):
            return value
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
        key: Any = value
        if isinstance(value, Mapping):
            if value.get("id") is not None and value.get("key") is None and value.get("episode_key") is None:
                return _integer(value.get("id"), default=0)
            key = value.get("key") or value.get("episode_key") or value
        if isinstance(key, EpisodeKey):
            row = self.store.get_episode_by_key(key)
        elif isinstance(key, Mapping):
            row = self.store.get_episode_by_key(
                EpisodeKey(
                    str(key.get("media_source") or "legacy"),
                    str(key.get("media_id") or ""),
                    int(key.get("season") or 0),
                    int(key.get("episode") or 0),
                )
            )
        else:
            raise TypeError("episode_id must be an integer or EpisodeKey")
        if row is None:
            raise KeyError("episode identity not found")
        return int(row["id"])

    def _episode_subscription(self, episode: Mapping[str, Any]) -> Subscription:
        subscription_id = _text(episode.get("subscription_id"))
        row = None
        connection = getattr(self.store, "conn", None)
        if connection is not None and subscription_id:
            row = connection.execute(
                """SELECT * FROM subscriptions
                   WHERE subscription_id=? AND media_source=? AND media_id=? AND season=?
                   ORDER BY id DESC LIMIT 1""",
                (
                    subscription_id,
                    episode.get("media_source"),
                    episode.get("media_id"),
                    episode.get("season"),
                ),
            ).fetchone()
        if row is not None:
            values = dict(row)
            aliases = _json_list(values.get("aliases_json"))
            return Subscription(
                subscription_id=subscription_id,
                media_source=_text(values.get("media_source")) or "legacy",
                media_id=_text(values.get("media_id")) or "unknown",
                work=_text(values.get("work")) or _text(episode.get("canonical_work")) or "unknown",
                aliases=tuple(aliases),
                season=_integer(values.get("season"), default=0),
                include=_text(values.get("include_text")),
                exclude=_text(values.get("exclude_text")),
            )
        return Subscription(
            subscription_id=subscription_id or "episode",
            media_source=_text(episode.get("media_source")) or "legacy",
            media_id=_text(episode.get("media_id")) or "unknown",
            work=_text(episode.get("canonical_work")) or _text(episode.get("title")) or "unknown",
            aliases=tuple(_json_list(episode.get("aliases_json"))),
            season=_integer(episode.get("season"), default=0),
            include="",
            exclude="",
        )


def _weights(source: Any) -> dict[str, int]:
    values: Mapping[str, Any] = {}
    if source is not None:
        section = getattr(source, "scoring", source)
        section = getattr(section, "weights", section)
        if isinstance(section, Mapping):
            if isinstance(section.get("scoring"), Mapping):
                section = section["scoring"]
            if isinstance(section.get("weights"), Mapping):
                section = section["weights"]
            values = section
    result: dict[str, int] = {}
    for key, default in _DEFAULT_WEIGHTS.items():
        value = values.get(key, default)
        try:
            number = int(value)
        except (TypeError, ValueError):
            number = default
        result[key] = max(0, number)
    return result


def _read_limit(source: Any, *names: str) -> Any:
    if isinstance(source, Mapping):
        values = source
    else:
        values = {
            name: getattr(source, name, None)
            for name in names
            if getattr(source, name, None) is not None
        }
    for name in names:
        if name in values:
            return values[name]
    return None


def _non_negative_int(value: Any, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be a non-negative integer")
    try:
        parsed = int(value)
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{field} must be a non-negative integer") from exc
    if parsed < 0:
        raise ValueError(f"{field} must be a non-negative integer")
    return parsed


def _candidate_values(candidate: Any) -> dict[str, Any]:
    if isinstance(candidate, Mapping):
        values = dict(candidate)
    elif hasattr(candidate, "keys"):
        values = {key: candidate[key] for key in candidate.keys()}
    else:
        values = {
            key: getattr(candidate, key)
            for key in (
                "id", "infohash", "title", "provider", "source_id", "source_url",
                "published", "season", "episode", "episode_end", "release_group",
                "resolution", "language", "collection", "raw", "decision",
            )
            if hasattr(candidate, key)
        }
    traits = values.get("traits") or values.get("traits_json")
    if isinstance(traits, str):
        try:
            traits = json.loads(traits)
        except (TypeError, ValueError):
            traits = {}
    if isinstance(traits, Mapping):
        for key, value in traits.items():
            values.setdefault(key, value)
    raw = values.get("raw")
    if isinstance(raw, Mapping):
        for key, value in raw.items():
            values.setdefault(key, value)
    return values


def _subscription_values(subscription: Any) -> dict[str, Any]:
    if isinstance(subscription, Mapping):
        return dict(subscription)
    if hasattr(subscription, "keys"):
        return {key: subscription[key] for key in subscription.keys()}
    if isinstance(subscription, Subscription):
        return {
            "subscription_id": subscription.subscription_id,
            "media_source": subscription.media_source,
            "media_id": subscription.media_id,
            "work": subscription.work,
            "aliases": subscription.aliases,
            "season": subscription.season,
            "include": subscription.include,
            "exclude": subscription.exclude,
        }
    if subscription is None:
        return {}
    return {
        key: getattr(subscription, key)
        for key in (
            "subscription_id", "media_source", "media_id", "work", "aliases",
            "season", "include", "exclude", "language", "resolution",
            "release_group", "release_groups", "preferred_language",
            "preferred_resolution", "preferred_release_groups", "exclude_keywords",
        )
        if hasattr(subscription, key)
    }


def _hard_exclusion_reason(values: Mapping[str, Any], sub: Mapping[str, Any], title: str) -> str:
    if _bool_value(values.get("is_episode")) is False:
        return "non_episode_media"
    if _bool_value(values.get("non_episode")) or _bool_value(values.get("is_non_episode")):
        return "non_episode_media"
    media_types = [
        _safe_scalar_text(values.get(name)).casefold()
        for name in ("media_type", "content_type", "kind", "type")
    ]
    if any(
        media_type
        and media_type not in {"tv", "series", "show", "episode", "television"}
        for media_type in media_types
    ):
        return "non_episode_media"
    marker = _safe_scalar_text(values.get("marker") or values.get("media_marker"))
    if is_non_episode_media(title, marker=marker):
        return "non_episode_media"
    if _expression_match(
        sub.get("exclude") or sub.get("exclude_text") or sub.get("exclude_keywords"),
        title,
    ):
        return "subscription_exclude"
    episode = values.get("episode")
    if episode is not None and _integer(episode, default=0) <= 0:
        return "episode_identity_invalid"
    decision = _safe_scalar_text(values.get("decision"))
    if decision in {"excluded", "needs_review"}:
        return _safe_scalar_text(
            values.get("reason") or values.get("decision_reason")
        ) or "discovery_excluded"
    return ""


def _include_match(values: Mapping[str, Any], sub: Mapping[str, Any], title: str) -> bool:
    explicit = _explicit_bool(values, "include_match", "preferred_include")
    if explicit is not None:
        return explicit
    return _expression_match(sub.get("include") or sub.get("include_text"), title)


def _language_match(values: Mapping[str, Any], sub: Mapping[str, Any], title: str) -> bool:
    explicit = _explicit_bool(values, "language_match", "preferred_language_match")
    if explicit is not None:
        return explicit
    candidate_languages = _as_text_values(
        values.get("language") or values.get("languages") or values.get("subtitle_language")
    )
    preferred = _as_text_values(
        sub.get("language") or sub.get("preferred_language") or sub.get("languages")
    )
    if candidate_languages and preferred:
        return _values_match(candidate_languages, preferred)
    if candidate_languages:
        return any(_expression_match(item, title) for item in candidate_languages)
    return any(
        marker in title.casefold()
        for marker in ("简体", "繁体", "简繁", "双语", "中文", "chs", "cht")
    )


def _release_group_match(values: Mapping[str, Any], sub: Mapping[str, Any]) -> bool:
    explicit = _explicit_bool(values, "release_group_match", "preferred_release_group_match")
    if explicit is not None:
        return explicit
    candidate = _as_text_values(values.get("release_group") or values.get("group"))
    preferred = _as_text_values(
        sub.get("release_group")
        or sub.get("preferred_release_group")
        or sub.get("release_groups")
        or sub.get("preferred_release_groups")
    )
    return bool(candidate and preferred and _values_match(candidate, preferred))


def _resolution_match(values: Mapping[str, Any], sub: Mapping[str, Any]) -> bool:
    explicit = _explicit_bool(values, "resolution_match", "preferred_resolution_match")
    if explicit is not None:
        return explicit
    candidate = _as_text_values(values.get("resolution") or values.get("quality"))
    preferred = _as_text_values(sub.get("resolution") or sub.get("preferred_resolution"))
    return bool(candidate and preferred and _values_match(candidate, preferred))


def _resolution_contribution(
    values: Mapping[str, Any],
    sub: Mapping[str, Any],
    title: str,
    weight: int,
) -> float:
    explicit = _explicit_bool(values, "resolution_match", "preferred_resolution_match")
    if explicit is not None:
        return float(weight if explicit else 0)
    candidate = _as_text_values(
        values.get("resolution") or values.get("quality") or _resolution_from_title(title)
    )
    if not candidate:
        return 0.0
    preferred = _as_text_values(sub.get("resolution") or sub.get("preferred_resolution"))
    if preferred:
        return float(weight if _values_match(candidate, preferred) else 0)
    rank = max((_resolution_rank(item) for item in candidate), default=0)
    return float(weight * rank) if rank else 0.0


def _resolution_from_title(title: str) -> str:
    match = re.search(r"(?:^|[\s._\-\[\]()])(4320p|2160p|1080p|720p|4k)(?:$|[\s._\-\[\]()])", title, re.IGNORECASE)
    return match.group(1) if match else ""


def _resolution_rank(value: str) -> int:
    normalized = _text(value).casefold().replace(" ", "")
    if normalized == "4k":
        return 3
    match = re.search(r"(\d{3,4})p", normalized)
    if not match:
        return 0
    number = int(match.group(1))
    if number >= 4320:
        return 4
    if number >= 2160:
        return 3
    if number >= 1080:
        return 2
    if number >= 720:
        return 1
    return 0


def _collection_match(values: Mapping[str, Any], title: str) -> bool:
    explicit = _explicit_bool(values, "collection", "is_collection", "collection_match")
    if explicit is not None:
        return explicit
    start = _integer(values.get("episode"), default=0)
    end = _integer(values.get("episode_end"), default=0)
    if end > start > 0:
        return True
    lowered = title.casefold()
    return any(marker in lowered for marker in ("batch", "complete", "合集", "全集"))


def _explicit_bool(values: Mapping[str, Any], *names: str) -> Optional[bool]:
    for name in names:
        if name in values and values[name] is not None:
            return _bool_value(values[name])
    return None


def _bool_value(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if not isinstance(value, (str, int, float)):
        return None
    text = str(value).strip().casefold()
    if text in _TRUE_VALUES:
        return True
    if text in _FALSE_VALUES:
        return False
    return None


def _expression_match(expression: Any, title: str) -> bool:
    if expression is None:
        return False
    values = expression if isinstance(expression, (list, tuple, set)) else _EXPRESSION_SPLIT.split(_text(expression))
    lowered = title.casefold()
    return any(_text(value) and _text(value).casefold() in lowered for value in values)


def _as_text_values(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, (list, tuple, set)):
        return [_text(item) for item in value if _text(item)]
    return [_text(item) for item in _EXPRESSION_SPLIT.split(_text(value)) if _text(item)]


def _values_match(left: list[str], right: list[str]) -> bool:
    right_folded = [item.casefold() for item in right]
    return any(
        item.casefold() == candidate
        or item.casefold() in candidate
        or candidate in item.casefold()
        for item in left
        for candidate in right_folded
    )


def _text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def _int_or_none(value: Any) -> int | None:
    if isinstance(value, bool) or value is None:
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _safe_scalar_text(value: Any) -> str:
    """Normalize only scalar source values used in enum-like decisions."""

    if value is None or not isinstance(value, (str, int, float, bool)):
        return ""
    return str(value).strip()


def _integer(value: Any, *, default: int) -> int:
    try:
        if value is None or isinstance(value, bool):
            return default
        return int(value)
    except (TypeError, ValueError):
        return default


def _json_list(value: Any) -> list[Any]:
    if isinstance(value, (list, tuple)):
        return list(value)
    if isinstance(value, str):
        try:
            parsed = json.loads(value)
        except (TypeError, ValueError):
            return []
        return list(parsed) if isinstance(parsed, (list, tuple)) else []
    return []


__all__ = [
    "CandidateInput",
    "CandidateScore",
    "CandidateSelector",
    "SelectionResult",
    "SubmissionBudget",
]
