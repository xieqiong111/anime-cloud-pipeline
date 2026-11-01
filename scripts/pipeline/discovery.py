"""Shared, side-effect-bounded subscription discovery preparation.

``DiscoveryService.prepare`` is the one path used by dry-run and run.  It
fetches the MoviePilot snapshot, normalizes source results, parses episode
identity, and emits immutable decisions.  Run mode additionally writes the
same normalized records to ``StateStore``; it never submits a download.
"""
from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, replace
import datetime as dt
import email.utils
import hashlib
import json
import re
from types import MappingProxyType
from typing import Any, Iterable, Optional
from zoneinfo import ZoneInfo

from .adapters import FakeSourceProvider, MoviePilotAdapter
from .candidates import CandidateSelector
from .models import EpisodeKey, Subscription
from .parsers import SourceItem, is_non_episode_media, parse_season_episode
from .paths import SeasonRoutingError, season_label_from_air_date
from .security import sanitize_text
from .diagnostics import stage
from .subscriptions import (
    SubscriptionError,
    exclusion_reason,
    normalize_subscription,
    subscription_identifier,
)


class DiscoveryPreparationError(RuntimeError):
    """The subscription source itself could not produce a usable snapshot."""


@dataclass(frozen=True, slots=True)
class DiscoveryError:
    subscription_id: str
    message: str
    provider: str = ""
    field: str = ""

    def to_mapping(self) -> dict[str, str]:
        return {
            "subscription_id": self.subscription_id,
            "provider": self.provider,
            "field": self.field,
            "message": self.message,
        }


@dataclass(frozen=True, slots=True)
class DiscoveryExclusion:
    subscription_id: str
    reason: str
    detail: str = ""

    def to_mapping(self) -> dict[str, str]:
        return {
            "subscription_id": self.subscription_id,
            "reason": self.reason,
            "detail": self.detail,
        }


@dataclass(frozen=True, slots=True)
class CandidateDecision:
    """A normalized source result and its discovery decision."""

    subscription_id: str
    media_source: str
    media_id: str
    work: str
    season: Optional[int]
    episode: Optional[int]
    infohash: str
    title: str
    provider: str = ""
    source_id: str = ""
    source_url: str = ""
    torrent_url: str = ""
    published: str = ""
    episode_end: Optional[int] = None
    language: str = ""
    release_group: str = ""
    resolution: str = ""
    collection: bool = False
    decision: str = "candidate"
    reason: str = ""
    season_air_date: str = ""

    @property
    def episode_key(self) -> EpisodeKey | None:
        if self.season is None or self.episode is None:
            return None
        try:
            return EpisodeKey(self.media_source, self.media_id, self.season, self.episode)
        except ValueError:
            return None

    def to_mapping(self) -> dict[str, Any]:
        return {
            "subscription_id": self.subscription_id,
            "media_source": self.media_source,
            "media_id": self.media_id,
            "work": self.work,
            "season": self.season,
            "episode": self.episode,
            "episode_end": self.episode_end,
            "language": self.language,
            "release_group": self.release_group,
            "resolution": self.resolution,
            "collection": self.collection,
            "infohash": self.infohash,
            "title": self.title,
            "provider": self.provider,
            "source_id": self.source_id,
            "source_url": self.source_url,
            "torrent_url": self.torrent_url,
            "published": self.published,
            "decision": self.decision,
            "reason": self.reason,
            "season_air_date": self.season_air_date,
        }


@dataclass(frozen=True, slots=True)
class DiscoveryPlan:
    """Immutable output shared by dry-run and write-mode preparation."""

    subscriptions: tuple[Subscription, ...] = ()
    decisions: tuple[CandidateDecision, ...] = ()
    errors: tuple[DiscoveryError, ...] = ()
    exclusions: tuple[DiscoveryExclusion, ...] = ()
    summary: Mapping[str, Any] = MappingProxyType({})
    window: Any = None

    def __post_init__(self) -> None:
        object.__setattr__(self, "subscriptions", tuple(self.subscriptions))
        object.__setattr__(self, "decisions", tuple(self.decisions))
        object.__setattr__(self, "errors", tuple(self.errors))
        object.__setattr__(self, "exclusions", tuple(self.exclusions))
        if not isinstance(self.summary, MappingProxyType):
            object.__setattr__(self, "summary", MappingProxyType(dict(self.summary)))

    def decision_projection(self) -> bytes:
        """Return a stable byte projection for dry-run/run parity checks."""

        return json.dumps(
            [decision.to_mapping() for decision in self.decisions],
            ensure_ascii=False,
            sort_keys=True,
            separators=(",", ":"),
        ).encode("utf-8")

    def to_mapping(self) -> dict[str, Any]:
        return {
            "subscriptions": [
                {
                    "subscription_id": sub.subscription_id,
                    "media_source": sub.media_source,
                    "media_id": sub.media_id,
                    "work": sub.work,
                    "aliases": list(sub.aliases),
                    "season": sub.season,
                    "include": sub.include,
                    "exclude": sub.exclude,
                    "season_air_date": sub.season_air_date,
                }
                for sub in self.subscriptions
            ],
            "decisions": [decision.to_mapping() for decision in self.decisions],
            "errors": [error.to_mapping() for error in self.errors],
            "exclusions": [item.to_mapping() for item in self.exclusions],
            "summary": dict(self.summary),
        }


class DiscoveryService:
    """Prepare identical discovery decisions for dry-run and run."""

    def __init__(
        self,
        store: Any,
        moviepilot: MoviePilotAdapter,
        providers: Iterable[Any] = (),
        config: Any = None,
        secrets: Iterable[Any] = (),
        guard: Any = None,
        subscription_snapshots: Iterable[Any] = (),
    ) -> None:
        self.store = store
        self.moviepilot = moviepilot
        self.providers = tuple(providers or ())
        self.config = config
        self.secrets = tuple(secrets or ())
        # Optional safety guard (V5 deployment). ``None`` keeps the historical
        # behaviour; when set it fails a full discovery closed before a
        # download (anime genre gate + legacy/TMDB identity dedup).
        self.guard = guard
        self.subscription_snapshots = tuple(subscription_snapshots)
        self._history_progress: dict[tuple, dict] = {}
        self._history_sequence = 0

    def prepare(self, window: Any = None, read_only: bool = False) -> DiscoveryPlan:
        """Fetch, normalize, and optionally persist one discovery snapshot.

        Read-only mode never touches ``store``.  In write mode each successful
        subscription is persisted independently, so a later provider failure
        cannot roll back or erase earlier state.
        """

        if not read_only and (
            not isinstance(window, Mapping) or not window.get("season_quarter")
        ):
            raise ValueError("writable discovery requires a current-quarter window")

        subscriptions: list[Subscription] = []
        decisions: list[CandidateDecision] = []
        errors: list[DiscoveryError] = []
        exclusions: list[DiscoveryExclusion] = []
        backfill = {"aired_episodes": 0, "history_queries": 0,
                    "source_unavailable": 0, "deferred_episodes": 0, "query_errors": 0}

        if not read_only and self.store is None:
            raise ValueError("store is required for write-mode discovery")

        try:
            raw_subscriptions = self.moviepilot.list_subscriptions()
        except Exception as exc:
            errors.append(
                DiscoveryError(
                    "<unknown>",
                    self._safe_error(f"subscription source failed ({type(exc).__name__})"),
                    provider=_text("moviepilot", self.secrets),
                )
            )
            return self._plan(window, subscriptions, decisions, errors, exclusions)
        if not isinstance(raw_subscriptions, list):
            errors.append(
                DiscoveryError(
                    "<unknown>",
                    self._safe_error("subscription source returned an unexpected shape"),
                    provider=_text("moviepilot", self.secrets),
                )
            )
            return self._plan(window, subscriptions, decisions, errors, exclusions)

        self._load_history_progress(read_only)
        if isinstance(window, Mapping) and window.get("auto_backfill"):
            raw_subscriptions = sorted(raw_subscriptions, key=self._history_priority)

        if self.guard is not None:
            self._bind_guard()

        if not read_only and self.guard is not None:
            self._assert_guard_write_allowed(raw_subscriptions, window)

        allowed_sources = _allowed_subscription_sources(self.config)

        for raw in raw_subscriptions:
            identifier = _text(subscription_identifier(raw), self.secrets)
            # First, local, deterministic gate: only subscriptions whose
            # MoviePilot ``username`` (subscription source) is explicitly
            # allowed may proceed.  A missing username fails closed so a
            # payload that drops the field can never bypass the filter.
            if allowed_sources:
                username = _subscription_source_username(raw)
                if not username:
                    exclusions.append(
                        DiscoveryExclusion(
                            identifier,
                            "subscription_source_missing",
                            "subscription source username is empty",
                        )
                    )
                    continue
                if username not in allowed_sources:
                    exclusions.append(
                        DiscoveryExclusion(
                            identifier,
                            "excluded_subscription_source",
                            f"subscription source '{username}' is not an allowed source",
                        )
                    )
                    continue
            reason = exclusion_reason(raw)
            if reason is not None:
                exclusions.append(
                    DiscoveryExclusion(identifier, reason, "excluded by explicit MoviePilot field")
                )
                continue
            if self.guard is not None:
                guarded_reason = self.guard.subscription_exclusion_reason(raw)
                if guarded_reason is not None:
                    exclusions.append(
                        DiscoveryExclusion(identifier, guarded_reason, "excluded by anime guard")
                    )
                    continue
            try:
                subscription = normalize_subscription(raw)
            except SubscriptionError as exc:
                errors.append(
                    DiscoveryError(
                        identifier,
                        self._safe_error(str(exc)),
                        provider=_text("moviepilot", self.secrets),
                        field=exc.field,
                    )
                )
                continue

            quarter = window.get("season_quarter") if isinstance(window, Mapping) else None
            if quarter:
                try:
                    season_label = season_label_from_air_date(subscription.season_air_date)
                except SeasonRoutingError:
                    exclusions.append(DiscoveryExclusion(identifier, "season_date_missing"))
                    continue
                if not season_label.startswith(str(quarter) + "月"):
                    exclusions.append(DiscoveryExclusion(identifier, "outside_current_quarter"))
                    continue

            aired = None
            if isinstance(window, Mapping) and window.get("auto_backfill"):
                try:
                    aired, unknown_dates = self._aired_episodes(subscription, window)
                except Exception as exc:
                    errors.append(DiscoveryError(
                        identifier,
                        self._safe_error(f"season episode inventory unavailable ({type(exc).__name__})"),
                        provider="moviepilot", field="season_episodes",
                    ))
                    continue
                backfill["aired_episodes"] += len(aired)
                if unknown_dates:
                    exclusions.append(DiscoveryExclusion(
                        identifier, "episode_air_date_unknown",
                        f"{unknown_dates} episodes excluded from automatic backfill",
                    ))

            subscriptions.append(subscription)
            if not read_only:
                self._persist_subscription(subscription, raw)

            for provider in self.providers:
                provider_name = self._provider_name(provider)
                try:
                    source_results = self._source_results(
                        provider, subscription, window, aired, backfill, errors,
                    )
                    for source_result in source_results:
                        if not source_result_in_window(subscription, source_result, window):
                            continue
                        try:
                            decision = self._normalize_decision(
                                subscription, source_result, provider_name
                            )
                        except Exception as exc:
                            errors.append(
                                DiscoveryError(
                                    identifier,
                                    self._safe_error(
                                        f"source result rejected ({type(exc).__name__})"
                                    ),
                                    provider=_text(provider_name, self.secrets),
                                )
                            )
                            continue
                        if aired is not None and not self._decision_is_aired(decision, aired):
                            exclusions.append(DiscoveryExclusion(
                                identifier, "episode_not_verified_aired",
                                "candidate is outside the verified aired episode set",
                            ))
                            continue
                        if self.guard is not None:
                            duplicate = self.guard.decision_exclusion(decision)
                            if duplicate is not None:
                                exclusions.append(
                                    DiscoveryExclusion(
                                        identifier, duplicate, "deduped by anime guard"
                                    )
                                )
                                continue
                        decisions.append(decision)
                        if not read_only:
                            self._persist_decision(decision, subscription)
                except Exception as exc:
                    errors.append(
                        DiscoveryError(
                            identifier,
                            self._safe_error(f"provider failed ({type(exc).__name__})"),
                            provider=_text(provider_name, self.secrets),
                        )
                    )
            if not read_only and aired is not None:
                self._persist_subscription(subscription, raw)

        return self._plan(window, subscriptions, decisions, errors, exclusions, backfill)

    def _aired_episodes(self, subscription: Subscription, window: Mapping) -> tuple[set[int], int]:
        """Unknown dates never prove an aired episode, and invalid calendars fail closed."""
        if subscription.media_source.casefold() not in {"tmdb", "themoviedb"}:
            raise ValueError("episode_calendar_identity_invalid")
        today = dt.date.fromisoformat(str(window.get("as_of_date") or ""))
        reader = getattr(self.moviepilot, "list_season_episodes", None)
        if not callable(reader):
            raise ValueError("episode_calendar_unavailable")
        with stage("moviepilot.season_episodes"):
            entries = reader(subscription.media_id, subscription.season)
        cap = self.config.limits.max_recursive_files
        if not isinstance(entries, list) or not entries or len(entries) > cap:
            raise ValueError("episode_calendar_incomplete")
        seen, aired, unknown = set(), set(), 0
        for item in entries:
            if not isinstance(item, Mapping):
                raise ValueError("episode_calendar_invalid")
            number = item.get("episode_number")
            if (type(number) is not int or number <= 0 or number in seen
                    or type(item.get("season_number")) is not int
                    or item["season_number"] != subscription.season
                    or ("show_id" in item and str(item["show_id"]) != subscription.media_id)):
                raise ValueError("episode_calendar_identity_invalid")
            seen.add(number)
            date = item.get("air_date")
            if date in (None, ""):
                unknown += 1
                continue
            if not isinstance(date, str) or not re.fullmatch(r"\d{4}-\d{2}-\d{2}", date):
                raise ValueError("episode_calendar_date_invalid")
            if dt.date.fromisoformat(date) <= today:
                aired.add(number)
        return aired, unknown

    @staticmethod
    def _decision_is_aired(decision: CandidateDecision, aired: set[int]) -> bool:
        if decision.episode is None:
            return False
        end = decision.episode_end or decision.episode
        return (end >= decision.episode and end - decision.episode < len(aired)
                and all(number in aired for number in range(decision.episode, end + 1)))

    def _source_results(self, provider, subscription, window, aired, backfill, errors):
        """Supplement the normal feed with bounded, exact historical episode searches.

        Source absence is only an unavailable candidate, never inventory proof.
        Every yielded result still passes the common normalization and guard.
        """
        try:
            with stage("source.feed"):
                results = list(self._provider_discover(provider, subscription, window) or ())
        except Exception as exc:
            if aired is None:
                raise
            errors.append(DiscoveryError(
                _text(subscription.subscription_id, self.secrets),
                f"normal feed unavailable ({type(exc).__name__})",
                provider=_text(self._provider_name(provider), self.secrets), field="feed",
            ))
            results = []
        yield from results
        if aired is None or not aired:
            return
        covered = set()
        for result in results:
            if not source_result_in_window(subscription, result, window):
                continue
            try:
                decision = self._normalize_decision(subscription, result, self._provider_name(provider))
            except Exception:
                continue
            if decision.decision == "candidate" and self._decision_is_aired(decision, aired):
                # A collection currently creates only its starting episode row.
                # Do not hide the remaining episodes from historical searches.
                covered.add(decision.episode)
        # Tracking is queue ownership, not evidence that a file exists. Existing
        # rows resume via the unchanged runner rather than creating another job.
        context = getattr(self.guard, "context", None)
        for media_id, season, episode in getattr(context, "owned", {}):
            if media_id == subscription.media_id and season == subscription.season:
                covered.add(episode)
        missing_sources = sorted(aired - covered)
        if not missing_sources:
            return
        search = getattr(provider, "discover_episode", None)
        if not callable(search):
            raise ValueError("historical_episode_search_unavailable")
        progress = self._history_progress.setdefault(
            self._history_key(subscription), {"order": 0, "after_episode": {}},
        )
        provider_name = self._provider_name(provider)
        after = progress["after_episode"].get(provider_name, 0)
        ordered = [n for n in missing_sources if n > after] + [n for n in missing_sources if n <= after]
        remaining = max(0, self.config.limits.max_reconcile_batch - backfill["history_queries"])
        targets = ordered[:remaining]
        backfill["deferred_episodes"] += len(ordered) - len(targets)
        for number in targets:
            scope = {**window, "backfill": {"media_id": subscription.media_id,
                     "season": subscription.season, "start": number, "end": number},
                     "require_explicit_season": subscription.season != 1}
            backfill["history_queries"] += 1
            progress["after_episode"][provider_name] = number
            self._history_sequence += 1
            progress["order"] = self._history_sequence
            try:
                with stage("source.history"):
                    historical = list(search(subscription, number, scope) or ())
            except Exception as exc:
                backfill["query_errors"] += 1
                errors.append(DiscoveryError(
                    _text(subscription.subscription_id, self.secrets),
                    f"historical episode {number} search failed ({type(exc).__name__})",
                    provider=_text(provider_name, self.secrets), field="history_episode",
                ))
                continue
            found = False
            for result in historical:
                if not source_result_in_window(subscription, result, scope):
                    continue
                try:
                    decision = self._normalize_decision(subscription, result, provider_name)
                except Exception as exc:
                    errors.append(DiscoveryError(
                        _text(subscription.subscription_id, self.secrets),
                        f"historical source result rejected ({type(exc).__name__})",
                        provider=_text(provider_name, self.secrets), field="history_episode",
                    ))
                    continue
                if decision.decision == "candidate" and self._decision_is_aired(decision, aired):
                    found = True
                yield result
            if not found:
                backfill["source_unavailable"] += 1

    def _history_key(self, subscription: Subscription) -> tuple:
        return (
            self._storage_identity("media_source", subscription.media_source),
            self._storage_identity("media_id", subscription.media_id), subscription.season,
            self._storage_identity("subscription_id", subscription.subscription_id),
        )

    def _load_history_progress(self, read_only: bool) -> None:
        snapshots = self.subscription_snapshots
        reader = getattr(self.store, "subscriptions", None) if not read_only else None
        if callable(reader):
            snapshots = tuple(reader())
        self._history_progress = {}
        self._history_sequence = 0
        for row in snapshots:
            try:
                snapshot = json.loads(row["snapshot_json"])
                if not isinstance(snapshot, Mapping):
                    continue
                progress = snapshot.get("_v5_history_search")
                if (not isinstance(progress, dict) or type(progress.get("order")) is not int
                        or progress["order"] < 0 or not isinstance(progress.get("after_episode"), dict)
                        or any(not isinstance(k, str) or type(v) is not int or v < 0
                               for k, v in progress["after_episode"].items())):
                    continue
                key = (row["media_source"], row["media_id"], row["season"], row["subscription_id"])
                self._history_progress[key] = progress
                self._history_sequence = max(self._history_sequence, progress["order"])
            except (KeyError, TypeError, ValueError):
                continue

    def _history_priority(self, raw: Any) -> int:
        try:
            key = self._history_key(normalize_subscription(raw))
        except SubscriptionError:
            return 0
        return self._history_progress.get(key, {}).get("order", 0)

    def _bind_guard(self) -> None:
        """Recompute the guard's local ownership index from the state rows.

        In read-only mode the service has no store of its own; the caller may
        have already bound the guard to a read-only snapshot, so leave that
        context untouched instead of clearing it.
        """

        if self.store is None:
            return
        rows: list[Any] = []
        reader = getattr(self.store, "all_episodes", None)
        if callable(reader):
            try:
                rows = list(reader())
            except Exception:
                rows = []
        self.guard.bind(rows)

    def _assert_guard_write_allowed(self, raw_subscriptions: list, window: Any) -> None:
        """Fail a full discovery closed *before* any row is persisted."""

        quarter = window.get("season_quarter") if isinstance(window, Mapping) else None
        allowed_sources = _allowed_subscription_sources(self.config)
        prepared: list[Any] = []
        for raw in raw_subscriptions:
            if not isinstance(raw, Mapping):
                continue
            if allowed_sources:
                username = _subscription_source_username(raw)
                if not username or username not in allowed_sources:
                    continue
            if exclusion_reason(raw) is not None:
                continue
            if self.guard.subscription_exclusion_reason(raw) is not None:
                continue
            try:
                subscription = normalize_subscription(raw)
            except SubscriptionError:
                continue
            if quarter:
                try:
                    label = season_label_from_air_date(subscription.season_air_date)
                except SeasonRoutingError:
                    continue
                if not label.startswith(str(quarter) + "月"):
                    continue
            prepared.append(subscription)
        self.guard.assert_write_allowed(prepared)

    def _plan(
        self,
        window: Any,
        subscriptions: list[Subscription],
        decisions: list[CandidateDecision],
        errors: list[DiscoveryError],
        exclusions: list[DiscoveryExclusion],
        backfill: Mapping[str, int] | None = None,
    ) -> DiscoveryPlan:
        summary = {
            "subscription_count": len(subscriptions),
            "decision_count": len(decisions),
            "error_count": len(errors),
            "excluded_count": len(exclusions),
            "excluded": tuple(item.reason for item in exclusions),
            "exclusions": tuple(
                (item.subscription_id, item.reason) for item in exclusions
            ),
        }
        if isinstance(window, Mapping) and window.get("auto_backfill"):
            summary["backfill"] = dict(backfill or {})
        return DiscoveryPlan(
            subscriptions=tuple(self._sanitize_subscription(sub) for sub in subscriptions),
            decisions=tuple(self._sanitize_decision(item) for item in decisions),
            errors=tuple(errors),
            exclusions=tuple(exclusions),
            summary=summary,
            window=window,
        )

    def _persist_subscription(self, subscription: Subscription, raw: Any) -> None:
        # StateStore sanitizes JSON text before writing; keep the complete
        # snapshot for audit without making it a second source of truth.
        snapshot = raw
        if isinstance(raw, Mapping):
            snapshot = dict(raw)
            # Keep the exact verified routing field visible even when the
            # upstream payload used a nested TMDB season envelope.
            snapshot["season_air_date"] = subscription.season_air_date
            # Remote snapshots cannot supply or overwrite our local scan cursor.
            snapshot.pop("_v5_history_search", None)
            progress = self._history_progress.get(self._history_key(subscription))
            if progress is not None:
                snapshot["_v5_history_search"] = progress
        self.store.upsert_subscription(
            replace(self._sanitize_subscription(subscription), **{
                field: self._storage_identity(field, getattr(subscription, field))
                for field in ("subscription_id", "media_source", "media_id")
            }),
            snapshot=self._safe_snapshot(snapshot),
        )

    def _storage_identity(self, domain: str, value: str) -> str:
        """Keep redacted identities distinct without storing their plaintext.

        Hash the complete normalized component with a versioned domain, not its
        display placeholder. The reserved prefix is also encoded on raw input
        so a remote literal cannot impersonate another value's opaque identity.
        Ordinary unredacted identities remain compatible with existing state.
        """

        prefix = "opaque:v1:"
        if _text(value, self.secrets) == value and not value.startswith(prefix):
            return value
        material = json.dumps(
            ["discovery-identity-v1", domain, value], ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
        return prefix + hashlib.sha256(material).hexdigest()

    def _sanitize_decision(self, decision: CandidateDecision) -> CandidateDecision:
        """Create an output copy only after all discovery decisions are final."""

        return replace(decision, **{
            field: _text(getattr(decision, field), self.secrets)
            for field in (
                "subscription_id", "media_source", "media_id", "work", "infohash",
                "title", "provider", "source_id", "source_url", "torrent_url", "published",
                "language", "release_group", "resolution", "season_air_date",
            )
        })

    def _sanitize_subscription(self, subscription: Subscription) -> Subscription:
        """Keep configured credentials out of normalized plan/state fields."""

        return Subscription(
            subscription_id=_text(subscription.subscription_id, self.secrets),
            media_source=_text(subscription.media_source, self.secrets),
            media_id=_text(subscription.media_id, self.secrets),
            work=_text(subscription.work, self.secrets),
            aliases=tuple(
                alias
                for alias in (_text(value, self.secrets) for value in subscription.aliases)
                if alias
            ),
            season=subscription.season,
            include=_text(subscription.include, self.secrets),
            exclude=_text(subscription.exclude, self.secrets),
            season_air_date=_text(subscription.season_air_date, self.secrets),
        )

    def _safe_snapshot(self, raw: Any) -> str:
        try:
            encoded = json.dumps(raw, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            encoded = str(raw)
        return sanitize_text(encoded, secrets=self.secrets)

    def _persist_decision(
        self, decision: CandidateDecision, subscription: Subscription
    ) -> None:
        # Exclusions and unresolved identities remain visible in the dry-run
        # plan but must not become submit-ready candidate rows.
        if decision.decision != "candidate" or decision.episode_key is None:
            return
        score = CandidateSelector(None, config=self.config).score(decision, subscription)
        decision = replace(self._sanitize_decision(decision), **{
            field: self._storage_identity(field, getattr(decision, field))
            for field in ("subscription_id", "media_source", "media_id", "infohash")
        })
        key = decision.episode_key
        episode_id = self.store.get_or_create_episode(
            {
                "key": key,
                "subscription_id": decision.subscription_id,
                "canonical_work": decision.work,
                "aliases": tuple(_text(alias, self.secrets) for alias in subscription.aliases),
                "title": decision.title,
                "source_id": decision.source_id,
                "source_url": decision.source_url,
                "episode_end": decision.episode_end,
                "runtime_identity": {
                    **({
                        "_season_air_date": subscription.season_air_date,
                        "season_air_date": subscription.season_air_date,
                    } if subscription.season_air_date else {}),
                },
            }
        )
        self.store.add_candidate(
            episode_id,
            {
                "provider": decision.provider,
                "source_id": decision.source_id,
                "source_url": decision.source_url,
                "title": decision.title,
                "infohash": decision.infohash,
                "score": score.total,
                "reason": decision.reason,
                "traits": {
                    "decision": decision.decision,
                    "decision_reason": decision.reason,
                    "media_source": decision.media_source,
                    "media_id": decision.media_id,
                    "season": decision.season,
                    "episode": decision.episode,
                    "episode_end": decision.episode_end,
                    "language": decision.language,
                    "release_group": decision.release_group,
                    "resolution": decision.resolution,
                    "collection": decision.collection,
                    "score_components": dict(score.components),
                    "score_matches": dict(score.matches),
                    "score_total": score.total,
                    "season_air_date": decision.season_air_date,
                },
            },
        )

    def _provider_name(self, provider: Any) -> str:
        value = getattr(provider, "name", None) or getattr(provider, "provider", None)
        if value:
            return _plain_text(value)
        return type(provider).__name__

    @staticmethod
    def _provider_discover(provider: Any, subscription: Subscription, window: Any) -> Any:
        discover = getattr(provider, "discover", None)
        if callable(discover):
            return discover(subscription, window)
        if callable(provider):
            return provider(subscription, window)
        raise TypeError("provider has no discover operation")

    def _normalize_decision(
        self,
        subscription: Subscription,
        source_result: Any,
        provider_name: str,
    ) -> CandidateDecision:
        values = self._source_values(source_result)
        raw_title = _plain_text(values.get("title") or values.get("name"))
        if not raw_title:
            raise ValueError("source result title is blank")
        infohash = _plain_text(
            values.get("infohash")
            or values.get("hash")
            or values.get("btih"),
        ).lower()
        if not infohash:
            raise ValueError("source result infohash is blank")

        # Redaction may erase episode tokens or collapse distinct names into
        # one placeholder. Keep raw normalized data until the output boundary.
        parsed_season, parsed_episode, parsed_end = parse_season_episode(raw_title)
        explicit_season = _source_identity_int(
            values.get("season"), field="season"
        ) if values.get("season") not in (None, "") else parsed_season
        explicit_episode = _source_identity_int(
            values.get("episode"), field="episode"
        ) if values.get("episode") not in (None, "") else parsed_episode
        explicit_end = (
            _source_identity_int(values.get("episode_end"), field="episode_end")
            if values.get("episode_end") not in (None, "")
            else parsed_end
        )

        source_identity_conflict = (
            values.get("season") not in (None, "")
            and parsed_season is not None
            and explicit_season != parsed_season
        ) or (
            values.get("episode") not in (None, "")
            and parsed_episode is not None
            and explicit_episode != parsed_episode
        ) or (
            values.get("episode_end") not in (None, "")
            and parsed_end is not None
            and explicit_end != parsed_end
        )

        season = explicit_season
        episode = explicit_episode
        episode_end = explicit_end
        if season is None:
            season = subscription.season

        non_positive_identity = any(
            value is not None and value <= 0
            for value in (explicit_season, explicit_episode, explicit_end)
        )
        episode_range_invalid = (
            episode is not None
            and episode_end is not None
            and episode_end < episode
        )
        explicit_non_episode = _explicit_non_episode_marker(values)
        title_marker = _safe_scalar_text(
            values.get("marker") or values.get("media_marker")
        )

        decision = "candidate"
        reason = "eligible"
        if explicit_non_episode:
            decision, reason = "excluded", "non_episode_media"
        elif non_positive_identity:
            decision, reason = "needs_review", "episode_identity_invalid"
            season, episode, episode_end = None, None, None
        elif source_identity_conflict:
            decision, reason = "needs_review", "episode_identity_conflict"
            season, episode, episode_end = None, None, None
        elif episode_range_invalid:
            decision, reason = "needs_review", "episode_identity_invalid"
            season, episode, episode_end = None, None, None
        elif is_non_episode_media(
            raw_title,
            marker=title_marker,
        ):
            decision, reason = "excluded", "non_episode_media"
        elif episode is None:
            decision, reason = "needs_review", "episode_identity_unresolved"
        elif season != subscription.season:
            decision, reason = "needs_review", "season_mismatch"
            season, episode, episode_end = None, None, None
        elif _contains_excluded(raw_title, subscription.exclude):
            decision, reason = "excluded", "subscription_exclude"
        elif not _matches_work(raw_title, subscription):
            decision, reason = "excluded", "work_mismatch"

        return CandidateDecision(
            subscription_id=subscription.subscription_id,
            media_source=subscription.media_source,
            media_id=subscription.media_id,
            work=subscription.work,
            season=season,
            episode=episode,
            episode_end=episode_end,
            infohash=infohash,
            title=raw_title,
            provider=_plain_text(values.get("provider") or provider_name),
            source_id=_plain_text(values.get("source_id") or values.get("link")),
            source_url=_plain_text(values.get("source_url") or values.get("link")),
            torrent_url=_plain_text(values.get("torrent_url")),
            published=_plain_text(values.get("published")),
            language=_plain_text(
                values.get("language")
                or values.get("languages")
                or values.get("subtitle_language")
            ),
            release_group=_plain_text(
                values.get("release_group")
                or values.get("group")
                or ""
            ),
            resolution=_plain_text(
                values.get("resolution") or values.get("quality") or ""
            ),
            collection=(
                _safe_bool(values.get("collection")) is True
                or _safe_bool(values.get("is_collection")) is True
                or (
                    explicit_end is not None
                    and explicit_episode is not None
                    and explicit_end > explicit_episode
                )
            ),
            decision=decision,
            reason=reason,
            season_air_date=subscription.season_air_date,
        )

    @staticmethod
    def _source_values(source_result: Any) -> dict[str, Any]:
        if isinstance(source_result, SourceItem):
            return {
                "title": source_result.title,
                "link": source_result.link,
                "infohash": source_result.infohash,
                "torrent_url": source_result.torrent_url,
                "published": source_result.published,
                "season": source_result.season,
                "episode": source_result.episode,
                "episode_end": source_result.episode_end,
                "release_group": source_result.release_group,
                "resolution": source_result.resolution,
                "language": source_result.language,
                "collection": source_result.collection,
                **(source_result.raw or {}),
            }
        if isinstance(source_result, Mapping):
            return dict(source_result)
        raise TypeError("source result must be a mapping")

    def _safe_error(self, message: Any) -> str:
        return sanitize_text(message, secrets=self.secrets)


def _text(value: Any, secrets: Iterable[Any] = ()) -> str:
    return sanitize_text(_plain_text(value), secrets=secrets)


def _subscription_source_username(raw: Any) -> str:
    """Return the MoviePilot subscription-source ``username`` (may be empty)."""

    if isinstance(raw, Mapping):
        value = raw.get("username")
        if value is not None:
            return _plain_text(value)
    return ""


def _allowed_subscription_sources(config: Any) -> tuple[str, ...]:
    """Return the configured subscription-source allow-list, or ``()``.

    The allow-list lives at ``moviepilot.subscription_sources``.  An absent,
    ``None``, or empty value keeps discovery's historical unfiltered
    behaviour so nothing changes until an operator opts in.
    """

    moviepilot = getattr(config, "moviepilot", None)
    sources = getattr(moviepilot, "subscription_sources", ())
    if not sources:
        return ()
    names: list[str] = []
    for item in sources:
        text = _plain_text(item)
        if text:
            names.append(text)
    return tuple(names)


def _plain_text(value: Any) -> str:
    return "" if value is None else str(value).strip()


def source_result_in_window(
    subscription: Subscription,
    source_result: Any,
    window: Any,
) -> bool:
    """Return whether a provider result is proven to belong to ``window``.

    Discovery windows are enforced after every provider call and before
    normalization or persistence.  Missing date/episode evidence is rejected
    when the corresponding bounded window is requested; an unbounded window
    retains the historical provider behavior.
    """

    if not isinstance(window, Mapping):
        return True
    values = _window_source_values(source_result)
    if window.get("auto_backfill") and subscription.season != 1:
        declared_season = parse_season_episode(_plain_text(values.get("title") or values.get("name")))[0]
        if values.get("season") in (None, "") and declared_season is None:
            return False

    published_date = window.get("published_date")
    if published_date not in (None, ""):
        if not _published_on_date(
            values.get("published"),
            str(published_date),
            str(window.get("timezone") or "Asia/Shanghai"),
        ):
            return False

    backfill = window.get("backfill")
    if backfill is None:
        return True
    if not isinstance(backfill, Mapping):
        return False

    expected_media_id = _plain_text(backfill.get("media_id"))
    expected_media_source = _plain_text(backfill.get("media_source") or "tmdb").casefold()
    tmdb_sources = {"tmdb", "themoviedb"}
    # RSS entries inherit their work identity from the verified subscription,
    # exactly as normalization does. An explicit source identity may constrain
    # that context, but may never redirect it to another work or namespace.
    if (
        not expected_media_id
        or expected_media_source not in tmdb_sources
        or subscription.media_source.casefold() not in tmdb_sources
        or subscription.media_id != expected_media_id
    ):
        return False
    for name in ("media_id", "mediaId", "tmdbid", "tmdb_id"):
        if values.get(name) not in (None, "") and _plain_text(values[name]) != expected_media_id:
            return False
    for name in ("media_source", "mediaSource", "media_namespace"):
        if values.get(name) not in (None, "") and _plain_text(values[name]).casefold() not in tmdb_sources:
            return False

    title = _plain_text(values.get("title") or values.get("name"))
    parsed_season, parsed_episode, parsed_end = parse_season_episode(title)
    resolved = []
    for name, parsed in (("season", parsed_season), ("episode", parsed_episode), ("episode_end", parsed_end)):
        explicit = values.get(name)
        if explicit not in (None, ""):
            try:
                number = _source_identity_int(explicit, field=name)
            except ValueError:
                return False
            if number <= 0 or (parsed is not None and number != parsed):
                return False
            resolved.append(number)
        else:
            resolved.append(parsed)
    season, episode, episode_end = resolved
    if season is None:
        if window.get("require_explicit_season"):
            return False
        season = subscription.season
    if episode_end is None:
        episode_end = episode
    expected_season = _window_positive_int(backfill.get("season"))
    start = _window_positive_int(backfill.get("start"))
    end = _window_positive_int(backfill.get("end"))
    if None in (season, episode, episode_end, expected_season, start, end):
        return False
    if season != subscription.season or season != expected_season or start > end or episode_end < episode:
        return False
    # A collection is safe for a bounded backfill only when its complete
    # identity span is contained by the requested range.  Accepting an
    # overlap would persist the collection's starting episode (for example
    # E01) even when the operator requested E05 only.
    return int(episode) >= int(start) and int(episode_end) <= int(end)


def _window_source_values(source_result: Any) -> dict[str, Any]:
    if isinstance(source_result, SourceItem):
        return {
            "title": source_result.title,
            "link": source_result.link,
            "infohash": source_result.infohash,
            "torrent_url": source_result.torrent_url,
            "published": source_result.published,
            "season": source_result.season,
            "episode": source_result.episode,
            "episode_end": source_result.episode_end,
            **(source_result.raw or {}),
        }
    if isinstance(source_result, Mapping):
        return dict(source_result)
    return {}


def _first_window_value(values: Mapping[str, Any], names: tuple[str, ...]) -> Any:
    for name in names:
        if values.get(name) not in (None, ""):
            return values[name]
    return None


def _window_positive_int(value: Any) -> int | None:
    if value in (None, "") or isinstance(value, bool):
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed > 0 else None


def _published_on_date(value: Any, expected: str, timezone: str) -> bool:
    try:
        target = dt.date.fromisoformat(expected[:10])
        try:
            parsed = email.utils.parsedate_to_datetime(str(value or ""))
        except (TypeError, ValueError):
            parsed = dt.datetime.fromisoformat(str(value).replace("Z", "+00:00"))
        if parsed is None:
            return False
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return parsed.astimezone(ZoneInfo(timezone)).date() == target
    except (TypeError, ValueError, KeyError):
        return False


_NON_EPISODE_MEDIA_TYPES = frozenset({
    "movie", "film", "feature", "non_episode", "special", "ova", "电影", "电影订阅",
})
_TRUE_VALUES = frozenset({"1", "true", "yes", "on"})
_FALSE_VALUES = frozenset({"0", "false", "no", "off", "none", "null", ""})


def _safe_scalar_text(value: Any) -> str:
    """Return text only for scalar fields used as normalized markers."""

    if value is None or not isinstance(value, (str, int, float, bool)):
        return ""
    return str(value).strip()


def _safe_bool(value: Any) -> Optional[bool]:
    if value is None:
        return None
    if isinstance(value, bool):
        return value
    if not isinstance(value, (str, int, float)):
        return None
    normalized = str(value).strip().casefold()
    if normalized in _TRUE_VALUES:
        return True
    if normalized in _FALSE_VALUES:
        return False
    return None


def _explicit_non_episode_marker(values: Mapping[str, Any]) -> str:
    """Return a safe category for an explicit non-episode source marker."""

    if _safe_bool(values.get("is_episode")) is False:
        return "is_episode"
    if _safe_bool(values.get("non_episode")) is True:
        return "non_episode"
    if _safe_bool(values.get("is_non_episode")) is True:
        return "is_non_episode"
    for field in ("media_type", "content_type", "kind", "type"):
        media_type = _safe_scalar_text(values.get(field)).casefold()
        if media_type in _NON_EPISODE_MEDIA_TYPES:
            return "media_type"
    return ""


def _source_identity_int(value: Any, *, field: str) -> int:
    if isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    text = str(value).strip()
    digits = text[1:] if text[:1] in {"+", "-"} else text
    if not digits or not digits.isdigit():
        raise ValueError(f"{field} must be an integer")
    return int(text)


def _contains_excluded(title: str, exclude: str) -> bool:
    tokens = [token for token in re.split(r"[,，;；|\s]+", exclude or "") if token]
    lowered = title.casefold()
    return any(token.casefold() in lowered for token in tokens)


def _matches_work(title: str, subscription: Subscription) -> bool:
    lowered = title.casefold()
    names = (subscription.work,) + tuple(subscription.aliases)
    return any(name and name.casefold() in lowered for name in names)


__all__ = [
    "CandidateDecision",
    "DiscoveryError",
    "DiscoveryExclusion",
    "DiscoveryPlan",
    "DiscoveryPreparationError",
    "DiscoveryService",
    "FakeProvider",
    "FakeSourceProvider",
]


# Backwards-friendly test fixture spelling used by the task brief examples.
FakeProvider = FakeSourceProvider
