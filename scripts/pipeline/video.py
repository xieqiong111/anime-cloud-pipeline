"""Complete remote-video inventory selection.

The selector is deliberately pure until ``VideoSelectionResult.persist`` is
called.  A caller therefore has one obvious transaction boundary: materialize
the complete bounded inventory, select only unique primary files, then persist
all inventory rows before any episode-source relationship.
"""
from __future__ import annotations

from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
import os
import re
from typing import Any, Optional

from .adapters import RemoteInventoryIncomplete
from .models import EpisodeKey, RemoteDownload, RemoteFile, ReviewReason, SelectedVideo


DEFAULT_VIDEO_EXTENSIONS = frozenset({
    ".m2ts", ".mkv", ".mp4", ".ts", ".avi", ".mov", ".webm", ".flv", ".m4v",
})
DEFAULT_FEATURE_MARKERS = (
    "NCOP", "NCED", "OP", "ED", "PV", "SP", "SPECIAL", "TRAILER", "PREVIEW",
    "MENU", "BONUS", "CM", "CREDIT", "CREDITS", "OMAKE", "EXTRA",
    "片头", "片尾", "预告", "特典", "特别篇", "花絮", "菜单",
)


@dataclass(frozen=True, slots=True, init=False)
class VideoPolicy:
    """Validated knobs for remote video selection.

    Thresholds are opt-in.  A short file is not rejected merely because it is
    short: feature markers are the hard exclusion and a configured threshold is
    only an additional local policy.
    """

    supported_extensions: frozenset[str]
    min_duration: Optional[float]
    min_size: Optional[int]
    feature_markers: tuple[str, ...]
    max_episode: int

    def __init__(
        self,
        supported_extensions: Iterable[str] | None = None,
        *,
        video_extensions: Iterable[str] | None = None,
        min_duration: float | None = None,
        min_size: int | None = None,
        feature_markers: Iterable[str] | None = None,
        max_episode: int = 999,
    ) -> None:
        raw_extensions = (
            supported_extensions
            if supported_extensions is not None
            else video_extensions
        )
        extensions = frozenset(
            _normalize_extension(item) for item in (raw_extensions or DEFAULT_VIDEO_EXTENSIONS)
            if _normalize_extension(item)
        )
        if min_duration is not None and float(min_duration) < 0:
            raise ValueError("min_duration must be non-negative")
        if min_size is not None and int(min_size) < 0:
            raise ValueError("min_size must be non-negative")
        if int(max_episode) <= 0:
            raise ValueError("max_episode must be positive")
        markers = tuple(
            str(marker).strip().upper()
            for marker in (feature_markers or DEFAULT_FEATURE_MARKERS)
            if str(marker).strip()
        )
        object.__setattr__(self, "supported_extensions", extensions)
        object.__setattr__(self, "min_duration", None if min_duration is None else float(min_duration))
        object.__setattr__(self, "min_size", None if min_size is None else int(min_size))
        object.__setattr__(self, "feature_markers", markers)
        object.__setattr__(self, "max_episode", int(max_episode))

    @property
    def video_extensions(self) -> frozenset[str]:
        """Compatibility spelling used by adapter configuration."""

        return self.supported_extensions

    @classmethod
    def from_value(cls, value: Any) -> "VideoPolicy":
        if value is None:
            return cls()
        if isinstance(value, cls):
            return value
        if isinstance(value, Mapping):
            return cls(
                supported_extensions=value.get("supported_extensions")
                or value.get("video_extensions")
                or value.get("extensions"),
                min_duration=value.get("min_duration"),
                min_size=value.get("min_size"),
                feature_markers=value.get("feature_markers"),
                max_episode=value.get("max_episode", 999),
            )
        return cls(
            supported_extensions=getattr(value, "supported_extensions", None)
            or getattr(value, "video_extensions", None),
            min_duration=getattr(value, "min_duration", None),
            min_size=getattr(value, "min_size", None),
            feature_markers=getattr(value, "feature_markers", None),
            max_episode=getattr(value, "max_episode", 999),
        )


@dataclass(frozen=True, slots=True)
class RejectedVideo:
    """One inventory file excluded from selection with safe evidence."""

    file: RemoteFile
    reason: str
    detail: str = ""

    @property
    def filename(self) -> str:
        return self.file.filename

    @property
    def name(self) -> str:
        return self.file.filename

    @property
    def remote_file_id(self) -> Optional[str]:
        return self.file.remote_file_id


@dataclass(frozen=True, slots=True)
class _VideoCandidate:
    file: RemoteFile
    identity_rank: int
    marker_kind: str
    episodes: tuple[int, ...]
    seasons: tuple[int, ...]
    pairs: tuple[tuple[int, int], ...]

    @property
    def duration_score(self) -> float:
        return -1.0 if self.file.duration is None else float(self.file.duration)

    @property
    def size_score(self) -> float:
        return -1.0 if self.file.size is None else float(self.file.size)

    @property
    def score(self) -> tuple[float, float, float]:
        return (float(self.identity_rank), self.duration_score, self.size_score)


@dataclass
class VideoSelectionResult:
    """Selection evidence for a complete bounded inventory."""

    mapping: dict[EpisodeKey, SelectedVideo] = field(default_factory=dict)
    reviews: dict[EpisodeKey, ReviewReason] = field(default_factory=dict)
    candidates: dict[EpisodeKey, tuple[RemoteFile, ...]] = field(default_factory=dict)
    excluded: tuple[RejectedVideo, ...] = ()
    inventory: tuple[RemoteFile, ...] = ()
    complete: bool = True
    details: dict[EpisodeKey, dict[str, Any]] = field(default_factory=dict)

    @property
    def selected(self) -> dict[EpisodeKey, SelectedVideo]:
        return self.mapping

    @property
    def review_reasons(self) -> dict[EpisodeKey, ReviewReason]:
        return self.reviews

    @property
    def review(self) -> dict[EpisodeKey, ReviewReason]:
        """Singular compatibility alias for early orchestration drafts."""

        return self.reviews

    @property
    def excluded_files(self) -> tuple[RejectedVideo, ...]:
        return self.excluded

    @property
    def exclusion_reasons(self) -> dict[str, str]:
        """Return filename-to-reason evidence for operator-facing reports."""

        return {item.filename: item.reason for item in self.excluded}

    def persist(
        self,
        store: Any,
        download: int | str | RemoteDownload,
        *,
        episode_ids: Mapping[EpisodeKey, int] | None = None,
    ) -> list[int]:
        """Persist full inventory first, then candidate/source evidence.

        ``RemoteInventoryIncomplete`` is checked before touching the state
        store.  Ambiguous candidates are retained as non-primary evidence while
        only a unique winner receives ``selected_primary=1``.
        """

        if not self.complete:
            raise RemoteInventoryIncomplete("video selection inventory is incomplete", kind="files")
        episode_ids = dict(episode_ids or {})
        if isinstance(download, RemoteDownload):
            download_key: int | str = download.infohash
            download_fields = {
                "remote_task_id": download.remote_task_id,
                "status": download.status,
            }
        else:
            download_key = download
            download_fields = {}

        # This is intentionally the first state-store call.  A later mapping
        # failure leaves a complete inventory that can be reconciled safely.
        file_ids = store.record_download_files(
            download_key,
            [item.to_mapping() for item in self.inventory],
            **download_fields,
        )
        file_id_by_file = {
            item: file_id for item, file_id in zip(self.inventory, file_ids)
        }

        def resolve_episode_id(episode: EpisodeKey) -> int:
            if episode in episode_ids:
                return int(episode_ids[episode])
            row = store.get_episode_by_key(episode)
            if row is None:
                raise KeyError(f"episode {episode.as_tuple()} not found")
            return int(row["id"])

        for episode, files in self.candidates.items():
            episode_id = resolve_episode_id(episode)
            if episode not in self.mapping and hasattr(store, "clear_episode_source_selection"):
                store.clear_episode_source_selection(episode_id)
            winner = self.mapping.get(episode)
            for item in files:
                file_id = file_id_by_file[item]
                is_winner = winner is not None and winner.remote_file == item
                review_reason = self.reviews.get(episode)
                store.map_episode_source(
                    episode_id,
                    file_id,
                    selected_primary=is_winner,
                    evidence=self.details.get(episode, {}),
                    reason="selected" if is_winner else (
                        review_reason.value if review_reason is not None else "candidate"
                    ),
                )
            if winner is not None and not files:
                raise RuntimeError("selected video has no persisted candidate")
        return file_ids


class VideoSelector:
    """Map a complete remote inventory to target ``EpisodeKey`` values."""

    def __init__(
        self,
        policy: VideoPolicy | Mapping[str, Any] | Any = None,
        *,
        store: Any = None,
        download: int | str | RemoteDownload | None = None,
    ) -> None:
        self.policy = VideoPolicy.from_value(policy)
        self.store = store
        self.download = download

    def select(
        self,
        files: Iterable[RemoteFile | Mapping[str, Any]],
        episodes: Iterable[EpisodeKey | Mapping[str, Any] | tuple[Any, Any, Any, Any]],
        *,
        store: Any = None,
        download: int | str | RemoteDownload | None = None,
        episode_ids: Mapping[EpisodeKey, int] | None = None,
    ) -> VideoSelectionResult:
        inventory = self._complete_inventory(files)
        targets = _unique_episodes(episodes)
        candidates: dict[EpisodeKey, list[_VideoCandidate]] = {episode: [] for episode in targets}
        unknown_files: list[RemoteFile] = []
        excluded: list[RejectedVideo] = []

        for item in inventory:
            extension = os.path.splitext(item.filename)[1].casefold()
            if item.is_dir or extension not in self.policy.supported_extensions:
                excluded.append(RejectedVideo(item, "unsupported_extension" if not item.is_dir else "directory", extension))
                continue
            marker = _feature_marker(item.filename, self.policy.feature_markers)
            if marker:
                excluded.append(RejectedVideo(item, "feature_marker", marker))
                continue
            if self.policy.min_duration is not None and (
                item.duration is None or item.duration < self.policy.min_duration
            ):
                excluded.append(RejectedVideo(item, "below_min_duration"))
                continue
            if self.policy.min_size is not None and (
                item.size is None or item.size < self.policy.min_size
            ):
                excluded.append(RejectedVideo(item, "below_min_size"))
                continue

            parsed = _parse_filename_markers(item.filename, self.policy.max_episode)
            if not parsed.episodes:
                unknown_files.append(item)
                continue
            marker_kind, seasons, episode_numbers = parsed.kind, parsed.seasons, parsed.episodes
            for episode in targets:
                if episode.episode not in episode_numbers:
                    continue
                if parsed.pairs and (episode.season, episode.episode) not in parsed.pairs:
                    # An explicit season contradiction is never repaired by a
                    # larger file, longer duration, or a weaker marker.
                    continue
                rank = 3 if parsed.pairs else (2 if marker_kind in {"E", "bracket", "chinese"} else 1)
                candidates[episode].append(
                    _VideoCandidate(
                        file=item,
                        identity_rank=rank,
                        marker_kind=marker_kind,
                        episodes=tuple(sorted(episode_numbers)),
                        seasons=tuple(sorted(seasons)),
                        pairs=tuple(sorted(parsed.pairs)),
                    )
                )

        mapping: dict[EpisodeKey, SelectedVideo] = {}
        reviews: dict[EpisodeKey, ReviewReason] = {}
        candidate_files: dict[EpisodeKey, tuple[RemoteFile, ...]] = {}
        details: dict[EpisodeKey, dict[str, Any]] = {}
        for episode in targets:
            current = candidates[episode]
            if not current:
                candidate_files[episode] = tuple(unknown_files)
                reviews[episode] = (
                    ReviewReason.AMBIGUOUS_EPISODE
                    if unknown_files
                    else ReviewReason.NO_VALID_VIDEO
                )
                details[episode] = {
                    "reason": reviews[episode].value,
                    "candidate_names": [item.filename for item in unknown_files],
                }
                continue

            best_score = max(item.score for item in current)
            winners = [item for item in current if item.score == best_score]
            # The score only uses evidence that is allowed to break ties after
            # identity.  Equal evidence remains a visible human review.
            candidate_files[episode] = tuple(item.file for item in current)
            if len(winners) != 1:
                reviews[episode] = ReviewReason.MULTIPLE_VIDEO_FILES
                details[episode] = {
                    "reason": reviews[episode].value,
                    "candidate_names": [item.file.filename for item in current],
                    "scores": {item.file.filename: item.score for item in current},
                }
                continue
            winner = winners[0]
            selected = SelectedVideo(
                episode=episode,
                remote_file=winner.file,
                score=winner.score,
                evidence={
                    "marker_kind": winner.marker_kind,
                    "seasons": list(winner.seasons),
                    "episodes": list(winner.episodes),
                    "pairs": [list(pair) for pair in winner.pairs],
                },
            )
            mapping[episode] = selected
            details[episode] = dict(selected.evidence)

        result = VideoSelectionResult(
            mapping=mapping,
            reviews=reviews,
            candidates=candidate_files,
            excluded=tuple(excluded),
            inventory=tuple(inventory),
            complete=True,
            details=details,
        )
        state_store = store if store is not None else self.store
        state_download = download if download is not None else self.download
        if state_store is not None or state_download is not None:
            if state_store is None or state_download is None:
                raise ValueError("store and download must be supplied together")
            result.persist(state_store, state_download, episode_ids=episode_ids)
        return result

    @staticmethod
    def _complete_inventory(
        files: Iterable[RemoteFile | Mapping[str, Any]],
    ) -> list[RemoteFile]:
        if getattr(files, "complete", True) is False:
            raise RemoteInventoryIncomplete("remote file inventory is incomplete", kind="files")
        # Materialize before any candidate or persistence work.  If an adapter
        # raises RemoteInventoryIncomplete halfway through iteration, no partial
        # selection can escape this boundary.
        inventory: list[RemoteFile] = []
        for item in files:
            remote = item if isinstance(item, RemoteFile) else RemoteFile.from_mapping(item)
            if not remote.complete:
                raise RemoteInventoryIncomplete("remote file inventory is incomplete", kind="files")
            inventory.append(remote)
        return inventory


@dataclass(frozen=True, slots=True)
class _ParsedMarkers:
    kind: str
    seasons: frozenset[int]
    episodes: frozenset[int]
    pairs: frozenset[tuple[int, int]] = frozenset()


_FULL_EPISODE = re.compile(
    r"(?<![A-Za-z0-9])S(?P<season>\d{1,2})E(?P<start>\d{1,3})"
    r"(?:(?:\s*[-~]\s*E?|\s*E)(?P<end>\d{1,3}))?(?!\d)",
    re.IGNORECASE,
)
_STANDALONE_E = re.compile(r"(?<![A-Za-z0-9])E(?P<episode>\d{1,3})(?!\d)", re.IGNORECASE)
_BRACKET_EPISODE = re.compile(r"\[(?P<episode>\d{1,3})\](?![A-Za-z0-9])")
_CHINESE_EPISODE = re.compile(r"第\s*(?P<episode>\d{1,3})\s*[话集話]")
_SEPARATOR_EPISODE = re.compile(
    r"(?:(?<=^)|(?<=[._\-\s]))(?P<episode>\d{1,3})(?=$|[._\-\s\[\]()])"
)


def _parse_filename_markers(filename: str, max_episode: int) -> _ParsedMarkers:
    full = list(_FULL_EPISODE.finditer(filename or ""))
    if full:
        seasons: set[int] = set()
        episodes: set[int] = set()
        pairs: set[tuple[int, int]] = set()
        for match in full:
            season = int(match.group("season"))
            start = int(match.group("start"))
            end_text = match.group("end")
            end = int(end_text) if end_text else start
            if end < start:
                end = start
            seasons.add(season)
            for number in range(start, min(end, max_episode) + 1):
                if number > 0:
                    episodes.add(number)
                    pairs.add((season, number))
        return _ParsedMarkers(
            "full", frozenset(seasons), frozenset(episodes), frozenset(pairs)
        )

    patterns = (
        ("E", _STANDALONE_E),
        ("bracket", _BRACKET_EPISODE),
        ("chinese", _CHINESE_EPISODE),
        ("separator", _SEPARATOR_EPISODE),
    )
    for kind, pattern in patterns:
        episodes = {
            int(match.group("episode"))
            for match in pattern.finditer(filename or "")
            if 0 < int(match.group("episode")) <= max_episode
        }
        if episodes:
            return _ParsedMarkers(kind, frozenset(), frozenset(episodes))
    return _ParsedMarkers("unknown", frozenset(), frozenset())


def _feature_marker(filename: str, markers: Iterable[str]) -> str:
    for marker in markers:
        escaped = re.escape(str(marker).strip())
        if not escaped:
            continue
        expression = re.compile(
            rf"(?<![A-Za-z0-9]){escaped}(?:[._\-\s\[\]()]*\d+)?(?![A-Za-z0-9])",
            re.IGNORECASE,
        )
        match = expression.search(filename or "")
        if match:
            return str(marker).upper()
        # Some release names glue the feature marker directly to the episode
        # token (for example ``S01E08NCOP``). Permit that exact structural
        # boundary while keeping ordinary words such as ``OPERATION`` out of
        # the feature exclusion set.
        glued_expression = re.compile(
            rf"(?<![A-Za-z0-9])(?:S\d{{1,2}}E\d{{1,3}}|E\d{{1,3}})"
            rf"{escaped}(?:\d+)?(?![A-Za-z0-9])",
            re.IGNORECASE,
        )
        if glued_expression.search(filename or ""):
            return str(marker).upper()
    return ""


def _normalize_extension(value: Any) -> str:
    text = str(value or "").strip().casefold()
    if text and not text.startswith("."):
        text = "." + text
    return text


def _unique_episodes(values: Iterable[EpisodeKey | Mapping[str, Any] | tuple[Any, Any, Any, Any]]) -> list[EpisodeKey]:
    result: list[EpisodeKey] = []
    seen: set[EpisodeKey] = set()
    for value in values:
        if isinstance(value, EpisodeKey):
            episode = value
        elif isinstance(value, Mapping):
            key = value.get("key") or value.get("episode_key") or value
            if isinstance(key, EpisodeKey):
                episode = key
            else:
                episode = EpisodeKey(
                    str(key.get("media_source") or "tmdb"),
                    str(key.get("media_id")),
                    int(key.get("season", 0)),
                    int(key.get("episode")),
                )
        elif isinstance(value, tuple) and len(value) == 4:
            episode = EpisodeKey(str(value[0]), str(value[1]), int(value[2]), int(value[3]))
        else:
            raise TypeError("episodes must contain EpisodeKey values or compatible mappings")
        if episode not in seen:
            seen.add(episode)
            result.append(episode)
    return result


def persist_video_selection(
    store: Any,
    result: VideoSelectionResult,
    download: int | str | RemoteDownload,
    *,
    episode_ids: Mapping[EpisodeKey, int] | None = None,
) -> list[int]:
    """Functional spelling for callers that keep selection and persistence separate."""

    return result.persist(store, download, episode_ids=episode_ids)


__all__ = [
    "DEFAULT_VIDEO_EXTENSIONS",
    "RejectedVideo",
    "VideoPolicy",
    "VideoSelectionResult",
    "VideoSelector",
    "persist_video_selection",
]
