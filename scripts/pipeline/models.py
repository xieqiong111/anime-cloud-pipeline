"""Immutable domain values shared by the V5 pipeline.

The first V5 task deliberately keeps these values free of persistence or
adapter concerns.  They are small, hashable records that can safely be used as
dictionary keys and passed between the discovery and orchestration layers.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import Enum
import math
import numbers
from typing import Any, Mapping, Optional, Tuple


class EpisodeStatus(str, Enum):
    """Stable and pending states for one normalized episode."""

    DISCOVERED = "discovered"
    DOWNLOAD_PENDING = "download_pending"
    SUBMITTED = "submitted"
    CLOUD_READY = "cloud_ready"
    ORGANIZE_PENDING = "organize_pending"
    ORGANIZED = "organized"
    STRM_PENDING = "strm_pending"
    STRM_READY = "strm_ready"
    SCRAPE_PENDING = "scrape_pending"
    METADATA_READY = "metadata_ready"
    LIBRARY_PENDING = "library_pending"
    LIBRARY_READY = "library_ready"
    NEEDS_REVIEW = "needs_review"
    NOTIFIED = "notified"


class ReviewReason(str, Enum):
    """Reasons that require explicit operator evidence or a manual choice."""

    MULTIPLE_VIDEO_FILES = "MULTIPLE_VIDEO_FILES"
    AMBIGUOUS_EPISODE = "AMBIGUOUS_EPISODE"
    NO_VALID_VIDEO = "NO_VALID_VIDEO"
    PATH_CONFLICT = "PATH_CONFLICT"
    METADATA_CONFLICT = "METADATA_CONFLICT"
    DUPLICATE_EPISODE = "DUPLICATE_EPISODE"
    UNEXPECTED_REMOTE_STATE = "UNEXPECTED_REMOTE_STATE"
    LEGACY_IDENTITY_UNRESOLVED = "LEGACY_IDENTITY_UNRESOLVED"
    RUNTIME_MAPPING_AMBIGUOUS = "RUNTIME_MAPPING_AMBIGUOUS"


@dataclass(frozen=True, slots=True)
class EpisodeKey:
    """The canonical identity of an episode.

    Infohashes and remote task IDs intentionally do not participate in this
    value.  The tuple ``(media_source, media_id, season, episode)`` is the
    uniqueness boundary used by V5 state storage.
    """

    media_source: str
    media_id: str
    season: int
    episode: int

    def __post_init__(self) -> None:
        source = _required_text(self.media_source, "media_source")
        media_id = _required_text(self.media_id, "media_id")
        season = _positive_number(self.season, "season", allow_zero=True)
        episode = _positive_number(self.episode, "episode")
        object.__setattr__(self, "media_source", source)
        object.__setattr__(self, "media_id", media_id)
        object.__setattr__(self, "season", season)
        object.__setattr__(self, "episode", episode)

    def as_tuple(self) -> tuple[str, str, int, int]:
        """Return the identity tuple used by persistence and indexes."""
        return self.media_source, self.media_id, self.season, self.episode


@dataclass(frozen=True, slots=True, init=False)
class RemoteFile:
    """One file from a complete remote download inventory.

    Adapters historically returned dictionaries using both ``file_id``/``name``
    and the V5 spellings ``remote_file_id``/``filename``.  The value object
    accepts either spelling and exposes both aliases so selection code can stay
    independent of the adapter response shape.
    """

    filename: str
    remote_file_id: Optional[str]
    parent_id: Optional[str]
    parent_path: Optional[str]
    path: Optional[str]
    size: Optional[int]
    duration: Optional[float]
    fingerprint: Optional[str]
    infohash: Optional[str]
    is_dir: bool
    complete: bool

    def __init__(
        self,
        *args: Any,
        filename: str = "",
        name: Optional[str] = None,
        remote_file_id: Optional[str] = None,
        file_id: Optional[str] = None,
        parent_id: Optional[str] = None,
        parent_path: Optional[str] = None,
        path: Optional[str] = None,
        size: Optional[int] = None,
        duration: Optional[float] = None,
        fingerprint: Optional[str] = None,
        content_fingerprint: Optional[str] = None,
        infohash: Optional[str] = None,
        is_dir: bool = False,
        complete: bool = True,
    ) -> None:
        # A single positional argument is the natural ``RemoteFile(name)``
        # spelling.  Two positional arguments retain the common adapter-style
        # ``RemoteFile(file_id, name)`` convenience without changing keyword
        # callers used by the V5 tests.
        if len(args) > 2:
            raise TypeError("RemoteFile accepts at most two positional arguments")
        if len(args) == 1:
            if filename or name:
                raise TypeError("filename supplied both positionally and by keyword")
            filename = str(args[0])
        elif len(args) == 2:
            if remote_file_id is not None or file_id is not None or filename or name:
                raise TypeError("RemoteFile identity/name supplied twice")
            remote_file_id, filename = args[0], str(args[1])

        final_name = str(name if name is not None else filename or "").strip()
        if not final_name and path:
            final_name = str(path).replace("\\", "/").rsplit("/", 1)[-1].strip()
        remote_id = remote_file_id if remote_file_id is not None else file_id
        remote_id = _optional_text(remote_id)
        parent_id = _optional_text(parent_id)
        parent_path = _optional_text(parent_path)
        path = _optional_text(path)
        fingerprint = _optional_text(
            fingerprint if fingerprint is not None else content_fingerprint
        )
        infohash = _optional_text(infohash)
        normalized_size: Optional[int]
        if size is None or size == "":
            normalized_size = None
        else:
            try:
                normalized_size = int(size)
            except (TypeError, ValueError) as exc:
                raise ValueError("RemoteFile.size must be an integer or None") from exc
        normalized_duration: Optional[float]
        if duration is None or duration == "":
            normalized_duration = None
        else:
            try:
                normalized_duration = float(duration)
            except (TypeError, ValueError) as exc:
                raise ValueError("RemoteFile.duration must be a number or None") from exc

        object.__setattr__(self, "filename", final_name)
        object.__setattr__(self, "remote_file_id", remote_id)
        object.__setattr__(self, "parent_id", parent_id)
        object.__setattr__(self, "parent_path", parent_path)
        object.__setattr__(self, "path", path)
        object.__setattr__(self, "size", normalized_size)
        object.__setattr__(self, "duration", normalized_duration)
        object.__setattr__(self, "fingerprint", fingerprint)
        object.__setattr__(self, "infohash", infohash)
        object.__setattr__(self, "is_dir", bool(is_dir))
        object.__setattr__(self, "complete", bool(complete))

    @property
    def name(self) -> str:
        """Adapter-compatible filename alias."""

        return self.filename

    @property
    def file_id(self) -> Optional[str]:
        """Adapter-compatible remote-file identity alias."""

        return self.remote_file_id

    @classmethod
    def from_mapping(cls, value: Mapping[str, Any]) -> "RemoteFile":
        """Normalize one adapter mapping without retaining arbitrary metadata."""

        metadata = value.get("metadata")
        nested = metadata if isinstance(metadata, Mapping) else {}
        fingerprint = (
            value.get("fingerprint")
            or value.get("content_fingerprint")
            or nested.get("fingerprint")
            or nested.get("content_fingerprint")
            or nested.get("hash")
            or nested.get("sha1")
            or nested.get("sha")
            or nested.get("md5")
            or nested.get("etag")
        )
        infohash = (
            value.get("infohash")
            or nested.get("infohash")
            or nested.get("info_hash")
            or nested.get("btih")
        )

        return cls(
            filename=str(value.get("filename") or value.get("name") or value.get("path") or ""),
            remote_file_id=value.get("remote_file_id") or value.get("file_id") or value.get("fid"),
            parent_id=value.get("parent_id") or value.get("parent_fid") or value.get("cid"),
            parent_path=value.get("parent_path"),
            path=value.get("path") or value.get("relative_path"),
            size=value.get("size"),
            duration=value.get("duration"),
            fingerprint=fingerprint,
            infohash=infohash,
            is_dir=bool(value.get("is_dir", False)),
            complete=bool(value.get("complete", True)),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Return the persistence shape used by :class:`StateStore`."""

        return {
            "remote_file_id": self.remote_file_id,
            "parent_id": self.parent_id,
            "parent_path": self.parent_path,
            "path": self.path,
            "name": self.filename,
            "size": self.size,
            "duration": self.duration,
            "fingerprint": self.fingerprint,
            "infohash": self.infohash,
        }


@dataclass(frozen=True, slots=True)
class RemoteDownload:
    """Remote download identity shared by one or more Episode rows."""

    infohash: str
    remote_task_id: Optional[str] = None
    status: str = "done"


@dataclass(frozen=True, slots=True)
class SelectedVideo:
    """A unique primary remote file selected for one target Episode."""

    episode: EpisodeKey
    remote_file: RemoteFile
    score: tuple[float, ...] = ()
    evidence: Mapping[str, Any] = ()

    @property
    def file(self) -> RemoteFile:
        """Short alias used by organizer-facing callers."""

        return self.remote_file

    @property
    def filename(self) -> str:
        return self.remote_file.filename

    @property
    def name(self) -> str:
        return self.remote_file.filename

    @property
    def remote_file_id(self) -> Optional[str]:
        return self.remote_file.remote_file_id

    @property
    def file_id(self) -> Optional[str]:
        return self.remote_file.remote_file_id

    @property
    def size(self) -> Optional[int]:
        return self.remote_file.size

    @property
    def duration(self) -> Optional[float]:
        return self.remote_file.duration

    def __getattr__(self, name: str) -> Any:
        """Expose remaining ``RemoteFile`` evidence to organizer callers."""

        return getattr(self.remote_file, name)


def _optional_text(value: Any) -> Optional[str]:
    if value is None:
        return None
    text = str(value).strip()
    return text or None


@dataclass(frozen=True, slots=True)
class Subscription:
    """A normalized MoviePilot subscription snapshot."""

    subscription_id: str
    media_source: str
    media_id: str
    work: str
    aliases: Tuple[str, ...] = ()
    season: Optional[int] = None
    include: str = ""
    exclude: str = ""
    # This is routing evidence returned by the verified TMDB season metadata
    # nested in the MoviePilot snapshot.  It is deliberately separate from
    # generic episode/release dates, which are not safe season labels.
    season_air_date: str = ""

    def __post_init__(self) -> None:
        subscription_id = _required_text(self.subscription_id, "subscription_id")
        media_source = _required_text(self.media_source, "media_source")
        media_id = _required_text(self.media_id, "media_id")
        work = _required_text(self.work, "work")
        aliases = tuple(str(alias).strip() for alias in (self.aliases or ()) if str(alias).strip())
        season = self.season
        if season is not None:
            season = _positive_number(season, "season", allow_zero=True)
        object.__setattr__(self, "subscription_id", subscription_id)
        object.__setattr__(self, "media_source", media_source)
        object.__setattr__(self, "media_id", media_id)
        object.__setattr__(self, "work", work)
        object.__setattr__(self, "aliases", aliases)
        object.__setattr__(self, "season", season)
        object.__setattr__(self, "include", str(self.include or "").strip())
        object.__setattr__(self, "exclude", str(self.exclude or "").strip())
        object.__setattr__(self, "season_air_date", str(self.season_air_date or "").strip())


@dataclass(frozen=True, slots=True, init=False)
class RetryState:
    """Immutable retry metadata carried by a pending stage."""

    attempts: int = 0
    next_retry_at: Optional[float] = None
    last_error: str = ""

    def __init__(self, attempts: int = 0, next_retry_at: Optional[float] = None,
                 last_error: str = "", *, retry_count: Optional[int] = None) -> None:
        # ``retry_count`` is the spelling used by the SQLite schema.  Accept it
        # as a keyword while keeping ``attempts`` useful to callers that model
        # transport retries directly.
        if retry_count is not None:
            if attempts != 0 and attempts != retry_count:
                raise ValueError("attempts and retry_count disagree")
            attempts = retry_count
        attempts = _positive_number(attempts, "attempts", allow_zero=True)
        if next_retry_at is not None:
            try:
                next_retry_at = float(next_retry_at)
            except (TypeError, ValueError) as exc:
                raise ValueError("next_retry_at must be a number or None") from exc
        object.__setattr__(self, "attempts", attempts)
        object.__setattr__(self, "next_retry_at", next_retry_at)
        object.__setattr__(self, "last_error", str(last_error or ""))

    def __post_init__(self) -> None:
        # Kept for dataclass/introspection compatibility; validation is done in
        # the custom constructor so both retry-count spellings are accepted.
        return None

    @property
    def retry_count(self) -> int:
        """Compatibility spelling used by persistence rows."""
        return self.attempts


def _positive_number(value: object, field: str, *, allow_zero: bool = False) -> int:
    if value is None or isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    if isinstance(value, numbers.Integral):
        number = int(value)
    elif isinstance(value, numbers.Real):
        if not math.isfinite(float(value)) or not float(value).is_integer():
            raise ValueError(f"{field} must be an integer")
        number = int(value)
    elif isinstance(value, str):
        text = value.strip()
        if not text or not text.lstrip("+-").isdigit():
            raise ValueError(f"{field} must be an integer")
        number = int(text)
    else:
        raise ValueError(f"{field} must be an integer")
    if number < 0 or (number == 0 and not allow_zero):
        qualifier = "non-negative" if allow_zero else "positive"
        raise ValueError(f"{field} must be {qualifier}")
    return number


def _required_text(value: object, field: str) -> str:
    if value is None:
        raise ValueError(f"{field} must not be missing")
    text = str(value).strip()
    if not text:
        raise ValueError(f"{field} must not be empty")
    return text


__all__ = [
    "EpisodeKey",
    "RemoteDownload",
    "RemoteFile",
    "SelectedVideo",
    "Subscription",
    "EpisodeStatus",
    "ReviewReason",
    "RetryState",
]
