"""Containment-safe paths and season routing for the V5 media pipeline.

All service path translation lives here.  The mapper deliberately treats paths
as lexical, POSIX paths: it never asks the local operating system to resolve a
path and it never accepts a path which could be interpreted as a Windows
drive, UNC share, URL, or encoded traversal.
"""
from __future__ import annotations

from dataclasses import dataclass
from datetime import date
import hashlib
import json
import posixpath
import re
from typing import Any, Mapping


class PathMappingError(ValueError):
    """A configured root or service path is unsafe or outside its root."""


class SeasonRoutingError(PathMappingError):
    """The target TMDB season has no valid strict ISO air date."""


_STRICT_AIR_DATE = re.compile(r"^(?P<year>[0-9]{4})-(?P<month>0[1-9]|1[0-2])-(?P<day>0[1-9]|[12][0-9]|3[01])$")
_ENCODED_BYTE = re.compile(r"%[0-9A-Fa-f]{2}")
_DRIVE_PREFIX = re.compile(r"^[A-Za-z]:")
_DRIVE_COMPONENT = re.compile(r"^[A-Za-z]:")
_CONTROL = re.compile(r"[\x00-\x1f\x7f]")
_REMOTE_FILE_TOKEN = re.compile(r"^fid-[0-9a-f]{64}$")


@dataclass(frozen=True, slots=True)
class ServiceRoots:
    """The five lexical roots required by :class:`PathMapper`."""

    cloud_source_root: str
    cloud_library_root: str
    litepan_output_root: str
    moviepilot_mount_root: str
    emby_mount_root: str


def season_label_from_air_date(value: str) -> str:
    """Return the calendar-quarter ``YYYY-MM月新番`` label for a TMDB season.

    TMDB occasionally returns empty values or a date with a time component.
    Those values are intentionally rejected instead of being replaced with the
    run date or a release date: routing a file to the wrong season is a
    destructive, difficult-to-reconcile operation.
    """

    if not isinstance(value, str):
        raise SeasonRoutingError("season air_date must be a strict YYYY-MM-DD string")
    match = _STRICT_AIR_DATE.fullmatch(value)
    if match is None:
        raise SeasonRoutingError("season air_date must be a strict YYYY-MM-DD string")
    try:
        parsed = date(int(match.group("year")), int(match.group("month")), int(match.group("day")))
    except ValueError as exc:
        raise SeasonRoutingError("season air_date is not a valid calendar date") from exc
    quarter_month = ((parsed.month - 1) // 3) * 3 + 1
    return f"{parsed.year:04d}-{quarter_month:02d}月新番"


class PathMapper:
    """Translate paths between configured service roots without root escapes."""

    _ROOT_FIELDS = (
        "cloud_source_root",
        "cloud_library_root",
        "litepan_output_root",
        "moviepilot_mount_root",
        "emby_mount_root",
    )

    def __init__(self, paths: ServiceRoots | Mapping[str, Any] | Any):
        values = {
            field: _path_value(paths, field)
            for field in self._ROOT_FIELDS
        }
        normalized = {
            field: _normalize_root(value, field)
            for field, value in values.items()
        }
        # Cloud source and library share one remote namespace. Local service
        # paths may legitimately have the same spelling inside different
        # containers; each translation names its source root explicitly.
        if normalized["cloud_source_root"] == normalized["cloud_library_root"]:
            raise PathMappingError("cloud_library_root: must differ from cloud_source_root")
        self.roots = ServiceRoots(**normalized)
        # Optional legacy-layout segment (for example ``整理``) inserted into
        # the *published* STRM path only.  The raw LitePan output tree and the
        # cloud organizer targets are never affected by this value.
        self.publish_subdir = _optional_component(paths, "litepan_publish_subdir")
        # Optional v3-compatible directory (for example ``整理``) inserted into
        # every *cloud* target path immediately after the quarter label.  This
        # keeps the organizer/download cloud layout identical to the legacy v3
        # tree while the published STRM stays a one-to-one projection of the
        # cloud path (no duplicated ``整理``).
        self.cloud_library_subdir = _optional_component(paths, "cloud_library_subdir")

    # ---------- public cross-service mappings ----------
    def cloud_to_litepan(self, cloud_path: str) -> str:
        """Map a canonical cloud media file to its exact LitePan STRM path."""

        relative = self.relative_to(cloud_path, self.roots.cloud_library_root, field="cloud_path")
        if not relative:
            return self.roots.litepan_output_root
        if self.publish_subdir:
            # Reproduce the v3 layout ``<quarter>/整理/<work>/Season N/<file>``
            # by inserting the configured component after the quarter label.
            parts = relative.split("/")
            parts.insert(1, self.publish_subdir)
            relative = "/".join(parts)
        mapped = self._join_root(self.roots.litepan_output_root, relative, "litepan_path")
        return _replace_suffix(mapped, ".strm")

    def cloud_to_raw_litepan(self, cloud_path: str, raw_output_root: str) -> str:
        """Map a cloud file into LitePan's private scan output tree.

        LitePan names each generated STRM file after the cloud object only
        after replacing filesystem-invalid characters with ``_`` (the live
        tree shows ``:`` becoming ``_``; verified across every cloud video
        file on 2026-09-25).  Mirror that replacement per component, or a
        cloud name containing such a character can never match its raw
        STRM and the publish step would fail closed forever.  Only the
        cloud-relative path components are sanitized; the configured
        ``raw_output_root`` is preserved verbatim so a root that itself
        contains a character LitePan would replace is never silently
        rewritten into a path miss.
        """

        relative = self.relative_to(
            cloud_path, self.roots.cloud_library_root, field="cloud_path"
        )
        root = _normalize_root(raw_output_root, "litepan_raw_output_root")
        clean_relative = _litepan_fs_path(relative)
        mapped = self._join_root(root, clean_relative, "litepan_raw_path")
        return _replace_suffix(mapped, ".strm")

    def cloud_to_published_litepan(
        self, cloud_path: str, episode: Mapping[str, Any] | Any
    ) -> str:
        """Map an isolated cloud file to its canonical, published STRM path.

        Isolated cloud files live below ``Season N/fid-<hash>/<original name>``
        so the raw cloud object cannot collide with an existing file.  MoviePilot
        expects season-level directories and episode-identifiable filenames, so
        publish the projection directly under ``Season N`` using the episode
        row's canonical naming fields.  Non-isolated paths keep the ordinary
        one-to-one cloud-to-LitePan mapping.
        """

        relative = self.relative_to(
            cloud_path, self.roots.cloud_library_root, field="cloud_path"
        )
        parts = relative.split("/") if relative else []
        season_match = re.fullmatch(r"Season (?P<number>[0-9]+)", parts[-3]) if len(parts) >= 5 else None
        if (
            season_match is None
            or _REMOTE_FILE_TOKEN.fullmatch(parts[-2]) is None
        ):
            return self.cloud_to_litepan(cloud_path)

        # Reuse canonical_for_episode's field/date/extension validation while
        # bypassing its durable pending cloud_path, which intentionally points
        # to the isolated remote object rather than the published filename.
        episode_value = dict(_as_mapping(episode))
        episode_value.pop("cloud_path", None)
        episode_value["status"] = ""
        canonical_cloud_path = self.canonical_for_episode(
            episode_value, {"filename": parts[-1]}
        )
        isolated_season_dir = self._join_root(
            self.roots.cloud_library_root,
            "/".join(parts[:-2]),
            "cloud_path",
        )
        if posixpath.dirname(canonical_cloud_path) != isolated_season_dir:
            raise PathMappingError(
                "episode canonical season does not match isolated cloud directory"
            )
        return self.cloud_to_litepan(canonical_cloud_path)

    def litepan_to_moviepilot(self, litepan_path: str) -> str:
        """Map one LitePan path to the MoviePilot-mounted STRM path."""

        relative = self.relative_to(
            litepan_path, self.roots.litepan_output_root, field="litepan_path"
        )
        mapped = self._join_root(
            self.roots.moviepilot_mount_root, relative, "moviepilot_path"
        )
        return _normalize_strm_suffix(mapped)

    def litepan_to_emby(self, litepan_path: str) -> str:
        """Map one LitePan path to the Emby-mounted STRM path."""

        relative = self.relative_to(
            litepan_path, self.roots.litepan_output_root, field="litepan_path"
        )
        mapped = self._join_root(self.roots.emby_mount_root, relative, "emby_path")
        return _normalize_strm_suffix(mapped)

    def relative_to(self, path: str, root: str, *, field: str = "path") -> str:
        """Return a normalized path relative to a configured root.

        The root itself maps to an empty relative path.  Prefixes are compared
        with a path-component boundary, so ``/media/library-old`` can never be
        accepted as a child of ``/media/library``.
        """

        normalized_path = _normalize_input_path(path, field)
        normalized_root = _normalize_root(root, f"{field}.root")
        if normalized_path == normalized_root:
            return ""
        prefix = normalized_root.rstrip("/") + "/"
        if not normalized_path.startswith(prefix):
            raise PathMappingError(f"{field}: path is outside configured root")
        relative = normalized_path[len(prefix):]
        if not relative or relative == ".":
            return ""
        # A second lexical validation protects callers that pass a root from a
        # different configuration object and prevents absolute substitution in
        # the returned path.
        if relative.startswith("/") or any(part in {"..", "."} for part in relative.split("/")):
            raise PathMappingError(f"{field}: unsafe relative path")
        return relative

    # ---------- canonical target builders ----------
    def build_canonical_cloud_path(
        self,
        work: str,
        season: int,
        episode: int,
        extension: str | None = None,
        air_date: str | None = None,
        *,
        suffix: str | None = None,
    ) -> str:
        """Build the one canonical cloud path for a selected media file."""

        if extension is None:
            extension = suffix
        label = season_label_from_air_date(air_date)
        safe_work = _safe_component(work, "work")
        season_number = _positive_integer(season, "season", allow_zero=True)
        episode_number = _positive_integer(episode, "episode")
        suffix = _safe_suffix(extension)
        basename = (
            f"{safe_work} - S{season_number:02d}E{episode_number:02d}{suffix}"
        )
        relative = self._library_relative(
            label, f"{safe_work}/Season {season_number}/{basename}"
        )
        return self._join_root(
            self.roots.cloud_library_root, relative, "cloud_library_path"
        )

    def build_isolated_cloud_path(
        self,
        work: str,
        season: int,
        remote_file_id: int | str,
        original_basename: str,
        air_date: str,
    ) -> str:
        """Build a stable per-remote-file cloud destination.

        The ID is hashed into a filesystem-safe token so opaque or untrusted
        provider IDs cannot introduce path components.  The original basename
        is retained byte-for-byte as a single validated POSIX component.
        """

        label = season_label_from_air_date(air_date)
        safe_work = _safe_component(work, "work")
        season_number = _positive_integer(season, "season", allow_zero=True)
        file_token = _remote_file_id_token(remote_file_id)
        basename = _safe_basename(original_basename, "original_basename")
        relative = self._library_relative(
            label,
            f"{safe_work}/Season {season_number}/{file_token}/{basename}",
        )
        return self._join_root(
            self.roots.cloud_library_root, relative, "cloud_library_path"
        )

    def isolated_for_episode(
        self, episode: Mapping[str, Any] | Any, selected: Any = None
    ) -> str:
        """Build or recover the isolated cloud target for an episode.

        Pending ``cloud_path`` values are durable organizer intents and are
        returned as-is after containment validation.  Otherwise, this reuses
        the canonical episode routing logic for the season directory and adds
        the selected remote file's stable ID token and original basename.
        """

        value = _as_mapping(episode)
        persisted = value.get("runtime_identity") or value.get("runtime_identity_json")
        if isinstance(persisted, str):
            try:
                persisted = json.loads(persisted)
            except (TypeError, ValueError):
                persisted = {}
        if not isinstance(persisted, Mapping):
            persisted = {}
        persisted_cloud_path = value.get("cloud_path")
        status = value.get("status")
        status = getattr(status, "value", status)
        if (
            isinstance(persisted_cloud_path, str)
            and persisted_cloud_path
            and (str(status or "") == "organize_pending" or persisted)
        ):
            normalized = _normalize_input_path(persisted_cloud_path, "cloud_path")
            self.relative_to(normalized, self.roots.cloud_library_root, field="cloud_path")
            return normalized

        selected_value = selected
        if selected_value is None:
            for key in ("selected_video", "selected_file", "remote_file", "source_file"):
                if value.get(key) is not None:
                    selected_value = value[key]
                    break
        if selected_value is None:
            selected_value = value
        selected_mapping = _as_mapping(selected_value)
        remote_file_id = _first_value(
            selected_mapping, "remote_file_id", "file_id", "fid", "id"
        )
        original_name = _first_value(
            selected_mapping, "filename", "name", "path", "source_path"
        )
        if remote_file_id is None or remote_file_id == "":
            raise PathMappingError("remote_file_id: required for isolated cloud target")
        if not isinstance(original_name, str) or not original_name:
            raise PathMappingError("original_basename: required for isolated cloud target")
        original_basename = posixpath.basename(original_name)

        # canonical_for_episode owns the existing title/season/date extraction
        # and remains the direct canonical path used for local STRM projection.
        canonical_path = self.canonical_for_episode(episode, selected_value)
        season_directory = posixpath.dirname(canonical_path)
        season_relative = self.relative_to(
            season_directory, self.roots.cloud_library_root, field="canonical_path"
        )
        safe_basename = _safe_basename(original_basename, "original_basename")
        isolated_relative = (
            f"{season_relative}/{_remote_file_id_token(remote_file_id)}/"
            f"{safe_basename}"
        )
        return self._join_root(
            self.roots.cloud_library_root, isolated_relative, "cloud_library_path"
        )

    # Common spellings used by stage services and focused tests.  Keeping the
    # aliases here avoids letting each service reimplement path concatenation.
    canonical_cloud_path = build_canonical_cloud_path
    build_cloud_path = build_canonical_cloud_path
    build_canonical_path = build_canonical_cloud_path
    canonical_target = build_canonical_cloud_path
    canonical_target_path = build_canonical_cloud_path
    build_canonical_target = build_canonical_cloud_path
    build_canonical_cloud_target = build_canonical_cloud_path

    @staticmethod
    def season_label_from_air_date(value: str) -> str:
        return season_label_from_air_date(value)

    def canonical_paths(
        self,
        *,
        work: str,
        season: int,
        episode: int,
        extension: str,
        air_date: str,
    ) -> dict[str, str]:
        """Return all canonical service paths derived from one cloud target."""

        cloud = self.build_canonical_cloud_path(work, season, episode, extension, air_date)
        litepan = self.cloud_to_litepan(cloud)
        return {
            "cloud": cloud,
            "litepan": litepan,
            "moviepilot": self.litepan_to_moviepilot(litepan),
            "emby": self.litepan_to_emby(litepan),
        }

    def canonical_for_episode(self, episode: Mapping[str, Any] | Any, selected: Any = None) -> str:
        """Build a cloud target from an Episode row and optional selected file."""

        value = _as_mapping(episode)
        persisted = value.get("runtime_identity") or value.get("runtime_identity_json")
        if isinstance(persisted, str):
            try:
                persisted = json.loads(persisted)
            except (TypeError, ValueError):
                persisted = {}
        if not isinstance(persisted, Mapping):
            persisted = {}
        persisted_cloud_path = value.get("cloud_path")
        status = value.get("status")
        status = getattr(status, "value", status)
        if (
            isinstance(persisted_cloud_path, str)
            and persisted_cloud_path
            and (str(status or "") == "organize_pending" or persisted)
        ):
            # A pending intent is the durable source of truth after a crash.
            # Metadata may be refreshed while the remote move is in flight;
            # rebuilding from newer metadata would abandon that target.
            normalized = _normalize_input_path(persisted_cloud_path, "cloud_path")
            self.relative_to(normalized, self.roots.cloud_library_root, field="cloud_path")
            return normalized
        selected_value = selected
        if selected_value is None:
            for key in ("selected_video", "selected_file", "remote_file", "source_file"):
                if value.get(key) is not None:
                    selected_value = value[key]
                    break
        work = _first_value(value, "canonical_work", "work", "title") or persisted.get("_work")
        season = _first_value(value, "season") or persisted.get("_season")
        episode_number = _first_value(value, "episode") or persisted.get("_episode")
        air_date = _explicit_season_air_date(value)
        if air_date is None:
            air_date = _explicit_season_air_date(selected_value)
        if air_date is None:
            # Only the explicitly persisted TMDB season date is durable
            # routing evidence.  The old generic ``_air_date`` key is not
            # namespaced and may have represented an episode/release date.
            air_date = persisted.get("_season_air_date")
        extension = _selected_extension(selected_value)
        if not extension:
            extension = _selected_extension(value)
        if not extension:
            extension = str(persisted.get("_extension") or "")
        return self.build_canonical_cloud_path(work, season, episode_number, extension, air_date)

    def _library_relative(self, label: str, tail: str) -> str:
        """Join a quarter label, the optional library subdir, and a tail path.

        ``cloud_library_subdir`` reproduces the legacy v3 cloud layout where
        organized media lives under ``<quarter>/整理/<work>/Season N/...``.
        The subdir is inserted directly after the quarter label so the cloud
        path, and therefore the published STRM projection of it, matches the
        existing v3 tree without ever doubling the directory.
        """

        if self.cloud_library_subdir:
            return f"{label}/{self.cloud_library_subdir}/{tail}"
        return f"{label}/{tail}"

    def _join_root(self, root: str, relative: str, field: str) -> str:
        normalized_root = _normalize_root(root, f"{field}.root")
        if not relative:
            return normalized_root
        # ``relative`` is generated only after _normalize_input_path, but the
        # check is repeated at the write boundary for defense in depth.
        _validate_relative(relative, field)
        result = posixpath.normpath(normalized_root.rstrip("/") + "/" + relative)
        prefix = normalized_root.rstrip("/") + "/"
        if result != normalized_root and not result.startswith(prefix):
            raise PathMappingError(f"{field}: mapped path escapes configured root")
        return result


def _optional_component(paths: Any, field: str) -> str:
    """Read one optional single path component from a config or mapping."""

    if isinstance(paths, Mapping):
        value = paths.get(field)
    else:
        value = getattr(paths, field, None)
    if value in (None, ""):
        return ""
    return _safe_component(value, f"paths.{field}")


def _path_value(paths: Any, field: str) -> Any:
    if isinstance(paths, Mapping):
        aliases = {
            "cloud_source_root": ("cloud_source_root", "cloud_source", "source_root"),
            "cloud_library_root": ("cloud_library_root", "cloud_library", "cloud_root", "library_root"),
            "litepan_output_root": ("litepan_output_root", "litepan_root", "strm_root"),
            "moviepilot_mount_root": ("moviepilot_mount_root", "moviepilot_root", "mp_root"),
            "emby_mount_root": ("emby_mount_root", "emby_root"),
        }
        value = next((paths.get(alias) for alias in aliases[field] if paths.get(alias) is not None), None)
    else:
        value = getattr(paths, field, None)
    if value is None:
        raise PathMappingError(f"paths.{field}: required")
    return value


def _normalize_root(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise PathMappingError(f"{field}: root must be an absolute POSIX path")
    if _CONTROL.search(value) or "\\" in value:
        raise PathMappingError(f"{field}: Windows separators/control characters are not allowed")
    if _ENCODED_BYTE.search(value):
        raise PathMappingError(f"{field}: encoded path components are not allowed")
    if not value.startswith("/") or value.startswith("//") or _DRIVE_PREFIX.match(value.lstrip("/")):
        raise PathMappingError(f"{field}: root must be an absolute POSIX path")
    if "://" in value or "?" in value or "#" in value:
        raise PathMappingError(f"{field}: URL/query syntax is not allowed in a path root")
    parts = value.split("/")
    if any(part in {"..", "."} or _DRIVE_COMPONENT.match(part) for part in parts):
        raise PathMappingError(f"{field}: traversal/dot segments are not allowed")
    normalized = posixpath.normpath(value)
    if normalized in {"", ".", "/"} or not normalized.startswith("/"):
        raise PathMappingError(f"{field}: root must be a non-root absolute POSIX path")
    return normalized


def _normalize_input_path(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise PathMappingError(f"{field}: path must be an absolute POSIX path")
    if _CONTROL.search(value) or "\\" in value:
        raise PathMappingError(f"{field}: Windows separators/control characters are not allowed")
    if _ENCODED_BYTE.search(value):
        raise PathMappingError(f"{field}: encoded path components are not allowed")
    if not value.startswith("/") or value.startswith("//") or _DRIVE_PREFIX.match(value.lstrip("/")):
        raise PathMappingError(f"{field}: path must be an absolute POSIX path")
    if "://" in value or "?" in value or "#" in value:
        raise PathMappingError(f"{field}: URL/query syntax is not allowed in a path")
    parts = value.split("/")
    if any(part in {"..", "."} or _DRIVE_COMPONENT.match(part) for part in parts):
        raise PathMappingError(f"{field}: traversal is not allowed")
    normalized = posixpath.normpath(value)
    if normalized.startswith("//") or normalized == ".":
        raise PathMappingError(f"{field}: invalid normalized path")
    return normalized


def _validate_relative(relative: str, field: str) -> None:
    if not isinstance(relative, str) or not relative or relative.startswith("/"):
        raise PathMappingError(f"{field}: relative path is invalid")
    if _CONTROL.search(relative) or "\\" in relative or _ENCODED_BYTE.search(relative):
        raise PathMappingError(f"{field}: unsafe relative path")
    if any(
        part in {"..", "."} or _DRIVE_COMPONENT.match(part)
        for part in relative.split("/")
    ):
        raise PathMappingError(f"{field}: traversal is not allowed")
    if _DRIVE_PREFIX.match(relative) or "://" in relative:
        raise PathMappingError(f"{field}: drive/URL syntax is not allowed")


_LITEPAN_INVALID_NAME_CHARS = re.compile(r'[<>:"\\|?*]')


def _litepan_fs_path(path: str) -> str:
    """Replace filesystem-invalid characters the way LitePan names STRM files.

    The replacement mirrors LitePan's observed behavior; the path separator
    itself is preserved by sanitizing components individually.  A character
    LitePan leaves untouched but this function replaces would only make the
    mapped raw path miss (fail closed), never publish a wrong file.
    """

    return "/".join(
        _LITEPAN_INVALID_NAME_CHARS.sub("_", part) if part else part
        for part in path.split("/")
    )


def _replace_suffix(path: str, suffix: str) -> str:
    directory, basename = posixpath.split(path)
    stem, extension = posixpath.splitext(basename)
    if not stem or basename in {".", ".."}:
        raise PathMappingError("path: media basename is invalid")
    # Normalize STRM casing at the one boundary where the cloud media suffix
    # changes.  Repeated mapping is idempotent for both .strm and legacy
    # uppercase .STRM names.
    if extension:
        new_name = stem + suffix
    else:
        raise PathMappingError("path: selected cloud file must have a media suffix")
    return f"{directory}/{new_name}" if directory else new_name


def _normalize_strm_suffix(path: str) -> str:
    basename = posixpath.basename(path)
    extension = posixpath.splitext(basename)[1]
    if extension.casefold() == ".strm":
        return _replace_suffix(path, ".strm")
    return path


def _safe_component(value: Any, field: str) -> str:
    if not isinstance(value, str) or not value:
        raise PathMappingError(f"{field}: path component is required")
    if _CONTROL.search(value) or "\\" in value or "/" in value:
        raise PathMappingError(f"{field}: path separators/control characters are not allowed")
    if _ENCODED_BYTE.search(value) or value in {".", ".."} or _DRIVE_COMPONENT.match(value):
        raise PathMappingError(f"{field}: unsafe path component")
    if "?" in value or "#" in value or "://" in value:
        raise PathMappingError(f"{field}: URL/query syntax is not allowed")
    return value.strip() or (_raise_component(field))


def _safe_basename(value: Any, field: str) -> str:
    """Validate one original filename component without normalizing it."""

    if not isinstance(value, str) or not value or not value.strip():
        raise PathMappingError(f"{field}: filename component is required")
    if (
        _CONTROL.search(value)
        or "/" in value
        or "\\" in value
        or _ENCODED_BYTE.search(value)
        or value in {".", ".."}
        or _DRIVE_COMPONENT.match(value)
    ):
        raise PathMappingError(f"{field}: unsafe filename component")
    if "?" in value or "#" in value or "://" in value:
        raise PathMappingError(f"{field}: URL/query syntax is not allowed")
    return value


def _remote_file_id_token(value: Any) -> str:
    """Return a deterministic, path-safe token derived from a remote ID."""

    if isinstance(value, bool) or not isinstance(value, (int, str)):
        raise PathMappingError("remote_file_id: must be a non-empty string or integer")
    text = str(value)
    if (
        not text
        or not text.strip()
        or text != text.strip()
        or len(text) > 512
        or _CONTROL.search(text)
    ):
        raise PathMappingError("remote_file_id: invalid identifier")
    if isinstance(value, int) and value <= 0:
        raise PathMappingError("remote_file_id: must be positive")
    if isinstance(value, str) and text.isdecimal() and int(text) <= 0:
        raise PathMappingError("remote_file_id: must be positive")
    digest = hashlib.sha256(text.encode("utf-8")).hexdigest()
    return f"fid-{digest}"


def _raise_component(field: str) -> str:
    raise PathMappingError(f"{field}: path component is required")


def _safe_suffix(value: Any) -> str:
    if not isinstance(value, str) or not value:
        raise PathMappingError("extension: media suffix is required")
    text = value.strip()
    if "/" in text or "\\" in text or _ENCODED_BYTE.search(text) or "?" in text or "#" in text:
        raise PathMappingError("extension: invalid media suffix")
    if not text.startswith("."):
        text = "." + text
    if len(text) <= 1 or not re.fullmatch(r"\.[A-Za-z0-9][A-Za-z0-9._+-]*", text):
        raise PathMappingError("extension: invalid media suffix")
    return text


def _positive_integer(value: Any, field: str, *, allow_zero: bool = False) -> int:
    if isinstance(value, bool):
        raise PathMappingError(f"{field}: must be an integer")
    try:
        number = int(value)
    except (TypeError, ValueError) as exc:
        raise PathMappingError(f"{field}: must be an integer") from exc
    if str(value).strip() != str(number) and not isinstance(value, int):
        raise PathMappingError(f"{field}: must be an integer")
    if number < 0 or (number == 0 and not allow_zero):
        raise PathMappingError(f"{field}: must be positive")
    return number


def _as_mapping(value: Any) -> Mapping[str, Any]:
    if isinstance(value, Mapping):
        return value
    names = (
        "canonical_work", "work", "title", "season", "episode", "air_date",
        "season_air_date", "tmdb_season_air_date", "target_season_air_date",
        "selected_video", "selected_file", "remote_file", "source_file",
        "filename", "original_basename", "name", "path", "source_path",
        "cloud_path", "extension", "suffix", "file_id", "remote_file_id", "fid", "id",
    )
    return {name: getattr(value, name) for name in names if hasattr(value, name)}


def _first_value(value: Mapping[str, Any], *keys: str) -> Any:
    for key in keys:
        candidate = value.get(key)
        if candidate is not None and candidate != "":
            return candidate
    return None


def _explicit_season_air_date(value: Any) -> Any:
    """Return only an explicitly named TMDB season air date.

    A generic ``air_date`` may be an episode or release date and must never
    silently select a library quarter.
    """

    if isinstance(value, Mapping):
        for key in ("season_air_date", "tmdb_season_air_date", "target_season_air_date"):
            if key in value:
                return value[key]
        return None
    for key in ("season_air_date", "tmdb_season_air_date", "target_season_air_date"):
        if hasattr(value, key):
            return getattr(value, key)
    return None


def _selected_extension(value: Any) -> str:
    if value is None:
        return ""
    mapping = _as_mapping(value)
    explicit = _first_value(mapping, "extension", "suffix")
    if isinstance(explicit, str) and explicit:
        return explicit
    filename = _first_value(mapping, "filename", "name", "path", "source_path", "cloud_path")
    if not isinstance(filename, str):
        return ""
    basename = posixpath.basename(filename.replace("\\", "/"))
    suffix = posixpath.splitext(basename)[1]
    return suffix if suffix and suffix.casefold() != ".strm" else suffix


__all__ = [
    "PathMapper",
    "PathMappingError",
    "SeasonRoutingError",
    "ServiceRoots",
    "season_label_from_air_date",
]
