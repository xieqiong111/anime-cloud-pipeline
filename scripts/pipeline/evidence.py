"""Secret-safe filesystem evidence for the STRM and metadata stages.

The evidence classes deliberately know nothing about the state machine or
about service clients.  They inspect one exact path set and return a small,
JSON-safe result.  In particular, a STRM playback URL is never returned in a
result detail or exception; only its scheme, host, length, and digest are
retained.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import os
import posixpath
import re
import stat
from collections.abc import Mapping
from typing import Any, Iterable
from urllib.parse import urlsplit
import xml.etree.ElementTree as ET

from .models import ReviewReason
from .security import sanitize_text


_READ_ERROR = object()
_SYMLINK = object()
_MAX_XML_BYTES = 1 * 1024 * 1024
_MAX_XML_NODES = 10_000
_MAX_XML_DEPTH = 32
_MAX_XML_TEXT = 256 * 1024
_XML_DECLARATION = re.compile(r"<!\s*(?:DOCTYPE|ENTITY)\b", re.IGNORECASE)
_SEASON_DIR = re.compile(r"^Season [0-9]+$")


@dataclass(frozen=True, slots=True)
class EvidenceResult:
    """Safe result returned by an evidence validator."""

    ok: bool
    detail: Mapping[str, Any] = field(default_factory=dict)
    review_reason: ReviewReason | None = None
    missing: tuple[str, ...] = ()

    @property
    def reason(self) -> str:
        """Return a stable machine-readable reason for reports/tests."""

        value = self.detail.get("reason") if isinstance(self.detail, Mapping) else None
        if value:
            return str(value)
        if self.review_reason is not None:
            return self.review_reason.value
        return "ok" if self.ok else "evidence_missing"

    @property
    def status(self) -> str:
        """Compatibility spelling used by stage-service result consumers."""

        return "ready" if self.ok else "pending"


class StrmEvidence:
    """Validate one exact STRM path and its HTTP(S) content."""

    def __init__(self, files: Any, *, secrets: Iterable[Any] = ()) -> None:
        self.files = files
        self.secrets = tuple(secrets)

    def validate(self, path: str) -> EvidenceResult:
        safe_path = _safe_path(path, self.secrets)
        if not isinstance(path, str) or not path:
            return _failure("invalid_path", path=safe_path)
        if not path.casefold().endswith(".strm"):
            return _failure("invalid_extension", path=safe_path)

        present, content = _read_exact(self.files, path)
        if not present:
            if content is _SYMLINK:
                return _failure("symlink", path=safe_path)
            if content is _READ_ERROR:
                return _failure("reader_error")
            return _failure("missing", path=safe_path, missing=(safe_path,))
        try:
            text = _text_content(content)
        except Exception:
            return _failure("reader_error")
        if text is None:
            return _failure("invalid_encoding", path=safe_path)
        candidate = text
        # LitePan terminates STRM files with a POSIX newline.  Only the
        # trailing line terminator is tolerated here: leading, embedded, or
        # other whitespace still fails the URL proof below.
        candidate = candidate.rstrip("\r\n")
        parsed = _parse_http_url(candidate)
        if parsed is None:
            return _failure("invalid_url", path=safe_path)

        scheme, host = parsed
        raw_bytes = candidate.encode("utf-8", "replace")
        # Keep only non-sensitive URL facts.  Do not store query, fragment,
        # userinfo, or the complete path: LitePan playback paths commonly
        # contain opaque tokens.
        return EvidenceResult(
            True,
            detail={
                "path": safe_path,
                "scheme": scheme,
                "host": sanitize_text(host, secrets=self.secrets),
                "content_length": len(raw_bytes),
                "content_sha256": hashlib.sha256(raw_bytes).hexdigest(),
            },
        )


class SidecarEvidence:
    """Validate exact episode, show, and season sidecars for one episode."""

    def __init__(self, files: Any, *, secrets: Iterable[Any] = ()) -> None:
        self.files = files
        self.secrets = tuple(secrets)

    def validate(self, episode: Any) -> EvidenceResult:
        strm_path = _episode_value(
            episode, "strm_path", "litepan_path", "moviepilot_path", "emby_path"
        )
        if not isinstance(strm_path, str) or not strm_path:
            return _failure("strm_path_missing")

        for alias_path in _present_episode_values(
            episode, ("strm_path", "litepan_path", "moviepilot_path", "emby_path")
        ):
            alias_error = _validate_sidecar_alias_path(alias_path)
            if alias_error is not None:
                return _failure(alias_error)
            if alias_path != strm_path:
                return _failure("noncanonical_sidecar_path")
            alias_link_state = _symlink_status(self.files, alias_path)
            if alias_link_state is _SYMLINK:
                return _failure("symlink", path=_safe_path(alias_path, self.secrets))
            if alias_link_state is _READ_ERROR:
                return _failure("reader_error")

        path_error = _validate_sidecar_path(strm_path)
        if path_error is not None:
            return _failure(path_error, path=_safe_path(strm_path, self.secrets))
        expected_season = _int_value(_episode_value(episode, "season"))
        expected_episode = _int_value(_episode_value(episode, "episode"))
        if expected_season is None or expected_episode is None:
            return self._metadata_conflict(
                strm_path,
                {"field": "identity", "observed": "episode_identity_missing"},
            )
        season_dir = posixpath.dirname(strm_path)
        if posixpath.basename(season_dir) != f"Season {expected_season}":
            return _failure("invalid_strm_path", path=_safe_path(strm_path, self.secrets))

        strm_link_state = _symlink_status(self.files, strm_path)
        if strm_link_state is _SYMLINK:
            return _failure("symlink", path=_safe_path(strm_path, self.secrets))
        if strm_link_state is _READ_ERROR:
            return _failure("reader_error")

        stem = strm_path[:-5] if strm_path.casefold().endswith(".strm") else ""
        if not stem:
            return _failure("invalid_strm_path", path=_safe_path(strm_path, self.secrets))
        work_dir = posixpath.dirname(season_dir)
        expected_paths = {
            "episode_nfo": f"{stem}.nfo",
            "episode_image": f"{stem}.jpg",
            "tvshow_nfo": f"{work_dir}/tvshow.nfo",
            "tvshow_poster": f"{work_dir}/poster.jpg",
            "season_nfo": f"{season_dir}/season.nfo",
            "season_poster": f"{season_dir}/poster.jpg",
        }
        aliases = {
            "episode_nfo": ("episode_nfo_path", "nfo_path"),
            "episode_image": (
                "episode_image_path", "episode_image", "image_path", "episode_poster_path"
            ),
            "tvshow_nfo": ("tvshow_nfo_path", "tvshow_nfo"),
            "tvshow_poster": ("tvshow_poster_path", "tvshow_poster", "show_poster_path"),
            "season_nfo": ("season_nfo_path", "season_nfo"),
            "season_poster": ("season_poster_path", "season_poster"),
        }
        paths: dict[str, str] = {}
        supplied_by_name: dict[str, list[tuple[str, str]]] = {}
        for name, aliases_for_name in aliases.items():
            supplied_by_name[name] = []
            for supplied in _present_episode_values(episode, aliases_for_name):
                alias_error = _validate_sidecar_alias_path(supplied)
                if alias_error is not None:
                    return _failure(alias_error)
                alias_link_state = _symlink_status(self.files, supplied)
                if alias_link_state is _SYMLINK:
                    return _failure("symlink", path=_safe_path(supplied, self.secrets))
                if alias_link_state is _READ_ERROR:
                    return _failure("reader_error")
                supplied_by_name[name].append((supplied, posixpath.normpath(supplied)))

        seen_paths: dict[str, str] = {}
        for name, values_for_name in supplied_by_name.items():
            for _raw, normalized in values_for_name:
                owner = seen_paths.get(normalized)
                if owner is not None and owner != name:
                    return _failure("sidecar_path_collision")
                seen_paths[normalized] = name

        for name, expected in expected_paths.items():
            supplied_values = supplied_by_name[name]
            if supplied_values:
                normalized_values = [item[1] for item in supplied_values]
                if any(item != normalized_values[0] for item in normalized_values[1:]):
                    wrong_path = next(
                        raw
                        for raw, normalized in supplied_values
                        if normalized != normalized_values[0]
                    )
                    return _failure(
                        "noncanonical_sidecar_path",
                        path=_safe_path(wrong_path, self.secrets),
                    )
                if normalized_values[0] != posixpath.normpath(expected):
                    return _failure(
                        "noncanonical_sidecar_path",
                        path=_safe_path(supplied_values[0][0], self.secrets),
                    )
                paths[name] = expected
            else:
                paths[name] = expected

        if len(set(paths.values())) != len(paths) or strm_path in paths.values():
            return _failure("sidecar_path_collision")

        values: dict[str, Any] = {}
        missing: list[str] = []
        for name, path in paths.items():
            present, content = _read_exact(self.files, path)
            if content is _READ_ERROR:
                return _failure("reader_error")
            if content is _SYMLINK:
                return _failure("symlink", path=_safe_path(path, self.secrets))
            if not present or _empty_content(content):
                missing.append(_safe_path(path, self.secrets))
            else:
                values[name] = content

        expected_media_id = _text_value(_episode_value(episode, "media_id", "tmdb_id"))

        # An explicit identity contradiction is stronger evidence than a
        # concurrently missing image/poster.  Report the review immediately
        # so an operator cannot mistake a wrong NFO for a transient scrape.
        if "episode_nfo" in values:
            mismatch = _check_episode_nfo(
                values["episode_nfo"], expected_season, expected_episode
            )
            if mismatch is not None:
                return self._metadata_conflict(paths["episode_nfo"], mismatch)
        if "tvshow_nfo" in values:
            mismatch = _check_show_nfo(values["tvshow_nfo"], expected_media_id)
            if mismatch is not None:
                return self._metadata_conflict(paths["tvshow_nfo"], mismatch)
        if "season_nfo" in values:
            mismatch = _check_season_nfo(values["season_nfo"], expected_season)
            if mismatch is not None:
                return self._metadata_conflict(paths["season_nfo"], mismatch)

        if missing:
            return EvidenceResult(
                False,
                detail={
                    "reason": "missing_sidecars",
                    "missing": list(missing),
                    "required_count": len(paths),
                },
                missing=tuple(missing),
            )

        return EvidenceResult(
            True,
            detail={
                "reason": "complete",
                "paths": [_safe_path(path, self.secrets) for path in paths.values()],
                "required_count": len(paths),
            },
        )

    def _metadata_conflict(self, path: str, mismatch: Mapping[str, Any]) -> EvidenceResult:
        detail = {
            "reason": "metadata_identity_conflict",
            "path": _safe_path(path, self.secrets),
        }
        detail.update(_safe_mapping(mismatch, self.secrets))
        return EvidenceResult(
            False,
            detail=detail,
            review_reason=ReviewReason.METADATA_CONFLICT,
        )


def _failure(reason: str, *, path: str = "", missing: tuple[str, ...] = ()) -> EvidenceResult:
    detail: dict[str, Any] = {"reason": reason}
    if path:
        detail["path"] = path
    if missing:
        detail["missing"] = list(missing)
    return EvidenceResult(False, detail=detail, missing=missing)


def _read_exact(files: Any, path: str) -> tuple[bool, Any]:
    """Read one exact path from mappings, fakes, or the local filesystem."""

    symlink = getattr(files, "is_symlink", None)
    if callable(symlink) and not isinstance(files, (str, os.PathLike)):
        try:
            if bool(symlink(path)):
                return False, _SYMLINK
        except Exception:
            return False, _READ_ERROR

    if isinstance(files, Mapping):
        try:
            if path not in files:
                return False, None
            return True, files[path]
        except Exception:
            return False, _READ_ERROR
    values = getattr(files, "values", None)
    if isinstance(values, Mapping):
        try:
            if path not in values:
                return False, None
            return True, values[path]
        except Exception:
            return False, _READ_ERROR
    nested = getattr(files, "files", None)
    if isinstance(nested, Mapping):
        try:
            if path not in nested:
                return False, None
            return True, nested[path]
        except Exception:
            return False, _READ_ERROR
    try:
        if hasattr(files, "__contains__") and path in files:
            return True, files[path]
    except Exception:
        return False, _READ_ERROR

    exists = getattr(files, "exists", None)
    if callable(exists):
        try:
            if not bool(exists(path)):
                return False, None
        except FileNotFoundError:
            return False, None
        except Exception:
            return False, _READ_ERROR
    elif isinstance(files, (str, os.PathLike)):
        root = os.fspath(files)
        candidate = path if os.path.isabs(path) else os.path.join(root, path)
        try:
            info = os.lstat(candidate)
        except FileNotFoundError:
            return False, None
        except OSError:
            return False, _READ_ERROR
        if stat.S_ISLNK(info.st_mode):
            return False, _SYMLINK
        if not stat.S_ISREG(info.st_mode):
            return False, None
    else:
        # A reader-only fake is allowed; a missing path is reported when read
        # raises FileNotFoundError rather than guessing from nearby paths.
        exists = None

    for name in ("read", "read_file", "read_text"):
        reader = getattr(files, name, None)
        if not callable(reader):
            continue
        try:
            return True, reader(path)
        except FileNotFoundError:
            return False, None
        except Exception:
            return False, _READ_ERROR

    if isinstance(files, (str, os.PathLike)):
        root = os.fspath(files)
        candidate = path if os.path.isabs(path) else os.path.join(root, path)
        flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
        fd = None
        try:
            fd = os.open(candidate, flags)
            with os.fdopen(fd, "rb") as handle:
                fd = None
                return True, handle.read()
        except FileNotFoundError:
            return False, None
        except OSError:
            return False, _READ_ERROR
        finally:
            if fd is not None:
                try:
                    os.close(fd)
                except OSError:
                    pass
    return (bool(exists), None) if exists is not None else (False, None)


def _symlink_status(files: Any, path: str) -> object | None:
    """Probe one path without opening it, preserving no-follow semantics."""

    symlink = getattr(files, "is_symlink", None)
    if callable(symlink) and not isinstance(files, (str, os.PathLike)):
        try:
            return _SYMLINK if bool(symlink(path)) else None
        except Exception:
            return _READ_ERROR
    if not isinstance(files, (str, os.PathLike)):
        return None
    root = os.fspath(files)
    candidate = path if os.path.isabs(path) else os.path.join(root, path)
    try:
        info = os.lstat(candidate)
    except FileNotFoundError:
        return None
    except OSError:
        return _READ_ERROR
    return _SYMLINK if stat.S_ISLNK(info.st_mode) else None


def _text_content(value: Any) -> str | None:
    if isinstance(value, bytes):
        try:
            return value.decode("utf-8")
        except UnicodeDecodeError:
            return None
    if isinstance(value, str):
        return value
    if value is None:
        return None
    return str(value)


def _empty_content(value: Any) -> bool:
    if value is None:
        return True
    if isinstance(value, bytes):
        return not value
    if isinstance(value, str):
        return not value.strip()
    return False


def _parse_http_url(value: str) -> tuple[str, str] | None:
    if not value or any(char.isspace() or ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        return None
    try:
        parsed = urlsplit(value)
        host = parsed.hostname
        port = parsed.port
    except ValueError:
        return None
    scheme = parsed.scheme.casefold()
    if scheme not in {"http", "https"} or not host:
        return None
    # A playback URL containing userinfo would expose credentials if its
    # netloc were persisted.  Reject it rather than attempting to normalize
    # the unsafe shape.
    if parsed.username is not None or parsed.password is not None:
        return None
    if port is not None and not 1 <= port <= 65535:
        return None
    return scheme, host


def _episode_value(value: Any, *names: str) -> Any:
    values = _present_episode_values(value, names)
    return values[0] if values else None


def _present_episode_values(value: Any, names: tuple[str, ...]) -> list[Any]:
    values: list[Any] = []
    for name in names:
        if isinstance(value, Mapping):
            if name not in value:
                continue
            candidate = value[name]
        else:
            candidate = getattr(value, name, None)
        if candidate not in (None, ""):
            values.append(candidate)
    return values


def _text_value(value: Any) -> str:
    if value in (None, ""):
        return ""
    return str(value).strip()


def _int_value(value: Any) -> int | None:
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _parse_xml(value: Any) -> ET.Element | None:
    if isinstance(value, bytes) and len(value) > _MAX_XML_BYTES:
        return None
    text = _text_content(value)
    if not text or len(text.encode("utf-8", "replace")) > _MAX_XML_BYTES:
        return None
    if _XML_DECLARATION.search(text):
        return None
    try:
        root = ET.fromstring(text)
    except (ET.ParseError, TypeError, ValueError):
        return None
    nodes = 0

    def walk(node: ET.Element, depth: int) -> bool:
        nonlocal nodes
        nodes += 1
        if nodes > _MAX_XML_NODES or depth > _MAX_XML_DEPTH:
            return False
        for text_value in (node.text, node.tail):
            if text_value and len(text_value) > _MAX_XML_TEXT:
                return False
        return all(walk(child, depth + 1) for child in list(node))

    return root if walk(root, 1) else None


def _find_text(root: ET.Element, *names: str) -> str:
    values = _find_texts(root, *names)
    return values[0] if values else ""


def _find_texts(root: ET.Element, *names: str) -> list[str]:
    wanted = {name.casefold() for name in names}
    values: list[str] = []
    for node in root.iter():
        tag = node.tag.rsplit("}", 1)[-1].casefold() if isinstance(node.tag, str) else ""
        if tag in wanted and node.text:
            text = node.text.strip()
            if text:
                values.append(text)
    return values


def _provider_id(root: ET.Element) -> str:
    values = _provider_ids(root)
    return values[0] if values else ""


def _provider_ids(root: ET.Element) -> list[str]:
    """Collect the NFO file's own TMDB identity values.

    Only direct children of the NFO root carry the file's identity.
    Kodi/MoviePilot NFO files also embed credited-person ids deeper in the
    tree (``<actor><tmdbid>…</tmdbid></actor>``, ``<director tmdbid="…">``);
    those are person ids and must never be compared with the show identity,
    or every scrape with credited persons reports a false conflict.
    """

    values: list[str] = []
    for node in root:
        tag = node.tag.rsplit("}", 1)[-1].casefold() if isinstance(node.tag, str) else ""
        text = (node.text or "").strip()
        if not text:
            continue
        if tag in {"tmdbid", "tmdb_id", "providerid"}:
            values.append(text)
        elif tag == "uniqueid":
            provider = str(node.attrib.get("type") or "").casefold()
            if provider in {"tmdb", "tmdbid", "tmdb_id"}:
                values.append(text)
    return values


def _check_episode_nfo(value: Any, season: int | None, episode: int | None) -> dict[str, Any] | None:
    root = _parse_xml(value)
    if root is None:
        return {"observed": "xml_rejected", "expected": "episodedetails"}
    if root.tag.rsplit("}", 1)[-1].casefold() != "episodedetails":
        return {"observed_root": _safe_xml_tag(root.tag), "expected_root": "episodedetails"}
    season_values = _find_texts(root, "season")
    episode_values = _find_texts(root, "episode")
    if not season_values or not episode_values:
        return {"field": "identity", "observed": "required_field_missing"}
    if not _consistent_values(season_values) or not _consistent_values(episode_values):
        return {"field": "identity", "observed": "duplicate_values_differ"}
    observed_season = _int_value(season_values[0] if season_values else None)
    observed_episode = _int_value(episode_values[0] if episode_values else None)
    if season is not None and observed_season != season:
        return {"field": "season", "expected": season, "observed": observed_season}
    if episode is not None and observed_episode != episode:
        return {"field": "episode", "expected": episode, "observed": observed_episode}
    # MoviePilot writes the TMDB *episode* id into an episode NFO's provider
    # fields, not the show id, so this file cannot prove show identity; the
    # id is only required to be internally consistent.  Show identity is
    # proven by tvshow.nfo against the subscription media_id.
    #
    # Known limitation: with only an episode-level TMDB id, the link between
    # an episode NFO and the work is unproven, and no authoritative expected
    # episode id exists in the data rows to reject on (doing so would route
    # every new show into manual review).  The identity chain instead rests
    # on strict tvshow.nfo identity, the canonical directory anchor, and the
    # season/episode consistency validated above.
    if not _consistent_values(_provider_ids(root)):
        return {"field": "media_id", "observed": "duplicate_values_differ"}
    return None


def _check_show_nfo(value: Any, media_id: str) -> dict[str, Any] | None:
    root = _parse_xml(value)
    if root is None:
        return {"observed": "xml_rejected", "expected": "tvshow"}
    if root.tag.rsplit("}", 1)[-1].casefold() != "tvshow":
        return {"observed_root": _safe_xml_tag(root.tag), "expected_root": "tvshow"}
    # Identity must be proven, not merely uncontradicted: with no expected id
    # or no provider id in the NFO there is nothing to compare, so fail closed.
    if not media_id:
        return {"field": "media_id", "observed": "expected_media_id_missing"}
    provider_values = _provider_ids(root)
    if not provider_values:
        return {"field": "media_id", "observed": "provider_id_missing"}
    if not _consistent_values(provider_values):
        return {"field": "media_id", "observed": "duplicate_values_differ"}
    if provider_values[0] != media_id:
        return {"field": "media_id", "expected": media_id, "observed": provider_values[0]}
    return None


def _check_season_nfo(value: Any, season: int | None) -> dict[str, Any] | None:
    root = _parse_xml(value)
    if root is None:
        return {"observed": "xml_rejected", "expected": "seasondetails"}
    root_name = root.tag.rsplit("}", 1)[-1].casefold()
    if root_name not in {"seasondetails", "season"}:
        return {"observed_root": _safe_xml_tag(root.tag), "expected_root": "seasondetails"}
    values = _find_texts(root, "seasonnumber", "season")
    if not _consistent_values(values) or not values:
        return {"field": "season", "observed": "duplicate_values_differ" if values else None}
    observed = _int_value(values[0])
    if season is not None and observed != season:
        return {"field": "season", "expected": season, "observed": observed}
    return None


def _consistent_values(values: list[str]) -> bool:
    return len({str(value).strip() for value in values}) <= 1


def _safe_xml_tag(value: Any) -> str:
    text = str(value or "")
    return text[:80]


def _validate_sidecar_path(value: str) -> str | None:
    if not value or not value.startswith("/") or value.startswith("//"):
        return "invalid_strm_path"
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value) or "\\" in value:
        return "invalid_strm_path"
    if "?" in value or "#" in value or "://" in value:
        return "invalid_strm_path"
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts[1:]):
        return "invalid_strm_path"
    if posixpath.normpath(value) != value or not value.casefold().endswith(".strm"):
        return "invalid_strm_path"
    season_dir = posixpath.basename(posixpath.dirname(value))
    if not _SEASON_DIR.fullmatch(season_dir):
        return "invalid_strm_path"
    if not posixpath.basename(posixpath.dirname(posixpath.dirname(value))):
        return "invalid_strm_path"
    return None


def _validate_sidecar_alias_path(value: Any) -> str | None:
    if not isinstance(value, str) or not value:
        return "unsafe_path"
    if not value.startswith("/") or value.startswith("//"):
        return "unsafe_path"
    if any(ord(char) < 0x20 or ord(char) == 0x7F for char in value):
        return "unsafe_path"
    if "\\" in value or "://" in value or "?" in value or "#" in value:
        return "unsafe_path"
    parts = value.split("/")
    if any(part in {"", ".", ".."} for part in parts[1:]):
        return "unsafe_path"
    if posixpath.normpath(value) != value:
        return "unsafe_path"
    return None


def _safe_path(value: Any, secrets: Iterable[Any]) -> str:
    return sanitize_text("" if value is None else str(value), secrets=secrets)


def _safe_mapping(value: Mapping[str, Any], secrets: Iterable[Any]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, item in value.items():
        if isinstance(item, Mapping):
            result[str(key)] = _safe_mapping(item, secrets)
        elif isinstance(item, (str, int, float, bool)) or item is None:
            result[str(key)] = sanitize_text(item, secrets=secrets)
        else:
            result[str(key)] = sanitize_text(item, secrets=secrets)
    return result


__all__ = ["EvidenceResult", "SidecarEvidence", "StrmEvidence"]
