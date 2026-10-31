"""适配器层：真实/模拟实现共用同一接口。

每个适配器的"完成判定"都要求对应真实证据：
- Cloud115: 目标目录出现真实视频文件列表，不接受 HTTP 200 / accepted。
- LitePan: 预期 STRM 文件真实存在。
- MoviePilot: NFO 可解析且 season/episode 正确。
- Emby: 能按作品/季/集查询到条目。
"""
from __future__ import annotations

import os
import copy
import json
import posixpath
import re
import unicodedata
from dataclasses import dataclass, field
from typing import Any, Callable, Dict, List, Mapping, Optional

VIDEO_EXT = {".mkv", ".mp4", ".ts", ".iso", ".m2ts", ".avi", ".mov", ".webm"}


# ---------------- 115 离线下载 ----------------
@dataclass
class OfflineTask:
    infohash: str
    task_id: str
    status: str  # downloading | done | failed


class RemoteInventoryIncomplete(RuntimeError):
    """A bounded remote inventory ended without proof of completeness.

    A caller may retry the inventory later, but must not interpret this error
    as proof that an item is absent remotely.
    """

    def __init__(self, message: str = "remote inventory incomplete", *, kind: str = "remote") -> None:
        self.kind = str(kind)
        super().__init__(str(message))


class RemoteDirectoryMissing(RemoteInventoryIncomplete):
    """A complete parent inventory proves a requested directory is absent."""


class RemoteRateLimited(RemoteInventoryIncomplete):
    """A 115 read failed at the transport level (throttle or outage).

    Unlike an inventory contradiction, this says nothing about what the
    directory actually contains: the caller should back off and retry later
    instead of spending the episode's contradiction review budget.
    """


class RemotePathConflict(RuntimeError):
    """A remote no-replace operation observed a different target."""


class Cloud115Adapter:
    """115 离线下载适配器接口。"""

    def submit(self, magnet_or_torrent: str) -> str:
        raise NotImplementedError

    def list_tasks(self) -> List[OfflineTask]:
        raise NotImplementedError

    def iter_tasks(self, max_pages: Optional[int] = None):
        """Iterate a complete bounded remote task inventory."""
        del max_pages
        yield from self.list_tasks()

    def find_task(self, infohash: str) -> Optional[OfflineTask]:
        target = str(infohash or "").strip().lower()
        for task in self.iter_tasks():
            if str(task.infohash).strip().lower() == target:
                return task
        return None

    def list_files(self, directory_id: str) -> List[Dict[str, Any]]:
        raise NotImplementedError

    def get_file(self, path: str) -> Optional[Dict[str, Any]]:
        """Return one remote file by its canonical path when supported."""
        del path
        raise NotImplementedError

    def mkdir(self, path: str) -> Any:
        """Create/reconcile a directory and return its remote identity."""
        del path
        raise NotImplementedError

    def get_directory(self, directory_id: str) -> Optional[Dict[str, Any]]:
        """Re-read one directory identity after a create/reconcile write."""
        del directory_id
        raise NotImplementedError

    def move_file(self, file_id: str, directory_id: str) -> Any:
        raise NotImplementedError

    def rename_file(self, file_id: str, name: str) -> Any:
        raise NotImplementedError

    def rename_file_no_replace(
        self,
        file_id: str,
        name: str,
        *,
        expected_identity: Optional[Mapping[str, Any]] = None,
        target_path: Optional[str] = None,
    ) -> Any:
        """Atomically rename only when the destination is still absent.

        Implementations must return/raise a path conflict instead of replacing
        an existing destination.  ``expected_identity`` lets a backend with a
        conditional rename primitive bind the operation to the selected file.
        """
        del file_id, name, expected_identity, target_path
        raise NotImplementedError

    def rename_file_if_identity(
        self,
        file_id: str,
        name: str,
        *,
        expected_identity: Optional[Mapping[str, Any]] = None,
        target_path: Optional[str] = None,
    ) -> Any:
        """Compatibility spelling for the atomic rename contract."""
        return self.rename_file_no_replace(
            file_id,
            name,
            expected_identity=expected_identity,
            target_path=target_path,
        )


class Mock115(Cloud115Adapter):
    """可脚本化的 115 模拟器。

    fail_submit / timeout_submit / preexisting_hash 控制测试场景。
    """

    def __init__(self, video_names: Optional[Dict[str, List[str]]] = None,
                 fail_submit: bool = False, timeout_submit: bool = False,
                 preexisting_hash: Optional[str] = None):
        self.tasks: Dict[str, OfflineTask] = {}
        self.submit_calls = 0
        self.fail_submit = fail_submit
        self.timeout_submit = timeout_submit
        self.preexisting_hash = preexisting_hash
        self.video_names = video_names or {}  # infohash -> [file names]
        self.watched_dirs: Dict[str, Dict[str, List[str]]] = {}
        self.moves: List[tuple[str, str]] = []
        self.renames: List[tuple[str, str]] = []

    def submit(self, uri: str) -> str:
        self.submit_calls += 1
        ih = uri.split("btih:")[1].split("&")[0].lower() if "btih:" in uri else uri
        if self.timeout_submit:
            # 服务端实际已接收任务，但客户端未收到响应
            task_id = f"task-{len(self.tasks) + 1:04d}"
            self.tasks[ih] = OfflineTask(infohash=ih, task_id=task_id, status="done")
            raise TimeoutError("submit request timeout")
        if self.fail_submit:
            raise RuntimeError("115 submit rejected")
        task_id = f"task-{len(self.tasks) + 1:04d}"
        self.tasks[ih] = OfflineTask(infohash=ih, task_id=task_id, status="done")
        if self.preexisting_hash and ih == self.preexisting_hash.lower():
            # 模拟"其实早就提交过"
            task_id = "task-preexisting"
        return task_id

    def complete(self, infohash: str, files: List[str]) -> None:
        t = self.tasks.get(infohash)
        if t:
            t.status = "done"
        self.video_names[infohash] = files

    def list_tasks(self) -> List[OfflineTask]:
        return list(self.tasks.values())

    def iter_tasks(self, max_pages: Optional[int] = None):
        del max_pages
        yield from self.list_tasks()

    def find_task(self, infohash: str) -> Optional[OfflineTask]:
        target = str(infohash or "").strip().lower()
        for task in self.tasks.values():
            if task.infohash.lower() == target:
                return task
        return None

    def list_files(self, directory_id: str) -> List[Dict[str, Any]]:
        files = []
        for ih, names in self.video_names.items():
            if directory_id == "target" or directory_id in ("", None):
                for n in names:
                    files.append({"name": n, "infohash": ih, "file_id": f"f-{n}"})
        return files

    def move_file(self, file_id: str, directory_id: str) -> Dict[str, Any]:
        self.moves.append((str(file_id), str(directory_id)))
        return {"state": True}

    def rename_file(self, file_id: str, name: str) -> Dict[str, Any]:
        self.renames.append((str(file_id), str(name)))
        return {"state": True}

    def get_directory(self, directory_id: str) -> Dict[str, Any]:
        return {"directory_id": str(directory_id), "path": str(directory_id)}

    def rename_file_no_replace(
        self,
        file_id: str,
        name: str,
        *,
        expected_identity: Optional[Mapping[str, Any]] = None,
        target_path: Optional[str] = None,
    ) -> Dict[str, Any]:
        del expected_identity, target_path
        return self.rename_file(file_id, name)


def has_video_file(files: List[Dict[str, Any]]) -> bool:
    """cloud_ready 判定：目标目录中必须有真实视频文件。"""
    for f in files:
        name = str(f.get("name") or "")
        _, ext = os.path.splitext(name)
        if ext.lower() in VIDEO_EXT:
            return True
    return False


# ---------------- LitePan ----------------
class LitePanAdapter:
    def list_tasks(self) -> List[Dict[str, Any]]:
        """Return the bounded runtime task inventory used for reconciliation."""
        raise NotImplementedError

    def list_runtime_tasks(self) -> List[Dict[str, Any]]:
        """Compatibility spelling for the runtime-task inventory port."""
        return self.list_tasks()

    def trigger_event(self, event: str, source: str, message: str) -> Any:
        raise NotImplementedError

    def list_strm(self) -> List[str]:
        raise NotImplementedError


class MockLitePan(LitePanAdapter):
    """模拟 LitePan：事件返回 200，但 STRM 生成由 complete_batch 控制。"""

    def __init__(self, strm_root: str, tasks: Optional[List[Dict[str, Any]]] = None):
        self.strm_root = strm_root
        self.events: List[Dict[str, Any]] = []
        self.fail_event = False
        self.tasks = copy.deepcopy(tasks or [{
            "id": "mock-litepan-task",
            "name": "追番",
            "active": True,
            "source_root": "/source",
            "output_root": strm_root,
        }])

    def list_tasks(self) -> List[Dict[str, Any]]:
        return copy.deepcopy(self.tasks)

    def trigger_event(self, event: str, source: str, message: str) -> Any:
        if self.fail_event:
            raise RuntimeError("litepan event failed")
        self.events.append({"event": event, "source": source, "message": message})
        return {"status": 200, "accepted": True}

    def generate_strm(self, relative_paths: List[str]) -> None:
        for rp in relative_paths:
            path = os.path.join(self.strm_root, rp)
            os.makedirs(os.path.dirname(path), exist_ok=True)
            with open(path, "w", encoding="utf-8") as f:
                f.write("http://mock-litepan/api/strm/play/mock")

    def list_strm(self) -> List[str]:
        out = []
        for root, _dirs, files in os.walk(self.strm_root):
            for name in files:
                if name.lower().endswith(".strm"):
                    out.append(os.path.join(root, name))
        return out


def expected_strm_exists(strm_root: str, relative_path: str) -> bool:
    path = os.path.join(strm_root, relative_path)
    if not os.path.isfile(path):
        return False
    # STRM 有效性：内容非空且为 URL
    with open(path, "r", encoding="utf-8", errors="replace") as f:
        content = f.read().strip()
    return content.startswith(("http://", "https://"))


# ---------------- MoviePilot ----------------
@dataclass
class ScrapeResult:
    ok: bool
    nfo_path: str = ""
    season: Optional[int] = None
    episode: Optional[int] = None
    detail: Dict[str, Any] = field(default_factory=dict)


class MoviePilotAdapter:
    def list_subscriptions(self) -> List[Dict[str, Any]]:
        """Return the current MoviePilot subscription snapshot."""
        raise NotImplementedError

    def scrape_in_place(self, strm_path: str, media_source: str, media_id: str,
                        type_name: str = "电视剧") -> ScrapeResult:
        raise NotImplementedError


class MockMoviePilot(MoviePilotAdapter):
    """模拟 MP：可控制失败；写入的 NFO 季集由 nfo_season/nfo_episode 决定。"""

    def __init__(self, fail: bool = False, nfo_season: Optional[int] = None,
                 nfo_episode: Optional[int] = None,
                 subscriptions: Optional[List[Dict[str, Any]]] = None):
        self.calls: List[Dict[str, Any]] = []
        self.fail = fail
        self.nfo_season = nfo_season
        self.nfo_episode = nfo_episode
        self.nfo_root = ""
        self.subscriptions = copy.deepcopy(subscriptions or [])
        self.list_subscriptions_calls = 0

    def list_subscriptions(self) -> List[Dict[str, Any]]:
        self.list_subscriptions_calls += 1
        return copy.deepcopy(self.subscriptions)

    def scrape_in_place(self, strm_path: str, media_source: str, media_id: str,
                        type_name: str = "电视剧") -> ScrapeResult:
        self.calls.append({"strm": strm_path, "media_id": media_id})
        if self.fail:
            return ScrapeResult(ok=False, detail={"error": "mock scrape failure"})
        nfo_path = os.path.splitext(strm_path)[0] + ".nfo"
        os.makedirs(os.path.dirname(nfo_path) or ".", exist_ok=True)
        season = self.nfo_season if self.nfo_season is not None else 2
        episode = self.nfo_episode if self.nfo_episode is not None else 1
        with open(nfo_path, "w", encoding="utf-8") as f:
            f.write(f"<episodedetails><season>{season}</season>"
                    f"<episode>{episode}</episode>"
                    f"<tmdbid>{media_id}</tmdbid></episodedetails>")
        return ScrapeResult(ok=True, nfo_path=nfo_path, season=season, episode=episode)


class FakeMoviePilot(MockMoviePilot):
    """In-memory MoviePilot fake used by discovery tests."""


class SourceProvider:
    """Read-only source-provider port consumed by ``DiscoveryService``."""

    name = "source"

    def discover(self, subscription: Any, window: Any) -> List[Any]:
        raise NotImplementedError


class FakeSourceProvider(SourceProvider):
    """Deterministic in-memory source provider.

    ``results`` may be one list shared by all subscriptions or a mapping keyed
    by ``subscription_id``.  ``failures`` injects a per-subscription exception
    without affecting other subscriptions.
    """

    name = "fake"

    def __init__(
        self,
        results: Any = None,
        failures: Optional[Mapping[str, Any]] = None,
        name: str = "fake",
    ) -> None:
        self.results = copy.deepcopy(results if results is not None else [])
        self.failures = dict(failures or {})
        self.name = name
        self.calls = 0

    def discover(self, subscription: Any, window: Any) -> List[Any]:
        del window
        self.calls += 1
        subscription_id = str(getattr(subscription, "subscription_id", ""))
        if subscription_id in self.failures:
            failure = self.failures[subscription_id]
            if isinstance(failure, BaseException):
                raise failure
            raise RuntimeError(str(failure))

        if isinstance(self.results, Mapping):
            selected = self.results.get(subscription_id, [])
        else:
            selected = self.results
            if isinstance(selected, list) and selected and all(
                isinstance(item, Mapping) and "subscription_id" in item
                for item in selected
            ):
                selected = [
                    item for item in selected
                    if str(item.get("subscription_id")) == subscription_id
                ]
        if selected is None:
            return []
        return copy.deepcopy(list(selected))


# Friendly names used by focused tests and operator-facing examples.
FakeProvider = FakeSourceProvider
MockSourceProvider = FakeSourceProvider


def parse_episode_nfo(nfo_path: str):
    """完成判定：NFO 可解析且 season/episode 正确。返回 (season, episode, tmdbid)。"""
    import xml.etree.ElementTree as ET

    root = ET.parse(nfo_path).getroot()
    if root.tag != "episodedetails":
        raise ValueError(f"不是剧集 NFO: root={root.tag}")
    season = int(root.findtext("season") or "-1")
    episode = int(root.findtext("episode") or "-1")
    tmdbid = root.findtext("tmdbid") or ""
    return season, episode, tmdbid


# ---------------- Emby ----------------
class EmbyAdapter:
    def find_library(self, name: str, root: str) -> Optional[Dict[str, Any]]:
        raise NotImplementedError

    def iter_episodes(self, library_id: str):
        raise NotImplementedError

    def refresh_library(self, library_id: str) -> Any:
        raise NotImplementedError

    def find_episode(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        mapper = kwargs.pop("mapper", None)
        library_id = kwargs.pop("library_id", None)
        if kwargs:
            raise TypeError(f"unexpected Emby arguments: {', '.join(kwargs)}")
        if args and _looks_like_episode_identity(args[0]):
            identity = args[0] if isinstance(args[0], Mapping) else args[0]
            expected_path = args[1] if len(args) > 1 else ""
            identity_values = _identity_values(identity)
            resolved_library = identity_values.get("library_id") or library_id
            return find_episode_match(
                self.iter_episodes(resolved_library), identity, expected_path,
                mapper=mapper,
            )
        if len(args) < 4:
            raise TypeError("find_episode requires an identity/path or legacy library/work/season/episode")
        resolved_library, work, season, episode = args[:4]
        path_hint = args[4] if len(args) > 4 else ""
        for item in self.iter_episodes(resolved_library):
            if (
                _item_series_name(item) == str(work)
                and _item_number(item, "season") == season
                and _item_number(item, "episode") == episode
                and (not path_hint or item.get("Path", item.get("path")) == path_hint)
            ):
                return dict(item)
        return None


class EmbyInventoryError(RuntimeError):
    """The Emby inventory cannot prove a complete, safe answer."""


class EmbyMatchAmbiguous(RuntimeError):
    """Two equal-ranked Emby episodes satisfy the requested identity."""


_LIBRARY_LOCATION_KEYS = frozenset({
    "locations", "physicallocations", "path",
})


def _library_location_values(library: Mapping[str, Any]) -> list[str]:
    """Collect every known Emby library-root alias before normalizing it."""

    values: list[str] = []
    saw_alias = False
    for key, raw in library.items():
        normalized_key = re.sub(r"[^a-z]", "", str(key).casefold())
        if normalized_key not in _LIBRARY_LOCATION_KEYS:
            continue
        saw_alias = True
        if raw is None:
            raise EmbyInventoryError("Emby library location alias is null")
        entries = [raw] if isinstance(raw, str) else raw
        if not isinstance(entries, (list, tuple)):
            raise EmbyInventoryError("Emby library locations are malformed")
        for entry in entries:
            if not isinstance(entry, str) or not entry:
                raise EmbyInventoryError("Emby library location is malformed")
            values.append(entry)
    if not saw_alias:
        raise EmbyInventoryError("Emby library inventory lacks a location")
    return values


def _validate_emby_item_contract(item: Mapping[str, Any]) -> None:
    """Validate fields required before identity/path ranking is attempted."""

    if not isinstance(item, Mapping):
        raise EmbyInventoryError("Emby episode inventory entry is malformed")
    item_id = item.get("Id", item.get("id"))
    if type(item_id) is not str or not item_id.strip():
        raise EmbyInventoryError("Emby episode inventory entry lacks id")
    path = item.get("Path", item.get("path"))
    if type(path) is not str or not path.strip():
        raise EmbyInventoryError("Emby episode inventory entry lacks path")
    if not _normalize_emby_path(path):
        raise EmbyInventoryError("Emby episode inventory entry has an unsafe path")
    series_name = item.get("SeriesName", item.get("series_name"))
    if type(series_name) is not str or not series_name.strip():
        raise EmbyInventoryError("Emby episode inventory entry lacks SeriesName")
    series_id = item.get("SeriesId", item.get("series_id"))
    if type(series_id) is not str or not series_id.strip():
        raise EmbyInventoryError("Emby episode inventory entry lacks SeriesId")
    provider_ids = item.get("ProviderIds", item.get("provider_ids"))
    if type(provider_ids) is not dict or not provider_ids:
        raise EmbyInventoryError("Emby episode inventory entry lacks ProviderIds")
    for key, value in provider_ids.items():
        if type(key) is not str or not key.strip():
            raise EmbyInventoryError("Emby ProviderIds key is malformed")
        if type(value) is not str or not value.strip():
            raise EmbyInventoryError("Emby ProviderIds value is malformed")
    if _item_number(item, "season") is None:
        raise EmbyInventoryError("Emby episode inventory entry lacks season")
    if _item_number(item, "episode") is None:
        raise EmbyInventoryError("Emby episode inventory entry lacks episode")
    _provider_tmdb(item)


class MockEmby(EmbyAdapter):
    def __init__(self, items: Optional[List[Dict[str, Any]]] = None,
                 refresh_accepts: bool = True,
                 libraries: Optional[List[Dict[str, Any]]] = None):
        self.items = items or []
        self.refresh_calls: List[str] = []
        self.refresh_accepts = refresh_accepts
        self.libraries = list(libraries) if libraries is not None else []

    def find_library(self, name: str, root: str) -> Optional[Dict[str, Any]]:
        libraries = self.libraries
        if not libraries:
            ids = []
            for item in self.items:
                library_id = item.get("library_id") or item.get("LibraryId")
                if library_id and library_id not in ids:
                    ids.append(library_id)
            libraries = [
                {"Id": library_id, "Name": name, "Locations": [root]}
                for library_id in ids
            ]
        matches = []
        expected_root = _normalize_emby_path(root)
        if not expected_root:
            raise EmbyInventoryError("Emby library root is unsafe")
        for library in libraries:
            if not isinstance(library, Mapping):
                raise EmbyInventoryError("Emby library inventory entry is malformed")
            library_name = library.get("Name", library.get("name"))
            library_id = library.get("Id", library.get("id"))
            if not isinstance(library_name, str) or not library_name.strip():
                raise EmbyInventoryError("Emby library inventory entry lacks name")
            if not isinstance(library_id, (str, int)) or not str(library_id).strip():
                raise EmbyInventoryError("Emby library inventory entry lacks id")
            locations = _library_location_values(library)
            normalized_locations = [_normalize_emby_path(location) for location in locations]
            if any(not location for location in normalized_locations):
                raise EmbyInventoryError("Emby library locations are unsafe")
            if len(set(normalized_locations)) != 1:
                raise EmbyInventoryError("Emby library location aliases conflict")
            if (library_name == name and any(
                    location == expected_root for location in normalized_locations
            )):
                matches.append(dict(library))
        if len(matches) != 1:
            raise EmbyInventoryError("Emby library name and root are not unique")
        return matches[0]

    def iter_episodes(self, library_id: str):
        for item in self.items:
            if item.get("library_id", item.get("LibraryId")) == library_id:
                yield item

    def refresh_library(self, library_id: str) -> Any:
        self.refresh_calls.append(library_id)
        if not self.refresh_accepts:
            raise RuntimeError("emby refresh rejected")
        return {"accepted": True}

    def find_episode(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        mapper = kwargs.pop("mapper", None)
        library_id = kwargs.pop("library_id", None)
        if kwargs:
            raise TypeError(f"unexpected Emby arguments: {', '.join(kwargs)}")

        # V5 identity/path API: find_episode(identity, expected_path).
        if args and _looks_like_episode_identity(args[0]):
            identity = args[0] if isinstance(args[0], Mapping) else args[0]
            expected_path = args[1] if len(args) > 1 else ""
            selected = self.items
            identity_values = _identity_values(identity)
            if identity_values.get("library_id") or library_id:
                expected_library = identity_values.get("library_id") or library_id
                selected = [
                    item for item in selected
                    if item.get("library_id", item.get("LibraryId")) == expected_library
                ]
            return find_episode_match(selected, identity, expected_path, mapper=mapper)

        # Legacy executor API remains usable while callers migrate to V5.
        if len(args) < 4:
            raise TypeError("find_episode requires an identity/path or legacy library/work/season/episode")
        library_id, work, season, episode = args[:4]
        path_hint = args[4] if len(args) > 4 else ""
        for item in self.items:
            if (item.get("library_id", item.get("LibraryId")) == library_id
                    and _item_series_name(item) == str(work)
                    and _item_number(item, "season") == season
                    and _item_number(item, "episode") == episode):
                if path_hint and item.get("Path", item.get("path")) != path_hint:
                    continue
                return item
        return None


def find_episode_match(
    items: Any,
    identity: Any,
    expected_path: str,
    *,
    mapper: Any = None,
    series_provider_ids: Mapping[str, Any] | None = None,
) -> Optional[Dict[str, Any]]:
    """Select one Emby episode by series attribution, name, and exact path.

    Every candidate must match the season, episode, and normalized current
    path before series attribution is enforced.  An episode item's Tmdb
    provider id is episode-scoped in Emby, so it only informs ranking and
    never vetoes a candidate.  Series attribution is instead constrained
    jointly by: (a) a hard expected-SeriesId gate when the identity carries a
    trusted series id; (b) the optional ``series_provider_ids`` mapping,
    which requires the candidate's series to be one of the queried series
    whose series-level Tmdb work id equals the subscription ``media_id``;
    (c) the exact path anchor; and (d) name ranking (provider id, known
    series id, alias, canonical name).
    """

    values = _identity_values(identity)
    media_id = _normalize_provider_id(
        _identity_text(values, "media_id", "tmdb_id", "provider_id", "id")
    )
    season = _identity_number(values, "season")
    episode = _identity_number(values, "episode")
    canonical = _normalize_name(_identity_text(values, "canonical_work", "work", "title"))
    aliases = {
        _normalize_name(value)
        for value in _identity_aliases(values)
        if _normalize_name(value)
    }
    expected_series_id = _identity_text(values, "series_id", "known_series_id")
    known_series_ids = {expected_series_id} - {""}
    # The series->work mapping gate is armed only when the caller supplied
    # the mapping AND the identity carries an expected work id; an empty
    # mapping then rejects every candidate (fail-closed).
    enforce_series_work_id = series_provider_ids is not None and bool(media_id)
    expected_key = _normalize_emby_path(expected_path, mapper=mapper)
    if not expected_key:
        raise EmbyInventoryError("expected Emby path is missing or unsafe")

    materialized = list(items or [])
    normalized_items: list[tuple[Mapping[str, Any], str | None, str | None]] = []
    provider_series_ids: set[str] = set()
    for item in materialized:
        _validate_emby_item_contract(item)
        item_season = _item_number(item, "season")
        item_episode = _item_number(item, "episode")
        if item_season is None or item_episode is None:
            raise EmbyInventoryError("Emby episode inventory entry lacks season or episode")
        provider_id = _provider_tmdb(item)
        series_id = _item_series_id(item)
        if provider_id == media_id and series_id:
            provider_series_ids.add(series_id)
        path = item.get("Path", item.get("path"))
        _normalize_emby_path(path, mapper=mapper)
        normalized_items.append((item, provider_id, series_id if series_id else None))

    known_series_ids.update(provider_series_ids)
    candidates: list[tuple[int, Mapping[str, Any]]] = []
    for item, provider_id, series_id in normalized_items:
        if _item_number(item, "season") != season or _item_number(item, "episode") != episode:
            continue
        path = item.get("Path", item.get("path"))
        if _normalize_emby_path(path, mapper=mapper) != expected_key:
            continue
        # Hard gate: a trusted identity-side SeriesId can never be repaired
        # by names, aliases, or provider ids.
        if expected_series_id and series_id != expected_series_id:
            continue
        # Series-level work-id gate: the candidate's series must be one of
        # the queried series and its series-level Tmdb work id must equal
        # the subscription media_id (normalized comparison).
        if enforce_series_work_id:
            mapped_work_id = (
                series_provider_ids.get(series_id) if series_id is not None else None
            )
            if _normalize_provider_id(mapped_work_id) != media_id:
                continue
        # An episode item's Tmdb provider id is episode-scoped in Emby:
        # MoviePilot writes the TMDB episode id into episode NFOs, so it can
        # never equal the show-level media_id.  Show identity is already
        # proven by the exact canonical path match above; a provider id here
        # only informs ranking and must not veto the path-proven candidate.
        name = _normalize_name(_item_series_name(item))
        if provider_id == media_id:
            rank = 0
        elif series_id and series_id in known_series_ids:
            rank = 1
        elif name in aliases:
            rank = 2
        elif canonical and name == canonical:
            rank = 3
        else:
            continue
        candidates.append((rank, item))

    if not candidates:
        return None
    best_rank = min(rank for rank, _item in candidates)
    best = [item for rank, item in candidates if rank == best_rank]
    if len(best) != 1:
        raise EmbyMatchAmbiguous("Emby episode identity is ambiguous")
    return dict(best[0])


# Short public spelling used by adapter-focused callers.
find_match = find_episode_match


def _identity_values(identity: Any) -> dict[str, Any]:
    if isinstance(identity, Mapping):
        values = dict(identity)
        key = values.get("key") or values.get("episode_key")
        if isinstance(key, Mapping):
            values = {**values, **dict(key)}
        elif key is not None:
            for field in ("media_source", "media_id", "season", "episode"):
                if field not in values and hasattr(key, field):
                    values[field] = getattr(key, field)
        return values
    return {
        name: getattr(identity, name)
        for name in ("media_source", "media_id", "season", "episode", "canonical_work",
                     "work", "aliases", "series_id")
        if hasattr(identity, name)
    }


def _looks_like_episode_identity(value: Any) -> bool:
    if isinstance(value, Mapping):
        return True
    return any(hasattr(value, name) for name in ("media_id", "tmdb_id", "episode_key"))


def _identity_text(values: Mapping[str, Any], *names: str) -> str:
    for name in names:
        value = values.get(name)
        if value not in (None, ""):
            return str(value).strip()
    return ""


def _identity_number(values: Mapping[str, Any], name: str) -> int | None:
    value = values.get(name)
    if value in (None, ""):
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _identity_aliases(values: Mapping[str, Any]) -> list[str]:
    aliases = values.get("aliases", values.get("aliases_json", []))
    if isinstance(aliases, str):
        try:
            aliases = json.loads(aliases)
        except (TypeError, ValueError):
            aliases = [aliases]
    if not isinstance(aliases, (list, tuple, set)):
        return []
    return [str(value).strip() for value in aliases if str(value).strip()]


def _provider_tmdb(item: Mapping[str, Any]) -> str | None:
    provider_ids = item.get("ProviderIds", item.get("provider_ids", {}))
    if provider_ids is None:
        return None
    if type(provider_ids) is not dict:
        raise EmbyInventoryError("Emby ProviderIds is malformed")
    values: list[str] = []
    for key, value in provider_ids.items():
        if type(key) is not str or not key.strip():
            raise EmbyInventoryError("Emby ProviderIds key is malformed")
        if type(value) is not str or not value.strip():
            raise EmbyInventoryError("Emby ProviderIds value is malformed")
        alias = re.sub(r"[^a-z]", "", key.casefold())
        if alias not in {"tmdb", "tmdbid"}:
            continue
        normalized = _normalize_provider_id(value)
        if not normalized:
            raise EmbyInventoryError("Emby TMDB provider identity is empty")
        values.append(normalized)
    if not values:
        return None
    if len(set(values)) != 1:
        raise EmbyInventoryError("Emby TMDB provider aliases conflict")
    return values[0]


def _normalize_provider_id(value: Any) -> str:
    if type(value) is not str:
        return ""
    text = value.strip()
    if not text or any(ord(char) < 0x20 or ord(char) == 0x7F for char in text):
        return ""
    return text.casefold()


def _item_series_id(item: Mapping[str, Any]) -> str | None:
    value = item.get("SeriesId", item.get("series_id"))
    return value.strip() if type(value) is str and value.strip() else None


def _item_series_name(item: Mapping[str, Any]) -> str:
    value = item.get("SeriesName", item.get("series_name", item.get("series", "")))
    return str(value or "").strip()


def _item_number(item: Mapping[str, Any], field: str) -> int | None:
    aliases = {
        "season": ("ParentIndexNumber", "parent_index_number", "season"),
        "episode": ("IndexNumber", "index_number", "episode"),
    }[field]
    present = [item[name] for name in aliases if name in item]
    if not present:
        return None
    if any(type(value) is not int for value in present):
        raise EmbyInventoryError(f"Emby {field} number is malformed")
    result = present[0]
    if any(value != result for value in present[1:]):
        raise EmbyInventoryError(f"Emby {field} aliases conflict")
    return result


def _coerce_int(value: Any) -> int | None:
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _normalize_name(value: Any) -> str:
    text = unicodedata.normalize("NFKC", str(value or "")).casefold()
    return re.sub(r"[^\w\u3400-\u9fff]+", "", text, flags=re.UNICODE)


def _normalize_emby_path(value: Any, *, mapper: Any = None) -> str:
    if value in (None, ""):
        return ""
    text = str(value)
    if (text != text.strip() or "\\" in text or "%" in text
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in text)
            or "://" in text or "?" in text or "#" in text):
        return ""
    if not text.startswith("/") or text.startswith("//"):
        return ""
    raw_parts = text.split("/")[1:]
    if any(part in {"", ".", ".."} for part in raw_parts):
        return ""
    normalized = posixpath.normpath(text)
    parts = normalized.split("/")[1:]
    if normalized == "." or any(part in {"", ".", ".."} for part in parts):
        return ""
    if mapper is not None:
        try:
            root = str(mapper.roots.emby_mount_root)
            relative = mapper.relative_to(normalized, root, field="emby_path")
            normalized = root.rstrip("/") + ("/" + relative if relative else "")
        except Exception:
            return ""
    return normalized


# ---------------- 通知 ----------------
class NotifyAdapter:
    def deliver(self, message: str) -> Optional[str]:
        raise NotImplementedError


class MockNotify(NotifyAdapter):
    def __init__(self, fail_times: int = 0):
        self.fail_times = fail_times
        self.delivered: List[str] = []
        self.ids = 0

    def deliver(self, message: str) -> Optional[str]:
        if self.fail_times > 0:
            self.fail_times -= 1
            raise RuntimeError("notify gateway unavailable")
        self.ids += 1
        self.delivered.append(message)
        return f"msg-{self.ids}"
