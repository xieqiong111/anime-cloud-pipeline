"""Bounded quarter-library administration; runtime owns write authorization.

The verified Emby 4.9.5.0 UI defines the supported creation contract.
Public API reference: https://dev.emby.media/reference/RestAPI.html
No scan/refresh endpoint is called. Exceptions and public results use fixed codes.
"""
from __future__ import annotations

import copy
import posixpath
from collections.abc import Mapping

from .http import JsonHttpClient, RetryPolicy


class QuarterEmbyError(RuntimeError):
    pass


def _spec(spec):
    if not isinstance(spec, Mapping):
        raise QuarterEmbyError("emby_spec_invalid")
    name, path = spec.get("name"), spec.get("path")
    if (not isinstance(name, str) or not name.strip() or name != name.strip()
            or name in (".", "..") or "/" in name or "\\" in name
            or any(ord(c) < 32 for c in name)
            or not isinstance(path, str) or not path.startswith("/")
            or path == "/" or "\\" in path or "//" in path
            or any(ord(c) < 32 for c in path)
            or any(p in (".", "..") for p in path.split("/"))):
        raise QuarterEmbyError("emby_spec_invalid")
    return name, path.rstrip("/")


def _overlap(left, right):
    return (left in ("", "/") or right == "/" or left == right
            or left.startswith(right + "/") or right.startswith(left + "/"))


def _option_state(actual, expected):
    """Missing evidence is unknown; an explicitly different value conflicts.

    Extra object keys are harmless server defaults. TypeOptions are identified
    by Type rather than server serialization order, but every expected type and
    every disabling array must be explicitly present.
    """
    if isinstance(expected, Mapping):
        if not isinstance(actual, Mapping):
            return "unknown" if actual is None else "conflict"
        states = [_option_state(actual.get(k), v) if k in actual else "unknown"
                  for k, v in expected.items()]
    elif isinstance(expected, list) and expected and all(
            isinstance(i, Mapping) and "Type" in i for i in expected):
        if not isinstance(actual, list):
            return "unknown" if actual is None else "conflict"
        if any(not isinstance(i, Mapping) or "Type" not in i for i in actual):
            return "unknown"
        actual_types = [i["Type"] for i in actual]
        if any(not isinstance(t, str) for t in actual_types):
            return "conflict"
        if len(set(actual_types)) != len(actual_types):
            return "conflict"
        expected_types = {i["Type"] for i in expected}
        if set(actual_types) - expected_types:
            return "conflict"
        by_type = {i["Type"]: i for i in actual}
        states = [_option_state(by_type.get(i["Type"]), i) for i in expected]
    else:
        return "reuse" if type(actual) is type(expected) and actual == expected else (
            "unknown" if actual is None else "conflict")
    if "unknown" in states:
        return "unknown"
    return "conflict" if "conflict" in states else "reuse"


class QuarterEmbyClient:
    def __init__(self, base_url, api_key, transport=None):
        if not isinstance(api_key, str) or not api_key:
            raise QuarterEmbyError("emby_credentials_missing")
        self.base = base_url.rstrip("/")
        self._key = api_key
        self.http = JsonHttpClient(
            RetryPolicy(max_attempts=1), timeout=20, secrets=(api_key,), transport=transport)

    def _request(self, method, path, *, params=None, body=None):
        try:
            headers = {"X-Emby-Token": self._key}
            if body is not None:
                headers["Content-Type"] = "application/json"
            status, result = self.http.request_with_status(
                method, self.base + "/emby" + path,
                params=params, body=body, headers=headers)
            if status not in (200, 204):
                raise QuarterEmbyError("emby_request_failed")
            return result
        except Exception:
            raise QuarterEmbyError("emby_request_failed") from None

    def _libraries(self):
        rows = self._request("GET", "/Library/VirtualFolders")
        if not isinstance(rows, list):
            raise QuarterEmbyError("emby_inventory_incomplete")
        ids = set()
        for row in rows:
            if not isinstance(row, Mapping):
                raise QuarterEmbyError("emby_inventory_incomplete")
            identity = row.get("ItemId", row.get("Id"))
            locations = row.get("Locations")
            if (not isinstance(identity, str) or not identity or identity in ids
                    or not isinstance(row.get("Name"), str) or not row["Name"].strip()
                    or not isinstance(locations, list)
                    or any(not isinstance(p, str) or not p.startswith("/")
                           or posixpath.normpath(p) != (p.rstrip("/") or "/")
                           for p in locations)):
                raise QuarterEmbyError("emby_inventory_incomplete")
            ids.add(identity)
        return rows

    def inventory(self, spec):
        try:
            name, path = _spec(spec)
            rows = self._libraries()
        except QuarterEmbyError as exc:
            return {"state": "unknown", "reason": str(exc)}
        candidates = [r for r in rows if r["Name"] == name or any(
            _overlap(p.rstrip("/"), path) for p in r["Locations"])]
        if not candidates:
            return {"state": "create", "reason": "emby_library_missing"}
        if (len(candidates) == 1 and candidates[0]["Name"] == name
                and candidates[0]["Locations"] == [path]
                and candidates[0].get("CollectionType") == "tvshows"):
            try:
                expected = self._options(path, self._available())
            except QuarterEmbyError as exc:
                return {"state": "unknown", "reason": str(exc)}
            actual = candidates[0].get("LibraryOptions")
            quiet = _option_state(actual, expected)
            # Media ingestion may explicitly enable the local NFO reader.
            # All online providers and every other quiet option still match.
            if quiet != "reuse" and "Nfo" in expected["DisabledLocalMetadataReaders"]:
                local_nfo = copy.deepcopy(expected)
                local_nfo["DisabledLocalMetadataReaders"].remove("Nfo")
                if _option_state(actual, local_nfo) == "reuse":
                    quiet = "reuse"
            if quiet != "reuse":
                return {"state": quiet, "reason": "emby_library_options_" + (
                    "incomplete" if quiet == "unknown" else "conflict")}
            return {"state": "reuse", "reason": "emby_library_exact"}
        return {"state": "conflict", "reason": "emby_library_conflict"}

    def _available(self):
        available = self._request("GET", "/Libraries/AvailableOptions", params={
            "LibraryContentType": "tvshows", "IsNewLibrary": "true"})
        if not isinstance(available, Mapping):
            raise QuarterEmbyError("emby_provider_inventory_incomplete")
        types = available.get("TypeOptions")
        if not isinstance(types, list) or not types:
            raise QuarterEmbyError("emby_provider_inventory_incomplete")
        seen = set()
        for row in types:
            if (not isinstance(row, Mapping) or not isinstance(row.get("Type"), str)
                    or not row["Type"] or row["Type"] in seen):
                raise QuarterEmbyError("emby_provider_inventory_incomplete")
            seen.add(row["Type"])
            for field in ("MetadataFetchers", "ImageFetchers"):
                if not isinstance(row.get(field), list):
                    raise QuarterEmbyError("emby_provider_inventory_incomplete")
        if not {"Series", "Season", "Episode"}.issubset(seen):
            raise QuarterEmbyError("emby_provider_inventory_incomplete")
        for field in ("MetadataReaders", "SubtitleFetchers", "LyricsFetchers"):
            items = available.get(field)
            if (not isinstance(items, list) or any(
                    not isinstance(i, Mapping) or not isinstance(i.get("Name"), str)
                    or not i["Name"] for i in items)):
                raise QuarterEmbyError("emby_provider_inventory_incomplete")
        return available

    def creation_blockers(self):
        try:
            info = self._request("GET", "/System/Info/Public")
            if not isinstance(info, Mapping) or info.get("Version") != "4.9.5.0":
                return ["emby_version_unverified"]
            users = self._request("GET", "/Users/Query", params={"Limit": 1})
            if not isinstance(users, Mapping) or not isinstance(users.get("Items"), list):
                return ["emby_admin_read_unverified"]
            self._available()
        except QuarterEmbyError as exc:
            return [str(exc)]
        return []

    def verify_empty_directory(self, spec):
        """Report the GET-only verification boundary without guessing safety.

        Installed 4.9.5.0 apiclient.getDirectoryContents uses POST starting
        with 4.8.0.53. The primary REST reference also documents only POST:
        https://dev.emby.media/reference/RestAPI/EnvironmentService/postEnvironmentDirectorycontents.html
        No documented GET contract here distinguishes missing from empty.
        Runtime must prove the container mount/path correspondence and empty
        host directory by a separately authorized read-only mechanism. An
        empty HTTP response, host-only existence, or a legacy GET is no proof.
        """
        try:
            _spec(spec)
        except QuarterEmbyError as exc:
            return {"verified": False, "reason": str(exc)}
        return {"verified": False, "reason": "emby_directory_get_unverified"}

    def _options(self, path, available):
        options = {field: False for field in (
            "EnableRealtimeMonitor", "EnableMarkerDetection",
            "EnableMarkerDetectionDuringLibraryScan", "EnableChapterImageExtraction",
            "ExtractChapterImagesDuringLibraryScan", "DownloadImagesInAdvance",
            "SaveLocalMetadata", "SaveLocalThumbnailSets", "AutoGenerateChapters",
            "ImportCollections", "ImportPlaylists")}
        options.update(ContentType="tvshows", PathInfos=[{"Path": path}],
                       AutomaticRefreshIntervalDays=0, PlaceholderMetadataRefreshIntervalDays=0,
                       MetadataSavers=[], SubtitleDownloadLanguages=[], LyricsDownloadLanguages=[],
                       TypeOptions=[{"Type": row["Type"], "MetadataFetchers": [],
                                     "ImageFetchers": []} for row in available["TypeOptions"]])
        for source, target in (("MetadataReaders", "DisabledLocalMetadataReaders"),
                               ("SubtitleFetchers", "DisabledSubtitleFetchers"),
                               ("LyricsFetchers", "DisabledLyricsFetchers")):
            options[target] = [i["Name"] for i in available[source]]
        return options

    def create_library(self, spec):
        """Runtime must check resource grant/lock first. Never retry uncertain POSTs."""
        name, path = _spec(spec)
        state = self.inventory(spec)
        if state["state"] == "reuse":
            return state
        if state["state"] != "create":
            raise QuarterEmbyError(state["reason"])
        blockers = self.creation_blockers()
        if blockers:
            raise QuarterEmbyError(blockers[0])
        expected = self._options(path, self._available())
        self._request("POST", "/Library/VirtualFolders", params={
            "name": name, "collectionType": "tvshows", "refreshLibrary": "false"},
            body={"LibraryOptions": expected})
        rows = self._libraries()
        matching = [r for r in rows if r["Name"] == name or any(
            _overlap(p.rstrip("/"), path) for p in r["Locations"])]
        if (len(matching) != 1 or matching[0]["Name"] != name
                or matching[0]["Locations"] != [path]
                or matching[0].get("CollectionType") != "tvshows"):
            raise QuarterEmbyError("emby_creation_readback_failed")
        actual = matching[0].get("LibraryOptions") if len(matching) == 1 else None
        if _option_state(actual, expected) != "reuse":
            raise QuarterEmbyError("emby_creation_options_unverified")
        return {"state": "reuse", "reason": "emby_creation_verified"}
