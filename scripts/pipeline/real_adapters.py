"""Service adapter implementations for the configured external ports.

Contract notes live in ``references/moviepilot.md``, ``references/p115.md``,
``references/litepan.md``, ``references/emby.md``, and
``references/deployment-local.md``. Credential handling remains local-only:
- Secret files may contain DPAPI or explicitly configured plaintext material;
  values stay in memory and are never logged.
- MoviePilot authentication mode is an explicit configuration choice.
- Cloud submission and inventory require the configured client port and
  complete evidence before advancing pipeline state.
"""
from __future__ import annotations

import ctypes
import ctypes.wintypes
import gzip
import http.cookiejar
import json
import logging
import os
import posixpath
import re
import threading
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any, Dict, Iterable, List, Mapping, Optional

from .adapters import (Cloud115Adapter, EmbyAdapter, LitePanAdapter,
                       EmbyInventoryError, MoviePilotAdapter, OfflineTask,
                       RemoteDirectoryMissing, RemoteInventoryIncomplete, RemotePathConflict,
                       RemoteRateLimited, ScrapeResult,
                       _library_location_values,
                       _validate_emby_item_contract, find_episode_match)
from .diagnostics import request_timeout, stage
from .http import HttpError, JsonHttpClient, RetryPolicy
from .parsers import FeedFormatError, SourceItem, normalize_infohash, parse_feed
from .security import sanitize_text
from .services import _as_task_list

log = logging.getLogger("pipeline.real")

VIDEO_EXT = {".mkv", ".mp4", ".ts", ".iso", ".m2ts", ".avi", ".mov", ".webm"}
P115_API_COMPENSATION_SECONDS = 0.2


# ---------------- 凭据读取 ----------------
def read_secret_file(path: str) -> str:
    """读取受限凭据文件，返回明文字符串。支持 DPAPI(hex 文本) 与纯文本。

    绝不打印返回值；调用方只在内存中拼接请求。
    """
    with open(path, "rb") as f:
        raw_bytes = f.read()
    text = raw_bytes.decode("utf-8", "replace").strip()
    hex_like = re.fullmatch(r"[0-9a-fA-F\s]+", text)
    if hex_like and len(text) > 80:
        blob = bytes.fromhex(re.sub(r"\s", "", text))
        # DPAPI blob 魔数（8 字节）：01 00 00 00 d0 8c 9d df
        if blob[:8] == bytes.fromhex("01000000d08c9ddf"):
            plain = _dpapi_unprotect(blob)
        else:
            plain = blob
        if b"\x00" in plain:
            return plain.decode("utf-16").strip()
        return plain.decode("utf-8", "strict").strip()
    return text


def _dpapi_unprotect(blob: bytes) -> bytes:
    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", ctypes.wintypes.DWORD),
                    ("pbData", ctypes.POINTER(ctypes.c_byte))]

    buf = ctypes.create_string_buffer(bytes(blob), len(blob))
    bin_ = DATA_BLOB(len(blob), ctypes.cast(buf, ctypes.POINTER(ctypes.c_byte)))
    bout = DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptUnprotectData(
        ctypes.byref(bin_), None, None, None, None, 0, ctypes.byref(bout))
    if not ok:
        raise ctypes.WinError()
    out = ctypes.string_at(bout.pbData, bout.cbData)
    ctypes.windll.kernel32.LocalFree(bout.pbData)
    return out


def dpapi_protect(plaintext: bytes, description: str = "") -> bytes:
    """DPAPI 加密（仅 Windows）。用于改配置前的本地快照，与现有 .dpapi 文件同格式。"""
    class DATA_BLOB(ctypes.Structure):
        _fields_ = [("cbData", ctypes.wintypes.DWORD),
                    ("pbData", ctypes.POINTER(ctypes.c_byte))]

    buf = ctypes.create_string_buffer(bytes(plaintext), len(plaintext))
    bin_ = DATA_BLOB(len(plaintext), ctypes.cast(buf, ctypes.POINTER(ctypes.c_byte)))
    bout = DATA_BLOB()
    ok = ctypes.windll.crypt32.CryptProtectData(
        ctypes.byref(bin_), description or None, None, None, None, 0, ctypes.byref(bout))
    if not ok:
        raise ctypes.WinError()
    out = ctypes.string_at(bout.pbData, bout.cbData)
    ctypes.windll.kernel32.LocalFree(bout.pbData)
    return out


def _http_json(method: str, url: str, body=None, headers=None,
               timeout: int = 30, params=None,
               retry_policy: Optional[RetryPolicy] = None,
               secrets: Optional[Iterable[Any]] = None) -> tuple[int, Any]:
    """Call the shared bounded transport while retaining the legacy tuple API.

    Existing adapters intentionally continue to inspect ``(status, payload)``;
    only transport/retry/query handling moves into :class:`JsonHttpClient`.
    ``params`` is merged with any query already present in ``url`` by the
    client, so credentials are never assembled into diagnostic strings here.
    """
    hdrs = {"User-Agent": "anime-cloud-pipeline/2.0"}
    hdrs.update(headers or {})
    if body is not None:
        hdrs.setdefault("Content-Type", "application/json")
    client = JsonHttpClient(
        retry_policy or RetryPolicy(),
        secrets=tuple(secrets or ()) + tuple(
            value for key, value in (headers or {}).items()
            if str(key).casefold() in {"authorization", "cookie", "x-api-key"}
        ),
    )
    try:
        status, payload = client.request_with_status(
            method,
            url,
            params=params,
            body=body,
            headers=hdrs,
            timeout=timeout,
            raise_for_status=False,
        )
        # Legacy callers only need the status to classify a rejected request.
        # Do not pass a remote error body into their older error/diagnostic
        # paths: service responses may contain credentials or cookies.
        return status, None if status >= 400 else payload
    except HttpError:
        # Preserve the old behavior for connection failures: callers decide
        # whether the operation is recoverable, while the raised error is
        # already sanitized by the shared client.
        raise


def _runtime_section(config: Any, name: str) -> Any:
    if isinstance(config, Mapping):
        return config.get(name)
    return getattr(config, name, None)


def _runtime_value(config: Any, section: str, name: str) -> Any:
    values = _runtime_section(config, section)
    if isinstance(values, Mapping):
        return values.get(name)
    return getattr(values, name, None) if values is not None else None


def _required_runtime_value(config: Any, section: str, name: str) -> Any:
    value = _runtime_value(config, section, name)
    if value is None:
        raise ValueError(f"runtime config missing {section}.{name}")
    return value


def _resolve_http_runtime(
    runtime_config: Any,
    retry_policy: Optional[RetryPolicy],
    timeout: Optional[int],
) -> tuple[RetryPolicy, int, int | None]:
    """Resolve one explicit HTTP policy at the adapter construction boundary."""
    if runtime_config is None:
        return retry_policy or RetryPolicy(), 30 if timeout is None else int(timeout), None

    max_read_retries = _required_runtime_value(runtime_config, "limits", "max_read_retries")
    if isinstance(max_read_retries, bool) or not isinstance(max_read_retries, int) or max_read_retries <= 0:
        raise ValueError("limits.max_read_retries must be a positive integer")
    resolved_policy = retry_policy
    if resolved_policy is None:
        resolved_policy = RetryPolicy(
            max_attempts=_required_runtime_value(runtime_config, "retry", "max_attempts"),
            backoff_seconds=_required_runtime_value(runtime_config, "retry", "backoff_seconds"),
            backoff_max_seconds=_required_runtime_value(runtime_config, "retry", "backoff_max_seconds"),
            jitter_seconds=_runtime_value(runtime_config, "retry", "jitter_seconds") or 0,
            max_read_retries=max_read_retries,
        )
    elif resolved_policy.max_read_retries != max_read_retries:
        resolved_policy = RetryPolicy(
            max_attempts=resolved_policy.max_attempts,
            backoff_seconds=resolved_policy.backoff_seconds,
            backoff_max_seconds=resolved_policy.backoff_max_seconds,
            jitter_seconds=resolved_policy.jitter_seconds,
            max_read_retries=max_read_retries,
        )
    resolved_timeout = timeout
    if resolved_timeout is None:
        resolved_timeout = _required_runtime_value(runtime_config, "limits", "network_timeout_seconds")
    if isinstance(resolved_timeout, bool) or not isinstance(resolved_timeout, (int, float)) or resolved_timeout <= 0:
        raise ValueError("limits.network_timeout_seconds must be positive")
    return resolved_policy, int(resolved_timeout), max_read_retries


# ---------------- KissSub 抓取 ----------------
class _KissSubTransport:
    """Cookie-preserving raw transport for the shared bounded HTTP client."""

    def __init__(self, opener: Any):
        self.opener = opener

    def request(
        self,
        method: str,
        url: str,
        *,
        body: Any = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, bytes, Mapping[str, Any]]:
        request_body = body
        if isinstance(request_body, str):
            request_body = request_body.encode("utf-8")
        request = urllib.request.Request(
            url,
            data=request_body,
            headers=dict(headers or {}),
            method=str(method or "GET").upper(),
        )
        with self.opener.open(request, timeout=timeout) as response:
            raw = response.read()
            response_headers = dict(response.headers.items())
            if str(response_headers.get("Content-Encoding", "")).casefold() == "gzip":
                raw = gzip.decompress(raw)
            return int(response.status), raw, response_headers


class KissSubFetcher:
    """KissSub feed fetcher; visitor-test responses must parse as RSS."""

    def __init__(
        self,
        base_url: str = "https://www.kisssub.org",
        *,
        timeout: Optional[int] = None,
        retry_policy: Optional[RetryPolicy] = None,
        runtime_config: Any = None,
    ):
        self.base = base_url.rstrip("/")
        policy, resolved_timeout, _ = _resolve_http_runtime(
            runtime_config, retry_policy, timeout
        )
        self.timeout = resolved_timeout
        self.retry_policy = policy
        self._jar = http.cookiejar.CookieJar()
        self._opener = urllib.request.build_opener(
            urllib.request.HTTPCookieProcessor(self._jar))
        self._opener.addheaders = [
            ("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                           "AppleWebKit/537.36 Chrome/126 Safari/537.36"),
            ("Accept-Encoding", "gzip"),
        ]
        self._http = JsonHttpClient(
            policy,
            transport=_KissSubTransport(self._opener),
            timeout=self.timeout,
        )
        self._passed = False

    def _fetch(self, url: str, data: Optional[bytes] = None) -> str:
        headers = {"Accept-Encoding": "gzip"}
        if data is not None:
            headers["Content-Type"] = "application/x-www-form-urlencoded"
        payload = self._http.request(
            "POST" if data is not None else "GET",
            url,
            body=data,
            headers=headers,
            timeout=self.timeout,
        )
        if isinstance(payload, bytes):
            return payload.decode("utf-8", "replace")
        return str(payload or "")

    def _ensure_access(self) -> None:
        if self._passed:
            return
        first = self._fetch(self.base + "/")
        if "visitor-test" not in first and "<item>" not in first:
            # 非验证页也非 RSS——仍尝试走一次流程
            pass
        if "captcha" in first[:3000] or "visitor-test" in first:
            self._fetch(self.base + "/addon.php?r=document/view&page=visitor-test",
                        data=b"visitor_test=human")
        self._passed = True

    def fetch_feed(self, feed_url: str) -> str:
        self._ensure_access()
        xml_text = self._fetch(feed_url)
        if not xml_text.lstrip().startswith(("<?xml", "<rss")):
            raise FeedFormatError("来源返回的不是 RSS XML，可能为验证页或错误页")
        return xml_text

    def feed_for_keyword(self, keyword: str) -> str:
        """Construct the source feed URL for a keyword."""
        return f"{self.base}/rss-{urllib.parse.quote(keyword)}.xml"


class KissSubSourceProvider:
    """Read-only Discovery provider backed by :class:`KissSubFetcher`."""

    name = "kisssub"

    def __init__(self, fetcher: KissSubFetcher):
        self.fetcher = fetcher

    def discover(self, subscription: Any, window: Any = None) -> list[SourceItem]:
        keyword = str(getattr(subscription, "work", "") or "").strip()
        if not keyword:
            aliases = getattr(subscription, "aliases", ()) or ()
            keyword = next((str(alias).strip() for alias in aliases if str(alias).strip()), "")
        if not keyword:
            return []
        feed_url = self.fetcher.feed_for_keyword(keyword)
        from .discovery import source_result_in_window

        results = list(parse_feed(self.fetcher.fetch_feed(feed_url)))
        # RSS itself has no trusted media identity.  The subscription selected
        # the keyword, so carry that identity into the immutable source record
        # before the common window gate runs.
        for result in results:
            raw = dict(result.raw or {})
            raw.setdefault("media_source", getattr(subscription, "media_source", ""))
            raw.setdefault("media_id", getattr(subscription, "media_id", ""))
            result.raw = raw
        return [
            result for result in results
            if source_result_in_window(subscription, result, window)
        ]


# ---------------- MoviePilot client ----------------
class MoviePilotResponseError(RuntimeError):
    """A MoviePilot response cannot be safely interpreted by an adapter."""

    def __init__(self, endpoint: str, reason: str, *, status: int | None = None) -> None:
        self.endpoint = endpoint
        self.status = status
        self.reason = sanitize_text(reason)
        suffix = f" HTTP {status}" if status is not None else ""
        super().__init__(f"MoviePilot {endpoint}{suffix}: {self.reason}")


def _moviepilot_success_code(value: Any) -> bool:
    """Accept the success codes used by MoviePilot response envelopes."""

    if isinstance(value, bool):
        return value
    if value is None:
        return True
    text = str(value).strip().casefold()
    if text in {"0", "200", "ok", "success"}:
        return True
    return False


def _moviepilot_success_flag(value: Any) -> bool:
    """Accept only documented success values for an envelope flag."""

    if value is True:
        return True
    if isinstance(value, bool):
        return False
    if isinstance(value, (int, float)) and value == 1:
        return True
    if isinstance(value, str):
        return value.strip().casefold() in {"1", "true", "ok", "success"}
    return False


def _tmdb_genre_ids(detail_data: Any) -> list[int]:
    """Extract verified numeric TMDB genre ids from a media-detail payload.

    MoviePilot exposes ``genre_ids`` (numeric ids) and ``genres`` (objects with
    ``id``/``name``).  Only the numeric ids are returned, so downstream logic
    can gate on a stable identifier (animation is id ``16``) without trusting a
    localized name.  A payload with neither field yields an empty list, which
    callers must treat as *unknown*, never as proof.
    """

    if not isinstance(detail_data, Mapping):
        return []
    raw = detail_data.get("genre_ids")
    candidates: list[Any] = list(raw) if isinstance(raw, (list, tuple)) else []
    if not candidates:
        genres = detail_data.get("genres")
        if isinstance(genres, (list, tuple)):
            candidates = [
                genre.get("id") if isinstance(genre, Mapping) else genre
                for genre in genres
            ]
    out: list[int] = []
    for value in candidates:
        if isinstance(value, bool):
            continue
        try:
            number = int(str(value).strip())
        except (TypeError, ValueError):
            continue
        if number not in out:
            out.append(number)
    return out


class MoviePilotClient(MoviePilotAdapter):
    """MoviePilot client with an explicit configured authentication mode."""

    def __init__(self, base_url: str, token_file: Optional[str] = None,
                 token: Optional[str] = None, auth_mode: str = "x-api-key",
                 *, retry_policy: Optional[RetryPolicy] = None,
                 timeout: Optional[int] = None, runtime_config: Any = None):
        self.base = base_url.rstrip("/")
        self.token = token or (read_secret_file(token_file) if token_file else "")
        if not self.token:
            raise ValueError("MoviePilot API token 未配置")
        self.auth_mode = auth_mode.lower()
        self.retry_policy, self.timeout, self.max_read_retries = _resolve_http_runtime(
            runtime_config, retry_policy, timeout
        )
        services = _runtime_section(runtime_config, "services")
        moviepilot_config = _runtime_section(services, "moviepilot")
        self.subscription_sources = tuple(
            _runtime_section(moviepilot_config, "subscription_sources") or ()
        )

    def _call(self, method: str, path: str, params=None, body=None, timeout=None):
        q = dict(params or {})
        headers = {}
        if self.auth_mode in ("query", "token"):
            q["token"] = self.token
        else:
            headers["X-API-KEY"] = self.token
        # Let the shared client merge and encode query parameters.  Keeping
        # the credential in ``params`` also ensures errors never echo a
        # hand-built secret-bearing URL.
        if path == "/api/v1/subscribe/":
            diagnostic_label = "moviepilot.subscriptions"
        elif path.startswith("/api/v1/media/"):
            diagnostic_label = "moviepilot.media_detail"
        else:
            diagnostic_label = "moviepilot.request"
        with stage(diagnostic_label):
            return _http_json(
                method,
                f"{self.base}{path}",
                body=body,
                headers=headers,
                timeout=self.timeout if timeout is None else timeout,
                params=q,
                retry_policy=self.retry_policy,
            )

    def list_subscriptions(self) -> List[Dict[str, Any]]:
        """Read the MoviePilot subscription snapshot.

        The endpoint has two supported response forms in deployed versions:
        a bare JSON list and a ``{"data": [...]}`` envelope.  Any other shape
        is an adapter error; returning an empty list would erase the operator's
        view of the subscription source on the next run.
        """

        endpoint = "/api/v1/subscribe/"
        try:
            status, payload = self._call("GET", endpoint)
        except Exception as exc:
            # Do not retain the exception text: urllib errors can include the
            # query URL, which contains the token in legacy query auth mode.
            raise MoviePilotResponseError(
                endpoint, f"request failed ({type(exc).__name__})"
            ) from None
        if status != 200:
            raise MoviePilotResponseError(endpoint, "request rejected", status=status)

        if isinstance(payload, dict):
            if "success" in payload and not _moviepilot_success_flag(payload["success"]):
                raise MoviePilotResponseError(endpoint, "response envelope reports failure")
            if "code" in payload and not _moviepilot_success_code(payload.get("code")):
                raise MoviePilotResponseError(
                    endpoint,
                    "response envelope reports a non-success code",
                )

        if isinstance(payload, list):
            entries = payload
        elif (
            isinstance(payload, dict)
            and "data" in payload
            and isinstance(payload.get("data"), list)
            and set(payload).issubset(
                {"data", "code", "message", "success", "total", "page", "page_size"}
            )
        ):
            entries = payload["data"]
        else:
            shape = type(payload).__name__
            if isinstance(payload, dict):
                shape = "object"
            raise MoviePilotResponseError(
                endpoint, f"unexpected response shape ({shape}); expected list or data-list wrapper"
            )

        if not all(isinstance(entry, dict) for entry in entries):
            raise MoviePilotResponseError(
                endpoint, "unexpected response shape (subscription entries must be objects)"
            )
        # Subscription rows on MoviePilot v3 often omit the season date. The
        # media detail endpoint carries season_info with an explicit season
        # number, which is the routing evidence required by PathMapper.  The
        # same response carries the verified TMDB genre ids the anime gate
        # needs, so both are cached from one read.
        enriched: list[dict[str, Any]] = []
        season_cache: dict[str, tuple[list[dict[str, Any]], list[int]]] = {}
        if self.subscription_sources:
            from .discovery import _subscription_source_username
            from .subscriptions import exclusion_reason
        for entry in entries:
            item = dict(entry)
            source = str(item.get("media_source") or "").casefold()
            media_id = item.get("media_id") or item.get("tmdbid")
            may_enrich = not self.subscription_sources or (
                _subscription_source_username(item) in self.subscription_sources
                and exclusion_reason(item) is None
            )
            if (
                may_enrich
                and source in {"themoviedb", "tmdb"}
                and media_id not in (None, "")
            ):
                key = str(media_id)
                if key not in season_cache:
                    detail_endpoint = f"/api/v1/media/{urllib.parse.quote(key, safe='')}"
                    try:
                        detail_status, detail_payload = self._call(
                            "GET", detail_endpoint,
                            params={"media_source": "themoviedb", "type_name": "电视剧"},
                        )
                    except Exception as exc:
                        raise MoviePilotResponseError(
                            detail_endpoint, f"request failed ({type(exc).__name__})"
                        ) from None
                    if detail_status != 200 or not isinstance(detail_payload, Mapping):
                        raise MoviePilotResponseError(detail_endpoint, "media detail unavailable")
                    detail_data = detail_payload.get("data")
                    if detail_payload.get("success") is False or not isinstance(detail_data, Mapping):
                        raise MoviePilotResponseError(detail_endpoint, "media detail invalid")
                    season_info = detail_data.get("season_info")
                    if not isinstance(season_info, list):
                        raise MoviePilotResponseError(detail_endpoint, "season inventory unavailable")
                    seasons = [
                        {"season_number": season.get("season_number"),
                         "air_date": season.get("air_date")}
                        for season in season_info if isinstance(season, Mapping)
                    ]
                    season_cache[key] = (seasons, _tmdb_genre_ids(detail_data))
                seasons, genre_ids = season_cache[key]
                item["tmdb_seasons"] = seasons
                item["tmdb_genre_ids"] = genre_ids
            enriched.append(item)
        return enriched

    def version(self) -> str:
        st, data = self._call("GET", "/api/v1/system/env")
        if st != 200:
            raise RuntimeError(f"system/env HTTP {st}")
        return (data.get("data") or {}).get("VERSION", "")

    def storage_list(self, storage: str, path: str) -> List[Dict[str, Any]]:
        st, data = self._call("POST", "/api/v1/storage/list",
                              body={"storage": storage, "path": path})
        if st != 200:
            raise RuntimeError(f"storage/list HTTP {st}")
        return data if isinstance(data, list) else []

    def read_file(self, storage: str, path: str) -> bytes:
        name = path.rsplit("/", 1)[-1]
        st, data = self._call("POST", "/api/v1/storage/download",
                              body={"storage": storage, "path": path,
                                    "type": "file", "name": name})
        if st != 200:
            raise RuntimeError(f"storage/download HTTP {st}")
        if isinstance(data, bytes):
            return data
        return data.get("_head", "").encode("utf-8", "replace")

    def scrape_in_place(self, strm_path: str, media_source: str, media_id: str,
                        type_name: str = "电视剧") -> ScrapeResult:
        storage = "local"
        season_directory = os.path.dirname(strm_path)
        # MoviePilot's file scrape creates episode sidecars only. Its TV
        # directory scrape initializes show/season sidecars and visits direct
        # episode files. Published STRMs use that standard Season N layout.
        if re.fullmatch(r"Season [0-9]+", os.path.basename(season_directory)):
            scrape_path = os.path.dirname(season_directory)
            scrape_type = "dir"
        else:
            scrape_path = strm_path
            scrape_type = "file"
        st, data = self._call("POST", f"/api/v1/media/scrape/{storage}",
                              params={"media_source": media_source,
                                      "media_id": str(media_id),
                                      "type_name": type_name},
                              body={"storage": storage, "path": scrape_path,
                                    "name": os.path.basename(scrape_path),
                                    "type": scrape_type})
        nfo_path = os.path.splitext(strm_path)[0] + ".nfo"
        accepted = st == 200 and not (
            isinstance(data, Mapping) and data.get("success") is False
        )
        return ScrapeResult(ok=accepted, nfo_path=nfo_path,
                            detail={"http_status": st,
                                    "response": "HTTP request rejected" if st >= 400 else data})

    def emby_libraries(self, server_name: str) -> List[Dict[str, Any]]:
        st, data = self._call("GET", "/api/v1/mediaserver/library",
                              params={"server": server_name})
        if st != 200:
            raise RuntimeError(f"mediaserver/library HTTP {st}")
        return data.get("data") if isinstance(data, dict) else data

    def emby_exists_remote(self, media_info: Dict[str, Any]) -> Dict[str, Any]:
        st, data = self._call("POST", "/api/v1/mediaserver/exists_remote",
                              body=media_info)
        if st != 200:
            raise RuntimeError(f"mediaserver/exists_remote HTTP {st}")
        return data

    # ---- Plugin configuration; see references/cleanup.md ----
    def plugin_conf(self, plugin_id: str) -> Dict[str, Any]:
        """读插件完整配置。

        注意：返回扁平 dict 且敏感字段（cookies 等）是明文未掩码——只能在内存中
        使用，绝不打印、不落明文。
        """
        st, data = self._call("GET", f"/api/v1/plugin/{plugin_id}")
        if st != 200:
            raise RuntimeError(f"GET plugin/{plugin_id} HTTP {st}")
        return data

    def update_plugin_conf(self, plugin_id: str, conf: Dict[str, Any]) -> None:
        """全量替换插件配置并触发插件按新配置重初始化。

        PUT 是全量语义：必须先 GET 完整配置、内存中改字段、再整包 PUT。
        保存会重置插件运行时缓存（如 115 离线任务缓存计数），属正常现象。
        """
        st, data = self._call("PUT", f"/api/v1/plugin/{plugin_id}", body=conf)
        if st != 200:
            raise RuntimeError(f"PUT plugin/{plugin_id} HTTP {st}")

    def plugin_state(self, plugin_id: str):
        """从插件列表读取安装状态（installed/state/版本）。"""
        st, data = self._call("GET", "/api/v1/plugin/")
        if st != 200:
            raise RuntimeError(f"GET plugin/ HTTP {st}")
        for p in data:
            if p.get("id") == plugin_id:
                return p
        return None


# ---------------- LitePan ----------------
class LitePanClient(LitePanAdapter):
    """LitePan 事件入口（/api/open/automation/events，需普通任务 API Key）。

    完成判定不能只看 200：调用方必须用文件证据（expected_strm_exists）确认。
    """

    def __init__(self, base_url: str, api_key_file: Optional[str] = None,
                 api_key: Optional[str] = None, *,
                 task_db_path: Optional[str] = None,
                 strm_host_root: Optional[str] = None,
                 retry_policy: Optional[RetryPolicy] = None,
                 timeout: Optional[int] = None, runtime_config: Any = None):
        self.base = base_url.rstrip("/")
        self.api_key = api_key or (read_secret_file(api_key_file)
                                   if api_key_file else "")
        self.task_db_path = task_db_path
        self.strm_host_root = strm_host_root
        self.retry_policy, self.timeout, self.max_read_retries = _resolve_http_runtime(
            runtime_config, retry_policy, timeout
        )

    def health(self) -> bool:
        st, data = _http_json("GET", f"{self.base}/api/health",
                              timeout=self.timeout, retry_policy=self.retry_policy)
        return st == 200 and (data or {}).get("data", {}).get("status") == "ok"

    def list_tasks(self) -> List[Dict[str, Any]]:
        """Read the installed Go LitePan task table without changing its state.

        v0.5.x exposes only the automation webhook under ``/api/open``; its
        task-list endpoint is admin-session-only.  The pipeline runs on the
        same NAS and uses a read-only SQLite connection for identity proof.
        """

        if not self.task_db_path or not self.strm_host_root:
            raise RuntimeError("LitePan task DB and STRM host root are required")
        import sqlite3
        from pathlib import Path
        from urllib.parse import quote

        db_path = Path(self.task_db_path)
        if not db_path.is_absolute() or not db_path.is_file():
            raise RuntimeError("LitePan task DB is unavailable")
        root = Path(self.strm_host_root)
        if not root.is_absolute() or not root.is_dir():
            raise RuntimeError("LitePan STRM host root is unavailable")
        uri = f"file:{quote(str(db_path), safe='/')}?mode=ro"
        try:
            with sqlite3.connect(uri, uri=True, timeout=5) as db:
                db.execute("PRAGMA query_only=ON")
                rows = db.execute(
                    "SELECT id,name,path,output_folder,group_dir,status,recursive "
                    "FROM strm_tasks ORDER BY id"
                ).fetchall()
        except sqlite3.Error as exc:
            raise RuntimeError("LitePan task DB could not be read") from exc
        tasks: List[Dict[str, Any]] = []
        for task_id, name, source, folder, group, status, recursive in rows:
            parts = (str(group or ""), str(folder or ""))
            if any(part in {".", ".."} or "/" in part or "\\" in part for part in parts if part):
                raise RuntimeError("LitePan task output has unsafe segments")
            output = root.joinpath(*(part for part in parts if part))
            tasks.append({
                "id": task_id, "name": name, "source_root": source,
                "output_root": str(output), "status": status,
                "recursive": bool(recursive),
            })
        return _strict_litepan_task_entries(tasks)

    def list_runtime_tasks(self) -> List[Dict[str, Any]]:
        return self.list_tasks()

    def assert_event_targets_task(self, task_id: Any, event: str, source: str) -> None:
        """Reject a webhook which would launch a different STRM task."""

        import sqlite3
        from pathlib import Path
        from urllib.parse import quote

        if not self.task_db_path or not Path(self.task_db_path).is_file():
            raise RuntimeError("LitePan automation inventory is unavailable")
        uri = f"file:{quote(self.task_db_path, safe='/')}?mode=ro"
        try:
            with sqlite3.connect(uri, uri=True, timeout=5) as db:
                db.execute("PRAGMA query_only=ON")
                rows = db.execute(
                    "SELECT trigger_config,actions_json,status FROM automation_rules "
                    "WHERE trigger_type='webhook'"
                ).fetchall()
        except sqlite3.Error as exc:
            raise RuntimeError("LitePan automation rules could not be read") from exc
        matching: list[list[int]] = []
        for trigger_raw, actions_raw, status in rows:
            try:
                trigger = json.loads(trigger_raw)
                actions = json.loads(actions_raw)
            except (TypeError, ValueError) as exc:
                raise RuntimeError("LitePan automation rule is malformed") from exc
            if not isinstance(trigger, dict) or not isinstance(actions, list):
                raise RuntimeError("LitePan automation rule is malformed")
            if trigger.get("event") != event or trigger.get("source") != source:
                continue
            if str(status).lower() not in {"running", "active"}:
                continue
            ids = [
                str(action.get("params", {}).get("task_id"))
                for action in actions
                if isinstance(action, dict) and action.get("type") == "strm"
                and isinstance(action.get("params"), dict)
            ]
            matching.append(ids)
        if len(matching) != 1 or matching[0] != [str(task_id)]:
            raise RuntimeError("LitePan webhook does not target the selected STRM task")

    def trigger_event(self, event: str, source: str, message: str) -> Any:
        if not self.api_key:
            raise RuntimeError("LitePan API Key 未配置，无法触发 automation 事件")
        st, data = _http_json(
            "POST", f"{self.base}/api/open/automation/events",
            body={"event": event, "source": source, "message": message},
            headers={"Authorization": f"Bearer {self.api_key}"},
            timeout=self.timeout, retry_policy=self.retry_policy)
        log.info("litepan event %s -> HTTP %s", event, st)
        return {"status": st, "body": data}

    def list_strm(self) -> List[str]:
        raise NotImplementedError("LitePan 无凭据时用文件系统证据："
                                   "expected_strm_exists(strm_root, rel)")


def _strict_litepan_task_entries(data: Any) -> List[Dict[str, Any]]:
    """Decode one complete LitePan task page without dropping evidence."""
    return _as_task_list(data)


# ---------------- Emby ----------------
class EmbyClient(EmbyAdapter):
    """Emby 直连客户端 with complete, bounded inventory evidence."""

    _EPISODE_FIELDS = (
        "Path,ProviderIds,SeriesId,SeriesName,ParentIndexNumber,IndexNumber,Id"
    )

    def __init__(self, base_url: str, api_key_file: Optional[str] = None,
                 api_key: Optional[str] = None, *,
                 retry_policy: Optional[RetryPolicy] = None,
                 timeout: Optional[int] = None, runtime_config: Any = None,
                 http_client: Any = None, transport: Any = None,
                 page_size: int = 500, max_pages: int = 10000,
                 library_id: Optional[str] = None):
        self.base = base_url.rstrip("/")
        self.api_key = api_key or (read_secret_file(api_key_file)
                                   if api_key_file else "")
        if not self.api_key:
            raise ValueError("Emby API key 未配置")
        self.retry_policy, self.timeout, self.max_read_retries = _resolve_http_runtime(
            runtime_config, retry_policy, timeout
        )
        if isinstance(page_size, bool) or not isinstance(page_size, int) or page_size <= 0:
            raise ValueError("page_size must be a positive integer")
        if isinstance(max_pages, bool) or not isinstance(max_pages, int) or max_pages <= 0:
            raise ValueError("max_pages must be a positive integer")
        self.page_size = page_size
        self.max_pages = max_pages
        self.library_id = str(library_id) if library_id not in (None, "") else ""
        self._http_client = http_client
        if self._http_client is None and transport is not None:
            self._http_client = JsonHttpClient(
                self.retry_policy, timeout=self.timeout, secrets=(self.api_key,),
                transport=transport,
            )

    def _request(self, method: str, path: str, *, params: Optional[Mapping[str, Any]] = None) -> tuple[int, Any]:
        request_params = dict(params or {})
        request_params["api_key"] = self.api_key
        url = f"{self.base}{path}"
        client = self._http_client
        if client is None:
            return _http_json(
                method, url, params=request_params, timeout=self.timeout,
                retry_policy=self.retry_policy, secrets=(self.api_key,),
            )
        if hasattr(client, "request_with_status"):
            return client.request_with_status(
                method, url, params=request_params, timeout=self.timeout,
            )
        if hasattr(client, "request"):
            result = client.request(
                method, url, params=request_params, timeout=self.timeout,
            )
            if isinstance(result, tuple) and len(result) >= 2:
                return int(result[0]), result[1]
            return 200, result
        raise TypeError("http_client must expose request_with_status or request")

    def server_info(self) -> Dict[str, Any]:
        st, data = self._request("GET", "/emby/System/Info/Public")
        if st != 200 or not isinstance(data, Mapping):
            raise RuntimeError(f"System/Info/Public HTTP {st}")
        return dict(data)

    def refresh_library(self, library_id: str) -> Any:
        st, _ = self._request(
            "POST", f"/emby/Items/{_safe_emby_segment(library_id)}/Refresh",
            params={"Recursive": "true"},
        )
        if st not in (200, 204):
            raise RuntimeError(f"refresh HTTP {st}")
        return {"accepted": True}

    def list_libraries(self) -> list[Dict[str, Any]]:
        st, data = self._request("GET", "/emby/Library/VirtualFolders")
        if st != 200:
            raise RuntimeError(f"library inventory HTTP {st}")
        if isinstance(data, list):
            raw = data
        elif isinstance(data, Mapping):
            containers = [
                data.get("Items"), data.get("Libraries"), data.get("VirtualFolders")
            ]
            present = [value for value in containers if value is not None]
            if len(present) != 1 or not isinstance(present[0], list):
                raise EmbyInventoryError("Emby library inventory envelope is incomplete")
            raw = present[0]
        else:
            raise EmbyInventoryError("Emby library inventory shape is unsupported")
        if isinstance(data, Mapping) and "TotalRecordCount" in data:
            total = _strict_emby_total(data.get("TotalRecordCount"))
            if total != len(raw):
                raise EmbyInventoryError("Emby library inventory total is incomplete")
        libraries: list[Dict[str, Any]] = []
        for value in raw:
            if not isinstance(value, Mapping):
                raise EmbyInventoryError("Emby library inventory entry is malformed")
            library_id = value.get("Id", value.get("id"))
            name = value.get("Name", value.get("name"))
            if (library_id in (None, "") or not str(library_id).strip()
                    or not isinstance(name, str) or not name.strip()):
                raise EmbyInventoryError("Emby library inventory entry lacks id or name")
            locations = _library_location_values(value)
            normalized_locations = [_normalize_library_root(location) for location in locations]
            if any(not location for location in normalized_locations):
                raise EmbyInventoryError("Emby library inventory locations are malformed")
            # Emby legitimately allows one library to have several Locations.
            # The exact requested root is checked by find_library below.
            libraries.append(dict(value))
        return libraries

    def find_library(self, name: str, root: str) -> Optional[Dict[str, Any]]:
        expected_name = str(name or "").strip()
        expected_root = _normalize_library_root(root)
        if not expected_name or not expected_root:
            raise EmbyInventoryError("Emby library name or root is unsafe")
        matches = []
        for library in self.list_libraries():
            actual_name = str(library.get("Name", library.get("name", ""))).strip()
            locations = _library_location_values(library)
            if actual_name == expected_name and any(
                    _normalize_library_root(location) == expected_root
                    for location in locations
            ):
                matches.append(library)
        if len(matches) != 1:
            raise EmbyInventoryError("Emby library name and root are not unique")
        return matches[0]

    def iter_episodes(self, library_id: str | None):
        start = 0
        total: Optional[int] = None
        seen = 0
        seen_ids: set[str] = set()
        for page_number in range(self.max_pages):
            status, data = self._request(
                "GET", "/emby/Items",
                params={
                    **({"ParentId": str(library_id)} if library_id not in (None, "") else {}),
                    "Recursive": "true",
                    "IncludeItemTypes": "Episode", "Fields": self._EPISODE_FIELDS,
                    "StartIndex": start, "Limit": self.page_size,
                },
            )
            if status != 200:
                raise RuntimeError(f"Items 查询 HTTP {status}")
            if not isinstance(data, Mapping):
                raise EmbyInventoryError("Emby episode inventory envelope is malformed")
            page_items = data.get("Items")
            if not isinstance(page_items, list):
                raise EmbyInventoryError("Emby episode inventory Items is malformed")
            page_total = _strict_emby_total(data.get("TotalRecordCount"))
            if total is None:
                total = page_total
            elif page_total != total:
                raise EmbyInventoryError("Emby episode inventory total changed")
            response_start = data.get("StartIndex")
            if response_start not in (None, "") and _strict_emby_total(response_start) != start:
                raise EmbyInventoryError("Emby episode inventory StartIndex changed")
            if len(page_items) > self.page_size or start > total:
                raise EmbyInventoryError("Emby episode inventory page range is invalid")
            if total == 0:
                if page_items:
                    raise EmbyInventoryError("Emby episode inventory has items with total zero")
                return
            if not page_items:
                raise EmbyInventoryError("Emby episode inventory ended before total")
            if start + len(page_items) > total:
                raise EmbyInventoryError("Emby episode inventory exceeds total")
            for item in page_items:
                _validate_emby_episode_item(item)
                item_id = str(item.get("Id", item.get("id"))).strip()
                if item_id in seen_ids:
                    raise EmbyInventoryError("Emby episode inventory contains duplicate ids")
                seen_ids.add(item_id)
                yield item
            seen = start + len(page_items)
            if seen == total:
                return
            # A short page before the declared total is not proof of completion.
            if len(page_items) < self.page_size:
                raise EmbyInventoryError("Emby episode inventory page is truncated")
            start = seen
        raise EmbyInventoryError("Emby episode inventory exceeds page cap")

    def _series_provider_map(self, library_id: Any) -> Optional[Dict[str, str]]:
        """Map each series in one library to its series-level Tmdb id.

        ``find_episode`` 需要系列级 ProviderIds 才能证明候选剧集确实属于
        目标系列（剧集级 Tmdb id 是分集级的，不能代替）。查询失败必须
        fail-closed（调用方按 pending 处理），不允许静默降级为空映射；
        library_id 为空时无法查询，返回 None 表示此时不施加映射 gate。
        完成查询后得到的空映射仍表示没有可验证的系列，必须 fail-closed。
        """

        if library_id in (None, ""):
            return None
        mapping: Dict[str, str] = {}
        start = 0
        total: Optional[int] = None
        seen_ids: set[str] = set()
        for page_number in range(self.max_pages):
            status, data = self._request(
                "GET", "/emby/Items",
                params={
                    "ParentId": str(library_id),
                    "Recursive": "true",
                    "IncludeItemTypes": "Series", "Fields": "ProviderIds",
                    "StartIndex": start, "Limit": self.page_size,
                },
            )
            if status != 200:
                raise EmbyInventoryError(f"Emby series inventory HTTP {status}")
            if not isinstance(data, Mapping):
                raise EmbyInventoryError("Emby series inventory envelope is malformed")
            page_items = data.get("Items")
            if not isinstance(page_items, list):
                raise EmbyInventoryError("Emby series inventory Items is malformed")
            page_total = _strict_emby_total(data.get("TotalRecordCount"))
            if total is None:
                total = page_total
            elif page_total != total:
                raise EmbyInventoryError("Emby series inventory total changed")
            response_start = data.get("StartIndex")
            if response_start not in (None, "") and _strict_emby_total(response_start) != start:
                raise EmbyInventoryError("Emby series inventory StartIndex changed")
            if len(page_items) > self.page_size or start > total:
                raise EmbyInventoryError("Emby series inventory page range is invalid")
            if total == 0:
                if page_items:
                    raise EmbyInventoryError("Emby series inventory has items with total zero")
                return mapping
            if not page_items:
                raise EmbyInventoryError("Emby series inventory ended before total")
            if start + len(page_items) > total:
                raise EmbyInventoryError("Emby series inventory exceeds total")
            for item in page_items:
                if not isinstance(item, Mapping):
                    raise EmbyInventoryError("Emby series inventory entry is malformed")
                series_id = str(item.get("Id", item.get("id")) or "").strip()
                if not series_id:
                    raise EmbyInventoryError("Emby series inventory entry lacks an id")
                if series_id in seen_ids:
                    raise EmbyInventoryError("Emby series inventory contains duplicate ids")
                seen_ids.add(series_id)
                provider_ids = item.get("ProviderIds", item.get("provider_ids"))
                if not isinstance(provider_ids, Mapping):
                    continue
                # 与剧集侧 _provider_tmdb 相同的别名口径（Tmdb/tmdbid）。
                # 收集该系列 ProviderIds 里全部 tmdb 类值并归一比较：
                # 出现多个互不相同的值意味着系列身份不可信（脏数据/刮
                # 削错误），宁可让该系列回 pending 重试，也不能把不可
                # 信系列当作物联证据误判通过。
                tmdb_values: set[str] = set()
                for key, value in provider_ids.items():
                    if (str(key).strip().casefold() in {"tmdb", "tmdbid"}
                            and value not in (None, "")):
                        normalized = str(value).strip()
                        if normalized:
                            tmdb_values.add(normalized)
                if len(tmdb_values) != 1:
                    continue
                mapping[series_id] = next(iter(tmdb_values))
            start += len(page_items)
            if start == total:
                if not mapping:
                    raise EmbyInventoryError("Emby series inventory lacks TMDB identities")
                return mapping
            # A short page before the declared total is not proof of completion.
            if len(page_items) < self.page_size:
                raise EmbyInventoryError("Emby series inventory page is truncated")
        raise EmbyInventoryError("Emby series inventory exceeds page cap")

    def find_episode(self, *args: Any, **kwargs: Any) -> Optional[Dict[str, Any]]:
        mapper = kwargs.pop("mapper", None)
        library_id = kwargs.pop("library_id", None)
        if kwargs:
            raise TypeError(f"unexpected Emby arguments: {', '.join(kwargs)}")
        if args and (isinstance(args[0], Mapping) or any(
                hasattr(args[0], name) for name in ("media_id", "tmdb_id", "episode_key")
        )):
            identity = args[0]
            expected_path = args[1] if len(args) > 1 else ""
            identity_library = identity.get("library_id") if isinstance(identity, Mapping) else None
            resolved_library = (
                identity_library
                or getattr(args[0], "library_id", None)
                or library_id
                or self.library_id
            )
            resolved_library_id = (
                resolved_library if resolved_library not in (None, "") else None
            )
            episodes = list(self.iter_episodes(resolved_library_id))
            series_provider_ids = self._series_provider_map(resolved_library_id)
            if series_provider_ids is not None and any(
                str(item.get("SeriesId", item.get("series_id")) or "").strip()
                not in series_provider_ids for item in episodes
            ):
                raise EmbyInventoryError("Emby episode series identity is unavailable")
            return find_episode_match(
                episodes,
                identity, expected_path,
                mapper=mapper,
                series_provider_ids=series_provider_ids,
            )
        if len(args) < 4:
            raise TypeError("find_episode requires an identity/path or legacy library/work/season/episode")
        legacy_library, work, season, episode = args[:4]
        path_hint = args[4] if len(args) > 4 else ""
        for item in self.iter_episodes(str(legacy_library)):
            if (item.get("SeriesName") == work
                    and item.get("ParentIndexNumber") == season
                    and item.get("IndexNumber") == episode
                    and (not path_hint or item.get("Path") == path_hint)):
                return {"id": item.get("Id"), "name": item.get("Name"),
                        "path": item.get("Path"), **dict(item)}
        return None


def _safe_emby_segment(value: Any) -> str:
    text = str(value or "").strip()
    if not text or "/" in text or "\\" in text or any(
            ord(char) < 0x20 or ord(char) == 0x7F for char in text
    ):
        raise ValueError("Emby library id is unsafe")
    return urllib.parse.quote(text, safe="")


def _strict_emby_total(value: Any) -> int:
    if isinstance(value, bool) or value in (None, ""):
        raise EmbyInventoryError("Emby episode inventory total is missing")
    try:
        result = int(value)
    except (TypeError, ValueError) as exc:
        raise EmbyInventoryError("Emby episode inventory total is malformed") from exc
    if str(value).strip() != str(result) and not isinstance(value, int):
        raise EmbyInventoryError("Emby episode inventory total is malformed")
    if result < 0:
        raise EmbyInventoryError("Emby episode inventory total is negative")
    return result


def _validate_emby_episode_item(item: Any) -> None:
    _validate_emby_item_contract(item)


def _normalize_library_root(value: Any) -> str:
    text = str(value or "")
    if (text != text.strip() or "\\" in text or "%" in text
            or any(ord(char) < 0x20 or ord(char) == 0x7F for char in text)
            or not text.startswith("/") or text.startswith("//") or "://" in text
            or "?" in text or "#" in text):
        return ""
    if text != "/":
        text = text.rstrip("/")
    if any(part in {".", "..", ""} for part in text.split("/")[1:]):
        return ""
    normalized = posixpath.normpath(text)
    if normalized == "." or any(part in {".", "..", ""} for part in normalized.split("/")[1:]):
        return ""
    return normalized.rstrip("/") or "/"


# ---------------- P115 cloud submission ----------------
class P115CloudAdapter(Cloud115Adapter):
    """p115client cloud submission adapter; see ``references/p115.md``.

    Completion is established from the verified remote task and file
    inventory, not from a request response or an unverified deployment claim.
    """

    def __init__(
        self,
        credential_file: Optional[str],
        target_directory_id: Optional[str] = None,
        credential: Optional[str] = None,
        *,
        client: Any = None,
        runtime_config: Any = None,
        root_directory_id: Optional[str] = None,
        root_directory_path: Optional[str] = None,
        root_path: Optional[str] = None,
        source_directory_id: Optional[str] = None,
        max_task_pages: Optional[int] = None,
        page_size: int = 30,
        max_recursive_depth: Optional[int] = None,
        max_recursive_directories: int = 1000,
        max_recursive_files: Optional[int] = None,
        max_depth: Optional[int] = None,
        max_directories: Optional[int] = None,
        max_files: Optional[int] = None,
    ):
        self.credential_file = credential_file
        configured_root_id = root_directory_id or target_directory_id
        if configured_root_id in (None, ""):
            raise ValueError("115 root directory ID is required")
        self.root_directory_id = _remote_directory_id(configured_root_id)
        self.common_root_directory_id = self.root_directory_id
        # ``target_directory_id`` is the resolved download destination below
        # the common, verified path root.  Older callers supplied only one ID;
        # preserve that shape by treating it as both identities.  A new
        # runtime may provide the common root first and resolve the source
        # directory read-only before assigning this field.
        configured_source_id = source_directory_id or target_directory_id
        self.source_directory_id = (
            _remote_directory_id(configured_source_id)
            if configured_source_id not in (None, "") else None
        )
        self.target_directory_id = self.source_directory_id
        configured_root_path = root_directory_path or root_path
        if configured_root_path is None and isinstance(runtime_config, Mapping):
            p115_config = runtime_config.get("p115")
            if isinstance(p115_config, Mapping):
                configured_root_path = (
                    p115_config.get("root_directory_path")
                    or p115_config.get("root_path")
                )
        self.root_directory_path = _normalize_remote_path(
            configured_root_path,
            kind="directories",
            allow_empty=True,
        )
        self.common_root_directory_path = self.root_directory_path
        self._root_path_parts = _remote_path_parts(self.root_directory_path) if self.root_directory_path else ()
        # A recursive inventory is also an evidence boundary: a bare remote
        # directory ID is not enough to prove where a returned file lives.
        # Populate this map only from a complete, read-only path resolution.
        self._verified_directory_paths: dict[str, str] = {}
        if self.root_directory_path:
            self._verified_directory_paths[str(self.root_directory_id)] = self.root_directory_path
        config_caps = {}
        if runtime_config is not None:
            config_caps = {
                name: _required_runtime_value(runtime_config, "limits", name)
                for name in ("max_task_pages", "max_recursive_depth", "max_recursive_files")
            }
        configured_timeout = _runtime_value(
            runtime_config, "limits", "network_timeout_seconds"
        ) if runtime_config is not None else None
        if configured_timeout is None:
            configured_timeout = 30
        if (
            isinstance(configured_timeout, bool)
            or not isinstance(configured_timeout, (int, float))
            or configured_timeout <= 0
        ):
            raise ValueError("limits.network_timeout_seconds must be positive")
        if max_task_pages is None:
            max_task_pages = config_caps.get("max_task_pages")
        if max_recursive_depth is None:
            max_recursive_depth = config_caps.get("max_recursive_depth")
        if max_recursive_files is None:
            max_recursive_files = config_caps.get("max_recursive_files")
        if max_recursive_depth is None and max_depth is not None:
            max_recursive_depth = max_depth
        if max_recursive_files is None and max_files is not None:
            max_recursive_files = max_files
        if client is None:
            missing = [name for name, value in (
                ("max_task_pages", max_task_pages),
                ("max_recursive_depth", max_recursive_depth),
                ("max_recursive_files", max_recursive_files),
            ) if value is None]
            if missing:
                raise ValueError(
                    "runtime inventory caps required: " + ", ".join(missing)
                )
        # Local fakes from the pre-runtime-config adapter contract retain the
        # bounded test defaults; production clients must pass these explicitly
        # or through ConfigSchema/runtime_config above.
        max_task_pages = 20 if max_task_pages is None else max_task_pages
        max_recursive_depth = 8 if max_recursive_depth is None else max_recursive_depth
        max_recursive_files = 1000 if max_recursive_files is None else max_recursive_files
        self.max_task_pages = _positive_cap(max_task_pages, "max_task_pages")
        self.page_size = _positive_cap(page_size, "page_size")
        self.max_recursive_depth = _nonnegative_cap(max_depth if max_depth is not None else max_recursive_depth,
                                                    "max_recursive_depth")
        self.max_recursive_directories = _positive_cap(
            max_directories if max_directories is not None else max_recursive_directories,
            "max_recursive_directories",
        )
        self.max_recursive_files = _positive_cap(
            max_files if max_files is not None else max_recursive_files,
            "max_recursive_files",
        )
        if client is not None:
            self._client = client
            return
        try:
            from p115client import P115Client  # noqa: PLC0415
        except ImportError as e:
            raise RuntimeError(
                "p115client 未安装；或改用 MoviePilot CloudSubscribe 已有 115 能力"
            ) from e

        class _CompensatedP115Client(P115Client):
            """Leave an extra 200 ms between synchronous 115 API requests."""

            def __init__(self, *args: Any, **kwargs: Any) -> None:
                self._request_lock = threading.RLock()
                self._last_request_finished_at: float | None = None
                super().__init__(*args, **kwargs)

            def request(self, *args: Any, **kwargs: Any) -> Any:
                with self._request_lock:
                    if self._last_request_finished_at is not None:
                        remaining = (
                            P115_API_COMPENSATION_SECONDS
                            - (time.monotonic() - self._last_request_finished_at)
                        )
                        if remaining > 0:
                            time.sleep(remaining)
                    try:
                        timeout = kwargs.get("timeout")
                        if timeout is None:
                            timeout = configured_timeout
                        timeout = request_timeout(timeout)
                        kwargs["timeout"] = timeout
                        with stage("115.request", timeout_s=timeout):
                            return super().request(*args, **kwargs)
                    finally:
                        self._last_request_finished_at = time.monotonic()

        self._client = _CompensatedP115Client(
            read_secret_file(credential_file) if credential_file else (credential or "")
        )

    def validate_runtime_capabilities(self) -> None:
        """Verify real client primitives before opening writable pipeline state."""
        groups = (
            ("fs_files", "fs_files_aps"), ("fs_mkdir",), ("fs_move",),
            ("clouddownload_task_add_url",),
            ("clouddownload_task_list", "clouddownload_task_lists"),
        )
        missing = ["|".join(group) for group in groups
                   if not any(callable(getattr(self._client, name, None)) for name in group)]
        if missing:
            raise ValueError("services.cloud.client missing capabilities: " + ", ".join(missing))

    def supports_atomic_rename(self) -> bool:
        """Whether this client exposes a documented no-replace rename primitive."""
        return any(
            callable(getattr(self._client, name, None))
            for name in ("fs_rename_no_replace", "fs_rename_if_identity")
        )

    def supports_isolated_move(self) -> bool:
        """Organize by moving into a private, verified empty directory."""

        return True

    @staticmethod
    def _infohash_from(uri_or_hash: str) -> str:
        text = urllib.parse.unquote(str(uri_or_hash or "").strip())
        m = re.search(r"btih:([0-9a-zA-Z]{32,40})", text, flags=re.IGNORECASE)
        candidate = (m.group(1) if m else text).strip()
        normalized = normalize_infohash(candidate)
        if len(normalized) != 40:
            raise ValueError("115 infohash must normalize to 40 hex characters")
        return normalized

    def submit(self, magnet_or_torrent: str) -> str:
        # Accept the existing raw-BTIH entry point and standard magnets;
        # send only a canonical magnet, never an arbitrary download URL.
        identity = str(magnet_or_torrent or "").strip()
        try:
            if identity.lower().startswith("magnet:"):
                parsed = urllib.parse.urlsplit(identity)
                topics = urllib.parse.parse_qs(parsed.query).get("xt", [])
                if (parsed.netloc or parsed.path or parsed.fragment or len(topics) != 1
                        or not topics[0].lower().startswith("urn:btih:")):
                    raise ValueError("115_submit_input_invalid")
                identity = topics[0][len("urn:btih:"):]
            infohash = normalize_infohash(identity)
            if len(infohash) != 40:
                raise ValueError
        except (TypeError, ValueError):
            raise ValueError("115_submit_input_invalid") from None
        if self.source_directory_id in (None, ""):
            raise RemoteInventoryIncomplete(
                "115 download target directory has not been verified",
                kind="directories",
            )
        payload = {"url": "magnet:?xt=urn:btih:" + infohash,
                   "wp_path_id": self.source_directory_id}
        try:
            # Use the documented magnet entry point verified for this target.
            # An uncertain result must never trigger a second endpoint call.
            result = self._client.clouddownload_task_add_url(payload, type="ssp")
        except RemoteRateLimited:
            raise RemoteRateLimited("115_submit_rate_limited", kind="tasks") from None
        except TimeoutError:
            raise TimeoutError("115_submit_outcome_unknown") from None
        except Exception:
            raise RemoteInventoryIncomplete("115_submit_outcome_unknown", kind="tasks") from None
        try:
            info = json.loads(result) if isinstance(result, str) else result
        except (TypeError, ValueError):
            raise RemoteInventoryIncomplete("115_submit_receipt_malformed", kind="tasks") from None
        if not isinstance(info, Mapping):
            raise RemoteInventoryIncomplete("115_submit_receipt_malformed", kind="tasks")
        if _envelope_state_failed(info) and _envelope_reports_rate_limit(info):
            raise RemoteRateLimited("115_submit_rate_limited", kind="tasks")
        data = info.get("data", {})
        if not isinstance(data, Mapping):
            # The SSP client normally decrypts data into a mapping.  An
            # undecoded ciphertext or another shape is not an acceptance ack.
            raise RemoteInventoryIncomplete("115_submit_receipt_malformed", kind="tasks")
        envelopes = (info, data)
        code_keys = ("errno", "errNo", "errcode", "errCode", "code")
        for envelope in envelopes:
            bad_code = any(
                key in envelope and not (
                    type(envelope[key]) is int and envelope[key] == 0
                    or type(envelope[key]) is str and envelope[key] == "0")
                for key in code_keys
            )
            if (_envelope_state_failed(envelope) or bad_code) and _envelope_reports_rate_limit(envelope):
                raise RemoteRateLimited("115_submit_rate_limited", kind="tasks")
            if _envelope_state_failed(envelope) or bad_code:
                raise RemoteInventoryIncomplete("115_submit_receipt_rejected", kind="tasks")
        # Require a positive top-level ack, and reject contradictory or
        # non-boolean flags anywhere in the known receipt envelope.
        if (not any(info.get(key) is True for key in ("state", "success"))
                or any(envelope[key] is not True for envelope in envelopes
                       for key in ("state", "success") if key in envelope)):
            raise RemoteInventoryIncomplete("115_submit_receipt_unconfirmed", kind="tasks")
        hashes = [envelope[key] for envelope in envelopes
                  for key in ("info_hash", "infohash", "hash") if key in envelope]
        try:
            if not hashes or any(not isinstance(value, str)
                                 or normalize_infohash(value) != infohash for value in hashes):
                raise ValueError
        except (TypeError, ValueError):
            raise RemoteInventoryIncomplete("115_submit_receipt_identity_mismatch", kind="tasks") from None
        task_ids = [envelope[key] for envelope in envelopes
                    for key in ("task_id", "id") if key in envelope]
        if task_ids:
            if any(isinstance(value, bool) or not isinstance(value, (str, int))
                   or not re.fullmatch(r"[A-Za-z0-9_-]{1,128}", str(value))
                   for value in task_ids) or len({str(value) for value in task_ids}) != 1:
                raise RemoteInventoryIncomplete("115_submit_receipt_malformed", kind="tasks")
            return str(task_ids[0])
        # Some supported receipts key tasks by info_hash.  Return that key
        # only after both explicit acceptance and exact identity validation.
        return infohash

    def set_source_directory_id(self, directory_id: Any) -> str:
        """Bind a read-only verified source directory for new submissions."""

        token = _remote_directory_id(directory_id)
        self.source_directory_id = token
        self.target_directory_id = token
        return token

    def resolve_directory_path(self, path: str) -> str:
        """Resolve one directory below the common root using complete reads.

        This method deliberately never creates directories.  The account-root
        chain and every descendant listing are re-read, so callers can use the
        returned ID as a mutation target only after the complete path is
        proven unique and present.
        """

        normalized = _normalize_remote_path(path, kind="directories")
        parts = _remote_path_parts(normalized)
        root_parts = self._root_path_parts
        if root_parts and tuple(parts[:len(root_parts)]) != root_parts:
            raise RemoteInventoryIncomplete(
                "115 path is outside configured remote root", kind="directories"
            )
        if not root_parts:
            raise RemoteInventoryIncomplete(
                "115 path resolution requires configured root_directory_path",
                kind="directories",
            )

        current_id = self.root_directory_id
        current_parts = list(root_parts)
        entries = self._validated_root_entries()
        relative = list(parts[len(root_parts):])
        self._verified_directory_paths[str(current_id)] = "/" + "/".join(current_parts)
        for segment in relative:
            expected_parts = current_parts + [segment]
            match = _directory_match(entries, segment, expected_parts)
            if match is None:
                raise RemoteDirectoryMissing(
                    "115 directory path has no verified identity chain",
                    kind="directories",
                )
            current_id = _remote_directory_id(match[0])
            current_parts.append(segment)
            self._verified_directory_paths[str(current_id)] = "/" + "/".join(current_parts)
            entries = self.fs_files(current_id, require_complete=True)
        return str(current_id)

    def resolve_source_directory(self, path: str) -> str:
        """Resolve and bind ``cloud_source_root`` before any submission."""

        return self.set_source_directory_id(self.resolve_directory_path(path))

    def _task_method(self):
        for name in ("clouddownload_task_list", "clouddownload_task_lists"):
            fn = getattr(self._client, name, None)
            if callable(fn):
                return fn
        return None

    def _task_page(self, page: int) -> tuple[list, Any, int, Optional[bool]]:
        fn = self._task_method()
        if fn is None:
            raise RemoteInventoryIncomplete(
                "115 task-list API is unavailable",
                kind="tasks",
            )
        try:
            result = fn({"page": page, "page_size": self.page_size, "limit": self.page_size})
        except RemoteInventoryIncomplete:
            raise
        except Exception as exc:
            raise RemoteRateLimited("115 task-list API call failed", kind="tasks") from exc
        info = _decode_remote_response(result, kind="tasks")
        _raise_on_failed_envelope(info, kind="tasks")
        tasks, total, complete = _task_items_and_total(
            info, requested_page=page, requested_page_size=self.page_size
        )
        # Completion by short page is meaningful only against the size we
        # explicitly requested.  A remote envelope must not redefine that
        # proof with a contradictory page_size/limit field.
        return tasks, total, self.page_size, complete

    def _iter_task_records(self, max_pages: Optional[int] = None):
        cap = self.max_task_pages if max_pages is None else _positive_cap(max_pages, "max_task_pages")
        seen = 0
        expected_total: Optional[int] = None
        for page in range(1, cap + 1):
            tasks, total, page_size, complete = self._task_page(page)
            if any(not isinstance(task, dict) for task in tasks):
                raise RemoteInventoryIncomplete("115 task inventory contains invalid item", kind="tasks")
            if total is not None:
                if expected_total is not None and total != expected_total:
                    raise RemoteInventoryIncomplete("115 task inventory total changed", kind="tasks")
                expected_total = total
            projected_seen = seen + len(tasks)
            _raise_on_contradictory_total(expected_total, projected_seen, kind="tasks")
            # True means explicit end, False means explicit continuation,
            # and None allows the ordinary short-page completion contract.
            if complete is False and (not tasks or expected_total == projected_seen):
                raise RemoteInventoryIncomplete("115 task inventory continuation is contradictory", kind="tasks")
            terminal = complete is True or (complete is None and len(tasks) < page_size)
            if terminal and expected_total is not None and projected_seen < expected_total:
                raise RemoteInventoryIncomplete("115 task inventory ended before total", kind="tasks")
            for task in tasks:
                yield task
            seen = projected_seen
            if terminal or (expected_total is not None and seen == expected_total):
                return
            if page == cap:
                raise RemoteInventoryIncomplete(
                    "remote task inventory reached its safety cap",
                    kind="tasks",
                )

    def _task_list_raw(self) -> list:
        return list(self._iter_task_records(1))

    def iter_tasks(self, max_pages: Optional[int] = None):
        """Yield normalized tasks across every proven-complete page."""
        for task in self._iter_task_records(max_pages):
            infohash = str(
                task.get("info_hash") or task.get("infohash")
                or task.get("hash") or ""
            ).strip()
            try:
                normalized_hash = self._infohash_from(infohash)
            except ValueError:
                continue
            yield OfflineTask(
                infohash=normalized_hash,
                task_id=str(
                    task.get("task_id") or task.get("id")
                    or task.get("tid") or task.get("info_hash") or ""
                ),
                status=str(task.get("status") or task.get("process") or ""),
            )

    def list_tasks(self) -> List[OfflineTask]:
        return list(self.iter_tasks())

    def find_task(self, infohash: str) -> Optional[OfflineTask]:
        target = self._infohash_from(infohash)
        for task in self.iter_tasks():
            if task.infohash.casefold() == target.casefold():
                return task
        return None

    def _ls(self, directory_id: Any) -> list:
        # Recursive inventory must read every page; a first-page-only listing
        # would let callers mistake unread entries for absence.
        return self.fs_files(directory_id, require_complete=True)

    def _listing_request(
        self, payload: Any, profile: Optional[str] = None
    ) -> Mapping[str, Any]:
        """Read one 115 directory page across client API profiles.

        115 began rejecting this adapter's ``webapi /files`` profile with
        HTTP 405 while ``fs_files_aps`` kept serving the identical
        ``state``/``count``/``offset`` envelope (verified 2026-09-25).
        Without ``profile`` the listing prefers the app profile and falls
        back to the web profile; either response still has to pass the
        caller's completeness proof, so a profile serving a wrong listing
        fails closed instead of being trusted.  A pinned ``profile`` serves
        exactly one client method so a whole traversal reads every page
        from a single profile: mixing profiles cannot be detected once the
        count/offset sequences happen to agree.
        """

        body = dict(payload) if isinstance(payload, Mapping) else {"cid": str(payload)}
        body["cid"] = str(body.get("cid", ""))
        body.setdefault("limit", self.page_size)
        body.setdefault("offset", 0)
        body.setdefault("aid", 1)
        body.setdefault("show_dir", 1)
        if profile is None:
            profiles = (
                ("fs_files_aps", getattr(self._client, "fs_files_aps", None)),
                ("fs_files", getattr(self._client, "fs_files", None)),
            )
        else:
            profiles = ((profile, getattr(self._client, profile, None)),)
        failures: list[str] = []
        # 缺失画像只是客户端能力不全，不属于失败证据：先筛出实际可调用的
        # 画像，缺失者不参与失败分类。单画像配置下唯一画像超时也因此仍按
        # 全传输级失败聚合为限流退避，而不是被误降级为清单矛盾。
        available = [(name, fn) for name, fn in profiles if callable(fn)]
        if not available:
            raise RemoteInventoryIncomplete(
                "115 listing capability unavailable", kind="files"
            )
        transport_only = True
        transport_failure: BaseException | None = None
        for name, fn in available:
            try:
                info = _decode_remote_response(fn(dict(body)), kind="files")
            except RemoteRateLimited:
                # 决定性的限流信封：直接向上抛，不当作画像矛盾被聚合吞掉。
                raise
            except RemoteInventoryIncomplete as exc:
                failures.append(f"{name}: {exc}")
                transport_only = False
                continue
            except Exception as exc:
                failures.append(f"{name}: {type(exc).__name__} {exc}")
                transport_failure = transport_failure or exc
                continue
            # aps 信封会回显请求的 cid；回显不一致说明该画像服务的是另一个
            # 目录的清单，必须按错误信封 fail-closed。web 信封无此字段，
            # 无法校验时跳过，不要求服务端必须回显。
            envelope_cid = info.get("cid") if isinstance(info, Mapping) else None
            if envelope_cid not in (None, "") and str(envelope_cid) != body["cid"]:
                raise RemoteInventoryIncomplete(
                    "115 file-list envelope echoes a different directory cid",
                    kind="files",
                )
            return info
        if transport_only and transport_failure is not None:
            # 全部失败都是传输级（HTTP 错误/超时/断连），没有任何清单矛盾：
            # 这是限流或服务不可用信号，调用方应退避重试而非消耗审查预算。
            raise RemoteRateLimited(
                "115 file-list API call failed (" + "; ".join(failures) + ")",
                kind="files",
            ) from transport_failure
        raise RemoteInventoryIncomplete(
            "115 file-list API call failed (" + "; ".join(failures) + ")",
            kind="files",
        )

    def fs_files(self, directory_id: Any, *, require_complete: bool = False) -> list:
        """Call the real 115 directory-ID listing API fail-closed.

        ``require_complete=True`` binds one whole traversal to a single
        client profile (aps first, then web): a mid-traversal failure
        discards every page already read under that profile and restarts
        the complete walk on the next one, because pages mixed across
        profiles cannot be detected once their count/offset sequences
        agree.  Only when both profiles fail does the traversal raise
        ``RemoteInventoryIncomplete``.
        """

        directory_token = _remote_directory_id(directory_id)
        if not require_complete:
            info = self._listing_request(directory_token)
            return _inventory_list_from_envelope(
                info,
                keys=("data", "list", "items", "files", "rows"),
                kind="files",
            )
        failures: list[str] = []
        all_rate_limited = True
        # 与 _listing_request 相同：缺失画像不参与失败分类，只遍历实际可
        # 调用的画像；能力缺失按清单不可证明 fail-closed。
        callable_profiles = tuple(
            name
            for name in ("fs_files_aps", "fs_files")
            if callable(getattr(self._client, name, None))
        )
        if not callable_profiles:
            raise RemoteInventoryIncomplete(
                "115 listing capability unavailable", kind="directories"
            )
        for profile in callable_profiles:
            def request(payload: Any) -> Mapping[str, Any]:
                return self._listing_request(payload, profile=profile)
            try:
                first = request(
                    {"cid": directory_token, "offset": 0, "limit": self.page_size}
                )
                entries = _complete_directory_listing_pages(
                    request, directory_token, first, self.page_size, self.max_task_pages
                )
                _raise_on_missing_or_duplicate_listing_identities(entries)
            except RemoteRateLimited as exc:
                failures.append(f"{profile}: {exc}")
                continue
            except RemoteInventoryIncomplete as exc:
                failures.append(f"{profile}: {exc}")
                all_rate_limited = False
                continue
            return entries
        if all_rate_limited:
            # 两个画像都只在传输层失败：限流/不可用信号，与清单矛盾分开。
            raise RemoteRateLimited(
                "115 complete directory listing failed on every client profile ("
                + "; ".join(failures) + ")",
                kind="directories",
            )
        raise RemoteInventoryIncomplete(
            "115 complete directory listing failed on every client profile ("
            + "; ".join(failures) + ")",
            kind="directories",
        )

    def get_file(self, path: str) -> Optional[Dict[str, Any]]:
        """Read one exact file by walking directory IDs from the configured root.

        ``fs_files`` accepts only a directory ID.  In particular, a canonical
        path is never passed to the backend as though it were an ID and a
        direct path lookup method is deliberately not used: the latter cannot
        prove which directory was traversed when names collide.
        """

        normalized_path, components = self._relative_path(path, kind="files")
        if not components:
            raise RemoteInventoryIncomplete(
                "115 file lookup path has no file component", kind="files"
            )
        current_id = self.root_directory_id
        current_parts = list(self._root_path_parts)
        root_entries = self._validated_root_entries()
        for index, segment in enumerate(components[:-1]):
            entries = root_entries if index == 0 else self.fs_files(current_id, require_complete=True)
            match = _directory_match(entries, segment, current_parts + [segment])
            if match is None:
                return None
            current_id = match[0]
            current_parts.append(segment)

        entries = (
            root_entries
            if not components[:-1]
            else self.fs_files(current_id, require_complete=True)
        )
        basename = components[-1]
        matches = []
        for item in entries:
            if not isinstance(item, Mapping) or _item_name(item) != basename:
                continue
            if _item_is_directory(item):
                continue
            file_id = _strict_file_id(item)
            if file_id in (None, ""):
                raise RemoteInventoryIncomplete(
                    "115 file entry has no explicit file identity", kind="files"
                )
            item_path = _first_value(item, ("path", "full_path", "remote_path"))
            if item_path not in (None, "") and _normalize_remote_path(
                item_path, kind="files"
            ) != normalized_path:
                raise RemoteInventoryIncomplete(
                    "115 file entry path does not match requested path",
                    kind="files",
                )
            record = _normalize_file_record(item)
            record["path"] = normalized_path
            record.setdefault("parent_id", str(current_id))
            matches.append(record)
        if len(matches) > 1:
            raise RemoteInventoryIncomplete(
                "115 file lookup returned ambiguous basename", kind="files"
            )
        return matches[0] if matches else None

    def find_file_by_identity(
        self,
        identity: Mapping[str, Any],
        parent_path: str,
        *,
        directory_id: Any = None,
    ) -> Optional[Dict[str, Any]]:
        """Find a moved file in one verified parent directory by stable ID."""

        resolved_id = self.resolve_directory_path(parent_path)
        if directory_id is not None and str(resolved_id) != str(directory_id):
            raise RemoteInventoryIncomplete(
                "115 directory identity changed during organize", kind="directories"
            )
        directory_id = resolved_id
        normalized_parent = _normalize_remote_path(parent_path, kind="files")
        expected_id = identity.get("remote_file_id") or identity.get("file_id")
        expected_fingerprint = identity.get("fingerprint") or identity.get("etag")
        expected_size = identity.get("size")
        matches: list[Dict[str, Any]] = []
        for item in self.fs_files(directory_id, require_complete=True):
            if not isinstance(item, Mapping) or _item_is_directory(item):
                continue
            file_id = _strict_file_id(item)
            if expected_id not in (None, "") and str(file_id) != str(expected_id):
                continue
            fingerprint = _first_value(item, ("fingerprint", "etag", "sha1", "sha"))
            if (
                expected_fingerprint not in (None, "")
                and fingerprint not in (None, "")
                and str(fingerprint) != str(expected_fingerprint)
            ):
                continue
            size = _first_value(item, ("size", "s"))
            if expected_size not in (None, "") and size not in (None, "") and str(size) != str(expected_size):
                continue
            record = _normalize_file_record(item)
            record["path"] = posixpath.join(normalized_parent, _item_name(item))
            record.setdefault("parent_id", str(directory_id))
            matches.append(record)
        if len(matches) > 1:
            raise RemoteInventoryIncomplete(
                "115 identity lookup returned ambiguous files", kind="files"
            )
        return matches[0] if matches else None

    def list_directory_entries(self, directory_id: Any) -> list:
        """Read a complete directory inventory by its durable remote identity."""

        return self.fs_files(_remote_directory_id(directory_id), require_complete=True)

    def fs_mkdir(self, payload: Any, parent_id: Any = None) -> Any:
        """Call the real ``fs_mkdir`` wrapper and reject failed envelopes."""

        fn = getattr(self._client, "fs_mkdir", None)
        if not callable(fn):
            raise RemoteInventoryIncomplete(
                "115 directory-create API is unavailable", kind="directories"
            )
        parent_token = None if parent_id is None else _remote_directory_id(parent_id)
        try:
            raw = fn(payload) if parent_token is None else fn(payload, pid=parent_token)
        except TypeError:
            try:
                raw = fn(payload, parent_token) if parent_token is not None else fn(payload)
            except Exception as exc:
                raise RemoteInventoryIncomplete(
                    "115 directory-create API call failed", kind="directories"
                ) from exc
        except RemoteInventoryIncomplete:
            raise
        except Exception as exc:
            raise RemoteInventoryIncomplete(
                "115 directory-create API call failed", kind="directories"
            ) from exc
        info = _decode_remote_response(raw, kind="directories")
        if isinstance(info, Mapping):
            _raise_on_failed_envelope_deep(info, kind="directories")
        return info

    def mkdir(self, path: str) -> Dict[str, Any]:
        """Create/reconcile every path component using parent directory IDs.

        A create response is only a proposal.  The parent listing is read
        again and the returned child ID is then listed once more before it is
        accepted.  This prevents a response field such as ``path`` from being
        mistaken for a directory identity and closes create/list races.
        """

        normalized_path, components = self._relative_path(path, kind="directories")
        if not components:
            raise RemoteInventoryIncomplete(
                "115 directory path has no directory component", kind="directories"
            )
        current_id = self.root_directory_id
        current_parts = list(self._root_path_parts)
        entries = self._validated_root_entries()
        for index, segment in enumerate(components):
            expected_parts = current_parts + [segment]
            if index:
                entries = self.fs_files(current_id, require_complete=True)
            match = _directory_match(entries, segment, expected_parts)
            if match is not None:
                current_id = match[0]
                current_parts.append(segment)
                # Re-read the selected ID.  The ID, not a returned path, is
                # the only value allowed to become the next parent token.
                self.fs_files(current_id, require_complete=True)
                continue

            # P115Client.fs_mkdir is the legacy /files/add API. Its folder
            # name field is cname; name belongs to a different Open API.
            info = self.fs_mkdir({"cname": segment}, current_id)
            created_id = _directory_id_from_response(info)
            if created_id in (None, ""):
                raise RemoteInventoryIncomplete(
                    "115 directory-create response has no explicit directory identity",
                    kind="directories",
                )
            created_id = _remote_directory_id(created_id)
            response_name = _first_value_from_response(info, ("name", "n"))
            if response_name not in (None, "") and str(response_name) != segment:
                raise RemoteInventoryIncomplete(
                    "115 directory-create response name does not match request",
                    kind="directories",
                )
            response_path = _first_value_from_response(
                info, ("path", "full_path", "remote_path")
            )
            expected_path = "/" + "/".join(expected_parts)
            if response_path not in (None, "") and _normalize_remote_path(
                response_path, kind="directories"
            ) != expected_path:
                raise RemoteInventoryIncomplete(
                    "115 directory-create response path does not match request",
                    kind="directories",
                )
            after = self.fs_files(current_id, require_complete=True)
            match = _directory_match(after, segment, expected_parts)
            if match is None or str(match[0]) != str(created_id):
                raise RemoteInventoryIncomplete(
                    "115 directory-create identity could not be re-read",
                    kind="directories",
                )
            current_id = created_id
            current_parts.append(segment)
            self.fs_files(current_id, require_complete=True)

        return {
            "directory_id": str(current_id),
            "id": str(current_id),
            "path": normalized_path,
        }

    def _validated_root_entries(self) -> list:
        """Prove root identity by resolving its path from 115's account root.

        Account root ID 0 is the backend's fixed anchor. Listing the configured
        ID's children cannot prove the identity of that directory. Every path
        component must instead be found unambiguously under its proven parent;
        unavailable/incomplete listings or a different final ID reject writes.
        No cached assertion is reused across path operations.
        """

        if not self._root_path_parts:
            raise RemoteInventoryIncomplete(
                "115 path operations require configured root_directory_path",
                kind="directories",
            )
        current_id = "0"
        current_parts = []
        visited = {current_id}
        for segment in self._root_path_parts:
            current_parts.append(segment)
            match = _directory_match(
                self.fs_files(current_id, require_complete=True), segment, current_parts
            )
            if match is None or str(match[0]) in visited:
                raise RemoteInventoryIncomplete(
                    "115 root directory path has no verified identity chain",
                    kind="directories",
                )
            current_id = str(match[0])
            visited.add(current_id)
        if current_id != str(self.root_directory_id):
            raise RemoteInventoryIncomplete(
                "115 root directory ID does not match configured path",
                kind="directories",
            )
        return self.fs_files(current_id, require_complete=True)

    def get_directory(self, directory_id: str) -> Dict[str, Any]:
        """Confirm a directory ID by successfully reading its child listing."""

        token = _remote_directory_id(directory_id)
        self.fs_files(token, require_complete=True)
        return {"directory_id": str(token), "id": str(token)}

    def _relative_path(self, path: str, *, kind: str) -> tuple[str, list[str]]:
        normalized = _normalize_remote_path(path, kind=kind)
        parts = _remote_path_parts(normalized)
        if self._root_path_parts:
            root = self._root_path_parts
            if tuple(parts[:len(root)]) != root:
                raise RemoteInventoryIncomplete(
                    "115 path is outside configured remote root", kind=kind
                )
            relative = list(parts[len(root):])
        else:
            relative = []
        if not relative:
            raise RemoteInventoryIncomplete(
                "115 path does not contain a component below root directory",
                kind=kind,
            )
        return normalized, relative

    def _task_maps(self, max_pages: Optional[int] = None) -> tuple:
        """Return folder/pick-code → infohash maps from the full task ledger."""
        folder_map: dict[str, str] = {}
        pick_map: dict[str, str] = {}
        for task in self._iter_task_records(max_pages):
            infohash = str(
                task.get("info_hash") or task.get("infohash")
                or task.get("hash") or ""
            ).strip()
            try:
                infohash = self._infohash_from(infohash)
            except ValueError:
                continue
            for key in ("file_id", "fileid", "cid", "fid", "folder_id", "directory_id", "id", "task_id"):
                if task.get(key) not in (None, ""):
                    folder_map[str(task[key])] = infohash
            for key in ("pick_code", "pickcode", "pc"):
                if task.get(key) not in (None, ""):
                    pick_map[str(task[key])] = infohash
        return folder_map, pick_map

    def _task_pages(self, max_pages: int) -> list:
        return list(self._iter_task_records(max_pages))

    def list_files(
        self,
        directory_id: str,
        _depth: int = 0,
        _task_maps: Optional[tuple] = None,
        *,
        max_depth: Optional[int] = None,
        max_directories: Optional[int] = None,
        max_files: Optional[int] = None,
    ) -> List[Dict[str, Any]]:
        """Recursively inventory files with fail-closed safety caps.

        Folder/file classification uses explicit metadata and 115 identifiers;
        a dotted folder name or an extensionless file is never guessed from its
        filename.  Hitting any cap raises ``RemoteInventoryIncomplete`` so a
        caller cannot mistake a partial listing for absence.
        """
        depth_cap = self.max_recursive_depth if max_depth is None else _nonnegative_cap(max_depth, "max_depth")
        directory_cap = self.max_recursive_directories if max_directories is None else _positive_cap(max_directories, "max_directories")
        file_cap = self.max_recursive_files if max_files is None else _positive_cap(max_files, "max_files")
        maps = _task_maps if _task_maps is not None else self._task_maps()
        folder_map, pick_map = maps
        state = {"directories": 0, "files": 0, "visited": set()}
        output: list[dict[str, Any]] = []
        if self.root_directory_path:
            # Do not let the constructor's configured path act as proof.  A
            # complete account-root traversal is required before inventory
            # paths are exposed as common-root evidence.
            self._validated_root_entries()
        base_path = self._verified_directory_paths.get(str(directory_id))
        if self.root_directory_path and not base_path:
            raise RemoteInventoryIncomplete(
                "remote file inventory has no verified common-root path",
                kind="files",
            )

        def walk(current_id: Any, depth: int, prefix: str, inherited_hash: str = "") -> None:
            key = str(current_id)
            if key in state["visited"]:
                return
            state["visited"].add(key)
            state["directories"] += 1
            if state["directories"] > directory_cap:
                raise RemoteInventoryIncomplete(
                    "remote file inventory reached its directory cap",
                    kind="files",
                )
            entries = self._ls(current_id)
            for item in entries:
                if not isinstance(item, dict):
                    continue
                name = _item_name(item)
                is_directory = _item_is_directory(item)
                item_id = _item_id(item, is_directory)
                if is_directory:
                    if depth >= depth_cap or not item_id:
                        raise RemoteInventoryIncomplete(
                            "remote file inventory reached its depth cap",
                            kind="files",
                        )
                    child_hash = folder_map.get(str(item_id)) or inherited_hash
                    child_prefix = f"{prefix}/{name}" if prefix and name else (name or prefix)
                    if base_path:
                        child_path = posixpath.normpath(posixpath.join(base_path, child_prefix))
                        normalized_base = posixpath.normpath(base_path)
                        if not (
                            child_path == normalized_base
                            or child_path.startswith(normalized_base.rstrip("/") + "/")
                        ):
                            raise RemoteInventoryIncomplete(
                                "remote directory inventory escaped common root",
                                kind="files",
                            )
                    walk(item_id, depth + 1, child_prefix, child_hash)
                    continue

                state["files"] += 1
                if state["files"] > file_cap:
                    raise RemoteInventoryIncomplete(
                        "remote file inventory reached its file cap",
                        kind="files",
                    )
                if base_path and not name:
                    raise RemoteInventoryIncomplete(
                        "remote file inventory contains a file without a canonical name",
                        kind="files",
                    )
                file_id = item_id or item.get("id") or item.get("file_id")
                item_hash = (
                    folder_map.get(str(item.get("cid") or ""))
                    or folder_map.get(str(file_id or ""))
                    or pick_map.get(str(item.get("pc") or item.get("pick_code") or ""))
                    or str(item.get("infohash") or item.get("info_hash") or "").strip()
                    or inherited_hash
                    or ""
                )
                relative_name = f"{prefix}/{name}" if prefix and name else (name or prefix)
                parent_path = (
                    posixpath.join(base_path, prefix) if base_path and prefix
                    else (base_path or prefix)
                )
                full_path = posixpath.join(parent_path, name) if parent_path and name else (
                    parent_path or relative_name
                )
                if base_path:
                    normalized_parent = posixpath.normpath(parent_path)
                    normalized_candidate = posixpath.normpath(full_path)
                    if not (
                        normalized_candidate == normalized_parent
                        or normalized_candidate.startswith(normalized_parent.rstrip("/") + "/")
                    ):
                        raise RemoteInventoryIncomplete(
                            "remote file inventory escaped common root",
                            kind="files",
                        )
                    full_path = _normalize_remote_path(full_path, kind="files")
                    parent_path = _normalize_remote_path(parent_path, kind="files")
                fingerprint = _first_value(
                    item, ("fingerprint", "content_fingerprint", "sha1", "sha", "md5", "etag")
                )
                output.append({
                    "file_id": file_id,
                    "name": relative_name,
                    "filename": name,
                    "path": full_path,
                    "parent_path": parent_path,
                    "infohash": item_hash,
                    "is_dir": False,
                    "type": "file",
                    "parent_id": str(current_id),
                    "size": _first_value(item, ("size", "s")),
                    "duration": _first_value(item, ("duration", "dur", "play_long")),
                    "fingerprint": fingerprint,
                    "pick_code": _first_value(item, ("pc", "pick_code", "pickcode")),
                    "metadata": {
                        key: item[key] for key in ("sha1", "sha", "md5", "etag")
                        if key in item
                    },
                })

        walk(directory_id, _depth, "", folder_map.get(str(directory_id), ""))
        return output

    def move_file(self, file_id: str, directory_id: str) -> Any:
        fn = getattr(self._client, "fs_move", None)
        if not callable(fn):
            raise RuntimeError("115 client does not expose fs_move")
        target = _p115_id(directory_id)
        try:
            result = fn(str(file_id), pid=target)
        except TypeError:
            try:
                result = fn({"fid": str(file_id), "pid": target})
            except TypeError:
                result = fn(str(file_id), target)
        info = _decode_remote_response(result, kind="files")
        _raise_on_remote_conflict(info)
        if isinstance(info, Mapping):
            _raise_on_failed_envelope(info, kind="files")
        return info

    def rename_file_no_replace(
        self,
        file_id: str,
        name: str,
        *,
        expected_identity: Optional[Mapping[str, Any]] = None,
        target_path: Optional[str] = None,
    ) -> Any:
        """Use p115's conditional/no-replace rename shape.

        Only an explicitly named atomic/conditional primitive is accepted.
        A plain ``fs_rename`` may ignore destination flags, so it is never a
        substitute and is not called at all.  The organizer re-reads the
        target after this operation and verifies the expected identity.
        """

        fn = None
        for method_name in ("fs_rename_no_replace", "fs_rename_if_identity"):
            candidate = getattr(self._client, method_name, None)
            if callable(candidate):
                fn = candidate
                break
        if fn is None:
            raise RemoteInventoryIncomplete(
                "115 atomic rename API is unavailable", kind="files"
            )
        payload: dict[str, Any] = {
            "fid": str(file_id),
            "name": str(name),
            "replace": False,
            "overwrite": False,
            "no_replace": True,
        }
        if expected_identity:
            for key in ("remote_file_id", "fingerprint"):
                if expected_identity.get(key) not in (None, ""):
                    payload[f"expected_{key}"] = str(expected_identity[key])
        if target_path:
            payload["target_path"] = str(target_path)
        try:
            result = fn(payload)
        except TypeError:
            try:
                result = fn(
                    str(file_id),
                    str(name),
                    replace=False,
                    overwrite=False,
                )
            except TypeError as exc:
                raise RemoteInventoryIncomplete(
                    "115 atomic rename signature is unsupported", kind="files"
                ) from exc
        info = _decode_remote_response(result, kind="files")
        _raise_on_remote_conflict(info)
        if isinstance(info, Mapping):
            _raise_on_failed_envelope(info, kind="files")
        return info

    def rename_file(self, file_id: str, name: str) -> Any:
        """Legacy spelling retained with the safe no-replace semantics."""

        return self.rename_file_no_replace(file_id, name)


def _p115_id(value: Any) -> int | str:
    text = str(value)
    try:
        return int(text)
    except (TypeError, ValueError):
        return text


def _remote_directory_id(value: Any) -> int | str:
    """Validate one opaque remote directory ID before calling ``fs_files``.

    A p115 directory ID may be numeric or an opaque string in a local fake,
    but it is never a POSIX/Windows path.  Rejecting path-looking tokens here
    keeps every caller on the same cid/pid-only contract.
    """

    if isinstance(value, (bool, Mapping, list, tuple, set)) or value is None:
        raise RemoteInventoryIncomplete(
            "115 directory ID is missing or invalid", kind="directories"
        )
    text = str(value).strip()
    if (
        not text
        or text in {".", ".."}
        or "/" in text
        or "\\" in text
        or text.startswith("~")
        or re.match(r"^[A-Za-z]:", text)
        or "://" in text
        or any(char.isspace() for char in text)
    ):
        raise RemoteInventoryIncomplete(
            "115 directory API requires a cid/pid, not a path",
            kind="directories",
        )
    return _p115_id(text)


def _normalize_remote_path(
    value: Any,
    *,
    kind: str,
    allow_empty: bool = False,
) -> str:
    if value is None or value == "":
        if allow_empty:
            return ""
        raise RemoteInventoryIncomplete(
            "115 remote path is missing", kind=kind
        )
    if not isinstance(value, str) or "\\" in value:
        raise RemoteInventoryIncomplete(
            "115 remote path is invalid", kind=kind
        )
    if not value.startswith("/") or value.startswith("//"):
        raise RemoteInventoryIncomplete(
            "115 remote path must be absolute POSIX syntax", kind=kind
        )
    raw_parts = value.split("/")
    if len(raw_parts) <= 1 or any(part == "" for part in raw_parts[1:]):
        raise RemoteInventoryIncomplete(
            "115 remote path contains an empty component", kind=kind
        )
    for part in raw_parts[1:]:
        if (
            part in {".", ".."}
            or _REMOTE_DRIVE_COMPONENT.fullmatch(part)
            or _REMOTE_DRIVE_COMPONENT.match(part)
            or _ENCODED_TRAVERSAL.search(part)
        ):
            raise RemoteInventoryIncomplete(
                "115 remote path contains an unsafe component", kind=kind
            )
    return "/" + "/".join(raw_parts[1:])


def _remote_path_parts(path: str) -> tuple[str, ...]:
    if not path:
        return ()
    return tuple(path.lstrip("/").split("/"))


_REMOTE_DRIVE_COMPONENT = re.compile(r"^[A-Za-z]:")
_ENCODED_TRAVERSAL = re.compile(r"%2e|%2f|%5c", flags=re.IGNORECASE)


def _positive_cap(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value <= 0:
        raise ValueError(f"{name} must be a positive integer")
    return value


def _nonnegative_cap(value: Any, name: str) -> int:
    if isinstance(value, bool) or not isinstance(value, int) or value < 0:
        raise ValueError(f"{name} must be a non-negative integer")
    return value


def _task_items_and_total(
    info: Any, *, requested_page: int | None = None,
    requested_page_size: int | None = None,
) -> tuple[list, Optional[int], Optional[bool]]:
    if isinstance(info, list):
        return info, None, None
    if not isinstance(info, dict):
        raise RemoteInventoryIncomplete(
            "115 task-list response has no recognized envelope",
            kind="tasks",
        )
    containers = (info, info.get("data"))
    totals: set[int] = set()
    end_flags: set[bool] = set()
    sdk_pages: set[tuple[int, int, int, int]] = set()
    # Read all known envelope metadata before selecting a task list.  A
    # top-level list must not hide contradictory nested totals or markers.
    for container in containers:
        if not isinstance(container, dict):
            continue
        _raise_on_failed_envelope(container, kind="tasks")
        # The installed SDK's clouddownload helper uses count for task count
        # and page_count for termination.  In this verified shape total is
        # account capacity (live: total=3000, count=399), not item count.
        sdk_shape = ("page_count" in container or "count" in container and
                     any(key in container for key in ("page_size", "page")))
        if sdk_shape:
            pagination = []
            for key in ("count", "page_count", "page_size", "page"):
                value = container.get(key)
                if (isinstance(value, bool) or not isinstance(value, (int, str))
                        or not re.fullmatch(r"[0-9]+", str(value))):
                    raise RemoteInventoryIncomplete("115 task pagination is malformed", kind="tasks")
                pagination.append(int(value))
            count, pages, size, page = pagination
            if (size <= 0 or page <= 0 or pages != (count + size - 1) // size
                    or page > max(1, pages)
                    or requested_page is not None and page != requested_page
                    or requested_page_size is not None and size != requested_page_size):
                raise RemoteInventoryIncomplete("115 task pagination is contradictory", kind="tasks")
            sdk_pages.add((count, pages, size, page))
            totals.add(count)
            end_flags.add(page >= pages)
        for key in ("total", "total_count", "totalCount"):
            if key not in container or sdk_shape and key == "total":
                continue
            value = container[key]
            if (isinstance(value, bool) or not isinstance(value, (int, str))
                    or not re.fullmatch(r"[0-9]+", str(value))):
                raise RemoteInventoryIncomplete("115 task inventory total is malformed", kind="tasks")
            totals.add(int(value))
        for key in ("has_more", "hasMore", "more", "complete", "finished", "is_last"):
            if key not in container:
                continue
            value = container[key]
            if not isinstance(value, bool):
                raise RemoteInventoryIncomplete("115 task inventory end marker is malformed", kind="tasks")
            end_flags.add(not value if key in ("has_more", "hasMore", "more") else value)
    if len(totals) > 1 or len(end_flags) > 1 or len(sdk_pages) > 1:
        raise RemoteInventoryIncomplete("115 task inventory metadata is contradictory", kind="tasks")
    total = next(iter(totals), None)
    complete = next(iter(end_flags), None)
    for container in containers:
        if isinstance(container, list):
            tasks = container
        elif isinstance(container, dict):
            tasks = next((container[key] for key in ("tasks", "list", "rows", "items")
                          if isinstance(container.get(key), list)), None)
        else:
            continue
        if tasks is None:
            continue
        _raise_on_contradictory_total(total, len(tasks), kind="tasks")
        for count, _pages, size, page in sdk_pages:
            expected_items = min(size, max(0, count - (page - 1) * size))
            if len(tasks) != expected_items:
                raise RemoteInventoryIncomplete("115 task page length is contradictory", kind="tasks")
        return tasks, total, complete
    # Observed live (2026-09-25): a throttled task-list response carries no
    # recognized list at all instead of a state:false envelope.  A genuine
    # envelope always has one, so this shape is the throttle signature, not
    # an inventory contradiction.
    raise RemoteRateLimited(
        "115 task-list response has no recognized task list",
        kind="tasks",
    )


def _decode_remote_response(result: Any, *, kind: str) -> Any:
    if not isinstance(result, str):
        return result
    try:
        return json.loads(result)
    except (TypeError, ValueError) as exc:
        raise RemoteInventoryIncomplete(
            f"115 {kind}-list response is not valid JSON",
            kind=kind,
        ) from exc


def _trusted_total(info: Any) -> Optional[int]:
    if not isinstance(info, dict):
        return None
    for key in ("total", "total_count", "totalCount"):
        value = info.get(key)
        if isinstance(value, bool):
            continue
        try:
            number = int(value)
        except (TypeError, ValueError):
            continue
        if number >= 0:
            return number
    return None


def _complete_directory_listing(info: Any) -> list:
    """Require affirmative, consistent completeness evidence for path proofs.

    The legacy inventory parser intentionally remains unchanged. A path proof
    cannot use a bare/short list, a request cap, or an unknown pagination shape
    as evidence that no same-name entry remains on another page. No pagination
    is attempted here: unsupported or partial responses block the operation.
    """
    list_keys = {"data", "list", "items", "files", "rows"}
    total_keys = {"total", "total_count", "totalCount", "count"}
    more_keys = {"has_more", "hasMore", "more"}
    complete_keys = {"complete", "finished", "is_last"}
    next_keys = {"next", "next_page", "nextPage", "next_cursor"}
    page_keys = {"page", "page_number", "current_page"}
    size_keys = {"page_size", "pageSize", "limit", "page_limit"}
    containers = []
    current = info
    while isinstance(current, dict):
        _raise_on_failed_envelope(current, kind="directories")
        # p115client's real /files response includes benign file counters,
        # timing fields and account metadata.  Completeness is proved below
        # from the explicit total/offset, not from a metadata whitelist.
        containers.append(current)
        payload_keys = list_keys.intersection(current)
        if len(payload_keys) != 1:
            break
        key = next(iter(payload_keys))
        current = current[key]
        if isinstance(current, dict) and key != "data":
            break
    if not containers or not isinstance(current, list):
        raise RemoteInventoryIncomplete(
            "115 directory listing has no complete recognized envelope", kind="directories"
        )
    complete = False
    valid = all(isinstance(item, Mapping) for item in current)
    for container in containers:
        for key, value in container.items():
            if key in total_keys | page_keys | size_keys | {"offset"}:
                # Avoid bool/float coercion and malformed numeric hints.
                if isinstance(value, bool) or not re.fullmatch(r"[0-9]+", str(value)):
                    valid = False
                    continue
                number = int(value)
                if key in total_keys:
                    valid &= number == len(current)
                    complete = True
                elif key in page_keys:
                    valid &= number == 1
                elif key == "offset":
                    valid &= number == 0
                else:
                    valid &= number > 0
            elif key in more_keys:
                valid &= value is False
                complete |= value is False
            elif key in complete_keys:
                valid &= value is True
                complete |= value is True
            elif key in next_keys:
                valid &= value is None or value == ""
    if not valid or not complete:
        raise RemoteInventoryIncomplete(
            "115 directory listing completeness is unproven or contradictory",
            kind="directories",
        )
    return current


def _complete_directory_listing_pages(
    request: Any, directory_id: str, first: Any, page_size: int, max_pages: int
) -> list:
    """Consume a bounded 115 directory inventory with stable total and offset."""

    all_entries: list = []
    expected_total: int | None = None
    response = first
    for page in range(max_pages):
        if not isinstance(response, Mapping):
            raise RemoteRateLimited("115 directory page is malformed", kind="directories")
        _raise_on_failed_envelope(response, kind="directories")
        entries = response.get("data")
        total = response.get("count")
        offset = response.get("offset")
        if (
            not isinstance(entries, list)
            or isinstance(total, bool) or not isinstance(total, int) or total < 0
            or isinstance(offset, bool) or not isinstance(offset, int)
            or offset != len(all_entries)
            or any(not isinstance(item, Mapping) for item in entries)
        ):
            # Observed live (2026-09-25): throttled responses arrive as
            # structurally broken envelopes for a known-good directory id,
            # while genuine directories always return well-formed pages.
            raise RemoteRateLimited("115 directory page is incomplete", kind="directories")
        if expected_total is None:
            expected_total = total
        elif total != expected_total:
            raise RemoteInventoryIncomplete("115 directory count changed during listing", kind="directories")
        if len(all_entries) + len(entries) > total:
            raise RemoteInventoryIncomplete("115 directory count is contradictory", kind="directories")
        all_entries.extend(entries)
        if len(all_entries) == total:
            return all_entries
        if not entries:
            # A declared total with an empty page is the classic truncated
            # rate-limit response, not a completed listing.
            raise RemoteRateLimited("115 directory page made no progress", kind="directories")
        try:
            response = _decode_remote_response(
                request({"cid": directory_id, "offset": len(all_entries), "limit": page_size}),
                kind="files",
            )
        except RemoteInventoryIncomplete:
            raise
        except Exception as exc:
            raise RemoteRateLimited("115 directory next page failed", kind="directories") from exc
    raise RemoteInventoryIncomplete("115 directory page cap reached", kind="directories")


def _raise_on_missing_or_duplicate_listing_identities(entries: list) -> None:
    """Reject a complete listing whose entries lack or repeat remote IDs.

    Downstream path proofs and mutations address entries only by these IDs.
    A listing that cannot prove one identity per entry, or that returns the
    same identity twice, must fail closed instead of letting a later move
    or rename target the wrong item.
    """

    seen: set[tuple[str, str]] = set()
    for item in entries:
        if _item_is_directory(item):
            identity = _strict_directory_id(item)
            key = ("directory", str(identity))
        else:
            identity = _strict_file_id(item)
            key = ("file", str(identity))
        if identity in (None, "") or key in seen:
            raise RemoteInventoryIncomplete(
                "115 directory listing has missing or duplicate entry identities",
                kind="directories",
            )
        seen.add(key)


def _inventory_list_from_envelope(info: Any, *, keys: tuple[str, ...], kind: str) -> list:
    """Extract a recognized remote list; unknown objects are incomplete."""
    if isinstance(info, list):
        return info
    if not isinstance(info, dict):
        raise RemoteInventoryIncomplete(
            f"115 {kind}-list response has no recognized envelope",
            kind=kind,
        )
    _raise_on_failed_envelope(info, kind=kind)
    # ``data`` is a documented wrapper and may itself contain the list key.
    containers = (info, info.get("data"))
    for container in containers:
        if isinstance(container, list):
            return container
        if not isinstance(container, dict):
            continue
        _raise_on_failed_envelope(container, kind=kind)
        for key in keys:
            if key in container and isinstance(container.get(key), list):
                return container[key]
    raise RemoteInventoryIncomplete(
        f"115 {kind}-list response has no recognized list",
        kind=kind,
    )


def _envelope_state_failed(info: Mapping[str, Any]) -> bool:
    """Whether any 115 state/success flag in the envelope reports failure."""
    return any(
        _envelope_value_is_false(info.get(key))
        for key in ("state", "success")
        if key in info
    )


def _envelope_reports_rate_limit(info: Mapping[str, Any]) -> bool:
    """Whether a failed envelope's errNo/error text marks transport throttling.

    Observed live (2026-09-25): the account-level throttle answers with
    state:false envelopes carrying errNo 980005 / "405 METHOD NOT ALLOWED".
    Those report transport pacing, not an inventory contradiction, so they
    must not spend the review budget.
    """
    errno_text = str(info.get("errNo", info.get("errno", ""))).strip()
    if errno_text == "980005":
        return True
    error_text = str(info.get("error", "")).strip()
    return (
        "405 METHOD NOT ALLOWED" in error_text.upper()
        or "TOO MANY REQUEST" in error_text.upper()
        or "频繁" in error_text
    )


def _raise_on_failed_envelope(info: Mapping[str, Any], *, kind: str) -> None:
    if not _envelope_state_failed(info):
        return
    if str(info.get("errno", "")) == "990001":
        raise RemoteInventoryIncomplete(
            "115 authentication expired", kind=kind,
        )
    if _envelope_reports_rate_limit(info):
        error_text = str(info.get("error", "")).strip()
        errno_text = str(
            info.get("errNo", info.get("errno", ""))
        ).strip()
        raise RemoteRateLimited(
            f"115 {kind}-list response reports rate limiting: "
            + (error_text[:120] or f"errNo {errno_text}"),
            kind=kind,
        )
    raise RemoteInventoryIncomplete(
        f"115 {kind}-list response reports failure",
        kind=kind,
    )


def _raise_on_remote_conflict(info: Any) -> None:
    """Map an explicit no-replace conflict to the organizer's review path."""

    if _remote_conflict_marker(info):
        raise RemotePathConflict("115 atomic rename destination conflict")


def _remote_conflict_marker(info: Any, inherited_failed: bool = False) -> bool:
    if isinstance(info, Mapping):
        failed = inherited_failed or any(
            _envelope_value_is_false(info.get(key))
            for key in ("state", "success")
            if key in info
        )
        marker_values = [
            info.get(key)
            for key in ("reason", "code", "error", "status")
            if key in info
        ]
        if failed and any(_is_remote_conflict_value(value) for value in marker_values):
            return True
        return any(
            _remote_conflict_marker(nested, failed)
            for key in ("data", "result", "directory", "item")
            for nested in (info.get(key),)
            if isinstance(nested, (Mapping, list, tuple))
        )
    if isinstance(info, (list, tuple)):
        return any(_remote_conflict_marker(item, inherited_failed) for item in info)
    return False


def _envelope_value_is_false(value: Any) -> bool:
    if value is False or value == 0:
        return True
    return isinstance(value, str) and value.strip().casefold() in {
        "0", "false", "fail", "failed", "error"
    }


def _is_remote_conflict_value(value: Any) -> bool:
    if value is None:
        return False
    normalized = str(value).replace("-", "_").replace(" ", "_").casefold()
    return normalized in {
        "409",
        "path_conflict",
        "target_exists",
        "destination_exists",
        "already_exists",
        "file_exists",
        "name_exists",
        "eexist",
        "conflict",
    } or "conflict" in normalized or normalized.endswith("_exists")


def _raise_on_failed_envelope_deep(info: Mapping[str, Any], *, kind: str) -> None:
    """Reject failure markers at either the response or documented data layer."""

    _raise_on_failed_envelope(info, kind=kind)
    for key in ("data", "result", "directory", "item"):
        nested = info.get(key)
        if isinstance(nested, Mapping):
            _raise_on_failed_envelope_deep(nested, kind=kind)


def _raise_on_contradictory_total(total: Optional[int], consumed: int, *, kind: str) -> None:
    if total is not None and consumed > 0 and total < consumed:
        raise RemoteInventoryIncomplete(
            f"115 {kind}-list response has contradictory total",
            kind=kind,
        )


def _page_size_from_envelope(info: Any, default: int) -> int:
    """Read a validated page-size hint without trusting arbitrary counts."""
    containers = (info, info.get("data") if isinstance(info, dict) else None)
    for container in containers:
        if not isinstance(container, dict):
            continue
        for key in ("page_size", "pageSize", "limit", "page_limit"):
            value = container.get(key)
            if isinstance(value, bool):
                continue
            try:
                parsed = int(value)
            except (TypeError, ValueError):
                continue
            if parsed > 0:
                return parsed
    return default


def _item_name(item: Mapping[str, Any]) -> str:
    return str(_first_value(item, ("n", "name", "file_name", "filename")) or "")


def _normalize_file_record(item: Mapping[str, Any]) -> Dict[str, Any]:
    """Normalize one p115 file entry without guessing from its suffix."""

    result = dict(item)
    file_id = _first_value(item, ("fid", "file_id", "id"))
    name = _item_name(item)
    if file_id not in (None, ""):
        result.setdefault("file_id", file_id)
        result.setdefault("remote_file_id", file_id)
    if name and "name" not in result:
        result["name"] = name
    return result


def _file_from_response(info: Any, path: str) -> Optional[Dict[str, Any]]:
    """Extract exactly one path-matching file from a direct response."""

    entries = _iter_remote_items(info)
    normalized_path = posixpath.normpath(path.replace("\\", "/"))
    matches = []
    for item in entries:
        if not isinstance(item, Mapping):
            continue
        item_path = _first_value(item, ("path", "full_path", "remote_path"))
        if isinstance(item_path, str) and posixpath.normpath(item_path.replace("\\", "/")) == normalized_path:
            matches.append(_normalize_file_record(item))
    if len(matches) > 1:
        raise RemoteInventoryIncomplete(
            "115 file lookup returned ambiguous path", kind="files"
        )
    return matches[0] if matches else None


def _iter_remote_items(info: Any) -> list[Any]:
    if isinstance(info, Mapping):
        for key in ("file", "item", "result"):
            value = info.get(key)
            if isinstance(value, Mapping):
                return [value]
        for key in ("data", "files", "items", "list", "rows"):
            value = info.get(key)
            if isinstance(value, list):
                return value
            if key == "data" and isinstance(value, Mapping):
                return [value]
        return [info]
    if isinstance(info, list):
        return info
    return []


def _item_id(item: Mapping[str, Any], is_directory: bool) -> Any:
    keys = (("cid", "directory_id", "id", "fid", "file_id")
            if is_directory else ("fid", "file_id", "id"))
    return _first_value(item, keys)


def _item_is_directory(item: Mapping[str, Any]) -> bool:
    for key in ("is_dir", "is_directory", "directory"):
        if key in item:
            value = item[key]
            if isinstance(value, bool):
                return value
            if str(value).strip().casefold() in {"1", "true", "yes", "folder", "directory", "dir"}:
                return True
            if str(value).strip().casefold() in {"0", "false", "no", "file"}:
                return False
    for key in ("type", "file_type", "category"):
        if key in item:
            value = str(item[key]).strip().casefold()
            if value in {"folder", "directory", "dir", "cid", "0"}:
                return True
            if value in {"file", "video", "document", "1"}:
                return False
    # p115's stable shape uses cid for folders and fid for files.  This check
    # intentionally occurs before any extension inspection.
    if item.get("fid") not in (None, "") or item.get("file_id") not in (None, ""):
        return False
    if item.get("cid") not in (None, "") or item.get("directory_id") not in (None, ""):
        return True
    return bool(item.get("fc") is not None or item.get("file_count") is not None)


def _strict_directory_id(item: Mapping[str, Any]) -> Any:
    """Return only an explicit directory identity, never a path or file ID."""

    if not _item_is_directory(item):
        return None
    value = _first_value(item, ("cid", "id", "directory_id"))
    if value in (None, ""):
        return None
    return _remote_directory_id(value)


def _strict_file_id(item: Mapping[str, Any]) -> Any:
    """Return a file ID from an explicitly classified file entry."""

    if _item_is_directory(item):
        return None
    return _first_value(item, ("fid", "file_id", "id"))


def _directory_match(
    entries: list,
    name: str,
    expected_parts: list[str],
) -> Optional[tuple[Any, Mapping[str, Any]]]:
    matches: list[tuple[Any, Mapping[str, Any]]] = []
    expected_path = "/" + "/".join(expected_parts)
    for item in entries:
        if not isinstance(item, Mapping) or _item_name(item) != name:
            continue
        if not _item_is_directory(item):
            continue
        directory_id = _strict_directory_id(item)
        if directory_id in (None, ""):
            raise RemoteInventoryIncomplete(
                "115 directory entry has no explicit cid/id", kind="directories"
            )
        item_path = _first_value(item, ("path", "full_path", "remote_path"))
        if item_path not in (None, "") and _normalize_remote_path(
            item_path, kind="directories"
        ) != expected_path:
            raise RemoteInventoryIncomplete(
                "115 directory entry path does not match requested path",
                kind="directories",
            )
        matches.append((directory_id, item))
    if len(matches) > 1:
        raise RemoteInventoryIncomplete(
            "115 directory lookup returned ambiguous same-name entries",
            kind="directories",
        )
    return matches[0] if matches else None


def _directory_id_from_response(info: Any) -> Any:
    """Extract only cid/id-shaped directory fields from a create response."""

    if isinstance(info, Mapping):
        value = _first_value(info, ("cid", "id", "directory_id"))
        if value not in (None, ""):
            return value
        for key in ("data", "result", "directory", "item"):
            if key in info:
                value = _directory_id_from_response(info[key])
                if value not in (None, ""):
                    return value
    elif isinstance(info, list):
        for item in info:
            value = _directory_id_from_response(item)
            if value not in (None, ""):
                return value
    return None


def _first_value_from_response(info: Any, keys: tuple[str, ...]) -> Any:
    if isinstance(info, Mapping):
        value = _first_value(info, keys)
        if value not in (None, ""):
            return value
        for key in ("data", "result", "directory", "item"):
            if key in info:
                value = _first_value_from_response(info[key], keys)
                if value not in (None, ""):
                    return value
    elif isinstance(info, list):
        for item in info:
            value = _first_value_from_response(item, keys)
            if value not in (None, ""):
                return value
    return None


def _first_value(item: Mapping[str, Any], keys: tuple[str, ...]) -> Any:
    for key in keys:
        if item.get(key) not in (None, ""):
            return item[key]
    return None
