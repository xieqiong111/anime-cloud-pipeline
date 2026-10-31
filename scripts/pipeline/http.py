"""Bounded, secret-safe JSON HTTP transport shared by real adapters.

The transport deliberately treats POST as non-retryable.  A caller that has a
persisted write intent can reconcile that intent at its service boundary, but
the generic client must never issue a second non-idempotent request merely
because the first response was lost.
"""
from __future__ import annotations

from dataclasses import dataclass
import datetime as dt
from email.utils import parsedate_to_datetime
import gzip
import json
import math
import random
import socket
import time
from typing import Any, Callable, Iterable, Mapping, Sequence
import urllib.error
import urllib.parse
import urllib.request

from .security import REDACTED, sanitize_text
from .diagnostics import request_timeout, stage


class HttpError(RuntimeError):
    """A safe transport or HTTP response failure."""

    def __init__(
        self,
        status: int | None = None,
        reason: object = "request failed",
        *,
        method: str = "",
        url: str = "",
        secrets: Iterable[object] = (),
        headers: Mapping[str, Any] | None = None,
    ) -> None:
        self.status = int(status) if status is not None else None
        self.method = str(method or "").upper()
        self.url = _safe_error_url(url, secrets=secrets)
        self.headers = dict(headers or {})
        self.reason = _safe_reason(reason, secrets=secrets)
        status_text = f" HTTP {self.status}" if self.status is not None else ""
        endpoint = f" {self.method} {self.url}" if self.method or self.url else ""
        super().__init__(f"HTTP request failed{status_text}{endpoint}: {self.reason}")


class HttpResponseError(HttpError):
    """An HTTP response with a non-success status."""


class HttpTransportError(HttpError):
    """A timeout, connection, or transport-level failure."""


@dataclass(frozen=True, init=False)
class RetryPolicy:
    """Finite retry and backoff limits.

    ``max_attempts`` counts the initial request.  ``max_retries`` is accepted
    as a compatibility spelling and exposed as a property with the same
    total-attempt semantics used by the V5 configuration.
    """

    max_attempts: int
    max_read_retries: int | None
    backoff_seconds: float
    backoff_max_seconds: float
    jitter_seconds: float

    def __init__(
        self,
        max_attempts: int | None = None,
        backoff_seconds: float = 1.0,
        backoff_max_seconds: float = 30.0,
        jitter_seconds: float = 0.0,
        *,
        max_retries: int | None = None,
        max_read_retries: int | None = None,
    ) -> None:
        if max_attempts is None:
            max_attempts = max_retries if max_retries is not None else 3
        elif max_retries is not None and max_attempts != max_retries:
            raise ValueError("max_attempts and max_retries disagree")
        if isinstance(max_attempts, bool) or not isinstance(max_attempts, int):
            raise ValueError("max_attempts must be a positive integer")
        if max_attempts <= 0:
            raise ValueError("max_attempts must be a positive integer")
        if max_read_retries is not None:
            if isinstance(max_read_retries, bool) or not isinstance(max_read_retries, int) or max_read_retries <= 0:
                raise ValueError("max_read_retries must be a positive integer")
        values = {
            "backoff_seconds": backoff_seconds,
            "backoff_max_seconds": backoff_max_seconds,
            "jitter_seconds": jitter_seconds,
        }
        for name, value in values.items():
            if isinstance(value, bool):
                raise ValueError(f"{name} must be a finite non-negative number")
            try:
                number = float(value)
            except (TypeError, ValueError) as exc:
                raise ValueError(f"{name} must be a finite non-negative number") from exc
            if not math.isfinite(number) or number < 0:
                raise ValueError(f"{name} must be a finite non-negative number")
            object.__setattr__(self, name, number)
        if self.backoff_max_seconds <= 0:
            raise ValueError("backoff_max_seconds must be positive")
        if self.backoff_max_seconds < self.backoff_seconds:
            raise ValueError("backoff_max_seconds must not be less than backoff_seconds")
        object.__setattr__(self, "max_attempts", max_attempts)
        object.__setattr__(self, "max_read_retries", max_read_retries)

    @property
    def max_retries(self) -> int:
        return self.max_attempts

    def attempt_cap(self, method: str) -> int:
        """Return the configured total-attempt cap for one request kind."""
        if str(method or "").upper() in {"GET", "HEAD", "OPTIONS"} and self.max_read_retries is not None:
            return min(self.max_attempts, self.max_read_retries)
        return self.max_attempts


def retry_delay(
    policy: Any,
    attempt: int,
    random_source: Callable[[], float] | None = None,
) -> float:
    """Return one bounded exponential retry delay.

    ``attempt`` is one-based and denotes the failed attempt before the sleep.
    The helper accepts both :class:`RetryPolicy` and the small mapping/object
    compatibility ports used by the service and runner layers.  Jitter is
    applied before the final cap so no caller can lease or schedule beyond the
    configured maximum.
    """

    def read(*names: str, default: Any) -> Any:
        for name in names:
            if isinstance(policy, Mapping) and name in policy:
                return policy[name]
            value = getattr(policy, name, None)
            if value is not None:
                return value
        return default

    try:
        failed_attempt = max(1, int(attempt))
    except (TypeError, ValueError):
        failed_attempt = 1
    try:
        base = max(0.0, float(read("backoff_seconds", "base", default=0.0)))
    except (TypeError, ValueError, OverflowError):
        base = 0.0
    try:
        cap = max(base, float(read("backoff_max_seconds", "cap", default=base)))
    except (TypeError, ValueError, OverflowError):
        cap = base
    if not math.isfinite(base):
        base = 0.0
    if not math.isfinite(cap):
        cap = base
    delay = min(cap, base * (2 ** min(failed_attempt - 1, 30)))
    try:
        jitter = max(0.0, float(read("jitter_seconds", "jitter", default=0.0)))
    except (TypeError, ValueError, OverflowError):
        jitter = 0.0
    if not math.isfinite(jitter):
        jitter = 0.0
    if jitter:
        source = random_source or read("random_source", "random", default=None) or random.random
        try:
            random_value = float(source())
        except (TypeError, ValueError, OverflowError):
            random_value = 0.0
        if math.isfinite(random_value):
            delay += max(0.0, random_value) * jitter
    return min(cap, max(0.0, delay))


@dataclass(frozen=True)
class _Response:
    status: int
    payload: Any
    headers: Mapping[str, Any]


class _UrllibTransport:
    """Small stdlib transport kept behind the injectable client boundary."""

    def request(
        self,
        method: str,
        url: str,
        *,
        body: Any = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
    ) -> tuple[int, Any, Mapping[str, Any]]:
        encoded = _encode_body(body, headers or {})
        request = urllib.request.Request(
            url,
            data=encoded,
            headers=dict(headers or {}),
            method=method,
        )
        try:
            with urllib.request.urlopen(request, timeout=timeout) as response:
                raw = response.read()
                response_headers = dict(response.headers.items())
                if str(response_headers.get("Content-Encoding", "")).casefold() == "gzip":
                    raw = gzip.decompress(raw)
                return (
                    int(response.status),
                    _decode_body(raw, response_headers),
                    response_headers,
                )
        except urllib.error.HTTPError as exc:
            # urllib represents HTTP 4xx/5xx responses as exceptions.  Read
            # only the response shape; request URLs and bodies never enter the
            # raised error text.
            try:
                raw = exc.read()
            except Exception:
                raw = b""
            response_headers = dict(exc.headers.items()) if exc.headers else {}
            if str(response_headers.get("Content-Encoding", "")).casefold() == "gzip":
                raw = gzip.decompress(raw)
            return (
                int(exc.code),
                _decode_body(raw, response_headers),
                response_headers,
            )


class JsonHttpClient:
    """Injectable JSON client with bounded read retries and safe errors.

    A custom transport may return ``(status, payload)`` or
    ``(status, payload, headers)``.  It may also expose a ``request`` method,
    which keeps local mocks independent of urllib implementation details.
    """

    _SAFE_METHODS = frozenset({"GET", "HEAD", "OPTIONS"})
    _RETRY_STATUSES = frozenset({429})

    def __init__(
        self,
        policy: RetryPolicy | None = None,
        *,
        transport: Any = None,
        sleep: Callable[[float], Any] | None = None,
        jitter: Callable[[], float] | None = None,
        secrets: Iterable[object] = (),
        timeout: float | None = None,
        now: Callable[[], float] | None = None,
    ) -> None:
        self.policy = policy or RetryPolicy()
        self.transport = transport or _UrllibTransport()
        self.sleep = sleep or __import__("time").sleep
        self.jitter = jitter or __import__("random").random
        self.secrets = tuple(secrets)
        self.timeout = timeout
        self.now = now or time.time

    def request(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        body: Any = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        idempotent: bool = False,
    ) -> Any:
        """Return a decoded payload or raise a secret-safe ``HttpError``."""

        _status, payload = self.request_with_status(
            method,
            url,
            params=params,
            body=body,
            headers=headers,
            timeout=timeout,
            idempotent=idempotent,
        )
        return payload

    def request_with_status(
        self,
        method: str,
        url: str,
        *,
        params: Mapping[str, Any] | Sequence[tuple[str, Any]] | None = None,
        body: Any = None,
        headers: Mapping[str, str] | None = None,
        timeout: float | None = None,
        idempotent: bool = False,
        raise_for_status: bool = True,
    ) -> tuple[int, Any]:
        method = str(method or "GET").upper()
        merged_url = _merge_query(url, params)
        request_headers = {"User-Agent": "anime-cloud-pipeline/2.0"}
        request_headers.update(dict(headers or {}))
        effective_timeout = self.timeout if timeout is None else timeout
        attempts = 0
        attempt_cap = self.policy.attempt_cap(method)
        while True:
            attempts += 1
            try:
                bounded_timeout = request_timeout(effective_timeout)
                with stage("http.attempt", attempt=attempts, timeout_s=bounded_timeout):
                    response = self._call_transport(
                        method,
                        merged_url,
                        body=body,
                        headers=request_headers,
                        timeout=bounded_timeout,
                    )
                normalized = _normalize_response(response)
                if normalized.status >= 400:
                    error = HttpResponseError(
                        normalized.status,
                        _response_reason(normalized.status, normalized.payload),
                        method=method,
                        url=merged_url,
                        secrets=self.secrets,
                        headers=normalized.headers,
                    )
                    if self._should_retry(method, normalized.status, None, idempotent) and attempts < attempt_cap:
                        self._sleep_before_retry(
                            attempts,
                            retry_after=_retry_after_seconds(normalized.headers, now=self.now),
                        )
                        continue
                    if raise_for_status:
                        raise error
                    return normalized.status, normalized.payload
                return normalized.status, normalized.payload
            except HttpError as error:
                # Preserve status and safe reason while attaching a safe
                # endpoint if a transport-created error omitted context.
                if not error.method and not error.url:
                    error = type(error)(
                        error.status,
                        # A third-party transport may put an unstructured
                        # ``token=value`` fragment in its exception text;
                        # retain only the exception category at this
                        # boundary rather than echoing that text.
                        _safe_exception_reason(error),
                        method=method,
                        url=merged_url,
                        secrets=self.secrets,
                        headers=getattr(error, "headers", {}),
                    )
                if self._should_retry(method, error.status, error, idempotent) and attempts < attempt_cap:
                    self._sleep_before_retry(
                        attempts,
                        retry_after=_retry_after_seconds(getattr(error, "headers", {}), now=self.now),
                    )
                    continue
                raise error
            except (KeyboardInterrupt, SystemExit):
                raise
            except BaseException as exc:
                status = getattr(exc, "code", getattr(exc, "status", getattr(exc, "status_code", None)))
                if isinstance(status, int) and 400 <= status <= 599:
                    safe_error = HttpResponseError(
                        status,
                        f"HTTP {status} response",
                        method=method,
                        url=merged_url,
                        secrets=self.secrets,
                    )
                    if self._should_retry(method, status, None, idempotent) and attempts < attempt_cap:
                        self._sleep_before_retry(attempts)
                        continue
                    if not raise_for_status:
                        return status, None
                    raise safe_error from None
                safe_error = HttpTransportError(
                    None,
                    _safe_exception_reason(exc),
                    method=method,
                    url=merged_url,
                    secrets=self.secrets,
                )
                if self._should_retry(method, None, exc, idempotent) and attempts < attempt_cap:
                    self._sleep_before_retry(attempts)
                    continue
                raise safe_error from None

    def _call_transport(self, method: str, url: str, **kwargs: Any) -> Any:
        request = getattr(self.transport, "request", None)
        if callable(request):
            return request(method, url, **kwargs)
        if callable(self.transport):
            return self.transport(method, url, **kwargs)
        raise TypeError("transport must be callable or expose request()")

    def _should_retry(
        self,
        method: str,
        status: int | None,
        error: BaseException | None,
        idempotent: bool,
    ) -> bool:
        # ``idempotent`` is intentionally ignored for POST: a service method
        # must reconcile a persisted intent itself before any new submit.
        if method == "POST":
            return False
        if method not in self._SAFE_METHODS and not idempotent:
            return False
        if status is not None:
            return status in self._RETRY_STATUSES or 500 <= status <= 599
        if error is None:
            return False
        return _is_transient_exception(error)

    def _sleep_before_retry(self, failed_attempt: int, *, retry_after: float | None = None) -> None:
        if retry_after is not None and math.isfinite(retry_after):
            delay = max(0.0, min(retry_after, self.policy.backoff_max_seconds))
            self.sleep(float(delay))
            return
        delay = retry_delay(self.policy, failed_attempt, self.jitter)
        self.sleep(float(delay))


def _encode_body(body: Any, headers: Mapping[str, str]) -> bytes | None:
    if body is None:
        return None
    if isinstance(body, bytes):
        return body
    content_type = "".join(
        str(value) for key, value in headers.items() if str(key).casefold() == "content-type"
    ).casefold()
    if "json" in content_type or isinstance(body, (dict, list, tuple)):
        return json.dumps(body, ensure_ascii=False).encode("utf-8")
    return str(body).encode("utf-8")


def _decode_body(raw: bytes, headers: Mapping[str, Any]) -> Any:
    if not raw:
        return None
    content_type = str(
        next((value for key, value in headers.items() if str(key).casefold() == "content-type"), "")
    ).casefold()
    text = raw.decode("utf-8", "replace")
    if "json" not in content_type and not text.lstrip().startswith(("{", "[")):
        return raw
    try:
        return json.loads(text)
    except (TypeError, ValueError):
        return raw


def _normalize_response(response: Any) -> _Response:
    if isinstance(response, _Response):
        return response
    if isinstance(response, tuple) and response:
        if isinstance(response[0], int):
            status = int(response[0])
            payload = response[1] if len(response) > 1 else None
            headers = response[2] if len(response) > 2 and isinstance(response[2], Mapping) else {}
            if isinstance(payload, bytes):
                payload = _decode_body(payload, headers)
            return _Response(status, payload, headers)
    if isinstance(response, Mapping) and any(key in response for key in ("status", "status_code")):
        status = int(response.get("status", response.get("status_code", 200)))
        payload = response.get(
            "payload",
            response.get("json", response.get("data", response.get("body"))),
        )
        headers = response.get("headers") if isinstance(response.get("headers"), Mapping) else {}
        if isinstance(payload, bytes):
            payload = _decode_body(payload, headers)
        return _Response(status, payload, headers)
    status = int(getattr(response, "status", getattr(response, "status_code", 200)))
    headers = getattr(response, "headers", {})
    if hasattr(response, "read") and callable(response.read):
        payload = _decode_body(response.read(), headers if isinstance(headers, Mapping) else {})
    else:
        payload = response
    return _Response(status, payload, headers if isinstance(headers, Mapping) else {})


def _merge_query(
    url: str,
    params: Mapping[str, Any] | Sequence[tuple[str, Any]] | None,
) -> str:
    if not params:
        return str(url)
    split = urllib.parse.urlsplit(str(url))
    existing = urllib.parse.parse_qsl(split.query, keep_blank_values=True)
    additions = list(params.items()) if isinstance(params, Mapping) else list(params)
    replaced = {str(key) for key, _value in additions}
    merged = [(key, value) for key, value in existing if key not in replaced]
    for key, value in additions:
        if isinstance(value, (list, tuple)):
            merged.extend((key, item) for item in value)
        else:
            merged.append((key, value))
    query = urllib.parse.urlencode(merged, doseq=True)
    return urllib.parse.urlunsplit((split.scheme, split.netloc, split.path, query, split.fragment))


def _safe_error_url(url: str, *, secrets: Iterable[object] = ()) -> str:
    if not url:
        return ""
    try:
        split = urllib.parse.urlsplit(str(url))
        query = urllib.parse.parse_qsl(split.query, keep_blank_values=True)
        # Error output retains parameter names for diagnosis but never values;
        # this also protects unknown service-specific secret names.
        safe_query = urllib.parse.urlencode([(key, REDACTED if value else "") for key, value in query])
        # Userinfo is another URL credential form.  Preserve host/port for
        # diagnostics while dropping username/password entirely.
        host = split.hostname or ""
        if split.port is not None:
            host = f"{host}:{split.port}"
        safe = urllib.parse.urlunsplit((split.scheme, host, split.path, safe_query, ""))
    except Exception:
        # ``urlsplit`` can reject malformed bracketed hosts before it gives us
        # a chance to remove userinfo/query values.  Never fall back to the
        # original string: it may contain both credentials and secret params.
        safe = "<safe-endpoint>"
    return sanitize_text(safe, secrets=secrets)


def _safe_reason(reason: object, *, secrets: Iterable[object] = ()) -> str:
    """Keep structured third-party errors from becoming secret-bearing text."""
    if isinstance(reason, Mapping) or isinstance(reason, (list, tuple, set)):
        return "structured-error"
    if isinstance(reason, bytes):
        return "bytes-error"
    return sanitize_text(reason, secrets=secrets)


def _retry_after_seconds(headers: Mapping[str, Any] | None, *, now: Callable[[], float]) -> float | None:
    if not isinstance(headers, Mapping):
        return None
    value = next((value for key, value in headers.items()
                  if str(key).casefold() == "retry-after"), None)
    if value is None:
        return None
    try:
        seconds = float(str(value).strip())
    except (TypeError, ValueError):
        seconds = None
    if seconds is not None and math.isfinite(seconds):
        return max(0.0, seconds)
    try:
        parsed = parsedate_to_datetime(str(value))
        if parsed is None:
            return None
        if parsed.tzinfo is None:
            parsed = parsed.replace(tzinfo=dt.timezone.utc)
        return max(0.0, parsed.timestamp() - float(now()))
    except (TypeError, ValueError, OverflowError, OSError):
        return None


def _response_reason(status: int, payload: Any) -> str:
    # Never echo a remote response body: it may contain credentials or tokens.
    del payload
    return f"HTTP {status} response"


def _safe_exception_reason(exc: BaseException) -> str:
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        return f"{type(exc).__name__} ({type(reason).__name__})"
    return type(exc).__name__


def _is_transient_exception(exc: BaseException) -> bool:
    if isinstance(exc, (TimeoutError, socket.timeout, ConnectionError, OSError)):
        return True
    if isinstance(exc, urllib.error.URLError):
        reason = getattr(exc, "reason", None)
        return isinstance(reason, (TimeoutError, socket.timeout, ConnectionError, OSError))
    return False


__all__ = [
    "HttpError",
    "HttpResponseError",
    "HttpTransportError",
    "JsonHttpClient",
    "RetryPolicy",
    "retry_delay",
]
