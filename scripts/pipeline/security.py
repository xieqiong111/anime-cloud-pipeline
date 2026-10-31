"""Centralized redaction and project logging policy.

All persisted error text should call :func:`sanitize_text` before storage.  A
single handler on the ``pipeline`` logger applies the same function to output,
so adapter messages and child logger records follow one policy.
"""
from __future__ import annotations

import logging
import re
import sys
from typing import Iterable, TextIO


REDACTED = "[REDACTED]"

# Values are deliberately captured conservatively: stop at common separators
# but do not consume the next field in a log line.  Parameter names are kept so
# an operator can still understand which credential category was present.
_HEADER_BOUNDARY = (
    r"(?:Authorization|Cookies?|Set-Cookies?|X?-?API[-_ ]?Key|API[-_ ]?Key|"
    r"Access[-_ ]?Token|Refresh[-_ ]?Token|Password|Passwd|Credentials?|Secret|"
    r"Request[-_ ]?ID|Trace[-_ ]?ID|Correlation[-_ ]?ID)"
    r"\s*[:=]"
)
# A semicolon, comma, or whitespace followed by one of the known field names
# starts a new diagnostic field.  Unknown ``name=value`` pairs after a Cookie
# header remain cookie continuation, which avoids leaking simple cookie pairs
# while preserving unrelated fields such as Request-ID.
_NEXT_HEADER = rf"(?:[;,]\s*|\s+){_HEADER_BOUNDARY}"
_AUTH_VALUE = re.compile(
    rf"(?i)(\bAuthorization\s*[:=]\s*)([^\r\n;,]*?)"
    rf"(?={_NEXT_HEADER}|[;,]|$)"
)
_BEARER_VALUE = re.compile(r"(?i)(\bBearer\s+)([^\s,;\"']+)")
_COOKIE_HEADER = re.compile(
    rf"(?i)(\b(?:Set-)?Cookies?\s*[:=]\s*)([^\r\n]*?)"
    rf"(?={_NEXT_HEADER}|$)"
)
_HEADER_SECRET = re.compile(
    r"(?i)(\b(?:X?-?API[-_ ]?Key|API[-_ ]?Key|"
    r"Access[-_ ]?Token|Refresh[-_ ]?Token|Password|Passwd|Credentials?|Secret)"
    r"\s*[:=]\s*)([^\s,;&\"']+)"
)
_QUERY_SECRET = re.compile(
    r"(?i)([?&](?:access[_-]?token|refresh[_-]?token|api[_-]?key|x-api-key|token|"
    r"password|passwd|secret|credential|credentials|auth)=)([^&#\s]+)"
)
_LITEPAN_TOKEN = re.compile(r"(?i)(/t/)([^/\s?#]+)(/)")

_PIPELINE_SECRETS: tuple[object, ...] = ()
_RECORD_FACTORY_INSTALLED = False
_BASE_RECORD_FACTORY = logging.getLogRecordFactory()


def sanitize_text(value: object, secrets: Iterable[object] = ()) -> str:
    """Return ``value`` with credentials and playback tokens replaced.

    Configured literals are removed case-insensitively before pattern-based
    cleanup.  The function is intentionally deterministic and side-effect
    free, making it suitable for persisted history as well as logging.
    """
    text = "" if value is None else str(value)
    literal_secrets = sorted(
        {str(secret) for secret in secrets if secret not in (None, "")},
        key=len,
        reverse=True,
    )
    for secret in literal_secrets:
        text = re.sub(re.escape(secret), REDACTED, text, flags=re.IGNORECASE)

    text = _AUTH_VALUE.sub(rf"\1{REDACTED}", text)
    text = _COOKIE_HEADER.sub(rf"\1{REDACTED}", text)
    # A standalone Bearer value can appear without an Authorization header.
    text = _BEARER_VALUE.sub(rf"\1{REDACTED}", text)
    text = _HEADER_SECRET.sub(rf"\1{REDACTED}", text)
    text = _QUERY_SECRET.sub(rf"\1{REDACTED}", text)
    text = _LITEPAN_TOKEN.sub(rf"\1{REDACTED}\3", text)
    return text


def _pipeline_record_factory(*args, **kwargs) -> logging.LogRecord:
    """Sanitize records before any pipeline child handler can receive them."""
    record = _BASE_RECORD_FACTORY(*args, **kwargs)
    if record.name == "pipeline" or record.name.startswith("pipeline."):
        try:
            rendered = record.getMessage()
        except Exception:
            rendered = str(record.msg)
        record.msg = sanitize_text(rendered, _PIPELINE_SECRETS)
        record.args = ()
    return record


class SanitizingFilter(logging.Filter):
    """Apply :func:`sanitize_text` to a log record before formatting."""

    def __init__(self, secrets: Iterable[object] = ()):
        super().__init__()
        self.secrets = tuple(secrets)

    def filter(self, record: logging.LogRecord) -> bool:
        try:
            rendered = record.getMessage()
        except Exception:
            rendered = str(record.msg)
        record.msg = sanitize_text(rendered, self.secrets)
        record.args = ()
        return True


class SanitizingFormatter(logging.Formatter):
    """Formatter retained for callers that attach a project handler manually."""

    def __init__(self, secrets: Iterable[object] = ()):
        super().__init__("%(asctime)s %(levelname)s %(name)s: %(message)s")
        self.secrets = tuple(secrets)

    def format(self, record: logging.LogRecord) -> str:
        rendered = super().format(record)
        return sanitize_text(rendered, self.secrets)


def configure_logging(secrets: Iterable[object] = (), stream: TextIO | None = None) -> logging.Logger:
    """Install the one redacting handler for the ``pipeline`` logger.

    Existing handlers attached to ``pipeline.*`` children are removed and
    child propagation is enabled, preventing stale adapter-specific handlers
    from bypassing the project policy.  Calling this function again replaces
    the prior handler rather than duplicating output.
    """
    global _PIPELINE_SECRETS, _RECORD_FACTORY_INSTALLED
    _PIPELINE_SECRETS = tuple(secrets)
    if not _RECORD_FACTORY_INSTALLED:
        logging.setLogRecordFactory(_pipeline_record_factory)
        _RECORD_FACTORY_INSTALLED = True

    logger = logging.getLogger("pipeline")
    logger.setLevel(logging.DEBUG)
    logger.propagate = False
    for handler in list(logger.handlers):
        logger.removeHandler(handler)
        handler.close()

    redaction_filter = SanitizingFilter(secrets)
    handler = logging.StreamHandler(stream if stream is not None else sys.stderr)
    handler.setLevel(logging.DEBUG)
    handler.addFilter(redaction_filter)
    handler.setFormatter(logging.Formatter("%(levelname)s %(name)s: %(message)s"))
    logger.addHandler(handler)

    # Logger instances are created lazily.  Clean up all currently known
    # descendants; their records will then reach the one filtered handler.
    manager = logging.Logger.manager.loggerDict
    for name, child in list(manager.items()):
        if not name.startswith("pipeline.") or not isinstance(child, logging.Logger):
            continue
        for child_handler in list(child.handlers):
            child.removeHandler(child_handler)
            child_handler.close()
        child.propagate = True
    return logger


__all__ = [
    "REDACTED",
    "SanitizingFilter",
    "configure_logging",
    "sanitize_text",
]
