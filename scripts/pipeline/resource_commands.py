"""Read-only subscription qualification command helpers for V5."""
from __future__ import annotations

from datetime import datetime
import os
from typing import Any
from zoneinfo import ZoneInfo


_COUNT_FIELDS = (
    "api_total", "source_pass", "source_excluded", "disabled", "non_tv",
    "non_anime", "cross_quarter", "unknown", "duplicates", "eligible",
)
_ALLOWED_USERNAMES = ("admin", "Bangumi订阅")


def qualify_current_subscriptions(config: Any) -> dict[str, Any]:
    """Read and qualify the complete MoviePilot snapshot for the current quarter.

    The command intentionally constructs only MoviePilotClient.  It does not
    build discovery providers, inspect resources, open the V5 state database,
    or instantiate cloud/LitePan/Emby adapters.
    """

    try:
        quarter = _current_quarter()
        service = config.services.moviepilot
        secret = os.environ.get(service.secret_reference or "", "")
        if not secret:
            return _blocked("moviepilot_secret_missing")

        from .real_adapters import (
            MoviePilotClient, _moviepilot_success_code, _moviepilot_success_flag,
        )
        from .eligibility import qualify_subscriptions

        class QualificationClient(MoviePilotClient):
            def _call(self, method, path, params=None, body=None, timeout=None):
                status, payload = super()._call(
                    method, path, params=params, body=body, timeout=timeout,
                )
                if isinstance(payload, dict):
                    if (
                        ("success" in payload and not _moviepilot_success_flag(payload["success"]))
                        or ("code" in payload and not _moviepilot_success_code(payload["code"]))
                    ):
                        raise ValueError("moviepilot_response_failed")
                    if path == "/api/v1/subscribe/":
                        _require_complete_snapshot(payload)
                return status, payload

        client = QualificationClient(
            service.base_url,
            token=secret,
            auth_mode=service.auth_mode,
            runtime_config=config,
        )
        # Qualification has a fixed source policy independent of discovery's
        # configured source filter. Both allowed users need detail evidence.
        client.subscription_sources = _ALLOWED_USERNAMES
        rows = client.list_subscriptions()
    except Exception:
        return _blocked("subscription_snapshot_failed")

    try:
        return qualify_subscriptions(
            rows,
            quarter=quarter,
            allowed_usernames=_ALLOWED_USERNAMES,
        )
    except Exception:
        return _blocked("subscription_qualification_failed")


def _require_complete_snapshot(payload: dict[str, Any]) -> None:
    """Reject pagination that cannot prove the returned list is complete."""
    entries = payload.get("data")
    if not isinstance(entries, list):
        raise ValueError("subscription_snapshot_invalid")
    total = payload.get("total")
    if "total" in payload:
        if isinstance(total, bool) or not isinstance(total, int) or total != len(entries):
            raise ValueError("subscription_snapshot_incomplete")
    elif "page" in payload or "page_size" in payload:
        raise ValueError("subscription_snapshot_completeness_unknown")
    if "page" in payload:
        page = payload["page"]
        if isinstance(page, bool) or not isinstance(page, int) or page != 1:
            raise ValueError("subscription_snapshot_page_invalid")
    if "page_size" in payload:
        size = payload["page_size"]
        if isinstance(size, bool) or not isinstance(size, int) or size < max(1, len(entries)):
            raise ValueError("subscription_snapshot_page_size_invalid")


def _current_quarter() -> str:
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    quarter_month = ((now.month - 1) // 3) * 3 + 1
    return f"{now.year:04d}-{quarter_month:02d}"


def _blocked(reason: str) -> dict[str, Any]:
    return {
        "status": "blocked",
        "reason": reason,
        "counts": {field: None for field in _COUNT_FIELDS},
        "eligible": [],
        "issues": [{"reason": reason}],
    }


__all__ = ["qualify_current_subscriptions"]
