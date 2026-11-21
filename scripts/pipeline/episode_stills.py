"""Backfill missing episode stills one day after airing.

The scraper can emit an episode `.nfo` without the still image when the
episode has just aired and TMDB has no `still_path` yet.  This module fills
that gap on a later run: it only touches episodes that are already
`library_ready`, only writes a missing `<stem>.jpg`, and only when the
episode aired before today (Asia/Shanghai).  Nothing is overwritten and every
failure is contained to the single episode.
"""

from __future__ import annotations

import datetime
import json
import os
import posixpath
import tempfile
import urllib.parse
import urllib.request
from pathlib import Path

MAX_PER_RUN = 20
TIMEOUT_SECONDS = 20
IMAGE_BASE = "https://image.tmdb.org/t/p/original"


def _now_date() -> datetime.date:
    return datetime.datetime.now(datetime.timezone(datetime.timedelta(hours=8))).date()


def _load_tmdb_secret(state_directory: str) -> dict:
    path = Path(state_directory).parent / "config" / "bootstrap-secrets" / "tmdb.json"
    try:
        data = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return {}
    return data if isinstance(data, dict) else {}


def _opener(proxy_url: str | None):
    if proxy_url:
        handler = urllib.request.ProxyHandler({"http": proxy_url, "https": proxy_url})
        return urllib.request.build_opener(handler)
    return urllib.request.build_opener()


def _get_json(opener, url: str) -> dict | None:
    try:
        with opener.open(url, timeout=TIMEOUT_SECONDS) as response:
            payload = json.loads(response.read().decode("utf-8"))
    except (OSError, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def _download_image(opener, url: str, target: str) -> bool:
    try:
        with opener.open(url, timeout=TIMEOUT_SECONDS) as response:
            content = response.read()
    except OSError:
        return False
    if not content:
        return False
    directory = os.path.dirname(target)
    fd, tmp = tempfile.mkstemp(prefix=".still-", dir=directory)
    try:
        with os.fdopen(fd, "wb") as handle:
            handle.write(content)
        os.chmod(tmp, 0o644)
        os.replace(tmp, target)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        return False
    return True


def backfill_episode_stills(store, state_directory: str) -> dict:
    """Return a bounded report of what was filled and what was skipped."""
    report = {"checked": 0, "filled": [], "deferred_today": [], "no_still": [],
              "failed": [], "skipped_existing": 0, "disabled": False}
    secret = _load_tmdb_secret(state_directory)
    api_key = str(secret.get("api_key") or "").strip()
    if not api_key:
        report["disabled"] = True
        return report
    opener = _opener(str(secret.get("proxy") or os.environ.get("HTTPS_PROXY") or "").strip() or None)
    today = _now_date().isoformat()
    rows = store.conn.execute(
        "SELECT id, media_id, season, episode, strm_path FROM episodes_v5 "
        "WHERE status = 'library_ready' AND strm_path IS NOT NULL ORDER BY id"
    ).fetchall()
    for row in rows:
        episode_id, media_id, season, episode, strm_path = row
        if report["checked"] >= MAX_PER_RUN:
            break
        if not strm_path or not str(strm_path).lower().endswith(".strm"):
            continue
        stem = str(strm_path)[:-5]
        target = stem + ".jpg"
        try:
            if os.path.getsize(target) > 0:
                report["skipped_existing"] += 1
                continue
        except OSError:
            pass
        if not os.path.isfile(str(strm_path)):
            continue
        report["checked"] += 1
        meta_url = (
            f"https://api.themoviedb.org/3/tv/{urllib.parse.quote(str(media_id))}"
            f"/season/{int(season)}/episode/{int(episode)}?api_key={urllib.parse.quote(api_key)}"
        )
        meta = _get_json(opener, meta_url)
        aired = str((meta or {}).get("air_date") or "")
        still = str((meta or {}).get("still_path") or "")
        if aired and aired >= today:
            report["deferred_today"].append(episode_id)
            continue
        if not still:
            report["no_still"].append(episode_id)
            continue
        if not _download_image(opener, IMAGE_BASE + still, target):
            report["failed"].append(episode_id)
            continue
        report["filled"].append(episode_id)
    return report
