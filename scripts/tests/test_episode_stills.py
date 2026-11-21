import json
import os
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from pipeline import episode_stills


class _Store:
    def __init__(self, rows):
        self.conn = sqlite3.connect(":memory:")
        self.conn.execute(
            "CREATE TABLE episodes_v5 (id INTEGER, media_id TEXT, season INTEGER, "
            "episode INTEGER, strm_path TEXT, status TEXT)"
        )
        for row in rows:
            self.conn.execute(
                "INSERT INTO episodes_v5 VALUES (?,?,?,?,?,?)", (*row, "library_ready")
            )


def _layout(tmp_path):
    state = tmp_path / "state"
    state.mkdir()
    secret_dir = tmp_path / "config" / "bootstrap-secrets"
    secret_dir.mkdir(parents=True)
    return state


def test_disabled_without_secret(tmp_path):
    state = _layout(tmp_path)
    report = episode_stills.backfill_episode_stills(_Store([]), str(state))
    assert report["disabled"] is True


def test_existing_still_is_never_overwritten(tmp_path):
    state = _layout(tmp_path)
    (tmp_path / "config" / "bootstrap-secrets" / "tmdb.json").write_text(
        json.dumps({"api_key": "x"}), encoding="utf-8"
    )
    season = tmp_path / "media" / "Season 1"
    season.mkdir(parents=True)
    strm = season / "Show - S01E01.strm"
    strm.write_text("http://example/1", encoding="utf-8")
    jpg = season / "Show - S01E01.jpg"
    jpg.write_bytes(b"original")
    report = episode_stills.backfill_episode_stills(
        _Store([(1, "100", 1, 1, str(strm))]), str(state)
    )
    assert report["skipped_existing"] == 1
    assert jpg.read_bytes() == b"original"


def test_today_airing_defers_to_next_day(tmp_path, monkeypatch):
    state = _layout(tmp_path)
    (tmp_path / "config" / "bootstrap-secrets" / "tmdb.json").write_text(
        json.dumps({"api_key": "x"}), encoding="utf-8"
    )
    season = tmp_path / "media" / "Season 1"
    season.mkdir(parents=True)
    strm = season / "Show - S01E01.strm"
    strm.write_text("http://example/1", encoding="utf-8")
    today = episode_stills._now_date().isoformat()
    monkeypatch.setattr(
        episode_stills, "_get_json",
        lambda opener, url: {"air_date": today, "still_path": "/a.jpg"},
    )
    called = []
    monkeypatch.setattr(
        episode_stills, "_download_image",
        lambda opener, url, target: called.append(url) or True,
    )
    report = episode_stills.backfill_episode_stills(
        _Store([(2, "100", 1, 1, str(strm))]), str(state)
    )
    assert report["deferred_today"] == [2]
    assert called == []


def test_past_airing_is_filled(tmp_path, monkeypatch):
    state = _layout(tmp_path)
    (tmp_path / "config" / "bootstrap-secrets" / "tmdb.json").write_text(
        json.dumps({"api_key": "x"}), encoding="utf-8"
    )
    season = tmp_path / "media" / "Season 1"
    season.mkdir(parents=True)
    strm = season / "Show - S01E01.strm"
    strm.write_text("http://example/1", encoding="utf-8")
    monkeypatch.setattr(
        episode_stills, "_get_json",
        lambda opener, url: {"air_date": "2020-01-01", "still_path": "/a.jpg"},
    )

    def _fake_download(opener, url, target):
        Path(target).parent.mkdir(parents=True, exist_ok=True)
        Path(target).write_bytes(b"still")
        return True

    monkeypatch.setattr(episode_stills, "_download_image", _fake_download)
    report = episode_stills.backfill_episode_stills(
        _Store([(3, "100", 1, 1, str(strm))]), str(state)
    )
    assert report["filled"] == [3]
    assert (season / "Show - S01E01.jpg").read_bytes() == b"still"
