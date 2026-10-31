"""Task 11 RED tests for the V5 command-line boundary."""
from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import sqlite3
import sys
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline.models import ReviewReason
from pipeline.state import StateStore

import run_pipeline


def valid_config(state_directory: str) -> dict:
    return {
        "schema_version": 5,
        "services": {
            "moviepilot": {
                "name": "moviepilot",
                "base_url": "https://moviepilot.example.invalid",
                "token_env": "MOVIEPILOT_TOKEN",
                "media_source": "tmdb",
            },
            "cloud": {
                "name": "cloud",
                "credential_env": "CLOUD_COOKIE",
            },
            "litepan": {
                "name": "litepan",
                "base_url": "https://litepan.example.invalid",
                "api_key_env": "LITEPAN_API_KEY",
                "task_name": "anime-strm",
            },
            "emby": {
                "name": "emby",
                "base_url": "https://emby.example.invalid",
                "api_key_env": "EMBY_API_KEY",
                "library_name": "Anime",
            },
        },
        "paths": {
            "cloud_source_root": "/cloud/source",
            "cloud_library_root": "/cloud/library",
            "litepan_output_root": "/litepan/output",
            "moviepilot_mount_root": "/moviepilot/media",
            "emby_mount_root": "/emby/media",
            "state_directory": state_directory,
        },
        "scoring": {
            "include_weight": 10,
            "language_weight": 5,
            "release_group_weight": 3,
            "resolution_weight": 2,
            "collection_penalty": 4,
        },
        "limits": {
            "network_timeout_seconds": 30,
            "max_read_retries": 3,
            "max_task_pages": 20,
            "max_recursive_depth": 8,
            "max_recursive_files": 1000,
            "max_new_tasks_per_run": 4,
            "max_new_tasks_per_subscription": 2,
            "max_review_batch": 50,
            "max_reconcile_batch": 50,
        },
        "retry": {
            "max_attempts": 3,
            "backoff_seconds": 1,
            "backoff_max_seconds": 30,
        },
        "runtime": {
            "litepan_task_name": "anime-strm",
            "emby_library_name": "Anime",
            "reconcile_batch": 50,
        },
        "schedule": {"timezone": "Asia/Shanghai"},
    }


class FakeMoviePilot:
    def __init__(self, subscriptions=None):
        self.subscriptions = list(subscriptions or [])
        self.calls = 0

    def list_subscriptions(self):
        self.calls += 1
        return list(self.subscriptions)


class CliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="task11-cli-")
        # ConfigSchema intentionally accepts deployment-style absolute POSIX
        # roots.  The CLI state path is injected below so the fixture uses the
        # native temporary directory on every drive/OS.
        self.state_root = Path(self.tmp.name) / "state"
        name = Path(self.tmp.name).name
        self.state_dir = f"/test-fixtures/{name}/state"
        self.config_path = Path(self.tmp.name) / "config.json"
        self.config_path.write_text(
            json.dumps(valid_config(self.state_dir)), encoding="utf-8"
        )

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *argv):
        stdout, stderr = io.StringIO(), io.StringIO()
        native_state = self.state_root / "state.db"
        with mock.patch.object(run_pipeline, "_state_path", return_value=str(native_state)):
            with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
                code = run_pipeline.main(list(argv))
        return code, stdout.getvalue(), stderr.getvalue()

    def test_dry_run_does_not_create_state_database_or_call_writes(self):
        runtime = run_pipeline.RuntimeBundle(
            moviepilot=FakeMoviePilot(), providers=(), services={}
        )
        with mock.patch.object(run_pipeline, "build_runtime", return_value=runtime):
            code, out, err = self.run_cli(
                "dry-run", "--config", str(self.config_path)
            )
        state = self.state_root
        self.assertEqual(code, 0)
        self.assertFalse((state / "state.db").exists())
        self.assertFalse((state / "state.db-wal").exists())
        self.assertFalse((state / "state.db-shm").exists())
        self.assertNotIn("example.invalid", out + err)

    def test_invalid_config_fails_before_state_creation(self):
        invalid = json.loads(self.config_path.read_text(encoding="utf-8"))
        invalid["schema_version"] = 4
        self.config_path.write_text(json.dumps(invalid), encoding="utf-8")
        code, _out, err = self.run_cli(
            "status", "--config", str(self.config_path)
        )
        state = self.state_root
        self.assertEqual(code, 2)
        self.assertIn("schema_version", err)
        self.assertFalse(state.exists())

    def test_invalid_argv_is_static_and_does_not_echo_secret(self):
        secret = "Bearer-argv-secret-value"
        code, out, err = self.run_cli(
            "status", "--config", str(self.config_path), "--unknown", secret
        )
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertNotIn(secret, err)
        self.assertEqual(err.strip(), "error: invalid command-line arguments")

    def test_reviews_resolve_requires_reason_specific_resolution(self):
        state = self.state_root
        store = StateStore(state / "state.db")
        try:
            episode_id = store.get_or_create_episode(
                {
                    "media_source": "tmdb",
                    "media_id": "111",
                    "season": 1,
                    "episode": 4,
                    "canonical_work": "Demo",
                    "subscription_id": "sub-1",
                }
            )
            store.open_review(episode_id, ReviewReason.MULTIPLE_VIDEO_FILES, {})
            before = store.get_review(episode_id)
            code, _out, err = self.run_cli(
                "reviews", "resolve", "--config", str(self.config_path),
                "--id", str(episode_id), "--resolution", "{}"
            )
            after = store.get_review(episode_id)
            self.assertEqual(code, 2)
            self.assertIn("selected_file_id", err)
            self.assertEqual(before["review_generation"], after["review_generation"])
            self.assertIsNone(after["review_resolved_at"])
        finally:
            store.close()

    def test_config_migrate_writes_subscription_free_config_and_report(self):
        legacy = json.loads(self.config_path.read_text(encoding="utf-8"))
        services = legacy.pop("services")
        legacy["schema_version"] = 3
        legacy["moviepilot"] = services["moviepilot"]
        legacy["p115"] = services["cloud"]
        legacy["litepan"] = services["litepan"]
        legacy["emby"] = services["emby"]
        legacy["subscriptions"] = [{"media_id": "295071", "season": 1}]
        output = Path(self.tmp.name) / "v5.json"
        report = Path(self.tmp.name) / "report.json"
        self.config_path.write_text(json.dumps(legacy), encoding="utf-8")
        code, _out, err = self.run_cli(
            "config-migrate", "--legacy", str(self.config_path),
            "--output", str(output), "--report", str(report)
        )
        self.assertEqual(code, 0, err)
        self.assertNotIn("subscriptions", json.loads(output.read_text()))
        report_data = json.loads(report.read_text())
        self.assertIn("moviepilot_subscriptions_to_verify", report_data)

    def test_config_migrate_rejects_output_report_alias_without_mutation(self):
        legacy = json.loads(self.config_path.read_text(encoding="utf-8"))
        legacy["schema_version"] = 3
        legacy["moviepilot"] = legacy.pop("services")["moviepilot"]
        legacy_path = Path(self.tmp.name) / "legacy.json"
        legacy_path.write_text(json.dumps(legacy), encoding="utf-8")
        alias = Path(self.tmp.name) / "same.json"
        original = "do-not-overwrite"
        alias.write_text(original, encoding="utf-8")
        code, out, err = self.run_cli(
            "config-migrate", "--legacy", str(legacy_path),
            "--output", str(alias), "--report", str(alias / ".." / alias.name),
        )
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertIn("output and report", err)
        self.assertEqual(alias.read_text(encoding="utf-8"), original)

    def test_atomic_json_rejects_hardlink_target_without_mutation(self):
        victim = Path(self.tmp.name) / "victim.json"
        target = Path(self.tmp.name) / "output.json"
        victim.write_text("victim-content", encoding="utf-8")
        try:
            os.link(victim, target)
        except (AttributeError, OSError) as exc:
            self.skipTest(f"hardlinks unavailable: {exc}")
        with self.assertRaises(run_pipeline.CliError):
            run_pipeline._write_json_atomic(str(target), {"safe": True})
        self.assertEqual(victim.read_text(encoding="utf-8"), "victim-content")
        self.assertEqual(target.read_text(encoding="utf-8"), "victim-content")

    def test_readonly_commands_preserve_all_database_bytes_with_and_without_live_wal(self):
        state = self.state_root
        state.mkdir(parents=True)
        db = state / "state.db"
        store = StateStore(db)
        store.close()
        for live_wal in (False, True):
            with self.subTest(live_wal=live_wal):
                writer = StateStore(db) if live_wal else None
                try:
                    if writer:
                        writer.get_or_create_episode({
                            "media_source": "tmdb", "media_id": "wal-only",
                            "season": 1, "episode": 1, "canonical_work": "WAL",
                        })
                    before = {p.name: p.read_bytes() for p in state.iterdir()}
                    for args in (("status",), ("audit",), ("reviews", "list"), ("migrate", "--check-only")):
                        code, out, err = self.run_cli(*args, "--config", str(self.config_path))
                        self.assertEqual(code, 0, err)
                        self.assertEqual({p.name: p.read_bytes() for p in state.iterdir()}, before)
                    if writer:
                        connection = run_pipeline._readonly_connection(str(db))
                        try:
                            self.assertEqual(connection.execute(
                                "SELECT media_id FROM episodes_v5"
                            ).fetchone()[0], "wal-only")
                        finally:
                            connection.close()
                        self.assertEqual({p.name: p.read_bytes() for p in state.iterdir()}, before)
                finally:
                    if writer:
                        writer.close()


if __name__ == "__main__":
    unittest.main()
