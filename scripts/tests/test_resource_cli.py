from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import sys
import tempfile
import types
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from pipeline import real_adapters, resource_commands
from pipeline.resource_engine import RESOURCE_KEYS
from tests.test_cli import valid_config

import run_pipeline


def good_subscription():
    return {
        "id": "private-sub-id",
        "username": "admin",
        "type": "电视剧",
        "enabled": True,
        "media_source": "tmdb",
        "media_id": 111,
        "season": 1,
        "name": "private-show-name",
        "tmdb_genre_ids": [16],
        "tmdb_seasons": [{"season_number": 1, "air_date": "2026-10-03"}],
        "tmdb_season_air_date": "2026-10-03",
    }


def resource_report(status="plan_only"):
    return {
        "status": status,
        "reason": None,
        "partial": False,
        "counts": {"created": 0, "reused": 8},
        "resources": {key: "reuse" for key in RESOURCE_KEYS},
        "blockers": [],
    }


class ResourceCliTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="v5-resource-cli-")
        self.root = Path(self.tmp.name)
        self.config_path = self.root / "config.json"
        self.config_path.write_text(
            json.dumps(valid_config(f"/test-fixtures/{self.root.name}/state")),
            encoding="utf-8",
        )

    def tearDown(self):
        self.tmp.cleanup()

    def run_cli(self, *argv):
        stdout, stderr = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
            code = run_pipeline.main(list(argv))
        return code, stdout.getvalue(), stderr.getvalue()

    def test_qualify_uses_only_moviepilot_and_returns_safe_qualification(self):
        captured = {}

        class FakeMoviePilotClient:
            def __init__(self, base_url, **kwargs):
                captured["base_url"] = base_url
                captured.update(kwargs)

            def list_subscriptions(self):
                return [good_subscription()]

        with mock.patch.dict(os.environ, {"MOVIEPILOT_TOKEN": "secret-token"}), \
             mock.patch.object(real_adapters, "MoviePilotClient", FakeMoviePilotClient), \
             mock.patch.object(resource_commands, "_current_quarter", return_value="2026-10"), \
             mock.patch.object(run_pipeline, "build_runtime", side_effect=AssertionError), \
             mock.patch.object(run_pipeline, "_open_readonly", side_effect=AssertionError), \
             mock.patch.object(run_pipeline, "_open_writable", side_effect=AssertionError):
            code, out, _err = self.run_cli("qualify", "--config", str(self.config_path))

        report = json.loads(out)
        self.assertEqual(code, 0)
        self.assertEqual(report["command"], "qualify")
        self.assertEqual(report["status"], "ok")
        self.assertEqual(report["eligible"][0]["media_id"], "111")
        self.assertEqual(captured["auth_mode"], "x-api-key")
        self.assertEqual(captured["runtime_config"].services.moviepilot.base_url,
                         "https://moviepilot.example.invalid")
        self.assertEqual(captured["token"], "secret-token")
        self.assertNotIn("secret-token", out)
        self.assertNotIn("private-show-name", out)
        self.assertNotIn("private-sub-id", out)

    def test_qualify_snapshot_failure_reports_unknown_counts_not_zero(self):
        class FailingMoviePilotClient:
            def __init__(self, *_args, **_kwargs):
                pass

            def list_subscriptions(self):
                raise RuntimeError("https://private.invalid/?token=secret-token")

        with mock.patch.dict(os.environ, {"MOVIEPILOT_TOKEN": "secret-token"}), \
             mock.patch.object(real_adapters, "MoviePilotClient", FailingMoviePilotClient):
            code, out, _err = self.run_cli("qualify", "--config", str(self.config_path))
        report = json.loads(out)
        self.assertEqual(code, 2)
        self.assertEqual(report["status"], "blocked")
        self.assertEqual(report["reason"], "subscription_snapshot_failed")
        self.assertIsNone(report["counts"]["api_total"])
        self.assertIsNone(report["counts"]["eligible"])
        self.assertNotIn("secret-token", out)
        self.assertNotIn("private.invalid", out)

    def test_missing_moviepilot_secret_does_not_construct_client_or_claim_empty_snapshot(self):
        with mock.patch.dict(os.environ, {}, clear=True), \
             mock.patch.object(real_adapters, "MoviePilotClient") as client:
            code, out, _err = self.run_cli("qualify", "--config", str(self.config_path))
        report = json.loads(out)
        self.assertEqual(code, 2)
        self.assertEqual(report["reason"], "moviepilot_secret_missing")
        self.assertIsNone(report["counts"]["api_total"])
        client.assert_not_called()

    def test_resource_commands_route_to_runtime_with_correct_apply_flag(self):
        calls = []

        def run_resource_preparation(raw, config, *, config_path, apply):
            calls.append((raw, config, config_path, apply))
            response = resource_report("prepared" if apply else "plan_only")
            response["untrusted_private_value"] = "https://secret.invalid/token"
            return response

        runtime_module = types.ModuleType("pipeline.resource_runtime")
        runtime_module.run_resource_preparation = run_resource_preparation
        with mock.patch.dict(sys.modules, {"pipeline.resource_runtime": runtime_module}):
            plan_code, plan_out, _plan_err = self.run_cli(
                "plan-resources", "--config", str(self.config_path)
            )
            apply_code, apply_out, _apply_err = self.run_cli(
                "prepare-resources", "--apply", "--config", str(self.config_path)
            )

        plan = json.loads(plan_out)
        prepared = json.loads(apply_out)
        self.assertEqual(plan_code, 0)
        self.assertEqual(plan["command"], "plan-resources")
        self.assertEqual(plan["status"], "plan_only")
        self.assertEqual(apply_code, 0)
        self.assertEqual(prepared["command"], "prepare-resources")
        self.assertEqual(prepared["status"], "prepared")
        self.assertEqual([call[3] for call in calls], [False, True])
        self.assertEqual([call[2] for call in calls], [str(self.config_path)] * 2)
        self.assertNotIn("untrusted_private_value", plan_out + apply_out)
        self.assertNotIn("secret.invalid", plan_out + apply_out)

    def test_prepare_resources_requires_explicit_apply_flag(self):
        runtime_module = types.ModuleType("pipeline.resource_runtime")
        runtime_module.run_resource_preparation = mock.Mock()
        with mock.patch.dict(sys.modules, {"pipeline.resource_runtime": runtime_module}):
            code, out, err = self.run_cli(
                "prepare-resources", "--config", str(self.config_path)
            )
        self.assertEqual(code, 2)
        self.assertEqual(out, "")
        self.assertEqual(err.strip(), "error: invalid command-line arguments")
        runtime_module.run_resource_preparation.assert_not_called()

    def test_resource_projection_keeps_only_reviewable_targets_and_seven_mapping_fields(self):
        report = resource_report()
        report.update(quarter="2026-10", targets={"cloud_source": "/cloud/2026-10/未整理"},
                      config_changes=[{"field": "paths.cloud_source_root", "before": "/cloud/2026-07/未整理", "after": "/cloud/2026-10/未整理"}],
                      qualification_counts={"api_total": 123, "eligible": 1},
                      resource_reasons={"cloud_source": "cloud_directory_missing"},
                      credential="private-not-for-output")
        result = run_pipeline._safe_resource_report(report)
        self.assertEqual(result["quarter"], "2026-10")
        self.assertEqual(result["config_changes"], report["config_changes"])
        self.assertEqual(result["qualification_counts"]["api_total"], 123)
        self.assertNotIn("credential", result)
        for field in ("paths.cloud_library_root", "paths.litepan_publish_subdir"):
            report["config_changes"][0]["field"] = field
            self.assertEqual(run_pipeline._safe_resource_report(report)["reason"], "resource_result_invalid")

    def test_resource_projection_blocks_urls_and_arbitrary_reason_text(self):
        for extra in (
            {"targets": {"cloud_source": "https://private.invalid/token"}},
            {"resource_reasons": {"cloud_source": "private text"}},
            {"qualification_counts": {"eligible": True}},
        ):
            self.assertEqual(run_pipeline._safe_resource_report({**resource_report(), **extra})["status"], "blocked")

    def test_resource_revision_projection_is_strict_and_safe(self):
        for revision in ("a" * 64, None):
            result = run_pipeline._safe_resource_report({**resource_report(), "config_revision": revision})
            self.assertEqual(result["config_revision"], revision)
        for revision in ("A" * 64, "a" * 63, 123, "https://private.invalid/token", "a" * 64 + "\n"):
            result = run_pipeline._safe_resource_report({**resource_report(), "config_revision": revision})
            self.assertEqual(result["reason"], "resource_result_invalid")

    def test_existing_cli_commands_remain_registered(self):
        parser = run_pipeline._parser()
        self.assertIn("run", parser._subparsers._group_actions[0].choices)
        self.assertIn("dry-run", parser._subparsers._group_actions[0].choices)
        self.assertIn("preflight", parser._subparsers._group_actions[0].choices)



if __name__ == "__main__":
    unittest.main(verbosity=2)
