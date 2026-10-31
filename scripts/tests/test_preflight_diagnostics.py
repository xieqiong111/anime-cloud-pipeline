"""Regression coverage for secret-safe, bounded V5 read-only diagnostics."""
from __future__ import annotations

import contextlib
import io
import json
import os
from pathlib import Path
import signal
import sys
import subprocess
import tempfile
import unittest
from unittest import mock

ROOT = Path(__file__).resolve().parents[1]
TESTS = Path(__file__).resolve().parent
for entry in (str(ROOT), str(TESTS)):
    if entry not in sys.path:
        sys.path.insert(0, entry)

from pipeline import diagnostics
from pipeline.adapters import RemoteDirectoryMissing, RemoteInventoryIncomplete
from pipeline.diagnostics import ReadOnlyDeadline, request_timeout, stage
from pipeline.http import HttpError
from pipeline.real_adapters import EmbyClient, LitePanClient, P115CloudAdapter

import run_pipeline
from test_cli import valid_config


QUARTER = "2026-10月新番"
SYNTHETIC_SECRET = "synthetic-secret-marker"
SYNTHETIC_URL = "https://redaction.invalid/path?token=synthetic-secret-marker"
ENV = {
    "MOVIEPILOT_TOKEN": "test-moviepilot-secret",
    "CLOUD_COOKIE": "test-cloud-secret",
    "LITEPAN_API_KEY": "test-litepan-secret",
    "EMBY_API_KEY": "test-emby-secret",
    "P115_ROOT_DIRECTORY_ID": "synthetic-root-id",
}


def config_mapping(*, quarter: str | None = QUARTER, include_litepan_paths: bool = False):
    """Build a local fixture based on test_cli.valid_config, with no real endpoints."""
    raw = valid_config("/state")
    raw["services"]["cloud"]["root_directory_path"] = "/cloud"
    raw["source"] = {
        "name": "fixture-source",
        "kind": "kisssub",
        "base_url": "https://source.example.invalid",
    }
    raw["paths"]["litepan_raw_output_root"] = "/litepan/raw"
    runtime = raw["runtime"]
    if include_litepan_paths:
        runtime.update({
            "litepan_task_db_path": "/fixture/litepan/tasks.db",
            "litepan_strm_host_root": "/fixture/litepan/strm",
        })
    if quarter is not None:
        runtime["litepan_task_source_root"] = f"/cloud/library/{quarter}"
        runtime["litepan_task_output_root"] = f"/litepan/raw/{quarter}"
        runtime["emby_library_root"] = f"/emby/media/{quarter}"
    return raw


def adapter_stubs(*, p115_resolver=None, litepan_error=None, emby_error=None):
    """Return adapter subclasses so static capability checks remain realistic."""
    resolver = p115_resolver or (lambda _path: "synthetic-directory-id")

    class StubP115(P115CloudAdapter):
        def __init__(self, *_args, **_kwargs):
            self.resolved_paths = []

        def validate_runtime_capabilities(self):
            return None

        def resolve_source_directory(self, path):
            self.resolved_paths.append(path)
            return resolver(path)

    class StubLitePan(LitePanClient):
        def __init__(self, *_args, **_kwargs):
            pass

        def list_tasks(self):
            if litepan_error is not None:
                raise litepan_error
            return []

        def assert_event_targets_task(self, *_args, **_kwargs):
            return None

    class StubEmby(EmbyClient):
        def __init__(self, *_args, **_kwargs):
            pass

        def find_library(self, *_args, **_kwargs):
            if emby_error is not None:
                raise emby_error
            return {"Id": "synthetic-library-id"}

    return StubP115, StubLitePan, StubEmby


class PreflightDiagnosticTests(unittest.TestCase):
    def run_preflight(self, raw, *, p115_resolver=None, litepan_error=None, emby_error=None):
        p115_cls, litepan_cls, emby_cls = adapter_stubs(
            p115_resolver=p115_resolver,
            litepan_error=litepan_error,
            emby_error=emby_error,
        )
        with tempfile.TemporaryDirectory(prefix="preflight-diagnostic-") as temporary:
            config_path = Path(temporary) / "config.json"
            config_path.write_text(json.dumps(raw), encoding="utf-8")
            parsed_raw, parsed_config = run_pipeline._config_mapping(str(config_path))
            stdout, stderr = io.StringIO(), io.StringIO()
            with (
                mock.patch.dict(os.environ, ENV, clear=True),
                mock.patch.object(run_pipeline, "_window", return_value={"season_quarter": "2026-10"}),
                mock.patch("pipeline.real_adapters.P115CloudAdapter", p115_cls),
                mock.patch("pipeline.real_adapters.LitePanClient", litepan_cls),
                mock.patch("pipeline.real_adapters.EmbyClient", emby_cls),
                contextlib.redirect_stdout(stdout),
                contextlib.redirect_stderr(stderr),
            ):
                code = run_pipeline.main(["preflight", "--config", str(config_path)])
            return code, json.loads(stdout.getvalue()), stderr.getvalue(), parsed_raw, parsed_config

    @staticmethod
    def diagnostics_for(payload, capability):
        return [
            item for item in payload.get("capability_diagnostics", [])
            if item.get("capability") == capability
        ]

    def test_quarter_mismatch_survives_raw_parse_and_gets_fixed_reason(self):
        raw = config_mapping(quarter="2026-07月新番")
        code, payload, _stderr, parsed_raw, parsed = self.run_preflight(raw)

        self.assertEqual(code, 2)
        for field in ("litepan_task_source_root", "emby_library_root"):
            self.assertEqual(parsed_raw["runtime"][field], raw["runtime"][field])
            self.assertEqual(getattr(parsed.runtime, field), raw["runtime"][field])
            self.assertEqual(parsed.to_mapping()["runtime"][field], raw["runtime"][field])
            rows = self.diagnostics_for(payload, f"runtime.{field}")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["stage"], "quarter_mapping")
            self.assertEqual(rows[0]["reason"], "quarter_mapping_mismatch")

    def test_missing_quarter_fields_are_not_configured(self):
        raw = config_mapping(quarter=None)
        code, payload, _stderr, parsed_raw, parsed = self.run_preflight(raw)

        self.assertEqual(code, 2)
        for field in ("litepan_task_source_root", "emby_library_root"):
            self.assertNotIn(field, parsed_raw["runtime"])
            self.assertIsNone(getattr(parsed.runtime, field))
            rows = self.diagnostics_for(payload, f"runtime.{field}")
            self.assertEqual(len(rows), 1)
            self.assertEqual(rows[0]["stage"], "quarter_mapping")
            self.assertEqual(rows[0]["reason"], "not_configured")

    def test_complete_source_inventory_but_missing_quarter_keeps_legacy_capability(self):
        resolved = []

        def resolver(path):
            resolved.append(path)
            if path.endswith("/" + QUARTER):
                raise RemoteDirectoryMissing("absent from a complete synthetic inventory", kind="directories")
            return "synthetic-directory-id"

        code, payload, _stderr, _raw, _parsed = self.run_preflight(
            config_mapping(), p115_resolver=resolver
        )

        self.assertEqual(code, 2)
        self.assertEqual(resolved, ["/cloud/source", f"/cloud/library/{QUARTER}"])
        self.assertIn("services.cloud.source_inventory", payload["missing_capabilities"])
        row = self.diagnostics_for(payload, "services.cloud.source_inventory")
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["stage"], "115.quarter_library_root")
        self.assertEqual(row[0]["reason"], "not_found_in_complete_inventory")

    def test_unknown_source_inventory_exception_is_not_missing_and_is_redacted(self):
        def resolver(_path):
            raise RuntimeError(f"opaque adapter failure {SYNTHETIC_SECRET} {SYNTHETIC_URL}")

        code, payload, stderr, _raw, _parsed = self.run_preflight(
            config_mapping(), p115_resolver=resolver
        )

        self.assertEqual(code, 2)
        self.assertIn("services.cloud.source_inventory", payload["missing_capabilities"])
        row = self.diagnostics_for(payload, "services.cloud.source_inventory")
        self.assertEqual(len(row), 1)
        self.assertEqual(row[0]["reason"], "adapter_error")
        self.assertNotEqual(row[0]["reason"], "not_found_in_complete_inventory")
        rendered = json.dumps(payload) + stderr
        self.assertNotIn(SYNTHETIC_SECRET, rendered)
        self.assertNotIn(SYNTHETIC_URL, rendered)
        self.assertNotIn("opaque adapter failure", rendered)

    def test_authentication_expiry_keeps_existing_auth_capability_and_reason(self):
        def resolver(_path):
            raise RemoteInventoryIncomplete("115 authentication expired", kind="directories")

        code, payload, _stderr, _raw, _parsed = self.run_preflight(
            config_mapping(), p115_resolver=resolver
        )

        self.assertEqual(code, 2)
        self.assertIn("services.cloud.authentication_expired", payload["missing_capabilities"])
        self.assertNotIn("services.cloud.source_inventory", payload["missing_capabilities"])
        row = self.diagnostics_for(payload, "services.cloud.authentication_expired")
        self.assertEqual(row[0]["reason"], "authentication_expired")

    def test_litepan_and_emby_query_failures_have_safe_categories(self):
        litepan_code, litepan_payload, _stderr, _raw, _parsed = self.run_preflight(
            config_mapping(include_litepan_paths=True),
            litepan_error=HttpError(503, "synthetic LitePan outage"),
        )
        self.assertEqual(litepan_code, 2)
        litepan_row = self.diagnostics_for(litepan_payload, "runtime.litepan_task_inventory")
        self.assertEqual(litepan_row[0]["stage"], "litepan.tasks")
        self.assertEqual(litepan_row[0]["reason"], "query_failed")

        emby_code, emby_payload, _stderr, _raw, _parsed = self.run_preflight(
            config_mapping(),
            emby_error=HttpError(403, f"synthetic Emby rejection {SYNTHETIC_SECRET}"),
        )
        self.assertEqual(emby_code, 2)
        emby_row = self.diagnostics_for(emby_payload, "runtime.emby_quarter_library")
        self.assertEqual(emby_row[0]["stage"], "emby.library")
        self.assertEqual(emby_row[0]["reason"], "authentication_rejected")
        self.assertNotIn(SYNTHETIC_SECRET, json.dumps(emby_payload))

    def test_read_only_deadline_returns_blocked_incomplete_result(self):
        stdout, stderr = io.StringIO(), io.StringIO()
        with (
            mock.patch.object(run_pipeline, "cmd_dry_run", side_effect=ReadOnlyDeadline()),
            contextlib.redirect_stdout(stdout),
            contextlib.redirect_stderr(stderr),
        ):
            code = run_pipeline.main(["dry-run", "--config", "unused-fixture-path"])

        payload = json.loads(stdout.getvalue())
        self.assertEqual(code, 2)
        self.assertEqual(payload["status"], "blocked")
        self.assertEqual(payload["reason"], "read_only_deadline")
        self.assertIs(payload["incomplete"], True)
        self.assertNotEqual(payload["status"], "ok")
        self.assertNotIn("missing_capabilities", payload)

    @unittest.skipUnless(
        os.name == "posix" and hasattr(signal, "setitimer"),
        "requires POSIX interval timers",
    )
    def test_real_sigalrm_deadline_interrupts_blocking_dry_run_in_subprocess(self):
        child = r'''
import contextlib, io, json, sys, time
sys.path.insert(0, sys.argv[1])
import pipeline.diagnostics as diagnostics
import run_pipeline
diagnostics.READ_ONLY_BUDGET_SECONDS = 0.05
run_pipeline.cmd_dry_run = lambda _args: time.sleep(5)
stdout, stderr = io.StringIO(), io.StringIO()
started = time.monotonic()
with contextlib.redirect_stdout(stdout), contextlib.redirect_stderr(stderr):
    code = run_pipeline.main(["dry-run", "--config", "synthetic-config"])
elapsed = time.monotonic() - started
payload = json.loads(stdout.getvalue())
print(json.dumps({
    "code": code,
    "status": payload.get("status"),
    "reason": payload.get("reason"),
    "incomplete": payload.get("incomplete"),
    "elapsed_s": elapsed,
}, sort_keys=True))
'''
        result = subprocess.run(
            [sys.executable, "-c", child, str(ROOT)],
            cwd=ROOT,
            env={"PYTHONDONTWRITEBYTECODE": "1"},
            capture_output=True,
            text=True,
            timeout=3,
            check=False,
        )

        self.assertEqual(result.returncode, 0, result.stderr)
        summary = json.loads(result.stdout)
        self.assertEqual(summary["code"], 2)
        self.assertEqual(summary["status"], "blocked")
        self.assertEqual(summary["reason"], "read_only_deadline")
        self.assertIs(summary["incomplete"], True)
        self.assertLess(summary["elapsed_s"], 2.0)

    def test_stage_logs_use_fixed_labels_numeric_facts_and_no_exception_text(self):
        stderr = io.StringIO()
        with contextlib.redirect_stderr(stderr):
            with diagnostics.read_only_command("preflight"):
                try:
                    with stage(
                        "115.request", attempt=2, timeout_s=1.25, status=503,
                        url=SYNTHETIC_URL, secret=SYNTHETIC_SECRET,
                    ):
                        raise RuntimeError(f"raw diagnostic {SYNTHETIC_SECRET} {SYNTHETIC_URL}")
                except RuntimeError:
                    pass

        lines = stderr.getvalue().splitlines()
        events = [json.loads(line) for line in lines]
        self.assertTrue(events)
        self.assertTrue(all(event["stage"] in diagnostics.LABELS for event in events))
        for event in events:
            for field in ("elapsed_s", "duration_s", "attempt", "timeout_s", "status"):
                if field in event:
                    self.assertIsInstance(event[field], (int, float))
        rendered = stderr.getvalue()
        self.assertNotIn(SYNTHETIC_SECRET, rendered)
        self.assertNotIn(SYNTHETIC_URL, rendered)
        self.assertNotIn("raw diagnostic", rendered)

    def test_request_timeout_is_clamped_to_remaining_read_only_budget(self):
        with contextlib.redirect_stderr(io.StringIO()):
            with diagnostics.read_only_command("dry-run"):
                active = diagnostics._active.get()
                self.assertIsNotNone(active)
                with mock.patch.object(
                    diagnostics.time, "monotonic", return_value=active.deadline - 0.25
                ):
                    self.assertAlmostEqual(request_timeout(5.0), 0.25)
                    self.assertEqual(request_timeout(0.1), 0.1)
                    self.assertAlmostEqual(request_timeout(None), 0.25)
                with mock.patch.object(
                    diagnostics.time, "monotonic", return_value=active.deadline + 0.01
                ):
                    with self.assertRaises(ReadOnlyDeadline):
                        request_timeout(1.0)

    def test_read_only_command_restores_prior_signal_handler_and_timer(self):
        previous_handler = object()
        previous_timer = (8.0, 0.5)
        stderr = io.StringIO()
        with (
            mock.patch.object(diagnostics.signal, "getsignal", return_value=previous_handler),
            mock.patch.object(diagnostics.signal, "signal") as set_handler,
            mock.patch.object(
                diagnostics.signal, "setitimer",
                side_effect=[previous_timer, None, None],
            ) as set_timer,
            contextlib.redirect_stderr(stderr),
        ):
            with diagnostics.read_only_command("preflight"):
                pass

        self.assertEqual(set_handler.call_args_list[0].args[0], diagnostics.signal.SIGALRM)
        self.assertTrue(callable(set_handler.call_args_list[0].args[1]))
        self.assertIs(set_handler.call_args_list[-1].args[1], previous_handler)
        self.assertEqual(set_timer.call_args_list[0].args[1], diagnostics.READ_ONLY_BUDGET_SECONDS)
        self.assertEqual(set_timer.call_args_list[1].args[1], 0)
        self.assertGreater(set_timer.call_args_list[2].args[1], 0)
        self.assertEqual(set_timer.call_args_list[2].args[2], previous_timer[1])


if __name__ == "__main__":
    unittest.main()
