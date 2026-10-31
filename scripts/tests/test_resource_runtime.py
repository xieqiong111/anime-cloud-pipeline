from __future__ import annotations

import copy
import json
import os
from pathlib import Path
import stat
import sys
import tempfile
import types
import unittest
from unittest.mock import Mock, patch

from pipeline.config import ConfigSchema
from pipeline import resource_runtime as runtime
from pipeline.resource_engine import CONFIG_FIELDS, RESOURCE_KEYS
from tests.test_cli import valid_config


class ResourceRuntimeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="quarter-runtime-")
        self.root = Path(self.tmp.name).resolve()
        self.state = self.root / "state"
        self.state.mkdir()
        (self.root / "config").mkdir()
        self.path = self.root / "config" / "config.v5.json"
        self.raw = valid_config(str(self.state))
        self.raw["private_extension"] = {"keep": [1, 2]}
        self.raw["paths"]["cloud_library_subdir"] = "整理"
        self.original = (json.dumps(self.raw, ensure_ascii=False) + "\n").encode()
        self.path.write_bytes(self.original)
        self.path.chmod(0o640)
        self.config = ConfigSchema.from_mapping(self.raw)
        self.values = {
            "paths.cloud_source_root": "/cloud/source/2026-10",
            "runtime.litepan_task_name": "quarter-task",
            "runtime.litepan_task_source_root": "/cloud/source/2026-10",
            "runtime.litepan_task_output_root": "/litepan/output/2026-10",
            "runtime.litepan_event_name": "quarter-event",
            "runtime.emby_library_name": "quarter-library",
            "runtime.emby_library_root": "/emby/media/2026-10",
        }
        self.layout = {"quarter": "2026-10", "targets": {key: key for key in RESOURCE_KEYS},
                       "patch": self.values}
        self.qualification = {"status": "ok", "counts": {"eligible": 1, "unknown": 0}}
        self.ports = Mock()
        self.ports.inventory.return_value = {key: {"state": "reuse"} for key in RESOURCE_KEYS}
        self.ports.creation_capabilities.return_value = []
        self.ports.config_patch.return_value = self.values
        self.constructor = Mock(return_value=self.ports)
        self.builder = Mock(return_value=self.layout)
        self.contexts = [
            patch.dict(sys.modules, {
                "pipeline.quarter_layout": types.SimpleNamespace(build_layout=self.builder),
                "pipeline.quarter_ports": types.SimpleNamespace(QuarterResourcePorts=self.constructor),
            }),
            patch.object(runtime, "qualify_current_subscriptions", return_value=self.qualification),
            patch.object(runtime, "_current_quarter", return_value="2026-10"),
            patch.dict(os.environ, {
                "OCTOP_PIPELINE_ALLOW_RESOURCES": "1",
                "OCTOP_PIPELINE_RESOURCE_REVISION": runtime._resource_plan_revision(
                    self.original, self.layout["quarter"]
                ),
            }),
        ]
        for context in self.contexts:
            context.start()

    def tearDown(self):
        for context in reversed(self.contexts):
            context.stop()
        self.tmp.cleanup()

    def run_command(self, apply=True):
        return runtime.run_resource_preparation(self.raw, self.config, config_path=str(self.path), apply=apply)

    def files(self):
        return {str(path.relative_to(self.root)) for path in self.root.rglob("*")}

    def test_unknown_and_zero_never_build_layout_or_ports(self):
        before = self.files()
        for qualification, status in (({"status": "blocked", "counts": {"eligible": 0}}, "blocked"),
                                      ({"status": "ok", "counts": {"eligible": 0, "unknown": 0}}, "noop"),
                                      ({"status": "ok", "counts": {"eligible": 1, "unknown": 1}}, "blocked")):
            with patch.object(runtime, "qualify_current_subscriptions", return_value=qualification):
                self.assertEqual(self.run_command()["status"], status)
        self.builder.assert_not_called()
        self.constructor.assert_not_called()
        self.assertEqual(self.files(), before)

    def test_unauthorized_apply_has_no_lock_journal_or_ports(self):
        before = self.files()
        with patch.dict(os.environ, {"OCTOP_PIPELINE_ALLOW_RESOURCES": "0"}):
            report = self.run_command()
        self.assertEqual(report["reason"], "authorization_required")
        self.constructor.assert_not_called()
        self.assertEqual(self.files(), before)

    def test_plan_is_read_only_even_when_state_directory_is_absent(self):
        self.state.rmdir()
        before = self.files()
        original = self.path.stat()
        report = self.run_command(apply=False)
        self.assertEqual(report["status"], "plan_only")
        self.assertEqual(
            report["config_revision"],
            runtime._resource_plan_revision(self.original, self.layout["quarter"]),
        )
        self.assertRegex(report["config_revision"], r"^[0-9a-f]{64}$")
        self.assertEqual(len(report["config_changes"]), 7)
        self.assertEqual(self.files(), before)
        self.assertEqual(self.path.stat().st_ino, original.st_ino)
        self.ports.create.assert_not_called()
        self.ports.config_patch.assert_not_called()

    def test_same_config_plans_for_different_quarters_have_different_revisions(self):
        october = self.run_command(apply=False)
        january_layout = {**self.layout, "quarter": "2027-01"}
        self.builder.return_value = january_layout

        with patch.object(runtime, "_current_quarter", return_value="2027-01"):
            january = self.run_command(apply=False)

        self.assertEqual(october["status"], "plan_only")
        self.assertEqual(january["status"], "plan_only")
        self.assertEqual(self.original, self.path.read_bytes())
        self.assertNotEqual(october["config_revision"], january["config_revision"])
        self.assertEqual(
            october["config_revision"],
            runtime._resource_plan_revision(self.original, "2026-10"),
        )
        self.assertEqual(
            january["config_revision"],
            runtime._resource_plan_revision(self.original, "2027-01"),
        )

    def test_previous_quarter_revision_blocks_before_ports_or_writes(self):
        october = self.run_command(apply=False)
        self.constructor.reset_mock()
        self.ports.reset_mock()
        january_layout = {**self.layout, "quarter": "2027-01"}
        self.builder.return_value = january_layout
        before = self.files()

        with patch.object(runtime, "_current_quarter", return_value="2027-01"):
            report = self.run_command()

        self.assertEqual(report["reason"], "resource_plan_stale")
        self.constructor.assert_not_called()
        self.ports.inventory.assert_not_called()
        self.ports.create.assert_not_called()
        self.ports.config_patch.assert_not_called()
        self.assertEqual(self.path.read_bytes(), self.original)
        self.assertEqual(self.files(), before)
        self.assertFalse((self.state / "run.lock").exists())
        self.assertFalse((self.state / "resource-preparation.jsonl").exists())
        self.assertFalse((self.root / "backups").exists())
        self.assertNotEqual(
            october["config_revision"],
            runtime._resource_plan_revision(self.original, january_layout["quarter"]),
        )

    def test_same_quarter_fresh_plan_revision_allows_normal_apply(self):
        plan = self.run_command(apply=False)
        self.constructor.reset_mock()
        with patch.dict(
            os.environ,
            {"OCTOP_PIPELINE_RESOURCE_REVISION": plan["config_revision"]},
        ):
            report = self.run_command()

        self.assertEqual(report["status"], "prepared")
        self.assertEqual(report["config_revision"], plan["config_revision"])
        self.constructor.assert_called_once()
        self.ports.create.assert_not_called()
        self.assertTrue((self.state / "resource-preparation.jsonl").exists())

    def test_missing_or_invalid_revision_blocks_before_lock_ports_or_writes(self):
        before = self.files()
        for value in (None, "", "0" * 63, "A" * 64, "g" * 64, "0" * 64 + "\n"):
            with self.subTest(value=value), patch.dict(os.environ):
                if value is None:
                    os.environ.pop("OCTOP_PIPELINE_RESOURCE_REVISION", None)
                else:
                    os.environ["OCTOP_PIPELINE_RESOURCE_REVISION"] = value
                with patch.object(runtime, "ProcessLock") as lock:
                    report = self.run_command()
                self.assertEqual(report["reason"], "resource_plan_stale")
                lock.assert_not_called()
                self.constructor.assert_not_called()
                self.assertEqual(self.files(), before)

    def test_plan_revision_rejects_changed_config_even_with_fresh_raw_mapping(self):
        plan = self.run_command(apply=False)
        self.constructor.reset_mock()
        self.raw["runtime"]["litepan_task_name"] = "changed-after-plan"
        self.path.write_text(json.dumps(self.raw))
        self.config = ConfigSchema.from_mapping(self.raw)
        before = self.files()
        with patch.dict(os.environ, {"OCTOP_PIPELINE_RESOURCE_REVISION": plan["config_revision"]}), \
             patch.object(runtime, "ProcessLock") as lock:
            report = self.run_command()
        self.assertEqual(report["reason"], "resource_plan_stale")
        lock.assert_not_called()
        self.constructor.assert_not_called()
        self.assertEqual(self.files(), before)
        self.assertEqual(json.loads(self.path.read_text()), self.raw)

    def test_zero_and_unknown_keep_early_gate_without_revision(self):
        with patch.dict(os.environ):
            os.environ.pop("OCTOP_PIPELINE_RESOURCE_REVISION", None)
            for qualification, status in (({"status": "ok", "counts": {"eligible": 0, "unknown": 0}}, "noop"),
                                           ({"status": "blocked", "counts": {"eligible": None}}, "blocked")):
                with patch.object(runtime, "qualify_current_subscriptions", return_value=qualification):
                    self.assertEqual(self.run_command()["status"], status)
        self.builder.assert_not_called()
        self.constructor.assert_not_called()

    def test_live_process_lock_blocks_before_resource_reads(self):
        lock = self.state / "run.lock"
        lock.write_text(str(os.getpid()))
        report = self.run_command()
        self.assertEqual(report["reason"], "resource_lock_held")
        self.constructor.assert_not_called()
        self.assertTrue(lock.exists())

    def test_apply_requires_existing_state_parent(self):
        self.state.rmdir()
        self.assertEqual(self.run_command()["status"], "blocked")
        self.assertFalse(self.state.exists())
        self.constructor.assert_not_called()

    def test_apply_requalifies_under_lock_before_constructing_resource_ports(self):
        for second, status in (({"status": "ok", "counts": {"eligible": 0, "unknown": 0}}, "noop"),
                               ({"status": "blocked", "counts": {"eligible": None}}, "blocked"),
                               ({"status": "ok", "counts": {"eligible": 1, "unknown": 1}}, "blocked")):
            with self.subTest(status=status, unknown=second["counts"].get("unknown")):
                observed_lock = []

                def qualify(config):
                    observed_lock.append((self.state / "run.lock").exists())
                    return self.qualification if len(observed_lock) == 1 else second

                with patch.object(runtime, "qualify_current_subscriptions", side_effect=qualify):
                    report = self.run_command()
                self.assertEqual(observed_lock, [False, True])
                self.assertEqual(report["status"], status)
                self.assertEqual(report["qualification_counts"], second["counts"])
                self.constructor.assert_not_called()
                self.assertFalse((self.state / "run.lock").exists())
                self.assertFalse((self.state / "resource-preparation.jsonl").exists())
                self.assertFalse((self.root / "backups").exists())

    def test_commit_only_seven_fields_preserves_unknown_and_original_backup_permissions(self):
        original_info = self.path.stat()
        report = self.run_command()
        self.assertEqual(report["status"], "prepared")
        updated = json.loads(self.path.read_bytes())
        expected = copy.deepcopy(self.raw)
        for field, value in self.values.items():
            section, name = field.split(".")
            expected[section][name] = value
        self.assertEqual(updated, expected)
        self.assertEqual(updated["paths"]["cloud_library_subdir"], "整理")
        self.assertNotIn("litepan_publish_subdir", updated["paths"])
        self.assertEqual(set(change["field"] for change in report["config_changes"]), CONFIG_FIELDS)
        backups = list((self.root / "backups").glob("quarter-resources-*/config.v5.json"))
        self.assertEqual(len(backups), 1)
        self.assertEqual(backups[0].read_bytes(), self.original)
        self.assertEqual(stat.S_IMODE(backups[0].stat().st_mode), 0o640)
        self.assertEqual(stat.S_IMODE(backups[0].parent.stat().st_mode), 0o700)
        self.assertEqual(stat.S_IMODE(self.path.stat().st_mode), 0o640)
        self.assertEqual(self.path.stat().st_uid, original_info.st_uid)
        self.assertEqual(self.path.stat().st_gid, original_info.st_gid)
        self.assertFalse((self.state / "run.lock").exists())
        events = [json.loads(line) for line in (self.state / "resource-preparation.jsonl").read_text().splitlines()]
        self.assertEqual(events, [{"key": "pipeline", "action": "complete", "outcome": "prepared"}])

    def test_changed_content_and_replaced_identity_reject_cas(self):
        for replace in (False, True):
            self.path.write_bytes(self.original)
            commit = runtime._ConfigCommit(str(self.path), self.raw, self.values)
            if replace:
                substitute = self.path.with_suffix(".new")
                substitute.write_bytes(self.original)
                substitute.replace(self.path)
            else:
                self.path.write_bytes(self.original + b" ")
            with self.assertRaises(ValueError):
                commit(self.values)
        self.assertFalse((self.root / "backups").exists())

    def test_changed_config_during_inventory_prevents_commit(self):
        snapshot = self.ports.inventory.return_value

        def inventory(layout):
            self.path.write_bytes(self.original + b" ")
            return snapshot

        self.ports.inventory.side_effect = inventory
        report = self.run_command()
        self.assertEqual(report["reason"], "configuration_commit_failed")
        self.assertFalse((self.root / "backups").exists())
        self.assertFalse((self.state / "run.lock").exists())

    def test_idempotent_commit_does_not_rewrite_or_backup(self):
        updated = runtime._patch(self.raw, self.values)
        self.path.write_text(json.dumps(updated))
        info = self.path.stat()
        commit = runtime._ConfigCommit(str(self.path), updated, self.values)
        commit(self.values)
        self.assertEqual(commit.changes, [])
        self.assertEqual(self.path.stat().st_ino, info.st_ino)
        self.assertEqual(self.path.stat().st_mtime_ns, info.st_mtime_ns)
        self.assertFalse((self.root / "backups").exists())

    def test_unapproved_patch_and_config_hardlink_or_symlink_rejected(self):
        invalid = dict(self.values, **{"paths.cloud_library_root": "/other"})
        with self.assertRaises(ValueError):
            runtime._ConfigCommit(str(self.path), self.raw, invalid)
        alias = self.path.with_suffix(".alias")
        os.link(self.path, alias)
        with self.assertRaises(ValueError):
            runtime._ConfigCommit(str(self.path), self.raw, self.values)
        alias.unlink()
        alias.symlink_to(self.path)
        with self.assertRaises(ValueError):
            runtime._ConfigCommit(str(alias), self.raw, self.values)

    def test_journal_rejects_unknown_values_and_symlink_or_hardlink(self):
        journal = self.state / "resource-preparation.jsonl"
        with self.assertRaises(ValueError):
            runtime._journal(journal, {"key": "pipeline", "action": "complete", "outcome": "private"})
        self.assertFalse(journal.exists())
        journal.symlink_to(self.path)
        with self.assertRaises(OSError):
            runtime._journal(journal, {"key": "pipeline", "action": "complete", "outcome": "prepared"})
        self.assertEqual(self.path.read_bytes(), self.original)
        journal.unlink()
        os.link(self.path, journal)
        with self.assertRaises(ValueError):
            runtime._journal(journal, {"key": "pipeline", "action": "complete", "outcome": "prepared"})
        self.assertEqual(self.path.read_bytes(), self.original)

    def test_extended_attributes_preserved_when_supported(self):
        name = "com.example.quartertest" if sys.platform == "darwin" else "user.quartertest"
        try:
            runtime._set_xattr(self.path, name, b"retained")
        except OSError:
            self.skipTest("filesystem does not support user extended attributes")
        commit = runtime._ConfigCommit(str(self.path), self.raw, self.values)
        commit(self.values)
        self.assertEqual(runtime._xattrs(self.path)[name], b"retained")
        backup = next((self.root / "backups").glob("quarter-resources-*/config.v5.json"))
        self.assertEqual(runtime._xattrs(backup)[name], b"retained")


if __name__ == "__main__":
    unittest.main(verbosity=2)
