from __future__ import annotations

import copy
import unittest

from pipeline.resource_engine import (
    CONFIG_FIELDS,
    RESOURCE_KEYS,
    ResourceBlocked,
    coordinate_resources,
)


def inventory(state="reuse"):
    return {key: {"state": state} for key in RESOURCE_KEYS}


def inventory_map(states):
    return {key: {"state": states[key]} for key in RESOURCE_KEYS}


class FakePorts:
    def __init__(self, *, states=None, capabilities=None, patch=None):
        self.states = dict(states or {key: "reuse" for key in RESOURCE_KEYS})
        self.capabilities = list(capabilities or [])
        self.patch = patch if patch is not None else {
            "paths.cloud_source_root": "/cloud/source/2026-10",
            "runtime.litepan_task_source_root": "/cloud/library/2026-10",
        }
        self.inventory_calls = 0
        self.capability_calls = 0
        self.create_calls = []
        self.config_calls = 0
        self.deleted = 0
        self.create_actions = {}
        self.inventory_actions = []
        self.inventory_error = None

    def inventory(self, layout):
        self.inventory_calls += 1
        if self.inventory_actions:
            action = self.inventory_actions.pop(0)
            if isinstance(action, Exception):
                raise action
            return copy.deepcopy(action)
        if self.inventory_error is not None:
            raise self.inventory_error
        return {key: {"state": self.states[key]} for key in RESOURCE_KEYS}

    def creation_capabilities(self, layout):
        self.capability_calls += 1
        return list(self.capabilities)

    def create(self, key, layout):
        self.create_calls.append(key)
        action = self.create_actions.get(key)
        if action == "timeout_after_create":
            self.states[key] = "reuse"
            raise TimeoutError("private endpoint https://secret.invalid/?token=do-not-log")
        if action == "timeout_uncertain":
            raise TimeoutError("private endpoint https://secret.invalid/?token=do-not-log")
        self.states[key] = "reuse"

    def config_patch(self, layout):
        return self.patch

    def delete(self, key):
        self.deleted += 1


class TickClock:
    def __init__(self, value=0.0):
        self.value = value

    def __call__(self):
        return self.value


def run(
    ports,
    *,
    apply=True,
    authorized=True,
    qualification=None,
    journal=None,
    commit=None,
    clock=lambda: 0.0,
    max_seconds=150,
):
    journal = journal if journal is not None else lambda event: None
    commit = commit if commit is not None else lambda patch: None
    if qualification is None:
        qualification = {"status": "ok", "counts": {"eligible": 1}}
    return coordinate_resources(
        layout={"quarter": "2026-10", "private_layout_value": "must-not-return"},
        qualification=qualification,
        ports=ports,
        apply=apply,
        authorized=authorized,
        journal=journal,
        commit_config=commit,
        clock=clock,
        max_seconds=max_seconds,
    )


class ResourceEngineTests(unittest.TestCase):
    def test_zero_eligible_and_unknown_qualification_make_no_calls(self):
        for qualification, expected_status in (
            ({"status": "ok", "counts": {"eligible": 0}}, "noop"),
            ({"status": "blocked", "counts": {"eligible": 3}}, "blocked"),
        ):
            ports = FakePorts(states={key: "create" for key in RESOURCE_KEYS})
            journal_calls = []
            commit_calls = []
            result = run(
                ports,
                qualification=qualification,
                journal=journal_calls.append,
                commit=commit_calls.append,
            )
            self.assertEqual(result["status"], expected_status)
            self.assertEqual(ports.inventory_calls, 0)
            self.assertEqual(ports.capability_calls, 0)
            self.assertEqual(ports.create_calls, [])
            self.assertEqual(journal_calls, [])
            self.assertEqual(commit_calls, [])

    def test_read_only_plan_checks_inventory_and_capabilities_without_writes(self):
        states = {key: "reuse" for key in RESOURCE_KEYS}
        states["raw_dir"] = "create"
        ports = FakePorts(states=states, capabilities=["management_permission_missing"])
        journal_calls = []
        commit_calls = []
        result = run(
            ports,
            apply=False,
            journal=journal_calls.append,
            commit=commit_calls.append,
        )
        self.assertEqual(result["status"], "plan_only")
        self.assertEqual(result["reason"], "creation_capabilities_blocked")
        self.assertEqual(result["blockers"], ["management_permission_missing"])
        self.assertEqual(ports.inventory_calls, 1)
        self.assertEqual(ports.capability_calls, 1)
        self.assertEqual(ports.create_calls, [])
        self.assertEqual(journal_calls, [])
        self.assertEqual(commit_calls, [])

    def test_apply_requires_authorization_before_any_port_call(self):
        ports = FakePorts(states={key: "create" for key in RESOURCE_KEYS})
        result = run(ports, authorized=False)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "authorization_required")
        self.assertEqual(ports.inventory_calls, 0)
        self.assertEqual(ports.capability_calls, 0)
        self.assertEqual(ports.create_calls, [])

    def test_complete_eight_resource_prepare_and_repeated_apply_do_not_recreate(self):
        ports = FakePorts(states={key: "create" for key in RESOURCE_KEYS})
        journal_calls = []
        commit_calls = []
        first = run(ports, journal=journal_calls.append, commit=commit_calls.append)
        self.assertEqual(first["status"], "prepared")
        self.assertFalse(first["partial"])
        self.assertEqual(first["counts"], {"created": 8, "reused": 0})
        self.assertEqual(ports.create_calls, list(RESOURCE_KEYS))
        self.assertEqual(len(commit_calls), 1)
        self.assertEqual(journal_calls[-1], {
            "key": "pipeline", "action": "complete", "outcome": "prepared",
        })
        for event in journal_calls:
            self.assertEqual(set(event), {"key", "action", "outcome"})

        before = len(ports.create_calls)
        second = run(ports, journal=journal_calls.append, commit=commit_calls.append)
        self.assertEqual(second["status"], "prepared")
        self.assertEqual(second["counts"], {"created": 0, "reused": 8})
        self.assertEqual(len(ports.create_calls), before)
        self.assertEqual(len(commit_calls), 2)

    def test_any_initial_unknown_or_conflict_prevents_every_creation(self):
        for state in ("unknown", "conflict"):
            states = {key: "create" for key in RESOURCE_KEYS}
            states["cloud_source"] = state
            ports = FakePorts(states=states)
            result = run(ports)
            self.assertEqual(result["status"], "blocked")
            self.assertEqual(ports.create_calls, [])
            self.assertEqual(ports.capability_calls, 1)

    def test_one_creation_capability_blocker_prevents_all_creations(self):
        ports = FakePorts(
            states={key: "create" for key in RESOURCE_KEYS},
            capabilities=["litepan_admin_required"],
        )
        result = run(ports)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "creation_capabilities_blocked")
        self.assertEqual(ports.create_calls, [])

    def test_create_timeout_that_reads_back_reuse_is_recovered(self):
        states = {key: "reuse" for key in RESOURCE_KEYS}
        states["cloud_quarter"] = "create"
        ports = FakePorts(states=states)
        ports.create_actions["cloud_quarter"] = "timeout_after_create"
        journal_calls = []
        result = run(ports, journal=journal_calls.append)
        self.assertEqual(result["status"], "prepared")
        self.assertEqual(result["counts"]["created"], 1)
        self.assertEqual(ports.create_calls, ["cloud_quarter"])
        self.assertIn({
            "key": "cloud_quarter", "action": "create", "outcome": "recovered",
        }, journal_calls)
        self.assertNotIn("secret.invalid", repr(result))
        self.assertNotIn("do-not-log", repr(journal_calls))

    def test_uncertain_timeout_stops_without_retry_or_delete(self):
        states = {key: "reuse" for key in RESOURCE_KEYS}
        states["cloud_quarter"] = "create"
        ports = FakePorts(states=states)
        ports.create_actions["cloud_quarter"] = "timeout_uncertain"
        journal_calls = []
        result = run(ports, journal=journal_calls.append)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "creation_uncertain")
        self.assertTrue(result["partial"])
        self.assertEqual(ports.create_calls, ["cloud_quarter"])
        self.assertEqual(ports.deleted, 0)
        self.assertIn({
            "key": "cloud_quarter", "action": "create", "outcome": "uncertain",
        }, journal_calls)

    def test_failed_mandatory_readback_marks_all_resources_unknown(self):
        states = {key: "reuse" for key in RESOURCE_KEYS}
        states["cloud_quarter"] = "create"
        ports = FakePorts(states=states)
        ports.inventory_actions = [
            inventory_map(states),
            inventory_map(states),
            RuntimeError("private readback endpoint"),
        ]
        result = run(ports)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "inventory_incomplete_after_create")
        self.assertTrue(result["partial"])
        self.assertEqual(set(result["resources"].values()), {"unknown"})
        self.assertEqual(ports.create_calls, ["cloud_quarter"])

    def test_later_partial_failure_keeps_prior_create_and_never_deletes(self):
        states = {key: "reuse" for key in RESOURCE_KEYS}
        states["cloud_quarter"] = "create"
        states["cloud_source"] = "create"
        ports = FakePorts(states=states)
        ports.create_actions["cloud_source"] = "timeout_uncertain"
        result = run(ports)
        self.assertEqual(result["status"], "blocked")
        self.assertTrue(result["partial"])
        self.assertEqual(ports.create_calls, ["cloud_quarter", "cloud_source"])
        self.assertEqual(ports.deleted, 0)
        self.assertEqual(result["counts"]["created"], 1)

    def test_budget_expiry_after_inventory_stops_before_any_create(self):
        clock = TickClock()
        ports = FakePorts(states={key: "create" for key in RESOURCE_KEYS})
        original_inventory = ports.inventory

        def slow_inventory(layout):
            value = original_inventory(layout)
            clock.value = 151.0
            return value

        ports.inventory = slow_inventory
        result = run(ports, clock=clock, max_seconds=150)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "budget_exhausted")
        self.assertEqual(ports.create_calls, [])

    def test_config_patch_rejects_unapproved_fields_urls_and_controls(self):
        invalid_patches = (
            {"paths.cloud_library_root": "/must/not/restore"},
            {"runtime.litepan_task_name": "https://secret.invalid"},
            {"runtime.litepan_event_name": "anime\nready"},
        )
        for patch in invalid_patches:
            with self.subTest(patch=patch):
                ports = FakePorts(patch=patch)
                commits = []
                result = run(ports, commit=commits.append)
                self.assertEqual(result["status"], "blocked")
                self.assertEqual(result["reason"], "configuration_patch_invalid")
                self.assertEqual(commits, [])
        self.assertEqual(len(CONFIG_FIELDS), 7)

    def test_callback_and_port_exceptions_never_echo_exception_text(self):
        ports = FakePorts()
        ports.inventory_error = RuntimeError("https://private.invalid/?token=secret")
        result = run(ports)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "inventory_failed")
        self.assertNotIn("private.invalid", repr(result))
        self.assertNotIn("secret", repr(result))

    def test_journal_failure_before_create_prevents_write(self):
        states = {key: "reuse" for key in RESOURCE_KEYS}
        states["cloud_quarter"] = "create"
        ports = FakePorts(states=states)

        def fail_journal(event):
            raise RuntimeError("secret token must not appear")

        result = run(ports, journal=fail_journal)
        self.assertEqual(result["status"], "blocked")
        self.assertEqual(result["reason"], "journal_failed")
        self.assertEqual(ports.create_calls, [])
        self.assertNotIn("secret token", repr(result))

    def test_resource_blocked_exception_sanitizes_unrecognized_codes(self):
        error = ResourceBlocked("token=private")
        self.assertEqual(error.code, "resource_blocked")
        self.assertEqual(str(error), "resource_blocked")


if __name__ == "__main__":
    unittest.main(verbosity=2)
