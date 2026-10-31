"""Focused Task 8 tests for exact STRM evidence and LitePan reconciliation."""
from __future__ import annotations

import json
import os
import tempfile
import unittest

from pipeline.evidence import StrmEvidence
from pipeline.adapters import LitePanAdapter
from pipeline.models import EpisodeKey, EpisodeStatus, ReviewReason
from pipeline.paths import PathMapper, ServiceRoots
from pipeline.real_adapters import _strict_litepan_task_entries
from pipeline.services import StrmService
from pipeline.state import StateStore


EXPECTED_STRM = "/lite/Show/Season 1/Show - S01E08.strm"
OLD_STRM = "/lite/old/Show - S01E08.strm"


class FakeFiles:
    def __init__(self, values: dict[str, object] | None = None):
        self.values = dict(values or {})
        self.read_calls: list[str] = []
        self.symlinks: set[str] = set()

    def exists(self, path: str) -> bool:
        return path in self.values

    def read(self, path: str) -> object:
        self.read_calls.append(path)
        if path not in self.values:
            raise FileNotFoundError(path)
        return self.values[path]

    def is_symlink(self, path: str) -> bool:
        return path in self.symlinks


class FakeLitePan:
    def __init__(self, *, tasks=None, status: int = 200):
        self.tasks = list(tasks or [{
            "id": "task-7",
            "name": "追番",
            "active": True,
            "source_root": "/source",
            "output_root": "/lite",
        }])
        self.status = status
        self.trigger_calls: list[tuple[str, str, str]] = []

    def list_tasks(self):
        return list(self.tasks)

    def trigger_event(self, event: str, source: str, message: str):
        self.trigger_calls.append((event, source, message))
        return {"status": self.status, "accepted": self.status == 200}


class MalformedInventoryLitePan(FakeLitePan):
    def __init__(self, inventory):
        super().__init__()
        self.inventory = inventory

    def list_tasks(self):
        return self.inventory


class RuntimeOnlyLitePan(LitePanAdapter):
    """Adapter whose real inventory method is list_runtime_tasks only."""

    def __init__(self, inventory):
        self.inventory = inventory
        self.trigger_calls: list[tuple[str, str, str]] = []

    def list_runtime_tasks(self):
        return self.inventory

    def trigger_event(self, event: str, source: str, message: str):
        self.trigger_calls.append((event, source, message))
        return {"status": 200, "accepted": True}


class TasksAttributeLitePan(LitePanAdapter):
    """Adapter exposing inventory through the tasks attribute only."""

    def __init__(self, inventory):
        self.tasks = inventory
        self.trigger_calls: list[tuple[str, str, str]] = []

    def trigger_event(self, event: str, source: str, message: str):
        self.trigger_calls.append((event, source, message))
        return {"status": 200, "accepted": True}


class LitePanServiceTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def episode(self, **overrides):
        values = {
            "key": EpisodeKey("tmdb", "111", 1, 8),
            "canonical_work": "Show",
            "title": "Show - S01E08",
            "status": EpisodeStatus.ORGANIZED,
            "cloud_path": "/cloud/Show/Season 1/Show - S01E08.mkv",
            "strm_path": EXPECTED_STRM,
        }
        values.update(overrides)
        return self.store.get_or_create_episode(values)

    @property
    def mapper(self):
        return PathMapper(ServiceRoots(
            cloud_source_root="/cloud-source",
            cloud_library_root="/cloud",
            litepan_output_root="/lite",
            moviepilot_mount_root="/moviepilot",
            emby_mount_root="/emby",
        ))

    def service(self, files, litepan=None, **kwargs):
        return StrmService(
            self.store,
            litepan or FakeLitePan(),
            files=files,
            mapper=kwargs.pop("mapper", self.mapper),
            task_name="追番",
            source_root="/source",
            output_root="/lite",
            **kwargs,
        )

    def test_http_200_without_exact_strm_stays_pending(self):
        episode_id = self.episode()
        litepan = FakeLitePan(status=200)

        self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))

        row = self.store.get_episode(episode_id)
        self.assertEqual(row["status"], EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(len(litepan.trigger_calls), 1)

    def test_state_store_status_lookup_returns_current_v5_status(self):
        episode_id = self.episode()
        self.store.transition(episode_id, EpisodeStatus.STRM_PENDING)

        self.assertEqual(
            self.store.episode_status(episode_id),
            EpisodeStatus.STRM_PENDING.value,
        )

    def test_strm_must_be_exact_path_and_http_url(self):
        files = FakeFiles({EXPECTED_STRM: "not-a-url", OLD_STRM: "https://valid.example/old"})

        result = StrmEvidence(files).validate(EXPECTED_STRM)

        self.assertFalse(result.ok)
        self.assertEqual(result.detail["reason"], "invalid_url")
        self.assertNotIn("https://valid.example/old", json.dumps(result.detail))

    def test_multiple_matching_tasks_fail_closed_without_trigger(self):
        episode_id = self.episode()
        litepan = FakeLitePan(tasks=[
            {"id": "task-a", "name": "追番", "active": True,
             "source_root": "/source", "output_root": "/lite"},
            {"id": "task-b", "name": "追番", "active": True,
             "source_root": "/source", "output_root": "/lite"},
        ])

        result = self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))

        row = self.store.get_episode(episode_id)
        self.assertEqual(row["status"], EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(row["review_reason"], ReviewReason.RUNTIME_MAPPING_AMBIGUOUS.value)
        self.assertEqual(litepan.trigger_calls, [])
        self.assertEqual(result.review_reason, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS)

    def test_unique_exact_strm_reconciles_before_trigger_and_persists_safe_id(self):
        episode_id = self.episode()
        secret = "https://media.example/api/strm/play/1/42/t/secret-token/n/file"
        litepan = FakeLitePan()

        result = self.service(FakeFiles({EXPECTED_STRM: secret}), litepan).advance(
            self.store.get_episode(episode_id)
        )

        row = self.store.get_episode(episode_id)
        self.assertEqual(row["status"], EpisodeStatus.STRM_READY.value)
        self.assertEqual(litepan.trigger_calls, [])
        identity = json.loads(row["runtime_identity_json"])
        self.assertEqual(identity["task_id"], "task-7")
        self.assertNotIn(secret, json.dumps(identity))
        self.assertNotIn(secret, json.dumps(result.detail))

    def test_persisted_runtime_task_id_must_still_match_complete_active_inventory(self):
        episode_id = self.episode(
            status=EpisodeStatus.STRM_PENDING,
            runtime_identity_json=json.dumps({
                "task_id": "task-7",
                "task_name": "追番",
                "source_root": "/source",
                "output_root": "/lite",
            }),
        )
        litepan = FakeLitePan(tasks=[{
            "id": "task-8", "name": "追番", "active": True,
            "source_root": "/source", "output_root": "/lite",
        }])

        result = self.service(FakeFiles({}), litepan).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.NEEDS_REVIEW.value)
        self.assertEqual(result.review_reason, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS)
        self.assertEqual(litepan.trigger_calls, [])
        self.assertEqual(
            json.loads(self.store.get_episode(episode_id)["runtime_identity_json"])["task_id"],
            "task-7",
        )

    def test_resolved_runtime_task_is_consumed_without_reopening_review(self):
        episode_id = self.episode()
        litepan = FakeLitePan(tasks=[
            {"id": "task-a", "name": "追番", "active": True,
             "source_root": "/source", "output_root": "/lite"},
            {"id": "task-b", "name": "追番", "active": True,
             "source_root": "/source", "output_root": "/lite"},
        ])
        first = self.service(FakeFiles({}), litepan).advance(
            self.store.get_episode(episode_id)
        )
        self.assertEqual(first.review_reason, ReviewReason.RUNTIME_MAPPING_AMBIGUOUS)

        from pipeline.services import ReviewService
        ReviewService(self.store).resolve(
            episode_id,
            {
                "task_id": "task-b",
                "task_name": "追番",
                "source_root": "/source",
                "output_root": "/lite",
            },
        )
        second = self.service(FakeFiles({}), litepan).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(second.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(len(litepan.trigger_calls), 1)
        self.assertEqual(
            self.store.history(episode_id).count(EpisodeStatus.NEEDS_REVIEW.value),
            1,
        )

    def test_authoritative_cloud_mapping_ignores_stale_persisted_strm_path(self):
        episode_id = self.episode(strm_path=OLD_STRM)
        litepan = FakeLitePan()

        self.service(FakeFiles({EXPECTED_STRM: "https://media.example/current"}), litepan).advance(
            self.store.get_episode(episode_id)
        )

        row = self.store.get_episode(episode_id)
        self.assertEqual(row["status"], EpisodeStatus.STRM_READY.value)
        self.assertEqual(row["strm_path"], EXPECTED_STRM)
        self.assertEqual(litepan.trigger_calls, [])

    def test_symlinked_exact_strm_is_not_evidence(self):
        files = FakeFiles({EXPECTED_STRM: "https://media.example/current"})
        files.symlinks.add(EXPECTED_STRM)

        result = StrmEvidence(files).validate(EXPECTED_STRM)

        self.assertFalse(result.ok)
        self.assertEqual(result.detail["reason"], "symlink")

    def test_malformed_task_entry_fails_closed_without_trigger(self):
        episode_id = self.episode()
        litepan = MalformedInventoryLitePan([{
            "id": "task-7",
            "name": "追番",
            "source_root": "/source",
            "output_root": "/lite",
        }])

        result = self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(litepan.trigger_calls, [])
        self.assertEqual(self.store.get_episode(episode_id)["status"], EpisodeStatus.STRM_PENDING.value)

    def test_matching_task_without_id_is_not_selectable(self):
        episode_id = self.episode()
        litepan = MalformedInventoryLitePan([{
            "name": "追番",
            "active": True,
            "source_root": "/source",
            "output_root": "/lite",
        }])

        result = self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(litepan.trigger_calls, [])

    def test_incomplete_task_envelope_fails_closed_without_trigger(self):
        episode_id = self.episode()
        litepan = MalformedInventoryLitePan({
            "tasks": [{
                "id": "task-7", "name": "追番", "active": True,
                "source_root": "/source", "output_root": "/lite",
            }],
            "next": "page-2",
        })

        result = self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(litepan.trigger_calls, [])

    def test_nested_incomplete_total_envelope_fails_closed_without_trigger(self):
        episode_id = self.episode()
        litepan = MalformedInventoryLitePan({
            "data": {
                "tasks": [{
                    "id": "task-7", "name": "追番", "active": True,
                    "source_root": "/source", "output_root": "/lite",
                }],
            },
            "pagination": {"page": 1, "total": 2},
        })

        result = self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(litepan.trigger_calls, [])

    def test_failure_task_envelope_fails_closed_without_trigger(self):
        episode_id = self.episode()
        litepan = MalformedInventoryLitePan({
            "success": False,
            "error": "secret endpoint payload",
            "tasks": [{
                "id": "task-7", "name": "追番", "active": True,
                "source_root": "/source", "output_root": "/lite",
            }],
        })

        result = self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(litepan.trigger_calls, [])
        self.assertNotIn("secret endpoint payload", json.dumps(result.detail))

    def test_missing_runtime_configuration_is_rejected_instead_of_wildcarding(self):
        with self.assertRaises(ValueError):
            StrmService(
                self.store,
                FakeLitePan(),
                files=FakeFiles({}),
                mapper=self.mapper,
                task_name=None,
                source_root="/source",
                output_root="/lite",
            )

    def test_url_controls_and_malformed_port_are_rejected(self):
        for value in (
            "https://media.example/path\x00secret",
            "https://media.example:not-a-port/path",
            "https://user:password@media.example/path",
        ):
            with self.subTest(value=value):
                result = StrmEvidence(FakeFiles({EXPECTED_STRM: value})).validate(EXPECTED_STRM)
                self.assertFalse(result.ok)
                self.assertEqual(result.detail["reason"], "invalid_url")

    def test_backoff_reconciles_existing_exact_strm_before_skipping_trigger(self):
        episode_id = self.episode(
            status=EpisodeStatus.STRM_PENDING,
            retry_count=4,
            next_retry_at=9999999999,
        )
        litepan = FakeLitePan()

        result = self.service(
            FakeFiles({EXPECTED_STRM: "https://media.example/current"}), litepan,
            now=lambda: 100,
        ).advance(self.store.get_episode(episode_id))

        self.assertEqual(result.status, EpisodeStatus.STRM_READY.value)
        self.assertEqual(litepan.trigger_calls, [])
        row = self.store.get_episode(episode_id)
        self.assertEqual(row["retry_count"], 0)
        self.assertIsNone(row["next_retry_at"])

    def test_runtime_tasks_override_is_used_when_base_list_tasks_is_stub(self):
        episode_id = self.episode()
        litepan = RuntimeOnlyLitePan([{
            "id": "task-7", "name": "追番", "active": True,
            "source_root": "/source", "output_root": "/lite",
        }])

        result = self.service(FakeFiles({}), litepan).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(len(litepan.trigger_calls), 1)

    def test_tasks_attribute_is_strictly_validated_when_methods_are_stubs(self):
        episode_id = self.episode()
        litepan = TasksAttributeLitePan([{
            "id": "task-7", "name": "追番", "active": True,
            "source_root": "/source", "output_root": "/lite",
        }])

        result = self.service(FakeFiles({}), litepan).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(len(litepan.trigger_calls), 1)

    def test_conflicting_task_aliases_fail_closed_without_trigger(self):
        aliases = (
            {"id": "task-7", "task_id": "task-8"},
            {"active": True, "enabled": False},
            {"active": True, "status": "disabled"},
            {"source_root": "/source", "source_path": "/other"},
            {"output_root": "/lite", "output_path": "/other"},
            {"name": "追番", "task_name": "另一个任务"},
        )
        for conflicting in aliases:
            with self.subTest(conflicting=conflicting):
                episode_id = self.episode()
                task = {
                    "id": "task-7", "name": "追番", "active": True,
                    "source_root": "/source", "output_root": "/lite",
                }
                task.update(conflicting)
                litepan = FakeLitePan(tasks=[task])

                result = self.service(FakeFiles({}), litepan).advance(
                    self.store.get_episode(episode_id)
                )

                self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
                self.assertEqual(litepan.trigger_calls, [])

    def test_equivalent_task_aliases_are_normalized_before_matching(self):
        episode_id = self.episode()
        litepan = FakeLitePan(tasks=[{
            "id": " task-7 ", "task_id": "task-7", "name": " 追番 ",
            "task_name": "追番", "active": 1, "enabled": "true",
            "status": "active", "source_root": "/source/",
            "source_path": "/source", "output_root": "/lite/",
            "output_path": "/lite",
        }])

        result = self.service(FakeFiles({}), litepan).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(len(litepan.trigger_calls), 1)

    def test_status_alias_supplies_active_state(self):
        episode_id = self.episode()
        litepan = FakeLitePan(tasks=[{
            "id": "task-7", "name": "追番", "status": "active",
            "source_root": "/source", "output_root": "/lite",
        }])

        result = self.service(FakeFiles({}), litepan).advance(
            self.store.get_episode(episode_id)
        )

        self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
        self.assertEqual(len(litepan.trigger_calls), 1)

    def test_invalid_pagination_metadata_fails_closed_without_trigger(self):
        valid = {
            "id": "task-7", "name": "追番", "active": True,
            "source_root": "/source", "output_root": "/lite",
        }
        envelopes = (
            {"tasks": [valid], "total": -1},
            {"tasks": [valid], "total": 0},
            {"tasks": [valid, dict(valid, id="task-8")], "count": 1},
            {"tasks": [valid], "page": 3, "pages": 2},
            {"tasks": [valid], "page": 1, "pages": 0},
            {"tasks": [valid], "page": 1, "pages": 2, "has_more": False},
            {"tasks": [valid], "has_more": True},
            {"tasks": [valid], "has_more": False, "next": "cursor"},
            {"tasks": [valid], "total": 1, "count": 2},
            {"tasks": [valid], "page": 1, "currentPage": 2, "pages": 2},
        )
        for envelope in envelopes:
            with self.subTest(envelope=envelope):
                episode_id = self.episode()
                litepan = MalformedInventoryLitePan(envelope)

                result = self.service(FakeFiles({}), litepan).advance(
                    self.store.get_episode(episode_id)
                )

                self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
                self.assertEqual(litepan.trigger_calls, [])

    def test_recursive_inventory_proof_rejects_ambiguity_before_trigger(self):
        valid = FakeLitePan().tasks[0]
        envelopes = [
            {"tasks": [valid], "total": 1, "meta": {"pagination": {"count": 2}}},
            {"tasks": [valid], "has_more": False, "meta": {"result": {"ok": False}}},
            {"tasks": [valid], "total": 1, "meta": {"pagination": {"unknown": 1}}},
            {"tasks": [valid], "limit": 20},
            {"data": {"tasks": [valid], "page_size": 20}},
            {"tasks": [valid]},
            {"tasks": [valid], "next": "", "meta": {"pagination": {"continuation": "x"}}},
            {"tasks": [valid], "total": 1, "data": {"count": 2}},
            {"tasks": [valid], "total": 1, "meta": []},
            {"tasks": [valid], "total": 1, "meta": {"pagination": {"count": None}}},
            {"tasks": [valid], "total": 1, "meta": {"result": {"status": 200, "code": 500}}},
            dict(valid, total=1, meta={"result": {"ok": False}}),
        ]
        for outer, inner in (
            ({"total": 1}, {"totalCount": 2}),
            ({"page": 1, "total": 1}, {"currentPage": 2}),
            ({"pages": 1, "page": 1, "total": 1}, {"page_count": 2}),
            ({"offset": 0, "total": 1}, {"startIndex": 1}),
            ({"limit": 20, "total": 1}, {"pageSize": 10}),
            ({"has_more": False}, {"hasMore": True}),
            ({"next": ""}, {"continuationToken": "cursor"}),
        ):
            envelopes.append({"root": dict(outer, tasks=[valid]), "meta": {"pagination": inner}})
        cycle = {"tasks": [valid], "total": 1}
        cycle["meta"] = cycle
        envelopes.append(cycle)
        deep = {"tasks": [valid], "total": 1}
        for _ in range(40):
            deep = {"data": deep}
        envelopes.append(deep)
        for index, envelope in enumerate(envelopes):
            with self.subTest(index=index):
                with self.assertRaises(RuntimeError):
                    _strict_litepan_task_entries(envelope)
                episode_id = self.episode()
                litepan = MalformedInventoryLitePan(envelope)
                result = self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))
                self.assertEqual(result.status, EpisodeStatus.STRM_PENDING.value)
                self.assertEqual(litepan.trigger_calls, [])

    def test_recursive_explicit_completion_preserved_by_adapter(self):
        valid = FakeLitePan().tasks[0]
        for index, proof in enumerate(({"total": "1"}, {"hasMore": False}, {"next": None}, {"next": ""})):
            with self.subTest(proof=proof):
                envelope = {"data": {"result": {"tasks": [valid]}},
                            "meta": {"pagination": dict(proof, limit=20)}}
                self.assertEqual(_strict_litepan_task_entries(envelope), [valid])
                episode_id = self.episode(key=EpisodeKey("tmdb", str(200 + index), 1, 8))
                litepan = MalformedInventoryLitePan(envelope)
                self.service(FakeFiles({}), litepan).advance(self.store.get_episode(episode_id))
                self.assertEqual(len(litepan.trigger_calls), 1)

    def test_equivalent_recursive_aliases_and_empty_inventory_are_complete(self):
        valid = FakeLitePan().tasks[0]
        for tasks in ([], [valid]):
            envelope = {"root": {"data": {"tasks": tasks}, "total": len(tasks)},
                        "meta": {"pagination": {"count": str(len(tasks)), "has_more": False,
                                                "result": {"hasMore": "false", "next": None,
                                                           "continuation": ""}}}}
            self.assertEqual(_strict_litepan_task_entries(envelope), tasks)

    def test_real_adapter_rejects_invalid_pagination_before_flattening(self):
        valid = {
            "id": "task-7", "name": "追番", "active": True,
            "source_root": "/source", "output_root": "/lite",
        }
        for envelope in (
            {"data": {"tasks": [valid]}, "pagination": {"total": 2}},
            {"data": {"tasks": [valid]}, "meta": {"has_more": True}},
            {"tasks": [valid], "total": -1},
            {"tasks": [valid], "page": 3, "pages": 2},
            {"tasks": [valid], "total": 1, "count": 2},
        ):
            with self.subTest(envelope=envelope):
                with self.assertRaises(RuntimeError):
                    _strict_litepan_task_entries(envelope)


class PublishLocalStrmTests(unittest.TestCase):
    """Publication must be idempotent against a pre-existing v3 STRM file."""

    REL = "2026-07月新番/整理/Show/Season 1/Show - S01E08.strm"

    def _layout(self, tmp, raw_payload: bytes, pub_payload: bytes):
        from pipeline.services import _publish_local_strm  # local import keeps deps tight

        pub_root = os.path.join(tmp, "pub")
        raw_root = os.path.join(tmp, "raw")
        raw_path = os.path.join(raw_root, self.REL)
        target = os.path.join(pub_root, self.REL)
        os.makedirs(os.path.dirname(raw_path), exist_ok=True)
        os.makedirs(os.path.dirname(target), exist_ok=True)
        with open(raw_path, "wb") as fh:
            fh.write(raw_payload)
        with open(target, "wb") as fh:
            fh.write(pub_payload)
        return _publish_local_strm, raw_path, target, pub_root, raw_root

    def test_republish_accepts_pre_existing_newline_terminated_identical_file(self):
        # The migrated v3 published tree stores LitePan's trailing newline.
        # Projecting the same raw content over it must be a no-op, not a
        # spurious "different content" failure.
        payload = b"http://example.invalid/api/strm/play/token\n"
        with tempfile.TemporaryDirectory() as tmp:
            publish, raw_path, target, pub_root, raw_root = self._layout(
                tmp, payload, payload,
            )
            publish(raw_path, target, pub_root, raw_root)
            with open(target, "rb") as fh:
                self.assertEqual(fh.read(), payload)

    def test_republish_still_rejects_genuinely_different_content(self):
        with tempfile.TemporaryDirectory() as tmp:
            publish, raw_path, target, pub_root, raw_root = self._layout(
                tmp,
                b"http://example.invalid/api/strm/play/new\n",
                b"http://example.invalid/api/strm/play/old\n",
            )
            with self.assertRaises(ValueError):
                publish(raw_path, target, pub_root, raw_root)


if __name__ == "__main__":
    unittest.main(verbosity=2)
