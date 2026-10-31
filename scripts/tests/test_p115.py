from __future__ import annotations

import base64
import json
import os
import tempfile
import unittest

from pipeline.executor import Executor
from pipeline.adapters import RemotePathConflict
from pipeline.real_adapters import P115CloudAdapter, RemoteInventoryIncomplete
from pipeline.state import StateStore


TARGET_HASH = "a" * 40


class FakeP115Client:
    def __init__(self, task_pages=None, file_lists=None):
        self.task_pages = task_pages or {}
        self.file_lists = file_lists or {}
        self.task_calls = []
        self.task_payloads = []
        self.file_calls = []
        self.mkdir_calls = []
        self.moves = []
        self.renames = []

    def clouddownload_task_list(self, payload):
        page = int(payload.get("page", 1)) if isinstance(payload, dict) else int(payload)
        self.task_calls.append(page)
        self.task_payloads.append(payload)
        return {"tasks": self.task_pages.get(page, [])}

    def fs_files(self, payload):
        request = dict(payload)
        directory_id = str(request["cid"])
        offset = int(request.get("offset", 0))
        limit = int(request.get("limit", 30))
        self.file_calls.append(directory_id)
        entries = self.file_lists.get(directory_id, [])
        return {
            "state": True,
            "cid": directory_id,
            "data": entries[offset:offset + limit],
            "count": len(entries),
            "offset": offset,
        }

    def fs_move(self, payload, pid=0):
        self.moves.append((payload, pid))
        return {"state": True}

    def fs_mkdir(self, payload, pid=0):
        self.mkdir_calls.append((payload, pid))
        return {"state": True, "data": {"cid": "created-dir"}}

    def fs_rename(self, payload):
        self.renames.append(payload)
        return {"state": True}

    def fs_rename_no_replace(self, payload):
        self.renames.append(payload)
        return {"state": True}


class HierarchicalP115Client:
    """Small ID-only fake for the real adapter's directory contract."""

    def __init__(self, entries=None, *, mkdir_result=None, fail_list_id=None):
        self.entries = {str(key): list(value) for key, value in (entries or {}).items()}
        self.entries.setdefault("0", [{"cid": "root", "n": "影视库", "type": "folder"}])
        self.file_calls = []
        self.mkdir_calls = []
        self.mkdir_result = mkdir_result
        self.fail_list_id = fail_list_id
        self._next_id = 1

    def fs_files(self, payload):
        request = dict(payload)
        token = str(request["cid"])
        offset = int(request.get("offset", 0))
        limit = int(request.get("limit", 30))
        self.file_calls.append(token)
        if token == str(self.fail_list_id):
            return {"state": False, "data": []}
        entries = list(self.entries.get(token, []))
        return {
            "state": True,
            "cid": token,
            "data": entries[offset:offset + limit],
            "count": len(entries),
            "offset": offset,
        }

    def fs_mkdir(self, payload, pid=0):
        parent_id = str(pid)
        self.mkdir_calls.append((payload, parent_id))
        if self.mkdir_result is not None:
            return self.mkdir_result
        name = str(
            payload.get("cname", payload.get("name"))
            if isinstance(payload, dict) else payload
        )
        directory_id = f"created-{self._next_id}"
        self._next_id += 1
        self.entries.setdefault(parent_id, []).append({
            "cid": directory_id,
            "n": name,
            "type": "folder",
        })
        self.entries[directory_id] = []
        return {
            "state": True,
            "data": {"cid": directory_id, "n": name, "name": name},
        }


def task(infohash: str, task_id: str) -> dict:
    return {"info_hash": infohash, "task_id": task_id, "status": "done"}


class P115PaginationTests(unittest.TestCase):
    def test_find_task_reaches_second_page_and_normalizes_infohash_case(self):
        pages = {1: [task("b" * 40, "remote-1")] * 30,
                 2: [task(TARGET_HASH.upper(), "remote-2")]}
        client = FakeP115Client(task_pages=pages)
        adapter = P115CloudAdapter(None, "root", client=client, page_size=30)

        found = adapter.find_task(TARGET_HASH.lower())

        self.assertIsNotNone(found)
        self.assertEqual(found.task_id, "remote-2")
        self.assertEqual(client.task_calls, [1, 2])

    def test_find_task_reaches_tenth_page(self):
        pages = {page: [task(f"{page:040x}", f"remote-{page}")] * 30 for page in range(1, 10)}
        pages[10] = [task(TARGET_HASH, "remote-10")]
        client = FakeP115Client(task_pages=pages)
        adapter = P115CloudAdapter(None, "root", client=client, page_size=30)

        self.assertEqual(adapter.find_task(TARGET_HASH).task_id, "remote-10")
        self.assertEqual(client.task_calls, list(range(1, 11)))

    def test_safety_cap_does_not_prove_absence(self):
        pages = {page: [task(f"{page:040x}", f"remote-{page}")] * 30 for page in range(1, 4)}
        client = FakeP115Client(task_pages=pages)
        adapter = P115CloudAdapter(None, "root", client=client, page_size=30, max_task_pages=3)

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.find_task(TARGET_HASH)

    def test_task_requests_include_configured_page_size_and_limit(self):
        client = FakeP115Client(task_pages={1: [task(TARGET_HASH, "remote-1")] * 2})
        adapter = P115CloudAdapter(None, "root", client=client, page_size=2)

        self.assertEqual(len(adapter.list_tasks()), 2)
        self.assertEqual(client.task_payloads, [
            {"page": 1, "page_size": 2, "limit": 2},
            {"page": 2, "page_size": 2, "limit": 2},
        ])

    def test_short_page_proof_uses_requested_size_not_remote_hint(self):
        class MisleadingPageSizeClient:
            def __init__(self):
                self.pages = {
                    1: {"tasks": [task("b" * 40, "remote-1")] * 2, "page_size": 99},
                    2: {"tasks": [task(TARGET_HASH, "remote-2")]},
                }

            def clouddownload_task_list(self, payload):
                return self.pages[int(payload["page"])]

        adapter = P115CloudAdapter(
            None, "root", client=MisleadingPageSizeClient(), page_size=2
        )

        self.assertEqual(adapter.find_task(TARGET_HASH).task_id, "remote-2")

    def test_missing_task_api_is_incomplete_not_absence(self):
        class MissingTaskClient:
            pass

        adapter = P115CloudAdapter(None, "root", client=MissingTaskClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.find_task(TARGET_HASH)

    def test_unrecognized_task_envelope_is_incomplete_not_absence(self):
        class InvalidTaskClient:
            def clouddownload_task_list(self, _payload):
                return {"unexpected": []}

        adapter = P115CloudAdapter(None, "root", client=InvalidTaskClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.find_task(TARGET_HASH)

    def test_malformed_task_json_is_incomplete_not_absence(self):
        class MalformedTaskClient:
            def clouddownload_task_list(self, _payload):
                return "not-json"

        adapter = P115CloudAdapter(None, "root", client=MalformedTaskClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.find_task(TARGET_HASH)

    def test_failed_empty_task_envelope_is_not_absence(self):
        class FailedTaskClient:
            def clouddownload_task_list(self, _payload):
                return {"state": False, "data": []}

        adapter = P115CloudAdapter(None, "root", client=FailedTaskClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.find_task(TARGET_HASH)

    def test_nested_failed_task_envelope_is_incomplete_not_absence(self):
        class NestedFailedTaskClient:
            def clouddownload_task_list(self, _payload):
                return {"data": {"success": False, "tasks": []}}

        adapter = P115CloudAdapter(None, "root", client=NestedFailedTaskClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.find_task(TARGET_HASH)

    def test_contradictory_short_page_total_raises_immediately(self):
        class ShortContradictoryClient:
            def clouddownload_task_list(self, _payload):
                return {"tasks": [task("b" * 40, "remote")], "total": 0}

        adapter = P115CloudAdapter(
            None, "root", client=ShortContradictoryClient(), page_size=30
        )

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.find_task(TARGET_HASH)

    def test_missing_task_inventory_keeps_uncertain_without_second_submit(self):
        class MissingTaskClient:
            pass

        class GuardedAdapter(P115CloudAdapter):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.submit_calls = 0

            def submit(self, _magnet):
                self.submit_calls += 1
                return "unexpected"

        with tempfile.TemporaryDirectory(prefix="p115-reconcile-") as temp:
            store = StateStore(os.path.join(temp, "state.db"))
            episode_id = store.create_episode(
                infohash=TARGET_HASH,
                work="Show",
                season=1,
                episode=1,
                stage="uncertain",
            )
            cloud = GuardedAdapter(None, "root", client=MissingTaskClient())
            executor = Executor(
                store,
                cloud,
                object(),
                object(),
                object(),
                object(),
                lambda _url: "",
                os.path.join(temp, "run.lock"),
                {"strm_root": os.path.join(temp, "strm")},
            )

            executor.run([], discover=False)

            submit_calls = cloud.submit_calls
            stage = store.get_by_id(episode_id)["stage"]
            store.close()
            self.assertEqual(submit_calls, 0)
            self.assertEqual(stage, "uncertain")

    def test_nested_failed_inventory_keeps_uncertain_across_two_rounds(self):
        class NestedFailedTaskClient:
            def clouddownload_task_list(self, _payload):
                return {"data": {"success": False, "tasks": []}}

        class GuardedAdapter(P115CloudAdapter):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.submit_calls = 0

            def submit(self, _magnet):
                self.submit_calls += 1
                return "unexpected"

        with tempfile.TemporaryDirectory(prefix="p115-nested-reconcile-") as temp:
            store = StateStore(os.path.join(temp, "state.db"))
            episode_id = store.create_episode(
                infohash=TARGET_HASH,
                work="Show",
                season=1,
                episode=1,
                stage="uncertain",
            )
            cloud = GuardedAdapter(None, "root", client=NestedFailedTaskClient())
            executor = Executor(
                store,
                cloud,
                object(),
                object(),
                object(),
                object(),
                lambda _url: "",
                os.path.join(temp, "run.lock"),
                {"strm_root": os.path.join(temp, "strm")},
            )

            executor.run([], discover=False)
            executor.run([], discover=False)

            submit_calls = cloud.submit_calls
            stage = store.get_by_id(episode_id)["stage"]
            store.close()
            self.assertEqual(submit_calls, 0)
            self.assertEqual(stage, "uncertain")

    def test_contradictory_total_keeps_uncertain_across_two_rounds(self):
        class ShortContradictoryClient:
            def clouddownload_task_list(self, _payload):
                return {"tasks": [task("b" * 40, "remote")], "total": 0}

        class GuardedAdapter(P115CloudAdapter):
            def __init__(self, *args, **kwargs):
                super().__init__(*args, **kwargs)
                self.submit_calls = 0

            def submit(self, _magnet):
                self.submit_calls += 1
                return "unexpected"

        with tempfile.TemporaryDirectory(prefix="p115-total-reconcile-") as temp:
            store = StateStore(os.path.join(temp, "state.db"))
            episode_id = store.create_episode(
                infohash=TARGET_HASH,
                work="Show",
                season=1,
                episode=1,
                stage="uncertain",
            )
            cloud = GuardedAdapter(None, "root", client=ShortContradictoryClient())
            executor = Executor(
                store,
                cloud,
                object(),
                object(),
                object(),
                object(),
                lambda _url: "",
                os.path.join(temp, "run.lock"),
                {"strm_root": os.path.join(temp, "strm")},
            )

            executor.run([], discover=False)
            executor.run([], discover=False)

            submit_calls = cloud.submit_calls
            stage = store.get_by_id(episode_id)["stage"]
            store.close()
            self.assertEqual(submit_calls, 0)
            self.assertEqual(stage, "uncertain")

    def test_recognized_empty_task_page_proves_absence(self):
        client = FakeP115Client(task_pages={1: []})
        adapter = P115CloudAdapter(None, "root", client=client)

        self.assertIsNone(adapter.find_task(TARGET_HASH))

    def test_contradictory_total_cannot_prove_completion(self):
        class ContradictoryClient:
            def clouddownload_task_list(self, _payload):
                return {"tasks": [task("b" * 40, "remote")] * 30, "total": 0}

        adapter = P115CloudAdapter(
            None, "root", client=ContradictoryClient(), page_size=30, max_task_pages=1
        )

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.find_task(TARGET_HASH)

    def test_base32_task_hash_is_normalized_and_invalid_alnum_is_rejected(self):
        raw = bytes.fromhex(TARGET_HASH)
        base32_hash = base64.b32encode(raw).decode().rstrip("=")
        client = FakeP115Client(task_pages={1: [task(base32_hash, "remote-base32")]})
        adapter = P115CloudAdapter(None, "root", client=client)

        self.assertEqual(adapter.find_task(TARGET_HASH).task_id, "remote-base32")
        with self.assertRaises(ValueError):
            adapter._infohash_from("g" * 40)

    def test_runtime_config_injects_p115_inventory_caps(self):
        runtime = {"limits": {
            "max_task_pages": 7,
            "max_recursive_depth": 4,
            "max_recursive_files": 23,
        }}
        adapter = P115CloudAdapter(
            None,
            "root",
            client=FakeP115Client(),
            runtime_config=runtime,
        )

        self.assertEqual(adapter.max_task_pages, 7)
        self.assertEqual(adapter.max_recursive_depth, 4)
        self.assertEqual(adapter.max_recursive_files, 23)

    def test_runtime_config_supplies_remote_root_path_without_using_it_as_id(self):
        adapter = P115CloudAdapter(
            None,
            "root-id",
            client=FakeP115Client(),
            runtime_config={
                "p115": {"root_directory_path": "/影视库/动画"},
                "limits": {
                    "max_task_pages": 1,
                    "max_recursive_depth": 1,
                    "max_recursive_files": 1,
                },
            },
        )

        self.assertEqual(adapter.root_directory_id, "root-id")
        self.assertEqual(adapter.root_directory_path, "/影视库/动画")


class P115FileInventoryTests(unittest.TestCase):
    def test_incomplete_identity_listings_block_all_writes_at_every_ancestor(self):
        from unittest.mock import Mock

        # A write must stop when any directory in the configured identity
        # chain omits or contradicts the count/offset pagination contract.
        failure_cases = ("missing_count", "wrong_count", "missing_offset", "wrong_offset")
        for bad_id in ("0", "media", "anime"):
            for failure in failure_cases:
                with self.subTest(id=bad_id, failure=failure):
                    class PartialClient(HierarchicalP115Client):
                        def fs_files(self, payload):
                            result = super().fs_files(payload)
                            if str(payload["cid"]) != bad_id:
                                return result
                            result = dict(result)
                            if failure == "missing_count":
                                result.pop("count")
                            elif failure == "wrong_count":
                                result["count"] += 1
                            elif failure == "missing_offset":
                                result.pop("offset")
                            else:
                                result["offset"] += 1
                            return result

                    client = PartialClient({
                        "0": [{"cid": "media", "n": "影视库"}],
                        "media": [{"cid": "anime", "n": "动画"}],
                        "anime": [],
                    })
                    client.fs_move = Mock()
                    client.fs_rename_no_replace = Mock()
                    adapter = P115CloudAdapter(None, "anime", client=client,
                        root_directory_path="/影视库/动画", max_recursive_files=1)
                    for operation in (adapter.mkdir, adapter.get_file):
                        with self.assertRaises(RemoteInventoryIncomplete):
                            operation("/影视库/动画/New Season")
                    self.assertEqual(client.mkdir_calls, [])
                    client.fs_move.assert_not_called()
                    client.fs_rename_no_replace.assert_not_called()

    def test_complete_count_offset_listing_accepts_consistent_evidence(self):
        client = HierarchicalP115Client({"root": []})
        adapter = P115CloudAdapter(None, "root", client=client,
            root_directory_path="/影视库")
        self.assertEqual(adapter.mkdir("/影视库/New")["directory_id"], "created-1")
        self.assertEqual(len(client.mkdir_calls), 1)

    def test_inconsistent_root_count_or_offset_blocks_write(self):
        for metadata in ({"count": 2}, {"offset": 1}):
            with self.subTest(metadata=metadata):
                class InconsistentClient(HierarchicalP115Client):
                    def fs_files(self, payload):
                        result = super().fs_files(payload)
                        if str(payload["cid"]) == "0":
                            return {**result, **metadata}
                        return result

                client = InconsistentClient({"root": []})
                adapter = P115CloudAdapter(None, "root", client=client,
                    root_directory_path="/影视库")
                with self.assertRaises(RemoteInventoryIncomplete):
                    adapter.mkdir("/影视库/New")
                self.assertEqual(client.mkdir_calls, [])

    def test_bare_list_cannot_prove_path_completeness_but_inventory_is_unchanged(self):
        class BareClient(HierarchicalP115Client):
            def fs_files(self, payload):
                return super().fs_files(payload)["data"]

        client = BareClient({"root": []})
        adapter = P115CloudAdapter(None, "root", client=client,
            root_directory_path="/影视库")
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter._ls("root")
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/New")
        self.assertEqual(client.mkdir_calls, [])

    def test_positive_root_chain_permits_write_and_is_rechecked_on_next_operation(self):
        client = HierarchicalP115Client({
            "0": [{"cid": "media", "n": "影视库", "type": "folder"}],
            "media": [{"cid": "anime", "n": "动画", "type": "folder"}],
            "anime": [],
        })
        adapter = P115CloudAdapter(None, "anime", client=client,
            root_directory_path="/影视库/动画")
        result = adapter.mkdir("/影视库/动画/New Season")
        self.assertEqual(result["directory_id"], "created-1")
        self.assertEqual(client.file_calls[:3], ["0", "media", "anime"])
        self.assertEqual(client.mkdir_calls, [({"cname": "New Season"}, "anime")])
        client.entries["media"] = [{"cid": "replacement", "n": "动画", "type": "folder"}]
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/动画/Other Season")
        self.assertEqual(len(client.mkdir_calls), 1)

    def test_root_identity_requires_positive_chain_before_any_mkdir(self):
        for configured_id in ("media", "season", "unrelated"):
            with self.subTest(configured_id=configured_id):
                client = HierarchicalP115Client({
                    "0": [{"cid": "media", "n": "影视库", "type": "folder"}],
                    "media": [{"cid": "anime", "n": "动画", "type": "folder"}],
                    "anime": [{"cid": "season", "n": "Season 1", "type": "folder"}],
                    "season": [], "unrelated": [],
                })
                adapter = P115CloudAdapter(None, configured_id, client=client,
                    root_directory_path="/影视库/动画")
                with self.assertRaises(RemoteInventoryIncomplete):
                    adapter.mkdir("/影视库/动画/New Season")
                self.assertEqual(client.mkdir_calls, [])

    def test_root_children_alone_cannot_authorize_writes(self):
        client = HierarchicalP115Client({"0": [], "root": []})
        adapter = P115CloudAdapter(None, "root", client=client,
            root_directory_path="/影视库")
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/New Season")
        self.assertEqual(client.mkdir_calls, [])

    def test_unavailable_top_level_inventory_cannot_authorize_writes(self):
        client = HierarchicalP115Client({"root": []}, fail_list_id="0")
        adapter = P115CloudAdapter(None, "root", client=client,
            root_directory_path="/影视库")
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/New Season")
        self.assertEqual(client.mkdir_calls, [])

    def test_executor_rejects_missing_p115_root_path_before_client_construction(self):
        from anime_pipeline import build_executor
        from unittest.mock import patch
        for id_key in ("root_directory_id", "target_directory_id"):
            with self.subTest(id_key=id_key):
                config = {"p115": {"credential_file": "unused", id_key: "root"},
                          "paths": {"cloud_library_root": "/影视库/动画"}}
                with patch("pipeline.real_adapters.P115CloudAdapter") as client:
                    with self.assertRaisesRegex(ValueError, "root_directory_path"):
                        build_executor(config)
                client.assert_not_called()

    def setUp(self):
        self.client = FakeP115Client(
            task_pages={1: [task(TARGET_HASH, "remote") ]},
            file_lists={
                "root": [
                    {"cid": "folder-1", "n": "Season.mkv", "type": "folder"},
                    {"cid": "folder-2", "n": "Show.S01.mkv", "fc": 1},
                ],
                "folder-1": [
                    {"fid": "file-1", "n": "episode-without-extension", "size": 123},
                ],
                "folder-2": [
                    {"fid": "file-2", "n": "episode.01.mkv", "size": 456},
                ],
            },
        )
        self.adapter = P115CloudAdapter(
            None,
            "root",
            client=self.client,
            max_recursive_depth=3,
            max_recursive_directories=10,
            max_recursive_files=10,
        )

    def test_recursive_listing_uses_metadata_and_ids_for_dotted_folders(self):
        files = self.adapter.list_files("root")

        self.assertEqual({item["file_id"] for item in files}, {"file-1", "file-2"})
        self.assertEqual({item["name"] for item in files}, {
            "Season.mkv/episode-without-extension",
            "Show.S01.mkv/episode.01.mkv",
        })
        self.assertTrue(all(item["is_dir"] is False for item in files))

    def test_recursive_file_cap_is_explicit(self):
        self.adapter.max_recursive_files = 1
        with self.assertRaises(RemoteInventoryIncomplete):
            self.adapter.list_files("root")

    def test_missing_file_api_is_incomplete_not_empty(self):
        class MissingFileClient:
            def clouddownload_task_list(self, _payload):
                return {"tasks": []}

        adapter = P115CloudAdapter(None, "root", client=MissingFileClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter._ls("root")

    def test_unrecognized_file_envelope_is_incomplete_not_empty(self):
        class InvalidFileClient:
            def clouddownload_task_list(self, _payload):
                return {"tasks": []}

            def fs_files(self, _payload):
                return {"unexpected": []}

        adapter = P115CloudAdapter(None, "root", client=InvalidFileClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter._ls("root")

    def test_malformed_file_json_is_incomplete_not_empty(self):
        class MalformedFileClient:
            def clouddownload_task_list(self, _payload):
                return {"tasks": []}

            def fs_files(self, _payload):
                return "not-json"

        adapter = P115CloudAdapter(None, "root", client=MalformedFileClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter._ls("root")

    def test_failed_empty_file_envelope_is_not_empty(self):
        class FailedFileClient:
            def clouddownload_task_list(self, _payload):
                return {"tasks": []}

            def fs_files(self, _payload):
                return {"success": False, "data": []}

        adapter = P115CloudAdapter(None, "root", client=FailedFileClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter._ls("root")

    def test_nested_failed_file_envelope_is_incomplete_not_empty(self):
        class NestedFailedFileClient:
            def clouddownload_task_list(self, _payload):
                return {"tasks": []}

            def fs_files(self, _payload):
                return {"data": {"success": False, "files": []}}

        adapter = P115CloudAdapter(None, "root", client=NestedFailedFileClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter._ls("root")

    def test_move_and_rename_delegate_without_http_or_filename_guessing(self):
        self.adapter.move_file("file-1", "target-dir")
        self.adapter.rename_file("file-1", "renamed-file.mkv")

        self.assertEqual(len(self.client.moves), 1)
        self.assertEqual(len(self.client.renames), 1)

    def test_get_file_and_mkdir_resolve_each_component_from_root_id(self):
        path = "/影视库/动画/2026-10月新番/Show/Season 1/episode.mkv"
        entries = {
            "root": [{"cid": "anime", "n": "动画", "type": "folder"}],
            "anime": [{"cid": "quarter", "n": "2026-10月新番", "type": "folder"}],
            "quarter": [{"cid": "show", "n": "Show", "type": "folder"}],
            "show": [{"cid": "season", "n": "Season 1", "type": "folder"}],
            "season": [{"fid": "file-1", "n": "episode.mkv", "type": "file", "path": path}],
        }
        client = HierarchicalP115Client(entries)
        adapter = P115CloudAdapter(
            None,
            "root",
            client=client,
            root_directory_path="/影视库",
        )

        file_value = adapter.get_file(path)
        directory = adapter.mkdir("/影视库/动画/2026-10月新番/Show/New Season")

        self.assertEqual(file_value["file_id"], "file-1")
        self.assertEqual(directory["directory_id"], "created-1")
        self.assertEqual(client.file_calls[:6], ["0", "root", "anime", "quarter", "show", "season"])
        self.assertTrue(all("/" not in token for token in client.file_calls))
        self.assertEqual(
            client.mkdir_calls,
            [({"cname": "New Season"}, "show")],
        )

    def test_path_operations_require_explicit_root_path_without_guessing_first_component(self):
        client = HierarchicalP115Client({"root": []})
        adapter = P115CloudAdapter(None, "root", client=client)

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.get_file("/影视库/动画/episode.mkv")
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/动画/Season 1")
        self.assertEqual(client.file_calls, [])
        self.assertEqual(client.mkdir_calls, [])

    def test_deeper_root_id_mismatch_fails_before_mkdir_write(self):
        client = HierarchicalP115Client({
            "root": [{
                "cid": "anime",
                "n": "动画",
                "type": "folder",
                "path": "/影视库/动画",
            }],
            "anime": [],
        })
        adapter = P115CloudAdapter(
            None, "root", client=client, root_directory_path="/影视库/动画"
        )

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/动画/2026-10月新番/Show/Season 1")
        self.assertEqual(client.mkdir_calls, [])

    def test_get_file_does_not_use_path_lookup_method(self):
        client = HierarchicalP115Client({
            "root": [{"cid": "anime", "n": "动画", "type": "folder"}],
            "anime": [{"fid": "file-1", "n": "episode.mkv", "type": "file"}],
        })
        client.fs_get_file = lambda _path: (_ for _ in ()).throw(
            AssertionError("path lookup must not be used")
        )
        adapter = P115CloudAdapter(
            None, "root", client=client, root_directory_path="/影视库"
        )

        result = adapter.get_file("/影视库/动画/episode.mkv")

        self.assertEqual(result["file_id"], "file-1")

    def test_get_file_and_mkdir_fail_closed_when_real_wrappers_are_unavailable(self):
        class MissingMethods:
            pass

        adapter = P115CloudAdapter(None, "root", client=MissingMethods())
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.get_file("/影视库/动画/Show/episode.mkv")
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/动画/Show/Season 1")

    def test_atomic_rename_wrapper_sets_no_replace_contract(self):
        self.adapter.rename_file_no_replace(
            "file-1",
            "episode.mkv",
            expected_identity={"remote_file_id": "file-1"},
            target_path="/影视库/动画/Show/episode.mkv",
        )
        self.assertEqual(self.client.renames[-1]["replace"], False)
        self.assertEqual(self.client.renames[-1]["overwrite"], False)

    def test_fs_files_rejects_path_tokens_instead_of_using_path_as_directory_id(self):
        with self.assertRaises(RemoteInventoryIncomplete):
            self.adapter.fs_files("/影视库/动画")
        with self.assertRaises(RemoteInventoryIncomplete):
            self.adapter.fs_files({"cid": "root"})
        self.assertEqual(self.client.file_calls, [])

    def test_mkdir_rejects_directory_response_without_explicit_id(self):
        client = HierarchicalP115Client(
            {"root": []},
            mkdir_result={"state": True, "data": {"path": "/影视库/动画"}},
        )
        adapter = P115CloudAdapter(
            None, "root", client=client, root_directory_path="/影视库"
        )

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/动画")

    def test_mkdir_failed_envelope_is_incomplete_before_reconciliation(self):
        client = HierarchicalP115Client(
            {"root": []},
            mkdir_result={"state": False, "data": {"cid": "should-not-use"}},
        )
        adapter = P115CloudAdapter(
            None, "root", client=client, root_directory_path="/影视库"
        )

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/动画")
        self.assertEqual(client.file_calls, ["0", "root"])

    def test_mkdir_nested_failed_envelope_is_incomplete(self):
        client = HierarchicalP115Client(
            {"root": []},
            mkdir_result={"data": {"success": False, "cid": "should-not-use"}},
        )
        adapter = P115CloudAdapter(
            None, "root", client=client, root_directory_path="/影视库"
        )

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/动画")

    def test_directory_lookup_duplicate_names_is_ambiguous(self):
        client = HierarchicalP115Client({
            "root": [
                {"cid": "anime-1", "n": "动画", "type": "folder"},
                {"cid": "anime-2", "n": "动画", "type": "folder"},
            ],
        })
        adapter = P115CloudAdapter(
            None, "root", client=client, root_directory_path="/影视库"
        )

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.get_file("/影视库/动画/Show/episode.mkv")

    def test_directory_lookup_failed_envelope_is_incomplete(self):
        client = HierarchicalP115Client({"root": []}, fail_list_id="root")
        adapter = P115CloudAdapter(
            None, "root", client=client, root_directory_path="/影视库"
        )

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.mkdir("/影视库/动画")

    def test_plain_fs_rename_is_not_treated_as_atomic_even_with_flags(self):
        class FlagsIgnoredClient:
            def __init__(self):
                self.called = False

            def fs_rename(self, _payload):
                self.called = True
                return {"state": True}

        client = FlagsIgnoredClient()
        adapter = P115CloudAdapter(None, "root", client=client)

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.rename_file_no_replace("file-1", "episode.mkv")
        self.assertFalse(client.called)

    def test_atomic_rename_conflict_is_a_path_conflict_not_incomplete_inventory(self):
        class ConflictClient:
            def __init__(self):
                self.calls = []

            def fs_rename_no_replace(self, payload):
                self.calls.append(payload)
                return {
                    "state": False,
                    "reason": "target_exists",
                    "code": "EEXIST",
                }

        client = ConflictClient()
        adapter = P115CloudAdapter(None, "root", client=client)

        with self.assertRaises(RemotePathConflict):
            adapter.rename_file_no_replace("file-1", "episode.mkv")
        self.assertEqual(len(client.calls), 1)

    def test_atomic_rename_unknown_failure_remains_incomplete_inventory(self):
        class FailureClient:
            def fs_rename_no_replace(self, _payload):
                return {"success": False, "code": "EIO"}

        adapter = P115CloudAdapter(None, "root", client=FailureClient())

        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.rename_file_no_replace("file-1", "episode.mkv")


if __name__ == "__main__":
    unittest.main(verbosity=2)
