"""Regression tests for deleting the emptied fid directory after a flat move."""

import unittest

from pipeline.adapters import RemoteDirectoryMissing, RemoteInventoryIncomplete
from pipeline.real_adapters import P115CloudAdapter, _remote_file_id_token

ROOT = "/影视库/动画"
QUARTER = "2026-10月新番"
WORK = "测试作品"
SEASON_ID = "5001"
STAGING_ID = "5002"
FILE_ID = "9001"
SEASON_PATH = f"{ROOT}/{QUARTER}/整理/{WORK}/Season 1"
CANONICAL = f"{SEASON_PATH}/{WORK} - S01E01.mkv"
TOKEN = _remote_file_id_token(FILE_ID)


class _FakeClient:
    def __init__(self, state, response):
        self.state = state
        self.response = response
        self.delete_calls = []

    def fs_move(self, *args, **kwargs):
        raise AssertionError("the delete path must never move or archive")

    def fs_files(self, *args, **kwargs):
        return []

    def fs_delete(self, ids, **kwargs):
        self.delete_calls.append([str(item) for item in ids])
        if self.response.get("state") is True:
            self.state["deleted"] = True
            self.state["season"] = [entry for entry in self.state["season"]
                                    if str(entry.get("cid")) != STAGING_ID]
        return self.response


class FlatStagingDeleteTests(unittest.TestCase):
    def build(self, *, staging_entries=None, response=None):
        season_file = {"fid": FILE_ID, "pid": SEASON_ID, "n": CANONICAL.rsplit("/", 1)[-1],
                       "fc": 1, "s": 1024, "sha1": "ABCDEF0123"}
        state = {"season": [season_file, {"cid": STAGING_ID, "pid": SEASON_ID, "n": TOKEN, "fc": 0}],
                 "staging": list(staging_entries if staging_entries is not None else []),
                 "deleted": False}
        client = _FakeClient(state, response if response is not None else {"state": True})
        adapter = P115CloudAdapter(None, root_directory_id="900", root_directory_path=ROOT,
                                   credential="", client=client, max_recursive_files=1000)
        adapter.resolve_directory_path = lambda path: SEASON_ID

        def listing(directory_id):
            if str(directory_id) == SEASON_ID:
                return list(state["season"])
            if str(directory_id) == STAGING_ID and not state["deleted"]:
                return list(state["staging"])
            raise RemoteDirectoryMissing("gone", kind="directories")

        adapter.list_directory_entries = listing
        owners = [{"file_id": FILE_ID, "size": 1024, "fingerprint": "ABCDEF0123",
                   "canonical_path": CANONICAL, "staging_directory_id": STAGING_ID}]
        return adapter, client, state, owners

    def delete(self, adapter, owners):
        return adapter.delete_empty_staging_directory(
            STAGING_ID, SEASON_PATH, file_id=FILE_ID, owners=owners,
        )

    def test_deletes_the_owned_empty_directory_and_proves_absence(self):
        adapter, client, state, owners = self.build()
        result = self.delete(adapter, owners)
        self.assertEqual(result["directory_id"], STAGING_ID)
        self.assertTrue(result["deleted"])
        self.assertEqual(result["already_applied"], False)
        self.assertEqual(result["_mutation_performed"], True)
        self.assertEqual(client.delete_calls, [[STAGING_ID]])
        self.assertTrue(state["deleted"])

    def test_repeat_delete_is_idempotent_and_issues_no_second_call(self):
        adapter, client, state, owners = self.build()
        self.delete(adapter, owners)
        again = self.delete(adapter, owners)
        self.assertTrue(again["already_applied"])
        self.assertEqual(again["_mutation_performed"], False)
        self.assertEqual(len(client.delete_calls), 1)

    def test_non_empty_staging_directory_is_refused_before_any_call(self):
        adapter, client, _, owners = self.build(staging_entries=[{"fid": "9100", "pid": STAGING_ID,
                                                                  "n": "leftover.mkv", "fc": 1}])
        with self.assertRaises(RemoteInventoryIncomplete):
            self.delete(adapter, owners)
        self.assertEqual(client.delete_calls, [])

    def test_rejected_delete_envelope_never_reports_success(self):
        adapter, client, _, owners = self.build(response={"state": False, "error": "denied"})
        with self.assertRaises(RemoteInventoryIncomplete):
            self.delete(adapter, owners)
        self.assertEqual(len(client.delete_calls), 1)


class DownloadSourceDeleteTests(unittest.TestCase):
    SOURCE_PATH = f"{ROOT}/{QUARTER}/未整理/{WORK} - S01E01.mkv"
    SOURCE_ID = "6001"

    def build(self, entries):
        state = {"entries": list(entries), "deleted": False}
        client = _FakeClient(state, {"state": True})

        def delete(ids, **kwargs):
            client.delete_calls.append([str(item) for item in ids])
            state["deleted"] = True
            return {"state": True}

        client.fs_delete = delete
        adapter = P115CloudAdapter(None, root_directory_id="900", root_directory_path=ROOT,
                                   credential="", client=client, max_recursive_files=1000)

        def resolve(path):
            if path != self.SOURCE_PATH or state["deleted"]:
                raise RemoteDirectoryMissing("gone", kind="directories")
            return self.SOURCE_ID

        adapter.resolve_directory_path = resolve
        adapter.list_directory_entries = lambda directory_id: list(state["entries"])
        return adapter, client, state

    def test_empty_download_directory_is_deleted(self):
        adapter, client, state = self.build([])
        result = adapter.delete_empty_source_directory(self.SOURCE_PATH)
        self.assertTrue(result["deleted"])
        self.assertEqual(result["_mutation_performed"], True)
        self.assertEqual(client.delete_calls, [[self.SOURCE_ID]])

    def test_missing_download_directory_is_idempotent(self):
        adapter, client, state = self.build([])
        state["deleted"] = True
        result = adapter.delete_empty_source_directory(self.SOURCE_PATH)
        self.assertTrue(result["already_applied"])
        self.assertEqual(client.delete_calls, [])

    def test_non_empty_download_directory_is_kept(self):
        adapter, client, state = self.build([{"fid": "6100", "pid": self.SOURCE_ID,
                                            "n": "still.mkv", "fc": 1}])
        result = adapter.delete_empty_source_directory(self.SOURCE_PATH)
        self.assertFalse(result["deleted"])
        self.assertEqual(result["skipped"], "not_empty")
        self.assertEqual(client.delete_calls, [])

    def test_path_outside_quarter_untidy_root_is_refused(self):
        adapter, client, state = self.build([])
        with self.assertRaises(RemoteInventoryIncomplete):
            adapter.delete_empty_source_directory(f"{ROOT}/{QUARTER}/整理/{WORK}/Season 1")
        self.assertEqual(client.delete_calls, [])


if __name__ == "__main__":
    unittest.main()
