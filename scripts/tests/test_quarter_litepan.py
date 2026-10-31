import copy
import hashlib
import json
import sqlite3
import tempfile
import unittest
from unittest.mock import Mock, patch
from urllib.parse import parse_qs, urlsplit
from pathlib import Path

from pipeline.quarter_litepan import QuarterLitePanClient, LitePanResourceError


TASK = {"name": "2026-10", "account_id": 1, "parent_id": "123", "path": "/library/2026-10",
        "recursive": True, "output_folder": "2026-10", "group_dir": "追番-v5raw",
        "schedule_mode": "manual", "status": "active", "sync_metadata": False,
        "time_window_enabled": False, "scan_mode": "incremental_update"}
RULE = {"name": "v5-quarter", "trigger_type": "webhook", "trigger_config": {"event": "v5-q-2026-10", "source": "hermes-v5"},
        "status": "running", "actions": [{"id": "v5-strm", "type": "strm", "name": "", "condition": "always", "params": {"task_id": 1, "run_mode": "full"}}]}


class Fake:
    def __init__(self):
        self.tasks, self.rules, self.calls = [], [], []
        self.admin = True
        self.after_post = True
        self.static = b"verified-static-fixture"

    def __call__(self, method, url, **kwargs):
        self.calls.append((method, url, kwargs))
        if url.endswith("/") or "/assets/" in url:
            return 200, self.static
        if url.endswith("/auth/status"):
            return 200, {"success": True, "data": {"is_admin": self.admin}}
        rows = self.tasks if url.endswith("strm/tasks") else self.rules
        if method == "POST" and self.after_post:
            rows.append(dict(copy.deepcopy(kwargs["body"]), id=len(rows)+1))
        return 200, {"success": True, "data": copy.deepcopy(rows)}


class QuarterLitePanTests(unittest.TestCase):
    def setUp(self):
        # Use deterministic tiny fixture bytes; production hashes remain pinned.
        fixture_hash = hashlib.sha256(b"verified-static-fixture").hexdigest()
        guard = patch.object(QuarterLitePanClient, "STATIC_HASHES",
                             {path: fixture_hash for path in QuarterLitePanClient.STATIC_HASHES})
        guard.start()
        self.addCleanup(guard.stop)

    def client(self, fake, **kwargs):
        return QuarterLitePanClient("http://localhost:5211", "admin_session=opaque", fake, **kwargs)

    def test_missing_cookie_blocks_creation_but_not_db_read(self):
        with tempfile.TemporaryDirectory() as tmp:
            db = Path(tmp)/"litepan.db"
            with sqlite3.connect(db) as con:
                fields = "id INTEGER," + ",".join(k + " TEXT" for k in QuarterLitePanClient.TASK_PAYLOAD)
                con.execute("CREATE TABLE strm_tasks (" + fields + ")")
                con.execute("CREATE TABLE automation_rules (id INTEGER,name TEXT,trigger_type TEXT,trigger_config TEXT,actions_json TEXT,status TEXT)")
            before = db.read_bytes()
            client = QuarterLitePanClient("http://localhost:5211", task_db_path=str(db))
            self.assertEqual(client.creation_blockers(), ["LITEPAN_ADMIN_SESSION_REQUIRED"])
            self.assertEqual(client.inventory(TASK, RULE)["litepan_task"]["state"], "create")
            self.assertEqual(db.read_bytes(), before)

    def test_create_manual_task_only_and_readback(self):
        fake = Fake()
        client = self.client(fake)
        self.assertEqual(client.create_task(TASK)["state"], "reuse")
        posts = [c for c in fake.calls if c[0] == "POST"]
        self.assertEqual(len(posts), 1)
        self.assertTrue(posts[0][1].endswith("/api/admin/strm/tasks"))
        self.assertEqual(posts[0][2]["headers"]["Origin"], "http://localhost:5211")
        self.assertEqual(posts[0][2]["body"]["schedule_mode"], "manual")
        client.create_task(TASK)
        self.assertEqual(len([c for c in fake.calls if c[0] == "POST"]), 1)

    def test_readback_failure_is_failure(self):
        fake = Fake()
        fake.after_post = False
        with self.assertRaisesRegex(LitePanResourceError, "LITEPAN_TASK_READBACK_FAILED"):
            self.client(fake).create_task(TASK)
        self.assertEqual(len([c for c in fake.calls if c[0] == "POST"]), 1)

    def test_unsafe_task_never_posts(self):
        for changes in ({"schedule_mode": "window"}, {"status": "paused"}, {"group_dir": "../other"}, {"account_id": True},
                        {"sync_metadata": True}, {"sync_metadata": 0}, {"scan_mode": "incremental"}):
            fake = Fake()
            with self.assertRaisesRegex(LitePanResourceError, "LITEPAN_TASK_SPEC_UNSAFE"):
                self.client(fake).create_task(dict(TASK, **changes))
            self.assertFalse(any(c[0] == "POST" for c in fake.calls))

    def test_duplicate_and_conflicting_tasks(self):
        fake = Fake()
        fake.tasks = [dict(TASK, id=1), dict(TASK, id=2)]
        self.assertEqual(self.client(fake).inventory(TASK, RULE)["litepan_task"]["state"], "conflict")
        fake.tasks = [dict(TASK, id=1, schedule_mode="window")]
        self.assertEqual(self.client(fake).inventory(TASK, RULE)["litepan_task"]["state"], "conflict")

    def test_explicit_task_settings_must_match_inventory(self):
        for changes in ({"sync_metadata": True}, {"time_window_enabled": True},
                        {"scan_mode": "full_sync"}, {"extensions": ".mkv"}):
            fake = Fake()
            spec = dict(TASK, extensions="")
            fake.tasks = [dict(spec, id=1, **changes)]
            self.assertEqual(self.client(fake).inventory(spec, RULE)["litepan_task"]["state"], "conflict")

    def test_missing_metadata_policy_rejected_before_post(self):
        fake = Fake()
        spec = dict(TASK)
        spec.pop("sync_metadata")
        with self.assertRaisesRegex(LitePanResourceError, "LITEPAN_TASK_SPEC_UNSAFE"):
            self.client(fake).create_task(spec)
        self.assertFalse(any(c[0] == "POST" for c in fake.calls))

    def test_create_readback_rejects_changed_metadata_policy(self):
        fake = Fake()
        def transport(method, url, **kwargs):
            result = fake(method, url, **kwargs)
            if method == "POST":
                fake.tasks[0]["sync_metadata"] = True
            return result
        with self.assertRaisesRegex(LitePanResourceError, "LITEPAN_TASK_READBACK_FAILED"):
            self.client(transport).create_task(TASK)
        self.assertEqual(len([c for c in fake.calls if c[0] == "POST"]), 1)

    def test_partial_envelope_not_absence(self):
        def transport(*args, **kwargs):
            return 200, {"success": True, "data": {"items": [], "total": 10}}
        client = self.client(transport)
        self.assertEqual(client.inventory(TASK, RULE)["litepan_task"]["state"], "unknown")

    def test_rule_only_webhook_and_manual_target(self):
        fake = Fake()
        fake.tasks = [dict(TASK, id=1)]
        client = self.client(fake)
        self.assertEqual(client.create_rule(RULE)["state"], "reuse")
        self.assertEqual(len([c for c in fake.calls if c[0] == "POST"]), 1)
        fake.tasks[0]["schedule_mode"] = "window"
        with self.assertRaisesRegex(LitePanResourceError, "LITEPAN_RULE_TARGET_UNSAFE"):
            client.create_rule(RULE)

    def test_non_admin_no_post(self):
        fake = Fake()
        fake.admin = False
        with self.assertRaisesRegex(LitePanResourceError, "LITEPAN_ADMIN_SESSION_INVALID"):
            self.client(fake).create_task(TASK)
        self.assertFalse(any(c[0] == "POST" for c in fake.calls))

    def test_secrets_not_in_errors(self):
        def broken(*args, **kwargs):
            raise RuntimeError("admin_session=SECRET")
        with self.assertRaisesRegex(LitePanResourceError, "^LITEPAN_API_UNAVAILABLE$"):
            self.client(broken).read_tasks()

    def test_rule_collision_event_and_source(self):
        fake = Fake()
        fake.rules = [dict(RULE, id=1, name="other")]
        self.assertEqual(self.client(fake).inventory(TASK, RULE)["litepan_rule"]["state"], "conflict")

    def test_upgrade_rejected_without_post_and_rechecked_each_time(self):
        fake = Fake()
        client = self.client(fake)
        self.assertEqual(client.creation_blockers(), [])
        public_calls = [c for c in fake.calls if "/assets/" in c[1] or c[1].endswith("/")]
        self.assertEqual(len(public_calls), 5)
        self.assertTrue(all(c[2]["headers"] == {} for c in public_calls))
        fake.static = b"new-version"
        self.assertEqual(client.creation_blockers(), ["LITEPAN_VERSION_UNSUPPORTED"])
        with self.assertRaisesRegex(LitePanResourceError, "LITEPAN_VERSION_UNSUPPORTED"):
            client.create_task(TASK)
        self.assertFalse(any(c[0] == "POST" for c in fake.calls))

    def test_unknown_static_response_is_not_version_proof(self):
        fake = Fake()
        fake.static = {"success": True, "data": "v0.5.5-beta"}
        self.assertEqual(self.client(fake).creation_blockers(), ["LITEPAN_VERSION_UNVERIFIED"])

    def account_client(self, tmp, *, provider="115_open", active=1, status="active", access_token="fixture-access-token"):
        path = Path(tmp)/"accounts.db"
        with sqlite3.connect(path) as db:
            db.execute("CREATE TABLE cloud_accounts (id INTEGER,driver_type TEXT,is_active INTEGER,config TEXT)")
            db.execute("CREATE TABLE account_auth_states (account_id INTEGER,status TEXT,access_token TEXT,refresh_token TEXT,cookie TEXT)")
            db.execute("INSERT INTO cloud_accounts VALUES (1,?,?,?)", (provider, active, "DO_NOT_READ_CONFIG"))
            db.execute("INSERT INTO account_auth_states VALUES (1,?,?,?,?)", (status, access_token, "DO_NOT_READ_REFRESH", "DO_NOT_USE_COOKIE"))
        return QuarterLitePanClient("http://localhost:5211", task_db_path=str(path)), path

    def open_transport(self, *, user_id=77, mutate=None):
        calls = []
        tree = {"0": [("10", "cloud", "0")], "10": [("20", "library", "0")],
                "20": [("30", "2026-07", "0")], "30": []}
        def transport(method, url, **kwargs):
            self.assertEqual(method, "GET")
            self.assertIsNone(kwargs["body"])
            self.assertEqual(urlsplit(url).netloc, "proapi.115.com")
            self.assertEqual(kwargs["headers"], {"Authorization": "Bearer fixture-access-token"})
            calls.append((method, url))
            if urlsplit(url).path == "/open/user/info":
                return 200, {"state": True, "data": {"user_id": user_id}}
            self.assertEqual(urlsplit(url).path, "/open/ufile/files")
            query = parse_qs(urlsplit(url).query)
            parent = query["cid"][0]
            offset = int(query["offset"][0])
            rows = [{"file_id": i, "file_name": n, "file_category": c, "parent_id": parent}
                    for i, n, c in tree[parent]]
            response = {"state": True, "count": len(rows), "data": rows[offset:offset+300]}
            return 200, mutate(parent, offset, response) if mutate else response
        return transport, calls, tree

    def verify(self, client, transport, **changes):
        args = dict(expected_user_id=77, root_directory_id="20", root_directory_path="/cloud/library",
                    target_path="/cloud/library/2026-07", expected_target_id="30", transport=transport)
        args.update(changes)
        return client.verify_account_binding(1, **args)

    def test_account_metadata_is_not_credential_or_identity_proof(self):
        with tempfile.TemporaryDirectory() as tmp:
            client, path = self.account_client(tmp)
            before = path.read_bytes()
            result = client.read_account_identity(1)
            self.assertEqual(result["state"], "unknown")
            self.assertEqual(result["reason"], "LITEPAN_ACCOUNT_OAUTH_REQUIRES_VERIFICATION")
            self.assertEqual(path.read_bytes(), before)
            self.assertFalse(any(word in json.dumps(result) for word in ("fixture-access-token", "DO_NOT_")))

    def test_oauth_binding_requires_same_user_and_complete_root_chain(self):
        with tempfile.TemporaryDirectory() as tmp:
            client, path = self.account_client(tmp)
            before = path.read_bytes()
            transport, calls, tree = self.open_transport()
            result = self.verify(client, transport)
            self.assertEqual(result["state"], "verified")
            self.assertEqual(result["target_directory_id"], "30")
            self.assertEqual(len(calls), 5)  # user info plus root, cloud, library, target
            self.assertEqual(path.read_bytes(), before)
            self.assertNotIn("fixture-access-token", json.dumps(result))

    def test_oauth_user_root_and_target_mismatch(self):
        with tempfile.TemporaryDirectory() as tmp:
            client, _ = self.account_client(tmp)
            transport, calls, _ = self.open_transport(user_id=88)
            self.assertEqual(self.verify(client, transport)["reason"], "LITEPAN_ACCOUNT_USER_MISMATCH")
            self.assertEqual(len(calls), 1)
            transport, _, _ = self.open_transport()
            self.assertEqual(self.verify(client, transport, root_directory_id="999")["reason"], "LITEPAN_ACCOUNT_ROOT_MISMATCH")
            self.assertEqual(self.verify(client, transport, expected_target_id="999")["reason"], "LITEPAN_ACCOUNT_TARGET_MISMATCH")

    def test_oauth_pagination_is_complete_and_bounded(self):
        with tempfile.TemporaryDirectory() as tmp:
            client, _ = self.account_client(tmp)
            transport, calls, tree = self.open_transport()
            tree["0"] += [(str(1000+i), "file"+str(i), "3") for i in range(301)]
            self.assertEqual(self.verify(client, transport)["state"], "verified")
            offsets = [parse_qs(urlsplit(url).query)["offset"][0] for _, url in calls if "cid=0&" in url]
            self.assertEqual(offsets, ["0", "300"])
            self.assertEqual(self.verify(client, transport, max_pages=1)["reason"], "LITEPAN_ACCOUNT_INVENTORY_LIMIT")

    def test_oauth_incomplete_duplicate_or_wrong_parent_fails_closed(self):
        with tempfile.TemporaryDirectory() as tmp:
            client, _ = self.account_client(tmp)
            for mode in ("incomplete", "duplicate", "wrong_parent", "changed_total"):
                def mutate(parent, offset, page):
                    if parent != "0":
                        return page
                    if mode == "incomplete":
                        page["count"] = 2
                        if offset:
                            page["data"] = []
                    elif mode == "duplicate":
                        page["data"] *= 2
                        page["count"] = 2
                    elif mode == "wrong_parent":
                        page["data"][0]["parent_id"] = "99"
                    else:
                        page["count"] = 2 if not offset else 3
                        if offset:
                            page["data"] = []
                    return page
                transport, _, _ = self.open_transport(mutate=mutate)
                self.assertEqual(self.verify(client, transport)["state"], "unknown", mode)

    def test_oauth_invalid_auth_never_refreshes_or_uses_cookie(self):
        for changes, expected in (({"active": 0}, "LITEPAN_ACCOUNT_DISABLED"),
                                  ({"status": "token_expired"}, "LITEPAN_ACCOUNT_AUTH_INACTIVE"),
                                  ({"provider": "other"}, "LITEPAN_ACCOUNT_PROVIDER_UNSUPPORTED"),
                                  ({"access_token": ""}, "LITEPAN_ACCOUNT_ACCESS_TOKEN_MISSING")):
            with tempfile.TemporaryDirectory() as tmp:
                client, _ = self.account_client(tmp, **changes)
                transport = Mock(side_effect=AssertionError("network_must_not_run"))
                self.assertEqual(self.verify(client, transport)["reason"], expected)
                transport.assert_not_called()

    def test_oauth_rejection_and_secret_exceptions_are_safe(self):
        with tempfile.TemporaryDirectory() as tmp:
            client, _ = self.account_client(tmp)
            denied = Mock(return_value=(401, {"message": "fixture-access-token"}))
            result = self.verify(client, denied)
            self.assertEqual(result["reason"], "LITEPAN_ACCOUNT_OPEN_API_REJECTED")
            self.assertEqual(denied.call_count, 1)
            broken = Mock(side_effect=RuntimeError("fixture-access-token"))
            self.assertEqual(self.verify(client, broken)["reason"], "LITEPAN_ACCOUNT_BINDING_UNVERIFIED")


if __name__ == "__main__":
    unittest.main()
