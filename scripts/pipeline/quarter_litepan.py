"""Bounded quarterly resource management for LitePan v0.5.5-beta.

No login, credential synthesis, scan, toggle, or event endpoint is exposed.
The caller owns authorization and the shared pipeline ProcessLock.
"""
from __future__ import annotations

import hashlib
from contextlib import closing
import json
import sqlite3
from pathlib import Path
from urllib.parse import quote, urlencode, urlsplit


class LitePanResourceError(RuntimeError):
    """Only a fixed, credential-free reason code may leave this adapter."""


class QuarterLitePanClient:
    # Installed assets byte-matched to official v0.5.5-beta. Check the entry
    # document as well, because upgraded services may retain obsolete assets.
    STATIC_HASHES = {
        "/": "3d07aaa70470af61274ff049cca46f266226b46015c5350559db9ac3289e1823",
        "/assets/index-Dvw1dm6S.js": "011d8a54a591899e1c41454e3514e50bb701d1f1faf952d0a84c56820946a6c7",
        "/assets/TaskManagement-CeMxLR8R.js": "65f12d17bceaa148610b5264b916c7040427ef9a265eb1892c0ed9669bbc380f",
        "/assets/AutomationPanel-C8R9b-gy.js": "0cfb3d6ebb1b57167ee38419ee59d204e55f683bb7b6993e9b649b0e637115f9",
        "/assets/strm-C6fQtvDT.js": "70a1de811bf7bf58754c8e25d67aef3f0f5081b1e2f0cb825809b4fa5891100f",
    }
    TASK_PATH = "/api/admin/strm/tasks"
    RULE_PATH = "/api/admin/automation/rules"
    TASK_FIELDS = ("name", "account_id", "parent_id", "path", "recursive",
                   "output_folder", "group_dir", "schedule_mode", "status")
    TASK_PAYLOAD = TASK_FIELDS + ("scan_interval", "scan_mode", "extensions",
                    "api_interval", "exclude_dir_keywords", "exclude_file_keywords",
                    "sync_metadata", "branch_check_enabled", "time_window_enabled",
                    "time_start", "time_end")

    def __init__(self, base_url, admin_cookie=None, transport=None, *,
                 task_db_path=None, strm_host_root=None):
        parsed = urlsplit(base_url)
        if parsed.scheme not in {"http", "https"} or not parsed.netloc or parsed.username or parsed.password or parsed.query or parsed.fragment:
            raise ValueError("LITEPAN_BASE_URL_INVALID")
        self.base = base_url.rstrip("/")
        self.origin = f"{parsed.scheme}://{parsed.netloc}"
        self._cookie = admin_cookie or ""
        # Require the actual browser Cookie header, never treat a task key as one.
        if self._cookie and (not self._cookie.startswith("admin_session=") or any(c in self._cookie for c in "\r\n")):
            raise ValueError("LITEPAN_ADMIN_COOKIE_INVALID")
        self._transport = transport
        self.task_db_path = task_db_path
        self.strm_host_root = strm_host_root

    def _request(self, method, path, body=None):
        headers = {"Cookie": self._cookie}
        if method != "GET":
            headers["Origin"] = self.origin
        try:
            if self._transport is not None:
                status, payload = self._transport(method, self.base + path,
                                                  headers=headers, body=body, timeout=30)
            else:
                from .real_adapters import _http_json
                status, payload = _http_json(method, self.base + path, headers=headers,
                                             body=body, timeout=30)
            if status not in {200, 201} or not isinstance(payload, dict) or payload.get("success") is not True or "data" not in payload:
                raise LitePanResourceError("LITEPAN_API_REJECTED")
            return payload["data"]
        except LitePanResourceError:
            raise
        except Exception:
            raise LitePanResourceError("LITEPAN_API_UNAVAILABLE") from None

    def creation_blockers(self):
        if not self._cookie:
            return ["LITEPAN_ADMIN_SESSION_REQUIRED"]
        try:
            auth = self._request("GET", "/api/auth/status")
            if not isinstance(auth, dict) or auth.get("is_admin") is not True or auth.get("must_change_password") is True:
                return ["LITEPAN_ADMIN_SESSION_INVALID"]
        except LitePanResourceError:
            return ["LITEPAN_ADMIN_SESSION_UNVERIFIED"]
        # Public static requests carry no administrator cookie. Never cache a
        # prior successful version check across separate creation attempts.
        try:
            for path, expected in self.STATIC_HASHES.items():
                raw = self._static_bytes(path)
                if hashlib.sha256(raw).hexdigest() != expected:
                    return ["LITEPAN_VERSION_UNSUPPORTED"]
        except LitePanResourceError:
            return ["LITEPAN_VERSION_UNVERIFIED"]
        return []

    def _static_bytes(self, path):
        limit = 2 * 1024 * 1024
        try:
            if self._transport is not None:
                status, raw = self._transport("GET", self.base + path,
                                               headers={}, body=None, timeout=30)
            else:
                import urllib.request
                request = urllib.request.Request(self.base + path,
                                                  headers={"Accept-Encoding": "identity"})
                with urllib.request.urlopen(request, timeout=30) as response:
                    status, raw = response.status, response.read(limit + 1)
            if status != 200 or not isinstance(raw, bytes) or len(raw) > limit:
                raise LitePanResourceError("LITEPAN_VERSION_UNVERIFIED")
            return raw
        except LitePanResourceError:
            raise
        except Exception:
            raise LitePanResourceError("LITEPAN_VERSION_UNVERIFIED") from None

    def _rows(self, kind):
        if self.task_db_path:
            path = Path(self.task_db_path)
            if not path.is_absolute() or not path.is_file():
                raise LitePanResourceError("LITEPAN_INVENTORY_UNAVAILABLE")
            try:
                db = sqlite3.connect(f"file:{quote(str(path), safe='/')}?mode=ro", uri=True, timeout=5)
                try:
                    db.execute("PRAGMA query_only=ON")
                    db.row_factory = sqlite3.Row
                    # Both installed endpoints return complete arrays, no paging.
                    if kind == "task":
                        fields = ",".join(("id",) + self.TASK_PAYLOAD)
                        rows = [dict(r) for r in db.execute("SELECT " + fields + " FROM strm_tasks ORDER BY id")]
                        for r in rows:
                            for k in ("recursive", "sync_metadata", "branch_check_enabled", "time_window_enabled"):
                                if r[k] not in (0, 1):
                                    raise ValueError()
                                r[k] = bool(r[k])
                    else:
                        rows = [dict(r) for r in db.execute("SELECT id,name,trigger_type,trigger_config,actions_json,status FROM automation_rules ORDER BY id")]
                        for r in rows:
                            r["trigger_config"] = json.loads(r["trigger_config"])
                            r["actions"] = json.loads(r.pop("actions_json"))
                finally:
                    db.close()
            except Exception:
                raise LitePanResourceError("LITEPAN_INVENTORY_UNAVAILABLE") from None
        else:
            if not self._cookie:
                raise LitePanResourceError("LITEPAN_INVENTORY_UNAVAILABLE")
            rows = self._request("GET", self.TASK_PATH if kind == "task" else self.RULE_PATH)
        if not isinstance(rows, list):
            # Do not unwrap guessed pagination envelopes or accept partial pages.
            raise LitePanResourceError("LITEPAN_INVENTORY_INCOMPLETE")
        ids = set()
        required = ("id",) + self.TASK_FIELDS if kind == "task" else ("id", "name", "trigger_type", "trigger_config", "actions", "status")
        for row in rows:
            if not isinstance(row, dict) or any(k not in row for k in required):
                raise LitePanResourceError("LITEPAN_INVENTORY_INCOMPLETE")
            ident = row["id"]
            if type(ident) is not int or ident <= 0 or ident in ids or not isinstance(row["name"], str) or not row["name"]:
                raise LitePanResourceError("LITEPAN_INVENTORY_INCOMPLETE")
            ids.add(ident)
            if kind == "task" and (type(row["account_id"]) is not int or row["account_id"] <= 0 or
                    type(row["recursive"]) is not bool or
                    any(not isinstance(row[k], str) for k in ("parent_id", "path", "output_folder", "group_dir", "schedule_mode", "status"))):
                raise LitePanResourceError("LITEPAN_INVENTORY_INCOMPLETE")
            if kind == "rule" and (not isinstance(row["trigger_config"], dict) or not isinstance(row["actions"], list)):
                raise LitePanResourceError("LITEPAN_INVENTORY_INCOMPLETE")
        return rows

    def read_tasks(self):
        return self._rows("task")

    def read_rules(self):
        return self._rows("rule")

    def read_account_identity(self, account_id):
        """Read safe account metadata, never promote task IDs to identity proof.

        v0.5.5-beta's sole 115 provider is OAuth ``115_open``. Its driver does
        not consume the cookie field, so passing that field to a Cookie P115
        adapter would verify a different credential. Metadata alone is insufficient; the separate GET-only
        verify_account_binding method must establish live OAuth identity.
        This method reads neither config nor cookie/access/refresh tokens.
        """
        result = {"state": "unknown", "reason": "LITEPAN_ACCOUNT_IDENTITY_UNVERIFIED"}
        if type(account_id) is not int or account_id <= 0:
            return dict(result, reason="LITEPAN_ACCOUNT_ID_INVALID")
        if not self.task_db_path:
            return dict(result, reason="LITEPAN_ACCOUNT_DATABASE_REQUIRED")
        path = Path(self.task_db_path)
        if not path.is_absolute() or not path.is_file():
            return dict(result, reason="LITEPAN_ACCOUNT_DATABASE_UNAVAILABLE")
        try:
            db = sqlite3.connect(f"file:{quote(str(path), safe='/')}?mode=ro", uri=True, timeout=5)
            try:
                db.execute("PRAGMA query_only=ON")
                rows = db.execute(
                    "SELECT a.id,a.driver_type,a.is_active,s.status "
                    "FROM cloud_accounts AS a LEFT JOIN account_auth_states AS s "
                    "ON s.account_id=a.id WHERE a.id=?", (account_id,)
                ).fetchall()
            finally:
                db.close()
        except Exception:
            return dict(result, reason="LITEPAN_ACCOUNT_DATABASE_UNAVAILABLE")
        if not rows:
            return dict(result, reason="LITEPAN_ACCOUNT_MISSING")
        if len(rows) != 1:
            return dict(result, reason="LITEPAN_ACCOUNT_AMBIGUOUS")
        ident, provider, active, auth_status = rows[0]
        if type(ident) is not int or ident != account_id or not isinstance(provider, str):
            return dict(result, reason="LITEPAN_ACCOUNT_METADATA_INVALID")
        # Do not echo arbitrary database strings into diagnostics. The actual
        # provider/auth fields are exposed only when they match known enums.
        if provider != "115_open":
            return dict(result, reason="LITEPAN_ACCOUNT_PROVIDER_UNSUPPORTED")
        result.update(account_id=ident, driver_type=provider)
        if type(active) is not int or active not in (0, 1):
            return dict(result, reason="LITEPAN_ACCOUNT_METADATA_INVALID")
        result["is_active"] = bool(active)
        if not active:
            return dict(result, reason="LITEPAN_ACCOUNT_DISABLED")
        if auth_status not in {"active", "cooldown", "token_expired", "failed"}:
            return dict(result, reason="LITEPAN_ACCOUNT_AUTH_UNVERIFIED")
        result["auth_status"] = auth_status
        if auth_status != "active":
            return dict(result, reason="LITEPAN_ACCOUNT_AUTH_INACTIVE")
        return dict(result, reason="LITEPAN_ACCOUNT_OAUTH_REQUIRES_VERIFICATION")

    def verify_account_binding(self, account_id, *, expected_user_id,
                               root_directory_id, root_directory_path,
                               target_path=None, expected_target_id=None,
                               transport=None, max_pages=20, max_entries=10000):
        """Verify the exact stored OAuth credential using GET-only 115 APIs.

        No refresh, login, LitePan account-list GET, or persistent auth update.
        Return only fixed codes and verified non-secret identities.
        """
        unknown = {"state": "unknown", "reason": "LITEPAN_ACCOUNT_BINDING_UNVERIFIED"}
        def ident(value):
            if isinstance(value, bool) or not isinstance(value, (str, int)):
                raise LitePanResourceError("LITEPAN_ACCOUNT_IDENTITY_INVALID")
            token = str(value)
            if not token.isascii() or not token.isdecimal():
                raise LitePanResourceError("LITEPAN_ACCOUNT_IDENTITY_INVALID")
            return str(int(token))
        def components(path):
            if not isinstance(path, str) or not path.startswith("/") or "\\" in path or any(ord(c) < 32 for c in path):
                raise LitePanResourceError("LITEPAN_ACCOUNT_PATH_INVALID")
            if path == "/":
                return []
            parts = path[1:].split("/")
            if any(p in {"", ".", ".."} for p in parts):
                raise LitePanResourceError("LITEPAN_ACCOUNT_PATH_INVALID")
            return parts
        try:
            user_id, root_id = ident(expected_user_id), ident(root_directory_id)
            if user_id == "0" or type(max_pages) is not int or max_pages <= 0 or type(max_entries) is not int or max_entries <= 0:
                raise LitePanResourceError("LITEPAN_ACCOUNT_BINDING_INPUT_INVALID")
            root_parts = components(root_directory_path)
            target_parts = components(target_path) if target_path is not None else root_parts
            if target_parts[:len(root_parts)] != root_parts:
                raise LitePanResourceError("LITEPAN_ACCOUNT_PATH_INVALID")
            target_id = ident(expected_target_id) if expected_target_id is not None else None
            evidence = self.read_account_identity(account_id)
            if evidence["reason"] != "LITEPAN_ACCOUNT_OAUTH_REQUIRES_VERIFICATION":
                return evidence
            # Recheck provider/active/auth alongside the token in one read
            # transaction; never read config, cookie or refresh_token.
            path = Path(self.task_db_path)
            with closing(sqlite3.connect(f"file:{quote(str(path), safe='/')}?mode=ro", uri=True, timeout=5)) as db:
                db.execute("PRAGMA query_only=ON")
                rows = db.execute(
                    "SELECT a.driver_type,a.is_active,s.status,s.access_token "
                    "FROM cloud_accounts AS a JOIN account_auth_states AS s "
                    "ON s.account_id=a.id WHERE a.id=?", (account_id,)
                ).fetchall()
            if len(rows) != 1 or rows[0][:3] != ("115_open", 1, "active"):
                raise LitePanResourceError("LITEPAN_ACCOUNT_AUTH_UNVERIFIED")
            access_token = rows[0][3]
            if not isinstance(access_token, str) or not access_token or any(c in access_token for c in "\r\n"):
                raise LitePanResourceError("LITEPAN_ACCOUNT_ACCESS_TOKEN_MISSING")
            def get(endpoint, params=None):
                url = "https://proapi.115.com" + endpoint
                headers = {"Authorization": "Bearer " + access_token}
                if transport is not None:
                    if params:
                        url += "?" + urlencode(params)
                    status, data = transport("GET", url, headers=headers, body=None, timeout=30)
                else:
                    from .real_adapters import _http_json
                    status, data = _http_json("GET", url, headers=headers, params=params,
                                             timeout=30, secrets=(access_token,))
                if status != 200 or not isinstance(data, dict) or type(data.get("state")) not in (bool, int) or data["state"] not in (True, 1):
                    raise LitePanResourceError("LITEPAN_ACCOUNT_OPEN_API_REJECTED")
                return data
            profile = get("/open/user/info").get("data")
            if not isinstance(profile, dict) or ident(profile.get("user_id")) != user_id:
                raise LitePanResourceError("LITEPAN_ACCOUNT_USER_MISMATCH")
            pages_left, entries_left = max_pages, max_entries
            def listing(parent):
                nonlocal pages_left, entries_left
                entries, seen, total = [], set(), None
                while True:
                    if pages_left <= 0:
                        raise LitePanResourceError("LITEPAN_ACCOUNT_INVENTORY_LIMIT")
                    pages_left -= 1
                    page = get("/open/ufile/files", {"cid": parent, "limit": 300,
                               "offset": len(entries), "show_dir": 1})
                    count = page.get("count")
                    if type(count) is not int or count < 0 or (total is not None and count != total):
                        raise LitePanResourceError("LITEPAN_ACCOUNT_INVENTORY_INCOMPLETE")
                    total = count
                    batch = page.get("data")
                    if not isinstance(batch, list) or len(entries) + len(batch) > total or len(batch) > 300 or total > entries_left:
                        raise LitePanResourceError("LITEPAN_ACCOUNT_INVENTORY_INCOMPLETE")
                    for item in batch:
                        if not isinstance(item, dict):
                            raise LitePanResourceError("LITEPAN_ACCOUNT_INVENTORY_INCOMPLETE")
                        item_id = ident(item.get("fid") or item.get("file_id"))
                        item_parent = ident(item.get("pid") or item.get("parent_id") or item.get("cid"))
                        category = item.get("file_category", item.get("fc"))
                        name = item.get("fn") or item.get("file_name")
                        if item_id == "0" or item_id in seen or item_parent != parent or isinstance(category, bool) or not str(category).isascii() or not str(category).isdecimal() or not isinstance(name, str) or not name:
                            raise LitePanResourceError("LITEPAN_ACCOUNT_INVENTORY_INCOMPLETE")
                        seen.add(item_id)
                        entries.append((item_id, name, str(category) == "0"))
                    if len(entries) == total:
                        entries_left -= len(entries)
                        return entries
                    if not batch:
                        raise LitePanResourceError("LITEPAN_ACCOUNT_INVENTORY_INCOMPLETE")
            current = "0"
            for index in range(len(target_parts) + 1):
                # Read every traversed directory completely, including the
                # final target. A bare matching ID is not sufficient evidence.
                entries = listing(current)
                if index == len(root_parts) and current != root_id:
                    raise LitePanResourceError("LITEPAN_ACCOUNT_ROOT_MISMATCH")
                if index == len(target_parts):
                    break
                matches = [row for row in entries if row[1] == target_parts[index]]
                if len(matches) != 1 or not matches[0][2]:
                    raise LitePanResourceError("LITEPAN_ACCOUNT_PATH_UNVERIFIED")
                current = matches[0][0]
            if target_id is not None and current != target_id:
                raise LitePanResourceError("LITEPAN_ACCOUNT_TARGET_MISMATCH")
            return {"state": "verified", "reason": "LITEPAN_ACCOUNT_BINDING_VERIFIED",
                    "account_id": account_id, "user_id": user_id,
                    "root_directory_id": root_id, "target_directory_id": current}
        except LitePanResourceError as exc:
            return dict(unknown, reason=str(exc))
        except Exception:
            return unknown

    @staticmethod
    def _result(state, reason, row=None):
        result = {"state": state, "reason": reason}
        if row is not None:
            result["id"] = row["id"]
        return result

    def _task_result(self, rows, spec):
        related = [r for r in rows if r["name"] == spec.get("name") or r["path"] == spec.get("path") or
                   (r["output_folder"] == spec.get("output_folder") and r["group_dir"] == spec.get("group_dir"))]
        if not related:
            return self._result("create", "LITEPAN_TASK_ABSENT")
        expected_fields = set(self.TASK_FIELDS) | (set(spec) & set(self.TASK_PAYLOAD))
        if len(related) != 1 or any(k not in spec or k not in related[0] or related[0][k] != spec[k] for k in expected_fields):
            return self._result("conflict", "LITEPAN_TASK_CONFLICT")
        return self._result("reuse", "LITEPAN_TASK_EXACT", related[0])

    def _rule_result(self, rows, spec):
        trigger = spec.get("trigger_config", {})
        related = [r for r in rows if r["name"] == spec.get("name") or
                   (r["trigger_type"] == "webhook" and r["trigger_config"].get("event") == trigger.get("event") and
                    r["trigger_config"].get("source") == trigger.get("source"))]
        if not related:
            return self._result("create", "LITEPAN_RULE_ABSENT")
        if len(related) != 1:
            return self._result("conflict", "LITEPAN_RULE_CONFLICT")
        row = related[0]
        def actions(value):
            if not isinstance(value, list):
                return None
            return [{k: a.get(k) for k in ("type", "condition", "params")} for a in value if isinstance(a, dict)] if all(isinstance(a, dict) for a in value) else None
        if (any(row[k] != spec.get(k) for k in ("name", "trigger_type", "status")) or
                any(row["trigger_config"].get(k) != trigger.get(k) for k in ("event", "source")) or
                any(row["trigger_config"].get(k) not in (None, "", False, 0) for k in ("path_prefix", "require_path", "account_id", "parent_id", "path")) or
                actions(row["actions"]) != actions(spec.get("actions"))):
            return self._result("conflict", "LITEPAN_RULE_CONFLICT")
        return self._result("reuse", "LITEPAN_RULE_EXACT", row)

    def inventory(self, task_spec, rule_spec):
        result = {}
        for kind, spec in (("task", task_spec), ("rule", rule_spec)):
            try:
                result["litepan_" + kind] = getattr(self, "_" + kind + "_result")(self._rows(kind), spec)
            except LitePanResourceError as exc:
                result["litepan_" + kind] = self._result("unknown", str(exc))
        return result

    def _require_admin(self):
        blockers = self.creation_blockers()
        if blockers:
            raise LitePanResourceError(blockers[0])

    def create_task(self, spec):
        """Only the authorized runtime may call; POST is never retried here."""
        self._require_admin()
        if (any(k not in spec for k in self.TASK_FIELDS) or spec.get("schedule_mode") != "manual" or
                spec.get("status") != "active" or spec.get("recursive") is not True or
                spec.get("sync_metadata") is not False or
                ("scan_mode" in spec and spec["scan_mode"] not in {"incremental_missing", "incremental_update", "full_sync"}) or
                type(spec.get("account_id")) is not int or spec["account_id"] <= 0 or
                not isinstance(spec.get("parent_id"), str) or not spec["parent_id"] or
                not isinstance(spec.get("path"), str) or not spec["path"].startswith("/") or
                not isinstance(spec.get("name"), str) or not spec["name"]):
            raise LitePanResourceError("LITEPAN_TASK_SPEC_UNSAFE")
        for key in ("group_dir", "output_folder"):
            part = spec[key]
            if not isinstance(part, str) or not part or any(s in {"", ".", ".."} for s in part.split("/")) or "\\" in part:
                raise LitePanResourceError("LITEPAN_TASK_SPEC_UNSAFE")
        before = self._task_result(self.read_tasks(), spec)
        if before["state"] == "reuse":
            return before
        if before["state"] != "create":
            raise LitePanResourceError(before["reason"])
        payload = {k: spec[k] for k in self.TASK_PAYLOAD if k in spec}
        self._request("POST", self.TASK_PATH, payload)
        after = self._task_result(self.read_tasks(), spec)
        if after["state"] != "reuse":
            raise LitePanResourceError("LITEPAN_TASK_READBACK_FAILED")
        return after

    def create_rule(self, spec):
        self._require_admin()
        trigger = spec.get("trigger_config", {})
        actions = spec.get("actions", [])
        if (spec.get("trigger_type") != "webhook" or spec.get("status") != "running" or
                not spec.get("name") or not isinstance(trigger, dict) or set(trigger) != {"event", "source"} or
                not all(isinstance(trigger[k], str) and trigger[k] for k in trigger) or
                not isinstance(actions, list) or len(actions) != 1 or not isinstance(actions[0], dict)):
            raise LitePanResourceError("LITEPAN_RULE_SPEC_UNSAFE")
        action = actions[0]
        params = action.get("params", {})
        if (action.get("type") != "strm" or action.get("condition") != "always" or
                not isinstance(params, dict) or set(params) != {"task_id", "run_mode"} or
                type(params["task_id"]) is not int or params["task_id"] <= 0 or params["run_mode"] != "full"):
            raise LitePanResourceError("LITEPAN_RULE_SPEC_UNSAFE")
        targets = [r for r in self.read_tasks() if r["id"] == params["task_id"]]
        if len(targets) != 1 or targets[0]["schedule_mode"] != "manual" or targets[0]["status"] != "active":
            raise LitePanResourceError("LITEPAN_RULE_TARGET_UNSAFE")
        before = self._rule_result(self.read_rules(), spec)
        if before["state"] == "reuse":
            return before
        if before["state"] != "create":
            raise LitePanResourceError(before["reason"])
        self._request("POST", self.RULE_PATH, {k: spec[k] for k in ("name", "trigger_type", "trigger_config", "status", "actions")})
        after = self._rule_result(self.read_rules(), spec)
        if after["state"] != "reuse":
            raise LitePanResourceError("LITEPAN_RULE_READBACK_FAILED")
        return after
