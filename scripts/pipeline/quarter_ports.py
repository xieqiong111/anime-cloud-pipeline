"""Actual quarter resource ports. Never download, scan, scrape or edit databases."""
from __future__ import annotations

import copy
import os
from pathlib import Path
import posixpath
import json
import socket
import stat
import subprocess
from urllib.parse import urlsplit

from .quarter_emby import QuarterEmbyClient
from .quarter_litepan import QuarterLitePanClient
from .real_adapters import (
    P115CloudAdapter, _directory_match, _item_name, _item_is_directory,
)


def _row(state, reason):
    return {"state": state, "reason": reason.lower()}


def _secret(service):
    return os.environ.get(service.secret_reference or "", "")


def _local_state(base, target):
    try:
        if (posixpath.dirname(target) != base or os.path.realpath(base) != base
                or not Path(base).is_dir()):
            return _row("unknown", "local_parent_unverified")
        info = os.lstat(target)
        if not stat.S_ISDIR(info.st_mode):
            return _row("conflict", "local_target_not_directory")
        return _row("reuse", "local_directory_exact")
    except FileNotFoundError:
        return _row("create", "local_directory_missing")
    except OSError:
        return _row("unknown", "local_directory_unreadable")


def _admin_cookie(config):
    value = os.environ.get("LITEPAN_ADMIN_COOKIE", "")
    if value:
        return value
    # Optional operator-supplied existing session; no login or credential synthesis.
    path = Path(config.paths.state_directory).parent / "config/litepan-admin-session"
    try:
        fd = os.open(path, os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0))
    except FileNotFoundError:
        return None
    try:
        info = os.fstat(fd)
        if (not stat.S_ISREG(info.st_mode) or info.st_nlink != 1
                or stat.S_IMODE(info.st_mode) & 0o077 or info.st_size > 8192):
            raise ValueError("litepan_admin_session_file_unsafe")
        with os.fdopen(fd, "r", encoding="utf-8") as handle:
            fd = None
            return handle.read().strip() or None
    finally:
        if fd is not None:
            os.close(fd)


def _emby_mount_verified(config, layout):
    """Prove this local service maps the published host tree at the expected path."""
    try:
        endpoint = urlsplit(config.services.emby.base_url)
        addresses = {r[4][0] for r in socket.getaddrinfo(endpoint.hostname, None)}
        network = subprocess.run(['ip', '-j', 'address', 'show'], check=True,
                                 capture_output=True, text=True, timeout=5)
        local = {'127.0.0.1', '::1'} | {r['local'] for i in json.loads(network.stdout)
                                       for r in i.get('addr_info', []) if 'local' in r}
        if not addresses or not addresses.issubset(local):
            return False
        # Read only the established local Emby service's routes and mounts,
        # never Docker Config/Env (which can contain credentials).
        inspected = subprocess.run([
            'docker', 'inspect', 'Emby', '--format',
            '{"mounts":{{json .Mounts}},"ports":{{json .NetworkSettings.Ports}},"running":{{json .State.Running}}}',
        ], check=True, capture_output=True, text=True, timeout=5)
        data = json.loads(inspected.stdout)
        port = endpoint.port or (443 if endpoint.scheme == 'https' else 80)
        if not data.get('running') or not any(
                r.get('HostPort') == str(port)
                for r in (data.get('ports', {}).get('8096/tcp') or [])):
            return False
        target = layout['emby_library']['path']
        matches = []
        for mount in data['mounts']:
            destination = mount['Destination'].rstrip('/') or '/'
            if target == destination or target.startswith(destination.rstrip('/') + '/'):
                relative = posixpath.relpath(target, destination)
                host = posixpath.normpath(posixpath.join(mount['Source'], relative))
                matches.append((len(destination), host))
        return bool(matches) and max(matches)[1] == layout['targets']['published_dir']
    except Exception:
        return False


class QuarterResourcePorts:
    def __init__(self, config, layout, *, cloud=None, litepan=None, emby=None):
        self.config = config
        self.layout = layout
        db_path = config.runtime.litepan_task_db_path
        if not db_path or not Path(db_path).is_absolute() or not Path(db_path).is_file():
            raise ValueError("litepan_readonly_database_required")
        root_id = os.environ.get("P115_ROOT_DIRECTORY_ID") or os.environ.get("P115_ROOT_ID")
        root_path = (config.services.cloud.root_directory_path
                     or os.environ.get("P115_ROOT_DIRECTORY_PATH")
                     or os.environ.get("P115_ROOT_PATH"))
        self.cloud = cloud or P115CloudAdapter(
            None, credential=_secret(config.services.cloud), root_directory_id=root_id,
            root_directory_path=root_path, runtime_config=config)
        self.litepan = litepan or QuarterLitePanClient(
            config.services.litepan.base_url, admin_cookie=_admin_cookie(config),
            task_db_path=config.runtime.litepan_task_db_path,
            strm_host_root=config.runtime.litepan_strm_host_root)
        self.emby = emby or QuarterEmbyClient(config.services.emby.base_url, _secret(config.services.emby))
        self._quarter_id = None
        self._task_spec = self._rule_spec = None
        self._account_verified = False
        self._snapshot = {}

    def _cloud_inventory(self, layout):
        targets = layout["targets"]
        unknown = {key: _row("unknown", "cloud_inventory_unverified") for key in (
            "cloud_quarter", "cloud_source", "cloud_organized")}
        self._quarter_id = None
        try:
            parent = self.cloud.resolve_directory_path(layout["cloud_library_root"])
            entries = self.cloud.list_directory_entries(parent)
            name = posixpath.basename(targets["cloud_quarter"])
            if any(_item_name(r) == name and not _item_is_directory(r) for r in entries):
                return {k: _row("conflict", "cloud_name_is_file") for k in unknown}
            quarter = _directory_match(entries, name, targets["cloud_quarter"].strip("/").split("/"))
            if quarter is None:
                return {k: _row("create", "cloud_parent_inventory_proves_missing") for k in unknown}
            self._quarter_id = str(quarter[0])
            children = self.cloud.list_directory_entries(self._quarter_id)
            result = {"cloud_quarter": _row("reuse", "cloud_quarter_exact")}
            for key in ("cloud_source", "cloud_organized"):
                child_name = posixpath.basename(targets[key])
                if any(_item_name(r) == child_name and not _item_is_directory(r) for r in children):
                    result[key] = _row("conflict", "cloud_name_is_file")
                    continue
                match = _directory_match(children, child_name, targets[key].strip("/").split("/"))
                if match is not None:
                    self.cloud.list_directory_entries(match[0])
                result[key] = _row("reuse" if match else "create", "cloud_directory_exact" if match else "cloud_directory_missing")
            return result
        except Exception:
            return unknown

    def _litepan_inventory(self, layout):
        self._account_verified = False
        self._task_spec = self._rule_spec = None
        try:
            rows = self.litepan.read_tasks()
            # Reuse only the existing configured task's verified cross-service binding.
            current_path = self.config.runtime.litepan_task_source_root
            current_output = self.config.runtime.litepan_task_output_root
            anchors = [r for r in rows if r["name"] == self.config.runtime.litepan_task_name
                       and r["path"] == current_path
                       and str(Path(self.config.runtime.litepan_strm_host_root) / r["group_dir"] / r["output_folder"]) == current_output]
            if len(anchors) != 1:
                raise ValueError("litepan_account_binding_ambiguous")
            anchor = anchors[0]
            current_id = self.cloud.resolve_directory_path(current_path)
            if str(anchor["parent_id"]) != str(current_id):
                raise ValueError("litepan_account_binding_mismatch")
            binding = self.litepan.verify_account_binding(
                anchor["account_id"],
                expected_user_id=getattr(self.cloud._client, 'user_id', None),
                root_directory_id=self.cloud.root_directory_id,
                root_directory_path=self.cloud.root_directory_path,
                target_path=current_path, expected_target_id=current_id,
                max_pages=self.config.limits.max_task_pages,
                max_entries=self.config.limits.max_recursive_files,
            )
            if binding.get('state') != 'verified':
                reason = str(binding.get('reason', 'litepan_account_binding_unverified')).lower()
                return {key: _row('unknown', reason) for key in ('litepan_task', 'litepan_rule')}
            self._account_verified = True
            task = dict(layout["litepan_task"], account_id=anchor["account_id"],
                        parent_id=self._quarter_id or "pending-quarter-id")
            for key in ("scan_mode", "extensions", "exclude_dir_keywords", "exclude_file_keywords"):
                task[key] = anchor.get(key, "")
            task_result = self.litepan._task_result(rows, task)
            if not self._quarter_id and task_result['state'] == 'reuse':
                task_result = _row('conflict', 'litepan_task_parent_unverified')
            selected = next((r for r in rows if r.get("id") == task_result.get("id")), None)
            rule = copy.deepcopy(layout["litepan_rule"])
            rule["actions"] = [{"type": "strm", "condition": "always",
                                "params": {"task_id": selected["id"] if selected else 0, "run_mode": "full"}}]
            rule_result = self.litepan._rule_result(self.litepan.read_rules(), rule)
            if selected is None and rule_result['state'] == 'reuse':
                rule_result = _row('conflict', 'litepan_rule_target_unverified')
            self._task_spec, self._rule_spec = task, rule
            return {"litepan_task": _row(task_result["state"], task_result["reason"]),
                    "litepan_rule": _row(rule_result["state"], rule_result["reason"])}
        except Exception:
            return {key: _row("unknown", "litepan_binding_or_inventory_unverified")
                    for key in ("litepan_task", "litepan_rule")}

    def inventory(self, layout):
        result = self._cloud_inventory(layout)
        result.update(_locals(layout))
        result.update(self._litepan_inventory(layout))
        result["emby_library"] = self.emby.inventory(layout["emby_library"])
        self._snapshot = result
        return result

    def creation_capabilities(self, layout):
        blockers = []
        if not self._account_verified:
            blockers.append("litepan_account_binding_unverified")
        needs = {key for key, row in self._snapshot.items() if row['state'] == 'create'}
        if needs.intersection({'cloud_quarter', 'cloud_source', 'cloud_organized'}) and not callable(getattr(getattr(self.cloud, "_client", None), "fs_mkdir", None)):
            blockers.append("cloud_directory_create_unavailable")
        if needs.intersection({'litepan_task', 'litepan_rule'}):
            blockers.extend(str(x).lower() for x in self.litepan.creation_blockers())
        if 'emby_library' in needs:
            blockers.extend(self.emby.creation_blockers())
        if not _emby_mount_verified(self.config, layout):
            blockers.append('emby_path_mapping_unverified')
        for key, base in (("raw_dir", layout["raw_base"]), ("published_dir", layout["published_base"])):
            if key in needs and not os.access(base, os.W_OK | os.X_OK):
                blockers.append("local_parent_not_writable")
        return sorted(set(blockers))

    def create(self, key, layout):
        targets = layout["targets"]
        if key in ("cloud_quarter", "cloud_source", "cloud_organized"):
            path = targets[key]
            parent = self.cloud.resolve_directory_path(posixpath.dirname(path))
            self.cloud.fs_mkdir({"cname": posixpath.basename(path)}, parent)
        elif key in ("raw_dir", "published_dir"):
            base = layout["raw_base"] if key == "raw_dir" else layout["published_base"]
            if _local_state(base, targets[key])["state"] != "create":
                raise ValueError("local_create_scope_changed")
            fd = os.open(base, os.O_RDONLY | os.O_DIRECTORY | getattr(os, "O_NOFOLLOW", 0))
            try:
                os.mkdir(posixpath.basename(targets[key]), dir_fd=fd)
            finally:
                os.close(fd)
        elif key == "litepan_task":
            if not self._quarter_id or not self._account_verified:
                raise ValueError("litepan_binding_unverified")
            self.litepan.create_task(self._task_spec)
        elif key == "litepan_rule":
            self.litepan.create_rule(self._rule_spec)
        elif key == "emby_library":
            # An initial empty library has no existing media to inspect even
            # if a concurrent server-owned scheduled refresh occurs later.
            published = targets["published_dir"]
            if (not _emby_mount_verified(self.config, layout)
                    or _local_state(layout["published_base"], published)["state"] != "reuse"
                    or any(Path(published).iterdir())):
                raise ValueError("emby_initial_directory_not_empty")
            self.emby.create_library(layout["emby_library"])
        else:
            raise ValueError("resource_key_invalid")

    def config_patch(self, layout):
        return dict(layout["patch"])


def _locals(layout):
    return {key: _local_state(layout[base], layout["targets"][key])
            for key, base in (("raw_dir", "raw_base"), ("published_dir", "published_base"))}


__all__ = ["QuarterResourcePorts"]
