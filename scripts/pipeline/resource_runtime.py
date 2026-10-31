"""Read-only planning and explicitly authorized quarter-resource preparation."""
from __future__ import annotations

import copy
import ctypes
import datetime as dt
import hashlib
import json
import os
from pathlib import Path
import re
import stat
import sys
import tempfile
import uuid
from typing import Any

from .config import ConfigSchema
from .resource_commands import _current_quarter, qualify_current_subscriptions
from .resource_engine import CONFIG_FIELDS, RESOURCE_KEYS, coordinate_resources
from .state import ProcessLock


def _darwin_xattr(name: str, *args: Any) -> int:
    # CPython exposes os.*xattr on Linux but not macOS. Native Darwin calls
    # preserve the same no-follow policy without an optional dependency.
    if sys.platform != "darwin":
        raise ValueError("extended_attributes_unavailable")
    function = getattr(ctypes.CDLL(None, use_errno=True), name)
    function.restype = ctypes.c_ssize_t if name != "setxattr" else ctypes.c_int
    result = function(*args)
    if result < 0:
        error = ctypes.get_errno()
        raise OSError(error, "extended_attribute_operation_failed")
    return result


def _xattrs(path: Path) -> dict[str, bytes]:
    if hasattr(os, "listxattr"):
        return {name: os.getxattr(path, name, follow_symlinks=False)
                for name in os.listxattr(path, follow_symlinks=False)}
    encoded = os.fsencode(path)
    size = _darwin_xattr("listxattr", encoded, None, ctypes.c_size_t(0), 1)
    names = ctypes.create_string_buffer(size)
    size = _darwin_xattr("listxattr", encoded, names, ctypes.c_size_t(size), 1)
    values = {}
    for name in names.raw[:size].split(b"\0"):
        if not name:
            continue
        length = _darwin_xattr("getxattr", encoded, name, None, ctypes.c_size_t(0), 0, 1)
        value = ctypes.create_string_buffer(length)
        length = _darwin_xattr("getxattr", encoded, name, value, ctypes.c_size_t(length), 0, 1)
        values[os.fsdecode(name)] = value.raw[:length]
    return values


def _set_xattr(path: Path, name: str, value: bytes) -> None:
    if hasattr(os, "setxattr"):
        os.setxattr(path, name, value, follow_symlinks=False)
    else:
        _darwin_xattr("setxattr", os.fsencode(path), os.fsencode(name), value,
                      ctypes.c_size_t(len(value)), 0, 1)


def _directory(path: Path) -> None:
    """Require existing directory components without following symlinks."""
    absolute = Path(os.path.abspath(path))
    for component in (absolute, *absolute.parents):
        info = component.lstat()
        if not stat.S_ISDIR(info.st_mode):
            raise ValueError("directory_unsafe")


def _sync_directory(path: Path) -> None:
    fd = os.open(path, os.O_RDONLY | os.O_DIRECTORY | os.O_NOFOLLOW)
    try:
        os.fsync(fd)
    finally:
        os.close(fd)


def _identity(info: os.stat_result) -> tuple[int, ...]:
    return (info.st_dev, info.st_ino, info.st_nlink, info.st_size,
            info.st_mtime_ns, info.st_ctime_ns, info.st_mode, info.st_uid, info.st_gid)


def _read_config(path: Path) -> tuple[bytes, os.stat_result]:
    _directory(path.parent)
    before = path.lstat()
    if not stat.S_ISREG(before.st_mode) or before.st_nlink != 1:
        raise ValueError("configuration_file_unsafe")
    fd = os.open(path, os.O_RDONLY | os.O_NOFOLLOW)
    with os.fdopen(fd, "rb") as handle:
        info = os.fstat(handle.fileno())
        if _identity(before) != _identity(info):
            raise ValueError("configuration_changed")
        content = handle.read()
        if _identity(info) != _identity(os.fstat(handle.fileno())):
            raise ValueError("configuration_changed")
    if _identity(info) != _identity(path.lstat()):
        raise ValueError("configuration_changed")
    return content, info


def _resource_plan_revision(original: bytes, quarter: str) -> str:
    """Bind a plan token to both exact config bytes and its frozen quarter."""

    quarter_bytes = quarter.encode("utf-8")
    payload = (
        b"anime-cloud-pipeline-resource-plan-v1\0"
        + len(quarter_bytes).to_bytes(4, "big")
        + quarter_bytes
        + len(original).to_bytes(8, "big")
        + original
    )
    return hashlib.sha256(payload).hexdigest()


def _patch(raw: dict[str, Any], values: dict[str, str]) -> dict[str, Any]:
    if set(values) != set(CONFIG_FIELDS) or any(type(value) is not str for value in values.values()):
        raise ValueError("configuration_patch_invalid")
    result = copy.deepcopy(raw)
    for field, value in values.items():
        section, name = field.split(".")
        if not isinstance(result.get(section), dict):
            raise ValueError("configuration_section_invalid")
        result[section][name] = value
    ConfigSchema.from_mapping(result, os.environ, sys.platform)
    return result


class _ConfigCommit:
    def __init__(self, path: str, raw: dict[str, Any], patch: dict[str, str]):
        self.path = Path(os.path.abspath(path))
        self.original, self.info = _read_config(self.path)
        if json.loads(self.original) != raw:
            raise ValueError("configuration_snapshot_changed")
        self.values = dict(patch)
        self.updated = _patch(raw, self.values)
        self.changes = [
            {"field": field, "before": raw[field.split(".")[0]].get(field.split(".")[1]),
             "after": value}
            for field, value in sorted(self.values.items())
            if raw[field.split(".")[0]].get(field.split(".")[1]) != value
        ]
        self.xattrs = _xattrs(self.path)
        self._check()

    def _check(self) -> None:
        content, info = _read_config(self.path)
        if content != self.original or _identity(info) != _identity(self.info):
            raise ValueError("configuration_changed")

    def _metadata(self, fd: int, path: Path) -> None:
        info = os.fstat(fd)
        if (info.st_uid, info.st_gid) != (self.info.st_uid, self.info.st_gid):
            os.fchown(fd, self.info.st_uid, self.info.st_gid)
        os.fchmod(fd, stat.S_IMODE(self.info.st_mode))
        for name, value in self.xattrs.items():
            _set_xattr(path, name, value)

    def __call__(self, values: dict[str, str]) -> None:
        if values != self.values:
            raise ValueError("configuration_patch_changed")
        self._check()
        if not self.changes:
            return

        backup_root = self.path.parent.parent / "backups"
        try:
            backup_root.mkdir(mode=0o700)
        except FileExistsError:
            pass
        _directory(backup_root)
        stamp = dt.datetime.now(dt.timezone.utc).strftime("%Y%m%dT%H%M%S%fZ")
        backup_dir = backup_root / f"quarter-resources-{stamp}-{uuid.uuid4().hex[:12]}"
        backup_dir.mkdir(mode=0o700)
        backup = backup_dir / self.path.name
        fd = os.open(backup, os.O_WRONLY | os.O_CREAT | os.O_EXCL | os.O_NOFOLLOW, 0o600)
        with os.fdopen(fd, "wb") as handle:
            handle.write(self.original)
            handle.flush()
            self._metadata(handle.fileno(), backup)
            os.fsync(handle.fileno())
        _sync_directory(backup_dir)
        _sync_directory(backup_root)
        _sync_directory(backup_root.parent)

        temporary: Path | None = None
        try:
            fd, name = tempfile.mkstemp(prefix=".quarter-config-", dir=self.path.parent)
            temporary = Path(name)
            with os.fdopen(fd, "wb") as handle:
                handle.write((json.dumps(self.updated, ensure_ascii=False, indent=2) + "\n").encode())
                handle.flush()
                self._metadata(handle.fileno(), temporary)
                os.fsync(handle.fileno())
            self._check()
            os.replace(temporary, self.path)
            temporary = None
            _sync_directory(self.path.parent)
        finally:
            if temporary is not None:
                temporary.unlink(missing_ok=True)


def _journal(path: Path, event: dict[str, str]) -> None:
    allowed = {
        "key": set(RESOURCE_KEYS) | {"pipeline"},
        "action": {"create", "complete"},
        "outcome": {"intent", "uncertain", "recovered", "verified", "prepared"},
    }
    if set(event) != set(allowed) or any(event[key] not in allowed[key] for key in allowed):
        raise ValueError("journal_event_invalid")
    _directory(path.parent)
    fd = os.open(path, os.O_WRONLY | os.O_APPEND | os.O_CREAT | os.O_NOFOLLOW | os.O_NONBLOCK, 0o600)
    with os.fdopen(fd, "ab") as handle:
        info = os.fstat(handle.fileno())
        if not stat.S_ISREG(info.st_mode) or info.st_nlink != 1:
            raise ValueError("journal_file_unsafe")
        os.fchmod(handle.fileno(), 0o600)
        handle.write((json.dumps(event, sort_keys=True) + "\n").encode())
        handle.flush()
        os.fsync(handle.fileno())


def run_resource_preparation(
    raw_config: dict[str, Any], config: Any, *, config_path: str, apply: bool,
) -> dict[str, Any]:
    """Qualify first, then plan or coordinate a bounded resource preparation."""
    counts: dict[str, Any] = {}
    layout: dict[str, Any] = {}
    changes: list[dict[str, Any]] = []
    revision: str | None = None

    def finish(report: dict[str, Any]) -> dict[str, Any]:
        return {**report, "quarter": layout.get("quarter", _current_quarter()),
                "targets": layout.get("targets", {}), "config_changes": changes,
                "config_revision": revision,
                "qualification_counts": counts}

    def blocked(reason: str, *, status="blocked") -> dict[str, Any]:
        return finish({"status": status, "reason": reason, "partial": False,
                       "counts": {"created": 0, "reused": 0},
                       "resources": {key: "unknown" for key in RESOURCE_KEYS}, "blockers": []})

    try:
        qualification = qualify_current_subscriptions(config)
        counts = qualification.get("counts", {})
        eligible = counts.get("eligible")
        if qualification.get("status") != "ok" or counts.get("unknown", 0) != 0:
            return blocked("qualification_blocked")
        if type(eligible) is not int or eligible < 0:
            return blocked("qualification_invalid")
        if eligible == 0:
            return blocked(None, status="noop")
    except Exception:
        return blocked("qualification_blocked")
    if type(apply) is not bool:
        return blocked("request_invalid")
    authorized = os.environ.get("OCTOP_PIPELINE_ALLOW_RESOURCES") == "1"
    if apply and not authorized:
        return blocked("authorization_required")
    approved_revision = os.environ.get("OCTOP_PIPELINE_RESOURCE_REVISION", "")
    if apply and not re.fullmatch(r"[0-9a-f]{64}", approved_revision):
        return blocked("resource_plan_stale")

    try:
        from .quarter_layout import build_layout
        layout = build_layout(raw_config, config)
        if layout["quarter"] != _current_quarter():
            return blocked("quarter_invalid")
        commit = _ConfigCommit(config_path, raw_config, layout["patch"])
        changes = commit.changes
        revision = _resource_plan_revision(commit.original, layout["quarter"])
    except Exception:
        return blocked("configuration_snapshot_invalid")
    if apply and approved_revision != revision:
        return blocked("resource_plan_stale")

    lock = None
    try:
        if apply:
            state = Path(os.path.abspath(config.paths.state_directory))
            _directory(state)
            lock = ProcessLock(str(state / "run.lock"))
            if not lock.acquire():
                lock = None
                return blocked("resource_lock_held")
            commit._check()
            # Subscription state can change while waiting for the shared
            # runner lock. Re-read it before constructing any resource port.
            qualification = qualify_current_subscriptions(config)
            counts = qualification.get("counts", {})
            eligible = counts.get("eligible")
            if qualification.get("status") != "ok" or counts.get("unknown", 0) != 0:
                return blocked("qualification_blocked")
            if type(eligible) is not int or eligible < 0:
                return blocked("qualification_invalid")
            if layout["quarter"] != _current_quarter():
                return blocked("quarter_invalid")
            if eligible == 0:
                return blocked(None, status="noop")
        from .quarter_ports import QuarterResourcePorts
        ports = QuarterResourcePorts(config, layout)
        report = coordinate_resources(
            layout=layout, qualification=qualification, ports=ports,
            apply=apply, authorized=authorized,
            journal=(lambda event: _journal(state / "resource-preparation.jsonl", event)) if apply else None,
            commit_config=commit if apply else None,
        )
        return finish(report)
    except Exception:
        return blocked("resource_runtime_failed")
    finally:
        if lock is not None:
            lock.release()


__all__ = ["run_resource_preparation"]
