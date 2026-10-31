#!/usr/bin/env python3
"""Credential-safe MoviePilot plugin configuration CLI.

The plugin endpoint has full-replacement semantics.  This wrapper therefore
fetches the complete configuration, changes only explicitly permitted fields,
verifies the result, and rolls back in memory on a failed assertion.  Secret
replacement is intentionally outside ordinary ``--set``; use the MoviePilot
secret-management workflow instead.
"""
from __future__ import annotations

import argparse
import json
import os
from pathlib import Path
import re
import stat
import sys
from typing import Any, Mapping

HERE = os.path.dirname(os.path.abspath(__file__))
if HERE not in sys.path:
    sys.path.insert(0, HERE)
_HOST_OS_NAME = os.name

from pipeline.real_adapters import MoviePilotClient, dpapi_protect  # noqa: E402
from pipeline.security import sanitize_text  # noqa: E402


SENSITIVE_RE = re.compile(
    r"(?:cookie|cookies|token|password|passwd|api[_-]?key|secret|credential|authorization)",
    re.I,
)
PLAINTEXT_SUFFIX = ".SECRET-PLAINTEXT.json"


class PluginCliError(ValueError):
    """A safe, deterministic plugin CLI input error."""


class _SafeArgumentParser(argparse.ArgumentParser):
    """Argument parser whose diagnostics never include raw argv values."""

    def error(self, _message: str) -> None:
        raise PluginCliError("invalid command-line arguments")


def _is_sensitive_key(key: str) -> bool:
    normalized = str(key or "").strip().casefold()
    if not normalized:
        return False
    parts = [part for part in re.split(r"[._/:-]+", normalized) if part]
    return any(
        part in {
            "cookie", "cookies", "token", "tokens", "password", "passwd",
            "apikey", "api", "key", "secret", "secrets", "credential",
            "credentials", "authorization", "bearer",
        }
        for part in parts
    ) or bool(SENSITIVE_RE.search(normalized))


def build_client(args) -> MoviePilotClient:
    if args.mp_base and args.token_file:
        return MoviePilotClient(
            args.mp_base,
            args.token_file,
            auth_mode=getattr(args, "auth_mode", "x-api-key"),
        )
    if args.config:
        try:
            with open(args.config, encoding="utf-8") as handle:
                cfg = json.load(handle)
            mp = cfg["moviepilot"]
            return MoviePilotClient(
                mp["base_url"],
                mp.get("token_file"),
                token=os.environ.get(mp.get("token_env", ""), ""),
                auth_mode=mp.get("auth_mode", "x-api-key"),
            )
        except (OSError, KeyError, TypeError, json.JSONDecodeError) as exc:
            raise PluginCliError(f"plugin config could not be read ({type(exc).__name__})") from None
    raise PluginCliError("need --mp-base + --token-file or --config")


def _masked_value(value: Any, key: str = "") -> Any:
    if _is_sensitive_key(key):
        if value in (None, ""):
            return value
        return f"<redacted len={len(str(value))}>"
    if isinstance(value, Mapping):
        return {str(k): _masked_value(item, str(k)) for k, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_masked_value(item, key) for item in value]
    if isinstance(value, str):
        safe = sanitize_text(value)
        if re.search(r"https?://", safe, flags=re.I):
            return "<redacted url>"
        if len(safe) > 256:
            return f"<redacted text len={len(safe)}>"
        return safe
    return value


def mask_summary(conf: Mapping[str, Any]) -> dict[str, Any]:
    """Return a recursively masked, JSON-compatible configuration summary."""

    return {str(key): _masked_value(value, str(key)) for key, value in conf.items()}


def parse_value(raw: str) -> Any:
    """Parse booleans, integers, JSON values, and finally plain strings."""

    low = raw.strip().lower()
    if low == "true":
        return True
    if low == "false":
        return False
    try:
        return int(raw)
    except ValueError:
        pass
    try:
        return json.loads(raw)
    except (json.JSONDecodeError, ValueError):
        return raw


def _parse_set_values(values: list[str] | None) -> dict[str, Any]:
    parsed: dict[str, Any] = {}
    for item in values or []:
        if "=" not in item:
            raise PluginCliError("--set requires key=value")
        key, _, raw = item.partition("=")
        key = key.strip()
        if not key:
            raise PluginCliError("--set requires a non-empty key")
        if _is_sensitive_key(key):
            # Never include ``item`` or ``raw`` in this error: argv may contain
            # a credential and error text is often captured by schedulers.
            raise PluginCliError("sensitive --set key rejected")
        parsed[key] = parse_value(raw)
    return parsed


def _default_snapshot_path(allow_plaintext: bool) -> str:
    if os.name == "nt":
        return os.path.join(HERE, "plugin-config-snapshot.dpapi")
    if allow_plaintext:
        return os.path.join(HERE, "plugin-config-snapshot" + PLAINTEXT_SUFFIX)
    return os.path.join(HERE, "plugin-config-snapshot" + PLAINTEXT_SUFFIX)


def _validate_snapshot_policy(path: str, *, allow_plaintext: bool) -> bool:
    """Validate snapshot policy and return whether the snapshot is plaintext."""

    if os.name == "nt":
        return False
    if not sys.platform.startswith("linux"):
        raise PluginCliError("plaintext snapshots are supported only on Linux")
    if not allow_plaintext:
        raise PluginCliError(
            "Linux plaintext snapshot is disabled; pass --allow-plaintext-snapshot for explicit opt-in"
        )
    if not str(path).endswith(PLAINTEXT_SUFFIX):
        raise PluginCliError(f"Linux plaintext snapshot path must end with {PLAINTEXT_SUFFIX}")
    return True


class _SnapshotLease:
    """An open snapshot inode retained until the update finally completes."""

    __slots__ = ("path", "fd", "identity", "length")

    def __init__(self, path: str, fd: int, identity: tuple[int, int] | None, length: int):
        self.path = path
        self.fd = fd
        self.identity = identity
        self.length = length


class _SnapshotPath(str):
    """String-compatible path carrying the internal open-inode lease."""

    def __new__(cls, path: str, lease: _SnapshotLease):
        value = str.__new__(cls, path)
        value.lease = lease
        return value


def _snapshot_identity(path: str) -> tuple[int, int, int]:
    """Return a regular, single-link snapshot identity without following links."""

    info = os.lstat(path)
    if (
        stat.S_ISLNK(info.st_mode)
        or not stat.S_ISREG(info.st_mode)
        or int(getattr(info, "st_nlink", 1)) != 1
    ):
        raise PluginCliError("snapshot path is not a safe regular file")
    return (int(info.st_dev), int(info.st_ino), int(info.st_nlink))


def _lease_matches_fd(lease: _SnapshotLease) -> bool:
    info = os.fstat(lease.fd)
    current = (int(info.st_dev), int(info.st_ino))
    if lease.identity is None:
        lease.identity = current
        return True
    return current == lease.identity


def _path_matches_lease(lease: _SnapshotLease) -> bool:
    if lease.identity is None:
        return False
    try:
        info = os.lstat(lease.path)
    except OSError:
        return False
    return (int(info.st_dev), int(info.st_ino)) == lease.identity


def _scrub_lease(lease: _SnapshotLease) -> None:
    """Attempt every fd cleanup step, even when metadata is unavailable."""

    failed = False
    length = lease.length
    try:
        info = os.fstat(lease.fd)
        length = max(int(info.st_size), length)
        current = (int(info.st_dev), int(info.st_ino))
        if lease.identity is None:
            lease.identity = current
        elif current != lease.identity:
            failed = True
    except BaseException:
        failed = True
    # We own this descriptor continuously from exclusive creation until close.
    # Metadata failure must not revoke the ability to erase that inode.
    try:
        os.lseek(lease.fd, 0, os.SEEK_SET)
        remaining = length
        block = b"\x00" * 4096
        while remaining:
            count = os.write(lease.fd, block[: min(len(block), remaining)])
            if count <= 0:
                raise OSError("snapshot scrub write failed")
            remaining -= count
    except BaseException:
        failed = True
    try:
        os.fsync(lease.fd)
    except BaseException:
        failed = True
    try:
        os.ftruncate(lease.fd, 0)
    except BaseException:
        failed = True
    try:
        os.fsync(lease.fd)
    except BaseException:
        failed = True
    if failed:
        raise RuntimeError("snapshot scrub failed")


def _force_empty_lease(lease: _SnapshotLease) -> None:
    """Fallback that removes bytes even when a scrub pass reports an error."""

    try:
        os.ftruncate(lease.fd, 0)
        os.fsync(lease.fd)
    except BaseException:
        pass


def _close_lease(lease: _SnapshotLease) -> None:
    if lease.fd < 0:
        return
    fd = lease.fd
    lease.fd = -1
    os.close(fd)


def _cleanup_plaintext_snapshot(lease: _SnapshotLease) -> None:
    """Scrub through the held fd, unlink only the known path, and verify zero links."""

    failed = False
    fd_closed_for_windows = False
    nlink_before_close: int | None = None
    try:
        # Scrubbing never depends on successful path or fd identity queries.
        try:
            _scrub_lease(lease)
        except BaseException:
            failed = True
            _force_empty_lease(lease)

        # An unknown identity never establishes ownership of a pathname.
        # Metadata recovery during scrubbing may establish the held identity;
        # otherwise leave the scrubbed inode's unverifiable path untouched.
        # Retry a failed check so transient lstat failures cannot skip unlink.
        path_identity_ok = _path_matches_lease(lease)
        if not path_identity_ok:
            failed = True
            path_identity_ok = _path_matches_lease(lease)
        if path_identity_ok:
            try:
                os.remove(lease.path)
            except BaseException as exc:
                share_violation = (
                    _HOST_OS_NAME == "nt" and int(getattr(exc, "winerror", 0) or 0) == 32
                )
                try:
                    nlink_before_close = int(os.fstat(lease.fd).st_nlink)
                except BaseException:
                    failed = True
                # Windows does not permit unlinking an open file. Plaintext
                # mode is Linux-only in production; this branch keeps the
                # platform-simulated tests and DPAPI host behavior compatible.
                if _HOST_OS_NAME == "nt" and lease.fd >= 0:
                    try:
                        _close_lease(lease)
                        fd_closed_for_windows = True
                    except BaseException:
                        failed = True
                if not share_violation:
                    failed = True
                # This fallback still targets only the known path. Unknown
                # hardlink paths are never enumerated or deleted.
                if _path_matches_lease(lease):
                    try:
                        os.unlink(lease.path)
                    except BaseException:
                        failed = True
                else:
                    failed = True
                if nlink_before_close is not None and nlink_before_close != 1:
                    failed = True
        else:
            failed = True

        if not fd_closed_for_windows:
            try:
                if os.fstat(lease.fd).st_nlink != 0:
                    failed = True
            except BaseException:
                failed = True
        elif os.path.lexists(lease.path):
            failed = True
    finally:
        try:
            _close_lease(lease)
        except BaseException:
            failed = True
    if failed:
        raise RuntimeError("plaintext snapshot cleanup failed")


def snapshot(conf: Mapping[str, Any], path: str, *, allow_plaintext: bool = False) -> str:
    """Write a local snapshot under the platform-specific safety policy."""

    payload = json.dumps(conf, ensure_ascii=False, sort_keys=True).encode("utf-8")
    plaintext = _validate_snapshot_policy(path, allow_plaintext=allow_plaintext)
    target = os.path.abspath(path)
    parent = os.path.dirname(target) or "."
    os.makedirs(parent, exist_ok=True)
    if os.path.islink(parent) or not os.path.isdir(parent):
        raise PluginCliError("snapshot directory is not safe")
    if plaintext:
        # O_EXCL prevents overwriting an existing symlink or hardlink.  Linux
        # also gets O_NOFOLLOW; Windows test shims may not expose that flag.
        flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
        try:
            fd = os.open(target, flags, 0o600)
        except FileExistsError:
            raise PluginCliError("snapshot path must not already exist") from None
        lease = _SnapshotLease(target, fd, None, len(payload))
        try:
            try:
                info = os.fstat(fd)
            except OSError:
                # Retry once while the exclusively created descriptor is held.
                # A transient metadata fault need not abort a safe update.
                info = os.fstat(fd)
            lease.identity = (int(info.st_dev), int(info.st_ino))
            if not stat.S_ISREG(info.st_mode) or int(getattr(info, "st_nlink", 1)) != 1:
                raise PluginCliError("snapshot path is not a safe regular file")
            fchmod = getattr(os, "fchmod", None)
            if fchmod is not None:
                fchmod(fd, 0o600)
            if not _lease_matches_fd(lease) or not _path_matches_lease(lease):
                raise RuntimeError("snapshot path changed during validation")
            if _snapshot_identity(target)[2] != 1:
                raise RuntimeError("snapshot path is not a safe regular file")
            with os.fdopen(fd, "r+b", closefd=False) as handle:
                handle.write(payload)
                handle.flush()
            os.fsync(fd)
            if not _lease_matches_fd(lease) or not _path_matches_lease(lease):
                raise RuntimeError("snapshot path changed during write")
            return _SnapshotPath(target, lease)
        except BaseException:
            if lease is not None:
                try:
                    _cleanup_plaintext_snapshot(lease)
                except BaseException:
                    _force_empty_lease(lease)
                    try:
                        _close_lease(lease)
                    except BaseException:
                        pass
            else:
                try:
                    os.close(fd)
                except OSError:
                    pass
            raise
    else:
        blob = dpapi_protect(payload, "plugin-config-snapshot")
        with open(target, "w", encoding="ascii", newline="\n") as handle:
            handle.write(blob.hex())
    return target


def load_snapshot(path: str) -> dict[str, Any]:
    """Load a snapshot for explicit recovery code without printing values."""

    try:
        _snapshot_identity(path)
    except (FileNotFoundError, OSError, PluginCliError):
        raise PluginCliError("snapshot path is not a safe regular file") from None
    with open(path, encoding="utf-8") as handle:
        text = handle.read().strip()
    if str(path).endswith(".dpapi") or (re.fullmatch(r"[0-9a-fA-F\s]+", text) and len(text) > 80):
        from pipeline.real_adapters import _dpapi_unprotect  # noqa: PLC0415

        plain = _dpapi_unprotect(bytes.fromhex(re.sub(r"\s", "", text)))
        if b"\x00" in plain:
            plain = plain.decode("utf-16").encode("utf-8")
        return json.loads(plain.decode("utf-8"))
    if not str(path).endswith(PLAINTEXT_SUFFIX):
        raise PluginCliError("plaintext snapshot filename is not approved")
    return json.loads(text)


def _safe_plugin_state(state: Mapping[str, Any] | None) -> dict[str, Any]:
    version = str(state.get("plugin_version") or "?") if state else "?"
    if version != "?" and not re.fullmatch(r"[A-Za-z0-9._+:-]{1,64}", version):
        version = "<present>"
    return {
        "installed": bool(state and state.get("installed")),
        "state": bool(state and state.get("state")),
        "version": version,
    }


_SHOW_BOOL_FIELDS = frozenset({"enabled", "disabled", "debug", "use_proxy"})
_SHOW_NUMBER_FIELDS = frozenset({"timeout", "interval", "page_size", "max_pages", "retry_count"})
_SHOW_ENUM_FIELDS = frozenset({
    "mode", "auth_mode", "media_source", "source_type", "task_name", "library_name",
    "version", "plugin_version",
})
_SHOW_HASH_RE = re.compile(r"(?:hash|digest|checksum|etag|revision)", re.I)


def _show_config_summary(conf: Mapping[str, Any] | None) -> dict[str, Any]:
    """Expose only bounded operational fields and structural counts."""

    if not isinstance(conf, Mapping):
        return {"field_count": 0, "safe_fields": {}, "redacted_field_count": 0}
    safe_fields: dict[str, Any] = {}
    type_counts: dict[str, int] = {}
    redacted = 0
    for raw_key, value in conf.items():
        key = str(raw_key)
        normalized = key.casefold()
        type_name = type(value).__name__
        type_counts[type_name] = type_counts.get(type_name, 0) + 1
        if _is_sensitive_key(key):
            redacted += 1
            continue
        if normalized in _SHOW_BOOL_FIELDS and isinstance(value, bool):
            safe_fields[key] = value
            continue
        if normalized in _SHOW_NUMBER_FIELDS and isinstance(value, (int, float)) and not isinstance(value, bool):
            safe_fields[key] = value
            continue
        if _SHOW_HASH_RE.search(normalized) and isinstance(value, (str, bytes)):
            text = value.hex() if isinstance(value, bytes) else value
            safe_fields[key] = {"prefix": text[:8], "length": len(text)}
            continue
        if normalized in _SHOW_ENUM_FIELDS and isinstance(value, str):
            if re.fullmatch(r"[A-Za-z0-9._+:-]{1,64}", value):
                safe_fields[key] = value
            else:
                redacted += 1
            continue
        redacted += 1
    return {
        "field_count": len(conf),
        "safe_fields": safe_fields,
        "value_types": type_counts,
        "redacted_field_count": redacted,
    }


def cmd_show(args) -> int:
    client = build_client(args)
    conf = client.plugin_conf(args.plugin)
    state = client.plugin_state(args.plugin)
    print(json.dumps({
        "plugin": str(args.plugin),
        "state": _safe_plugin_state(state),
        "config": _show_config_summary(conf),
    }, ensure_ascii=False, sort_keys=True))
    return 0


def cmd_set(args) -> int:
    # This parse and policy check must happen before build_client or any GET.
    values = _parse_set_values(args.set)
    allow_plaintext = bool(getattr(args, "allow_plaintext_snapshot", False))
    snapshot_path = args.snapshot or _default_snapshot_path(allow_plaintext)
    plaintext = _validate_snapshot_policy(snapshot_path, allow_plaintext=allow_plaintext)
    snapshot_target = os.path.abspath(snapshot_path)
    if plaintext and os.path.lexists(snapshot_target):
        raise PluginCliError("snapshot path must not already exist")

    client = build_client(args)
    old = client.plugin_conf(args.plugin)
    created_snapshot: _SnapshotLease | None = None
    try:
        written_snapshot = snapshot(old, snapshot_path, allow_plaintext=allow_plaintext)
        if plaintext:
            created_snapshot = getattr(written_snapshot, "lease", None)
            if created_snapshot is None:
                raise RuntimeError("plaintext snapshot lease unavailable")
        new = dict(old)
        new.update(values)
        changed = {key: new[key] for key in new if old.get(key) != new[key]}
        if not changed:
            print(json.dumps({"status": "unchanged", "changed": {}}, ensure_ascii=False))
            return 0

        client.update_plugin_conf(args.plugin, new)
        after = client.plugin_conf(args.plugin)
        failed = [key for key in changed if after.get(key) != changed[key]]
        leaked = [
            key for key in set(old) | set(after)
            if _is_sensitive_key(key) and old.get(key) != after.get(key)
        ]
        if failed or leaked:
            try:
                client.update_plugin_conf(args.plugin, old)
            except Exception:
                # The rollback attempt is deliberately reported without its
                # remote exception text or any configuration value.
                print("plugin verification failed; rollback attempt failed", file=sys.stderr)
                return 1
            print("plugin verification failed; rolled back", file=sys.stderr)
            return 1

        state = client.plugin_state(args.plugin)
        print(json.dumps({
            "status": "updated",
            "changed": {key: "[CHANGED]" for key in changed},
            "plugin_state": _safe_plugin_state(state),
        }, ensure_ascii=False, sort_keys=True))
        return 0
    finally:
        # Plaintext snapshots exist only for the bounded update/rollback
        # operation.  Cleanup is unconditional, including client failures.
        if created_snapshot:
            _cleanup_plaintext_snapshot(created_snapshot)


def _parser() -> argparse.ArgumentParser:
    parser = _SafeArgumentParser(description="Safe MoviePilot plugin configuration")
    sub = parser.add_subparsers(
        dest="cmd", required=True, parser_class=_SafeArgumentParser
    )

    def add_conn(command: argparse.ArgumentParser) -> None:
        command.add_argument("--plugin", required=True, help="plugin id")
        command.add_argument("--mp-base", help="MoviePilot base URL")
        command.add_argument("--token-file", help="API token file")
        command.add_argument("--config", help="pipeline config JSON")
        command.add_argument(
            "--auth-mode", choices=("x-api-key", "query"), default="x-api-key"
        )

    show = sub.add_parser("show", help="show a masked plugin summary")
    add_conn(show)
    show.set_defaults(handler=cmd_show)

    set_command = sub.add_parser("set", help="safe full-configuration update")
    add_conn(set_command)
    set_command.add_argument("--set", action="append", help="permitted key=value field")
    set_command.add_argument("--snapshot", help="local snapshot path")
    set_command.add_argument(
        "--allow-plaintext-snapshot", "--allow-plaintext", action="store_true",
        dest="allow_plaintext_snapshot",
        help="explicitly allow a Linux .SECRET-PLAINTEXT.json snapshot",
    )
    set_command.set_defaults(handler=cmd_set)
    return parser


def main(argv: list[str] | None = None) -> int:
    try:
        args = _parser().parse_args(argv)
        return int(args.handler(args))
    except PluginCliError as exc:
        print(f"error: {sanitize_text(str(exc))[:600]}", file=sys.stderr)
        return 2
    except (OSError, ValueError, RuntimeError) as exc:
        print(f"error: operation failed ({type(exc).__name__})", file=sys.stderr)
        return 1
    except Exception as exc:
        print(f"error: operation failed ({type(exc).__name__})", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
