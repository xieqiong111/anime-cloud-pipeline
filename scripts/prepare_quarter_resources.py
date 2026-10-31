#!/usr/bin/env python3
"""Print a safe, deterministic quarter resource plan from a config JSON file."""

from __future__ import annotations

import argparse
from datetime import date, datetime
import json
import posixpath
import re
import sys
from typing import Any
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError


PATH_FIELDS = (
    "paths.cloud_source_root",
    "runtime.litepan_task_source_root",
    "runtime.litepan_task_output_root",
    "runtime.emby_library_root",
)
NAME_FIELDS = (
    "runtime.litepan_task_name",
    "runtime.litepan_event_name",
    "runtime.emby_library_name",
)
CONFIG_CHANGES = (
    PATH_FIELDS[0],
    NAME_FIELDS[0],
    PATH_FIELDS[1],
    PATH_FIELDS[2],
    NAME_FIELDS[1],
    NAME_FIELDS[2],
    PATH_FIELDS[3],
)

CONSTRAINTS = [
    "Plan only: this helper does not create directories, LitePan tasks, webhook rules, or Emby libraries.",
    "The owning agent must use this plan only during the explicitly authorized writable flow; preflight and dry-run remain read-only.",
    "Keep Emby scanning, metadata fetch, realtime monitoring, and initial refresh disabled while creating the library.",
    "Remote NAS paths are derived from config and are not checked with local filesystem existence.",
    "The helper does not dump the config or credential fields; output is limited to derived paths, candidate names, and field references.",
]
NOTES = [
    "Candidate names and paths are deterministic for the selected quarter.",
    "This plan is not evidence that any remote resource exists or is absent; read back inventory before applying changes.",
    "Existing matching resources may be reused after the owning agent verifies their exact mapping.",
    "Emby no-action flags express required behavior and are not an API request body.",
]


class PlanError(ValueError):
    """A validation failure whose message contains only a fixed field name."""


def _field(mapping: dict[str, Any], key: str, field_name: str) -> Any:
    if key not in mapping:
        raise PlanError(f"missing or invalid field: {field_name}")
    return mapping[key]


def _valid_absolute_posix_path(value: Any) -> bool:
    if (
        not isinstance(value, str)
        or not value.startswith("/")
        or "\\" in value
        or "//" in value
        or any(character in value for character in "?:#")
    ):
        return False
    if any(ord(character) < 32 or ord(character) == 127 for character in value):
        return False
    if value == "/":
        return True
    segments = value[1:].split("/")
    return all(segment and segment not in (".", "..") for segment in segments)


def _valid_safe_name(value: Any) -> bool:
    if not isinstance(value, str) or not value or value != value.strip():
        return False
    if not re.fullmatch(r"[\w .-]{1,128}", value, flags=re.UNICODE):
        return False
    if not any(character.isalnum() for character in value):
        return False
    # Host-like values can be URLs even when they omit a scheme.
    if re.fullmatch(r"[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)+", value):
        return False
    return True


def _validate_path(value: Any, field_name: str) -> str:
    if not _valid_absolute_posix_path(value):
        raise PlanError(f"missing or invalid field: {field_name}")
    return value


def _validate_name(value: Any, field_name: str) -> str:
    if not _valid_safe_name(value):
        raise PlanError(f"missing or invalid field: {field_name}")
    return value


def _validate_safe_token(value: Any, field_name: str) -> None:
    if not isinstance(value, str) or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise PlanError(f"missing or invalid field: {field_name}")


def _get_before(mapping: dict[str, Any], field_name: str, is_path: bool) -> Any:
    parent_name, key = field_name.split(".", 1)
    parent = mapping[parent_name]
    if key not in parent:
        return None
    value = parent[key]
    if is_path:
        return _validate_path(value, field_name)
    return _validate_name(value, field_name)


def _overlaps(left: str, right: str) -> bool:
    left_norm = posixpath.normpath(left)
    right_norm = posixpath.normpath(right)
    return (
        left_norm == right_norm
        or left_norm == "/"
        or right_norm == "/"
        or left_norm.startswith(right_norm.rstrip("/") + "/")
        or right_norm.startswith(left_norm.rstrip("/") + "/")
    )


def _check_separation(
    raw_root: str,
    published_root: str,
    emby_root: str,
    source_roots: list[tuple[str, str]],
    state_directory: str | None,
) -> None:
    if _overlaps(raw_root, published_root):
        raise PlanError("overlapping fields: paths.litepan_raw_output_root and paths.litepan_output_root")
    if _overlaps(raw_root, emby_root):
        raise PlanError("overlapping fields: raw output and runtime.emby_library_root")
    for field_name, source_root in source_roots:
        if _overlaps(source_root, raw_root) or _overlaps(source_root, published_root):
            raise PlanError(f"overlapping field: {field_name}")
    if state_directory is not None:
        if _overlaps(state_directory, raw_root) or _overlaps(state_directory, published_root):
            raise PlanError("overlapping field: paths.state_directory")


def _quarter_for(selected_date: date) -> tuple[str, str]:
    start_month = ((selected_date.month - 1) // 3) * 3 + 1
    key = f"{selected_date.year:04d}-{start_month:02d}"
    return key, f"{key}月新番"


def build_plan(config: Any, selected_date: date) -> dict[str, Any]:
    if not isinstance(config, dict):
        raise PlanError("missing or invalid field: config root")
    paths = config.get("paths")
    runtime = config.get("runtime")
    if not isinstance(paths, dict):
        raise PlanError("missing or invalid field: paths")
    if not isinstance(runtime, dict):
        raise PlanError("missing or invalid field: runtime")

    cloud_library_root = _validate_path(
        _field(paths, "cloud_library_root", "paths.cloud_library_root"),
        "paths.cloud_library_root",
    )
    litepan_raw_base = _validate_path(
        _field(paths, "litepan_raw_output_root", "paths.litepan_raw_output_root"),
        "paths.litepan_raw_output_root",
    )
    litepan_published_base = _validate_path(
        _field(paths, "litepan_output_root", "paths.litepan_output_root"),
        "paths.litepan_output_root",
    )
    emby_mount_root = _validate_path(
        _field(paths, "emby_mount_root", "paths.emby_mount_root"),
        "paths.emby_mount_root",
    )
    _validate_safe_token(
        _field(runtime, "litepan_event_source", "runtime.litepan_event_source"),
        "runtime.litepan_event_source",
    )
    if _overlaps(litepan_raw_base, litepan_published_base):
        raise PlanError("overlapping fields: paths.litepan_raw_output_root and paths.litepan_output_root")

    quarter_key, quarter_label = _quarter_for(selected_date)
    cloud_quarter_root = posixpath.join(cloud_library_root, quarter_label)
    cloud_source_root = posixpath.join(cloud_quarter_root, "未整理")
    litepan_raw_root = posixpath.join(litepan_raw_base, quarter_label)
    litepan_published_root = posixpath.join(litepan_published_base, quarter_label)
    emby_root = posixpath.join(emby_mount_root, quarter_label)
    task_name = f"追番V5-{quarter_key}"
    event_name = f"anime_v5_{quarter_key.replace('-', '_')}_ready"

    source_roots = [
        ("paths.cloud_source_root", cloud_source_root),
        ("runtime.litepan_task_source_root", cloud_quarter_root),
    ]
    for field_name in (PATH_FIELDS[0], PATH_FIELDS[1]):
        value = _get_before(config, field_name, True)
        if value is not None:
            source_roots.append((field_name, value))

    state_directory = None
    if "state_directory" in paths:
        state_directory = _validate_path(paths["state_directory"], "paths.state_directory")

    _check_separation(
        litepan_raw_root,
        litepan_published_root,
        emby_root,
        source_roots,
        state_directory,
    )

    after_values = {
        "paths.cloud_source_root": cloud_source_root,
        "runtime.litepan_task_name": task_name,
        "runtime.litepan_task_source_root": cloud_quarter_root,
        "runtime.litepan_task_output_root": litepan_raw_root,
        "runtime.litepan_event_name": event_name,
        "runtime.emby_library_name": quarter_label,
        "runtime.emby_library_root": emby_root,
    }
    changes = []
    for field_name in CONFIG_CHANGES:
        before = _get_before(config, field_name, field_name in PATH_FIELDS)
        after = after_values[field_name]
        if before == after:
            continue
        changes.append(
            {
                "field": field_name,
                "after": after,
            }
        )

    return {
        "quarter": {"key": quarter_key, "label": quarter_label},
        "resources": {
            "directories": {
                "cloud_quarter_root": cloud_quarter_root,
                "cloud_source_root": cloud_source_root,
                "litepan_raw_root": litepan_raw_root,
                "litepan_published_root": litepan_published_root,
                "emby_root": emby_root,
            },
            "litepan_task": {
                "name": task_name,
                "source_root": cloud_quarter_root,
                "output_root": litepan_raw_root,
                "recursive": True,
            },
            "webhook": {
                "event_name": event_name,
                "source_config_field": "runtime.litepan_event_source",
            },
            "emby_library": {
                "name": quarter_label,
                "root": emby_root,
                "no_scan": True,
                "no_metadata_fetch": True,
                "no_realtime_monitor": True,
                "no_initial_refresh": True,
            },
        },
        "config_changes": changes,
        "status": "plan_only",
        "constraints": CONSTRAINTS,
        "notes": NOTES,
    }


def _selected_date(raw_date: str | None) -> date:
    if raw_date is not None:
        if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", raw_date):
            raise PlanError("invalid --date: expected YYYY-MM-DD")
        try:
            return date.fromisoformat(raw_date)
        except ValueError as exc:
            raise PlanError("invalid --date: expected YYYY-MM-DD") from exc
    try:
        return datetime.now(ZoneInfo("Asia/Shanghai")).date()
    except ZoneInfoNotFoundError as exc:
        raise PlanError("timezone unavailable: Asia/Shanghai") from exc


class SafeArgumentParser(argparse.ArgumentParser):
    def error(self, message: str) -> None:
        raise PlanError("invalid command line arguments")


def main(argv: list[str] | None = None) -> int:
    parser = SafeArgumentParser(description=__doc__)
    parser.add_argument("--config", required=True, help="path to the current deployment config JSON")
    parser.add_argument("--date", help="planning date in YYYY-MM-DD format (defaults to Asia/Shanghai today)")

    try:
        args = parser.parse_args(argv)
        selected_date = _selected_date(args.date)
        with open(args.config, "r", encoding="utf-8") as config_file:
            config = json.load(config_file)
        plan = build_plan(config, selected_date)
    except PlanError as exc:
        print(f"ERROR: {exc}", file=sys.stderr)
        return 2
    except (OSError, UnicodeError, json.JSONDecodeError):
        print("ERROR: unable to read a valid config JSON", file=sys.stderr)
        return 2

    json.dump(plan, sys.stdout, ensure_ascii=False, indent=2)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
