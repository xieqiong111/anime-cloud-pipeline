"""Derive one quarter below the existing, validated V5 roots."""
from __future__ import annotations

from datetime import datetime
import posixpath
from zoneinfo import ZoneInfo

from .paths import PathMapper, season_label_from_air_date

_EMBY_QUARTER_DISPLAY_NAMES = {
    "01": "一月新番",
    "04": "四月新番",
    "07": "七月新番",
    "10": "十月新番",
}


def build_layout(raw_config, config):
    now = datetime.now(ZoneInfo("Asia/Shanghai"))
    label = season_label_from_air_date(now.date().isoformat())
    quarter = label[:7]
    # Library display names use the Chinese quarter month ("十月新番") while
    # every directory keeps the year-qualified label ("2026-10月新番").
    month_code = quarter.split("-", 1)[-1]
    emby_label = _EMBY_QUARTER_DISPLAY_NAMES.get(
        month_code, label.split("-", 1)[-1] if "-" in label else label
    )
    paths = config.paths
    raw_base = paths.litepan_raw_output_root
    if not raw_base or not paths.cloud_library_subdir:
        raise ValueError("resource_layout_incomplete")
    if paths.litepan_publish_subdir:
        raise ValueError("legacy_publish_layout_requires_review")
    host_root = config.runtime.litepan_strm_host_root
    if not host_root:
        raise ValueError("litepan_host_root_missing")
    mapper = PathMapper(paths)
    group = mapper.relative_to(raw_base, host_root, field="litepan_raw_output_root")
    # LitePan's output_folder/group_dir are single directory components.
    if not group or "/" in group:
        raise ValueError("litepan_group_mapping_unverified")
    cloud_quarter = posixpath.join(paths.cloud_library_root, label)
    raw_dir = posixpath.join(raw_base, label)
    published_dir = posixpath.join(paths.litepan_output_root, label)
    emby_dir = posixpath.join(paths.emby_mount_root, label)
    task_name = "追番V5-" + quarter
    event_name = "anime_v5_" + quarter.replace("-", "_") + "_ready"
    patch = {
        "paths.cloud_source_root": posixpath.join(cloud_quarter, "未整理"),
        "runtime.litepan_task_name": task_name,
        "runtime.litepan_task_source_root": cloud_quarter,
        "runtime.litepan_task_output_root": raw_dir,
        "runtime.litepan_event_name": event_name,
        "runtime.emby_library_name": emby_label,
        "runtime.emby_library_root": emby_dir,
    }
    return {
        "quarter": quarter,
        "targets": {
            "cloud_quarter": cloud_quarter,
            "cloud_source": patch["paths.cloud_source_root"],
            "cloud_organized": posixpath.join(cloud_quarter, paths.cloud_library_subdir),
            "raw_dir": raw_dir,
            "published_dir": published_dir,
            "litepan_task": task_name,
            "litepan_rule": event_name,
            "emby_library": emby_label,
        },
        "patch": patch,
        "cloud_library_root": paths.cloud_library_root,
        "raw_base": raw_base,
        "published_base": paths.litepan_output_root,
        "litepan_task": {
            "name": task_name, "path": cloud_quarter, "recursive": True,
            "output_folder": label, "group_dir": group,
            "schedule_mode": "manual", "status": "active",
            "sync_metadata": False, "time_window_enabled": False,
            "scan_mode": "incremental_update", "scan_interval": 60,
            "api_interval": 1, "branch_check_enabled": False,
        },
        "litepan_rule": {
            "name": event_name, "trigger_type": "webhook", "status": "running",
            "trigger_config": {"event": event_name, "source": config.runtime.litepan_event_source},
        },
        "emby_library": {"name": emby_label, "path": emby_dir},
    }


__all__ = ["build_layout"]