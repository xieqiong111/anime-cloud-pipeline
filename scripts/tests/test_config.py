"""Focused tests for the V5 configuration and legacy migration contract."""
from __future__ import annotations

import copy
import json
from pathlib import Path
import unittest

from pipeline.config import ConfigError, ConfigSchema, migrate_legacy_config
from pipeline.models import EpisodeKey, EpisodeStatus, ReviewReason, RetryState, Subscription


ROOT = Path(__file__).resolve().parents[2]
EXAMPLE_CONFIG = ROOT / "assets" / "config.example.json"


def valid_config() -> dict:
    """Small complete V5 mapping used by validation tests."""
    return {
        "schema_version": 5,
        "services": {
            "moviepilot": {
                "name": "moviepilot",
                "base_url": "https://moviepilot.example.invalid",
                "token_env": "MOVIEPILOT_TOKEN",
                "media_source": "tmdb",
            },
            "cloud": {
                "name": "cloud",
                "credential_env": "CLOUD_COOKIE",
            },
            "litepan": {
                "name": "litepan",
                "base_url": "https://litepan.example.invalid",
                "api_key_env": "LITEPAN_API_KEY",
                "task_name": "anime-strm",
            },
            "emby": {
                "name": "emby",
                "base_url": "https://emby.example.invalid",
                "api_key_env": "EMBY_API_KEY",
                "library_name": "Anime",
            },
        },
        "paths": {
            "cloud_source_root": "/cloud/source",
            "cloud_library_root": "/cloud/library",
            "litepan_output_root": "/litepan/output",
            "moviepilot_mount_root": "/moviepilot/media",
            "emby_mount_root": "/emby/media",
            "state_directory": "/state/anime-pipeline",
        },
        "scoring": {
            "include_weight": 10,
            "language_weight": 5,
            "release_group_weight": 3,
            "resolution_weight": 2,
            "collection_penalty": 4,
        },
        "limits": {
            "network_timeout_seconds": 30,
            "max_read_retries": 3,
            "max_task_pages": 20,
            "max_recursive_depth": 8,
            "max_recursive_files": 1000,
            "max_new_tasks_per_run": 4,
            "max_new_tasks_per_subscription": 2,
            "max_review_batch": 50,
            "max_reconcile_batch": 50,
        },
        "retry": {
            "max_attempts": 3,
            "backoff_seconds": 1,
            "backoff_max_seconds": 30,
        },
        "schedule": {
            "timezone": "Asia/Shanghai",
            "cron": "0 2 * * *",
        },
    }


LEGACY_CONFIG = {
    "schema_version": 3,
    "source": {"type": "kisssub", "site_url": "https://source.example.invalid"},
    "subscriptions": [
        {
            "media_id": "295071",
            "season": 1,
            "kisssub_keyword": "示例动画",
            "include": "简",
        }
    ],
    "p115": {
        "credential_env": "P115_COOKIE",
        "target_directory_id": "legacy-directory",
        "target_path": "影视库/动画/新番",
    },
    "litepan": {
        "base_url": "https://litepan.example.invalid",
        "api_key_env": "LITEPAN_API_KEY",
        "strm_task_id": 5,
        "strm_task_name": "新番专用",
    },
    "moviepilot": {
        "base_url": "https://moviepilot.example.invalid",
        "token_env": "MOVIEPILOT_TOKEN",
        "auth_mode": "x-api-key",
        "media_source": "tmdb",
    },
    "emby": {
        "base_url": "https://emby.example.invalid",
        "api_key_env": "EMBY_API_KEY",
        "library_id": "legacy-library-id",
        "library_name": "Anime",
    },
    "paths": {
        "state_directory": "/state/anime-pipeline",
        "strm_root_local": "/media/strm/litepan",
        "moviepilot_strm_root": "/media/strm/litepan",
        "litepan_strm_prefix": "影视库/动画/新番",
    },
    "schedule": {"cron": "0 2 * * *", "local_time": "02:00"},
    "limits": {
        "network_timeout_seconds": 30,
        "max_read_retries": 3,
        "max_new_tasks_per_run": 4,
    },
}


def complete_legacy_alias_config() -> dict:
    """Complete legacy shape using every supported V5 section alias."""
    legacy = valid_config()
    services = legacy.pop("services")
    legacy["schema_version"] = 3
    legacy["moviepilot"] = services["moviepilot"]
    legacy["p115"] = services["cloud"]
    legacy["litepan"] = services["litepan"]
    legacy["emby"] = services["emby"]
    legacy["subscriptions"] = [{"media_id": "295071", "season": 1}]
    legacy["candidate_scoring"] = legacy.pop("scoring")
    limits = legacy.pop("limits")
    legacy["submission_limits"] = {
        key: value for key, value in limits.items()
        if key not in {"max_recursive_depth", "max_recursive_files"}
    }
    legacy["scan_limits"] = {
        "max_recursive_depth": limits["max_recursive_depth"],
        "max_recursive_files": limits["max_recursive_files"],
    }
    legacy["network_retry"] = legacy.pop("retry")
    return legacy


class ConfigTests(unittest.TestCase):
    def test_v5_config_rejects_local_subscription_catalogue(self):
        raw = valid_config()
        raw["subscriptions"] = [{"media_id": "111"}]
        with self.assertRaisesRegex(ConfigError, r"subscriptions.*MoviePilot"):
            ConfigSchema.from_mapping(raw, {}, "linux")

    def test_missing_path_reports_exact_field_and_repair(self):
        raw = valid_config()
        del raw["paths"]["emby_mount_root"]
        with self.assertRaisesRegex(ConfigError, r"paths\.emby_mount_root.*required"):
            ConfigSchema.from_mapping(raw, {}, "linux")

    def test_example_config_has_no_runtime_ids_or_subscriptions(self):
        cfg = json.loads(EXAMPLE_CONFIG.read_text(encoding="utf-8"))
        self.assertNotIn("subscriptions", cfg)
        self.assertNotIn("strm_task_id", cfg.get("litepan", {}))
        self.assertNotIn("library_id", cfg.get("emby", {}))
        parsed = ConfigSchema.from_mapping(cfg, {}, "linux")
        self.assertEqual(parsed.schema_version, 5)

    def test_legacy_config_migration_removes_subscription_copy_and_reports_it(self):
        migrated, report = migrate_legacy_config(LEGACY_CONFIG)
        self.assertNotIn("subscriptions", migrated)
        self.assertEqual(report.moviepilot_subscriptions_to_verify, ("295071:1",))
        self.assertEqual(migrated["limits"]["network_timeout_seconds"], 30)
        self.assertNotIn("strm_task_id", migrated.get("litepan", {}))
        self.assertNotIn("library_id", migrated.get("emby", {}))

    def test_config_sections_are_immutable_and_paths_are_absolute_posix(self):
        cfg = ConfigSchema.from_mapping(valid_config(), {}, "linux")
        with self.assertRaises((AttributeError, TypeError)):
            cfg.paths.cloud_source_root = "/other"  # type: ignore[misc]
        self.assertTrue(cfg.paths.cloud_source_root.startswith("/"))

    def test_invalid_limit_has_field_and_repair(self):
        raw = valid_config()
        raw["limits"]["max_task_pages"] = 0
        with self.assertRaisesRegex(ConfigError, r"limits\.max_task_pages.*positive"):
            ConfigSchema.from_mapping(raw, {}, "linux")

    def test_v5_config_rejects_nested_subscription_catalogues(self):
        raw = valid_config()
        raw["services"]["moviepilot"]["subscriptions"] = [{"media_id": "111"}]
        with self.assertRaisesRegex(ConfigError, r"subscriptions.*MoviePilot"):
            ConfigSchema.from_mapping(raw, {}, "linux")

    def test_present_invalid_retry_values_are_not_defaulted(self):
        for value in ("bad", float("nan"), float("inf"), float("-inf")):
            raw = valid_config()
            raw["retry"]["jitter_seconds"] = value
            with self.subTest(value=value), self.assertRaisesRegex(
                ConfigError, r"retry\.jitter_seconds"
            ):
                ConfigSchema.from_mapping(raw, {}, "linux")

    def test_nested_legacy_subscription_catalogue_is_removed_and_reported(self):
        legacy = copy.deepcopy(LEGACY_CONFIG)
        legacy["moviepilot"]["subscriptions"] = [
            {"media_id": "295071", "season": 1}
        ]
        original = copy.deepcopy(legacy)
        migrated, report = migrate_legacy_config(legacy)
        self.assertEqual(legacy, original)
        self.assertNotIn("subscriptions", migrated["moviepilot"])
        self.assertIn("moviepilot.subscriptions[0]", report.dropped_fields)
        self.assertEqual(report.moviepilot_subscriptions_to_verify, ("295071:1",))

    def test_migration_preserves_recognized_sections_and_input(self):
        original = copy.deepcopy(LEGACY_CONFIG)
        migrated, _ = migrate_legacy_config(original)
        self.assertEqual(original, LEGACY_CONFIG)
        self.assertEqual(migrated["moviepilot"]["token_env"], "MOVIEPILOT_TOKEN")
        self.assertEqual(migrated["paths"], LEGACY_CONFIG["paths"])
        self.assertEqual(migrated["limits"]["network_timeout_seconds"], 30)
        self.assertEqual(migrated["schedule"]["cron"], "0 2 * * *")

    def test_complete_legacy_mapping_migrates_to_loadable_v5(self):
        legacy = complete_legacy_alias_config()
        migrated, report = migrate_legacy_config(legacy)
        self.assertEqual(report.blocking_unmapped_fields, ())
        self.assertEqual(ConfigSchema.from_mapping(migrated, {}, "linux").schema_version, 5)

    def test_malformed_complete_alias_mapping_is_blocked_with_actionable_fields(self):
        mutations = (
            ("paths.emby_mount_root", lambda cfg: cfg["paths"].update({"emby_mount_root": "../unsafe"})),
            ("retry.jitter_seconds", lambda cfg: cfg["network_retry"].update({"jitter_seconds": "bad"})),
            ("services.moviepilot.token_env", lambda cfg: cfg["moviepilot"].update({"token_env": "bad ref"})),
        )
        for expected_field, mutate in mutations:
            with self.subTest(expected_field=expected_field):
                legacy = complete_legacy_alias_config()
                mutate(legacy)
                migrated, report = migrate_legacy_config(legacy)
                self.assertIn(expected_field, report.blocking_unmapped_fields)
                self.assertTrue(migrated.get("migration_blocked"))
                with self.assertRaises(ConfigError):
                    ConfigSchema.from_mapping(migrated, {}, "linux")

    def test_malformed_canonical_retry_cannot_be_shadowed_by_valid_alias(self):
        legacy = complete_legacy_alias_config()
        legacy["retry"] = {"max_attempts": "bad"}
        migrated, report = migrate_legacy_config(legacy)
        self.assertIn("retry.max_attempts", report.blocking_unmapped_fields)
        self.assertTrue(migrated.get("migration_blocked"))
        self.assertNotEqual(migrated.get("schema_version"), 5)

    def test_non_mapping_alias_sections_are_blocked(self):
        aliases = ("scoring", "limits", "retry", "candidate_scoring",
                   "submission_limits", "scan_limits", "network_retry", "moviepilot")
        for alias in aliases:
            with self.subTest(alias=alias):
                legacy = complete_legacy_alias_config()
                legacy[alias] = "not-a-mapping"
                migrated, report = migrate_legacy_config(legacy)
                self.assertTrue(report.blocking_unmapped_fields)
                self.assertTrue(migrated.get("migration_blocked"))
                self.assertNotEqual(migrated.get("schema_version"), 5)

    def test_conflicting_valid_alias_sections_are_blocked_as_ambiguous(self):
        legacy = complete_legacy_alias_config()
        legacy["retry"] = {"max_attempts": 9}
        migrated, report = migrate_legacy_config(legacy)
        self.assertTrue(migrated.get("migration_blocked"))
        self.assertNotEqual(migrated.get("schema_version"), 5)
        self.assertIn("retry.max_attempts", report.blocking_unmapped_fields)

    def test_incomplete_legacy_mapping_is_marked_blocked(self):
        legacy = copy.deepcopy(LEGACY_CONFIG)
        migrated, report = migrate_legacy_config(legacy)
        self.assertTrue(report.blocking_unmapped_fields)
        self.assertTrue(migrated.get("migration_blocked"))
        with self.assertRaisesRegex(ConfigError, r"migration_blocked"):
            ConfigSchema.from_mapping(migrated, {}, "linux")

    def test_service_aliases_normalize_to_loadable_canonical_semantics(self):
        for service, key, alias, expected in (
            ("litepan", "task_name", "strm_task_name", "anime-strm"),
            ("emby", "library_name", "library", "Anime"),
        ):
            with self.subTest(service=service):
                legacy = valid_config()
                legacy["schema_version"] = 3
                legacy[service] = copy.deepcopy(legacy["services"][service])
                legacy[service][alias] = legacy[service].pop(key)
                legacy[service]["base_url"] += "/"
                legacy[service]["auth_mode"] = "x-api-key"
                legacy[service]["media_source"] = "tmdb"
                original = copy.deepcopy(legacy)
                migrated, report = migrate_legacy_config(legacy)
                self.assertEqual(report.blocking_unmapped_fields, ())
                self.assertEqual(legacy, original)
                parsed = ConfigSchema.from_mapping(migrated, {}, "linux")
                self.assertEqual(getattr(getattr(parsed.services, service), key), expected)
                self.assertEqual(migrated["services"][service][key], expected)
                self.assertNotIn(alias, migrated["services"][service])

    def test_migration_service_names_default_consistently_for_all_aliases(self):
        for cloud_alias in ("cloud", "p115", "storage"):
            with self.subTest(cloud_alias=cloud_alias):
                legacy = complete_legacy_alias_config()
                cloud = legacy.pop("p115")
                legacy[cloud_alias] = cloud
                for name in ("moviepilot", cloud_alias, "litepan", "emby"):
                    legacy[name].pop("name", None)
                migrated, report = migrate_legacy_config(legacy)
                self.assertEqual(report.blocking_unmapped_fields, ())
                parsed = ConfigSchema.from_mapping(migrated, {}, "linux")
                self.assertEqual(parsed.services.cloud.name, "p115")
                self.assertEqual(parsed.services.litepan.name, "litepan")

    def test_malformed_services_root_is_preserved_and_blocks_migration(self):
        for value in (None, [], "invalid", 0, False):
            with self.subTest(value=value):
                legacy = complete_legacy_alias_config()
                legacy["services"] = value
                migrated, report = migrate_legacy_config(legacy)
                self.assertEqual(report.blocking_unmapped_fields, ("services",))
                self.assertEqual(migrated["services"], value)
                self.assertTrue(migrated["migration_blocked"])
                self.assertNotEqual(migrated["schema_version"], 5)
                with self.assertRaisesRegex(ConfigError, "services.*fix:"):
                    ConfigSchema.from_mapping(migrated, {}, "linux")

    def test_conflicting_service_field_aliases_block_migration(self):
        for separate in (True, False):
            with self.subTest(separate=separate):
                legacy = valid_config()
                legacy["schema_version"] = 3
                if separate:
                    legacy["litepan"] = copy.deepcopy(legacy["services"]["litepan"])
                    service = legacy["litepan"]
                    service.pop("task_name")
                else:
                    service = legacy["services"]["litepan"]
                service["strm_task_name"] = "other-task"
                migrated, report = migrate_legacy_config(legacy)
                self.assertTrue(report.blocking_unmapped_fields)
                self.assertTrue(migrated.get("migration_blocked"))
                self.assertNotEqual(migrated["schema_version"], 5)

    def test_cloud_library_subdir_round_trips_and_keeps_roots_distinct(self):
        # Invariant (3): cloud_library_subdir only shapes cloud targets; the raw
        # and published STRM roots stay disjoint so _validate_distinct_roots
        # still holds.
        raw = valid_config()
        raw["paths"]["cloud_library_subdir"] = "整理"
        raw["paths"]["litepan_raw_output_root"] = "/litepan/raw"
        cfg = ConfigSchema.from_mapping(raw, {}, "linux")

        self.assertEqual(cfg.paths.cloud_library_subdir, "整理")
        self.assertIsNone(cfg.paths.litepan_publish_subdir)
        dumped = cfg.to_mapping()["paths"]
        self.assertEqual(dumped["cloud_library_subdir"], "整理")
        self.assertNotIn("litepan_publish_subdir", dumped)
        raw_root = cfg.paths.litepan_raw_output_root
        published_root = cfg.paths.litepan_output_root
        self.assertFalse(
            raw_root == published_root
            or raw_root.startswith(published_root + "/")
            or published_root.startswith(raw_root + "/")
        )

    def test_cloud_library_subdir_does_not_break_litepan_geometry(self):
        # The subdir sits below the quarter root, so the litepan_task_source_root
        # / litepan_task_output_root quarter regex must still accept the deploy.
        raw = valid_config()
        raw["paths"]["cloud_library_subdir"] = "整理"
        raw["paths"]["litepan_raw_output_root"] = "/litepan/raw"
        raw["runtime"] = {
            "litepan_task_name": "anime-strm",
            "emby_library_name": "Anime",
            "litepan_task_source_root": "/cloud/library/2026-07月新番",
            "litepan_task_output_root": "/litepan/raw/2026-07月新番",
            "emby_library_root": "/emby/media/2026-07月新番",
        }
        cfg = ConfigSchema.from_mapping(raw, {}, "linux")
        self.assertEqual(cfg.runtime.litepan_task_source_root, "/cloud/library/2026-07月新番")

    def test_cloud_library_subdir_rejects_path_separators(self):
        raw = valid_config()
        raw["paths"]["cloud_library_subdir"] = "a/b"
        with self.assertRaisesRegex(ConfigError, r"paths\.cloud_library_subdir"):
            ConfigSchema.from_mapping(raw, {}, "linux")


class DomainModelTests(unittest.TestCase):
    def test_episode_key_has_requested_identity_semantics(self):
        a = EpisodeKey("tmdb", "111", 1, 8)
        b = EpisodeKey("tmdb", "111", 1, 8)
        self.assertEqual(a, b)
        self.assertEqual(a.as_tuple(), (a.media_source, a.media_id, a.season, a.episode))

    def test_episode_key_rejects_missing_identity_and_non_integral_numbers(self):
        for args in (
            (None, "111", 1, 8),
            ("tmdb", None, 1, 8),
            ("tmdb", "", 1, 8),
            ("tmdb", "111", None, 8),
            ("tmdb", "111", 1.5, 8),
            ("tmdb", "111", True, 8),
            ("tmdb", "111", 1, None),
            ("tmdb", "111", 1, 8.5),
        ):
            with self.subTest(args=args), self.assertRaises(ValueError):
                EpisodeKey(*args)

    def test_domain_types_are_frozen(self):
        models = [
            EpisodeKey("tmdb", "111", 1, 8),
            Subscription("7", "tmdb", "111", "Show", (), 1, "", ""),
            RetryState(),
        ]
        for model in models:
            with self.subTest(model=type(model).__name__):
                with self.assertRaises((AttributeError, TypeError)):
                    model.changed = True  # type: ignore[attr-defined]

    def test_subscription_rejects_missing_identity_text(self):
        for kwargs in (
            {"subscription_id": None},
            {"media_source": None},
            {"media_id": None},
            {"work": None},
            {"work": "   "},
        ):
            values = {
                "subscription_id": "7", "media_source": "tmdb", "media_id": "111",
                "work": "Show",
            }
            values.update(kwargs)
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                Subscription(**values)

    def test_status_and_review_reason_values_are_stable(self):
        self.assertEqual(EpisodeStatus.DISCOVERED.value, "discovered")
        self.assertEqual(EpisodeStatus.NOTIFIED.value, "notified")
        self.assertEqual(ReviewReason.RUNTIME_MAPPING_AMBIGUOUS.value,
                         "RUNTIME_MAPPING_AMBIGUOUS")

    def test_required_status_and_review_reason_values_are_complete(self):
        self.assertEqual(
            {item.value for item in EpisodeStatus},
            {
                "discovered", "download_pending", "submitted", "cloud_ready",
                "organize_pending", "organized", "strm_pending", "strm_ready",
                "scrape_pending", "metadata_ready", "library_pending", "library_ready",
                "needs_review", "notified",
            },
        )
        self.assertEqual(
            {item.value for item in ReviewReason},
            {
                "MULTIPLE_VIDEO_FILES", "AMBIGUOUS_EPISODE", "NO_VALID_VIDEO",
                "PATH_CONFLICT", "METADATA_CONFLICT", "DUPLICATE_EPISODE",
                "UNEXPECTED_REMOTE_STATE", "LEGACY_IDENTITY_UNRESOLVED",
                "RUNTIME_MAPPING_AMBIGUOUS",
            },
        )


if __name__ == "__main__":
    unittest.main(verbosity=2)
