"""Red/green tests for the opt-in subscription-source filter.

The filter is the first, local, deterministic gate in ``DiscoveryService``.
It only activates when ``moviepilot.subscription_sources`` is configured:

* a subscription whose MoviePilot ``username`` (source) is allowed passes,
* a subscription from an unlisted source is excluded with a stable reason,
* a subscription with a missing/empty ``username`` fails closed, and
* an unconfigured allow-list leaves discovery's historical behaviour intact.
"""
from __future__ import annotations

import copy
import unittest

from pipeline.adapters import FakeMoviePilot
from pipeline.config import ConfigError, ConfigSchema
from pipeline.discovery import DiscoveryService


WINDOW = {"season_quarter": "2026-07", "start": "2026-07-01", "end": "2026-09-30"}

BANGUMI = "Bangumi订阅"
ADMIN = "admin"
PAN = "网盘订阅助手"


def raw_config(subscription_sources=None):
    """Complete V5 mapping; optionally opts in to source filtering."""
    raw = {
        "schema_version": 5,
        "services": {
            "moviepilot": {
                "name": "moviepilot",
                "base_url": "https://moviepilot.example.invalid",
                "token_env": "MOVIEPILOT_TOKEN",
                "media_source": "tmdb",
            },
            "cloud": {"name": "cloud", "credential_env": "CLOUD_COOKIE"},
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
        "schedule": {"timezone": "Asia/Shanghai", "cron": "0 2 * * *"},
    }
    if subscription_sources is not None:
        raw["services"]["moviepilot"]["subscription_sources"] = subscription_sources
    return raw


def config_with(subscription_sources=None):
    return ConfigSchema.from_mapping(raw_config(subscription_sources))


def subscription(sub_id, *, username, name=None, tmdbid=111):
    payload = {
        "id": sub_id,
        "name": name or f"Show {sub_id}",
        "tmdbid": tmdbid,
        "season": 1,
        "tmdb_season_air_date": "2026-07-03",
    }
    if username is not None:
        payload["username"] = username
    return payload


class ConfigParsingTests(unittest.TestCase):
    def test_subscription_sources_parse_and_serialize(self):
        cfg = config_with([BANGUMI, ADMIN])
        self.assertEqual(cfg.moviepilot.subscription_sources, (BANGUMI, ADMIN))
        mapping = cfg.to_mapping()
        self.assertEqual(
            mapping["services"]["moviepilot"]["subscription_sources"], [BANGUMI, ADMIN]
        )

    def test_subscription_sources_default_to_empty_when_absent(self):
        cfg = config_with()
        self.assertEqual(cfg.moviepilot.subscription_sources, ())
        self.assertNotIn(
            "subscription_sources", cfg.to_mapping()["services"]["moviepilot"]
        )

    def test_empty_list_is_treated_as_no_filter(self):
        cfg = config_with([])
        self.assertEqual(cfg.moviepilot.subscription_sources, ())

    def test_malformed_subscription_sources_fail_closed(self):
        for bad in ("Bangumi订阅", [""], [1], [BANGUMI, None], {"a": "b"}):
            with self.assertRaises(ConfigError):
                config_with(bad)

    def test_round_trip_preserves_allow_list(self):
        first = config_with([BANGUMI, ADMIN])
        second = ConfigSchema.from_mapping(first.to_mapping())
        self.assertEqual(second.moviepilot.subscription_sources, (BANGUMI, ADMIN))


class SubscriptionSourceFilterTests(unittest.TestCase):
    def _prepare(self, subscriptions, *, sources=None, config=None):
        adapter = FakeMoviePilot(subscriptions=subscriptions)
        if config is None:
            config = config_with(sources)
        return DiscoveryService(None, adapter, (), config=config).prepare(WINDOW, True)

    def test_allowed_source_passes(self):
        plan = self._prepare(
            [subscription("sub-bangumi", username=BANGUMI)], sources=[BANGUMI, ADMIN]
        )
        self.assertEqual(plan.summary["subscription_count"], 1)
        self.assertEqual(plan.summary["excluded_count"], 0)
        self.assertEqual([s.subscription_id for s in plan.subscriptions], ["sub-bangumi"])

    def test_unlisted_source_is_excluded_with_reason(self):
        plan = self._prepare(
            [subscription("sub-pan", username=PAN)], sources=[BANGUMI, ADMIN]
        )
        self.assertEqual(plan.summary["subscription_count"], 0)
        self.assertEqual(plan.summary["excluded_count"], 1)
        self.assertIn("excluded_subscription_source", plan.summary["excluded"])
        self.assertEqual(plan.exclusions[0].subscription_id, "sub-pan")
        self.assertEqual(plan.exclusions[0].reason, "excluded_subscription_source")

    def test_missing_username_is_excluded(self):
        plan = self._prepare(
            [subscription("sub-nouser", username=None)], sources=[BANGUMI, ADMIN]
        )
        self.assertEqual(plan.summary["subscription_count"], 0)
        self.assertEqual(plan.summary["excluded_count"], 1)
        self.assertEqual(plan.exclusions[0].reason, "subscription_source_missing")

    def test_mixed_batch_keeps_allowed_and_drops_others(self):
        plan = self._prepare(
            [
                subscription("sub-a", username=BANGUMI, tmdbid=1),
                subscription("sub-b", username=PAN, tmdbid=2),
                subscription("sub-c", username=ADMIN, tmdbid=3),
                subscription("sub-d", username=None, tmdbid=4),
            ],
            sources=[BANGUMI, ADMIN],
        )
        self.assertEqual(plan.summary["subscription_count"], 2)
        self.assertEqual(plan.summary["excluded_count"], 2)
        self.assertEqual(
            sorted(s.subscription_id for s in plan.subscriptions), ["sub-a", "sub-c"]
        )
        reasons = sorted(item.reason for item in plan.exclusions)
        self.assertEqual(
            reasons, ["excluded_subscription_source", "subscription_source_missing"]
        )

    def test_unconfigured_filter_matches_baseline(self):
        baseline = self._prepare([subscription("sub-pan", username=PAN)], config=None)
        unconfigured = self._prepare(
            [subscription("sub-pan", username=PAN)], config=config_with()
        )
        for plan in (baseline, unconfigured):
            self.assertEqual(plan.summary["subscription_count"], 1)
            self.assertEqual(plan.summary["excluded_count"], 0)
        self.assertEqual(baseline.to_mapping(), unconfigured.to_mapping())

    def test_unconfigured_filter_ignores_missing_username(self):
        plan = self._prepare([subscription("sub-nouser", username=None)], config=None)
        self.assertEqual(plan.summary["subscription_count"], 1)
        self.assertEqual(plan.summary["excluded_count"], 0)


if __name__ == "__main__":
    unittest.main()
