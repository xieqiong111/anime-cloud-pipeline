"""Bounded discovery uses the same subscription identity as RSS normalization."""
from dataclasses import replace
import unittest

from pipeline.discovery import DiscoveryService, source_result_in_window
from pipeline.models import Subscription
from pipeline.parsers import SourceItem


SUB = Subscription("sub-1", "tmdb", "12345", "Show", season=1, season_air_date="2026-10-03")
WINDOW = {"season_quarter": "2026-10", "backfill": {"media_id": "12345", "season": 1, "start": 4, "end": 4}}


def rss(title="Show - 04 [1080p]", **kwargs):
    return SourceItem(title=title, link="https://source.invalid/item", infohash="a" * 40, torrent_url="", **kwargs)


class RssBackfillWindowTests(unittest.TestCase):
    def test_plain_rss_inherits_verified_subscription_identity(self):
        item = rss()
        self.assertTrue(source_result_in_window(SUB, item, WINDOW))
        decision = DiscoveryService(None, None, [])._normalize_decision(SUB, item, "rss")
        self.assertEqual((decision.media_id, decision.season, decision.episode, decision.decision), ("12345", 1, 4, "candidate"))
        self.assertEqual(item.raw, {})
        self.assertIsNone(item.season)

    def test_tmdb_aliases_are_equivalent(self):
        for source in ("tmdb", "themoviedb"):
            self.assertTrue(source_result_in_window(replace(SUB, media_source=source), rss(raw={"media_source": "themoviedb", "media_id": "12345"}), WINDOW))

    def test_subscription_must_match_target_work_source_and_season(self):
        for sub in (replace(SUB, media_id="999"), replace(SUB, media_source="legacy"), replace(SUB, media_source="other"), replace(SUB, season=2), replace(SUB, season=None)):
            with self.subTest(sub=sub):
                self.assertFalse(source_result_in_window(sub, rss(), WINDOW))

    def test_explicit_identity_conflicts_are_rejected(self):
        for raw in ({"media_id": "999"}, {"tmdb_id": "999"}, {"media_id": "12345", "tmdb_id": "999"}, {"media_source": "other"}, {"season": 2}, {"episode": 3}, {"episode_end": 0}, {"season": 1.2}, {"episode": True}):
            with self.subTest(raw=raw):
                self.assertFalse(source_result_in_window(SUB, rss(raw=raw), WINDOW))
        self.assertFalse(source_result_in_window(SUB, rss("Show S02E04"), WINDOW))
        self.assertFalse(source_result_in_window(SUB, rss("Show S01E04", season=2), WINDOW))

    def test_episode_identity_must_be_known_and_fully_contained(self):
        for title in ("Show [1080p]", "Show - 03", "Show S01E03-E04", "Show S01E04-E05", "Show S01E05-E04"):
            with self.subTest(title=title):
                self.assertFalse(source_result_in_window(SUB, rss(title), WINDOW))

    def test_publication_window_still_requires_date_evidence(self):
        window = {**WINDOW, "published_date": "2026-10-28", "timezone": "Asia/Shanghai"}
        self.assertTrue(source_result_in_window(SUB, rss(published="2026-10-28T10:00:00+08:00"), window))
        self.assertFalse(source_result_in_window(SUB, rss(published="2026-10-27T10:00:00+08:00"), window))
        self.assertFalse(source_result_in_window(SUB, rss(), window))

    def test_normalization_still_rejects_another_work_title(self):
        item = rss("Other Work - 04")
        self.assertTrue(source_result_in_window(SUB, item, WINDOW))
        decision = DiscoveryService(None, None, [])._normalize_decision(SUB, item, "rss")
        self.assertEqual((decision.decision, decision.reason), ("excluded", "work_mismatch"))
