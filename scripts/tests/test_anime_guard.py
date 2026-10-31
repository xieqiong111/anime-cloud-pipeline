"""Red/green tests for the opt-in V5 safety discovery guard.

The guard must:

* exclude a non-animation subscription (TMDB genre id 16 absent),
* fail closed on a subscription whose genre cannot be proven at all,
* dedup a discovery decision against a migrated ``legacy`` row that already
  owns the same ``(tmdb_id, season, episode)`` identity,
* block the write entirely when a legacy identity is unverified (so an
  unknown duplicate can never be silently re-downloaded), and
* leave a genuinely missing episode downloadable (the guard must never
  permanently block a real gap).
"""
import json
import os
import tempfile
import unittest

from pipeline.adapters import FakeMoviePilot, FakeSourceProvider
from pipeline.anime_guard import (
    DUPLICATE_OWNED,
    GENRE_UNKNOWN,
    NON_ANIME,
    UNVERIFIED_COLLISION,
    AnimeClassification,
    DiscoveryGuard,
    DiscoveryGuardError,
    anime_exclusion_reason,
    classify_anime,
)
from pipeline.discovery import DiscoveryService
from pipeline.state import StateStore

WINDOW = {"season_quarter": "2026-07", "start": "2026-07-01", "end": "2026-09-30"}


def _sub(sub_id, media_id, *, genre_ids=None, air_date="2026-07-03", season=1):
    payload = {
        "id": sub_id,
        "name": f"Show {sub_id}",
        "media_source": "tmdb",
        "media_id": media_id,
        "season": season,
        "tmdb_season_air_date": air_date,
    }
    if genre_ids is not None:
        payload["tmdb_genre_ids"] = list(genre_ids)
    return payload


def _insert_legacy_row(store, *, media_id, season, episode, tmdb_id=None,
                       legacy_id=None, media_source="legacy"):
    evidence = {"tmdb_id": tmdb_id} if tmdb_id is not None else {}
    store.conn.execute(
        """INSERT INTO episodes_v5
           (legacy_id, media_source, media_id, canonical_work, season, episode,
            status, legacy_evidence_json, created_at, updated_at)
           VALUES (?, ?, ?, 'Work', ?, ?, 'needs_review', ?, 0.0, 0.0)""",
        (legacy_id, media_source, media_id, season, episode, json.dumps(evidence)),
    )
    store.conn.commit()


class AnimeGateUnitTests(unittest.TestCase):
    def test_tmdb_animation_genre_id_is_accepted(self):
        self.assertIs(classify_anime({"tmdb_genre_ids": [10759, 16, 10765]}),
                      AnimeClassification.ANIME)
        self.assertIsNone(anime_exclusion_reason({"tmdb_genre_ids": [16, 35]}))

    def test_non_animation_genre_is_excluded(self):
        self.assertIs(classify_anime({"tmdb_genre_ids": [35]}),
                      AnimeClassification.NON_ANIME)
        self.assertEqual(anime_exclusion_reason({"tmdb_genre_ids": [35]}), NON_ANIME)

    def test_missing_genre_fails_closed(self):
        for payload in ({}, {"tmdb_genre_ids": []}, {"tmdb_genre_ids": None},
                        {"tmdb_genre_ids": ["", "x"]}):
            with self.subTest(payload=payload):
                self.assertIs(classify_anime(payload), AnimeClassification.UNKNOWN)
                self.assertEqual(anime_exclusion_reason(payload), GENRE_UNKNOWN)

    def test_localized_name_without_numeric_id_is_unknown(self):
        # A name is localized and ambiguous; only the numeric id may prove it.
        self.assertIs(classify_anime({"genres": [{"id": None, "name": "动画"}]}),
                      AnimeClassification.UNKNOWN)
        self.assertIs(classify_anime({"genres": ["动画"]}), AnimeClassification.UNKNOWN)


class IdentityDedupUnitTests(unittest.TestCase):
    def test_verified_legacy_row_owns_identity(self):
        guard = DiscoveryGuard()
        guard.bind([
            {"id": 7, "media_source": "legacy", "media_id": "111", "season": 1,
             "episode": 1, "legacy_evidence_json": json.dumps({"tmdb_id": 111})},
        ])
        self.assertEqual(guard.context.verified_rows, 1)
        self.assertEqual(guard.context.unverified_rows, 0)
        self.assertEqual(guard.decision_exclusion(
            {"media_id": "111", "season": 1, "episode": 1}), DUPLICATE_OWNED)

    def test_gap_episode_is_not_deduped(self):
        guard = DiscoveryGuard()
        guard.bind([
            {"id": 7, "media_source": "legacy", "media_id": "111", "season": 1,
             "episode": 1, "legacy_evidence_json": json.dumps({"tmdb_id": 111})},
        ])
        # Same title/season but a different episode is a genuine gap: allowed.
        self.assertIsNone(guard.decision_exclusion(
            {"media_id": "111", "season": 1, "episode": 2}))

    def test_unverified_legacy_row_is_not_claimed_and_blocks(self):
        guard = DiscoveryGuard()
        guard.bind([
            {"id": 9, "media_source": "legacy", "media_id": "111", "season": 1,
             "episode": 5, "legacy_evidence_json": "{}"},
        ])
        self.assertEqual(guard.context.verified_rows, 0)
        self.assertEqual(guard.context.unverified_rows, 1)
        self.assertEqual(guard.decision_exclusion(
            {"media_id": "111", "season": 1, "episode": 5}), UNVERIFIED_COLLISION)
        verdict = guard.preflight([{"media_id": "111", "season": 1}])
        self.assertFalse(verdict.safe)
        self.assertTrue(verdict.blocked_reasons[0].startswith(UNVERIFIED_COLLISION))
        with self.assertRaises(DiscoveryGuardError):
            guard.assert_write_allowed([{"media_id": "111", "season": 1}])


class GuardedDiscoveryIntegrationTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory(prefix="anime-guard-")
        self.db_path = os.path.join(self.tmp.name, "state.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_disabled_by_default_keeps_historical_behaviour(self):
        adapter = FakeMoviePilot(subscriptions=[
            _sub("sub-anime", "111", genre_ids=[16]),
            _sub("sub-live", "222", genre_ids=[35]),
            _sub("sub-unknown", "333", genre_ids=None),
        ])
        provider = FakeSourceProvider(results={
            "sub-anime": [{"title": "Show sub-anime S01E01", "infohash": "a" * 40}],
            "sub-live": [{"title": "Show sub-live S01E01", "infohash": "b" * 40}],
            "sub-unknown": [{"title": "Show sub-unknown S01E01", "infohash": "c" * 40}],
        })
        store = StateStore(self.db_path)
        try:
            plan = DiscoveryService(store, adapter, [provider]).prepare(WINDOW, False)
            self.assertFalse(plan.errors)
            self.assertEqual(len(plan.subscriptions), 3)  # no guard -> unchanged
            self.assertEqual(store.conn.execute(
                "SELECT COUNT(*) FROM episodes_v5").fetchone()[0], 3)
        finally:
            store.close()

    def test_guard_excludes_non_anime_and_unknown(self):
        adapter = FakeMoviePilot(subscriptions=[
            _sub("sub-anime", "111", genre_ids=[16]),
            _sub("sub-live", "222", genre_ids=[35]),
            _sub("sub-unknown", "333", genre_ids=None),
        ])
        provider = FakeSourceProvider(results={
            "sub-anime": [{"title": "Show sub-anime S01E01", "infohash": "a" * 40}],
            "sub-live": [{"title": "Show sub-live S01E01", "infohash": "b" * 40}],
            "sub-unknown": [{"title": "Show sub-unknown S01E01", "infohash": "c" * 40}],
        })
        store = StateStore(self.db_path)
        try:
            plan = DiscoveryService(
                store, adapter, [provider], guard=DiscoveryGuard(),
            ).prepare(WINDOW, False)
            self.assertFalse(plan.errors)
            self.assertEqual([s.media_id for s in plan.subscriptions], ["111"])
            reasons = [item.reason for item in plan.exclusions]
            self.assertIn(NON_ANIME, reasons)
            self.assertIn(GENRE_UNKNOWN, reasons)
            rows = store.conn.execute(
                "SELECT media_id FROM episodes_v5 ORDER BY media_id").fetchall()
            self.assertEqual([tuple(r) for r in rows], [("111",)])
        finally:
            store.close()

    def test_guard_dedups_legacy_owner_but_allows_gap(self):
        adapter = FakeMoviePilot(subscriptions=[_sub("sub-anime", "111", genre_ids=[16])])
        provider = FakeSourceProvider(results={
            "sub-anime": [
                {"title": "Show sub-anime S01E01", "infohash": "a" * 40},
                {"title": "Show sub-anime S01E02", "infohash": "b" * 40},
            ],
        })
        store = StateStore(self.db_path)
        try:
            _insert_legacy_row(store, media_id="111", season=1, episode=1,
                               tmdb_id=111, legacy_id=1)
            plan = DiscoveryService(
                store, adapter, [provider], guard=DiscoveryGuard(),
            ).prepare(WINDOW, False)
            self.assertFalse(plan.errors)
            # E01 is owned by the migrated legacy row -> deduped, not re-added.
            self.assertEqual([(d.media_id, d.season, d.episode) for d in plan.decisions],
                             [("111", 1, 2)])
            self.assertIn(DUPLICATE_OWNED, [item.reason for item in plan.exclusions])
            keys = store.conn.execute(
                "SELECT media_source, media_id, season, episode FROM episodes_v5 "
                "ORDER BY media_source, episode").fetchall()
            self.assertEqual([tuple(r) for r in keys],
                             [("legacy", "111", 1, 1), ("tmdb", "111", 1, 2)])
        finally:
            store.close()

    def test_guard_blocks_write_when_legacy_identity_unverified(self):
        adapter = FakeMoviePilot(subscriptions=[_sub("sub-anime", "111", genre_ids=[16])])
        provider = FakeSourceProvider(results={
            "sub-anime": [{"title": "Show sub-anime S01E01", "infohash": "a" * 40}],
        })
        store = StateStore(self.db_path)
        try:
            _insert_legacy_row(store, media_id="111", season=1, episode=3,
                               tmdb_id=None, legacy_id=1)
            with self.assertRaises(DiscoveryGuardError) as caught:
                DiscoveryService(
                    store, adapter, [provider], guard=DiscoveryGuard(),
                ).prepare(WINDOW, False)
            self.assertFalse(caught.exception.verdict.safe)
            # Fail closed *before* persisting anything.
            self.assertEqual(store.conn.execute(
                "SELECT COUNT(*) FROM episodes_v5").fetchone()[0], 1)
            self.assertEqual(store.conn.execute(
                "SELECT COUNT(*) FROM subscriptions").fetchone()[0], 0)
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
