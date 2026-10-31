import json
import os
import tempfile
import unittest

from pipeline.adapters import FakeMoviePilot, FakeSourceProvider
from pipeline.discovery import DiscoveryService
from pipeline.state import StateStore


SUB_A = {"id": "sub-a", "name": "Show A", "tmdbid": 111, "season": 1,
         "tmdb_season_air_date": "2026-07-03"}
SUB_B = {"id": "sub-b", "name": "Show B", "tmdbid": 222, "season": 2,
         "tmdb_season_air_date": "2026-08-03"}
WINDOW = {"start": "2026-09-19", "end": "2026-09-20", "season_quarter": "2026-07"}
RESULTS = {
    "sub-a": [{
        "title": "Show A S01E01 1080p",
        "link": "https://source.invalid/a",
        "infohash": "a" * 40,
    }],
    "sub-b": [{
        "title": "Show B S02E03 1080p",
        "link": "https://source.invalid/b",
        "infohash": "b" * 40,
    }],
}


class DiscoveryPreparationTests(unittest.TestCase):
    def test_secret_identity_components_keep_distinct_persistent_keys(self):
        secrets = ("alpha-private-id", "beta-private-id")
        adapter = FakeMoviePilot(subscriptions=[
            {**SUB_A, "id": secret, "media_source": secret, "media_id": secret}
            for secret in secrets
        ])
        provider = FakeSourceProvider(results=[{
            "title": "Show A S01E03", "infohash": "a" * 40,
        }])
        dry = DiscoveryService(None, adapter, [provider], secrets=secrets).prepare(WINDOW, True)
        store = StateStore(self.db_path)
        try:
            original_keys = None
            for configured_secrets in (secrets, tuple(reversed(secrets)) + ("unused-secret",)):
                run = DiscoveryService(
                    store, adapter, [provider], secrets=configured_secrets,
                ).prepare(WINDOW, False)
                self.assertFalse(run.errors)
                self.assertEqual(dry.to_mapping(), run.to_mapping())
                for table in ("subscriptions", "episodes_v5", "candidates"):
                    self.assertEqual(store.conn.execute(
                        f"SELECT COUNT(*) FROM {table}"
                    ).fetchone()[0], 2)
                rows = store.conn.execute(
                    "SELECT s.subscription_id, s.media_source, s.media_id "
                    "FROM subscriptions s JOIN episodes_v5 e "
                    "ON e.subscription_id=s.subscription_id AND e.media_source=s.media_source "
                    "AND e.media_id=s.media_id AND e.season=s.season ORDER BY s.id"
                ).fetchall()
                keys = [tuple(row) for row in rows]
                self.assertEqual(len(keys), 2)
                self.assertNotEqual(keys[0], keys[1])
                for key in keys:
                    self.assertEqual(len(set(key)), 3)  # Domain separation for equal raw text.
                    self.assertNotIn("[REDACTED]", key)
                if original_keys is not None:
                    self.assertEqual(keys, original_keys)
                original_keys = keys
                dump = "\n".join(store.conn.iterdump())
                for secret in secrets:
                    self.assertNotIn(secret, dump)
                    self.assertNotIn(secret, json.dumps(run.to_mapping()))
        finally:
            store.close()

    def test_secret_candidate_hashes_keep_distinct_idempotent_records(self):
        secrets = ("alpha-private-hash", "beta-private-hash")
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(results=[
            {"title": "Show A S01E03", "infohash": secret} for secret in secrets
        ])
        store = StateStore(self.db_path)
        try:
            for _ in range(2):
                plan = DiscoveryService(store, adapter, [provider], secrets=secrets).prepare(WINDOW, False)
                self.assertFalse(plan.errors)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM episodes_v5").fetchone()[0], 1)
                self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 2)
                row = store.conn.execute(
                    "SELECT subscription_id, media_source, media_id FROM subscriptions"
                ).fetchone()
                self.assertEqual(tuple(row), ("sub-a", "tmdb", "111"))
                for secret in secrets:
                    self.assertNotIn(secret, "\n".join(store.conn.iterdump()))
                    self.assertNotIn(secret, json.dumps(plan.to_mapping()))
        finally:
            store.close()

    def test_redaction_collisions_do_not_change_matching_decisions(self):
        cases = (
            ({"exclude": "AlphaSecret"}, "Show A BetaSecret S01E03", "candidate", "eligible"),
            ({"name": "AlphaSecret"}, "BetaSecret S01E03", "excluded", "work_mismatch"),
            ({"names": ["AlphaSecret"]}, "BetaSecret S01E03", "excluded", "work_mismatch"),
            ({"exclude": "AlphaSecret"}, "Show A AlphaSecret S01E03", "excluded", "subscription_exclude"),
            ({"name": "AlphaSecret"}, "AlphaSecret S01E03", "candidate", "eligible"),
            ({"names": ["AlphaSecret"]}, "AlphaSecret S01E03", "candidate", "eligible"),
        )
        for index, (fields, title, expected, reason) in enumerate(cases):
            with self.subTest(fields=fields, title=title):
                adapter = FakeMoviePilot(subscriptions=[{**SUB_A, **fields}])
                provider = FakeSourceProvider(results=[{"title": title, "infohash": "f" * 40}])
                baseline = DiscoveryService(None, adapter, [provider]).prepare(WINDOW, True)
                secrets = ("AlphaSecret", "BetaSecret", "S01", "E03")
                dry = DiscoveryService(None, adapter, [provider], secrets=secrets).prepare(WINDOW, True)
                path = os.path.join(self.tmp.name, f"collision-{index}.db")
                self.assertFalse(os.path.exists(path))
                store = StateStore(path)
                try:
                    run = DiscoveryService(store, adapter, [provider], secrets=secrets).prepare(WINDOW, False)
                    for plan in (baseline, dry, run):
                        self.assertFalse(plan.errors)
                        decision = plan.decisions[0]
                        self.assertEqual((decision.decision, decision.reason), (expected, reason))
                        self.assertEqual((decision.season, decision.episode), (1, 3))
                    self.assertEqual(dry.to_mapping(), run.to_mapping())
                    persisted = "\n".join(store.conn.iterdump())
                    for secret in secrets:
                        self.assertNotIn(secret, json.dumps(dry.to_mapping()))
                        self.assertNotIn(secret, persisted)
                finally:
                    store.close()

    def test_provider_receives_original_subscription_before_output_redaction(self):
        from pipeline.subscriptions import normalize_subscription

        raw = {
            **SUB_A, "id": "IdSecret", "media_source": "sourcesecret",
            "media_id": "MediaSecret", "name": "WorkSecret",
            "names": ["AliasSecret"], "include": "IncludeSecret", "exclude": "ExcludeSecret",
        }
        expected = normalize_subscription(raw)
        secrets = ("IdSecret", "sourcesecret", "MediaSecret", "WorkSecret",
                   "AliasSecret", "IncludeSecret", "ExcludeSecret")

        def provider(subscription, window):
            # Simulate a provider selecting by all normalized subscription fields.
            if subscription != expected:
                return []
            return [{"title": "AliasSecret IncludeSecret S01E02", "infohash": "e" * 40}]

        adapter = FakeMoviePilot(subscriptions=[raw])
        baseline = DiscoveryService(None, adapter, [provider]).prepare(WINDOW, True)
        dry = DiscoveryService(None, adapter, [provider], secrets=secrets).prepare(WINDOW, True)
        store = StateStore(self.db_path)
        try:
            run = DiscoveryService(store, adapter, [provider], secrets=secrets).prepare(WINDOW, False)
            for plan in (baseline, dry, run):
                self.assertEqual(len(plan.decisions), 1)
                self.assertEqual(plan.decisions[0].reason, "eligible")
            self.assertEqual(dry.to_mapping(), run.to_mapping())
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 1)
            for secret in secrets:
                self.assertNotIn(secret, json.dumps(dry.to_mapping()))
                self.assertNotIn(secret, "\n".join(store.conn.iterdump()))
        finally:
            store.close()

    def test_conflicting_content_types_never_produce_or_persist_candidates(self):
        adapter = FakeMoviePilot(subscriptions=[
            {**SUB_A, "type": "tv", "media_type": "movie"},
        ])
        provider = FakeSourceProvider(results=RESULTS)
        dry = DiscoveryService(
            store=None, moviepilot=adapter, providers=[provider]
        ).prepare(WINDOW, True)
        self.assertFalse(os.path.exists(self.db_path))
        store = StateStore(self.db_path)
        try:
            run = DiscoveryService(
                store=store, moviepilot=adapter, providers=[provider]
            ).prepare(WINDOW, False)
            for plan in (dry, run):
                self.assertEqual(plan.summary["excluded_count"], 1)
                self.assertEqual(plan.exclusions[0].reason, "non_tv")
                self.assertFalse(plan.decisions)
            self.assertEqual(dry.decision_projection(), run.decision_projection())
            for table in ("subscriptions", "episodes_v5", "candidates"):
                self.assertEqual(store.conn.execute(
                    f"SELECT COUNT(*) FROM {table}"
                ).fetchone()[0], 0)
        finally:
            store.close()

    def test_secret_overlapping_episode_token_cannot_hide_invalid_range(self):
        for secret in ("E03", "E05", "S01"):
            with self.subTest(secret=secret):
                adapter = FakeMoviePilot(subscriptions=[SUB_A])
                provider = FakeSourceProvider(results={"sub-a": [{
                    "title": "Show A S01E05-E03 1080p",
                    "infohash": "h" * 40,
                }]})
                dry = DiscoveryService(
                    store=None, moviepilot=adapter, providers=[provider], secrets=(secret,)
                ).prepare(WINDOW, True)
                store = StateStore(self.db_path)
                try:
                    run = DiscoveryService(
                        store=store, moviepilot=adapter, providers=[provider], secrets=(secret,)
                    ).prepare(WINDOW, False)
                    for plan in (dry, run):
                        decision = plan.decisions[0]
                        self.assertEqual(decision.decision, "needs_review")
                        self.assertEqual(decision.reason, "episode_identity_invalid")
                        self.assertIsNone(decision.episode_key)
                        self.assertIsNone(decision.episode_end)
                        self.assertNotIn(secret, json.dumps(plan.to_mapping()))
                    self.assertEqual(dry.decision_projection(), run.decision_projection())
                    for table in ("episodes_v5", "candidates"):
                        self.assertEqual(store.conn.execute(
                            f"SELECT COUNT(*) FROM {table}"
                        ).fetchone()[0], 0)
                finally:
                    store.close()

    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.db_path = os.path.join(self.tmp.name, "state.db")

    def tearDown(self):
        self.tmp.cleanup()

    def test_dry_run_and_run_share_subscription_and_candidate_decisions(self):
        adapter = FakeMoviePilot(subscriptions=[SUB_A, SUB_B])
        provider = FakeSourceProvider(results=RESULTS)

        dry = DiscoveryService(
            store=None, moviepilot=adapter, providers=[provider]
        ).prepare(WINDOW, True)
        self.assertFalse(os.path.exists(self.db_path))

        store = StateStore(self.db_path)
        run = DiscoveryService(
            store=store, moviepilot=adapter, providers=[provider]
        ).prepare(WINDOW, False)

        self.assertEqual(dry.decisions, run.decisions)
        self.assertEqual(dry.decision_projection(), run.decision_projection())
        self.assertEqual([row[0] for row in store.conn.execute(
            "SELECT subscription_id FROM subscriptions ORDER BY subscription_id"
        ).fetchall()], ["sub-a", "sub-b"])
        self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 2)
        self.assertEqual(provider.calls, 4)
        store.close()
        self.assertTrue(os.path.exists(self.db_path))

    def test_disabled_and_non_tv_exclusions_have_explicit_reasons(self):
        adapter = FakeMoviePilot(subscriptions=[
            {**SUB_A, "enabled": False},
            {**SUB_B, "type": "电影"},
        ])
        plan = DiscoveryService(
            store=None, moviepilot=adapter,
            providers=[FakeSourceProvider(results=RESULTS)],
        ).prepare(WINDOW, True)
        reasons = {item.subscription_id: item.reason for item in plan.exclusions}
        self.assertEqual(reasons["sub-a"], "disabled")
        self.assertEqual(reasons["sub-b"], "non_tv")
        self.assertEqual(plan.summary["excluded_count"], 2)

    def test_provider_failure_is_scoped_to_one_subscription(self):
        adapter = FakeMoviePilot(subscriptions=[SUB_A, SUB_B])
        provider = FakeSourceProvider(results=RESULTS, failures={"sub-a": RuntimeError("Bearer top-secret")})
        plan = DiscoveryService(
            store=None, moviepilot=adapter, providers=[provider], secrets=("top-secret",)
        ).prepare(WINDOW, True)
        self.assertEqual([decision.subscription_id for decision in plan.decisions], ["sub-b"])
        self.assertEqual(len(plan.errors), 1)
        self.assertEqual(plan.errors[0].subscription_id, "sub-a")
        self.assertNotIn("top-secret", json.dumps(plan.to_mapping(), ensure_ascii=False))

    def test_provider_name_is_redacted_in_errors_and_projection(self):
        secret = "provider-secret"
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(
            results=RESULTS,
            failures={"sub-a": RuntimeError("provider failed")},
            name=secret,
        )
        plan = DiscoveryService(
            store=None,
            moviepilot=adapter,
            providers=[provider],
            secrets=(secret,),
        ).prepare(WINDOW, True)
        self.assertEqual(len(plan.errors), 1)
        self.assertNotIn(secret, plan.errors[0].provider)
        self.assertNotIn(secret, json.dumps(plan.to_mapping(), ensure_ascii=False))

    def test_run_persists_no_download_submission(self):
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(results=RESULTS)
        store = StateStore(self.db_path)
        plan = DiscoveryService(
            store=store, moviepilot=adapter, providers=[provider]
        ).prepare(WINDOW, False)
        self.assertEqual(plan.summary["decision_count"], 1)
        self.assertFalse(hasattr(adapter, "submit_calls"))
        self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM downloads").fetchone()[0], 0)
        store.close()

    def test_configured_secret_is_redacted_from_persisted_snapshot(self):
        subscription = {
            **SUB_A,
            "token": "snapshot-secret",
        }
        adapter = FakeMoviePilot(subscriptions=[subscription])
        provider = FakeSourceProvider(results=RESULTS)
        store = StateStore(self.db_path)
        try:
            DiscoveryService(
                store=store,
                moviepilot=adapter,
                providers=[provider],
                secrets=("snapshot-secret",),
            ).prepare(WINDOW, False)
            snapshot = store.conn.execute(
                "SELECT snapshot_json FROM subscriptions"
            ).fetchone()[0]
            self.assertNotIn("snapshot-secret", snapshot)
        finally:
            store.close()

    def test_source_identity_conflict_is_visible_and_not_persisted(self):
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(results={
            "sub-a": [{
                "title": "Show A S02E01 1080p",
                "season": 1,
                "episode": 1,
                "infohash": "c" * 40,
            }],
        })
        store = StateStore(self.db_path)
        try:
            plan = DiscoveryService(
                store=store, moviepilot=adapter, providers=[provider]
            ).prepare(WINDOW, False)
            self.assertEqual(len(plan.decisions), 1)
            self.assertEqual(plan.decisions[0].decision, "needs_review")
            self.assertEqual(plan.decisions[0].reason, "episode_identity_conflict")
            self.assertEqual(store.conn.execute(
                "SELECT COUNT(*) FROM episodes_v5"
            ).fetchone()[0], 0)
        finally:
            store.close()

    def test_title_season_mismatch_is_visible_and_not_persisted(self):
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(results={
            "sub-a": [{
                "title": "Show A S02E01 1080p",
                "infohash": "d" * 40,
            }],
        })
        store = StateStore(self.db_path)
        try:
            plan = DiscoveryService(
                store=store, moviepilot=adapter, providers=[provider]
            ).prepare(WINDOW, False)
            self.assertEqual(plan.decisions[0].reason, "season_mismatch")
            self.assertEqual(store.conn.execute(
                "SELECT COUNT(*) FROM episodes_v5"
            ).fetchone()[0], 0)
        finally:
            store.close()

    def test_source_secrets_are_absent_from_projection_and_records(self):
        secret = "source-secret"
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(results={
            "sub-a": [{
                "title": f"Show A S01E01 {secret}",
                "link": f"https://source.invalid/?token={secret}",
                "torrent_url": f"https://source.invalid/{secret}.torrent",
                "provider": secret,
                "infohash": "e" * 40,
            }],
        })
        store = StateStore(self.db_path)
        try:
            service = DiscoveryService(
                store=store, moviepilot=adapter, providers=[provider],
                secrets=(secret,),
            )
            dry = service.prepare(WINDOW, True)
            self.assertNotIn(secret, dry.decision_projection().decode("utf-8"))
            service.prepare(WINDOW, False)
            records = store.conn.execute(
                "SELECT e.title, c.title, c.source_url, c.provider "
                "FROM episodes_v5 e JOIN candidates c ON c.episode_id=e.id"
            ).fetchone()
            self.assertNotIn(secret, json.dumps(tuple(records), ensure_ascii=False))
        finally:
            store.close()

    def test_subscription_secrets_are_absent_from_projection_and_persistence(self):
        secret = "subscription-secret"
        subscription = {
            **SUB_A,
            "names": [secret, "Show Alias"],
            "include": f"include-{secret}",
            "exclude": f"exclude-{secret}",
        }
        adapter = FakeMoviePilot(subscriptions=[subscription])
        provider = FakeSourceProvider(results=RESULTS)
        dry = DiscoveryService(
            store=None,
            moviepilot=adapter,
            providers=[provider],
            secrets=(secret,),
        ).prepare(WINDOW, True)
        self.assertNotIn(secret, json.dumps(dry.to_mapping(), ensure_ascii=False))

        store = StateStore(self.db_path)
        try:
            DiscoveryService(
                store=store,
                moviepilot=adapter,
                providers=[provider],
                secrets=(secret,),
            ).prepare(WINDOW, False)
            row = store.conn.execute(
                "SELECT work, aliases_json, include_text, exclude_text, snapshot_json "
                "FROM subscriptions"
            ).fetchone()
            self.assertIsNotNone(row)
            self.assertNotIn(secret, json.dumps(tuple(row), ensure_ascii=False))
        finally:
            store.close()

    def test_non_positive_episode_is_reviewed_without_persistence(self):
        results = [
            {
                "title": "Show A S01E00 1080p",
                "infohash": "f" * 40,
            },
            {
                "title": "Show A S01E01 1080p",
                "episode": 0,
                "infohash": "g" * 40,
            },
        ]
        for source_result in results:
            with self.subTest(source_result=source_result):
                adapter = FakeMoviePilot(subscriptions=[SUB_A])
                provider = FakeSourceProvider(results={"sub-a": [source_result]})
                dry = DiscoveryService(
                    store=None, moviepilot=adapter, providers=[provider]
                ).prepare(WINDOW, True)
                self.assertEqual(dry.decisions[0].decision, "needs_review")
                self.assertEqual(dry.decisions[0].reason, "episode_identity_invalid")
                self.assertIsNone(dry.decisions[0].episode)

                store = StateStore(self.db_path)
                try:
                    run = DiscoveryService(
                        store=store, moviepilot=adapter, providers=[provider]
                    ).prepare(WINDOW, False)
                    self.assertEqual(run.decisions[0].decision, "needs_review")
                    self.assertEqual(
                        store.conn.execute("SELECT COUNT(*) FROM episodes_v5").fetchone()[0],
                        0,
                    )
                finally:
                    store.close()

    def test_episode_end_before_episode_is_reviewed_without_persistence(self):
        source_result = {
            "title": "Show A S01E05-E03 1080p",
            "infohash": "h" * 40,
        }
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(results={"sub-a": [source_result]})
        dry = DiscoveryService(
            store=None, moviepilot=adapter, providers=[provider]
        ).prepare(WINDOW, True)
        self.assertEqual(dry.decisions[0].decision, "needs_review")
        self.assertEqual(dry.decisions[0].reason, "episode_identity_invalid")
        self.assertIsNone(dry.decisions[0].episode)
        self.assertIsNone(dry.decisions[0].episode_end)

        store = StateStore(self.db_path)
        try:
            run = DiscoveryService(
                store=store, moviepilot=adapter, providers=[provider]
            ).prepare(WINDOW, False)
            self.assertEqual(run.decisions[0].decision, "needs_review")
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM episodes_v5").fetchone()[0],
                0,
            )
            self.assertEqual(
                store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0],
                0,
            )
        finally:
            store.close()

    def test_active_invalid_subscription_is_reported_with_id_and_field(self):
        adapter = FakeMoviePilot(subscriptions=[
            {"id": "bad-sub", "name": "Show A", "season": 1},
            SUB_B,
        ])
        plan = DiscoveryService(
            store=None, moviepilot=adapter,
            providers=[FakeSourceProvider(results=RESULTS)],
        ).prepare(WINDOW, True)
        self.assertEqual(plan.errors[0].subscription_id, "bad-sub")
        self.assertIn("media_id", plan.errors[0].message)
        self.assertEqual([d.subscription_id for d in plan.decisions], ["sub-b"])

    def test_explicit_non_episode_markers_are_hard_excluded_without_persistence(self):
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(results={"sub-a": [
            {
                "title": "Show A S01E01 1080p",
                "infohash": "m" * 40,
                "media_type": "movie",
            },
            {
                "title": "Show A S01E02 1080p",
                "infohash": "n" * 40,
                "is_episode": False,
            },
            {
                "title": "Show A S01E03 1080p",
                "infohash": "o" * 40,
                "non_episode": True,
            },
        ]})
        dry = DiscoveryService(None, adapter, [provider]).prepare(WINDOW, True)

        self.assertEqual(len(dry.decisions), 3)
        self.assertTrue(all(item.decision == "excluded" for item in dry.decisions))
        self.assertTrue(all(item.reason == "non_episode_media" for item in dry.decisions))

        store = StateStore(self.db_path)
        try:
            run = DiscoveryService(store, adapter, [provider]).prepare(WINDOW, False)
            self.assertEqual(dry.decision_projection(), run.decision_projection())
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM episodes_v5").fetchone()[0], 0)
            self.assertEqual(store.conn.execute("SELECT COUNT(*) FROM candidates").fetchone()[0], 0)
        finally:
            store.close()

    def test_string_false_collection_marker_is_normalized_before_persistence(self):
        adapter = FakeMoviePilot(subscriptions=[SUB_A])
        provider = FakeSourceProvider(results={"sub-a": [{
            "title": "Show A S01E04 1080p",
            "infohash": "p" * 40,
            "collection": "false",
        }]})
        store = StateStore(self.db_path)
        try:
            plan = DiscoveryService(store, adapter, [provider]).prepare(WINDOW, False)
            self.assertFalse(plan.decisions[0].collection)
            traits = json.loads(store.conn.execute(
                "SELECT traits_json FROM candidates"
            ).fetchone()[0])
            self.assertFalse(traits["collection"])
        finally:
            store.close()


if __name__ == "__main__":
    unittest.main(verbosity=2)
