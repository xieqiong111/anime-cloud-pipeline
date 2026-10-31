from __future__ import annotations

import json
import sqlite3
import unittest

from pipeline.migrations import LATEST_SCHEMA_VERSION, MigrationRunner


def legacy_database(**overrides: object) -> sqlite3.Connection:
    values = {
        "infohash": "a" * 40,
        "source_id": "https://source.invalid/item",
        "source_url": "https://source.invalid/item",
        "title": "Old Show S01E08",
        "work": "Old Show",
        "season": None,
        "episode": 8,
        "stage": "discovered",
        "media_id": None,
        "remote_task_id": None,
        "file_id": None,
        "strm_path": None,
        "cloud_path": None,
        "library_id": None,
        "library_path": None,
        "metadata_evidence": None,
    }
    values.update(overrides)
    db = sqlite3.connect(":memory:")
    db.executescript(
        """
        CREATE TABLE episodes (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            infohash TEXT NOT NULL UNIQUE,
            source_id TEXT NOT NULL,
            source_url TEXT NOT NULL,
            title TEXT NOT NULL,
            work TEXT,
            season INTEGER,
            episode INTEGER,
            episode_end INTEGER,
            release_group TEXT,
            quality TEXT,
            media_id TEXT,
            remote_task_id TEXT,
            file_id TEXT,
            strm_path TEXT,
            cloud_path TEXT,
            library_id TEXT,
            library_path TEXT,
            metadata_evidence TEXT,
            submit_count INTEGER NOT NULL DEFAULT 0,
            stage TEXT NOT NULL,
            stage_updated_at REAL NOT NULL,
            notify_id TEXT
        );
        CREATE TABLE stage_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            episode_id INTEGER NOT NULL,
            stage TEXT NOT NULL,
            detail TEXT NOT NULL DEFAULT '{}',
            created_at REAL NOT NULL
        );
        CREATE TABLE outbox (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            episode_id INTEGER NOT NULL,
            message TEXT NOT NULL,
            state TEXT NOT NULL DEFAULT 'delivery_pending',
            notify_id TEXT,
            attempts INTEGER NOT NULL DEFAULT 0,
            created_at REAL NOT NULL,
            updated_at REAL NOT NULL
        );
        CREATE TABLE baseline (
            work TEXT NOT NULL,
            season INTEGER NOT NULL,
            episode INTEGER NOT NULL,
            infohash TEXT NOT NULL,
            recorded_at REAL NOT NULL,
            PRIMARY KEY (work, season, episode)
        );
        """
    )
    db.execute(
        """INSERT INTO episodes(
            infohash, source_id, source_url, title, work, season, episode,
            media_id, remote_task_id, file_id, strm_path, cloud_path,
            library_id, library_path, metadata_evidence, stage, stage_updated_at
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 1.0)""",
        tuple(values[k] for k in (
            "infohash", "source_id", "source_url", "title", "work",
            "season", "episode", "media_id", "remote_task_id", "file_id",
            "strm_path", "cloud_path", "library_id", "library_path",
            "metadata_evidence", "stage",
        )),
    )
    db.execute(
        "INSERT INTO stage_history(episode_id, stage, detail, created_at) VALUES (1, ?, ?, 1.0)",
        (values["stage"], json.dumps({"legacy": "detail"})),
    )
    db.execute(
        "INSERT INTO outbox(episode_id, message, state, created_at, updated_at) VALUES (1, ?, 'delivery_pending', 1.0, 1.0)",
        ("legacy notification",),
    )
    db.commit()
    return db


class MigrationTests(unittest.TestCase):
    def test_v1_episode_is_preserved_and_ambiguous_identity_needs_review(self):
        db = legacy_database()

        report = MigrationRunner(db).migrate()

        self.assertEqual(report.version, LATEST_SCHEMA_VERSION)
        self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 5)
        episode = db.execute("SELECT * FROM episodes_v5").fetchone()
        columns = [item[1] for item in db.execute("PRAGMA table_info(episodes_v5)")]
        row = dict(zip(columns, episode))
        self.assertEqual(row["status"], "needs_review")
        self.assertEqual(row["review_reason"], "LEGACY_IDENTITY_UNRESOLVED")
        self.assertEqual(db.execute("SELECT COUNT(*) FROM episodes_legacy").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM stage_history_legacy").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications").fetchone()[0], 1)

    def test_interrupted_migration_is_safe_to_rerun(self):
        db = legacy_database()

        MigrationRunner(db, fail_after_version=3).migrate_expect_failure()
        MigrationRunner(db).migrate()

        self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 5)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM episodes_legacy").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM episodes_v5").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM stage_history").fetchone()[0], 2)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications").fetchone()[0], 1)

    def test_legacy_scraping_state_becomes_recoverable_scrape_pending(self):
        db = legacy_database(stage="scraping", media_id="111", season=1)

        MigrationRunner(db).migrate()

        self.assertEqual(
            db.execute("SELECT status FROM episodes_v5").fetchone()[0],
            "scrape_pending",
        )

    def test_empty_database_gets_sequential_schema_and_foreign_keys(self):
        db = sqlite3.connect(":memory:")

        MigrationRunner(db).migrate()

        self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 5)
        self.assertEqual(db.execute("PRAGMA foreign_keys").fetchone()[0], 1)
        versions = db.execute(
            "SELECT version FROM schema_migrations ORDER BY version"
        ).fetchall()
        self.assertEqual([row[0] for row in versions], [1, 2, 3, 4, 5])

    def test_v5_schema_has_transactional_review_generation(self):
        db = sqlite3.connect(":memory:")

        MigrationRunner(db).migrate()

        columns = {
            row[1]: row for row in db.execute("PRAGMA table_info(episodes_v5)")
        }
        self.assertIn("review_generation", columns)
        self.assertEqual(columns["review_generation"][4], "0")

    def test_existing_v5_without_generation_is_repaired_without_version_bump(self):
        db = sqlite3.connect(":memory:")
        MigrationRunner(db).migrate()
        db.execute("DROP VIEW episodes")
        db.execute("ALTER TABLE episodes_v5 DROP COLUMN review_generation")
        db.commit()

        report = MigrationRunner(db).migrate()

        self.assertEqual(report.version, LATEST_SCHEMA_VERSION)
        self.assertEqual(report.applied_versions, ())
        columns = [row[1] for row in db.execute("PRAGMA table_info(episodes_v5)")]
        self.assertIn("review_generation", columns)

    def test_migration_failure_does_not_advance_version(self):
        db = legacy_database()
        runner = MigrationRunner(db, fail_after_version=2)

        with self.assertRaises(RuntimeError):
            runner.migrate()

        self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM episodes_legacy").fetchone()[0], 1)

    def test_v2_failure_rolls_back_every_schema_change(self):
        db = legacy_database()

        MigrationRunner(db, fail_after_version=2).migrate_expect_failure()

        self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 1)
        names = {
            row[0]
            for row in db.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            )
        }
        self.assertIn("stage_history", names)
        self.assertIn("outbox", names)
        self.assertIn("baseline", names)
        self.assertNotIn("stage_history_legacy_original", names)
        self.assertNotIn("outbox_legacy_original", names)
        self.assertNotIn("episodes_v5", names)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM stage_history").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM outbox").fetchone()[0], 1)

        MigrationRunner(db).migrate()
        self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 5)

    def test_partial_legacy_library_ready_is_reviewed_without_redownload(self):
        db = legacy_database(
            stage="library_ready", media_id="111", season=1,
            remote_task_id="task-1",
        )

        MigrationRunner(db).migrate()

        row = db.execute(
            "SELECT status, review_reason, review_detail FROM episodes_v5"
        ).fetchone()
        self.assertEqual(row[0], "needs_review")
        self.assertEqual(row[1], "UNEXPECTED_REMOTE_STATE")
        self.assertIn("LEGACY_TERMINAL_EVIDENCE_INCOMPLETE", row[2])
        self.assertEqual(db.execute("SELECT COUNT(*) FROM downloads").fetchone()[0], 0)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM episode_sources").fetchone()[0], 0)

    def test_partial_legacy_notified_is_reviewed_without_redownload(self):
        db = legacy_database(
            stage="notified", media_id="111", season=1,
            file_id="file-1", strm_path="/old/show/S01E08.strm",
        )

        MigrationRunner(db).migrate()

        row = db.execute(
            "SELECT status, review_reason, strm_path FROM episodes_v5"
        ).fetchone()
        self.assertEqual(row[0], "needs_review")
        self.assertEqual(row[1], "UNEXPECTED_REMOTE_STATE")
        self.assertEqual(row[2], "/old/show/S01E08.strm")
        self.assertEqual(db.execute("SELECT COUNT(*) FROM downloads").fetchone()[0], 0)

    def test_duplicate_business_key_records_review_transition_history(self):
        db = legacy_database(media_id="111", season=1)
        db.execute(
            """INSERT INTO episodes(
                infohash, source_id, source_url, title, work, season, episode,
                media_id, stage, stage_updated_at
            ) VALUES ('b', 'source-2', 'url-2', 'Old Show S01E08 v2', 'Old Show',
                      1, 8, '111', 'discovered', 2.0)"""
        )
        db.commit()

        MigrationRunner(db).migrate()

        episode_id = db.execute("SELECT id FROM episodes_v5").fetchone()[0]
        history = db.execute(
            "SELECT from_status, to_status, detail_json FROM stage_history WHERE episode_id=? ORDER BY id",
            (episode_id,),
        ).fetchall()
        self.assertEqual(db.execute("SELECT status FROM episodes_v5").fetchone()[0], "needs_review")
        self.assertTrue(any(row[1] == "needs_review" for row in history))
        self.assertTrue(any("DUPLICATE_EPISODE" in row[2] for row in history))

    def test_duplicate_complete_terminal_rows_retain_all_evidence_and_reach_v5(self):
        db = legacy_database(
            media_id="111", season=1, stage="library_ready",
            remote_task_id="task-a", file_id="file-a",
            strm_path="/old/show/S01E08-a.strm",
            cloud_path="/cloud/show/S01E08-a.mkv",
            library_id="library-a", library_path="/library/show-a",
            metadata_evidence='{"provider":"legacy-a"}',
        )
        db.execute(
            """INSERT INTO episodes(
                infohash, source_id, source_url, title, work, season, episode,
                media_id, remote_task_id, file_id, strm_path, cloud_path,
                library_id, library_path, metadata_evidence, stage, stage_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                "b" * 40, "source-b", "url-b", "Old Show S01E08 B", "Old Show",
                1, 8, "111", "task-b", "file-b", "/old/show/S01E08-b.strm",
                "/cloud/show/S01E08-b.mkv", "library-b", "/library/show-b",
                '{"provider":"legacy-b"}', "notified", 2.0,
            ),
        )
        db.commit()

        MigrationRunner(db).migrate()

        self.assertEqual(db.execute("PRAGMA user_version").fetchone()[0], 5)
        episode_id = db.execute("SELECT id FROM episodes_v5").fetchone()[0]
        self.assertEqual(
            db.execute("SELECT status FROM episodes_v5 WHERE id=?", (episode_id,)).fetchone()[0],
            "needs_review",
        )
        self.assertEqual(
            db.execute("SELECT review_reason FROM episodes_v5 WHERE id=?", (episode_id,)).fetchone()[0],
            "DUPLICATE_EPISODE",
        )
        self.assertEqual(
            db.execute("SELECT COUNT(*) FROM candidates WHERE episode_id=?", (episode_id,)).fetchone()[0],
            2,
        )
        self.assertEqual(db.execute("SELECT COUNT(*) FROM downloads").fetchone()[0], 2)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM download_files").fetchone()[0], 2)
        self.assertEqual(
            db.execute("SELECT COUNT(*) FROM episode_sources WHERE episode_id=?", (episode_id,)).fetchone()[0],
            2,
        )
        self.assertEqual(
            db.execute("SELECT COUNT(*) FROM episode_sources WHERE episode_id=? AND selected_primary=1", (episode_id,)).fetchone()[0],
            1,
        )
        self.assertEqual(
            {
                row[0]
                for row in db.execute(
                    "SELECT remote_file_id FROM download_files ORDER BY remote_file_id"
                )
            },
            {"file-a", "file-b"},
        )
        self.assertTrue(
            any(
                "DUPLICATE_EPISODE" in row[0]
                for row in db.execute(
                    "SELECT detail_json FROM stage_history WHERE episode_id=?", (episode_id,)
                )
            )
        )

    def test_duplicate_selected_candidate_records_review_transition_history(self):
        db = legacy_database(media_id="111", season=1)
        MigrationRunner(db, fail_after_version=4).migrate_expect_failure()
        episode_id = db.execute("SELECT id FROM episodes_v5").fetchone()[0]
        db.execute(
            """INSERT INTO candidates(
                episode_id, provider, source_id, source_url, title, infohash,
                selected, decision_reason, traits_json, discovered_at, selected_at
            ) VALUES (?, 'legacy', '', '', 'conflict', 'b', 1,
                      'duplicate_conflict', '{}', 2.0, 2.0)""",
            (episode_id,),
        )
        db.commit()

        MigrationRunner(db).migrate()

        self.assertEqual(
            db.execute("SELECT status FROM episodes_v5 WHERE id=?", (episode_id,)).fetchone()[0],
            "needs_review",
        )
        history = db.execute(
            "SELECT to_status, detail_json FROM stage_history WHERE episode_id=?",
            (episode_id,),
        ).fetchall()
        self.assertTrue(any(row[0] == "needs_review" for row in history))
        self.assertTrue(any("DUPLICATE_EPISODE" in row[1] for row in history))

    def test_legacy_migration_is_idempotent_after_success(self):
        db = legacy_database()
        runner = MigrationRunner(db)

        runner.migrate()
        runner.migrate()

        self.assertEqual(db.execute("SELECT COUNT(*) FROM episodes_v5").fetchone()[0], 1)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM stage_history").fetchone()[0], 2)
        self.assertEqual(db.execute("SELECT COUNT(*) FROM notifications").fetchone()[0], 1)


if __name__ == "__main__":
    unittest.main()
