"""Transactional SQLite schema upgrades for the V5 pipeline.

The migration runner intentionally keeps the old tables in place.  They are
evidence of what the previous pipeline knew and are useful when an operator
has to resolve an identity by hand; the normalized V5 tables are the only
tables used by :class:`pipeline.state.StateStore` after an upgrade.
"""
from __future__ import annotations

from dataclasses import dataclass, field
import hashlib
import json
import os
import sqlite3
import time
from typing import Any, Iterable, Mapping

from .models import EpisodeStatus, ReviewReason
from .security import sanitize_text


LATEST_SCHEMA_VERSION = 5


@dataclass(frozen=True)
class MigrationReport:
    """Result of a migration run.

    ``version`` is the committed schema version.  The extra fields are kept
    deliberately small and useful to callers that want to display an audit
    summary without parsing the ``schema_migrations`` table themselves.
    """

    version: int
    applied_versions: tuple[int, ...] = ()
    detected_legacy: bool = False
    failed_version: int | None = None
    error: str | None = None

    @property
    def current_version(self) -> int:
        return self.version

    @property
    def success(self) -> bool:
        return self.error is None and self.version == LATEST_SCHEMA_VERSION


class MigrationRunner:
    """Apply explicit V1 -> V5 migrations, one transaction per version."""

    _MIGRATION_NAMES = {
        1: "legacy_bookkeeping",
        2: "normalized_tables",
        3: "legacy_normalization",
        4: "runtime_and_notifications",
        5: "final_constraints_and_views",
    }

    def __init__(
        self,
        connection_or_path: sqlite3.Connection | str | os.PathLike[str],
        *,
        fail_after_version: int | None = None,
    ) -> None:
        self._owns_connection = not isinstance(connection_or_path, sqlite3.Connection)
        if self._owns_connection:
            self.connection = sqlite3.connect(str(connection_or_path))
        else:
            self.connection = connection_or_path
        self.connection.execute("PRAGMA foreign_keys=ON")
        self.fail_after_version = fail_after_version

    def close(self) -> None:
        if self._owns_connection:
            self.connection.close()

    def migrate_expect_failure(self) -> MigrationReport:
        """Run a migration while returning injected failures as a report.

        This helper is useful for crash/interruption tests.  Normal callers
        should use :meth:`migrate`, which propagates failures after the failed
        transaction has been rolled back.
        """

        try:
            return self.migrate()
        except Exception as exc:  # pragma: no cover - exact exception is data dependent
            return MigrationReport(
                version=self._user_version(),
                applied_versions=self._completed_versions(),
                detected_legacy=self._has_legacy_schema(),
                failed_version=self._user_version() + 1,
                error=sanitize_text(exc),
            )

    def migrate(self) -> MigrationReport:
        connection = self.connection
        connection.execute("PRAGMA foreign_keys=ON")
        detected_legacy = self._has_legacy_schema()
        current = self._user_version()
        if current > LATEST_SCHEMA_VERSION:
            raise RuntimeError(
                f"database schema version {current} is newer than supported {LATEST_SCHEMA_VERSION}"
            )

        applied: list[int] = []
        for version in range(current + 1, LATEST_SCHEMA_VERSION + 1):
            self._apply_one(version)
            applied.append(version)

        # V5 was already released before the review CAS generation field was
        # added.  Keep the schema version stable and repair those databases
        # in place; new V2 tables receive the same column from the DDL below.
        self._ensure_review_generation_column()

        return MigrationReport(
            version=self._user_version(),
            applied_versions=tuple(applied),
            detected_legacy=detected_legacy,
        )

    def _apply_one(self, version: int) -> None:
        started_at = time.time()
        name = self._MIGRATION_NAMES[version]
        checksum = hashlib.sha256(f"{version}:{name}".encode("utf-8")).hexdigest()
        connection = self.connection
        try:
            connection.execute("BEGIN")
            if version == 1:
                self._v1_bookkeeping()
            elif version == 2:
                self._v2_normalized_tables()
            elif version == 3:
                self._v3_normalize_legacy_rows()
            elif version == 4:
                self._v4_runtime_and_notifications()
            elif version == 5:
                self._v5_constraints_and_views()
            if self.fail_after_version == version:
                # Fail while this version's transaction is open.  The caller
                # can then rerun from the prior committed user_version.
                raise RuntimeError(f"injected interruption during migration V{version}")
            connection.execute(
                """INSERT OR REPLACE INTO schema_migrations(
                    version, name, checksum, started_at, completed_at, outcome, error
                ) VALUES (?, ?, ?, ?, ?, 'completed', NULL)""",
                (version, name, checksum, started_at, time.time()),
            )
            connection.execute(f"PRAGMA user_version = {version}")
            connection.commit()
        except Exception:
            connection.rollback()
            raise

    def _v1_bookkeeping(self) -> None:
        connection = self.connection
        connection.execute(
            """CREATE TABLE IF NOT EXISTS schema_migrations(
                version INTEGER PRIMARY KEY,
                name TEXT NOT NULL,
                checksum TEXT NOT NULL,
                started_at REAL NOT NULL,
                completed_at REAL,
                outcome TEXT NOT NULL,
                error TEXT
            )"""
        )
        # Copy, never delete, every legacy table that exists.  CTAS is used on
        # purpose: old installations had small column differences and the
        # preservation table must retain all columns verbatim.
        for table in ("episodes", "stage_history", "outbox", "baseline"):
            if self._table_exists(table) and not self._table_exists(f"{table}_legacy"):
                connection.execute(
                    f"CREATE TABLE {table}_legacy AS SELECT * FROM {table}"
                )

    def _v2_normalized_tables(self) -> None:
        connection = self.connection
        # The old names are retained as *_legacy_original as well as copied to
        # *_legacy.  Renaming is metadata-only and avoids destroying evidence,
        # while making the canonical V5 table names available.
        for table in ("stage_history", "outbox", "baseline"):
            original = f"{table}_legacy_original"
            if self._table_exists(table) and not self._table_exists(original):
                connection.execute(f"ALTER TABLE {table} RENAME TO {original}")

        # ``executescript`` implicitly commits any open transaction before it
        # runs.  Split this static, semicolon-delimited DDL and execute each
        # internal statement on the transaction opened by ``_apply_one``.
        schema = """
            CREATE TABLE IF NOT EXISTS subscriptions (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                subscription_id TEXT NOT NULL,
                media_source TEXT NOT NULL,
                media_id TEXT NOT NULL,
                season INTEGER NOT NULL DEFAULT 0,
                work TEXT NOT NULL,
                aliases_json TEXT NOT NULL DEFAULT '[]',
                include_text TEXT NOT NULL DEFAULT '',
                exclude_text TEXT NOT NULL DEFAULT '',
                snapshot_json TEXT NOT NULL DEFAULT '{}',
                last_seen_at REAL NOT NULL,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(media_source, media_id, season, subscription_id)
            );

            CREATE TABLE IF NOT EXISTS episodes_v5 (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                legacy_id INTEGER UNIQUE,
                subscription_id TEXT,
                media_source TEXT NOT NULL,
                media_id TEXT NOT NULL,
                canonical_work TEXT NOT NULL DEFAULT '',
                aliases_json TEXT NOT NULL DEFAULT '[]',
                season INTEGER NOT NULL DEFAULT 0,
                episode INTEGER NOT NULL DEFAULT 0,
                status TEXT NOT NULL DEFAULT 'discovered',
                library_id TEXT,
                library_path TEXT,
                cloud_path TEXT,
                strm_path TEXT,
                runtime_identity_json TEXT NOT NULL DEFAULT '{}',
                retry_count INTEGER NOT NULL DEFAULT 0,
                next_retry_at REAL,
                last_error TEXT NOT NULL DEFAULT '',
                review_reason TEXT,
                review_detail TEXT NOT NULL DEFAULT '{}',
                review_created_at REAL,
                review_resolved_at REAL,
                review_resolution TEXT,
                resume_status TEXT,
                review_generation INTEGER NOT NULL DEFAULT 0,
                title TEXT NOT NULL DEFAULT '',
                source_id TEXT NOT NULL DEFAULT '',
                source_url TEXT NOT NULL DEFAULT '',
                episode_end INTEGER,
                release_group TEXT,
                quality TEXT,
                remote_task_id TEXT,
                file_id TEXT,
                submit_count INTEGER NOT NULL DEFAULT 0,
                notify_id TEXT,
                legacy_evidence_json TEXT NOT NULL DEFAULT '{}',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL,
                UNIQUE(media_source, media_id, season, episode)
            );

            CREATE TABLE IF NOT EXISTS candidates (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                episode_id INTEGER NOT NULL REFERENCES episodes_v5(id),
                provider TEXT NOT NULL DEFAULT '',
                source_id TEXT NOT NULL DEFAULT '',
                source_url TEXT NOT NULL DEFAULT '',
                title TEXT NOT NULL DEFAULT '',
                infohash TEXT NOT NULL,
                score REAL,
                selected INTEGER NOT NULL DEFAULT 0 CHECK(selected IN (0, 1)),
                decision_reason TEXT,
                traits_json TEXT NOT NULL DEFAULT '{}',
                discovered_at REAL NOT NULL,
                selected_at REAL,
                UNIQUE(episode_id, infohash)
            );

            CREATE TABLE IF NOT EXISTS downloads (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                infohash TEXT NOT NULL UNIQUE,
                remote_task_id TEXT,
                status TEXT NOT NULL DEFAULT 'pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                submitted_at REAL,
                completed_at REAL,
                last_error TEXT NOT NULL DEFAULT '',
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS download_files (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                download_id INTEGER NOT NULL REFERENCES downloads(id),
                remote_file_id TEXT,
                parent_id TEXT,
                parent_path TEXT,
                path TEXT,
                name TEXT NOT NULL DEFAULT '',
                size INTEGER,
                duration REAL,
                fingerprint TEXT,
                inventoried_at REAL NOT NULL,
                UNIQUE(download_id, remote_file_id, path)
            );

            CREATE TABLE IF NOT EXISTS episode_sources (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                episode_id INTEGER NOT NULL REFERENCES episodes_v5(id),
                download_file_id INTEGER NOT NULL REFERENCES download_files(id),
                selected_primary INTEGER NOT NULL DEFAULT 0 CHECK(selected_primary IN (0, 1)),
                evidence TEXT NOT NULL DEFAULT '{}',
                reason TEXT,
                created_at REAL NOT NULL,
                UNIQUE(episode_id, download_file_id)
            );

            CREATE TABLE IF NOT EXISTS stage_history (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                episode_id INTEGER NOT NULL REFERENCES episodes_v5(id),
                from_status TEXT,
                to_status TEXT NOT NULL,
                stage TEXT NOT NULL,
                detail TEXT NOT NULL DEFAULT '{}',
                detail_json TEXT NOT NULL DEFAULT '{}',
                legacy_history_id INTEGER UNIQUE,
                created_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS notifications (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                episode_id INTEGER NOT NULL REFERENCES episodes_v5(id),
                dedupe_key TEXT NOT NULL UNIQUE,
                message TEXT NOT NULL,
                delivery_state TEXT NOT NULL DEFAULT 'delivery_pending',
                state TEXT NOT NULL DEFAULT 'delivery_pending',
                attempts INTEGER NOT NULL DEFAULT 0,
                provider_id TEXT,
                notify_id TEXT,
                legacy_outbox_id INTEGER UNIQUE,
                created_at REAL NOT NULL,
                updated_at REAL NOT NULL
            );

            CREATE TABLE IF NOT EXISTS baseline (
                work TEXT NOT NULL,
                season INTEGER NOT NULL,
                episode INTEGER NOT NULL,
                infohash TEXT NOT NULL,
                recorded_at REAL NOT NULL,
                PRIMARY KEY(work, season, episode)
            );
            """
        for statement in schema.split(";"):
            statement = statement.strip()
            if statement:
                connection.execute(statement)

    def _v3_normalize_legacy_rows(self) -> None:
        connection = self.connection
        legacy_episodes = self._rows("episodes_legacy")
        for legacy in legacy_episodes:
            self._import_legacy_episode(legacy)
        self._import_legacy_history()
        self._import_legacy_outbox()
        self._import_legacy_baseline()

    def _v4_runtime_and_notifications(self) -> None:
        # V2 creates the complete nullable/future-proof column set.  This
        # version is kept explicit so an interrupted V3 can resume at a stable
        # boundary and so schema_migrations records the intended contract.
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_episode_status ON episodes_v5(status)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_episode_media ON episodes_v5(media_source, media_id)"
        )
        self.connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_notification_state ON notifications(delivery_state)"
        )

    def _ensure_review_generation_column(self) -> None:
        if not self._table_exists("episodes_v5"):
            return
        columns = {
            row[1]
            for row in self.connection.execute("PRAGMA table_info(episodes_v5)")
        }
        if "review_generation" in columns:
            return
        try:
            self.connection.execute("BEGIN")
            self.connection.execute(
                "ALTER TABLE episodes_v5 ADD COLUMN review_generation INTEGER NOT NULL DEFAULT 0"
            )
            self.connection.commit()
        except Exception:
            self.connection.rollback()
            raise

    def _v5_constraints_and_views(self) -> None:
        connection = self.connection
        # Legacy conflicts may have accumulated more than one selected
        # candidate before the V5 partial index existed.  Keep every candidate
        # as evidence, retain the first deterministic choice, and force the
        # Episode into review instead of making index creation fail or silently
        # merging the conflict.
        duplicate_selected = connection.execute(
            """SELECT episode_id FROM candidates WHERE selected=1
               GROUP BY episode_id HAVING COUNT(*) > 1"""
        ).fetchall()
        for row in duplicate_selected:
            episode_id = int(row[0])
            previous_status = connection.execute(
                "SELECT status FROM episodes_v5 WHERE id=?", (episode_id,)
            ).fetchone()[0]
            selected = connection.execute(
                "SELECT id FROM candidates WHERE episode_id=? AND selected=1 ORDER BY id",
                (episode_id,),
            ).fetchall()
            for candidate in selected[1:]:
                connection.execute("UPDATE candidates SET selected=0, selected_at=NULL WHERE id=?", (candidate[0],))
            connection.execute(
                """UPDATE episodes_v5 SET status='needs_review',
                          review_reason=?, review_detail=?, review_created_at=?,
                          review_generation=review_generation + 1,
                          resume_status=COALESCE(resume_status, status), updated_at=?
                   WHERE id=?""",
                (
                    ReviewReason.DUPLICATE_EPISODE.value,
                    _safe_json({"reason": ReviewReason.DUPLICATE_EPISODE.value}),
                    time.time(), time.time(), episode_id,
                ),
            )
            self._insert_history(
                episode_id,
                previous_status,
                EpisodeStatus.NEEDS_REVIEW.value,
                {"reason": ReviewReason.DUPLICATE_EPISODE.value},
            )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_selected_candidate ON candidates(episode_id) WHERE selected = 1"
        )
        connection.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS uq_selected_episode_source ON episode_sources(episode_id) WHERE selected_primary = 1"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_candidate_infohash ON candidates(infohash)"
        )
        connection.execute(
            "CREATE INDEX IF NOT EXISTS idx_history_episode ON stage_history(episode_id, id)"
        )
        # A new database has no legacy table named episodes.  The view keeps
        # old read-only SQL integrations useful while V5 remains authoritative.
        if not self._table_exists("episodes") and not self._view_exists("episodes"):
            connection.execute(
                """CREATE VIEW episodes AS
                SELECT e.id,
                       COALESCE(c.infohash, '') AS infohash,
                       e.source_id, e.source_url, e.title, e.canonical_work AS work,
                       e.season, e.episode, e.episode_end, e.release_group, e.quality,
                       e.remote_task_id, e.file_id, e.strm_path,
                       e.submit_count, e.status AS stage, e.updated_at AS stage_updated_at,
                       e.notify_id
                  FROM episodes_v5 e
                  LEFT JOIN candidates c
                    ON c.id = (SELECT c2.id FROM candidates c2
                                WHERE c2.episode_id = e.id
                                ORDER BY c2.selected DESC, c2.id ASC LIMIT 1)"""
            )

    def _import_legacy_episode(self, legacy: Mapping[str, Any]) -> int:
        connection = self.connection
        legacy_id = _int_or_none(legacy.get("id"))
        if legacy_id is not None:
            existing = connection.execute(
                "SELECT id FROM episodes_v5 WHERE legacy_id = ?", (legacy_id,)
            ).fetchone()
            if existing:
                return int(existing[0])

        now = time.time()
        work = _text(legacy.get("work") or legacy.get("title") or "")
        infohash = _text(legacy.get("infohash") or "").lower()
        media_source = _text(
            legacy.get("media_source") or legacy.get("provider") or "legacy"
        )
        media_id_value = legacy.get("media_id") or legacy.get("tmdb_id")
        season = _int_or_none(legacy.get("season"))
        episode = _int_or_none(legacy.get("episode"))
        trustworthy_identity = bool(media_id_value) and season is not None and episode is not None and episode > 0
        if season is None or season < 0:
            season = 0
        if episode is None or episode < 0:
            episode = 0
        if not media_id_value:
            # Keep every unresolved legacy row separate and deterministic.
            stable = legacy_id if legacy_id is not None else hashlib.sha1(work.encode("utf-8")).hexdigest()[:16]
            media_id = f"legacy:{stable}"
        else:
            media_id = _text(media_id_value)

        old_stage = _text(legacy.get("stage") or "discovered")
        status = _legacy_status(old_stage)
        review_reason: str | None = None
        review_detail: dict[str, Any] = {}
        if not trustworthy_identity or not infohash:
            status = EpisodeStatus.NEEDS_REVIEW.value
            review_reason = ReviewReason.LEGACY_IDENTITY_UNRESOLVED.value
        elif old_stage == "scraping":
            status = EpisodeStatus.SCRAPE_PENDING.value
        elif old_stage in {"library_ready", "notified", "metadata_ready", "strm_ready"}:
            missing = _legacy_terminal_missing(legacy)
            if missing:
                # A legacy terminal label is trusted only when a complete V5
                # download/file/source and current runtime evidence can be
                # rebuilt.  Partial evidence is retained but is review-only.
                status = EpisodeStatus.NEEDS_REVIEW.value
                review_reason = ReviewReason.UNEXPECTED_REMOTE_STATE.value
                review_detail = {
                    "reason": "LEGACY_TERMINAL_EVIDENCE_INCOMPLETE",
                    "legacy_stage": old_stage,
                    "missing": missing,
                }
        terminal_reconstructable = (
            old_stage in {"library_ready", "notified", "metadata_ready", "strm_ready"}
            and review_reason is None
        )

        evidence = _safe_json(dict(legacy))
        aliases_json = _safe_json(legacy.get("aliases") or [])
        source_id = _text(legacy.get("source_id") or "")
        source_url = _text(legacy.get("source_url") or source_id)
        params = (
            legacy_id,
            None,
            media_source,
            media_id,
            work,
            aliases_json,
            season,
            episode,
            status,
            review_reason,
            _safe_json(review_detail or {"reason": review_reason}) if review_reason else "{}",
            now if review_reason else None,
            status if review_reason else None,
            _text_or_none(legacy.get("library_id")),
            _text_or_none(legacy.get("library_path")),
            _text_or_none(legacy.get("cloud_path")),
            _text_or_none(legacy.get("strm_path")),
            _text(legacy.get("title") or work),
            source_id,
            source_url,
            _int_or_none(legacy.get("episode_end")),
            _text_or_none(legacy.get("release_group")),
            _text_or_none(legacy.get("quality")),
            _text_or_none(legacy.get("remote_task_id")),
            _text_or_none(legacy.get("file_id")),
            _int_or_zero(legacy.get("submit_count")),
            _text_or_none(legacy.get("notify_id")),
            evidence,
            now,
            now,
            1 if review_reason else 0,
        )
        history_recorded = False
        try:
            cur = connection.execute(
                """INSERT INTO episodes_v5(
                    legacy_id, subscription_id, media_source, media_id, canonical_work,
                    aliases_json, season, episode, status, review_reason, review_detail,
                    review_created_at, resume_status, library_id, library_path,
                    cloud_path, strm_path, title, source_id, source_url,
                    episode_end, release_group, quality, remote_task_id, file_id,
                    submit_count, notify_id, legacy_evidence_json, created_at, updated_at,
                    review_generation
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                params,
            )
            episode_id = int(cur.lastrowid)
        except sqlite3.IntegrityError:
            # Same business key from two legacy rows: retain the second row's
            # evidence on the first episode and force an explicit review.
            found = connection.execute(
                """SELECT id FROM episodes_v5
                   WHERE media_source=? AND media_id=? AND season=? AND episode=?""",
                (media_source, media_id, season, episode),
            ).fetchone()
            if not found:
                raise
            episode_id = int(found[0])
            previous_status = connection.execute(
                "SELECT status FROM episodes_v5 WHERE id=?", (episode_id,)
            ).fetchone()[0]
            connection.execute(
                """UPDATE episodes_v5
                   SET status='needs_review', review_reason=?,
                       review_detail=?, review_created_at=?, review_generation=review_generation + 1,
                       resume_status=?,
                       updated_at=? WHERE id=?""",
                (
                    ReviewReason.DUPLICATE_EPISODE.value,
                    _safe_json({"conflicting_legacy_row": evidence}),
                    now,
                    status,
                    now,
                    episode_id,
                ),
            )
            status = EpisodeStatus.NEEDS_REVIEW.value
            self._insert_history(
                episode_id,
                previous_status,
                status,
                {
                    "reason": ReviewReason.DUPLICATE_EPISODE.value,
                    "conflicting_legacy_row": evidence,
                },
            )
            history_recorded = True

        if infohash:
            self._insert_candidate_from_legacy(episode_id, legacy)
            if terminal_reconstructable:
                self._reconstruct_terminal_evidence(episode_id, legacy, infohash)

        # An import always gets a provenance history row.  The old history is
        # copied separately below, so operators can distinguish both facts.
        if not history_recorded and not connection.execute(
            "SELECT 1 FROM stage_history WHERE episode_id=? AND legacy_history_id IS NULL LIMIT 1",
            (episode_id,),
        ).fetchone():
            self._insert_history(
                episode_id,
                None,
                status,
                {"reason": "legacy_import", "legacy_id": legacy_id},
            )
        return episode_id

    def _reconstruct_terminal_evidence(
        self,
        episode_id: int,
        legacy: Mapping[str, Any],
        infohash: str,
    ) -> None:
        """Rebuild V5 download/file/source evidence for a complete legacy row."""
        now = time.time()
        self.connection.execute(
            """INSERT INTO downloads(
                infohash, remote_task_id, status, attempts, submitted_at,
                completed_at, last_error, created_at, updated_at
            ) VALUES (?, ?, 'completed', ?, ?, ?, '', ?, ?)
            ON CONFLICT(infohash) DO UPDATE SET
                remote_task_id=COALESCE(excluded.remote_task_id, downloads.remote_task_id),
                status='completed', completed_at=COALESCE(excluded.completed_at, downloads.completed_at),
                updated_at=excluded.updated_at""",
            (
                infohash,
                _text_or_none(legacy.get("remote_task_id")),
                _int_or_zero(legacy.get("submit_count")),
                legacy.get("stage_updated_at"),
                legacy.get("stage_updated_at", now),
                now,
                now,
            ),
        )
        download_id = int(self.connection.execute(
            "SELECT id FROM downloads WHERE infohash=?", (infohash,)
        ).fetchone()[0])
        path = _text_or_none(legacy.get("cloud_path") or legacy.get("strm_path"))
        self.connection.execute(
            """INSERT OR IGNORE INTO download_files(
                download_id, remote_file_id, parent_id, parent_path, path,
                name, size, duration, fingerprint, inventoried_at
            ) VALUES (?, ?, NULL, NULL, ?, ?, NULL, NULL, NULL, ?)""",
            (
                download_id,
                _text_or_none(legacy.get("file_id")),
                path,
                _text(legacy.get("title") or ""),
                legacy.get("stage_updated_at", now),
            ),
        )
        file_id = self.connection.execute(
            """SELECT id FROM download_files
               WHERE download_id=? AND remote_file_id=? AND path=?""",
            (download_id, _text_or_none(legacy.get("file_id")), path),
        ).fetchone()[0]
        selected_primary = 0 if self.connection.execute(
            """SELECT 1 FROM episode_sources
               WHERE episode_id=? AND selected_primary=1 LIMIT 1""",
            (episode_id,),
        ).fetchone() else 1
        self.connection.execute(
            """INSERT OR IGNORE INTO episode_sources(
                episode_id, download_file_id, selected_primary, evidence, reason, created_at
            ) VALUES (?, ?, ?, ?, 'legacy_reconstructed', ?)""",
            (
                episode_id,
                file_id,
                selected_primary,
                _safe_json({
                    "remote_task_id": legacy.get("remote_task_id"),
                    "file_id": legacy.get("file_id"),
                    "strm_path": legacy.get("strm_path"),
                    "library_path": legacy.get("library_path"),
                    "metadata_evidence": legacy.get("metadata_evidence"),
                }),
                now,
            ),
        )

    def _insert_candidate_from_legacy(self, episode_id: int, legacy: Mapping[str, Any]) -> None:
        infohash = _text(legacy.get("infohash") or "").lower()
        if not infohash:
            return
        has_selected = self.connection.execute(
            "SELECT 1 FROM candidates WHERE episode_id=? AND selected=1 LIMIT 1",
            (episode_id,),
        ).fetchone()
        self.connection.execute(
            """INSERT OR IGNORE INTO candidates(
                episode_id, provider, source_id, source_url, title, infohash,
                score, selected, decision_reason, traits_json, discovered_at,
                selected_at
            ) VALUES (?, ?, ?, ?, ?, ?, NULL, ?, ?, ?, ?, ?)""",
            (
                episode_id,
                _text(legacy.get("provider") or "legacy"),
                _text(legacy.get("source_id") or ""),
                _text(legacy.get("source_url") or ""),
                _text(legacy.get("title") or ""),
                infohash,
                0 if has_selected else 1,
                "legacy_import",
                _safe_json({"legacy": dict(legacy)}),
                time.time(),
                time.time(),
            ),
        )

    def _import_legacy_history(self) -> None:
        for legacy in self._rows("stage_history_legacy"):
            legacy_id = _int_or_none(legacy.get("id"))
            if legacy_id is None or self.connection.execute(
                "SELECT 1 FROM stage_history WHERE legacy_history_id=?", (legacy_id,)
            ).fetchone():
                continue
            episode_id = self._mapped_episode_id(legacy.get("episode_id"))
            if episode_id is None:
                continue
            stage = _legacy_status(_text(legacy.get("stage") or "discovered"))
            detail = _parse_json(legacy.get("detail"))
            detail["legacy_history_id"] = legacy_id
            self._insert_history(
                episode_id,
                None,
                stage,
                detail,
                legacy_history_id=legacy_id,
                created_at=_float_or_now(legacy.get("created_at")),
            )

    def _import_legacy_outbox(self) -> None:
        for legacy in self._rows("outbox_legacy"):
            legacy_id = _int_or_none(legacy.get("id"))
            if legacy_id is None or self.connection.execute(
                "SELECT 1 FROM notifications WHERE legacy_outbox_id=?", (legacy_id,)
            ).fetchone():
                continue
            episode_id = self._mapped_episode_id(legacy.get("episode_id"))
            if episode_id is None:
                continue
            state = _text(legacy.get("state") or "delivery_pending")
            dedupe_key = f"legacy:outbox:{legacy_id}"
            self.connection.execute(
                """INSERT OR IGNORE INTO notifications(
                    episode_id, dedupe_key, message, delivery_state, state, attempts,
                    notify_id, legacy_outbox_id, created_at, updated_at
                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                (
                    episode_id,
                    dedupe_key,
                    sanitize_text(legacy.get("message") or ""),
                    state,
                    state,
                    _int_or_zero(legacy.get("attempts")),
                    _text_or_none(legacy.get("notify_id")),
                    legacy_id,
                    _float_or_now(legacy.get("created_at")),
                    _float_or_now(legacy.get("updated_at")),
                ),
            )

    def _import_legacy_baseline(self) -> None:
        for legacy in self._rows("baseline_legacy"):
            try:
                self.connection.execute(
                    """INSERT OR IGNORE INTO baseline(work, season, episode, infohash, recorded_at)
                       VALUES (?, ?, ?, ?, ?)""",
                    (
                        _text(legacy.get("work") or ""),
                        _int_or_zero(legacy.get("season")),
                        _int_or_zero(legacy.get("episode")),
                        _text(legacy.get("infohash") or ""),
                        _float_or_now(legacy.get("recorded_at")),
                    ),
                )
            except sqlite3.IntegrityError:
                # A duplicate baseline is already represented; preserving the
                # source row in baseline_legacy is sufficient evidence.
                continue

    def _insert_history(
        self,
        episode_id: int,
        from_status: str | None,
        to_status: str,
        detail: Mapping[str, Any] | str | None,
        *,
        legacy_history_id: int | None = None,
        created_at: float | None = None,
    ) -> None:
        text = _safe_json(detail or {})
        self.connection.execute(
            """INSERT OR IGNORE INTO stage_history(
                episode_id, from_status, to_status, stage, detail, detail_json,
                legacy_history_id, created_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                episode_id,
                from_status,
                to_status,
                to_status,
                text,
                text,
                legacy_history_id,
                created_at if created_at is not None else time.time(),
            ),
        )

    def _mapped_episode_id(self, legacy_id: Any) -> int | None:
        value = _int_or_none(legacy_id)
        if value is None:
            return None
        row = self.connection.execute(
            "SELECT id FROM episodes_v5 WHERE legacy_id=?", (value,)
        ).fetchone()
        return int(row[0]) if row else None

    def _rows(self, table: str) -> list[dict[str, Any]]:
        if not self._table_exists(table):
            return []
        cursor = self.connection.execute(f"SELECT * FROM {table}")
        names = [item[0] for item in cursor.description or ()]
        return [dict(zip(names, row)) for row in cursor.fetchall()]

    def _completed_versions(self) -> tuple[int, ...]:
        if not self._table_exists("schema_migrations"):
            return ()
        rows = self.connection.execute(
            "SELECT version FROM schema_migrations WHERE outcome='completed' ORDER BY version"
        ).fetchall()
        return tuple(int(row[0]) for row in rows)

    def _user_version(self) -> int:
        return int(self.connection.execute("PRAGMA user_version").fetchone()[0])

    def _has_legacy_schema(self) -> bool:
        return self._table_exists("episodes") and not self._table_exists("episodes_v5")

    def _table_exists(self, name: str) -> bool:
        return bool(self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='table' AND name=?", (name,)
        ).fetchone())

    def _view_exists(self, name: str) -> bool:
        return bool(self.connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type='view' AND name=?", (name,)
        ).fetchone())


def _text(value: Any) -> str:
    return "" if value is None else sanitize_text(str(value))


def _text_or_none(value: Any) -> str | None:
    text = _text(value)
    return text or None


def _int_or_none(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        return int(value)
    except (TypeError, ValueError):
        return None


def _int_or_zero(value: Any) -> int:
    return _int_or_none(value) or 0


def _float_or_now(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return time.time()


def _parse_json(value: Any) -> dict[str, Any]:
    if isinstance(value, Mapping):
        return dict(value)
    try:
        parsed = json.loads(value or "{}")
    except (TypeError, ValueError):
        return {"legacy_detail": _text(value)}
    return dict(parsed) if isinstance(parsed, Mapping) else {"value": parsed}


def _safe_json(value: Any) -> str:
    if isinstance(value, str):
        text = value
    else:
        try:
            text = json.dumps(value, ensure_ascii=False, sort_keys=True, default=str)
        except (TypeError, ValueError):
            text = json.dumps(str(value), ensure_ascii=False)
    return sanitize_text(text)


def _legacy_status(stage: str) -> str:
    mapping = {
        "submitting": EpisodeStatus.DOWNLOAD_PENDING.value,
        "downloading": EpisodeStatus.DOWNLOAD_PENDING.value,
        "scraping": EpisodeStatus.SCRAPE_PENDING.value,
        "strm_ready": EpisodeStatus.STRM_READY.value,
        "strm_pending": EpisodeStatus.STRM_PENDING.value,
    }
    return mapping.get(stage, stage or EpisodeStatus.DISCOVERED.value)


def _legacy_terminal_missing(legacy: Mapping[str, Any]) -> list[str]:
    """Return fields needed to prove a terminal legacy row is V5-complete."""
    required = (
        "remote_task_id", "file_id", "strm_path", "library_id",
        "library_path", "metadata_evidence",
    )
    return [field for field in required if not legacy.get(field)]


__all__ = ["LATEST_SCHEMA_VERSION", "MigrationReport", "MigrationRunner"]
