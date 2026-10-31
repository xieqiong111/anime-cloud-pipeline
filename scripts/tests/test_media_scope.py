"""Offline regressions for the bounded media verification invocation."""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import Mock

from pipeline.candidates import SubmissionBudget
from pipeline.executor import PipelineRunner
from pipeline.models import EpisodeKey
from pipeline.services import DownloadService, normalize_backfill_scope
from pipeline.state import StateStore


WINDOW = {"season_quarter": "2026-10", "backfill": {"media_id": "111", "season": 1, "start": 8, "end": 8}}


class Cloud:
    def __init__(self, task=None):
        self.find_task = Mock(return_value=task)
        self.submit = Mock(return_value="task-1")


class MediaScopeTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.store = StateStore(os.path.join(self.tmp.name, "state.db"))
        self.store.get_subscription_season_air_date = lambda _: "2026-10-03"

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def episode(self, *, source="tmdb", media_id="111", season=1, episode=8, status="strm_pending"):
        ident = self.store.get_or_create_episode({"key": EpisodeKey(source, media_id, season, episode), "status": status, "canonical_work": "Show", "subscription_id": "sub"})
        candidate = self.store.add_candidate(ident, {"infohash": "a" * 40, "title": "Show S01E08"})
        self.store.select_candidate(candidate)
        return ident

    def test_fresh_dispatch_only_selected_identity(self):
        self._dispatch_selected(True)

    def test_resume_dispatch_only_selected_identity(self):
        self._dispatch_selected(False)

    def _dispatch_selected(self, discover):
        target = self.episode()
        outsiders = [self.episode(episode=9), self.episode(source="legacy"), self.episode(source="other"), self.episode(media_id="222"), self.episode(season=2)]
        stage = Mock()
        stage.advance.return_value = {"status": "strm_pending"}
        discovery = Mock()
        discovery.prepare.return_value.summary = {}
        runner = PipelineRunner(self.store, services={"strm": stage}, discovery=discovery)
        runner.run(discover=discover, window=WINDOW)
        self.assertEqual([call.args[0]["id"] for call in stage.advance.call_args_list], [target])
        self.assertTrue(all(self.store.get_episode(i)["status"] == "strm_pending" for i in outsiders))

    def test_invalid_scope_fails_before_discovery(self):
        for scope in (None, {}, {"media_id": "111", "season": 1, "start": 9, "end": 8}, {"media_id": "111", "season": True, "start": 8, "end": 8}, {"media_source": "legacy", "media_id": "111", "season": 1, "start": 8, "end": 8}):
            with self.subTest(scope=scope):
                discovery = Mock()
                runner = PipelineRunner(self.store, discovery=discovery)
                with self.assertRaisesRegex(ValueError, "invalid_backfill_scope"):
                    runner.run(discover=True, window={"season_quarter": "2026-10", "backfill": scope})
                discovery.prepare.assert_not_called()

    def test_latest_row_identity_cannot_escape_scope(self):
        target = self.episode()
        original = self.store.get_episode
        self.store.get_episode = lambda i: {**original(i), "media_id": "222"}
        stage = Mock()
        runner = PipelineRunner(self.store, services={"strm": stage})
        # Compatibility reads must see the changed identity too.
        self.store.get_by_id = self.store.get_episode
        runner.run(window=WINDOW)
        stage.advance.assert_not_called()

    def test_no_scope_preserves_dispatch(self):
        self.episode(media_id="222")
        stage = Mock()
        stage.advance.return_value = {"status": "strm_pending"}
        PipelineRunner(self.store, services={"strm": stage}).run(window={"season_quarter": "2026-10"})
        stage.advance.assert_called_once()

    def test_same_task_target_enumeration_stays_in_scope_and_allows_alias(self):
        target = self.episode(status="submitted")
        alias = self.episode(source="themoviedb", status="submitted")
        self.episode(episode=9, status="submitted")
        self.episode(source="legacy", status="submitted")
        service = DownloadService(self.store, Cloud(), budget=SubmissionBudget(1, 1))
        service.set_scope(WINDOW)
        targets, _ = service._targets(self.store.get_episode(target), "a" * 40)
        self.assertEqual({ident for _, ident in targets}, {target, alias})

    def test_shared_task_poll_promotes_only_the_scoped_episode(self):
        target = self.episode(status="submitted")
        sibling = self.episode(episode=9, status="submitted")
        cloud = Cloud()
        cloud.list_files = Mock(return_value=[{
            "infohash": "a" * 40, "remote_file_id": "file-8",
            "path": "/target/Show.S01E08.mkv", "name": "Show.S01E08.mkv",
            "size": 100, "duration": 60,
        }])
        service = DownloadService(self.store, cloud, budget=SubmissionBudget(1, 1))
        service.set_scope(WINDOW)
        service.poll(target)
        self.assertEqual(self.store.get_episode(target)["status"], "cloud_ready")
        self.assertEqual(self.store.get_episode(sibling)["status"], "submitted")
        self.assertEqual(self.store.history(sibling), ["submitted"])

    def test_pre_submit_inventory_gates_new_submit_without_consuming_budget(self):
        ident = self.episode(status="download_pending")
        for value, reason in (({"state": "present"}, "present"), ({"state": "unknown"}, "unknown"), (None, "unknown"), (RuntimeError("sensitive error"), "error")):
            with self.subTest(value=value):
                callback = Mock(side_effect=value) if isinstance(value, Exception) else Mock(return_value=value)
                cloud = Cloud()
                budget = SubmissionBudget(1, 1)
                service = DownloadService(self.store, cloud, budget=budget, pre_submit_inventory=callback)
                result = service.advance(ident)
                self.assertEqual(result.detail["reason"], "pre_submit_inventory_" + reason)
                self.assertEqual(result.status, "download_pending")
                cloud.submit.assert_not_called()
                self.assertEqual(budget.total, 0)
                callback.assert_called_once()

    def test_proven_missing_and_compatibility_can_submit(self):
        for episode, callback in enumerate((Mock(return_value={"state": "missing"}), None), 8):
            ident = self.episode(episode=episode, status="download_pending")
            cloud = Cloud()
            budget = SubmissionBudget(1, 1)
            DownloadService(self.store, cloud, budget=budget, pre_submit_inventory=callback).advance(ident)
            cloud.submit.assert_called_once()
            self.assertEqual(budget.total, 1)

    def test_inventory_unknown_stops_snapshot_before_another_allowed_row(self):
        self.episode(status="download_pending")
        self.episode(source="themoviedb", status="download_pending")
        cloud = Cloud()
        callback = Mock(return_value={"state": "unknown"})
        service = DownloadService(self.store, cloud, budget=SubmissionBudget(1, 1), pre_submit_inventory=callback)
        summary = PipelineRunner(self.store, services={"download": service}).run(window=WINDOW)
        self.assertEqual(summary["stop_reason"], "pre_submit_inventory_unknown")
        self.assertEqual(summary["advanced"], 0)
        self.assertEqual(summary["skipped_pacing"], 1)
        callback.assert_called_once()
        cloud.find_task.assert_called_once()
        cloud.submit.assert_not_called()

    def test_failed_reconcile_never_calls_inventory_or_submits(self):
        ident = self.episode(status="download_pending")
        cloud = Cloud()
        cloud.find_task.side_effect = RuntimeError("incomplete task inventory")
        callback = Mock(return_value={"state": "missing"})
        budget = SubmissionBudget(1, 1)
        DownloadService(self.store, cloud, budget=budget, pre_submit_inventory=callback).advance(ident)
        callback.assert_not_called()
        cloud.submit.assert_not_called()
        self.assertEqual(budget.total, 0)

    def test_scoped_advance_checks_deterministically_without_background_slot(self):
        ident = self.episode(status="download_pending")
        cloud = Cloud({"task_id": "existing"})
        service = DownloadService(self.store, cloud, budget=SubmissionBudget(1, 1),
                                  cloud_check_budget=1, rotation_seed=3)
        service.set_scope(WINDOW)
        result = service.advance(ident)
        self.assertEqual(result.status, "submitted")
        self.assertEqual(service.cloud_checks_used, 1)
        cloud.find_task.assert_called_once()
        cloud.submit.assert_not_called()

    def test_scoped_advance_preserves_zero_and_exhausted_read_budgets(self):
        ident = self.episode(status="download_pending")
        for limit, used in ((0, 0), (1, 1)):
            with self.subTest(limit=limit, used=used):
                cloud = Cloud()
                service = DownloadService(self.store, cloud, budget=SubmissionBudget(1, 1), cloud_check_budget=limit)
                service.set_scope(WINDOW)
                service.cloud_checks_used = used
                result = service.advance(ident)
                self.assertEqual(result.detail["reason"], "cloud_check_budget_exhausted")
                self.assertEqual(service.cloud_checks_used, used)
                cloud.find_task.assert_not_called()
                cloud.submit.assert_not_called()

    def test_scoped_advance_never_bypasses_account_cooldown(self):
        ident = self.episode(status="download_pending")
        for limit in (None, 0, 1):
            with self.subTest(limit=limit):
                cloud = Cloud()
                service = DownloadService(self.store, cloud, budget=SubmissionBudget(1, 1), cloud_check_budget=limit, now=lambda: 100)
                service.set_scope(WINDOW)
                service.throttle_state.renew(200)
                result = service.advance(ident)
                self.assertEqual(result.detail["reason"], "cloud_check_cooldown")
                self.assertEqual(service.cloud_checks_used, 0)
                cloud.find_task.assert_not_called()
                cloud.submit.assert_not_called()

    def test_unscoped_advance_retains_background_rotation(self):
        ident = self.episode(status="download_pending")
        cloud = Cloud()
        service = DownloadService(self.store, cloud, budget=SubmissionBudget(1, 1), cloud_check_budget=1, rotation_seed=3)
        result = service.advance(ident)
        self.assertEqual(result.detail["reason"], "cloud_check_deferred")
        self.assertEqual(service.cloud_checks_used, 0)
        cloud.find_task.assert_not_called()

    def test_reconciled_task_does_not_call_inventory_or_reserve(self):
        ident = self.episode(status="download_pending")
        cloud = Cloud({"task_id": "existing"})
        callback = Mock(side_effect=AssertionError("must not call"))
        budget = SubmissionBudget(1, 1)
        DownloadService(self.store, cloud, budget=budget, pre_submit_inventory=callback).advance(ident)
        callback.assert_not_called()
        cloud.submit.assert_not_called()
        self.assertEqual(budget.total, 0)
