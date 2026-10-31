"""Offline regressions for durable, at-most-once download submission."""
from __future__ import annotations

import os
import tempfile
import unittest
from unittest.mock import Mock

from pipeline.adapters import RemoteInventoryIncomplete
from pipeline.candidates import SubmissionBudget
from pipeline.models import EpisodeKey
from pipeline.services import DownloadService
from pipeline.state import StateStore


class SubmissionIntentGuardTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.path = os.path.join(self.tmp.name, "state.db")
        self.store = StateStore(self.path)
        self.ident = self.store.get_or_create_episode({
            "key": EpisodeKey("tmdb", "111", 1, 8),
            "status": "download_pending", "subscription_id": "sub",
        })
        self.store.add_candidate(self.ident, {"infohash": "a" * 40, "title": "Show"})
        self.cloud = Mock()
        self.cloud.find_task.return_value = None
        self.cloud.submit.return_value = "remote-1"
        self.budget = SubmissionBudget(1, 1)

    def tearDown(self):
        self.store.close()
        self.tmp.cleanup()

    def service(self, **kwargs):
        return DownloadService(self.store, self.cloud, budget=self.budget, **kwargs)

    def attempted(self, *, count=0):
        self.store.transition(self.ident, "download_pending",
                              {"reason": "download_submit_intent"}, extra={"submit_count": count})
        # The guard must inspect more than the most recent history event.
        self.store.transition(self.ident, "download_pending", {"reason": "download_submit_failed"})

    def test_attempt_and_budget_are_committed_before_external_write(self):
        def submit(_uri):
            observer = StateStore(self.path)
            try:
                self.assertEqual(observer.get_episode(self.ident)["submit_count"], 1)
                self.assertEqual(observer.last_history_detail(self.ident)["reason"], "download_submit_intent")
                self.assertEqual(self.budget.total, 1)
            finally:
                observer.close()
            return "remote-1"
        self.cloud.submit.side_effect = submit
        result = self.service().advance(self.ident)
        self.assertEqual(result.status, "submitted")
        self.assertEqual(self.store.get_episode(self.ident)["submit_count"], 1)

    def test_budget_denial_leaves_no_attempt_and_can_submit_later(self):
        self.budget.reserve("sub")
        result = self.service().advance(self.ident)
        self.assertEqual(result.detail["reason"], "submission_budget_exhausted")
        self.assertEqual(self.store.get_episode(self.ident)["submit_count"], 0)
        self.assertFalse(self.service()._submission_attempted(self.ident, self.store.get_episode(self.ident)))
        self.budget = SubmissionBudget(1, 1)
        self.assertEqual(self.service().advance(self.ident).status, "submitted")
        self.cloud.submit.assert_called_once()

    def test_timeout_marks_uncertain_and_stale_pending_cannot_resubmit(self):
        stale = self.store.get_episode(self.ident)
        self.cloud.submit.side_effect = TimeoutError("untrusted details")
        result = self.service().advance(stale)
        self.assertEqual(result.detail["reason"], "download_submission_uncertain")
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(self.store.get_episode(self.ident)["review_reason"], "UNEXPECTED_REMOTE_STATE")
        self.service().advance(stale)
        self.cloud.submit.assert_called_once()
        self.assertEqual(self.store.get_episode(self.ident)["submit_count"], 1)

    def test_legacy_crash_intent_survives_restart_and_blocks_submit(self):
        self.attempted()
        self.store.close()
        self.store = StateStore(self.path)
        callback = Mock(return_value={"state": "missing"})
        result = self.service(pre_submit_inventory=callback).advance(self.ident)
        self.assertEqual(result.status, "needs_review")
        self.assertEqual(result.detail["reason"], "download_submission_uncertain")
        self.cloud.submit.assert_not_called()
        callback.assert_not_called()
        self.assertEqual(self.budget.total, 0)

    def test_process_interruption_after_write_cannot_resubmit_after_restart(self):
        self.cloud.submit.side_effect = KeyboardInterrupt()
        with self.assertRaises(KeyboardInterrupt):
            self.service().advance(self.ident)
        self.store.close()
        self.store = StateStore(self.path)
        self.assertEqual(self.store.get_episode(self.ident)["submit_count"], 1)
        self.assertEqual(self.service().advance(self.ident).status, "needs_review")
        self.cloud.submit.assert_called_once()

    def test_count_alone_blocks_submit(self):
        self.store.transition(self.ident, "download_pending", extra={"submit_count": 1})
        result = self.service().advance(self.ident)
        self.assertEqual(result.status, "needs_review")
        self.cloud.submit.assert_not_called()
        self.assertEqual(self.budget.total, 0)

    def test_previous_attempt_can_reconcile_without_spending_budget(self):
        self.attempted()
        self.cloud.find_task.return_value = {"task_id": "existing"}
        result = self.service().advance(self.ident)
        self.assertEqual(result.status, "submitted")
        self.assertEqual(self.store.get_episode(self.ident)["remote_task_id"], "existing")
        self.cloud.submit.assert_not_called()
        self.assertEqual(self.budget.total, 0)

    def test_incomplete_reconcile_never_submits_or_queries_inventory(self):
        self.attempted()
        self.cloud.find_task.side_effect = RemoteInventoryIncomplete()
        callback = Mock(return_value={"state": "missing"})
        result = self.service(pre_submit_inventory=callback).advance(self.ident)
        self.assertEqual(result.status, "download_pending")
        self.cloud.submit.assert_not_called()
        callback.assert_not_called()
        self.assertEqual(self.budget.total, 0)

    def test_missing_finder_is_unknown_not_absence(self):
        del self.cloud.find_task
        result = self.service().advance(self.ident)
        self.assertEqual(result.detail["reason"], "download_reconcile_failed")
        self.cloud.submit.assert_not_called()
        self.assertEqual(self.budget.total, 0)

    def test_submitted_row_is_preserved_when_stale_pending_is_passed(self):
        stale = self.store.get_episode(self.ident)
        self.store.transition(self.ident, "submitted", extra={"submit_count": 1, "remote_task_id": "existing"})
        before = self.store.get_episode(self.ident)
        result = self.service().advance(stale)
        self.assertEqual(result.status, "submitted")
        self.assertEqual(self.store.get_episode(self.ident), before)
        self.cloud.find_task.assert_not_called()
        self.cloud.submit.assert_not_called()


if __name__ == "__main__":
    unittest.main()
