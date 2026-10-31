from __future__ import annotations

import unittest

from pipeline.candidates import SubmissionBudget


class SubmissionBudgetTests(unittest.TestCase):
    def test_ten_subscriptions_obey_global_and_per_subscription_limits(self):
        budget = SubmissionBudget(global_limit=8, per_subscription=2)

        accepted = [budget.reserve(str(index // 3)) for index in range(30)]

        self.assertEqual(sum(accepted), 8)
        self.assertLessEqual(max(budget.counts.values()), 2)
        self.assertEqual(budget.total, 8)

    def test_candidate_persistence_does_not_consume_budget(self):
        budget = SubmissionBudget(global_limit=8, per_subscription=2)

        self.assertEqual(budget.counts, {})
        self.assertTrue(budget.reserve("sub-1"))
        self.assertEqual(budget.counts, {"sub-1": 1})


if __name__ == "__main__":
    unittest.main(verbosity=2)
