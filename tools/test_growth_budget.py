from datetime import datetime, timezone
import unittest
from growth_budget import update_budget


class BudgetTests(unittest.TestCase):
    now = datetime(2026, 10, 13, 9, tzinfo=timezone.utc)

    def decision(self, used, reset=1800000000, allowed=True):
        return dict(allowed=allowed, weekly_used_percent=used, weekly_resets_at=reset)

    def test_ten_points_and_same_day_retry(self):
        state, ok = update_budget(None, self.decision(30), self.now)
        self.assertTrue(ok)
        state, ok = update_budget(state, self.decision(39), self.now)
        self.assertTrue(ok)
        state, ok = update_budget(state, self.decision(40), self.now)
        self.assertFalse(ok)
        self.assertFalse(update_budget(state, self.decision(40), self.now)[1])

    def test_next_daily_boundary_not_midnight(self):
        state, _ = update_budget(None, self.decision(30), self.now)
        state, _ = update_budget(state, self.decision(40), self.now)
        before = datetime(2026, 10, 14, 6, 59, tzinfo=timezone.utc)
        after = datetime(2026, 10, 14, 7, 0, tzinfo=timezone.utc)
        self.assertFalse(update_budget(state, self.decision(41), before)[1])
        self.assertTrue(update_budget(state, self.decision(41), after)[1])

    def test_reset_does_not_replenish_daily_budget(self):
        state, _ = update_budget(None, self.decision(90), self.now)
        state, _ = update_budget(state, self.decision(99), self.now)
        state, ok = update_budget(state, self.decision(2, reset=1800604800), self.now)
        self.assertFalse(ok)
        self.assertEqual(state["consumed"], 11)

    def test_twenty_percent_reserve_still_applies(self):
        self.assertFalse(update_budget(None, self.decision(80, allowed=False), self.now)[1])

    def test_weekly_grow_budget_stops_at_seventy_points(self):
        state = dict(day="2026-10-13", consumed=0, last_used=60,
                     reset=1800000000, weekly_consumed=60)
        state, ok = update_budget(state, self.decision(70), self.now)
        self.assertFalse(ok)
        self.assertEqual(state["weekly_consumed"], 70)

    def test_subscription_reset_keeps_today_daily_cap_but_restarts_grow_week(self):
        state, _ = update_budget(None, self.decision(0), self.now)
        state, _ = update_budget(state, self.decision(10), self.now)
        state, ok = update_budget(state, self.decision(1, reset=1800604800), self.now)
        self.assertFalse(ok)
        self.assertEqual(state["consumed"], 10)
        self.assertEqual(state["weekly_consumed"], 0)

    def test_missing_weekly_fails_closed(self):
        with self.assertRaises(RuntimeError):
            update_budget(None, {"allowed": True}, self.now)

    def test_downward_rounding_does_not_create_allowance(self):
        state, _ = update_budget(None, self.decision(30), self.now)
        for used in (35, 34, 40):
            state, ok = update_budget(state, self.decision(used), self.now)
        self.assertFalse(ok)
        self.assertEqual(state["consumed"], 10)


if __name__ == "__main__":
    unittest.main()
