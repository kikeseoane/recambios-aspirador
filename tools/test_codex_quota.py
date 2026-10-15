import unittest
from codex_quota import quota_decision, select_model


class QuotaTests(unittest.TestCase):
    def result(self, primary, secondary=None):
        return {"rateLimits": {"primary": {"usedPercent": primary, "windowDurationMins": 300, "resetsAt": 1800000000},
                               "secondary": {"usedPercent": secondary, "windowDurationMins": 10080, "resetsAt": 1800604800} if secondary is not None else None}}

    def test_reserve(self):
        self.assertTrue(quota_decision(self.result(80))["allowed"])
        for used in (81, 100):
            self.assertFalse(quota_decision(self.result(used))["allowed"])

    def test_weekly_usage_is_reported_but_not_mistaken_for_grow_budget(self):
        self.assertTrue(quota_decision(self.result(10, 90))["allowed"])

    def test_unknown_never_assumed_free(self):
        for value in ({}, self.result(None), self.result(-1), self.result(float("nan"))):
            with self.assertRaises(RuntimeError):
                quota_decision(value)

    def test_select_only_available_efficient_model(self):
        self.assertEqual(select_model([{"model": "gpt-5.6-luna"}]), "gpt-5.6-luna")
        with self.assertRaises(RuntimeError):
            select_model([{"model": "gpt-6-astra", "isDefault": True}])

    def test_weekly_identified_by_duration_not_window_position(self):
        result = {"rateLimits": {"primary": {"usedPercent": 15, "windowDurationMins": 10080, "resetsAt": 1800000000},
                                 "secondary": {"usedPercent": 90, "windowDurationMins": 300, "resetsAt": 1800000000}}}
        decision = quota_decision(result)
        self.assertEqual(decision["weekly_used_percent"], 15)
        self.assertFalse(decision["allowed"])


if __name__ == "__main__":
    unittest.main()
