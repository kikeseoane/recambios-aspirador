import unittest

from offer_refresh_guard import preserve_verified_offer, refresh_due, stale_cutoff


class OfferRefreshGuardTests(unittest.TestCase):
    def test_failed_refresh_keeps_live_offer(self):
        old = {"url": "https://s.click.aliexpress.com/e/good", "needs_url": False,
               "compatibility_status": "compatible_alto", "match_type": "exact_or_best_match",
               "live_check_last": "alive:item_page", "updated_at": "2026-10-01"}
        pending = {"url": "", "needs_url": True, "compatibility_status": "pending_ai_validation",
                   "ai_validation_reason": "daily_ai_budget_exhausted"}
        result = preserve_verified_offer(old, pending, "2026-10-18", "https://fallback")
        self.assertEqual(result["url"], old["url"])
        self.assertEqual(result["compatibility_status"], "compatible_alto")
        self.assertEqual(result["updated_at"], "2026-10-18")
        self.assertNotIn("ai_validation_status", result)

    def test_dead_link_is_not_restored(self):
        old = {"url": "", "needs_url": True, "compatibility_status": "compatible_alto"}
        unresolved = {"url": "", "needs_url": True, "compatibility_status": "fallback_buy_new"}
        self.assertIs(preserve_verified_offer(old, unresolved, "2026-10-18", "https://fallback"), unresolved)

    def test_valid_replacement_wins(self):
        old = {"url": "https://old", "compatibility_status": "compatible_alto"}
        new = {"url": "https://new", "compatibility_status": "compatible_alto"}
        self.assertIs(preserve_verified_offer(old, new, "2026-10-18", "https://fallback"), new)

    def test_weekly_rotation_only_refreshes_stale_offers(self):
        cutoff = stale_cutoff("2026-10-18", 7)
        self.assertTrue(refresh_due({"url": "https://valid", "updated_at": "2026-10-01"}, cutoff))
        self.assertFalse(refresh_due({"url": "https://valid", "updated_at": "2026-10-17"}, cutoff))


if __name__ == "__main__":
    unittest.main()
