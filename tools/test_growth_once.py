from contextlib import nullcontext
import tempfile
from pathlib import Path
import unittest
from unittest.mock import patch

import growth_once


class OneShotTests(unittest.TestCase):
    branch = "automation/codex-growth-20261015-090000-deadbeef"
    base = "a" * 40

    def common(self, state):
        def acquire():
            (state / "growth-lock.txt").write_text("test", encoding="utf-8")
        return patch.multiple(
            growth_once,
            STATE=state,
            discard_confirmed_checkpoint=lambda: None,
            acquire_remote_hold=acquire,
            hold_exists=lambda: False,
            new_checkout=lambda: (state, self.branch, self.base),
        )

    def test_failed_codex_never_dispatches_or_publishes(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            with self.common(state), \
                 patch.object(growth_once.legacy, "single_worker", return_value=nullcontext(True)), \
                 patch.object(growth_once.legacy, "preflight", return_value={"allowed": True, "model": "luna", "reason": "ready"}), \
                 patch.object(growth_once.legacy, "check_budget_status", return_value=(True, "ready")), \
                 patch.object(growth_once.legacy, "invoke_codex", return_value="failed"), \
                 patch.object(growth_once.legacy, "save_checkpoint"), \
                 patch.object(growth_once, "release_hold") as release, \
                 patch.object(growth_once, "dispatch_publisher") as dispatch:
                self.assertEqual(growth_once.main([]), 1)
            dispatch.assert_not_called()
            release.assert_called_once()

    def test_quota_deferral_keeps_pending_checkpoint_and_skips_publish(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            (state / "pending.patch").write_bytes(b"patch")
            with self.common(state), \
                 patch.object(growth_once.legacy, "single_worker", return_value=nullcontext(True)), \
                 patch.object(growth_once.legacy, "preflight", return_value={"allowed": False, "reason": "five_hour_reserve"}), \
                 patch.object(growth_once.legacy, "check_budget_status", return_value=(False, "five_hour_reserve")), \
                 patch.object(growth_once, "release_hold") as release, \
                 patch.object(growth_once, "dispatch_publisher") as dispatch:
                self.assertEqual(growth_once.main([]), 0)
            release.assert_not_called()
            dispatch.assert_not_called()

    def test_five_hour_shortage_requests_one_retry_without_acquiring_lock(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            decision = {"allowed": False, "reason": "five_hour_reserve", "five_hour_resets_at": 1800000000}
            with self.common(state), \
                 patch.object(growth_once.legacy, "single_worker", return_value=nullcontext(True)), \
                 patch.object(growth_once.legacy, "preflight", return_value=decision), \
                 patch.object(growth_once.legacy, "check_budget_status", return_value=(False, "five_hour_reserve")), \
                 patch.object(growth_once, "acquire_remote_hold") as acquire, \
                 patch.object(growth_once, "request_five_hour_retry") as retry:
                self.assertEqual(growth_once.main(["--schedule-five-hour-retry"]), 0)
            acquire.assert_not_called()
            retry.assert_called_once_with(decision)

    def test_daily_budget_never_requests_five_hour_retry(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            decision = {"allowed": False, "reason": "five_hour_reserve", "five_hour_resets_at": 1800000000}
            with self.common(state), \
                 patch.object(growth_once.legacy, "single_worker", return_value=nullcontext(True)), \
                 patch.object(growth_once.legacy, "preflight", return_value=decision), \
                 patch.object(growth_once.legacy, "check_budget_status", return_value=(False, "daily_budget")), \
                 patch.object(growth_once, "request_five_hour_retry") as retry:
                self.assertEqual(growth_once.main(["--schedule-five-hour-retry"]), 0)
            retry.assert_not_called()


if __name__ == "__main__":
    unittest.main()
