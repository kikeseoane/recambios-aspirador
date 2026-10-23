from contextlib import nullcontext
import tempfile
from pathlib import Path
import unittest
from unittest.mock import Mock, patch

import growth_once


class OneShotTests(unittest.TestCase):
    branch = "automation/codex-growth-20261015-090000-deadbeef"
    base = "a" * 40

    def test_no_change_reason_uses_decision_from_log(self):
        diff = ("+++ b/docs/growth-log.md\n"
                "+- **Cambio y decisión:** Esperar 28 días antes de tocar Rowenta.\n")
        with patch.object(growth_once.legacy, "run", return_value=Mock(stdout=diff.encode("utf-8"))):
            self.assertIn("Esperar 28 días", growth_once.no_change_reason(Path("checkout")))

    def test_no_change_reason_uses_alternate_decision_heading(self):
        diff = "+++ b/docs/growth-log.md\n+- **Decisión y cambio:** Se investigaron sustitutos y ninguno tiene recambio verificado.\n"
        with patch.object(growth_once.legacy, "run", return_value=Mock(stdout=diff.encode("utf-8"))):
            self.assertIn("Se investigaron sustitutos", growth_once.no_change_reason(Path("checkout")))

    def test_no_change_research_is_saved_for_next_run(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "docs").mkdir()
            (root / "docs" / "growth-log.md").write_text(
                "## 2026-10-18 - Anterior\nViejo\n\n## 2026-10-19 - Candidatos\nDos modelos pendientes\n",
                encoding="utf-8")
            with patch.object(growth_once, "STATE", root), \
                 patch.object(growth_once.legacy, "changed_paths", return_value={"docs/growth-log.md"}):
                growth_once.save_research_note(root)
            note = (root / "research-backlog.md").read_text(encoding="utf-8")
            self.assertIn("Dos modelos pendientes", note)
            self.assertNotIn("Viejo", note)

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

    def test_local_whitespace_error_never_stages_or_dispatches(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            with self.common(state), \
                 patch.object(growth_once.legacy, "single_worker", return_value=nullcontext(True)), \
                 patch.object(growth_once.legacy, "preflight", return_value={"allowed": True, "model": "luna", "reason": "ready"}), \
                 patch.object(growth_once.legacy, "check_budget_status", return_value=(True, "ready")), \
                 patch.object(growth_once.legacy, "invoke_codex", return_value="ready"), \
                 patch.object(growth_once.legacy, "save_checkpoint"), \
                 patch.object(growth_once, "validate_local_patch", side_effect=RuntimeError("trailing whitespace")), \
                 patch.object(growth_once.legacy, "stage_patch") as stage, \
                 patch.object(growth_once, "dispatch_publisher") as dispatch:
                self.assertEqual(growth_once.main([]), 1)
            stage.assert_not_called()
            dispatch.assert_not_called()

    def test_local_patch_check_reports_git_details(self):
        result = Mock(returncode=2, stdout=b"data/backlog.yaml:17: new blank line at EOF.\n")
        with patch.object(growth_once.legacy, "run", return_value=result):
            with self.assertRaisesRegex(RuntimeError, "new blank line at EOF"):
                growth_once.validate_local_patch(Path("checkout"))

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

    def test_log_only_run_releases_hold_without_publishing(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            with self.common(state), \
                 patch.object(growth_once.legacy, "single_worker", return_value=nullcontext(True)), \
                 patch.object(growth_once.legacy, "preflight", return_value={"allowed": True, "model": "luna"}), \
                 patch.object(growth_once.legacy, "check_budget_status", return_value=(True, "ready")), \
                 patch.object(growth_once.legacy, "invoke_codex", return_value="no-change"), \
                 patch.object(growth_once.legacy, "save_checkpoint"), \
                 patch.object(growth_once, "save_research_note") as research_note, \
                 patch.object(growth_once, "release_hold") as release, \
                 patch.object(growth_once, "dispatch_no_change") as status_notice, \
                 patch.object(growth_once, "dispatch_publisher") as dispatch:
                self.assertEqual(growth_once.main([]), 0)
            release.assert_called_once()
            research_note.assert_called_once_with(state)
            status_notice.assert_called_once_with(state)
            dispatch.assert_not_called()

    def test_daily_budget_deferral_reports_status(self):
        with tempfile.TemporaryDirectory() as directory:
            state = Path(directory)
            with self.common(state), \
                 patch.object(growth_once.legacy, "single_worker", return_value=nullcontext(True)), \
                 patch.object(growth_once.legacy, "preflight", return_value={"allowed": True, "reason": "ready"}), \
                 patch.object(growth_once.legacy, "check_budget_status", return_value=(False, "daily_budget")), \
                 patch.object(growth_once, "acquire_remote_hold") as acquire, \
                 patch.object(growth_once, "dispatch_no_change") as notice:
                self.assertEqual(growth_once.main([]), 0)
            acquire.assert_not_called()
            notice.assert_called_once()

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
