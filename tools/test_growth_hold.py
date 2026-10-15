import os
import unittest
from unittest.mock import Mock, patch
import growth_bridge
import growth_hold


class HoldTests(unittest.TestCase):
    @patch.object(growth_hold, "git")
    def test_acquire_is_idempotent(self, git):
        git.return_value = "existing ref"
        growth_hold.main("acquire")
        self.assertEqual(git.call_count, 1)
        git.reset_mock()
        git.return_value = ""
        growth_hold.main("acquire")
        self.assertEqual(git.call_args.args, ("push", "origin", f"HEAD:{growth_hold.REF}"))

    @patch.object(growth_hold, "git")
    def test_failed_lookup_never_releases(self, git):
        git.side_effect = RuntimeError("Network unavailable")
        with self.assertRaises(RuntimeError):
            growth_hold.main("release")
        self.assertEqual(git.call_count, 1)

    def bridge(self, status, dry=False, publish_failure=False):
        def execute(*args, **kwargs):
            if args[:2] == ("python", "publisher") and publish_failure:
                raise RuntimeError("Validation failed")
            if args[:3] == ("git", "rev-parse", "HEAD"):
                out = b"base"
            elif args[:3] == ("git", "rev-parse", "FETCH_HEAD"):
                out = b"head"
            elif args[:3] == ("git", "rev-parse", "head^"):
                out = b"base"
            elif args[:2] == ("git", "show"):
                out = f"growth-result:{status}".encode()
            elif args[:2] == ("git", "diff"):
                out = b""
            else:
                out = b"found"
            return Mock(stdout=out)
        env = dict(GROWTH_RESULT_BRANCH="automation/codex-growth-123", GROWTH_BASE_SHA="a" * 40,
                   GROWTH_PUBLISHER="publisher",
                   GROWTH_DRY_RUN="true" if dry else "false")
        with patch.dict(os.environ, env), patch.object(growth_bridge, "run", side_effect=execute) as run:
            if publish_failure or status == "failed":
                with self.assertRaises(RuntimeError):
                    growth_bridge.main()
            else:
                growth_bridge.main()
        return any(c.args[-1] == "release" for c in run.call_args_list)

    def test_deferred_keeps_hold(self):
        self.assertFalse(self.bridge("deferred"))

    def test_failure_keeps_hold(self):
        self.assertFalse(self.bridge("failed"))
        self.assertFalse(self.bridge("ready", publish_failure=True))

    def test_dry_run_does_not_release_existing_hold(self):
        self.assertFalse(self.bridge("ready", dry=True))

    def test_successful_publication_releases_hold(self):
        self.assertTrue(self.bridge("ready"))


if __name__ == "__main__":
    unittest.main()
