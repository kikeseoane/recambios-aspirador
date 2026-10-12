import unittest
from unittest.mock import patch, Mock
import publish_growth as growth


class PublicationTests(unittest.TestCase):
    def test_scope(self):
        for path in ("content/guias/filtros.md", "layouts/partials/test.html",
                     "data/brands/aspiradores/xiaomi.yaml", "docs/growth-log.md"):
            self.assertTrue(growth.allowed(path), path)
        for path in (".github/workflows/sync-ofertas.yml", "data/ofertas.json",
                     "data/vertical_defaults.yaml", "Consultas.csv", "tools/publish_growth.py",
                     "static/.env", "content/../secrets"):
            self.assertFalse(growth.allowed(path), path)

    @patch.dict("os.environ", {"GITHUB_ACTIONS": "true"})
    @patch.object(growth, "validate")
    @patch.object(growth, "changed_paths", return_value=["docs/growth-log.md"])
    @patch.object(growth, "git")
    def test_rejected_push_revalidates(self, git, paths, validate):
        pushes = iter([1, 0])
        def run(*args, **kwargs):
            return Mock(returncode=next(pushes) if args[0] == "push" else 0, stdout="sha")
        git.side_effect = run
        growth.main()
        self.assertEqual(validate.call_count, 3)
        self.assertEqual(sum(c.args[0] == "push" for c in git.call_args_list), 2)

    @patch.dict("os.environ", {"GITHUB_ACTIONS": "true"})
    @patch.object(growth, "validate")
    @patch.object(growth, "changed_paths", return_value=["docs/growth-log.md"])
    @patch.object(growth, "git")
    def test_conflict_never_pushes(self, git, paths, validate):
        git.side_effect = lambda *a, **k: Mock(returncode=int(a == ("rebase", "origin/main")), stdout="")
        with self.assertRaisesRegex(RuntimeError, "conflicting"):
            growth.main()
        self.assertFalse(any(c.args[0] == "push" for c in git.call_args_list))

    @patch.dict("os.environ", {"GITHUB_ACTIONS": "true"})
    @patch.object(growth, "changed_paths", return_value=["data/ofertas.json"])
    @patch.object(growth, "git")
    def test_generated_offers_rejected_before_commit(self, git, paths):
        with self.assertRaisesRegex(RuntimeError, "Out-of-scope"):
            growth.main()
        git.assert_not_called()


if __name__ == "__main__":
    unittest.main()
