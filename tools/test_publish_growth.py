import unittest
from unittest.mock import patch, Mock
from pathlib import Path
from tempfile import TemporaryDirectory
import publish_growth as growth


class PublicationTests(unittest.TestCase):
    def test_invalid_utf8_log_fails_before_validation(self):
        with TemporaryDirectory() as directory:
            path = Path(directory) / "growth-log.md"
            path.write_bytes(b"log\nAcento: \xed\n")
            with patch.object(growth, "Path", return_value=path), patch.object(growth, "git"):
                with self.assertRaises(UnicodeDecodeError):
                    growth.validate()

    def test_log_only_does_not_notify(self):
        self.assertIsNone(growth.improvement_message(["docs/growth-log.md"], "+Sin cambios", "url"))

    def test_summary_only_contains_added_log_and_real_files(self):
        message = growth.improvement_message(["docs/growth-log.md", "content/guias/filtros.md"],
            "+++ b/docs/growth-log.md\n-contexto anterior\n+Mejorada guia de filtros\n+Motivo: resolver dudas", "https://example.com/commit/abc")
        self.assertIn("Mejorada guia de filtros", message)
        self.assertIn("Motivo: resolver dudas", message)
        self.assertIn("content/guias/filtros.md", message)
        self.assertIn("https://example.com/commit/abc", message)
        self.assertNotIn("contexto anterior", message)
        self.assertNotIn("+++", message)

    def test_summary_fits_telegram(self):
        message = growth.improvement_message(["content/" + "a" * 300] * 60, "+" + "x" * 10000, "https://github.com/owner/repo/commit/" + "a" * 40)
        self.assertLess(len(message), 4096)

    def test_backlog_only_notifies_pending_research(self):
        message = growth.improvement_message(
            ["docs/growth-log.md", "data/growth-model-backlog.yaml"],
            "+Candidato X1 sin anuncio verificable; se revisara manana.", "url")
        self.assertIn("pendiente de incorporacion", message)
        self.assertIn("Candidato X1", message)

    def test_model_intake_notifies_incorporation(self):
        message = growth.improvement_message(
            ["docs/growth-log.md", "data/growth-model-backlog.yaml", "data/brands/aspiradores/demo.yaml"],
            "+Incorporado Demo X1.", "url")
        self.assertIn("Modelo nuevo Grow incorporado", message)

    def test_scope(self):
        for path in ("content/guias/filtros.md", "layouts/partials/test.html",
                     "data/brands/aspiradores/xiaomi.yaml", "data/growth-model-backlog.yaml", "docs/growth-log.md"):
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
