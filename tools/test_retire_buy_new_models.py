import json
import tempfile
import unittest
from pathlib import Path

from retire_buy_new_models import candidates, retire, without_models


class RetirementTests(unittest.TestCase):
    def fixture(self, root):
        (root / "data/brands/aspiradores").mkdir(parents=True)
        (root / "content/marcas/demo/old").mkdir(parents=True)
        (root / "static").mkdir()
        source = ("brand_key: demo\nname: Demo\nmodels:\n"
                  "  - model: Old\n    slug: old\n    sku_pack: stick10\n\n"
                  "  - model: New\n    slug: new\n    sku_pack: stick10\n")
        (root / "data/brands/aspiradores/demo.yaml").write_text(source, encoding="utf-8")
        (root / "data/aspiradores.yaml").write_text(
            "brands:\n  demo:\n    models:\n      - slug: old\n"
            "        recambios:\n          filtro:\n            - sku: old-filter\n"
            "      - slug: new\n        recambios:\n          filtro:\n            - sku: new-filter\n",
            encoding="utf-8")
        (root / "data/ofertas.json").write_text(json.dumps({"offers": {
            "old-filter": {"compatibility_status": "fallback_buy_new", "needs_url": True},
            "new-filter": {"compatibility_status": "compatible_alto", "url": "https://example.com"}
        }}), encoding="utf-8")
        (root / "content/marcas/demo/old/_index.md").write_text(
            "---\ngenerated: true\n---\n", encoding="utf-8")
        (root / "static/_redirects").write_text(
            "/modelos/old/ /marcas/demo/old/ 301\n", encoding="utf-8")

    def test_retires_only_all_buy_new_and_redirects_old_url(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            selected = candidates(root)
            self.assertEqual(selected, [("demo", "old", 1)])
            retire(root, selected)
            self.assertFalse((root / "content/marcas/demo/old").exists())
            source = (root / "data/brands/aspiradores/demo.yaml").read_text(encoding="utf-8")
            self.assertNotIn("slug: old", source)
            self.assertIn("slug: new", source)
            redirects = (root / "static/_redirects").read_text(encoding="utf-8")
            self.assertIn("/marcas/demo/old/ /marcas/demo/ 301", redirects)
            self.assertIn("/modelos/old/ /marcas/demo/ 301", redirects)

    def test_manual_page_prevents_retirement(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.fixture(root)
            (root / "content/marcas/demo/old/manual.md").write_text("Manual", encoding="utf-8")
            with self.assertRaises(ValueError):
                retire(root, candidates(root))
            self.assertTrue((root / "content/marcas/demo/old").exists())

    def test_missing_source_model_is_rejected(self):
        with self.assertRaises(ValueError):
            without_models("models:\n  - model: New\n    slug: new\n", {"old"})


if __name__ == "__main__":
    unittest.main()
