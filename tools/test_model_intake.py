import unittest
from datetime import date
from pathlib import Path
from tempfile import TemporaryDirectory
from unittest.mock import patch

import model_intake


def write(root: Path, relative: str, content: str):
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")


def candidate(status="incorporated", url="https://www.aliexpress.com/item/100500123.html"):
    return {
        "brand_key": "demo",
        "model": "Demo Vacuum X1",
        "slug": "demo-x1",
        "sku_pack": "stick10",
        "model_tokens": ["x1", "demo"],
        "status": status,
        "last_checked": "2026-10-19",
        "next_step": "Esperar la comprobacion horaria de ofertas.",
        "manufacturer": {"url": "https://example.com/demo-x1", "reference": "X1 EU"},
        "demand": {"source": "Search Console", "url": "https://search.google.com/search-console/",
                   "observed_on": "2026-10-19", "evidence": "Consulta de repuesto observada."},
        "aliexpress_parts": [
            {"category": "filtro", "url": url, "listing_title": "X1 filter",
             "compatibility_evidence": "El anuncio nombra X1.", "verified_on": "2026-10-19"},
            {"category": "cepillo", "url": url.replace("123", "124"), "listing_title": "X1 brush",
             "compatibility_evidence": "El anuncio nombra X1.", "verified_on": "2026-10-19"},
            {"category": "bateria", "url": url.replace("123", "125"), "listing_title": "X1 battery",
             "compatibility_evidence": "El anuncio nombra X1.", "verified_on": "2026-10-19"},
        ],
    }


class ModelIntakeTests(unittest.TestCase):
    def setUp(self):
        self.temp = TemporaryDirectory()
        self.root = Path(self.temp.name)
        write(self.root, "data/brands/aspiradores/demo.yaml", "brand_key: demo\nmodels:\n  - model: Demo Vacuum X1\n    slug: demo-x1\n    sku_pack: stick10\n")

    def tearDown(self):
        self.temp.cleanup()

    def save_backlog(self, entries):
        import yaml
        write(self.root, "data/growth-model-backlog.yaml", yaml.safe_dump(
            {"version": 1, "candidates": entries}, sort_keys=False, allow_unicode=True))

    def test_incorporated_candidate_requires_evidence_and_catalog_model(self):
        self.save_backlog([candidate()])
        self.assertEqual(model_intake.validate(self.root, date(2026, 10, 19)), [])

    def test_ready_candidate_rejects_non_item_link(self):
        self.save_backlog([candidate(status="ready_for_catalog", url="https://www.aliexpress.com/store/1")])
        errors = model_intake.validate(self.root, date(2026, 10, 19))
        self.assertTrue(any("anuncio directo" in error for error in errors))

    def test_researching_candidate_keeps_next_step_without_claiming_verification(self):
        item = candidate(status="researching")
        for field in ("manufacturer", "demand", "aliexpress_parts", "sku_pack", "model_tokens"):
            item.pop(field)
        self.save_backlog([item])
        self.assertEqual(model_intake.validate(self.root, date(2026, 10, 19)), [])

    def test_rejected_candidate_keeps_minimal_identity_without_pack_or_numeric_token(self):
        item = candidate(status="rejected")
        for field in ("manufacturer", "demand", "aliexpress_parts", "sku_pack", "model_tokens"):
            item.pop(field)
        item["rejection_reason"] = "Solo aparecen accesorios sin modelo verificable."
        self.save_backlog([item])
        self.assertEqual(model_intake.validate(self.root, date(2026, 10, 19)), [])

    def test_ready_candidate_rejects_stale_verified_listing(self):
        item = candidate(status="ready_for_catalog")
        for part in item["aliexpress_parts"]:
            part["verified_on"] = "2026-09-18"
        self.save_backlog([item])
        errors = model_intake.validate(self.root, date(2026, 10, 19))
        self.assertTrue(any("supera los 30 dias" in error for error in errors))

    def test_historical_incorporated_candidate_allows_old_evidence(self):
        item = candidate()
        for part in item["aliexpress_parts"]:
            part["verified_on"] = "2026-09-18"
        self.save_backlog([item])
        self.assertEqual(model_intake.validate(self.root, date(2026, 10, 19)), [])

    def test_newly_incorporated_candidate_rejects_stale_verified_listing(self):
        item = candidate()
        for part in item["aliexpress_parts"]:
            part["verified_on"] = "2026-09-18"
        self.save_backlog([item])
        errors = model_intake.validate(
            self.root, date(2026, 10, 19), fresh_incorporated={("demo", "demo-x1")})
        self.assertTrue(any("supera los 30 dias" in error for error in errors))

    def test_incorporated_candidate_must_match_source_model_and_sku_pack(self):
        item = candidate()
        item["model"] = "Otra Vacuum X1"
        item["sku_pack"] = "robot10"
        self.save_backlog([item])
        errors = model_intake.validate(self.root, date(2026, 10, 19))
        self.assertTrue(any(".model no coincide" in error for error in errors))
        self.assertTrue(any(".sku_pack no coincide" in error for error in errors))

    @patch.object(model_intake, "newly_incorporated", return_value=[("demo", "demo-x1"), ("demo", "demo-x2")])
    def test_daily_limit_rejects_more_than_one_new_model(self, _newly):
        self.save_backlog([candidate()])
        errors = model_intake.validate_daily_limit(self.root)
        self.assertEqual(len(errors), 1)
        self.assertIn("un modelo", errors[0])

    @patch.object(model_intake, "newly_incorporated", return_value=[("demo", "demo-x1")])
    def test_new_model_requires_incorporated_backlog_entry(self, _newly):
        self.save_backlog([candidate(status="ready_for_catalog")])
        errors = model_intake.validate_daily_limit(self.root)
        self.assertEqual(len(errors), 1)
        self.assertIn("necesita una ficha", errors[0])


if __name__ == "__main__":
    unittest.main()
