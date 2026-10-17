import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import growth_audit


class GrowthAuditAnalyticsTests(unittest.TestCase):
    def test_live_api_snapshot_replaces_missing_export_message(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "data").mkdir()
            (root / ".git").mkdir()
            (root / "data" / "aspiradores.yaml").write_text(
                "brands:\n  dyson:\n    models:\n      - slug: dyson-v8\n"
                "        recambios:\n          bateria:\n            - sku: v8-bateria\n",
                encoding="utf-8",
            )
            (root / "data" / "ofertas.json").write_text(
                json.dumps({"offers": {"v8-bateria": {
                    "url": "https://s.click.aliexpress.com/example",
                    "compatibility_status": "compatible_alto"
                }}}), encoding="utf-8",
            )
            (root / ".git" / "growth-analytics.json").write_text(json.dumps({
                "period": {"start_date": "2026-09-17", "end_date": "2026-10-14"},
                "search_console": {"status": "ok", "pages": [{
                    "page": "https://recambios-aspirador.com/marcas/dyson/dyson-v8/bateria/",
                    "clicks": 3, "impressions": 40, "ctr": .075, "position": 8.5
                }]},
                "ga4": {"status": "ok", "affiliate_clicks": 2, "pages": [{
                    "page": "/marcas/dyson/dyson-v8/bateria/", "sessions": "7",
                    "screenPageViews": "9"
                }]}
            }), encoding="utf-8")
            with patch.object(growth_audit, "ROOT", root):
                report = growth_audit.report(root)
        self.assertIn("Search Console API, periodo cerrado", report)
        self.assertIn("3 clics; 40 impresiones", report)
        self.assertIn("ofertas aceptadas=1", report)
        self.assertIn("affiliate_click=2", report)
        self.assertNotIn("Sin exportacion de Search Console", report)


if __name__ == "__main__":
    unittest.main()
