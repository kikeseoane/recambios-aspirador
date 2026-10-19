import unittest

from notify_growth_status import message


class StatusNoticeTests(unittest.TestCase):
    def test_no_change_message_says_no_publication_and_shows_sources(self):
        notice = message("2026-10-18", "Esperar 28 días antes de evaluar Rowenta.", "ok", "ok")
        self.assertIn("sin nueva mejora publicada", notice)
        self.assertIn("Search Console: disponible", notice)
        self.assertIn("GA4: disponible", notice)
        self.assertIn("Esperar 28 días", notice)

    def test_rejects_multiline_reason(self):
        with self.assertRaises(ValueError):
            message("2026-10-18", "texto\notro", "ok", "ok")

    def test_published_message_reports_change(self):
        notice = message("2026-10-19", "Retirados siete modelos buy new.", "ok", "ok", "published")
        self.assertIn("mejora publicada en GitHub", notice)
        self.assertNotIn("sin nueva mejora", notice)


if __name__ == "__main__":
    unittest.main()
