import unittest
from datetime import date
from unittest.mock import Mock

import requests

import growth_analytics as analytics


class FakeResponse:
    def __init__(self, status_code=200, payload=None):
        self.status_code = status_code
        self.ok = status_code < 400
        self._payload = payload or {}

    def json(self):
        return self._payload


class FakeSession:
    def __init__(self, responses):
        self.responses = list(responses)
        self.calls = []

    def request(self, *args, **kwargs):
        self.calls.append((args, kwargs))
        return self.responses.pop(0)


class GrowthAnalyticsTests(unittest.TestCase):
    def test_period_leaves_search_console_processing_delay(self):
        self.assertEqual(analytics.completed_period(today=date(2026, 10, 17)),
                         {"start_date": "2026-09-17", "end_date": "2026-10-14"})

    def test_missing_credentials_is_explicit_not_zero_traffic(self):
        result = analytics.collect(env={}, today=date(2026, 10, 17))
        self.assertEqual(result["search_console"]["status"], "unavailable")
        self.assertIn("No hay", result["ga4"]["reason"])
        self.assertIn("No disponible", analytics.markdown(result))

    def test_collects_gsc_and_ga4_with_access_token(self):
        session = FakeSession([
            FakeResponse(payload={"rows": [{"keys": ["https://example.test/guia/"], "clicks": 3,
                                               "impressions": 40, "ctr": .075, "position": 8.5}]}),
            FakeResponse(payload={"metricHeaders": [{"name": "sessions"}, {"name": "screenPageViews"}],
                                  "rows": [{"dimensionValues": [{"value": "/guia/"}],
                                            "metricValues": [{"value": "7"}, {"value": "9"}]}]}),
            FakeResponse(payload={"rows": [{"metricValues": [{"value": "2"}]}]}),
        ])
        env = {"GROWTH_GOOGLE_ACCESS_TOKEN": "not-printed", "GROWTH_GSC_SITE_URL": "sc-domain:example.test",
               "GROWTH_GA4_PROPERTY_ID": "123"}
        result = analytics.collect(env=env, session=session, today=date(2026, 10, 17))
        self.assertEqual(result["search_console"]["pages"][0]["clicks"], 3)
        self.assertEqual(result["ga4"]["pages"][0]["sessions"], "7")
        self.assertEqual(result["ga4"]["affiliate_clicks"], 2)
        self.assertEqual(len(session.calls), 3)
        self.assertNotIn("not-printed", analytics.markdown(result))

    def test_access_denied_is_reported_without_response_body(self):
        session = FakeSession([FakeResponse(403)])
        result = analytics.gsc_report("secret", "sc-domain:example.test", analytics.completed_period(), session)
        self.assertEqual(result["status"], "error")
        self.assertIn("denego", result["reason"])

    def test_collects_connected_windsor_accounts_without_exposing_key(self):
        session = FakeSession([
            FakeResponse(payload={"data": [{"page": "https://example.test/guia/", "clicks": 3,
                                            "impressions": 40, "ctr": .075, "position": 8.5}]}),
            FakeResponse(payload={"data": [{"page_path": "/guia/", "sessions": 7,
                                            "screen_page_views": 9}]}),
        ])
        result = analytics.collect(env={"GROWTH_WINDSOR_API_KEY": "not-printed"},
                                   session=session, today=date(2026, 10, 17))
        self.assertEqual(result["search_console"]["pages"][0]["clicks"], 3)
        self.assertEqual(result["ga4"]["pages"][0]["screenPageViews"], 9)
        self.assertEqual(session.calls[0][1]["params"]["select_accounts"], analytics.DEFAULT_GSC_SITE)
        self.assertEqual(session.calls[1][1]["params"]["select_accounts"], analytics.DEFAULT_GA4_PROPERTY_ID)
        self.assertNotIn("not-printed", analytics.markdown(result))

    def test_windsor_denial_does_not_echo_secret_or_server_body(self):
        session = FakeSession([FakeResponse(403, payload={"error": "not-printed"}),
                               FakeResponse(payload={"data": []})])
        result = analytics.collect(env={"GROWTH_WINDSOR_API_KEY": "not-printed"}, session=session)
        self.assertEqual(result["search_console"]["status"], "error")
        self.assertNotIn("not-printed", str(result))

    def test_api_connection_failure_is_nonfatal(self):
        session = Mock()
        session.request.side_effect = requests.ConnectionError("private diagnostic")
        result = analytics.gsc_report("secret", "sc-domain:example.test", analytics.completed_period(), session)
        self.assertEqual(result["status"], "error")
        self.assertNotIn("private diagnostic", result["reason"])


if __name__ == "__main__":
    unittest.main()
