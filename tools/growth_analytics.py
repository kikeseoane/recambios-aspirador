"""Read recent Google Search Console and GA4 data for the Growth worker.

The script is deliberately read-only.  It accepts a short-lived OAuth access
token, or (when ``google-auth`` is installed) an application/service-account
credential.  Missing configuration is reported as unavailable rather than
being presented as zero traffic.
"""
import argparse
import json
import os
from datetime import date, timedelta
from pathlib import Path

import requests


SCOPES = (
    "https://www.googleapis.com/auth/webmasters.readonly",
    "https://www.googleapis.com/auth/analytics.readonly",
)
GSC_ENDPOINT = "https://www.googleapis.com/webmasters/v3/sites/{site}/searchAnalytics/query"
GA4_ENDPOINT = "https://analyticsdata.googleapis.com/v1beta/properties/{property_id}:runReport"
WINDSOR_ENDPOINT = "https://connectors.windsor.ai/{connector}"
DEFAULT_GSC_SITE = "sc-domain:recambios-aspirador.com"
DEFAULT_GA4_PROPERTY_ID = "529119132"


def completed_period(days=28, today=None):
    """Return full dates, leaving three days for Search Console processing."""
    last = (today or date.today()) - timedelta(days=3)
    return {"start_date": (last - timedelta(days=days - 1)).isoformat(), "end_date": last.isoformat()}


def access_token(env=os.environ):
    """Get a token without ever returning credential contents in an error."""
    token = env.get("GROWTH_GOOGLE_ACCESS_TOKEN")
    if token:
        return token, None
    try:
        import google.auth
        from google.auth.transport.requests import Request
    except ImportError:
        return None, ("No hay GROWTH_GOOGLE_ACCESS_TOKEN. Para usar credenciales de "
                      "aplicacion instala google-auth.")
    try:
        credentials, _ = google.auth.default(scopes=SCOPES)
        credentials.refresh(Request())
        return credentials.token, None
    except Exception:
        return None, ("No hay credenciales de Google utilizables. Configura "
                      "GROWTH_GOOGLE_ACCESS_TOKEN o GOOGLE_APPLICATION_CREDENTIALS.")


def request_json(method, url, token, *, payload, session=requests):
    try:
        response = session.request(
            method, url, headers={"Authorization": f"Bearer {token}"}, json=payload, timeout=30
        )
    except requests.RequestException:
        return None, "No se pudo conectar con la API de Google."
    if response.ok:
        try:
            return response.json(), None
        except ValueError:
            return None, "Google devolvio una respuesta no valida."
    # Do not include Google's body: it can contain details unnecessary to Grow.
    if response.status_code in (401, 403):
        return None, "Google denego el acceso; revisa el permiso de la propiedad para la cuenta configurada."
    return None, f"Google no respondio correctamente (HTTP {response.status_code})."


def gsc_report(token, site_url, period, session=requests):
    if not site_url:
        return {"status": "unavailable", "reason": "Falta GROWTH_GSC_SITE_URL."}
    payload = {
        "startDate": period["start_date"], "endDate": period["end_date"],
        "dimensions": ["page"], "rowLimit": 25, "type": "web",
    }
    data, error = request_json("POST", GSC_ENDPOINT.format(site=requests.utils.quote(site_url, safe="")), token,
                               payload=payload, session=session)
    if error:
        return {"status": "error", "reason": error}
    pages = [
        {"page": row["keys"][0], "clicks": row.get("clicks", 0),
         "impressions": row.get("impressions", 0), "ctr": row.get("ctr", 0),
         "position": row.get("position", 0)}
        for row in data.get("rows", []) if row.get("keys")
    ]
    return {"status": "ok", "pages": pages}


def ga4_request(token, property_id, period, dimensions, metrics, session, dimension_filter=None):
    payload = {
        "dateRanges": [{"startDate": period["start_date"], "endDate": period["end_date"]}],
        "dimensions": [{"name": value} for value in dimensions],
        "metrics": [{"name": value} for value in metrics], "limit": 25,
    }
    if dimension_filter:
        payload["dimensionFilter"] = dimension_filter
    return request_json("POST", GA4_ENDPOINT.format(property_id=property_id), token, payload=payload, session=session)


def ga4_report(token, property_id, period, session=requests):
    if not property_id:
        return {"status": "unavailable", "reason": "Falta GROWTH_GA4_PROPERTY_ID."}
    if not str(property_id).isdigit():
        return {"status": "unavailable", "reason": "GROWTH_GA4_PROPERTY_ID debe ser numerico."}
    data, error = ga4_request(token, property_id, period, ["pagePath"], ["sessions", "screenPageViews"], session)
    if error:
        return {"status": "error", "reason": error}
    headers = [item["name"] for item in data.get("metricHeaders", [])]
    pages = []
    for row in data.get("rows", []):
        values = {name: row["metricValues"][index].get("value", "0") for index, name in enumerate(headers)}
        pages.append({"page": row["dimensionValues"][0].get("value", ""), **values})
    click_filter = {"filter": {"fieldName": "eventName", "stringFilter": {"matchType": "EXACT", "value": "affiliate_click"}}}
    clicks, click_error = ga4_request(token, property_id, period, [], ["eventCount"], session, click_filter)
    affiliate_clicks = None
    if not click_error:
        affiliate_clicks = sum(int(row["metricValues"][0].get("value", 0)) for row in clicks.get("rows", []))
    result = {"status": "ok", "pages": pages}
    if affiliate_clicks is not None:
        result["affiliate_clicks"] = affiliate_clicks
    elif click_error:
        result["affiliate_clicks_status"] = click_error
    return result


def windsor_rows(connector, fields, account, period, api_key, session=requests, filters=None):
    """Read one connected Windsor account without exposing the API key in errors."""
    params = {
        "api_key": api_key, "fields": ",".join(fields),
        "select_accounts": account, "date_from": period["start_date"],
        "date_to": period["end_date"], "_max_rows": 25,
    }
    if filters:
        params["filter"] = json.dumps(filters)
    try:
        response = session.request("GET", WINDSOR_ENDPOINT.format(connector=connector),
                                   params=params, timeout=60)
    except requests.RequestException:
        return None, "No se pudo conectar con la API de Windsor.ai."
    if not response.ok:
        if response.status_code in (401, 403):
            return None, "Windsor.ai denego el acceso; revisa la clave API y la cuenta conectada."
        return None, f"Windsor.ai no respondio correctamente (HTTP {response.status_code})."
    try:
        payload = response.json()
    except ValueError:
        return None, "Windsor.ai devolvio una respuesta no valida."
    rows = payload.get("data") if isinstance(payload, dict) else payload
    if not isinstance(rows, list):
        return None, "Windsor.ai no devolvio filas en el formato esperado."
    return [row for row in rows if isinstance(row, dict)], None


def windsor_reports(api_key, site_url, property_id, period, session=requests):
    if not site_url or not property_id:
        issue = "Falta la propiedad GSC o GA4 para consultar Windsor.ai."
        unavailable = {"status": "unavailable", "reason": issue}
        return unavailable, unavailable.copy()
    gsc_rows, gsc_error = windsor_rows(
        "searchconsole", ("page", "clicks", "impressions", "ctr", "position"),
        site_url, period, api_key, session)
    ga4_rows, ga4_error = windsor_rows(
        "googleanalytics4", ("page_path", "sessions", "screen_page_views"),
        property_id, period, api_key, session)
    gsc = ({"status": "error", "reason": gsc_error} if gsc_error else
           {"status": "ok", "source": "windsor", "pages": gsc_rows})
    ga4 = ({"status": "error", "reason": ga4_error} if ga4_error else
           {"status": "ok", "source": "windsor", "pages": [
               {"page": row.get("page_path", ""), "sessions": row.get("sessions", 0),
                "screenPageViews": row.get("screen_page_views", 0)} for row in ga4_rows
           ]})
    if not ga4_error:
        click_rows, click_error = windsor_rows(
            "googleanalytics4", ("event_name", "event_count"), property_id,
            period, api_key, session, filters=[["event_name", "eq", "affiliate_click"]])
        if click_error:
            ga4["affiliate_clicks_status"] = click_error
        else:
            ga4["affiliate_clicks"] = sum(int(row.get("event_count", 0)) for row in click_rows)
    return gsc, ga4


def windows_user_windsor_key():
    """Read the user's saved key when Grow is launched outside Task Scheduler."""
    if os.name != "nt":
        return None
    try:
        import winreg
        with winreg.OpenKey(winreg.HKEY_CURRENT_USER, "Environment") as key:
            value, _ = winreg.QueryValueEx(key, "GROWTH_WINDSOR_API_KEY")
        return value or None
    except (OSError, ImportError):
        return None


def collect(env=None, session=requests, today=None):
    use_user_environment = env is None
    if env is None:
        env = os.environ
    period = completed_period(today=today)
    windsor_key = env.get("GROWTH_WINDSOR_API_KEY") or (
        windows_user_windsor_key() if use_user_environment else None)
    if windsor_key:
        gsc, ga4 = windsor_reports(
            windsor_key,
            env.get("GROWTH_GSC_SITE_URL", DEFAULT_GSC_SITE),
            env.get("GROWTH_GA4_PROPERTY_ID", DEFAULT_GA4_PROPERTY_ID),
            period, session)
        return {"period": period, "search_console": gsc, "ga4": ga4}
    token, issue = access_token(env)
    result = {"period": period}
    if issue:
        unavailable = {"status": "unavailable", "reason": issue}
        result.update({"search_console": unavailable, "ga4": unavailable.copy()})
        return result
    result["search_console"] = gsc_report(token, env.get("GROWTH_GSC_SITE_URL"), period, session)
    result["ga4"] = ga4_report(token, env.get("GROWTH_GA4_PROPERTY_ID"), period, session)
    return result


def markdown(result):
    period = result["period"]
    lines = ["# Datos de Google para Grow", "", f"Periodo cerrado: {period['start_date']} a {period['end_date']}.", ""]
    for name, title in (("search_console", "Search Console"), ("ga4", "GA4")):
        source = result[name]
        lines.extend((f"## {title}", ""))
        if source["status"] != "ok":
            lines.extend((f"No disponible: {source['reason']}", ""))
            continue
        if name == "ga4" and "affiliate_clicks" in source:
            lines.append(f"Clics afiliados (`affiliate_click`): {source['affiliate_clicks']}.")
        if name == "ga4" and "affiliate_clicks_status" in source:
            lines.append(f"Clics afiliados no disponibles: {source['affiliate_clicks_status']}")
        pages = source.get("pages", [])
        if not pages:
            lines.extend(("La API no devolvio filas para el periodo.", ""))
            continue
        for page in pages:
            if name == "search_console":
                lines.append(f"- {page['page']}: {page['clicks']} clics, {page['impressions']} impresiones, "
                             f"CTR {float(page['ctr']):.1%}, posicion {float(page['position']):.1f}.")
            else:
                lines.append(f"- {page['page']}: {page.get('sessions', '0')} sesiones, "
                             f"{page.get('screenPageViews', '0')} vistas.")
        lines.append("")
    return "\n".join(lines) + "\n"


def main(argv=None):
    parser = argparse.ArgumentParser(description="Read recent Google Search Console and GA4 metrics for Grow")
    parser.add_argument("--output", type=Path, help="Markdown destination; stdout when omitted")
    parser.add_argument("--json", action="store_true", help="Emit the structured result instead of Markdown")
    args = parser.parse_args(argv)
    result = collect()
    value = json.dumps(result, ensure_ascii=False, indent=2) + "\n" if args.json else markdown(result)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(value, encoding="utf-8")
    else:
        print(value, end="")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
