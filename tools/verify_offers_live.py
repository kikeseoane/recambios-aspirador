"""Liveness check for stored AliExpress affiliate links.

The hourly sync only RE-MATCHES stale SKUs via the affiliate API; it never
opens an already-stored link to confirm it still resolves. AliExpress delists
products constantly, so a link that was good when stored rots into a 404 (or,
more often, a "soft 404": the product is gone and the link redirects to the
home page while still returning HTTP 200).

This script opens the stored link (following redirects) and, on a CONFIDENT
dead signal, degrades the offer to ``needs_url`` so the next sync re-resolves
it and Hugo stops serving the dead link in the meantime.

Design choices (deliberately conservative — a false positive kills a working
affiliate link / revenue):
  * CONFIDENT DEAD only on: HTTP 404/410, or a redirect that lands on an
    AliExpress page that is NOT a product page (``/item/``) — i.e. home / error
    / gateway. Everything ambiguous (bot block 403/429, 5xx, timeout, a link
    that never redirected away from ``s.click``) is ``unknown`` and left alone.
  * Two strikes: a link must look dead on TWO separate runs before it is
    degraded, to ride out transient blips and bot blocks.
  * ``needs_url`` stays the single source of truth for "unresolved"; we also
    blank ``url`` so the live site immediately stops linking to the dead product.

It never calls the AliExpress API, never writes credentials, and is safe to run
repeatedly (bounded by --max-minutes / --batch-size, oldest-checked first).
"""
import argparse
import json
import os
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from urllib.parse import urlsplit

import requests

ROOT = Path(__file__).resolve().parents[1]
OFFERS = ROOT / "data" / "ofertas.json"
DEFAULT_URL = "https://s.click.aliexpress.com/e/_c3VfQRLt"
# A date clearly older than any --only-stale cutoff so the next sync re-resolves
# the degraded SKU promptly (but not 0000-00-00; see fix in sync_ofertas.py).
FORCE_RESOLVE_AT = "1970-01-01"
USER_AGENT = ("Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
             "(KHTML, like Gecko) Chrome/125.0 Safari/537.36")


def classify(resp):
    """Return (verdict, reason): verdict in {'alive','dead','unknown'}."""
    status = resp.status_code
    if status in (404, 410):
        return "dead", f"http_{status}"
    if status == 403 or status == 429 or status >= 500 or status >= 400:
        # Bot block / rate limit / server error / other 4xx: cannot conclude.
        return "unknown", f"http_{status}"
    final = resp.url or ""
    parts = urlsplit(final)
    host, path = parts.netloc.lower(), parts.path or "/"
    if "s.click.aliexpress" in host:
        # Never redirected away from the tracking link (likely a JS/meta
        # redirect we cannot follow server-side). Do not conclude it is dead.
        return "unknown", "no_http_redirect_from_sclick"
    if "aliexpress" not in host:
        return "unknown", f"offsite:{host[:40]}"
    if "/item/" in path:
        return "alive", "item_page"
    # Redirected within AliExpress but NOT to a product page -> home / error.
    return "dead", f"redirect_non_item:{(path or '/')[:40]}"


def check_url(session, url, timeout):
    try:
        resp = session.get(url, allow_redirects=True, timeout=timeout)
    except requests.RequestException as exc:
        return "unknown", f"request_error:{type(exc).__name__}", "", None
    verdict, reason = classify(resp)
    return verdict, reason, resp.url or "", resp.status_code


def eligible(offer, cutoff):
    if not isinstance(offer, dict):
        return False
    url = str(offer.get("url") or "")
    if not url or url == DEFAULT_URL:
        return False
    if offer.get("needs_url") is True or offer.get("orphaned") is True:
        return False
    if int(offer.get("live_check_strikes") or 0) > 0:
        return True  # re-check striked links every run to confirm quickly
    last = str(offer.get("last_verified_at") or "")
    return (not last) or last < cutoff


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--max-minutes", type=float, default=0, metavar="MIN",
                    help="Time budget; 0 = no limit")
    ap.add_argument("--batch-size", type=int, default=0, metavar="N",
                    help="Max SKUs to check this run; 0 = all eligible")
    ap.add_argument("--stale-days", type=int, default=7, metavar="DAYS",
                    help="Only re-verify links not checked in the last DAYS")
    ap.add_argument("--strikes", type=int, default=2, metavar="N",
                    help="Consecutive dead verdicts required before degrading")
    ap.add_argument("--timeout", type=float, default=15.0)
    ap.add_argument("--delay", type=float, default=0.5,
                    help="Seconds between requests to stay polite")
    ap.add_argument("--only-sku", action="append", default=[])
    ap.add_argument("--dry-run", action="store_true",
                    help="Report verdicts but do not write changes")
    args = ap.parse_args()

    doc = json.loads(OFFERS.read_text(encoding="utf-8"))
    offers = doc.get("offers", {})
    today = date.today().isoformat()
    cutoff = (date.today() - timedelta(days=args.stale_days)).isoformat()

    only = {s.strip() for s in args.only_sku if s.strip()}
    if only:
        candidates = [s for s in only if s in offers]
    else:
        candidates = [s for s in offers if eligible(offers[s], cutoff)]
        # Striked first, then oldest-verified first.
        candidates.sort(key=lambda s: (
            0 if int(offers[s].get("live_check_strikes") or 0) > 0 else 1,
            str(offers[s].get("last_verified_at") or ""),
        ))
    if args.batch_size > 0:
        candidates = candidates[:args.batch_size]

    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "es-ES,es;q=0.9"})

    budget = args.max_minutes * 60 if args.max_minutes > 0 else 0
    t0 = time.time()
    checked = alive = dead_now = strike_pending = unknown = 0
    dirty = False

    for sku in candidates:
        if budget and (time.time() - t0) >= budget:
            print(f"  [time-budget] {args.max_minutes:.0f}min alcanzados tras {checked} enlaces.")
            break
        offer = offers[sku]
        verdict, reason, final_url, status = check_url(session, str(offer["url"]), args.timeout)
        checked += 1
        offer["last_verified_at"] = today
        offer["live_check_last"] = f"{verdict}:{reason}"

        if verdict == "alive":
            alive += 1
            offer["live_check_strikes"] = 0
            dirty = True
        elif verdict == "unknown":
            unknown += 1
            dirty = True
        else:  # dead
            strikes = int(offer.get("live_check_strikes") or 0) + 1
            if strikes >= args.strikes:
                dead_now += 1
                offer["dead_url"] = offer.get("url", "")
                offer["url"] = ""
                offer["needs_url"] = True
                offer["match_type"] = "dead_link"
                offer["updated_at"] = FORCE_RESOLVE_AT  # re-resolve ASAP next sync
                offer["live_check_strikes"] = 0
                offer["live_check_dead_at"] = today
                offer["live_check_final_url"] = final_url
                offer["live_check_http_status"] = status
            else:
                strike_pending += 1
                offer["live_check_strikes"] = strikes
            dirty = True

        print(f"  [{checked:4d}] {verdict:7s} {reason:28s} {sku}")
        if args.delay:
            time.sleep(args.delay)

    print(f"STATS_LIVE: checked={checked} alive={alive} dead_degraded={dead_now} "
          f"strike_pending={strike_pending} unknown={unknown} "
          f"candidates={len(candidates)} elapsed={int(time.time()-t0)}s")

    if dirty and not args.dry_run:
        doc["offers"] = offers
        tmp = OFFERS.with_suffix(".json.tmp")
        tmp.write_text(json.dumps(doc, ensure_ascii=False, indent=2), encoding="utf-8")
        os.replace(tmp, OFFERS)
        print(f"Escrito {OFFERS.name}")
    elif args.dry_run:
        print("dry-run: sin cambios escritos")


if __name__ == "__main__":
    main()
