# tools/audit_ofertas.py
# -*- coding: utf-8 -*-
"""
Audits ofertas.json to identify SKUs that repeatedly fail to find URLs.
Generates a report with failure patterns and keyword suggestions.

Usage:
    python tools/audit_ofertas.py
    python tools/audit_ofertas.py --top 30
    python tools/audit_ofertas.py --by-category
    python tools/audit_ofertas.py --suggest-fixes
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from collections import Counter, defaultdict
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any, Dict, List, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[1]
OFFERS = ROOT / "data" / "ofertas.json"
CATALOG_SKUS = ROOT / "data" / "catalog_skus.yaml"

# Known problematic patterns in keyword generation
KEYWORD_ISSUES = {
    "too_long": "Keyword exceeds 60 chars (AliExpress truncates)",
    "spanish_terms": "Contains Spanish terms that don't match AliExpress listings",
    "no_model_token": "Missing alphanumeric model identifier (e.g., V11, SV14)",
    "generic_brand": "Brand name too generic or missing",
    "rare_category": "Category has historically low match rate on AliExpress",
}

RARE_CATEGORIES = {"junta", "deposito", "cesta", "bomba", "resistencia", "rodamiento", "escobillas", "correa", "rueda", "freno"}

SPANISH_TERMS_IN_QUERIES = {
    "bateria": "battery",
    "filtro": "filter",
    "cepillo": "brush",
    "cargador": "charger",
    "soporte": "mount holder",
    "accesorios": "accessories",
    "aspirador": "vacuum cleaner",
    "aspiradora": "vacuum cleaner",
    "recambio": "replacement",
    "repuesto": "spare part",
}


def load_offers() -> Dict[str, Any]:
    if not OFFERS.exists():
        print(f"ERROR: {OFFERS} not found")
        sys.exit(1)
    return json.loads(OFFERS.read_text(encoding="utf-8"))


def load_catalog_skus() -> Dict[str, Any]:
    if not CATALOG_SKUS.exists():
        return {}
    return yaml.safe_load(CATALOG_SKUS.read_text(encoding="utf-8")) or {}


# Known category keywords that appear in SKU IDs
KNOWN_CAT_TOKENS = {
    "battery": "bateria", "bateria": "bateria",
    "filter": "filtro", "filtro": "filtro",
    "roller": "cepillo", "cepillo": "cepillo", "brush": "cepillo",
    "charger": "cargador", "cargador": "cargador",
    "wall": "soporte", "mount": "soporte", "dock": "soporte", "soporte": "soporte",
    "accessory": "accesorios", "accesorios": "accesorios", "kit": "accesorios",
    "laminas": "laminas", "foil": "laminas",
    "cabezal": "cabezal", "head": "cabezal",
    "junta": "junta", "seal": "junta",
    "deposito": "deposito", "tank": "deposito",
    "cesta": "cesta", "bin": "cesta",
    "bolsa": "bolsa", "bag": "bolsa",
    "correa": "correa", "belt": "correa",
    "bomba": "bomba", "pump": "bomba",
    "resistencia": "resistencia",
    "rodamiento": "rodamiento", "bearing": "rodamiento",
    "escobillas": "escobillas",
    "rueda": "rueda", "wheel": "rueda", "tire": "rueda",
    "freno": "freno", "brake": "freno",
    "nuevo": "nuevo",
}


def parse_sku(sku: str) -> Dict[str, str]:
    """Heuristic SKU parsing using token detection."""
    parts = sku.split("-")
    brand = parts[0] if parts else ""
    cat = ""
    for part in parts[1:]:
        if part in KNOWN_CAT_TOKENS:
            cat = KNOWN_CAT_TOKENS[part]
            break
    # Model is everything between brand and the category token
    model_parts = []
    for part in parts[1:]:
        if part in KNOWN_CAT_TOKENS:
            break
        model_parts.append(part)
    model = "-".join(model_parts)
    return {"brand": brand, "model": model, "category": cat, "title": ""}


def diagnose_sku(sku: str, offer: Dict[str, Any]) -> List[str]:
    """Identify potential issues with a SKU's search strategy."""
    issues = []
    parsed = parse_sku(sku)
    cat = parsed["category"]
    model = parsed["model"]
    brand = parsed["brand"]

    if cat in RARE_CATEGORIES:
        issues.append(f"rare_category:{cat}")

    if not model or not any(ch.isdigit() for ch in model):
        issues.append("no_model_token")

    if not brand or len(brand) < 3:
        issues.append("generic_brand")

    # Check if last query was too long
    last_query = str(offer.get("last_search_query") or offer.get("fallback_search_query") or "")
    if len(last_query) > 60:
        issues.append("too_long")

    # Check rejection reasons
    reason = str(offer.get("ai_validation_reason") or "")
    if "model" in reason.lower() and "mismatch" in reason.lower():
        issues.append("model_mismatch_rejected")
    if "category" in reason.lower() and "mismatch" in reason.lower():
        issues.append("category_mismatch_rejected")

    return issues


def suggest_override(sku: str, offer: Dict[str, Any], parsed: Dict[str, str]) -> str:
    """Suggest a query override for catalog_skus.yaml."""
    brand = parsed["brand"].replace("-", " ").title()
    model = parsed["model"].replace("-", " ")
    cat = parsed["category"]

    # Build a cleaner English query
    cat_en = {
        "bateria": "battery",
        "filtro": "hepa filter",
        "cepillo": "roller brush",
        "cargador": "charger adapter",
        "soporte": "wall mount holder",
        "accesorios": "accessory kit",
        "laminas": "shaver foil",
        "cabezal": "replacement head",
        "junta": "rubber seal gasket",
        "deposito": "water tank",
        "cesta": "dust bin",
        "bolsa": "dust bag",
        "bomba": "drain pump",
        "resistencia": "heating element",
        "rodamiento": "drum bearing",
        "escobillas": "carbon brush motor",
        "correa": "drive belt",
        "rueda": "wheel tire",
        "freno": "brake pad",
    }.get(cat, cat)

    # Extract model tokens (alphanumeric identifiers)
    tokens = re.findall(r"[a-z]*\d+[a-z0-9]*", model, re.IGNORECASE)
    model_id = " ".join(tokens[:2]) if tokens else model

    return f"{brand} {model_id} {cat_en}"


def report_summary(offers_doc: Dict[str, Any]) -> None:
    offers = offers_doc.get("offers", {})
    total = len(offers)
    needs_url = sum(1 for o in offers.values() if isinstance(o, dict) and o.get("needs_url") is True)
    orphaned = sum(1 for o in offers.values() if isinstance(o, dict) and o.get("orphaned") is True)
    has_real_url = sum(
        1 for o in offers.values()
        if isinstance(o, dict)
        and not o.get("needs_url")
        and not o.get("orphaned")
        and o.get("url", "").startswith("http")
        and o.get("url") != "https://s.click.aliexpress.com/e/_c3VfQRLt"
    )

    print("=" * 60)
    print("OFERTAS AUDIT REPORT")
    print("=" * 60)
    print(f"  Total SKUs:        {total}")
    print(f"  With real URL:     {has_real_url} ({100*has_real_url/max(total,1):.1f}%)")
    print(f"  Needs URL:         {needs_url} ({100*needs_url/max(total,1):.1f}%)")
    print(f"  Orphaned:          {orphaned} ({100*orphaned/max(total,1):.1f}%)")
    print(f"  Active (non-orph): {total - orphaned}")
    print()


def report_by_category(offers_doc: Dict[str, Any]) -> None:
    offers = offers_doc.get("offers", {})
    cat_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "ok": 0, "needs": 0})

    for sku, offer in offers.items():
        if not isinstance(offer, dict) or offer.get("orphaned"):
            continue
        parsed = parse_sku(sku)
        cat = parsed["category"]
        cat_stats[cat]["total"] += 1
        if offer.get("needs_url") or offer.get("url", "") == "https://s.click.aliexpress.com/e/_c3VfQRLt":
            cat_stats[cat]["needs"] += 1
        else:
            cat_stats[cat]["ok"] += 1

    print("\nCOVERAGE BY CATEGORY:")
    print("-" * 60)
    print(f"{'Category':<15} {'Total':<8} {'OK':<8} {'Needs URL':<10} {'Coverage':<10}")
    print("-" * 60)
    for cat in sorted(cat_stats, key=lambda c: cat_stats[c]["ok"] / max(cat_stats[c]["total"], 1)):
        s = cat_stats[cat]
        coverage = 100 * s["ok"] / max(s["total"], 1)
        flag = " <<<" if coverage < 50 else ""
        print(f"{cat:<15} {s['total']:<8} {s['ok']:<8} {s['needs']:<10} {coverage:>5.1f}%{flag}")
    print()


def report_by_brand(offers_doc: Dict[str, Any], top: int = 15) -> None:
    offers = offers_doc.get("offers", {})
    brand_stats: Dict[str, Dict[str, int]] = defaultdict(lambda: {"total": 0, "ok": 0, "needs": 0})

    for sku, offer in offers.items():
        if not isinstance(offer, dict) or offer.get("orphaned"):
            continue
        parsed = parse_sku(sku)
        brand = parsed["brand"]
        brand_stats[brand]["total"] += 1
        if offer.get("needs_url") or offer.get("url", "") == "https://s.click.aliexpress.com/e/_c3VfQRLt":
            brand_stats[brand]["needs"] += 1
        else:
            brand_stats[brand]["ok"] += 1

    print("\nWORST COVERAGE BY BRAND (bottom 15):")
    print("-" * 60)
    print(f"{'Brand':<20} {'Total':<8} {'OK':<8} {'Needs':<8} {'Coverage':<10}")
    print("-" * 60)
    sorted_brands = sorted(brand_stats, key=lambda b: brand_stats[b]["ok"] / max(brand_stats[b]["total"], 1))
    for brand in sorted_brands[:top]:
        s = brand_stats[brand]
        coverage = 100 * s["ok"] / max(s["total"], 1)
        print(f"{brand:<20} {s['total']:<8} {s['ok']:<8} {s['needs']:<8} {coverage:>5.1f}%")
    print()


def report_issues(offers_doc: Dict[str, Any], top: int = 20) -> None:
    offers = offers_doc.get("offers", {})
    issue_counts: Counter = Counter()
    issue_examples: Dict[str, List[str]] = defaultdict(list)

    for sku, offer in offers.items():
        if not isinstance(offer, dict) or offer.get("orphaned"):
            continue
        if not offer.get("needs_url") and offer.get("url", "") != "https://s.click.aliexpress.com/e/_c3VfQRLt":
            continue
        issues = diagnose_sku(sku, offer)
        for issue in issues:
            issue_counts[issue] += 1
            if len(issue_examples[issue]) < 3:
                issue_examples[issue].append(sku)

    print("\nFAILURE PATTERN ANALYSIS:")
    print("-" * 60)
    for issue, count in issue_counts.most_common(top):
        examples = ", ".join(issue_examples[issue][:2])
        print(f"  {issue:<35} {count:>5} hits  (e.g. {examples})")
    print()


def report_suggested_fixes(offers_doc: Dict[str, Any], top: int = 30) -> None:
    offers = offers_doc.get("offers", {})
    suggestions: List[Tuple[str, str]] = []

    for sku, offer in offers.items():
        if not isinstance(offer, dict) or offer.get("orphaned"):
            continue
        if not offer.get("needs_url") and offer.get("url", "") != "https://s.click.aliexpress.com/e/_c3VfQRLt":
            continue
        parsed = parse_sku(sku)
        if parsed["category"] not in RARE_CATEGORIES:
            suggestion = suggest_override(sku, offer, parsed)
            suggestions.append((sku, suggestion))

    print(f"\nSUGGESTED QUERY OVERRIDES (top {top} for catalog_skus.yaml):")
    print("-" * 60)
    print("# Add these to data/catalog_skus.yaml under sku_overrides:")
    print("sku_overrides:")
    for sku, query in suggestions[:top]:
        print(f"  {sku}:")
        print(f"    query: \"{query}\"")
    print()


def report_stale(offers_doc: Dict[str, Any], days: int = 30) -> None:
    offers = offers_doc.get("offers", {})
    cutoff = (datetime.now().date() - timedelta(days=days)).isoformat()
    stale = []

    for sku, offer in offers.items():
        if not isinstance(offer, dict) or offer.get("orphaned"):
            continue
        updated = str(offer.get("updated_at") or "0000-00-00")
        if updated < cutoff:
            stale.append((sku, updated))

    stale.sort(key=lambda x: x[1])
    print(f"\nSTALE SKUS (not updated in {days}+ days): {len(stale)}")
    if stale[:10]:
        print("  Oldest:")
        for sku, date in stale[:10]:
            print(f"    {sku}: last updated {date}")
    print()


def main() -> None:
    ap = argparse.ArgumentParser(description="Audit ofertas.json coverage and suggest improvements")
    ap.add_argument("--top", type=int, default=20, help="Number of entries to show in reports")
    ap.add_argument("--by-category", action="store_true", help="Show coverage breakdown by category")
    ap.add_argument("--by-brand", action="store_true", help="Show coverage breakdown by brand")
    ap.add_argument("--suggest-fixes", action="store_true", help="Suggest query overrides for failing SKUs")
    ap.add_argument("--stale-days", type=int, default=30, help="Days threshold for stale report")
    ap.add_argument("--all", action="store_true", help="Run all reports")
    args = ap.parse_args()

    offers_doc = load_offers()
    report_summary(offers_doc)

    if args.all or args.by_category:
        report_by_category(offers_doc)

    if args.all or args.by_brand:
        report_by_brand(offers_doc, top=args.top)

    if args.all or not (args.by_category or args.by_brand or args.suggest_fixes):
        report_issues(offers_doc, top=args.top)
        report_stale(offers_doc, days=args.stale_days)

    if args.all or args.suggest_fixes:
        report_suggested_fixes(offers_doc, top=args.top)


if __name__ == "__main__":
    main()
