# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from tools import sync_ofertas as so


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sku", required=True)
    ap.add_argument("--lang", default="EN")
    args = ap.parse_args()

    sku_ctx = so.sku_records_from_verticals(so.available_verticals())
    ctx = sku_ctx.get(args.sku)
    if not ctx:
        raise SystemExit(f"SKU no encontrado: {args.sku}")

    offers_doc = so.load_yaml(so.OFFERS)
    offers = offers_doc.get("offers") if isinstance(offers_doc.get("offers"), dict) else {}
    obj = so.ensure_offer_obj(offers.get(args.sku))

    brand = (ctx.get("brand") or "").lower()
    model = (ctx.get("model") or "").lower() or args.sku.lower()
    category = (ctx.get("category") or "")
    part_terms = so.cat_part_terms(category)
    if category == "nuevo":
        vertical = ctx.get("vertical") or ""
        vert_terms = so.VERTICAL_REQUIRED_TERMS.get(vertical, [])
        if vert_terms:
            part_terms = vert_terms

    must_not_default = so.cat_negative_terms(category)
    query, must_include, must_not_override, model_tokens_override = so.merge_overrides(
        sku=args.sku,
        ctx=ctx,
        offers_obj=obj,
    )
    must_not_combined = [*must_not_default, *must_not_override]
    effective_model_tokens = model_tokens_override[:] if model_tokens_override else so.model_tokens_from_ctx(model)
    strict_anchor_terms = so.extract_strict_anchor_terms(ctx, must_include, effective_model_tokens)
    specific_item_terms = so.extract_specific_item_terms(ctx, must_include, effective_model_tokens)
    kws = so.build_search_keywords(ctx, query, must_include)

    print("SKU:", args.sku)
    print("QUERY:", kws[0] if kws else query)
    print("BRAND:", brand)
    print("MODEL TOKENS:", effective_model_tokens)
    print("PART TERMS:", part_terms)
    print("MUST INCLUDE:", must_include)
    print("MUST NOT:", must_not_combined)
    print("STRICT ANCHORS:", strict_anchor_terms)
    print("SPECIFIC TERMS:", specific_item_terms)

    keyword = kws[0] if kws else query
    resp = so.product_query(keyword, lang=args.lang, page_no=1, use_cache=False)
    prods = so.extract_products(resp)
    print("RESULTS:", len(prods))

    for i, p in enumerate(prods[:12], 1):
        title = str(p.get("product_title") or "")
        tt = so.nrm(title)
        has_brand = so.title_has_required_brand(title, brand)
        has_model = so.title_has_required_model(tt, effective_model_tokens)
        has_part = any(pt in tt for pt in part_terms) if part_terms else True
        has_include = so.contains_all(tt, must_include) if must_include else True
        has_not = so.contains_any(tt, must_not_combined) if must_not_combined else False
        has_strict = so.count_anchor_hits(title, strict_anchor_terms)
        has_specific = so.count_anchor_hits(title, specific_item_terms)
        print(f"\n[{i:02d}] {title}")
        print(
            "  "
            f"bad={so.looks_bad(tt)} deceptive={so.is_deceptive_title(title, category)} "
            f"brand={has_brand} model={has_model} part={has_part} "
            f"include={has_include} must_not={has_not} "
            f"strict_hits={has_strict} specific_hits={has_specific}"
        )


if __name__ == "__main__":
    main()
