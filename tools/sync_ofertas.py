from __future__ import annotations

import os
import time
import json
import hmac
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional

import requests
import yaml

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "data" / "aspiradores.yaml"
OFFERS = ROOT / "data" / "ofertas.yaml"

DEFAULT_URL = "https://s.click.aliexpress.com/e/_c3VfQRLt"

PLACEHOLDER_URL = "RELLENAR_URL_ALIEXPRESS"
PLACEHOLDER_EST = "RELLENAR_COSTE_ESTIMADO"
PLACEHOLDER_BADGE = "RELLENAR_BADGES"

# =========================
# AliExpress ENV
# =========================
APP_KEY = (os.getenv("ALI_APP_KEY") or "").strip()
APP_SECRET = (os.getenv("ALI_APP_SECRET") or "").strip()
TRACKING_ID = (os.getenv("ALI_TRACKING_ID") or "").strip()
API_URL = (os.getenv("ALI_API_URL") or "https://api-sg.aliexpress.com/sync").strip()

SHIP_TO = "ES"
CURRENCY = "EUR"
PAGE_SIZE = 50

CACHE_DIR = ROOT / "data" / ".cache_aliexpress"
CACHE_TTL_SECONDS = 7 * 24 * 3600  # 7 días
RATE_SLEEP_SECONDS = 0.35

if not APP_KEY or not APP_SECRET:
    raise SystemExit("Faltan variables de entorno: ALI_APP_KEY y/o ALI_APP_SECRET")


# =========================
# YAML helpers
# =========================
def load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def dump_yaml(path: Path, data: dict) -> None:
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True).strip() + "\n"
    path.write_text(text, encoding="utf-8")


def is_empty(value: object | None) -> bool:
    return value is None or str(value).strip() == ""


def is_placeholder(value: object | None, placeholder: str) -> bool:
    if is_empty(value):
        return True
    return str(value).strip() == placeholder


def ensure_offer_obj(existing: object | None) -> dict:
    return existing if isinstance(existing, dict) else {}


# =========================
# Catalog parsing: SKU → context
# =========================
def normalize(s: str) -> str:
    return " ".join((s or "").strip().split())


def guess_brand_name(brand_key: str, brand_obj: dict) -> str:
    return normalize(str(brand_obj.get("name") or brand_obj.get("title") or brand_key))


def guess_model_name(model_obj: dict) -> str:
    return normalize(str(model_obj.get("name") or model_obj.get("title") or model_obj.get("model") or ""))


def guess_item_name(item_obj: dict) -> str:
    return normalize(str(item_obj.get("name") or item_obj.get("title") or item_obj.get("label") or ""))


def sku_records_from_catalog(catalog: dict) -> Dict[str, Dict[str, str]]:
    """
    Devuelve: sku -> {brand, model, category, item_name}
    """
    out: Dict[str, Dict[str, str]] = {}
    brands = catalog.get("brands") or {}
    if not isinstance(brands, dict):
        return out

    for brand_key, brand in brands.items():
        brand = brand or {}
        brand_name = guess_brand_name(str(brand_key), brand)

        models = brand.get("models") or []
        if not isinstance(models, list):
            continue

        for m in models:
            m = m or {}
            model_name = guess_model_name(m)

            rec = m.get("recambios") or {}
            if not isinstance(rec, dict):
                continue

            for cat, items in rec.items():
                if not items or not isinstance(items, list):
                    continue

                for it in items:
                    it = it or {}
                    sku = it.get("sku")
                    if not sku:
                        continue
                    sku = str(sku).strip()
                    item_name = guess_item_name(it)

                    out[sku] = {
                        "brand": brand_name,
                        "model": model_name,
                        "category": normalize(str(cat)),
                        "item_name": item_name,
                    }

    return out


# =========================
# AliExpress signing + cache + call
# =========================
def sign_params(params: Dict[str, Any], secret: str) -> str:
    items = sorted((k, str(v)) for k, v in params.items() if k != "sign" and v is not None and v != "")
    base = "".join([k + v for k, v in items])
    digest = hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest.upper()


def cache_key(method: str, params: Dict[str, Any]) -> str:
    blob = json.dumps({"method": method, "params": params}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def cache_get(key: str) -> Optional[Dict[str, Any]]:
    path = CACHE_DIR / f"{key}.json"
    if not path.exists():
        return None
    try:
        st = path.stat()
        if (time.time() - st.st_mtime) > CACHE_TTL_SECONDS:
            return None
        return json.loads(path.read_text(encoding="utf-8"))
    except Exception:
        return None


def cache_set(key: str, data: Dict[str, Any]) -> None:
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    path = CACHE_DIR / f"{key}.json"
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2), encoding="utf-8")


def ali_call_flat(method: str, biz_params: Dict[str, Any]) -> Dict[str, Any]:
    params: Dict[str, Any] = {
        "app_key": APP_KEY,
        "method": method,
        "timestamp": str(int(time.time() * 1000)),
        "sign_method": "hmac-sha256",
        "format": "json",
        "v": "2.0",
    }
    for k, v in biz_params.items():
        if v is None:
            continue
        params[k] = str(v)

    params["sign"] = sign_params(params, APP_SECRET)

    ck = cache_key(method, params)
    cached = cache_get(ck)
    if cached is not None:
        return cached

    r = requests.post(API_URL, data=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if "error_response" in data:
        er = data["error_response"]
        raise RuntimeError(
            f"AliExpress error_response: code={er.get('code')} msg={er.get('msg')} sub_msg={er.get('sub_msg')}"
        )

    cache_set(ck, data)
    time.sleep(RATE_SLEEP_SECONDS)
    return data


def product_query(keyword: str, lang: str = "EN", page_no: int = 1) -> Dict[str, Any]:
    biz = {
        "keywords": keyword,
        "page_no": page_no,
        "page_size": PAGE_SIZE,
        "target_currency": CURRENCY,
        "target_language": lang,
        "ship_to_country": SHIP_TO,
    }
    if TRACKING_ID:
        biz["tracking_id"] = TRACKING_ID
    return ali_call_flat("aliexpress.affiliate.product.query", biz)


def extract_products(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        return resp["aliexpress_affiliate_product_query_response"]["resp_result"]["result"]["products"]["product"]
    except Exception:
        return []


# =========================
# Relevancy / scoring
# =========================
def nrm(s: str) -> str:
    return " ".join((s or "").lower().split())


BANNED_TITLE = [
    "women", "woman", "spa", "sleep", "eye mask", "mask", "gift", "dress",
    "skincare", "beauty", "cosmetic", "lingerie", "bikini", "jewelry",
    "t-shirt", "shorts", "matching sets", "makeup", "perfume",
]

CATEGORY_PART_TERMS = {
    "bateria": ["battery", "bateria", "pack", "rechargeable", "22.2v", "21.6v", "25.2v"],
    "filtro": ["filter", "filtro", "hepa", "rear", "pre"],
    "cargador": ["charger", "cargador", "adapter", "adaptador", "power", "ac adapter"],
    "cepillo": ["brush", "cepillo", "head", "roller", "rodillo", "torque"],
}


def cat_part_terms(cat: str) -> List[str]:
    c = nrm(cat)
    if "bater" in c:
        return CATEGORY_PART_TERMS["bateria"]
    if "filt" in c:
        return CATEGORY_PART_TERMS["filtro"]
    if "carg" in c or "charger" in c:
        return CATEGORY_PART_TERMS["cargador"]
    if "cepi" in c or "brush" in c:
        return CATEGORY_PART_TERMS["cepillo"]
    return ["replacement", "spare", "parts", "compatible"]


def get_orders(p: Dict[str, Any]) -> int:
    raw = p.get("lastest_volume")
    if raw is None:
        raw = p.get("sale_volume") or p.get("orders") or 0
    try:
        return int(float(str(raw)))
    except Exception:
        return 0


def get_commission_rate(p: Dict[str, Any]) -> float:
    raw = p.get("commission_rate") or p.get("hot_product_commission_rate") or "0"
    s = str(raw).strip().replace("%", "")
    try:
        return float(s)
    except Exception:
        return 0.0


def score_product(title: str, p: Dict[str, Any], must_brand: str, model_hint: str, part_terms: List[str]) -> float:
    t = nrm(title)
    s = 0.0
    if must_brand and must_brand in t:
        s += 6.0
    if model_hint and model_hint in t:
        s += 7.0
    for pt in part_terms:
        if pt in t:
            s += 2.5
    if "compatible" in t or "replacement" in t or "spare" in t:
        s += 1.2
    s += get_orders(p) * 0.02
    s += get_commission_rate(p) * 0.5
    return s


def looks_bad(title: str) -> bool:
    t = nrm(title)
    return any(b in t for b in BANNED_TITLE)


def pick_best_promotion_link(keyword: str, must_brand: str, model_hint: str, part_terms: List[str]) -> Optional[str]:
    for lang in ("EN", "ES"):
        resp = product_query(keyword, lang=lang)
        prods = extract_products(resp)
        if not prods:
            continue

        candidates = []
        for p in prods:
            title = p.get("product_title") or ""
            if not title:
                continue
            if looks_bad(title):
                continue
            tt = nrm(title)
            if not any(pt in tt for pt in part_terms):
                continue
            candidates.append(p)

        if not candidates:
            continue

        candidates.sort(
            key=lambda p: score_product(
                p.get("product_title") or "",
                p,
                must_brand=nrm(must_brand),
                model_hint=nrm(model_hint),
                part_terms=part_terms,
            ),
            reverse=True,
        )
        best = candidates[0]
        url = best.get("promotion_link") or best.get("product_detail_url")
        if url:
            return str(url).strip()
    return None


# =========================
# Keyword builder (por SKU)
# =========================
def build_keyword(ctx: Dict[str, str]) -> str:
    brand = normalize(ctx.get("brand", ""))
    model = normalize(ctx.get("model", ""))
    cat = normalize(ctx.get("category", ""))
    item = normalize(ctx.get("item_name", ""))
    parts = [p for p in (brand, model, cat, item) if p]
    kw = " ".join(parts)
    return kw[:120]


# =========================
# MAIN
# =========================
def main() -> None:
    catalog = load_yaml(CATALOG)
    sku_ctx = sku_records_from_catalog(catalog)
    want = set(sku_ctx.keys())

    offers_doc = load_yaml(OFFERS)
    offers = offers_doc.get("offers")
    if not isinstance(offers, dict):
        offers = {}

    added = 0
    updated = 0
    orphaned = 0
    un_orphaned = 0
    changed_urls_to_default = 0
    filled_from_aliexpress = 0

    today = datetime.now().date().isoformat()

    for sku in sorted(want):
        prev = offers.get(sku)
        obj = ensure_offer_obj(prev)
        before = dict(obj)

        # placeholders base
        if is_placeholder(obj.get("estimated_price_range"), PLACEHOLDER_EST):
            obj["estimated_price_range"] = PLACEHOLDER_EST

        badges = obj.get("badges")
        if not isinstance(badges, list) or len(badges) == 0:
            obj["badges"] = [PLACEHOLDER_BADGE]

        if obj.get("orphaned") is True:
            obj.pop("orphaned", None)
            un_orphaned += 1

        url_now = obj.get("url")
        needs_lookup = is_placeholder(url_now, PLACEHOLDER_URL) or (str(url_now).strip() == DEFAULT_URL)

        if needs_lookup:
            ctx = sku_ctx.get(sku) or {}
            keyword = build_keyword(ctx)

            must_brand = (ctx.get("brand") or "").lower()
            model_hint = (ctx.get("model") or "").lower() or sku.lower()
            part_terms = cat_part_terms(ctx.get("category") or "")

            kws = [k for k in [
                keyword,
                " ".join([ctx.get("brand", ""), ctx.get("model", ""), ctx.get("category", "")]).strip(),
                " ".join([ctx.get("brand", ""), ctx.get("model", "")]).strip(),
            ] if k]

            found = None
            for kw in kws:
                found = pick_best_promotion_link(
                    keyword=kw,
                    must_brand=must_brand,
                    model_hint=model_hint,
                    part_terms=part_terms,
                )
                if found:
                    break

            if found:
                obj["url"] = found
                obj.pop("needs_url", None)
                obj["updated_at"] = today
                filled_from_aliexpress += 1
            else:
                if obj.get("url") != DEFAULT_URL:
                    obj["url"] = DEFAULT_URL
                    changed_urls_to_default += 1
                obj["needs_url"] = True
        else:
            obj.pop("needs_url", None)

        if sku not in offers:
            offers[sku] = obj
            added += 1
        else:
            offers[sku] = obj
            if before != obj:
                updated += 1

    for sku, obj in list(offers.items()):
        if sku not in want:
            obj = ensure_offer_obj(obj)
            if obj.get("orphaned") is not True:
                obj["orphaned"] = True
                offers[sku] = obj
                orphaned += 1

    dump_yaml(OFFERS, {"offers": offers})

    print("OK: sync_ofertas (AliExpress autolinks)")
    print(f"  SKUs en catálogo:       {len(want)}")
    print(f"  Offers total:           {len(offers)}")
    print(f"  Añadidos:               {added}")
    print(f"  Actualizados:           {updated}")
    print(f"  Rellenados AliExpress:  {filled_from_aliexpress}")
    print(f"  URLs a DEFAULT:         {changed_urls_to_default}")
    print(f"  Rehabilitados:          {un_orphaned}")
    print(f"  Marcados huérfano:      {orphaned}")
    print(f"  Cache dir:              {CACHE_DIR}")


if __name__ == "__main__":
    main()
