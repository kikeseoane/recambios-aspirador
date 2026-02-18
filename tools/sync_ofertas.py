# tools/sync_ofertas.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import hmac
import json
import os
import re
import time
from datetime import datetime
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import requests
import yaml

try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


# =========================
# Paths / constants
# =========================
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

SHIP_TO = (os.getenv("ALI_SHIP_TO") or "ES").strip()
CURRENCY = (os.getenv("ALI_CURRENCY") or "EUR").strip()
PAGE_SIZE = int((os.getenv("ALI_PAGE_SIZE") or "50").strip() or "50")

CACHE_DIR = ROOT / "data" / ".cache_aliexpress"
CACHE_TTL_SECONDS = int((os.getenv("ALI_CACHE_TTL") or str(7 * 24 * 3600)).strip() or str(7 * 24 * 3600))
RATE_SLEEP_SECONDS = float((os.getenv("ALI_RATE_SLEEP") or "0.35").strip() or "0.35")

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


def ensure_list_str(x: Any) -> List[str]:
    if not isinstance(x, list):
        return []
    out: List[str] = []
    for it in x:
        if it is None:
            continue
        s = str(it).strip()
        if s:
            out.append(s)
    return out


def normalize(s: str) -> str:
    return " ".join((s or "").strip().split())


def nrm(s: str) -> str:
    return " ".join((s or "").lower().split())


# =========================
# Catalog parsing: SKU → context (+ overrides)
# =========================
def guess_brand_name(brand_key: str, brand_obj: dict) -> str:
    return normalize(str(brand_obj.get("name") or brand_obj.get("title") or brand_key))


def guess_model_name(model_obj: dict) -> str:
    return normalize(str(model_obj.get("name") or model_obj.get("title") or model_obj.get("model") or ""))


def guess_item_title(item_obj: dict) -> str:
    return normalize(str(item_obj.get("title") or item_obj.get("name") or item_obj.get("label") or ""))


def sku_records_from_catalog(catalog: dict) -> Dict[str, Dict[str, Any]]:
    """
    Devuelve: sku -> {
      brand, model, category, item_title,
      query?, must_include?, must_not_include?, model_tokens?
    }
    """
    out: Dict[str, Dict[str, Any]] = {}
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
                    sku_s = str(sku).strip()
                    if not sku_s:
                        continue

                    out[sku_s] = {
                        "brand": brand_name,
                        "model": model_name,
                        "category": normalize(str(cat)),
                        "item_title": guess_item_title(it),
                        # overrides opcionales en el catálogo:
                        "query": normalize(str(it.get("query") or "")),
                        "must_include": ensure_list_str(it.get("must_include")),
                        "must_not_include": ensure_list_str(it.get("must_not_include")),
                        "model_tokens": [nrm(x) for x in ensure_list_str(it.get("model_tokens"))],
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


def cache_clear() -> None:
    if not CACHE_DIR.exists():
        return
    for p in CACHE_DIR.rglob("*"):
        if p.is_file():
            try:
                p.unlink()
            except Exception:
                pass
    # intenta borrar directorios vacíos
    try:
        for d in sorted([x for x in CACHE_DIR.rglob("*") if x.is_dir()], key=lambda x: len(str(x)), reverse=True):
            try:
                d.rmdir()
            except Exception:
                pass
        CACHE_DIR.rmdir()
    except Exception:
        pass


def ali_call_flat(method: str, biz_params: Dict[str, Any], use_cache: bool = True) -> Dict[str, Any]:
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
    if use_cache:
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

    if use_cache:
        cache_set(ck, data)

    time.sleep(RATE_SLEEP_SECONDS)
    return data


def product_query(keyword: str, lang: str = "EN", page_no: int = 1, use_cache: bool = True) -> Dict[str, Any]:
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
    return ali_call_flat("aliexpress.affiliate.product.query", biz, use_cache=use_cache)


def extract_products(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        return resp["aliexpress_affiliate_product_query_response"]["resp_result"]["result"]["products"]["product"]
    except Exception:
        return []


# =========================
# Relevancy / scoring
# =========================
BANNED_TITLE = [
    "women", "woman", "spa", "sleep", "eye mask", "mask", "gift", "dress",
    "skincare", "beauty", "cosmetic", "lingerie", "bikini", "jewelry",
    "t-shirt", "shorts", "matching sets", "makeup", "perfume",
]

CATEGORY_PART_TERMS = {
    "bateria": ["battery", "bateria", "pack", "rechargeable", "22.2v", "21.6v", "25.2v", "click-in", "click in"],
    "filtro": ["filter", "filtro", "hepa", "rear", "pre", "post"],
    "cargador": ["charger", "cargador", "adapter", "adaptador", "power", "ac adapter"],
    "cepillo": ["brush", "cepillo", "head", "roller", "rodillo", "torque", "drive"],
    "soporte": ["wall", "mount", "holder", "dock", "stand", "bracket", "storage", "rack", "base"],
    "accesorios": ["accessory", "accessories", "kit", "crevice", "tool", "brush", "nozzle", "boquilla"],
}

CATEGORY_NEGATIVE_TERMS = {
    # trampas típicas
    "soporte": ["trigger", "switch", "button", "pcb", "board", "handle", "motor"],
    "bateria": ["trigger", "switch", "button", "filter", "charger", "dock", "wall mount"],
    "filtro": ["battery", "charger", "trigger", "switch", "button"],
    "cargador": ["battery", "filter", "trigger", "switch", "button"],
    "cepillo": ["battery", "filter", "charger", "trigger", "switch", "button"],
}

MODEL_TOKEN_RE = re.compile(r"\b(v\d{1,2}|sv\d{2}|dc\d{2,3})\b", re.IGNORECASE)


def looks_bad(title: str) -> bool:
    t = nrm(title)
    return any(b in t for b in BANNED_TITLE)


def cat_part_terms(cat: str) -> List[str]:
    c = nrm(cat)
    for k, terms in CATEGORY_PART_TERMS.items():
        if k in c:
            return terms
    # fallback heurístico
    if "bater" in c:
        return CATEGORY_PART_TERMS["bateria"]
    if "filt" in c:
        return CATEGORY_PART_TERMS["filtro"]
    if "carg" in c or "charger" in c:
        return CATEGORY_PART_TERMS["cargador"]
    if "cepi" in c or "brush" in c:
        return CATEGORY_PART_TERMS["cepillo"]
    if "soport" in c or "mount" in c or "dock" in c:
        return CATEGORY_PART_TERMS["soporte"]
    return ["replacement", "spare", "parts", "compatible"]


def cat_negative_terms(cat: str) -> List[str]:
    c = nrm(cat)
    for k, terms in CATEGORY_NEGATIVE_TERMS.items():
        if k in c:
            return terms
    if "soport" in c or "mount" in c or "dock" in c:
        return CATEGORY_NEGATIVE_TERMS.get("soporte", [])
    return []


def model_tokens_from_ctx(model: str) -> List[str]:
    t = nrm(model)
    tokens = [m.group(1).lower() for m in MODEL_TOKEN_RE.finditer(t)]
    # heurística Dyson V11 -> SV14
    if "v11" in tokens and "sv14" not in tokens:
        tokens.append("sv14")
    return list(dict.fromkeys(tokens))


def title_has_required_model(title: str, required: List[str]) -> bool:
    tt = nrm(title)
    return any(tok in tt for tok in required)


def count_distinct_models_in_title(title: str) -> int:
    tt = nrm(title)
    toks = set(m.group(1).lower() for m in MODEL_TOKEN_RE.finditer(tt))
    return len(toks)


def model_mismatch_penalty(title: str, required: List[str]) -> float:
    tt = nrm(title)
    toks = set(m.group(1).lower() for m in MODEL_TOKEN_RE.finditer(tt))
    if not toks:
        return 0.0
    if required and not any(r in toks for r in required):
        return 999.0
    extra = [t for t in toks if t not in required]
    return float(len(extra)) * 3.5


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


def contains_all(title: str, must: List[str]) -> bool:
    tt = nrm(title)
    for m in must:
        if nrm(m) not in tt:
            return False
    return True


def contains_any(title: str, bad: List[str]) -> bool:
    tt = nrm(title)
    return any(nrm(b) in tt for b in bad)


def score_product(
    title: str,
    p: Dict[str, Any],
    must_brand: str,
    part_terms: List[str],
    req_models: List[str],
) -> float:
    t = nrm(title)
    s = 0.0

    if must_brand and must_brand in t:
        s += 6.0

    if req_models:
        if not title_has_required_model(t, req_models):
            return -1e9
        s -= model_mismatch_penalty(t, req_models)

    matches = 0
    for pt in part_terms:
        if pt in t:
            matches += 1
            s += 2.6
    if matches == 0:
        return -1e9

    if "compatible" in t or "replacement" in t or "spare" in t:
        s += 1.2

    n_models = count_distinct_models_in_title(t)
    if n_models >= 4:
        s -= 5.0

    s += get_orders(p) * 0.015
    s += get_commission_rate(p) * 0.4

    return s


def build_keyword(ctx: Dict[str, Any]) -> str:
    brand = normalize(str(ctx.get("brand") or ""))
    model = normalize(str(ctx.get("model") or ""))
    cat = normalize(str(ctx.get("category") or ""))
    item_title = normalize(str(ctx.get("item_title") or ""))
    parts = [p for p in (brand, model, cat, item_title) if p]
    kw = " ".join(parts)
    return kw[:120]


def merge_overrides(
    sku: str,
    ctx: Dict[str, Any],
    offers_obj: Dict[str, Any],
) -> Tuple[str, List[str], List[str], List[str]]:
    """
    Devuelve: (query, must_include, must_not_include, model_tokens_override)
    Prioridad:
      1) overrides en aspiradores.yaml (ctx)
      2) overrides en ofertas.yaml (offers_obj)
      3) defaults por categoría (solo para must_not)
    """
    # query
    query = normalize(str(ctx.get("query") or ""))
    if not query:
        query = normalize(str(offers_obj.get("query") or ""))

    # must include / not include
    must_include = ensure_list_str(ctx.get("must_include")) or ensure_list_str(offers_obj.get("must_include"))
    must_not_include = ensure_list_str(ctx.get("must_not_include")) or ensure_list_str(offers_obj.get("must_not_include"))

    # model tokens
    model_tokens = [nrm(x) for x in (ensure_list_str(ctx.get("model_tokens")) or ensure_list_str(offers_obj.get("model_tokens")))]

    return query, must_include, must_not_include, model_tokens


def pick_best_promotion_link(
    keyword: str,
    must_brand: str,
    model_hint: str,
    part_terms: List[str],
    must_include: List[str],
    must_not_include: List[str],
    model_tokens_override: List[str],
    use_cache: bool,
) -> Optional[str]:
    req_models = model_tokens_override[:] if model_tokens_override else model_tokens_from_ctx(model_hint)

    for lang in ("EN", "ES"):
        resp = product_query(keyword, lang=lang, use_cache=use_cache)
        prods = extract_products(resp)
        if not prods:
            continue

        candidates = []
        for p in prods:
            title = p.get("product_title") or ""
            if not title:
                continue

            tt = nrm(title)
            if looks_bad(tt):
                continue

            # hard: modelo
            if req_models and not title_has_required_model(tt, req_models):
                continue

            # hard: categoría (alguna señal de la pieza)
            if not any(pt in tt for pt in part_terms):
                continue

            # hard: must_include
            if must_include and not contains_all(tt, must_include):
                continue

            # hard: must_not_include
            if must_not_include and contains_any(tt, must_not_include):
                continue

            candidates.append(p)

        if not candidates:
            continue

        candidates.sort(
            key=lambda p: score_product(
                p.get("product_title") or "",
                p,
                must_brand=nrm(must_brand),
                part_terms=part_terms,
                req_models=req_models,
            ),
            reverse=True,
        )

        best = candidates[0]
        url = best.get("promotion_link") or best.get("product_detail_url")
        if url:
            return str(url).strip()

    return None


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Sincroniza data/ofertas.yaml desde data/aspiradores.yaml + AliExpress API")
    ap.add_argument("--clear-cache", action="store_true", help="Borra el cache y termina")
    ap.add_argument("--no-cache", action="store_true", help="Ignora cache (hace llamadas frescas)")
    ap.add_argument("--only-sku", action="append", default=[], help="Solo procesa este SKU (puedes repetir)")
    ap.add_argument("--force", action="store_true", help="Fuerza lookup incluso si ya hay URL no-default")
    return ap.parse_args()


# =========================
# MAIN
# =========================
def main() -> None:
    args = parse_args()

    if args.clear_cache:
        cache_clear()
        print(f"OK: cache borrado -> {CACHE_DIR}")
        return

    use_cache = not args.no_cache

    catalog = load_yaml(CATALOG)
    sku_ctx = sku_records_from_catalog(catalog)
    want = set(sku_ctx.keys())

    offers_doc = load_yaml(OFFERS)
    offers = offers_doc.get("offers")
    if not isinstance(offers, dict):
        offers = {}

    # filtra SKUs si se pide
    only = [str(x).strip() for x in args.only_sku if str(x).strip()]
    if only:
        want = set([s for s in only if s in sku_ctx])

    added = 0
    updated = 0
    orphaned = 0
    un_orphaned = 0
    changed_urls_to_default = 0
    filled_from_aliexpress = 0

    today = datetime.now().date().isoformat()

    for sku in sorted(want):
        ctx = sku_ctx.get(sku) or {}
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

        url_now = str(obj.get("url") or "").strip()
        needs_lookup = (
            args.force
            or obj.get("needs_url") is True
            or is_placeholder(url_now, PLACEHOLDER_URL)
            or (url_now == DEFAULT_URL)
            or (url_now == "")
        )

        if needs_lookup:
            brand = (ctx.get("brand") or "").lower()
            model = (ctx.get("model") or "").lower() or sku.lower()
            category = (ctx.get("category") or "")
            part_terms = cat_part_terms(category)

            # negativos por categoría (default)
            must_not_default = cat_negative_terms(category)

            # overrides desde catálogo/ofertas.yaml
            query, must_include, must_not_override, model_tokens_override = merge_overrides(
                sku=sku,
                ctx=ctx,
                offers_obj=obj,
            )
            must_not_combined = [*must_not_default, *must_not_override]

            # keywords fallback
            kw0 = query if query else build_keyword(ctx)
            kws = [k for k in [
                kw0,
                " ".join([str(ctx.get("brand") or ""), str(ctx.get("model") or ""), str(ctx.get("category") or "")]).strip(),
                " ".join([str(ctx.get("brand") or ""), str(ctx.get("model") or "")]).strip(),
            ] if k]

            found = None
            for kw in kws:
                found = pick_best_promotion_link(
                    keyword=kw,
                    must_brand=brand,
                    model_hint=model,
                    part_terms=part_terms,
                    must_include=must_include,
                    must_not_include=must_not_combined,
                    model_tokens_override=model_tokens_override,
                    use_cache=use_cache,
                )
                if found:
                    break

            if found:
                obj["url"] = found
                obj.pop("needs_url", None)
                obj["updated_at"] = today
                filled_from_aliexpress += 1
            else:
                # no pongas algo incorrecto: deja default + needs_url
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

    # marcar huérfanos (solo si no estamos en modo only-sku)
    if not only:
        for sku, o in list(offers.items()):
            if sku not in set(sku_ctx.keys()):
                o = ensure_offer_obj(o)
                if o.get("orphaned") is not True:
                    o["orphaned"] = True
                    offers[sku] = o
                    orphaned += 1

    dump_yaml(OFFERS, {"offers": offers})

    print("OK: sync_ofertas (AliExpress autolinks + catalog overrides + cache flags)")
    print(f"  Cache:                 {'ON' if use_cache else 'OFF'} (TTL={CACHE_TTL_SECONDS}s)")
    print(f"  SKUs en catálogo:       {len(set(sku_ctx.keys()))}")
    print(f"  Procesados ahora:       {len(want)}")
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
