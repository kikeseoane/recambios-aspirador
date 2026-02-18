# tools/sync_aliexpress.py
# -*- coding: utf-8 -*-

import os
import time
import hmac
import hashlib
import json
from typing import Any, Dict, List, Tuple, Optional

import requests
import yaml

# Load .env if present
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass


# =========================
# CONFIG (env)
# =========================
APP_KEY = (os.getenv("ALI_APP_KEY") or "").strip()
APP_SECRET = (os.getenv("ALI_APP_SECRET") or "").strip()
TRACKING_ID = (os.getenv("ALI_TRACKING_ID") or "").strip()
API_URL = (os.getenv("ALI_API_URL") or "https://api-sg.aliexpress.com/sync").strip()

if not APP_KEY or not APP_SECRET:
    raise SystemExit("Faltan variables de entorno: ALI_APP_KEY y/o ALI_APP_SECRET")

# Tunables
SHIP_TO = "ES"
CURRENCY = "EUR"
PAGE_SIZE = 50
MAX_RESULTS_PER_CATEGORY = 3
MIN_RESULTS_PER_CATEGORY = 2

CACHE_DIR = "data/.cache_aliexpress"
CACHE_TTL_SECONDS = 7 * 24 * 3600  # 7 días


# =========================
# SIGNING (HMAC-SHA256)
# =========================
def sign_params(params: Dict[str, Any], secret: str) -> str:
    items = sorted((k, str(v)) for k, v in params.items() if k != "sign" and v is not None and v != "")
    base = "".join([k + v for k, v in items])
    digest = hmac.new(secret.encode("utf-8"), base.encode("utf-8"), hashlib.sha256).hexdigest()
    return digest.upper()


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

    r = requests.post(API_URL, data=params, timeout=30)
    r.raise_for_status()
    data = r.json()

    if "error_response" in data:
        er = data["error_response"]
        raise RuntimeError(
            f"AliExpress error_response: code={er.get('code')} msg={er.get('msg')} sub_msg={er.get('sub_msg')}"
        )

    return data


# =========================
# CACHE
# =========================
def cache_key(method: str, params: Dict[str, Any]) -> str:
    blob = json.dumps({"method": method, "params": params}, sort_keys=True, ensure_ascii=False)
    return hashlib.sha1(blob.encode("utf-8")).hexdigest()


def cache_get(key: str) -> Optional[Dict[str, Any]]:
    path = os.path.join(CACHE_DIR, f"{key}.json")
    if not os.path.exists(path):
        return None
    try:
        st = os.stat(path)
        if (time.time() - st.st_mtime) > CACHE_TTL_SECONDS:
            return None
        with open(path, "r", encoding="utf-8") as f:
            return json.load(f)
    except Exception:
        return None


def cache_set(key: str, data: Dict[str, Any]) -> None:
    os.makedirs(CACHE_DIR, exist_ok=True)
    path = os.path.join(CACHE_DIR, f"{key}.json")
    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


# =========================
# AFFILIATE: PRODUCT QUERY
# =========================
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

    k = cache_key("aliexpress.affiliate.product.query", biz)
    cached = cache_get(k)
    if cached is not None:
        return cached

    data = ali_call_flat("aliexpress.affiliate.product.query", biz)
    cache_set(k, data)
    return data


def extract_products(resp: Dict[str, Any]) -> List[Dict[str, Any]]:
    try:
        return resp["aliexpress_affiliate_product_query_response"]["resp_result"]["result"]["products"]["product"]
    except Exception:
        return []


# =========================
# RELEVANCY / RANKING
# =========================
def norm(s: str) -> str:
    return " ".join((s or "").lower().split())


BANNED_TITLE = [
    "women", "woman", "spa", "sleep", "eye mask", "mask", "gift", "dress",
    "skincare", "beauty", "cosmetic", "lingerie", "bikini", "jewelry",
    "t-shirt", "shorts", "matching sets", "makeup", "perfume",
]

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


def candidate(title: str, brand_terms: List[str], model_terms: List[str], part_terms: List[str], relaxed: bool) -> bool:
    t = norm(title)
    if any(b in t for b in BANNED_TITLE):
        return False
    if not any(bt in t for bt in brand_terms):
        # en relajado permitimos sin marca
        if not relaxed:
            return False
    if not any(mt in t for mt in model_terms):
        return False
    if not any(pt in t for pt in part_terms):
        return False
    return True


def score(title: str, p: Dict[str, Any], brand_terms: List[str], model_terms: List[str], part_terms: List[str]) -> float:
    t = norm(title)
    s = 0.0
    for bt in brand_terms:
        if bt in t:
            s += 5.0
    for mt in model_terms:
        if mt in t:
            s += 6.0
    for pt in part_terms:
        if pt in t:
            s += 3.0
    if "compatible" in t or "replacement" in t or "spare" in t:
        s += 1.5
    s += get_orders(p) * 0.02
    s += get_commission_rate(p) * 0.5
    return s


def dedupe(products: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen = set()
    out = []
    for p in products:
        pid = p.get("product_id")
        key = str(pid) if pid is not None else (p.get("product_detail_url") or "")
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(p)
    return out


# =========================
# KEYWORDS PER CATEGORY
# =========================
def config_for(model_slug: str, category: str) -> Tuple[List[str], List[str], List[str], List[str]]:
    brand_terms = ["dyson"]
    # Dyson V11 suele aparecer como SV14 y también se mezcla con SV15/V15 en repuestos
    model_terms = ["v11", "sv14"]

    if category == "bateria":
        part_terms = ["battery", "bateria", "25.2v", "pack"]
        keywords = [
            "dyson v11 battery",
            "dyson v11 25.2v battery",
            "v11 battery for dyson",
            "sv14 battery replacement",
            "bonacell dyson v11 battery",
            "bateria dyson v11 25.2v",
        ]
    elif category == "filtro":
        part_terms = ["filter", "filtro", "hepa", "rear", "pre"]
        keywords = [
            "dyson v11 hepa filter",
            "dyson v11 rear filter",
            "dyson v11 pre filter",
            "sv14 hepa filter",
            "filtro dyson v11 hepa",
        ]
    elif category == "cargador":
        part_terms = ["charger", "cargador", "adapter", "adaptador", "power"]
        keywords = [
            "dyson v11 charger",
            "dyson v11 power adapter",
            "sv14 charger dyson",
            "cargador dyson v11",
        ]
    elif category == "cepillo":
        part_terms = ["brush", "cepillo", "head", "roller", "rodillo"]
        keywords = [
            "dyson v11 brush head",
            "dyson v11 roller brush",
            "dyson v11 torque drive head",
            "cepillo dyson v11",
        ]
    else:
        part_terms = ["replacement", "spare", "parts", "compatible"]
        keywords = [f"dyson v11 {category} replacement", f"dyson sv14 {category}"]

    return brand_terms, model_terms, part_terms, keywords


# =========================
# FIND PRODUCTS WITH FALLBACKS
# =========================
def find_for(model_slug: str, category: str) -> List[Dict[str, Any]]:
    brand_terms, model_terms, part_terms, keywords = config_for(model_slug, category)

    collected: List[Dict[str, Any]] = []

    # idioma fallback: EN primero suele dar mejor recall
    for lang in ["EN", "ES"]:
        for kw in keywords:
            resp = product_query(kw, lang=lang)
            products = extract_products(resp)

            strict_hits = []
            relaxed_hits = []

            for p in products:
                title = p.get("product_title") or ""
                if candidate(title, brand_terms, model_terms, part_terms, relaxed=False):
                    strict_hits.append(p)
                elif candidate(title, brand_terms, model_terms, part_terms, relaxed=True):
                    relaxed_hits.append(p)

            strict_hits = dedupe(strict_hits)
            relaxed_hits = dedupe(relaxed_hits)

            strict_hits.sort(key=lambda p: score(p.get("product_title") or "", p, brand_terms, model_terms, part_terms), reverse=True)
            relaxed_hits.sort(key=lambda p: score(p.get("product_title") or "", p, brand_terms, model_terms, part_terms), reverse=True)

            collected.extend(strict_hits[:MAX_RESULTS_PER_CATEGORY])
            collected = dedupe(collected)

            if len(collected) >= MIN_RESULTS_PER_CATEGORY:
                return collected[:MAX_RESULTS_PER_CATEGORY]

            # si aún no llega, añadimos relajados
            collected.extend(relaxed_hits[:MAX_RESULTS_PER_CATEGORY])
            collected = dedupe(collected)

            if len(collected) >= MIN_RESULTS_PER_CATEGORY:
                return collected[:MAX_RESULTS_PER_CATEGORY]

    return collected[:MAX_RESULTS_PER_CATEGORY]


# =========================
# YAML OUTPUT
# =========================
def product_to_yaml_item(p: Dict[str, Any]) -> Dict[str, Any]:
    # promotion_link ya viene (ideal). Si faltase, cae al detail_url.
    url = p.get("promotion_link") or p.get("product_detail_url") or ""
    return {
        "title": p.get("product_title") or "",
        "price": p.get("target_sale_price") or p.get("sale_price") or "",
        "currency": p.get("target_sale_price_currency") or p.get("sale_price_currency") or "",
        "orders": get_orders(p),
        "commission_rate": p.get("commission_rate") or "",
        "image": p.get("product_main_image_url") or "",
        "url": url,
        "source": "aliexpress",
        "product_id": p.get("product_id"),
        "shop_name": p.get("shop_name") or "",
    }


def write_yaml(path: str, data: Dict[str, Any]) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    with open(path, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, allow_unicode=True, sort_keys=False)


# =========================
# MAIN
# =========================
if __name__ == "__main__":
    # Demo: 1 modelo + 4 categorías
    model_slug = "dyson-v11"
    categories = ["bateria", "filtro", "cargador", "cepillo"]

    ofertas: Dict[str, Any] = {model_slug: {}}

    for cat in categories:
        best = find_for(model_slug, cat)
        ofertas[model_slug][cat] = [product_to_yaml_item(p) for p in best]
        print(f"[{model_slug}/{cat}] -> {len(best)} productos")

    out_path = os.path.join("data", "ofertas.aliexpress.yaml")
    write_yaml(out_path, ofertas)
    print("OK. YAML generado en:", out_path)
