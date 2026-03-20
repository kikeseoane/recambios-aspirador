# tools/sync_ofertas.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import hashlib
import hmac
import io
import json
import os
import re
import sys
import time

# Windows cp1252 fix: force UTF-8 output
if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
from datetime import datetime, timedelta
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
OFFERS = ROOT / "data" / "ofertas.yaml"
VERTICALS_YAML = ROOT / "data" / "verticals.yaml"

DEFAULT_URL = "https://s.click.aliexpress.com/e/_c3VfQRLt"

PLACEHOLDER_URL = "RELLENAR_URL_ALIEXPRESS"
PLACEHOLDER_EST = "RELLENAR_COSTE_ESTIMADO"
PLACEHOLDER_BADGE = "RELLENAR_BADGES"


# =========================
# AliExpress ENV
# =========================
APP_KEY = (os.getenv("ALI_APP_KEY") or "").strip()
APP_SECRET = (os.getenv("ALI_APP_SECRET") or "").strip()
TRACKING_ID = (os.getenv("ALI_TRACKING_ID") or "recambiosaspiradora").strip()
API_URL = (os.getenv("ALI_API_URL") or "https://api-sg.aliexpress.com/sync").strip()

SHIP_TO = (os.getenv("ALI_SHIP_TO") or "ES").strip()
CURRENCY = (os.getenv("ALI_CURRENCY") or "EUR").strip()
PAGE_SIZE = int((os.getenv("ALI_PAGE_SIZE") or "50").strip() or "50")

CACHE_DIR = ROOT / "data" / ".cache_aliexpress"
CACHE_TTL_SECONDS = int((os.getenv("ALI_CACHE_TTL") or str(7 * 24 * 3600)).strip() or str(7 * 24 * 3600))
RATE_SLEEP_SECONDS = float((os.getenv("ALI_RATE_SLEEP") or "0.35").strip() or "0.35")

# Contadores globales de llamadas reales a la API (excluye cache hits)
_api_calls_real: int = 0
_api_time_real: float = 0.0

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


def available_verticals() -> List[str]:
    verticals_doc = load_yaml(VERTICALS_YAML)
    verticals_obj = verticals_doc.get("verticals")
    if not isinstance(verticals_obj, dict):
        return ["aspiradores"]

    out: List[str] = []
    for vertical in verticals_obj.keys():
        if (ROOT / "data" / f"{vertical}.yaml").exists():
            out.append(str(vertical).strip())
    return out or ["aspiradores"]


def resolve_verticals(raw_vertical: str) -> List[str]:
    wanted = [x.strip() for x in str(raw_vertical or "all").split(",") if x.strip()]
    all_verticals = available_verticals()
    if not wanted or wanted == ["all"] or "all" in wanted:
        return all_verticals

    resolved = [v for v in wanted if v in all_verticals]
    missing = [v for v in wanted if v not in all_verticals]
    if missing:
        raise SystemExit(f"Vertical(es) no válidas: {', '.join(missing)}. Disponibles: {', '.join(all_verticals)}")
    return resolved


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
            model_slug = str(m.get("slug") or "").strip()

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

            # Pseudo-SKU "nuevo": uno por modelo para el botón "comprar nuevo"
            if model_slug:
                nuevo_sku = f"{brand_key}-{model_slug}-nuevo"
                comprar_nuevo = m.get("comprar_nuevo") or {}
                nuevo_query = normalize(str(comprar_nuevo.get("query") or f"{brand_name} {model_name}"))
                out[nuevo_sku] = {
                    "brand": brand_name,
                    "model": model_name,
                    "category": "nuevo",
                    "item_title": f"{model_name} nuevo",
                    "query": nuevo_query,
                    "must_include": [],
                    "must_not_include": [],
                    "model_tokens": [nrm(x) for x in ensure_list_str(m.get("model_tokens"))],
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

    global _api_calls_real, _api_time_real
    r: Optional[requests.Response] = None
    _t_call = time.time()
    for _attempt in range(4):
        try:
            r = requests.post(API_URL, data=params, timeout=45)
            r.raise_for_status()
            break
        except requests.exceptions.HTTPError:
            raise
        except requests.exceptions.RequestException as exc:
            wait = 2 ** (_attempt + 2)  # 4s, 8s, 16s, 32s
            print(f"  [request] intento {_attempt+1}/4 - reintentando en {wait}s ({exc})")
            if _attempt == 3:
                raise
            time.sleep(wait)
    if r is None:
        raise RuntimeError(f"No se pudo obtener respuesta de AliExpress para method={method}")
    _api_calls_real += 1
    _api_time_real += time.time() - _t_call
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
    "lip balm", "lip gloss", "lipstick", "nail polish", "nail art", "eyeliner",
    "vitroceramic", "induction hob", "induction cooker", "ceramic hob",
    "led strip", "led lamp", "fairy light", "phone case", "phone cover",
    "dog collar", "cat collar", "pet collar", "pet toy",
]

CATEGORY_PART_TERMS = {
    "bateria": ["battery", "bateria", "pack", "rechargeable", "22.2v", "21.6v", "25.2v", "click-in", "click in"],
    "filtro": ["filter", "filtro", "hepa", "rear", "pre", "post", "membrane", "sediment", "carbon filter"],
    "cargador": ["charger", "cargador", "adapter", "adaptador", "power", "ac adapter"],
    "cepillo": ["brush", "cepillo", "head", "roller", "rodillo", "torque", "drive"],
    "soporte": ["wall", "mount", "holder", "dock", "stand", "bracket", "storage", "rack", "base"],
    "accesorios": ["accessory", "accessories", "kit", "crevice", "tool", "brush", "nozzle", "boquilla"],
    # Categorías de lavadora y electrodomésticos
    "bomba": ["pump", "drain pump", "drain", "impeller"],
    "resistencia": ["heating element", "heater", "heating", "resistencia", "thermostat"],
    "rodamiento": ["bearing", "drum bearing", "ball bearing"],
    "escobillas": ["carbon brush", "motor brush", "brush holder"],
    "correa": ["belt", "drive belt", "poly v", "poly-v", "v-belt"],
    "bolsa": ["dust bag", "vacuum bag", "paper bag"],
    # Categorías de ósmosis, cafeteras, freidoras, etc.
    "junta": ["seal", "gasket", "door seal", "o-ring", "rubber seal", "boot seal"],
    "deposito": ["tank", "container", "reservoir", "water tank", "dust cup"],
    "cesta": ["basket", "tray", "bin", "dust cup", "cup"],
    # Categorías de afeitadoras y cepillos eléctricos
    "laminas": ["foil", "shaving foil", "cutting foil", "blade", "foil replacement"],
    "cabezal": ["shaver head", "replacement head", "head", "rotary head"],
}

CATEGORY_QUERY_TERMS = {
    "bateria": ["battery", "replacement"],
    "filtro": ["filter", "replacement"],
    "cargador": ["charger", "adapter"],
    "cepillo": ["brush", "roller"],
    "soporte": ["dock", "wall mount"],
    "accesorios": ["accessory", "kit"],
    "laminas": ["foil", "replacement"],
    "cabezal": ["head", "replacement"],
    "junta": ["gasket", "seal"],
    "deposito": ["tank", "container"],
    "cesta": ["basket", "tray"],
    # Categorías de lavadora y electrodomésticos
    "bomba": ["drain pump", "pump"],
    "resistencia": ["heating element", "heater"],
    "rodamiento": ["bearing", "drum"],
    "escobillas": ["carbon brush", "motor brush"],
    "correa": ["drive belt", "belt"],
    "bolsa": ["dust bag", "bag"],
}

CATEGORY_NEGATIVE_TERMS = {
    "soporte": ["trigger", "switch", "button", "pcb", "board", "handle", "motor"],
    "bateria": ["trigger", "switch", "button", "filter", "charger", "dock", "wall mount"],
    "filtro": ["battery", "charger", "trigger", "switch", "button"],
    "cargador": ["battery", "filter", "trigger", "switch", "button"],
    "cepillo": ["battery", "filter", "charger", "trigger", "switch", "button"],
    # Lavadora: exclusiones cruzadas entre recambios incompatibles
    "bomba": ["belt", "bearing", "heating element", "heater", "seal", "gasket", "carbon brush", "filter"],
    "resistencia": ["pump", "drain", "belt", "bearing", "seal", "gasket", "carbon brush"],
    "rodamiento": ["pump", "drain", "belt", "heating element", "heater", "seal", "gasket", "carbon brush"],
    "escobillas": ["pump", "drain", "belt", "bearing", "heating element", "heater", "seal", "gasket"],
    "correa": ["pump", "drain", "bearing", "heating element", "heater", "seal", "gasket", "carbon brush"],
    "junta": ["pump", "drain", "belt", "bearing", "heating element", "heater", "carbon brush"],
    "bolsa": ["pump", "drain", "belt", "bearing", "heating element", "heater", "carbon brush"],
}

MODEL_TOKEN_RE = re.compile(r"\b(v\d{1,2}|sv\d{2}|dc\d{2,3})\b", re.IGNORECASE)
GENERIC_MODEL_WORD_RE = re.compile(r"[a-z0-9][a-z0-9+.-]{1,}", re.IGNORECASE)
QUERY_NOISE_RE = re.compile(
    r"\b(compatible|compatibles|para|repuesto|recambio|replacement|spare|kit|pack)\b",
    re.IGNORECASE,
)
QUERY_TOKEN_RE = re.compile(r"[a-z0-9]+(?:[.+/-][a-z0-9]+)*", re.IGNORECASE)
MODEL_STOPWORDS = {
    "series", "serie", "robot", "aspirador", "aspiradora", "vacuum", "cordless",
    "airfryer", "freidora", "cafetera", "coffee", "maker", "shaver", "toothbrush",
    "taladro", "drill", "martillo", "sierra", "amoladora", "impacto", "one", "plus",
}
QUERY_NOISE_TERMS = {
    "compatible", "compatibles", "para", "con", "sin", "repuesto", "repuestos",
    "recambio", "recambios", "replacement", "replacements", "spare", "spares",
    "part", "parts", "pieza", "piezas", "kit", "pack",
}


def looks_bad(title: str) -> bool:
    t = nrm(title)
    return any(b in t for b in BANNED_TITLE)


def cat_part_terms(cat: str) -> List[str]:
    c = nrm(cat)
    for k, terms in CATEGORY_PART_TERMS.items():
        if k in c:
            return terms
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
    # Fallback: usa el nombre de categoría como término de ancla — mucho más específico
    # que "replacement/spare" que pasaría cualquier producto del mundo.
    return [c, "replacement"] if c else ["replacement"]


def cat_negative_terms(cat: str) -> List[str]:
    c = nrm(cat)
    for k, terms in CATEGORY_NEGATIVE_TERMS.items():
        if k in c:
            return terms
    if "soport" in c or "mount" in c or "dock" in c:
        return CATEGORY_NEGATIVE_TERMS.get("soporte", [])
    return []


def cat_query_terms(cat: str) -> List[str]:
    c = nrm(cat)
    for k, terms in CATEGORY_QUERY_TERMS.items():
        if k in c:
            return terms
    return cat_part_terms(cat)[:2]


def extract_identifier_tokens(text: str) -> List[str]:
    raw_tokens = [t.lower().strip(" .,/()[]") for t in GENERIC_MODEL_WORD_RE.findall(nrm(text))]
    out: List[str] = []
    for idx, tok in enumerate(raw_tokens):
        if len(tok) < 2 or tok in MODEL_STOPWORDS:
            continue
        has_digit = any(ch.isdigit() for ch in tok)
        prev_tok = raw_tokens[idx - 1] if idx > 0 else ""
        next_tok = raw_tokens[idx + 1] if idx + 1 < len(raw_tokens) else ""

        keep = False
        if has_digit:
            keep = True
        elif tok.isalpha() and 2 <= len(tok) <= 5:
            keep = any(any(ch.isdigit() for ch in n) for n in (prev_tok, next_tok))

        if keep and tok not in out:
            out.append(tok)
    return out[:5]


def model_tokens_from_ctx(model: str) -> List[str]:
    t = nrm(model)
    tokens = [m.group(1).lower() for m in MODEL_TOKEN_RE.finditer(t)]
    tokens.extend(extract_identifier_tokens(t))
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


def compact_spaces(s: str) -> str:
    return re.sub(r"\s+", " ", str(s or "")).strip()


def clean_query_fragment(text: str) -> str:
    cleaned = QUERY_NOISE_RE.sub(" ", normalize(text))
    return compact_spaces(cleaned)


def query_tokens(text: str) -> List[str]:
    return [tok.lower() for tok in QUERY_TOKEN_RE.findall(compact_spaces(text))]


def query_phrase(text: str, seen: set[str]) -> str:
    out: List[str] = []
    for tok in query_tokens(text):
        if tok in QUERY_NOISE_TERMS or tok in seen:
            continue
        seen.add(tok)
        out.append(tok)
    return " ".join(out)


def compose_search_query(base_parts: List[str], extra_parts: List[str]) -> str:
    parts: List[str] = []
    seen: set[str] = set()

    for part in base_parts:
        clean = compact_spaces(part)
        if not clean:
            continue
        parts.append(clean)
        seen.update(query_tokens(clean))

    for part in extra_parts:
        clean = query_phrase(part, seen)
        if clean:
            parts.append(clean)

    return compact_spaces(" ".join(parts))[:120]


def unique_keywords(candidates: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for candidate in candidates:
        keyword = compact_spaces(candidate)[:120]
        if not keyword:
            continue
        key = keyword.lower()
        if key in seen:
            continue
        seen.add(key)
        out.append(keyword)
    return out


def sku_records_from_verticals(verticals: List[str]) -> Dict[str, Dict[str, Any]]:
    out: Dict[str, Dict[str, Any]] = {}
    for vertical in verticals:
        catalog_path = ROOT / "data" / f"{vertical}.yaml"
        catalog = load_yaml(catalog_path)
        vertical_records = sku_records_from_catalog(catalog)
        for sku, ctx in vertical_records.items():
            ctx_copy = dict(ctx)
            ctx_copy["vertical"] = vertical
            out[sku] = ctx_copy
    return out


def build_search_keywords(ctx: Dict[str, Any], query_override: str, must_include: List[str]) -> List[str]:
    brand = compact_spaces(str(ctx.get("brand") or ""))
    model = compact_spaces(str(ctx.get("model") or ""))
    item_title = clean_query_fragment(str(ctx.get("item_title") or ""))
    category_terms = " ".join(cat_query_terms(str(ctx.get("category") or ""))[:2])
    include_terms = " ".join([compact_spaces(x) for x in must_include[:4] if compact_spaces(x)])
    model_tokens = " ".join(model_tokens_from_ctx(model)[:3])

    # Cascada de mayor a menor precisión:
    # 1. Query override explícito
    # 2. Marca + modelo completo + términos must_include + tipo recambio
    # 3. Marca + modelo completo + tipo recambio
    # 4. Marca + tokens de modelo + tipo recambio
    # 5. Marca + modelo + título del item
    # 6. Marca + modelo + nombre categoría + título
    # 7. Marca + tipo recambio (sin modelo — cubre listings genéricos de marca)
    # 8. Marca + modelo solo
    candidates = [
        compose_search_query([], [query_override]),
        compose_search_query([brand, model], [include_terms, category_terms]),
        compose_search_query([brand, model], [category_terms]),
        compose_search_query([brand, model], [model_tokens, category_terms]),
        compose_search_query([brand, model], [item_title]),
        compose_search_query([brand, model], [str(ctx.get("category") or ""), str(ctx.get("item_title") or "")]),
        compose_search_query([brand], [category_terms, "replacement"]),
        compose_search_query([brand, model], []),
    ]
    return unique_keywords(candidates)


def choose_fallback_search_query(ctx: Dict[str, Any], query_override: str) -> str:
    """
    Prioridad:
      1) query override
      2) brand + model + category + item_title
      3) brand + model + category
      4) brand + model
    """
    candidates = [
        compose_search_query([], [query_override]),
        compose_search_query(
            [str(ctx.get("brand") or ""), str(ctx.get("model") or "")],
            [str(ctx.get("category") or ""), str(ctx.get("item_title") or "")],
        ),
        compose_search_query(
            [str(ctx.get("brand") or ""), str(ctx.get("model") or "")],
            [str(ctx.get("category") or "")],
        ),
        compose_search_query(
            [str(ctx.get("brand") or ""), str(ctx.get("model") or "")],
            [],
        ),
    ]

    for c in candidates:
        c = compact_spaces(c)
        if c:
            return c[:120]
    return ""


def choose_fallback_search_label(ctx: Dict[str, Any], query_text: str) -> str:
    category = compact_spaces(str(ctx.get("category") or ""))
    model = compact_spaces(str(ctx.get("model") or ""))
    brand = compact_spaces(str(ctx.get("brand") or ""))

    if category and model:
        return f"Buscar {category} para {model}"
    if model:
        return f"Buscar para {model}"
    if brand and category:
        return f"Buscar {category} {brand}"
    if query_text:
        return f"Buscar: {query_text}"
    return "Buscar en AliExpress"


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
    query = normalize(str(ctx.get("query") or ""))
    if not query:
        query = normalize(str(offers_obj.get("query") or ""))

    must_include = ensure_list_str(ctx.get("must_include")) or ensure_list_str(offers_obj.get("must_include"))
    must_not_include = ensure_list_str(ctx.get("must_not_include")) or ensure_list_str(offers_obj.get("must_not_include"))

    model_tokens = [nrm(x) for x in (ensure_list_str(ctx.get("model_tokens")) or ensure_list_str(offers_obj.get("model_tokens")))]

    return query, must_include, must_not_include, model_tokens


def pick_relaxed_link(
    brand: str,
    category: str,
    part_terms: List[str],
    must_not_include: List[str],
    use_cache: bool,
) -> Optional[str]:
    """
    Búsqueda relajada sin filtro de modelo: solo marca + categoría.
    Aplica must_not_include para evitar contaminar con categorías cruzadas.
    """
    relaxed_terms = " ".join(cat_query_terms(category)[:2])
    keyword = compact_spaces(f"{brand} {relaxed_terms} replacement")[:120]
    if not keyword.strip():
        return None

    for lang in ("EN", "ES"):
        for page_no in (1,):
            try:
                resp = product_query(keyword, lang=lang, page_no=page_no, use_cache=use_cache)
            except Exception as exc:
                print(f"  [relaxed-error] kw='{keyword}' lang={lang} page={page_no} - {exc}")
                continue
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
                if not any(pt in tt for pt in part_terms):
                    continue
                if must_not_include and contains_any(tt, must_not_include):
                    continue
                candidates.append(p)

            if not candidates:
                continue

            candidates.sort(
                key=lambda p: get_orders(p) * 0.015 + get_commission_rate(p) * 0.4,
                reverse=True,
            )
            best = candidates[0]
            url = best.get("promotion_link") or best.get("product_detail_url")
            if url:
                return {
                    "url": str(url).strip(),
                    "image_url": str(best.get("product_main_image_url") or "").strip(),
                    "sale_price": str(best.get("sale_price") or "").strip(),
                    "sale_price_currency": str(best.get("sale_price_currency") or "").strip(),
                    "original_price": str(best.get("original_price") or "").strip(),
                    "discount": str(best.get("discount") or "").strip(),
                    "product_title": str(best.get("product_title") or "").strip(),
                }

    return None


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
        for page_no in (1,):
            try:
                resp = product_query(keyword, lang=lang, page_no=page_no, use_cache=use_cache)
            except Exception as exc:
                print(f"  [query-error] kw='{keyword}' lang={lang} page={page_no} - {exc}")
                continue
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

                if req_models and not title_has_required_model(tt, req_models):
                    continue

                if not any(pt in tt for pt in part_terms):
                    continue

                if must_include and not contains_all(tt, must_include):
                    continue

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
                return {
                    "url": str(url).strip(),
                    "image_url": str(best.get("product_main_image_url") or "").strip(),
                    "sale_price": str(best.get("sale_price") or "").strip(),
                    "sale_price_currency": str(best.get("sale_price_currency") or "").strip(),
                    "original_price": str(best.get("original_price") or "").strip(),
                    "discount": str(best.get("discount") or "").strip(),
                    "product_title": str(best.get("product_title") or "").strip(),
                }

    return None


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Sincroniza data/ofertas.yaml desde data/<vertical>.yaml + AliExpress API")
    ap.add_argument("--clear-cache", action="store_true", help="Borra el cache y termina")
    ap.add_argument("--no-cache", action="store_true", help="Ignora cache (hace llamadas frescas)")
    ap.add_argument("--only-sku", action="append", default=[], help="Solo procesa este SKU (puedes repetir)")
    ap.add_argument("--force", action="store_true", help="Fuerza lookup incluso si ya hay URL no-default")
    ap.add_argument("--vertical", default="all", help="Vertical a sincronizar: una, varias separadas por comas, o 'all' (default)")
    ap.add_argument("--only-stale", type=int, default=0, metavar="DAYS",
                    help="Solo procesa SKUs cuyo updated_at tiene más de DAYS días (0 = ignorar). Útil para refrescar enlaces sin relanzar todo.")
    ap.add_argument("--batch-size", type=int, default=0, metavar="N",
                    help="Procesa como máximo N SKUs por ejecución, los más antiguos primero (0 = todos). Combina con --only-stale para repartir en días.")
    ap.add_argument("--max-minutes", type=float, default=0, metavar="MIN",
                    help="Detiene el procesado cuando se acerca a MIN minutos de ejecución, guarda y sale limpiamente (0 = sin límite).")
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

    selected_verticals = resolve_verticals(args.vertical)
    sku_ctx = sku_records_from_verticals(selected_verticals)
    want = set(sku_ctx.keys())

    offers_doc = load_yaml(OFFERS)
    offers = offers_doc.get("offers")
    if not isinstance(offers, dict):
        offers = {}

    only = [str(x).strip() for x in args.only_sku if str(x).strip()]
    if only:
        want = set([s for s in only if s in sku_ctx])

    # --only-stale: filtra SKUs cuyo updated_at supera N días
    if not only and args.only_stale > 0:
        cutoff = (datetime.now().date() - timedelta(days=args.only_stale)).isoformat()
        stale = {sku for sku in want if str(ensure_offer_obj(offers.get(sku)).get("updated_at") or "").strip() < cutoff}
        print(f"  --only-stale {args.only_stale}d: {len(stale)}/{len(want)} SKUs sin actualizar desde {cutoff}")
        want = stale

    # --batch-size: toma los N más antiguos para no saturar la API en una sola pasada
    if not only and args.batch_size > 0 and len(want) > args.batch_size:
        sorted_want = sorted(want, key=lambda sku: str(ensure_offer_obj(offers.get(sku)).get("updated_at") or "0000-00-00"))
        want = set(sorted_want[:args.batch_size])
        print(f"  --batch-size {args.batch_size}: procesando {args.batch_size} SKUs más antiguos")

    added = 0
    updated = 0
    orphaned = 0
    un_orphaned = 0
    changed_urls_to_default = 0
    filled_from_aliexpress = 0
    failed_skus = 0
    _processed = 0
    SAVE_EVERY = 25  # guarda progreso cada N SKUs

    today = datetime.now().date().isoformat()
    _t0 = time.time()
    _total_want = len(want)
    _time_budget_s = args.max_minutes * 60 if args.max_minutes > 0 else 0

    print(f"  Iniciando procesado de {_total_want} SKUs...")

    for sku in sorted(want):
        if _time_budget_s and (time.time() - _t0) >= _time_budget_s:
            print(f"  [time-budget] {args.max_minutes:.0f}min alcanzados, saliendo limpiamente tras {_processed} SKUs.")
            break
        ctx = sku_ctx.get(sku) or {}
        prev = offers.get(sku)
        obj = ensure_offer_obj(prev)
        before = dict(obj)

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

            must_not_default = cat_negative_terms(category)

            query, must_include, must_not_override, model_tokens_override = merge_overrides(
                sku=sku,
                ctx=ctx,
                offers_obj=obj,
            )
            must_not_combined = [*must_not_default, *must_not_override]

            kws = build_search_keywords(ctx, query, must_include)
            fallback_search_query = kws[0] if kws else choose_fallback_search_query(ctx, query)
            fallback_search_label = choose_fallback_search_label(ctx, fallback_search_query)

            found = None
            matched_kw = ""

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
                    matched_kw = kw
                    break

            if found:
                obj["url"] = found["url"]
                obj["match_type"] = "exact_or_best_match"
                obj["matched_query"] = matched_kw or fallback_search_query
                obj["fallback_search_query"] = fallback_search_query
                obj["fallback_search_label"] = fallback_search_label
                if found.get("image_url"):
                    obj["image_url"] = found["image_url"]
                if found.get("sale_price"):
                    obj["sale_price"] = found["sale_price"]
                    obj["sale_price_currency"] = found.get("sale_price_currency", "EUR")
                if found.get("original_price"):
                    obj["original_price"] = found["original_price"]
                if found.get("discount"):
                    obj["discount"] = found["discount"]
                if found.get("product_title"):
                    obj["product_title"] = found["product_title"]
                obj.pop("needs_url", None)
                obj["updated_at"] = today
                filled_from_aliexpress += 1
            else:
                # Fallback relajado: busca en API solo marca + categoría
                relaxed = pick_relaxed_link(
                    brand=brand,
                    category=category,
                    part_terms=part_terms,
                    must_not_include=must_not_combined,
                    use_cache=use_cache,
                )
                if relaxed:
                    obj["url"] = relaxed["url"]
                    obj["match_type"] = "relaxed_fallback"
                    obj["fallback_search_query"] = fallback_search_query
                    obj["fallback_search_label"] = fallback_search_label
                    if relaxed.get("image_url"):
                        obj["image_url"] = relaxed["image_url"]
                    if relaxed.get("sale_price"):
                        obj["sale_price"] = relaxed["sale_price"]
                        obj["sale_price_currency"] = relaxed.get("sale_price_currency", "EUR")
                    if relaxed.get("original_price"):
                        obj["original_price"] = relaxed["original_price"]
                    if relaxed.get("discount"):
                        obj["discount"] = relaxed["discount"]
                    if relaxed.get("product_title"):
                        obj["product_title"] = relaxed["product_title"]
                    obj.pop("needs_url", None)
                    obj.pop("matched_query", None)
                    obj["updated_at"] = today
                    filled_from_aliexpress += 1
                else:
                    # Sin producto encontrado: limpia la URL para que el template
                    # muestre un botón de búsqueda construido desde fallback_search_query.
                    # Nunca usamos DEFAULT_URL genérico (era un link de aspiradoras para todos los verticals).
                    if obj.get("url") in ("", DEFAULT_URL, None) or obj.get("needs_url"):
                        obj["url"] = ""
                        changed_urls_to_default += 1

                    obj["needs_url"] = True
                    obj["match_type"] = "fallback_search"
                    obj["fallback_search_query"] = fallback_search_query
                    obj["fallback_search_label"] = fallback_search_label
                    obj.pop("matched_query", None)

        else:
            obj.pop("needs_url", None)
            if "match_type" not in obj:
                obj["match_type"] = "manual_or_existing"

        if sku not in offers:
            offers[sku] = obj
            added += 1
        else:
            offers[sku] = obj
            if before != obj:
                updated += 1

        _processed += 1
        _elapsed = time.time() - _t0
        _rate = _processed / _elapsed if _elapsed > 0 else 0
        _remaining = _total_want - _processed
        _eta_s = int(_remaining / _rate) if _rate > 0 else 0
        _eta_str = f"{_eta_s//3600:02d}h{(_eta_s%3600)//60:02d}m{_eta_s%60:02d}s" if _eta_s >= 3600 else f"{_eta_s//60:02d}m{_eta_s%60:02d}s"
        _status = "OK" if obj.get("url") and obj.get("url") != DEFAULT_URL else "~"
        print(f"  [{_processed:4d}/{_total_want}] {_status} {sku[:55]:<55} | elapsed {int(_elapsed//60):02d}m{int(_elapsed%60):02d}s ETA {_eta_str}", flush=True)
        if _processed % SAVE_EVERY == 0:
            dump_yaml(OFFERS, {"offers": offers})
            print(f"  --- checkpoint guardado ({filled_from_aliexpress} AliExpress, {added} nuevos, {updated} actualizados) ---", flush=True)

    if not only and set(selected_verticals) == set(available_verticals()):
        for sku, o in list(offers.items()):
            if sku not in set(sku_ctx.keys()):
                o = ensure_offer_obj(o)
                if o.get("orphaned") is not True:
                    o["orphaned"] = True
                    offers[sku] = o
                    orphaned += 1

    dump_yaml(OFFERS, {"offers": offers})

    _total_elapsed = time.time() - _t0
    _avg_api = (_api_time_real / _api_calls_real) if _api_calls_real else 0.0

    # Calcular pendientes restantes tras el proceso
    _cutoff = (datetime.now().date() - timedelta(days=args.only_stale)).isoformat() if args.only_stale > 0 else None
    _needs_url = sum(1 for s, o in offers.items() if ensure_offer_obj(o).get("needs_url") and not ensure_offer_obj(o).get("orphaned"))
    _still_stale = sum(1 for s in sku_ctx if _cutoff and str(ensure_offer_obj(offers.get(s)).get("updated_at") or "").strip() < _cutoff) if _cutoff else 0

    print("OK: sync_ofertas (AliExpress autolinks + catalog overrides + cache flags)")
    print(f"  Verticales:            {', '.join(selected_verticals)}")
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
    print(f"  --- Rendimiento API ---")
    print(f"  Llamadas reales API:    {_api_calls_real}")
    print(f"  Tiempo total API:       {_api_time_real:.1f}s")
    print(f"  Media por llamada:      {_avg_api:.2f}s")
    print(f"  Tiempo total script:    {_total_elapsed:.1f}s")
    print(f"  --- Pendientes restantes ---")
    print(f"  Sin URL (needs_url):    {_needs_url}")
    print(f"  Stale >{args.only_stale}d:            {_still_stale}")
    print(f"STATS: api_calls={_api_calls_real} avg_call={_avg_api:.2f}s total={_total_elapsed:.0f}s skus={len(want)} needs_url={_needs_url} stale={_still_stale}")


if __name__ == "__main__":
    main()
