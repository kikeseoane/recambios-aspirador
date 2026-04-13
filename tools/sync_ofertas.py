# tools/sync_ofertas.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import atexit
import argparse
import hashlib
import hmac
import io
import json
import os
import re
import sys
import time
import unicodedata

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
OFFERS = ROOT / "data" / "ofertas.json"
LEGACY_OFFERS_YAML = ROOT / "data" / "ofertas.yaml"
VERTICALS_YAML = ROOT / "data" / "verticals.yaml"
VERTICAL_DEFAULTS_YAML = ROOT / "data" / "vertical_defaults.yaml"

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
GITHUB_REPO = (os.getenv("GITHUB_REPO") or os.getenv("GITHUB_REPOSITORY") or "").strip()
GITHUB_ACTIONS_PAUSE_TOKEN = (os.getenv("GITHUB_ACTIONS_PAUSE_TOKEN") or os.getenv("GITHUB_TOKEN") or "").strip()
GITHUB_ACTIONS_PAUSE_VAR = (os.getenv("GITHUB_ACTIONS_PAUSE_VAR") or "SYNC_OFERTAS_PAUSED").strip()
GITHUB_ACTIONS_PAUSE_AT_VAR = (os.getenv("GITHUB_ACTIONS_PAUSE_AT_VAR") or f"{GITHUB_ACTIONS_PAUSE_VAR}_AT").strip()
AI_VALIDATION_PROVIDER = (os.getenv("AI_VALIDATION_PROVIDER") or "").strip().lower()
AI_VALIDATION_MODEL = (os.getenv("AI_VALIDATION_MODEL") or "").strip()
AI_VALIDATION_API_KEY = (
    os.getenv("AI_VALIDATION_API_KEY")
    or os.getenv("OPENROUTER_API_KEY")
    or ""
).strip()
AI_VALIDATION_URL = (os.getenv("AI_VALIDATION_URL") or "https://openrouter.ai/api/v1/chat/completions").strip()
AI_VALIDATION_MAX_CHECKS = int((os.getenv("AI_VALIDATION_MAX_CHECKS") or "0").strip() or "0")
AI_VALIDATION_TIMEOUT = float((os.getenv("AI_VALIDATION_TIMEOUT") or "20").strip() or "20")
AI_VALIDATION_RESPONSE_FORMAT = (os.getenv("AI_VALIDATION_RESPONSE_FORMAT") or "").strip().lower()
AI_VALIDATION_FALLBACK_MODELS = tuple(
    x.strip() for x in (os.getenv("AI_VALIDATION_FALLBACK_MODELS") or "").split(",") if x.strip()
)

SHIP_TO = (os.getenv("ALI_SHIP_TO") or "ES").strip()
CURRENCY = (os.getenv("ALI_CURRENCY") or "EUR").strip()
PAGE_SIZE = int((os.getenv("ALI_PAGE_SIZE") or "50").strip() or "50")

CACHE_DIR = ROOT / "data" / ".cache_aliexpress"
CACHE_TTL_SECONDS = int((os.getenv("ALI_CACHE_TTL") or str(7 * 24 * 3600)).strip() or str(7 * 24 * 3600))
RATE_SLEEP_SECONDS = float((os.getenv("ALI_RATE_SLEEP") or "0.35").strip() or "0.35")
MAX_EXACT_KEYWORDS = int((os.getenv("SYNC_MAX_EXACT_KEYWORDS") or "3").strip() or "3")
MAX_RESCUE_KEYWORDS = int((os.getenv("SYNC_MAX_RESCUE_KEYWORDS") or "3").strip() or "3")
MAX_WIDE_KEYWORDS = int((os.getenv("SYNC_MAX_WIDE_KEYWORDS") or "2").strip() or "2")
MAX_SHORTLIST_CANDIDATES = int((os.getenv("SYNC_MAX_SHORTLIST_CANDIDATES") or "8").strip() or "8")
EXACT_LANGS = tuple(x.strip().upper() for x in (os.getenv("SYNC_EXACT_LANGS") or "EN,ES").split(",") if x.strip())
RESCUE_LANGS = tuple(x.strip().upper() for x in (os.getenv("SYNC_RESCUE_LANGS") or "EN,ES").split(",") if x.strip())

# Contadores globales de llamadas reales a la API (excluye cache hits)
_api_calls_real: int = 0
_api_time_real: float = 0.0
_ai_validation_calls: int = 0
_ai_budget_state: Dict[str, Any] = {}
_ai_validation_active_model: str = ""

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


def load_offers_doc() -> dict:
    if OFFERS.exists():
        return json.loads(OFFERS.read_text(encoding="utf-8") or "{}")
    if LEGACY_OFFERS_YAML.exists():
        return load_yaml(LEGACY_OFFERS_YAML)
    return {}


def dump_offers_doc(data: dict) -> None:
    OFFERS.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")
    if LEGACY_OFFERS_YAML.exists():
        LEGACY_OFFERS_YAML.unlink()


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


def reason_key(s: str, max_len: int = 90) -> str:
    text = unicodedata.normalize("NFKD", str(s or "")).encode("ascii", "ignore").decode("ascii").lower()
    text = re.sub(r"[^a-z0-9]+", "_", text).strip("_")
    return (text[:max_len].strip("_") or "ai_pending_unknown")


def nrm(s: str) -> str:
    return " ".join((s or "").lower().split())


def folded_nrm(s: str) -> str:
    return nrm(fold_query_text(s))


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
        raise SystemExit(f"Vertical(es) no vÃ¡lidas: {', '.join(missing)}. Disponibles: {', '.join(all_verticals)}")
    return resolved


# =========================
# GitHub Actions pause switch
# =========================
def github_pause_enabled() -> bool:
    if os.getenv("GITHUB_ACTIONS") == "true":
        return False
    return bool(GITHUB_REPO and GITHUB_ACTIONS_PAUSE_TOKEN and GITHUB_ACTIONS_PAUSE_VAR)


def github_headers() -> Dict[str, str]:
    return {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {GITHUB_ACTIONS_PAUSE_TOKEN}",
        "X-GitHub-Api-Version": "2022-11-28",
    }


def github_variable_url(name: str) -> str:
    return f"https://api.github.com/repos/{GITHUB_REPO}/actions/variables/{name}"


def github_get_repo_variable(name: str) -> Optional[str]:
    r = requests.get(github_variable_url(name), headers=github_headers(), timeout=20)
    if r.status_code == 404:
        return None
    r.raise_for_status()
    data = r.json() or {}
    return str(data.get("value") or "")


def github_set_repo_variable(name: str, value: str) -> None:
    payload = {"name": name, "value": value}
    put = requests.patch(github_variable_url(name), headers=github_headers(), json=payload, timeout=20)
    if put.status_code == 404:
        create_url = f"https://api.github.com/repos/{GITHUB_REPO}/actions/variables"
        post = requests.post(create_url, headers=github_headers(), json=payload, timeout=20)
        post.raise_for_status()
        return
    put.raise_for_status()


def github_delete_repo_variable(name: str) -> None:
    r = requests.delete(github_variable_url(name), headers=github_headers(), timeout=20)
    if r.status_code not in (204, 404):
        r.raise_for_status()


def github_pause_sync_workflow() -> Dict[str, Optional[str]]:
    if not github_pause_enabled():
        return {"paused": None, "paused_at": None}
    previous = github_get_repo_variable(GITHUB_ACTIONS_PAUSE_VAR)
    previous_at = github_get_repo_variable(GITHUB_ACTIONS_PAUSE_AT_VAR)
    if previous != "1":
        github_set_repo_variable(GITHUB_ACTIONS_PAUSE_VAR, "1")
        github_set_repo_variable(GITHUB_ACTIONS_PAUSE_AT_VAR, datetime.now().date().isoformat())
        print(f"  GitHub Action pausada via variable {GITHUB_ACTIONS_PAUSE_VAR}=1")
    else:
        print(f"  GitHub Action ya estaba pausada ({GITHUB_ACTIONS_PAUSE_VAR}=1)")
    return {"paused": previous, "paused_at": previous_at}


def github_restore_sync_workflow(previous: Dict[str, Optional[str]]) -> None:
    if not github_pause_enabled():
        return
    previous_paused = previous.get("paused")
    previous_paused_at = previous.get("paused_at")
    if previous_paused is None:
        github_delete_repo_variable(GITHUB_ACTIONS_PAUSE_VAR)
        print(f"  GitHub Action reactivada borrando variable {GITHUB_ACTIONS_PAUSE_VAR}")
    else:
        github_set_repo_variable(GITHUB_ACTIONS_PAUSE_VAR, previous_paused)
        print(f"  GitHub Action restaurada: {GITHUB_ACTIONS_PAUSE_VAR}={previous_paused}")
    if previous_paused_at is None:
        github_delete_repo_variable(GITHUB_ACTIONS_PAUSE_AT_VAR)
    else:
        github_set_repo_variable(GITHUB_ACTIONS_PAUSE_AT_VAR, previous_paused_at)


# =========================
# Catalog parsing: SKU â†’ context (+ overrides)
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
                        # overrides opcionales en el catÃ¡logo:
                        "query": normalize(str(it.get("query") or "")),
                        "must_include": ensure_list_str(it.get("must_include")),
                        "must_not_include": ensure_list_str(it.get("must_not_include")),
                        "model_tokens": [nrm(x) for x in ensure_list_str(it.get("model_tokens"))],
                    }

            # Pseudo-SKU "nuevo": uno por modelo para el botÃ³n "comprar nuevo"
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
    data: Dict[str, Any] = {}
    _t_call = time.time()
    for _attempt in range(4):
        try:
            r = requests.post(API_URL, data=params, timeout=45)
            r.raise_for_status()
            data = r.json()
            break
        except requests.exceptions.HTTPError:
            raise
        except requests.exceptions.RequestException as exc:
            wait = 2 ** (_attempt + 2)  # 4s, 8s, 16s, 32s
            print(f"  [request] intento {_attempt+1}/4 - reintentando en {wait}s ({exc})")
            if _attempt == 3:
                raise
            time.sleep(wait)
        except ValueError as exc:
            body = normalize((r.text or "")[:200]) if r is not None else ""
            wait = 2 ** (_attempt + 2)  # 4s, 8s, 16s, 32s
            print(f"  [request] intento {_attempt+1}/4 - JSON/EOF invalido, reintentando en {wait}s (status={r.status_code if r is not None else 'n/a'} body={body})")
            if _attempt == 3:
                raise RuntimeError(
                    f"AliExpress invalid JSON/EOF: method={method} status={r.status_code if r is not None else 'n/a'} body={body}"
                ) from exc
            time.sleep(wait)
    if r is None:
        raise RuntimeError(f"No se pudo obtener respuesta de AliExpress para method={method}")
    _api_calls_real += 1
    _api_time_real += time.time() - _t_call

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
    # AutomociÃ³n â€” se cuelan por comisiÃ³n alta
    "common rail", "fuel injector", "injector valve", "diesel injector",
    "car valve", "auto valve", "automotive", "bosch f00", "denso",
    "oil filter car", "car filter", "truck filter", "engine oil",
    "brake pad", "brake disc", "shock absorber", "suspension",
    "car seat", "steering wheel", "gear shift", "clutch",
    "obd", "ecu", "alternator", "radiator hose", "timing belt car",
    # ElectrÃ³nica genÃ©rica
    "arduino", "raspberry", "pcb board", "motherboard", "gpu", "cpu cooler",
    # Ropa / calzado
    "sneakers", "shoes", "boots", "jacket", "hoodie", "pants", "jeans",
]

# TÃ©rminos mÃ­nimos que debe contener el tÃ­tulo para ser vÃ¡lido en cada vertical
VERTICAL_REQUIRED_TERMS = {
    "aspiradores":          ["vacuum", "aspirador", "robot", "cleaner", "mop"],
    "afeitadoras":          ["shaver", "razor", "foil", "afeitadora", "electric shav"],
    "aspiradoras-normales": ["vacuum", "aspirador", "cleaner"],
    "auriculares":          ["earbuds", "earphone", "headphone", "auricular", "headset", "tws"],
    "cafeteras":            ["coffee", "espresso", "cafetera", "capsule", "brew"],
    "cepillos":             ["toothbrush", "cepillo", "electric brush", "sonic"],
    "secadores-pelo":       ["hair dryer", "dryer", "blow dryer", "secador"],
    "planchas-pelo":        ["hair straightener", "straightener", "flat iron", "plancha"],
    "aspiradores-mano":     ["handheld vacuum", "portable vacuum", "mini vacuum", "aspirador de mano"],
    "vaporetas":            ["steam cleaner", "steam mop", "vaporeta", "steamer"],
    "centros-planchado":    ["steam iron", "steam generator", "ironing station", "centro de planchado"],
    "freidoras":            ["air fryer", "freidora", "airfryer"],
    "herramientas":         ["drill", "screwdriver", "grinder", "saw", "impact", "tool", "taladro"],
    "lavadoras":            ["washing machine", "washer", "lavadora", "dryer"],
    "mascotas":             ["pet", "dog", "cat", "groomer", "clipper", "mascotas"],
    "osmosis":              ["osmosis", "water filter", "filtro agua", "reverse osmosis", "purifier"],
    "patinetes-electricos": ["electric scooter", "escooter", "patinete", "scooter"],
    "robots-cristales":     ["window", "glass", "cristal", "robot"],
    "robots-fregar":        ["robot", "mop", "floor", "fregar", "washing"],
    "robots-piscina":       ["pool", "piscina", "robot"],
}

CATEGORY_PART_TERMS = {
    "bateria": ["battery", "bateria", "pack", "rechargeable", "22.2v", "21.6v", "25.2v", "click-in", "click in"],
    "filtro": ["filter", "filtro", "hepa", "rear", "pre", "post", "membrane", "sediment", "carbon filter"],
    "cargador": ["charger", "cargador", "adapter", "adaptador", "power", "ac adapter"],
    "cepillo": ["brush", "cepillo", "head", "roller", "rodillo", "torque", "drive"],
    "soporte": ["wall", "mount", "holder", "dock", "stand", "bracket", "storage", "rack", "base"],
    "accesorios": ["accessory", "accessories", "kit", "crevice", "tool", "brush", "nozzle", "boquilla"],
    # CategorÃ­as de lavadora y electrodomÃ©sticos
    "bomba": ["pump", "drain pump", "drain", "impeller"],
    "resistencia": ["heating element", "heater", "heating", "resistencia", "thermostat"],
    "rodamiento": ["bearing", "drum bearing", "ball bearing"],
    "escobillas": ["carbon brush", "motor brush", "brush holder"],
    "correa": ["belt", "drive belt", "poly v", "poly-v", "v-belt"],
    "bolsa": ["dust bag", "vacuum bag", "paper bag"],
    # CategorÃ­as de Ã³smosis, cafeteras, freidoras, etc.
    "junta": ["seal", "gasket", "door seal", "o-ring", "rubber seal", "boot seal"],
    "deposito": ["tank", "container", "reservoir", "water tank", "dust cup"],
    "cesta": ["basket", "tray", "bin", "dust cup", "cup"],
    # CategorÃ­as de afeitadoras y cepillos elÃ©ctricos
    "rueda": ["tire", "tyre", "wheel", "inner tube", "tubeless"],
    "freno": ["brake", "brake pad", "disc brake", "brake disc", "caliper"],
    "laminas": ["foil", "shaving foil", "cutting foil", "blade", "foil replacement"],
    "cabezal": ["shaver head", "replacement head", "head", "rotary head"],
}

CATEGORY_QUERY_TERMS = {
    "bateria": ["battery pack", "replacement battery"],
    "filtro": ["hepa filter", "replacement filter"],
    "cargador": ["charger", "charging dock"],
    "cepillo": ["roller brush", "main brush"],
    "soporte": ["wall mount", "charging dock"],
    "accesorios": ["attachment", "nozzle"],
    "laminas": ["shaver foil", "replacement foil"],
    "cabezal": ["replacement head", "shaver head"],
    "junta": ["gasket", "o-ring"],
    "deposito": ["water tank", "dust cup"],
    "cesta": ["basket", "tray"],
    # CategorÃ­as de lavadora y electrodomÃ©sticos
    "bomba": ["drain pump", "pump"],
    "resistencia": ["heating element", "heater"],
    "rodamiento": ["bearing", "drum"],
    "escobillas": ["carbon brush", "motor brush"],
    "correa": ["drive belt", "belt"],
    "bolsa": ["dust bag", "bag"],
    "rueda": ["tire", "wheel"],
    "freno": ["brake", "pad"],
}

CATEGORY_REQUIRED_SIGNALS = {
    "bateria": ["battery", "battery pack", "bateria", "rechargeable", "li-ion", "li ion", "akku"],
    "filtro": ["filter", "hepa", "filtro", "cartridge", "membrane", "prefilter", "pre filter", "post filter"],
    "cargador": ["charger", "charging dock", "charging base", "ac adapter", "usb charger", "cargador"],
    "cepillo": ["brush", "roller brush", "main brush", "side brush", "rodillo", "cepillo"],
    "soporte": ["wall mount", "mount", "holder", "dock", "stand", "bracket", "base"],
    "accesorios": ["attachment", "tool", "nozzle", "boquilla", "hose", "wand", "crevice", "accessory"],
    "laminas": ["foil", "shaving foil", "cutting foil", "blade", "cuchilla"],
    "cabezal": ["replacement head", "shaver head", "brush head", "rotary head", "cabezal", "head"],
    "junta": ["gasket", "seal", "o-ring", "oring", "junta"],
    "deposito": ["tank", "reservoir", "dust cup", "water tank", "deposito", "container"],
    "cesta": ["basket", "tray", "bin", "bucket", "bandeja"],
    "bolsa": ["dust bag", "vacuum bag", "paper bag", "filter bag", "bolsa"],
    "bomba": ["pump", "drain pump", "drain", "impeller", "bomba"],
    "resistencia": ["heating element", "heater", "heating", "thermostat", "resistencia"],
    "rodamiento": ["bearing", "ball bearing", "drum bearing", "cojinete", "rodamiento"],
    "escobillas": ["carbon brush", "motor brush", "brush holder", "escobilla"],
    "correa": ["belt", "drive belt", "poly-v", "poly v", "v-belt", "correa"],
    "rueda": ["tire", "tyre", "wheel", "inner tube", "tubeless", "rueda"],
    "freno": ["brake", "brake pad", "brake disc", "caliper", "freno"],
}

CATEGORY_MIN_SIGNAL_HITS = {
    "accesorios": 1,
    "soporte": 1,
    "bateria": 1,
    "filtro": 1,
    "cargador": 1,
    "cepillo": 1,
    "laminas": 1,
    "cabezal": 1,
    "junta": 1,
    "deposito": 1,
    "cesta": 1,
    "bolsa": 1,
    "bomba": 1,
    "resistencia": 1,
    "rodamiento": 1,
    "escobillas": 1,
    "correa": 1,
    "rueda": 1,
    "freno": 1,
}

CATEGORY_NEGATIVE_TERMS = {
    # Para SKUs -nuevo: excluir recambios, filtros y accesorios â€” queremos el producto completo
    "nuevo": [
        "replacement filter", "filter replacement", "hepa filter", "pre filter", "post filter",
        "filtro de agua", "water filter replacement", "descal", "descaling", "limescale",
        "spare part", "spare parts", "recambio", "repuesto", "accessory", "accessories",
        "accesorios", "refill", "consumable",
        "dust bag", "bolsa", "cover", "funda", "protector cover",
        "pipe", "hose", "tube",
        "valve", "valvula", "gasket", "seal", "junta",
        "drive belt", "v-belt", "bearing", "carbon brush",
        "heating element", "thermostat",
        "wall mount", "bracket", "dock stand",
    ],
    "soporte": ["trigger", "switch", "button", "pcb", "board", "handle", "motor"],
    "bateria": ["trigger", "switch", "button", "filter", "charger", "dock", "wall mount"],
    "filtro": ["battery", "charger", "trigger", "switch", "button"],
    "cargador": ["battery", "filter", "trigger", "switch", "button", "dock", "holder", "bracket", "hanger", "wall mount", "storage rack"],
    "cepillo": ["battery", "filter", "charger", "trigger", "switch", "button"],
    # Lavadora: exclusiones cruzadas entre recambios incompatibles
    "bomba": ["belt", "bearing", "heating element", "heater", "seal", "gasket", "carbon brush", "filter"],
    "resistencia": ["pump", "drain", "belt", "bearing", "seal", "gasket", "carbon brush"],
    "rodamiento": ["pump", "drain", "belt", "heating element", "heater", "seal", "gasket", "carbon brush"],
    "escobillas": ["pump", "drain", "belt", "bearing", "heating element", "heater", "seal", "gasket"],
    "correa": ["pump", "drain", "bearing", "heating element", "heater", "seal", "gasket", "carbon brush"],
    "junta": ["pump", "drain", "belt", "bearing", "heating element", "heater", "carbon brush"],
    "bolsa": ["pump", "drain", "belt", "bearing", "heating element", "heater", "carbon brush"],
    "rueda": ["battery", "charger", "brake", "pad", "disc", "fender", "hook"],
    "freno": ["battery", "charger", "tire", "tyre", "wheel", "fender", "hook"],
}

VERTICAL_COMPLETE_TERMS = {
    "aspiradores":          ["vacuum cleaner", "robot vacuum", "stick vacuum", "cordless vacuum", "vacuum"],
    "afeitadoras":          ["electric shaver", "foil shaver", "rotary shaver", "shaver"],
    "aspiradoras-normales": ["vacuum cleaner", "canister vacuum", "upright vacuum", "vacuum"],
    "auriculares":          ["wireless earbuds", "bluetooth earbuds", "headphones", "earbuds", "headset"],
    "cafeteras":            ["coffee machine", "espresso machine", "coffee maker", "espresso maker", "cafetera"],
    "cepillos":             ["electric toothbrush", "sonic toothbrush", "toothbrush"],
    "secadores-pelo":       ["hair dryer", "blow dryer", "dryer"],
    "planchas-pelo":        ["hair straightener", "flat iron", "straightener"],
    "aspiradores-mano":     ["handheld vacuum", "portable vacuum", "mini vacuum", "vacuum cleaner"],
    "vaporetas":            ["steam cleaner", "steam mop", "handheld steamer"],
    "centros-planchado":    ["steam iron", "steam generator iron", "ironing station"],
    "freidoras":            ["air fryer", "airfryer", "fryer oven"],
    "herramientas":         ["cordless drill", "impact driver", "power tool", "drill", "tool set"],
    "lavadoras":            ["washing machine", "washer", "washer dryer"],
    "mascotas":             ["pet groomer", "dog clipper", "pet clipper", "grooming vacuum"],
    "osmosis":              ["reverse osmosis system", "water purifier", "ro system", "osmosis system"],
    "patinetes-electricos": ["electric scooter", "adult electric scooter", "e scooter", "escooter"],
    "robots-cristales":     ["window cleaning robot", "window cleaner robot", "glass cleaning robot"],
    "robots-fregar":        ["robot vacuum mop", "floor washing robot", "robot mop", "self-clean floor"],
    "robots-piscina":       ["pool cleaner robot", "robot pool cleaner", "robotic pool cleaner", "pool robot"],
}

VERTICAL_NEW_NEGATIVE_TERMS = {
    "aspiradores":          ["filter", "brush head", "roller brush", "battery pack", "charger", "dust bag", "mop pad", "main brush"],
    "afeitadoras":          ["replacement foil", "foil head", "blade", "replacement head", "charger", "shaving head"],
    "aspiradoras-normales": ["dust bag", "filter", "brush head", "hose", "belt", "charger"],
    "auriculares":          ["ear tips", "ear cushions", "ear pads", "charging case cover", "battery", "cable"],
    "cafeteras":            ["filter", "water filter", "capsule", "gasket", "seal", "descaler", "tank", "milk container", "drip tray", "portafilter"],
    "cepillos":             ["brush head", "replacement head", "charger", "travel case", "battery", "cabezal"],
    "secadores-pelo":       ["filter", "nozzle", "diffuser", "cable", "cord", "holder", "concentrator"],
    "planchas-pelo":        ["plate", "heater", "cable", "cord", "hinge", "case", "cover"],
    "aspiradores-mano":     ["filter", "battery", "charger", "nozzle", "brush head", "wall mount"],
    "vaporetas":            ["filter", "gasket", "seal", "tank", "mop pad", "nozzle", "hose"],
    "centros-planchado":    ["filter", "descaler", "gasket", "seal", "tank", "hose", "water tank"],
    "freidoras":            ["basket", "tray", "rack", "liner", "paper", "grill plate", "accessory", "silicone pot"],
    "herramientas":         ["battery", "charger", "drill bit", "saw blade", "socket", "accessory"],
    "lavadoras":            ["drain pump", "filter", "bearing", "gasket", "door seal", "heater", "belt"],
    "mascotas":             ["blade", "clipper blade", "comb", "filter", "hose", "battery", "charger"],
    "osmosis":              ["membrane", "filter cartridge", "sediment filter", "carbon filter", "housing", "o-ring", "faucet", "tank", "pump head"],
    "patinetes-electricos": ["tire", "tyre", "wheel", "inner tube", "battery", "charger", "brake pad", "brake disc", "fender", "mudguard", "hook"],
    "robots-cristales":     ["cleaning pad", "mop pad", "spray nozzle", "battery", "charger", "rope"],
    "robots-fregar":        ["mop pad", "filter", "main brush", "side brush", "battery", "charger", "dust bag"],
    "robots-piscina":       ["filter bag", "filter cartridge", "brush", "cable", "impeller", "caddy", "charger"],
}

VERTICAL_NEW_MIN_PRICE = {
    "aspiradores": 65.0,
    "afeitadoras": 40.0,
    "aspiradoras-normales": 80.0,
    "auriculares": 25.0,
    "cafeteras": 70.0,
    "cepillos": 20.0,
    "secadores-pelo": 45.0,
    "planchas-pelo": 35.0,
    "aspiradores-mano": 35.0,
    "vaporetas": 55.0,
    "centros-planchado": 90.0,
    "freidoras": 45.0,
    "herramientas": 45.0,
    "lavadoras": 180.0,
    "mascotas": 35.0,
    "osmosis": 70.0,
    "patinetes-electricos": 140.0,
    "robots-cristales": 90.0,
    "robots-fregar": 120.0,
    "robots-piscina": 180.0,
}

MODEL_TOKEN_RE = re.compile(r"\b(v\d{1,2}|sv\d{2}|dc\d{2,3})\b", re.IGNORECASE)
GENERIC_MODEL_WORD_RE = re.compile(r"[a-z0-9][a-z0-9+.-]{1,}", re.IGNORECASE)
QUERY_NOISE_RE = re.compile(
    r"\b(compatible|compatibles|para|repuesto|recambio|replacement|spare|kit|pack)\b",
    re.IGNORECASE,
)
QUERY_TOKEN_RE = re.compile(r"[^\W_]+(?:[.+/-][^\W_]+)*", re.IGNORECASE | re.UNICODE)
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

RELAXED_ANCHOR_STOPWORDS = {
    "accessory", "accessories", "accesorio", "accesorios", "replacement", "compatible",
    "spare", "part", "parts", "kit", "pack", "tool", "tools", "pieza", "piezas",
    "for", "para", "con", "sin", "robot", "vacuum", "cleaner", "brush", "filter",
    "battery", "charger", "adapter", "head", "roller", "dock", "mount", "wall",
    "tank", "container", "seal", "gasket", "basket", "tray", "bag",
}
SPECIFIC_ITEM_STOPWORDS = RELAXED_ANCHOR_STOPWORDS | {
    "group", "oring", "o-ring",
    # Familias de producto demasiado genÃ©ricas para decidir compatibilidad.
    "shaver", "razor", "coffee", "machine", "espresso", "maker",
    # Modificadores comerciales o de gama que no describen la pieza.
    "series", "pro", "plus", "one",
}
SPECIFIC_TERM_ALIASES = {
    "boquilla": ["nozzle", "crevice"],
    "boquillas": ["nozzle", "crevice"],
    "cepillo": ["brush"],
    "cepillos": ["brush"],
    "rodillo": ["roller"],
    "principal": ["main"],
    "soporte": ["holder", "mount"],
    "pared": ["wall", "wall mount"],
    "funda": ["case", "travel case"],
    "limpiador": ["cleaner", "cleaning"],
    "perfilador": ["trimmer"],
    "barbero": ["trimmer"],
    "laminas": ["foil"],
    "cabezal": ["head"],
    "deposito": ["tank", "reservoir"],
}
REQUIRED_TERM_ALIASES = {
    "accessory": ["attachment", "tool", "kit", "set"],
    "accessories": ["attachment", "tool", "kit", "set"],
    "accesorio": ["attachment", "tool", "kit"],
    "accesorios": ["attachment", "tool", "kit", "set"],
    "holder": ["stand", "bracket", "hanger", "dock", "base", "mount"],
    "stand": ["holder", "bracket", "dock", "base"],
    "bracket": ["holder", "stand", "mount", "hanger"],
    "mount": ["holder", "stand", "bracket", "dock"],
    "dock": ["holder", "stand", "base", "mount"],
    "charger": ["charging dock", "charging base", "power adapter", "ac adapter", "adapter"],
    "cargador": ["charging dock", "charging base", "power adapter", "ac adapter", "adapter"],
    "filter": ["hepa", "prefilter", "pre filter", "post filter", "mesh filter", "rear filter"],
    "filtro": ["hepa", "prefilter", "pre filter", "post filter", "mesh filter", "rear filter"],
    "brush": ["roller", "roller brush", "main brush", "side brush"],
    "cepillo": ["roller", "roller brush", "main brush", "side brush"],
    "roller": ["brush", "roller brush", "main brush", "soft roller", "fluffy roller"],
    "rodillo": ["brush", "roller brush", "main brush", "soft roller", "fluffy roller"],
    "trimmer": ["detail trimmer", "precision trimmer"],
    "foil": ["foil head", "shaving foil"],
}
QUERY_TERM_ALIASES = {
    "accessory": ["attachment", "attachments", "tool", "tools", "kit", "set"],
    "kit": ["set", "tool kit", "attachment kit"],
    "pet": ["pet groom", "pet tool", "mini motorized", "mini brush"],
    "holder": ["stand", "bracket", "hanger", "mount", "base"],
    "dock": ["charging dock", "charging base", "stand", "base", "holder"],
    "wall": ["wall mount", "wall holder", "wall bracket"],
    "charger": ["adapter", "power adapter", "ac adapter", "charger dock"],
    "adapter": ["charger", "power adapter", "ac adapter"],
    "filter": ["hepa", "prefilter", "post filter", "rear filter"],
    "roller": ["brush", "roller brush", "main brush", "soft roller"],
}

STRICT_RELAXED_CATEGORIES = {"accesorios", "soporte", "deposito", "cesta", "junta", "bolsa"}
AI_RESCUE_CATEGORIES = {
    "bateria", "filtro", "cargador", "cepillo", "laminas", "cabezal",
    "accesorios", "soporte", "deposito", "bolsa",
}
EXACT_SHARED_COMPAT_CATEGORIES = {
    "bateria", "filtro", "cargador", "cepillo", "laminas", "cabezal", "soporte",
}
EXACT_LOW_SPECIFICITY_CATEGORIES = {
    "cepillo", "soporte",
}
STRICT_EXACT_MATCH_CATEGORIES = {
    "bateria", "filtro", "cargador", "cepillo", "laminas", "cabezal",
    "accesorios", "soporte", "deposito", "cesta", "junta", "bolsa",
    "bomba", "resistencia", "rodamiento", "escobillas", "correa", "rueda", "freno",
}
RELAXED_FALLBACK_ALLOWED_CATEGORIES = {
    # Para un sistema de alta precisiÃ³n, el fallback relajado solo debe vivir
    # en categorÃ­as donde el riesgo de confundir pieza/modelo es bajo.
}
LOW_QUALITY_NEW_TITLE_TERMS = {
    "oem", "odm", "wholesale", "factory direct", "factory price", "supplier",
    "custom logo", "private label", "dropshipping", "bulk order",
}
SUSPICIOUS_TITLE_RE = re.compile(r"[*#%$]{2,}|\b[A-Z0-9]{7,}\b")


def looks_bad(title: str) -> bool:
    t = nrm(title)
    return any(b in t for b in BANNED_TITLE)


def cat_part_terms(cat: str) -> List[str]:
    c = nrm(cat)
    if c == "nuevo":
        # BÃºsqueda de producto completo: no exigir tÃ©rminos de recambio.
        # El filtrado se hace con CATEGORY_NEGATIVE_TERMS["nuevo"].
        return []
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
    # Fallback: usa el nombre de categorÃ­a como tÃ©rmino de ancla â€” mucho mÃ¡s especÃ­fico
    # que "replacement/spare" que pasarÃ­a cualquier producto del mundo.
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


def rescue_category_enabled(cat: str) -> bool:
    c = nrm(cat)
    return any(key in c for key in AI_RESCUE_CATEGORIES)


def candidate_pages_for_category(category: str) -> Tuple[int, ...]:
    cat = nrm(category)
    if cat in {"accesorios", "soporte", "cargador", "deposito", "bolsa"}:
        return (1, 2)
    return (1,)


def category_signal_terms(cat: str) -> List[str]:
    c = nrm(cat)
    for k, terms in CATEGORY_REQUIRED_SIGNALS.items():
        if k in c:
            return terms
    return []


def min_category_signal_hits(cat: str) -> int:
    c = nrm(cat)
    for k, min_hits in CATEGORY_MIN_SIGNAL_HITS.items():
        if k in c:
            return min_hits
    return 0


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


def title_has_required_brand(title: str, brand_hint: str) -> bool:
    folded_title = folded_nrm(title)
    folded_brand = folded_nrm(brand_hint)
    if not folded_brand:
        return True
    if folded_brand in folded_title:
        return True
    brand_tokens = [tok for tok in query_tokens(folded_brand) if len(tok) >= 3]
    if not brand_tokens:
        return True
    return any(tok in query_tokens(folded_title) for tok in brand_tokens)


def title_matches_vertical(title: str, vertical: str) -> bool:
    required_terms = VERTICAL_REQUIRED_TERMS.get(vertical, [])
    if not required_terms:
        return True
    tt = nrm(fold_query_text(title))
    return any(nrm(fold_query_text(term)) in tt for term in required_terms)


def count_distinct_models_in_title(title: str) -> int:
    tt = nrm(title)
    toks = set(m.group(1).lower() for m in MODEL_TOKEN_RE.finditer(tt))
    return len(toks)


def is_shared_compatibility_title(title: str) -> bool:
    tt = folded_nrm(title)
    return any(
        marker in tt
        for marker in (
            "compatible",
            "replacement",
            "spare",
            "for ",
            " for",
            "para ",
            "fit ",
            "fits ",
        )
    )


def model_mismatch_penalty(title: str, required: List[str], category: str = "") -> float:
    tt = nrm(title)
    toks = set(m.group(1).lower() for m in MODEL_TOKEN_RE.finditer(tt))
    if not toks:
        return 0.0
    if required and not any(r in toks for r in required):
        return 999.0

    extra = [t for t in toks if t not in required]
    if not extra:
        return 0.0

    cat = nrm(category)
    shared_ok = cat in {
        "bateria",
        "cargador",
        "filtro",
        "cepillo",
        "cabezal",
        "laminas",
        "deposito",
        "bolsa",
        "junta",
        "rueda",
        "rodamiento",
        "escobillas",
        "correa",
        "soporte",
        "accesorios",
    }

    if shared_ok and is_shared_compatibility_title(title):
        # Muchos recambios reales se venden como compatibles con una familia
        # corta de modelos. Seguimos penalizando, pero mucho menos.
        return min(float(len(extra)) * 1.1, 3.3)

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


def get_price_value(p: Dict[str, Any]) -> float:
    raw = p.get("sale_price") or p.get("original_price") or p.get("target_sale_price") or 0
    try:
        return float(str(raw).replace(",", "."))
    except Exception:
        return 0.0


def contains_all(title: str, must: List[str]) -> bool:
    return required_term_match_count(title, must) >= len([m for m in must if required_term_variants(m)])


def required_term_match_count(title: str, must: List[str]) -> int:
    tt = nrm(fold_query_text(title))
    hits = 0
    for m in must:
        variants = required_term_variants(m)
        if variants and any(variant in tt for variant in variants):
            hits += 1
    return hits


def contains_any(title: str, bad: List[str]) -> bool:
    tt = nrm(fold_query_text(title))
    return any((bb := nrm(fold_query_text(b))) and bb in tt for b in bad)


def is_model_like_token(token: str) -> bool:
    tok = nrm(fold_query_text(token))
    if not tok:
        return False
    if MODEL_TOKEN_RE.search(tok):
        return True
    return any(ch.isdigit() for ch in tok) and len(tok) <= 8


def effective_must_not_terms(title: str, category: str, req_models: List[str], must_not_include: List[str]) -> List[str]:
    cat = nrm(category)
    if cat in EXACT_SHARED_COMPAT_CATEGORIES and req_models and title_has_required_model(nrm(title), req_models) and is_shared_compatibility_title(title):
        return [term for term in must_not_include if not is_model_like_token(term)]
    return must_not_include


def min_specific_item_hits(category: str, specific_item_terms: List[str]) -> int:
    cat = nrm(category)
    if cat == "accesorios":
        # En accesorios seguimos exigiendo una seÃ±al especÃ­fica real, pero no
        # pedimos dos hits cuando solo hemos podido derivar un tÃ©rmino Ãºtil.
        return 1 if specific_item_terms else 0
    if cat in EXACT_LOW_SPECIFICITY_CATEGORIES:
        return 0
    if cat in EXACT_SHARED_COMPAT_CATEGORIES:
        return 0 if len(specific_item_terms) <= 1 else 1
    return 1 if specific_item_terms else 0


def required_term_variants(term: str) -> List[str]:
    raw = compact_spaces(str(term or ""))
    if not raw:
        return []
    base = nrm(fold_query_text(raw))
    variants = [base]
    variants.extend(REQUIRED_TERM_ALIASES.get(base, []))
    return list(dict.fromkeys(nrm(fold_query_text(variant)) for variant in variants if variant))


def must_include_satisfied(title: str, must_include: List[str], category: str) -> bool:
    active_terms = [term for term in must_include if required_term_variants(term)]
    if not active_terms:
        return True
    hits = required_term_match_count(title, active_terms)
    cat = nrm(category)
    if cat in {"soporte", "cargador", "accesorios"} and len(active_terms) >= 3:
        return hits >= (len(active_terms) - 1)
    return hits >= len(active_terms)


def query_term_variants(term: str) -> List[str]:
    raw = compact_spaces(str(term or ""))
    if not raw:
        return []
    base = nrm(fold_query_text(raw))
    variants = [raw]
    for alias in QUERY_TERM_ALIASES.get(base, []):
        variants.append(alias)
    return unique_keywords([compact_spaces(variant) for variant in variants if compact_spaces(variant)])


def expand_query_parts(parts: List[str], limit: int = 3) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for part in parts:
        for variant in query_term_variants(part):
            key = variant.lower()
            if key in seen:
                continue
            seen.add(key)
            out.append(variant)
            if len(out) >= limit:
                return out
    return out


def score_product(
    title: str,
    p: Dict[str, Any],
    must_brand: str,
    part_terms: List[str],
    req_models: List[str],
    category: str = "",
) -> float:
    t = nrm(title)
    s = 0.0

    if must_brand and must_brand in t:
        s += 6.0

    if req_models:
        if not title_has_required_model(t, req_models):
            return -1e9
        s -= model_mismatch_penalty(t, req_models, category)

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
    if n_models >= 4 and not (is_shared_compatibility_title(title) and nrm(category) != "nuevo"):
        s -= 5.0

    s += get_orders(p) * 0.015
    s += get_commission_rate(p) * 0.4

    return s


def is_complete_new_product(title: str, vertical: str) -> bool:
    tt = nrm(title)
    positives = VERTICAL_COMPLETE_TERMS.get(vertical, [])
    negatives = VERTICAL_NEW_NEGATIVE_TERMS.get(vertical, [])

    if positives and not any(nrm(term) in tt for term in positives):
        return False
    if negatives and any(nrm(term) in tt for term in negatives):
        return False
    return True


def looks_like_complete_product_for_category(title: str, vertical: str, category: str, part_terms: List[str]) -> bool:
    if not is_complete_new_product(title, vertical):
        return False
    if nrm(category) == "nuevo":
        return True
    tt = nrm(title)
    if any(pt in tt for pt in part_terms):
        return False
    return True


def min_new_product_price(vertical: str) -> float:
    return float(VERTICAL_NEW_MIN_PRICE.get(str(vertical or "").strip(), 35.0))


def score_new_product(
    title: str,
    p: Dict[str, Any],
    must_brand: str,
    req_models: List[str],
    vertical: str,
) -> float:
    t = nrm(title)
    price = get_price_value(p)
    if is_low_quality_new_title(title):
        return -1e9
    if not is_complete_new_product(t, vertical):
        return -1e9
    if price < min_new_product_price(vertical):
        return -1e9

    s = 0.0
    if must_brand and must_brand in t:
        s += 7.0

    if req_models:
        if not title_has_required_model(t, req_models):
            return -1e9
        s -= model_mismatch_penalty(t, req_models)

    # Para "nuevo" preferimos producto completo y precio creÃ­ble antes que comisiÃ³n.
    s += min(price, 3000.0) * 0.14
    s += get_orders(p) * 0.003
    s += get_commission_rate(p) * 0.05
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


def fold_query_text(text: str) -> str:
    folded = unicodedata.normalize("NFKD", str(text or ""))
    folded = "".join(ch for ch in folded if not unicodedata.combining(ch))
    return compact_spaces(folded)


def clean_query_fragment(text: str) -> str:
    cleaned = QUERY_NOISE_RE.sub(" ", normalize(text))
    return compact_spaces(cleaned)


def query_tokens(text: str) -> List[str]:
    return [tok.lower() for tok in QUERY_TOKEN_RE.findall(fold_query_text(text))]


def query_phrase(text: str, seen: set[str]) -> str:
    out: List[str] = []
    for tok in query_tokens(text):
        if tok in QUERY_NOISE_TERMS or tok in seen:
            continue
        seen.add(tok)
        out.append(tok)
    return " ".join(out)


def extract_relaxed_anchor_terms(
    ctx: Dict[str, Any],
    must_include: List[str],
    limit: int = 4,
) -> List[str]:
    parts = [
        *must_include,
        str(ctx.get("item_title") or ""),
        str(ctx.get("query") or ""),
    ]
    out: List[str] = []
    seen: set[str] = set()
    for part in parts:
        for tok in query_tokens(clean_query_fragment(part)):
            if tok in QUERY_NOISE_TERMS or tok in RELAXED_ANCHOR_STOPWORDS:
                continue
            if tok.isdigit() or (len(tok) < 3 and not any(ch.isdigit() for ch in tok)):
                continue
            if tok in seen:
                continue
            seen.add(tok)
            out.append(tok)
            if len(out) >= limit:
                return out
    return out


def extract_strict_anchor_terms(
    ctx: Dict[str, Any],
    must_include: List[str],
    model_tokens: List[str],
    limit: int = 6,
) -> List[str]:
    parts = [
        *must_include,
        *model_tokens,
        str(ctx.get("item_title") or ""),
        str(ctx.get("query") or ""),
        str(ctx.get("model") or ""),
    ]
    out: List[str] = []
    seen: set[str] = set()
    for part in parts:
        for tok in query_tokens(clean_query_fragment(part)):
            if tok in QUERY_NOISE_TERMS or tok in RELAXED_ANCHOR_STOPWORDS:
                continue
            if tok.isdigit() or (len(tok) < 3 and not any(ch.isdigit() for ch in tok)):
                continue
            if tok in seen:
                continue
            seen.add(tok)
            out.append(tok)
            if len(out) >= limit:
                return out
    return out


def extract_specific_item_terms(
    ctx: Dict[str, Any],
    must_include: List[str],
    model_tokens: List[str],
    limit: int = 4,
) -> List[str]:
    parts = [
        *must_include,
        str(ctx.get("item_title") or ""),
        str(ctx.get("query") or ""),
    ]
    blocked = {nrm(fold_query_text(tok)) for tok in model_tokens if tok}
    blocked.update(
        tok
        for tok in query_tokens(str(ctx.get("brand") or ""))
        if len(tok) >= 3
    )
    blocked.update(
        tok
        for tok in query_tokens(str(ctx.get("model") or ""))
        if len(tok) >= 3
    )
    out: List[str] = []
    seen: set[str] = set()
    for part in parts:
        for tok in query_tokens(clean_query_fragment(part)):
            norm_tok = nrm(fold_query_text(tok))
            if not norm_tok:
                continue
            if norm_tok in QUERY_NOISE_TERMS or norm_tok in SPECIFIC_ITEM_STOPWORDS:
                continue
            if norm_tok in blocked:
                continue
            if norm_tok.isdigit() or (len(norm_tok) < 3 and not any(ch.isdigit() for ch in norm_tok)):
                continue
            if norm_tok in seen:
                continue
            seen.add(norm_tok)
            out.append(norm_tok)
            if len(out) >= limit:
                return out
    return out


def expand_specific_item_terms(terms: List[str]) -> List[str]:
    out: List[str] = []
    seen: set[str] = set()
    for term in terms:
        raw = compact_spaces(str(term or ""))
        if not raw:
            continue
        variants = [raw]
        if "/" in raw:
            variants.extend([compact_spaces(part) for part in raw.split("/") if compact_spaces(part)])
        alias_variants: List[str] = []
        for variant in variants:
            alias_variants.extend(SPECIFIC_TERM_ALIASES.get(nrm(fold_query_text(variant)), []))
        variants.extend(alias_variants)
        for variant in variants:
            norm_variant = nrm(fold_query_text(variant))
            if not norm_variant or norm_variant in seen:
                continue
            seen.add(norm_variant)
            out.append(norm_variant)
    return out


def count_anchor_hits(title: str, terms: List[str]) -> int:
    tt = nrm(fold_query_text(title))
    hits = 0
    for term in terms:
        norm_term = nrm(fold_query_text(term))
        if norm_term and norm_term in tt:
            hits += 1
    return hits


def title_matches_category_signals(title: str, category: str, specific_item_terms: List[str]) -> bool:
    if nrm(category) == "nuevo":
        return True
    signal_terms = category_signal_terms(category)
    min_hits = min_category_signal_hits(category)
    if not signal_terms or min_hits <= 0:
        return True
    signal_hits = count_anchor_hits(title, signal_terms)
    if signal_hits >= min_hits:
        return True

    # En accesorios y cubetas amplias dejamos que el item_title especÃ­fico
    # rescate el match si el anuncio no usa la taxonomÃ­a exacta.
    if nrm(category) == "accesorios" and specific_item_terms:
        return count_anchor_hits(title, specific_item_terms) >= 1

    return False


def is_deceptive_title(title: str, category: str) -> bool:
    tt = nrm(title)
    if SUSPICIOUS_TITLE_RE.search(title) and not is_shared_compatibility_title(title):
        return True
    if category != "nuevo" and ("universal" in tt or "all model" in tt or "all models" in tt):
        return True
    return False


def is_low_quality_new_title(title: str) -> bool:
    tt = nrm(title)
    if SUSPICIOUS_TITLE_RE.search(title):
        return True
    return any(term in tt for term in LOW_QUALITY_NEW_TITLE_TERMS)


def derive_compatibility_status(offer_obj: Dict[str, Any]) -> str:
    match_type = str(offer_obj.get("match_type") or "").strip()
    needs_url = bool(offer_obj.get("needs_url"))
    orphaned = bool(offer_obj.get("orphaned"))
    url_now = str(offer_obj.get("url") or "").strip()
    ai_status = str(offer_obj.get("ai_validation_status") or "").strip()

    if orphaned:
        return "sin_cobertura"
    if ai_status == "pending":
        return "pending_ai_validation"
    if match_type == "exact_or_best_match":
        return "compatible_alto"
    if match_type == "relaxed_fallback":
        return "dudoso"
    if match_type == "fallback_buy_new":
        return "fallback_buy_new"
    if needs_url and not url_now:
        return "sin_cobertura"
    if match_type == "manual_or_existing" and url_now:
        return "compatible_probable"
    if url_now and not needs_url:
        return "compatible_probable"
    return "sin_cobertura"


def derive_compatibility_note(status: str) -> str:
    if status == "pending_ai_validation":
        return "Pendiente de validacion final por IA"
    if status == "compatible_alto":
        return "Compatibilidad alta por modelo y tipo de pieza"
    if status == "compatible_probable":
        return "Compatibilidad probable: revisa modelo y encaje"
    if status == "dudoso":
        return "Coincidencia heuristica: valida compatibilidad antes de comprar"
    if status == "fallback_buy_new":
        return "Sin recambio fiable: mejor ver producto nuevo"
    return "Sin cobertura fiable de recambio"


def compatibility_priority(status: str) -> int:
    priorities = {
        "pending_ai_validation": -1,
        "fallback_buy_new": 0,
        "sin_cobertura": 1,
        "dudoso": 2,
        "compatible_probable": 3,
        "compatible_alto": 4,
    }
    return priorities.get(str(status or "").strip(), 5)


def ai_validation_is_gemini() -> bool:
    provider_url = f"{AI_VALIDATION_PROVIDER} {AI_VALIDATION_URL}".lower()
    return "gemini" in provider_url or "generativelanguage.googleapis.com" in provider_url


def normalize_ai_model_name(model: str) -> str:
    raw = str(model or "").strip()
    if raw.startswith("models/"):
        raw = raw.split("/", 1)[1].strip()
    folded = raw.lower()
    compact = re.sub(r"[^a-z0-9]+", "", folded)
    aliases = {
        # AI Studio may show newer/free-tier labels before the OpenAI-compatible
        # endpoint exposes them. Use the stable, documented Flash-Lite id.
        "gemini-3.1-flash-lite": "gemini-2.5-flash-lite",
        "gemini-3-flash-lite": "gemini-2.5-flash-lite",
    }
    compact_aliases = {
        "gemini31flashlite": "gemini-2.5-flash-lite",
        "gemini3flashlite": "gemini-2.5-flash-lite",
        "gemini25flashlite": "gemini-2.5-flash-lite",
        "gemini25flash": "gemini-2.5-flash",
    }
    return aliases.get(folded) or compact_aliases.get(compact) or raw


def ai_validation_model_candidates() -> List[str]:
    raw_models: List[str] = []
    if _ai_validation_active_model:
        raw_models.append(_ai_validation_active_model)
    raw_models.extend(x.strip() for x in str(AI_VALIDATION_MODEL or "").split(",") if x.strip())
    raw_models.extend(AI_VALIDATION_FALLBACK_MODELS)
    if ai_validation_is_gemini():
        raw_models.extend(["gemini-2.5-flash-lite", "gemini-2.5-flash"])

    models: List[str] = []
    seen: set[str] = set()
    for raw_model in raw_models:
        model = normalize_ai_model_name(raw_model)
        key = model.lower()
        if model and key not in seen:
            models.append(model)
            seen.add(key)
    return models


def effective_ai_validation_model() -> str:
    models = ai_validation_model_candidates()
    return models[0] if models else AI_VALIDATION_MODEL


def ai_validation_enabled() -> bool:
    return bool(AI_VALIDATION_PROVIDER and AI_VALIDATION_API_KEY and ai_validation_model_candidates())


def init_ai_budget_state(offers_doc: Dict[str, Any], today: str) -> None:
    global _ai_budget_state
    state = offers_doc.get("ai_validation_budget")
    if not isinstance(state, dict):
        state = {}
    if str(state.get("date") or "") != today:
        state = {"date": today, "used": 0, "limit": AI_VALIDATION_MAX_CHECKS}
    else:
        state["limit"] = AI_VALIDATION_MAX_CHECKS
        state["used"] = int(state.get("used") or 0)
    offers_doc["ai_validation_budget"] = state
    _ai_budget_state = state


def ai_budget_used() -> int:
    return int(_ai_budget_state.get("used") or 0)


def ai_budget_limit() -> int:
    return int(_ai_budget_state.get("limit") or 0)


def consume_ai_budget(count: int = 1) -> None:
    _ai_budget_state["used"] = max(0, ai_budget_used() + max(0, int(count)))


def exhaust_ai_budget() -> None:
    if ai_budget_limit() > 0:
        _ai_budget_state["used"] = ai_budget_limit()


def format_ai_budget_status() -> str:
    limit = ai_budget_limit()
    used = ai_budget_used()
    if limit > 0:
        return f"{used}/{limit}"
    return f"{used}/unlimited"


UNRESOLVED_UPDATED_AT = "0000-00-00"


def candidate_fingerprint(candidate: Dict[str, Any]) -> str:
    payload = {
        "url": str(candidate.get("url") or "").strip(),
        "product_title": str(candidate.get("product_title") or "").strip(),
        "sale_price": str(candidate.get("sale_price") or "").strip(),
        "sale_price_currency": str(candidate.get("sale_price_currency") or "").strip(),
        "original_price": str(candidate.get("original_price") or "").strip(),
    }
    return hashlib.sha1(json.dumps(payload, sort_keys=True, ensure_ascii=False).encode("utf-8")).hexdigest()[:16]


def product_fingerprint(product: Dict[str, Any]) -> str:
    return candidate_fingerprint({
        "url": str(product.get("promotion_link") or product.get("product_detail_url") or "").strip(),
        "product_title": str(product.get("product_title") or "").strip(),
        "sale_price": str(product.get("sale_price") or "").strip(),
        "sale_price_currency": str(product.get("sale_price_currency") or "").strip(),
        "original_price": str(product.get("original_price") or "").strip(),
    })


def rejected_candidate_fingerprints(offer_obj: Dict[str, Any]) -> set[str]:
    vals = ensure_list_str(offer_obj.get("ai_rejected_candidate_fingerprints"))
    return {str(v).strip() for v in vals if str(v).strip()}


def append_rejected_candidate_fingerprint(offer_obj: Dict[str, Any], fingerprint: str) -> None:
    if not fingerprint:
        return
    vals = rejected_candidate_fingerprints(offer_obj)
    vals.add(fingerprint)
    offer_obj["ai_rejected_candidate_fingerprints"] = sorted(vals)


def clear_ai_pending_candidate(offer_obj: Dict[str, Any]) -> None:
    for key in (
        "ai_pending_candidate",
        "ai_pending_reason",
        "ai_pending_attempts",
        "ai_validation_reason",
        "ai_validation_model",
        "ai_validation_at",
        "ai_validation_candidate_fingerprint",
    ):
        offer_obj.pop(key, None)


def apply_offer_candidate(
    offer_obj: Dict[str, Any],
    candidate: Dict[str, Any],
    *,
    match_type: str,
    matched_query: str,
    fallback_search_query: str,
    fallback_search_label: str,
    today: str,
) -> None:
    current_ai_status = str(offer_obj.get("ai_validation_status") or "").strip()
    if current_ai_status != "validated":
        offer_obj.pop("ai_validation_status", None)
        clear_ai_pending_candidate(offer_obj)
    offer_obj["url"] = str(candidate.get("url") or "").strip()
    offer_obj["match_type"] = match_type
    if matched_query:
        offer_obj["matched_query"] = matched_query
    else:
        offer_obj.pop("matched_query", None)
    offer_obj["fallback_search_query"] = fallback_search_query
    offer_obj["fallback_search_label"] = fallback_search_label
    for key in ("image_url", "sale_price", "sale_price_currency", "original_price", "discount", "product_title"):
        val = str(candidate.get(key) or "").strip()
        if val:
            offer_obj[key] = val
        else:
            offer_obj.pop(key, None)
    offer_obj.pop("needs_url", None)
    offer_obj["compatibility_status"] = derive_compatibility_status(offer_obj)
    offer_obj["compatibility_note"] = derive_compatibility_note(offer_obj["compatibility_status"])
    offer_obj["updated_at"] = today
    offer_obj["last_attempted_at"] = today
    for debug_key in (
        "debug_last_query",
        "debug_model_tokens",
        "debug_must_include",
        "debug_must_not_include",
        "debug_specific_item_terms",
        "debug_relaxed_allowed",
        "debug_failure_stage",
    ):
        offer_obj.pop(debug_key, None)


def apply_doubtful_candidate(
    offer_obj: Dict[str, Any],
    candidate: Dict[str, Any],
    *,
    matched_query: str,
    fallback_search_query: str,
    fallback_search_label: str,
    reason: str,
    today: str,
) -> None:
    offer_obj["ai_validation_status"] = "doubtful"
    offer_obj["ai_validation_reason"] = reason
    offer_obj["ai_validation_model"] = effective_ai_validation_model()
    offer_obj["ai_validation_at"] = today
    offer_obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(candidate)
    apply_offer_candidate(
        offer_obj,
        candidate,
        match_type="relaxed_fallback",
        matched_query=matched_query,
        fallback_search_query=fallback_search_query,
        fallback_search_label=fallback_search_label,
        today=today,
    )
    offer_obj["ai_validation_status"] = "doubtful"
    offer_obj["ai_validation_reason"] = reason
    offer_obj["ai_validation_model"] = effective_ai_validation_model()
    offer_obj["ai_validation_at"] = today
    offer_obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(candidate)
    offer_obj["compatibility_status"] = derive_compatibility_status(offer_obj)
    offer_obj["compatibility_note"] = derive_compatibility_note(offer_obj["compatibility_status"])


def stage_candidate_for_ai(
    offer_obj: Dict[str, Any],
    candidate: Dict[str, Any],
    *,
    reason: str,
    today: str,
) -> None:
    fp = candidate_fingerprint(candidate)
    current_fp = str(offer_obj.get("ai_validation_candidate_fingerprint") or "").strip()
    attempts = int(offer_obj.get("ai_pending_attempts") or 0)
    if current_fp != fp:
        attempts = 0
    offer_obj["url"] = ""
    offer_obj["needs_url"] = True
    offer_obj["match_type"] = ""
    offer_obj["ai_validation_status"] = "pending"
    offer_obj["ai_validation_reason"] = reason
    offer_obj["ai_pending_reason"] = reason
    offer_obj["ai_pending_attempts"] = attempts + 1
    offer_obj["ai_validation_model"] = effective_ai_validation_model()
    offer_obj["ai_validation_at"] = today
    offer_obj["ai_validation_candidate_fingerprint"] = fp
    offer_obj["ai_pending_candidate"] = dict(candidate)
    offer_obj["compatibility_status"] = derive_compatibility_status(offer_obj)
    offer_obj["compatibility_note"] = derive_compatibility_note(offer_obj["compatibility_status"])
    offer_obj["updated_at"] = UNRESOLVED_UPDATED_AT
    offer_obj["last_attempted_at"] = today


def shortlist_ai_payload(ctx: Dict[str, Any], candidates: List[Dict[str, Any]]) -> str:
    data = {
        "brand": ctx.get("brand") or "",
        "model": ctx.get("model") or "",
        "vertical": ctx.get("vertical") or "",
        "category": ctx.get("category") or "",
        "item_title": ctx.get("item_title") or "",
        "must_include": ensure_list_str(ctx.get("must_include")),
        "must_not_include": ensure_list_str(ctx.get("must_not_include")),
        "model_tokens": ensure_list_str(ctx.get("model_tokens")),
        "candidates": [
            {
                "id": idx + 1,
                "title": cand.get("product_title") or "",
                "url": cand.get("url") or "",
                "price": cand.get("sale_price") or "",
                "currency": cand.get("sale_price_currency") or "",
                "query": cand.get("matched_query") or "",
                "tier": cand.get("candidate_tier") or "",
            }
            for idx, cand in enumerate(candidates)
        ],
    }
    return json.dumps(data, ensure_ascii=False, indent=2)


def ai_prompt_payload(ctx: Dict[str, Any], candidate: Dict[str, Any]) -> str:
    data = {
        "brand": ctx.get("brand") or "",
        "model": ctx.get("model") or "",
        "vertical": ctx.get("vertical") or "",
        "category": ctx.get("category") or "",
        "item_title": ctx.get("item_title") or "",
        "must_include": ensure_list_str(ctx.get("must_include")),
        "must_not_include": ensure_list_str(ctx.get("must_not_include")),
        "model_tokens": ensure_list_str(ctx.get("model_tokens")),
        "candidate_title": candidate.get("product_title") or "",
        "candidate_url": candidate.get("url") or "",
        "candidate_price": candidate.get("sale_price") or "",
        "candidate_price_currency": candidate.get("sale_price_currency") or "",
    }
    return json.dumps(data, ensure_ascii=False, indent=2)


def parse_ai_json(text: str) -> Dict[str, Any]:
    text = str(text or "").strip()
    if not text:
        return {}
    try:
        return json.loads(text)
    except Exception:
        pass
    m = re.search(r"\{.*\}", text, re.DOTALL)
    if not m:
        return {}
    try:
        return json.loads(m.group(0))
    except Exception:
        return {}


def wants_ai_response_format() -> bool:
    if AI_VALIDATION_RESPONSE_FORMAT in {"1", "true", "yes", "on"}:
        return True
    if AI_VALIDATION_RESPONSE_FORMAT in {"0", "false", "no", "off"}:
        return False
    return not ai_validation_is_gemini()


def response_is_ai_model_not_found(response: requests.Response) -> bool:
    if response.status_code != 404:
        return False
    body = nrm(response.text or "")
    return "model" in body and ("not found" in body or "not_found" in body)


def call_ai_json(system_prompt: str, user_prompt: str) -> Dict[str, str]:
    global _ai_validation_calls, _ai_validation_active_model

    if not ai_validation_enabled():
        return {"status": "disabled", "reason": "ai_validation_disabled"}
    if ai_budget_limit() > 0 and ai_budget_used() >= ai_budget_limit():
        return {"status": "pending", "reason": "daily_ai_budget_exhausted"}

    headers = {
        "Authorization": f"Bearer {AI_VALIDATION_API_KEY}",
        "Content-Type": "application/json",
    }
    last_model_not_found = ""

    for model in ai_validation_model_candidates():
        payload = {
            "model": model,
            "temperature": 0,
            "messages": [
                {"role": "system", "content": system_prompt},
                {"role": "user", "content": user_prompt},
            ],
        }
        if wants_ai_response_format():
            payload["response_format"] = {"type": "json_object"}

        try:
            r = requests.post(AI_VALIDATION_URL, headers=headers, json=payload, timeout=AI_VALIDATION_TIMEOUT)
            _ai_validation_calls += 1
        except Exception:
            return {"status": "pending", "reason": "ai_validation_request_failed"}

        if response_is_ai_model_not_found(r):
            last_model_not_found = f"ai_model_not_found_{reason_key(model, 60)}"
            continue

        consume_ai_budget(1)
        if r.status_code == 429:
            exhaust_ai_budget()
            return {"status": "pending", "reason": "ai_rate_limited"}
        if r.status_code >= 500:
            return {"status": "pending", "reason": f"ai_server_error_{r.status_code}"}
        if r.status_code >= 400:
            reason = f"ai_http_{r.status_code}"
            try:
                err = r.json() or {}
                msg = err.get("error", {}).get("message") if isinstance(err.get("error"), dict) else ""
                if msg:
                    reason = f"{reason}_{reason_key(str(msg), 80)}"
            except Exception:
                body = normalize(r.text or "")
                if body:
                    reason = f"{reason}_{reason_key(body, 80)}"
            return {"status": "error", "reason": reason}

        _ai_validation_active_model = model
        try:
            data = r.json() or {}
        except ValueError:
            body = normalize(r.text or "")
            suffix = f"_{reason_key(body, 80)}" if body else ""
            return {"status": "pending", "reason": f"ai_invalid_json_response_{r.status_code}{suffix}"}
        choices = data.get("choices") or []
        message = choices[0].get("message", {}) if choices and isinstance(choices[0], dict) else {}
        content = message.get("content") or ""
        parsed = parse_ai_json(content)
        if not parsed:
            return {"status": "pending", "reason": "ai_unparseable_response"}
        return {"status": "ok", "reason": "", "content": parsed}

    return {"status": "pending", "reason": last_model_not_found or "ai_model_not_found"}


def validate_candidate_with_ai(ctx: Dict[str, Any], candidate: Dict[str, Any]) -> Dict[str, str]:
    system_prompt = (
        "You validate whether an ecommerce product listing really matches a spare part request. "
        "Reply only as JSON with keys verdict and reason. "
        "verdict must be one of: valid, doubtful, invalid."
    )
    user_prompt = (
        "Decide if the candidate listing is a correct match for the requested spare part. "
        "Return invalid if the title suggests another product type, another ecosystem, or lacks enough evidence. "
        "Return valid when brand/model or compatible family and part type clearly fit. "
        "Return doubtful when it is the right ecosystem and part family but title evidence is incomplete. "
        "Return invalid only when it is clearly wrong or unrelated.\n\n"
        f"{ai_prompt_payload(ctx, candidate)}"
    )
    raw = call_ai_json(system_prompt, user_prompt)
    if raw.get("status") != "ok":
        return {"status": raw.get("status") or "pending", "reason": raw.get("reason") or "ai_unknown_error"}
    parsed = raw.get("content") or {}
    verdict = str(parsed.get("verdict") or parsed.get("status") or "").strip().lower()
    reason = normalize(str(parsed.get("reason") or "")) or "ai_no_reason"
    if verdict in {"valid", "approved", "approve", "match"}:
        return {"status": "validated", "reason": reason}
    if verdict in {"invalid", "reject", "rejected", "wrong"}:
        return {"status": "rejected", "reason": reason}
    if verdict in {"doubtful", "uncertain", "unsure"}:
        return {"status": "doubtful", "reason": reason}
    return {"status": "pending", "reason": "ai_unparseable_response"}


def choose_best_candidate_with_ai(ctx: Dict[str, Any], candidates: List[Dict[str, Any]]) -> Dict[str, Any]:
    if not candidates:
        return {"status": "none", "reason": "no_candidates"}

    system_prompt = (
        "You choose the best ecommerce listing for a spare part request. "
        "Reply only as JSON with keys verdict, best_id and reason. "
        "verdict must be one of: valid, invalid, doubtful. "
        "best_id must be an integer candidate id when verdict is valid or doubtful, otherwise 0."
    )
    user_prompt = (
        "From the candidate list, choose the single best match for the requested spare part. "
        "Return valid when brand/model or compatible family and part type clearly fit. "
        "Return doubtful when the best candidate fits the ecosystem and part family but compatibility evidence is incomplete. "
        "Return invalid only if none are plausibly related. "
        "Prefer exact ecosystem, exact part type, and clear compatibility.\n\n"
        f"{shortlist_ai_payload(ctx, candidates)}"
    )
    raw = call_ai_json(system_prompt, user_prompt)
    if raw.get("status") != "ok":
        return {"status": raw.get("status") or "pending", "reason": raw.get("reason") or "ai_unknown_error"}

    parsed = raw.get("content") or {}
    verdict = str(parsed.get("verdict") or parsed.get("status") or "").strip().lower()
    reason = normalize(str(parsed.get("reason") or "")) or "ai_no_reason"
    try:
        best_id = int(parsed.get("best_id") or 0)
    except Exception:
        best_id = 0

    if verdict in {"valid", "approved", "approve", "match"} and 1 <= best_id <= len(candidates):
        chosen = dict(candidates[best_id - 1])
        return {"status": "validated", "reason": reason, "candidate": chosen}
    if verdict in {"doubtful", "uncertain", "unsure"} and 1 <= best_id <= len(candidates):
        chosen = dict(candidates[best_id - 1])
        return {"status": "doubtful", "reason": reason, "candidate": chosen}
    if verdict in {"invalid", "reject", "rejected", "wrong"}:
        return {"status": "rejected", "reason": reason}
    return {"status": "pending", "reason": "ai_unparseable_response"}


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
    category = str(ctx.get("category") or "")
    category_terms = " ".join(cat_query_terms(category)[:2])
    include_terms = " ".join([compact_spaces(x) for x in must_include[:4] if compact_spaces(x)])
    model_tokens = " ".join(model_tokens_from_ctx(model)[:3])
    include_variants = expand_query_parts(must_include, limit=4)
    if nrm(category) == "soporte":
        support_terms = " ".join(expand_query_parts(["dock", "wall", "holder"], limit=4))
        candidates = [
            compose_search_query([], [query_override]),
            compose_search_query([brand, model], [support_terms]),
            compose_search_query([brand, model], ["wall mount", "holder"]),
            compose_search_query([brand, model], ["charging dock", "stand"]),
            compose_search_query([brand, model], [include_terms, category_terms]),
        ]
        return unique_keywords(candidates)[:max(1, MAX_EXACT_KEYWORDS)]
    if nrm(category) == "cargador":
        charger_terms = " ".join(expand_query_parts(["charger", "adapter"], limit=4))
        candidates = [
            compose_search_query([], [query_override]),
            compose_search_query([brand, model], [charger_terms]),
            compose_search_query([brand, model], ["charger", "adapter"]),
            compose_search_query([brand, model], ["power adapter", "ac adapter"]),
            compose_search_query([brand, model], [include_terms, category_terms]),
        ]
        return unique_keywords(candidates)[:max(1, MAX_EXACT_KEYWORDS)]
    if nrm(category) == "accesorios":
        specific_terms = " ".join(
            expand_specific_item_terms(
                extract_specific_item_terms(ctx, must_include, model_tokens_from_ctx(model))
            )[:3]
        )
        accessory_terms = " ".join(include_variants[:3])
        candidates = [
            compose_search_query([], [query_override]),
            compose_search_query([brand, model], [accessory_terms]),
            compose_search_query([brand, model], [specific_terms, "accessory kit"]),
            compose_search_query([brand, model], [specific_terms, "tool kit"]),
            compose_search_query([brand, model], [specific_terms, "attachment set"]),
            compose_search_query([brand, model], [include_terms]),
            compose_search_query([brand], [model, specific_terms, "accessory"]),
        ]
        return unique_keywords(candidates)[:max(1, MAX_EXACT_KEYWORDS)]

    # Cascada de mayor a menor precisiÃ³n:
    # 1. Query override explÃ­cito
    # 2. Marca + modelo completo + tÃ©rminos must_include + tipo recambio
    # 3. Marca + modelo completo + tipo recambio
    # 4. Marca + tokens de modelo + tipo recambio
    # 5. Marca + modelo + tÃ­tulo del item
    # 6. Marca + modelo + nombre categorÃ­a + tÃ­tulo
    # 7. Marca + tipo recambio (sin modelo â€” cubre listings genÃ©ricos de marca)
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
    return unique_keywords(candidates)[:max(1, MAX_EXACT_KEYWORDS)]


def build_ai_rescue_keywords(ctx: Dict[str, Any], query_override: str, must_include: List[str]) -> List[str]:
    brand = compact_spaces(str(ctx.get("brand") or ""))
    model = compact_spaces(str(ctx.get("model") or ""))
    item_title = clean_query_fragment(str(ctx.get("item_title") or ""))
    category = compact_spaces(str(ctx.get("category") or ""))
    category_terms = " ".join(cat_query_terms(category)[:2])
    part_terms = " ".join(cat_part_terms(category)[:2])
    include_terms = " ".join([compact_spaces(x) for x in must_include[:3] if compact_spaces(x)])
    model_tokens_list = model_tokens_from_ctx(model)
    model_family = " ".join(model_tokens_list[:2])
    include_variants = expand_query_parts(must_include, limit=4)
    specific_terms = " ".join(
        expand_specific_item_terms(
            extract_specific_item_terms(ctx, must_include, model_tokens_list)
        )[:3]
    )
    if nrm(category) == "soporte":
        support_terms = " ".join(expand_query_parts(["dock", "wall", "holder"], limit=4))
        candidates = [
            compose_search_query([], [query_override]),
            compose_search_query([brand, model], [support_terms]),
            compose_search_query([brand, model], ["wall mount", "holder"]),
            compose_search_query([brand, model], ["charging dock", "stand"]),
            compose_search_query([brand, model], ["bracket", "storage rack"]),
            compose_search_query([brand], [model_family, "wall mount", "holder"]),
            compose_search_query([brand], [model_family, "bracket"]),
        ]
        return unique_keywords(candidates)[:max(1, MAX_RESCUE_KEYWORDS)]
    if nrm(category) == "cargador":
        charger_terms = " ".join(expand_query_parts(["charger", "adapter"], limit=4))
        candidates = [
            compose_search_query([], [query_override]),
            compose_search_query([brand, model], [charger_terms]),
            compose_search_query([brand, model], ["charger", "adapter"]),
            compose_search_query([brand, model], ["power adapter", "ac adapter"]),
            compose_search_query([brand], [model_family, "charger", "adapter"]),
        ]
        return unique_keywords(candidates)[:max(1, MAX_RESCUE_KEYWORDS)]
    if nrm(category) == "accesorios":
        accessory_terms = " ".join(include_variants[:3])
        candidates = [
            compose_search_query([], [query_override]),
            compose_search_query([brand, model], [accessory_terms]),
            compose_search_query([brand, model], [specific_terms, "accessory kit"]),
            compose_search_query([brand, model], [specific_terms, "tool kit"]),
            compose_search_query([brand, model], [specific_terms, "attachment set"]),
            compose_search_query([brand], [model_family, specific_terms, "accessory"]),
            compose_search_query([brand], [specific_terms, "attachment"]),
        ]
        return unique_keywords(candidates)[:max(1, MAX_RESCUE_KEYWORDS)]

    candidates = [
        compose_search_query([brand, model], [item_title, part_terms]),
        compose_search_query([brand, model], [category_terms, part_terms]),
        compose_search_query([brand, model], [include_terms, "compatible"]),
        compose_search_query([brand, model_family], [item_title, part_terms]),
        compose_search_query([brand, model_family], [category_terms, "compatible"]),
        compose_search_query([brand], [model_family, item_title, part_terms]),
        compose_search_query([brand], [item_title, part_terms, "replacement"]),
        compose_search_query([], [query_override, item_title]),
    ]
    return unique_keywords(candidates)[:max(1, MAX_RESCUE_KEYWORDS)]


def build_ai_wide_keywords(ctx: Dict[str, Any], query_override: str, must_include: List[str]) -> List[str]:
    brand = compact_spaces(str(ctx.get("brand") or ""))
    model = compact_spaces(str(ctx.get("model") or ""))
    item_title = clean_query_fragment(str(ctx.get("item_title") or ""))
    category = compact_spaces(str(ctx.get("category") or ""))
    category_terms = " ".join(cat_query_terms(category)[:2])
    part_terms = " ".join(cat_part_terms(category)[:2])
    include_terms = " ".join([compact_spaces(x) for x in must_include[:3] if compact_spaces(x)])
    model_tokens_list = model_tokens_from_ctx(model)
    model_family = " ".join(model_tokens_list[:2])
    specific_terms = " ".join(
        expand_specific_item_terms(
            extract_specific_item_terms(ctx, must_include, model_tokens_list)
        )[:3]
    )
    broad_part = specific_terms or include_terms or item_title or category_terms or part_terms
    candidates = [
        compose_search_query([], [query_override]),
        compose_search_query([brand, model], [broad_part]),
        compose_search_query([brand, model], [category_terms]),
        compose_search_query([brand], [model_family, broad_part]),
        compose_search_query([model], [broad_part]),
        compose_search_query([model], [category_terms]),
        compose_search_query([brand], [item_title, "replacement"]),
    ]
    return unique_keywords(candidates)[:max(1, MAX_WIDE_KEYWORDS)]


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

    if category == "nuevo" and model:
        return f"Ver {model} nuevo"
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
      2) overrides en ofertas.json / legado ofertas.yaml (offers_obj)
      3) defaults por categorÃ­a (solo para must_not)
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
    vertical: str,
    part_terms: List[str],
    must_not_include: List[str],
    anchor_terms: List[str],
    specific_item_terms: List[str],
    rejected_fingerprints: set[str],
    use_cache: bool,
    keywords: Optional[List[str]] = None,
) -> Optional[Dict[str, str]]:
    """
    BÃºsqueda relajada sin filtro de modelo: solo marca + categorÃ­a.
    Aplica must_not_include para evitar contaminar con categorÃ­as cruzadas.
    """
    relaxed_terms = " ".join(cat_query_terms(category)[:2])
    category_key = nrm(category)
    require_anchor = category_key in STRICT_RELAXED_CATEGORIES
    min_anchor_hits = 2 if require_anchor else 1
    if require_anchor and anchor_terms:
        min_anchor_hits = min(min_anchor_hits, len(anchor_terms))
    min_specific_hits = min_specific_item_hits(category, specific_item_terms)
    if require_anchor and len(anchor_terms) < min_anchor_hits:
        return None

    anchor_text = " ".join(anchor_terms[:2])
    fallback_keyword = compact_spaces(f"{brand} {anchor_text} {relaxed_terms} replacement")[:120]
    query_list = unique_keywords([*(keywords or []), fallback_keyword])
    if not query_list:
        return None

    for keyword in query_list:
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
                    if product_fingerprint(p) in rejected_fingerprints:
                        continue
                    tt = nrm(title)
                    if looks_bad(tt):
                        continue
                    if is_deceptive_title(title, category):
                        continue
                    if vertical and not title_matches_vertical(title, vertical):
                        continue
                    if category != "nuevo" and vertical and looks_like_complete_product_for_category(title, vertical, category, part_terms):
                        continue
                    if brand and not title_has_required_brand(title, brand):
                        continue
                    has_part_term = any(pt in tt for pt in part_terms)
                    if not has_part_term:
                        if not (category == "accesorios" and specific_item_terms and count_anchor_hits(title, specific_item_terms) >= 1):
                            continue
                    active_must_not = effective_must_not_terms(title, category, [], must_not_include)
                    if active_must_not and contains_any(tt, active_must_not):
                        continue
                    if not title_matches_category_signals(title, category, specific_item_terms):
                        continue
                    if anchor_terms and count_anchor_hits(title, anchor_terms) < min_anchor_hits:
                        continue
                    if min_specific_hits and count_anchor_hits(title, specific_item_terms) < min_specific_hits:
                        continue
                    candidates.append(p)

                if not candidates:
                    continue

                candidates.sort(
                    key=lambda p: (
                        sum(1 for anchor in anchor_terms if anchor in nrm(str(p.get("product_title") or ""))) * 4.0
                        + get_orders(p) * 0.015
                        + get_commission_rate(p) * 0.4
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


def collect_relaxed_candidates(
    keywords: List[str],
    *,
    brand: str,
    category: str,
    vertical: str,
    part_terms: List[str],
    must_not_include: List[str],
    anchor_terms: List[str],
    specific_item_terms: List[str],
    rejected_fingerprints: set[str],
    use_cache: bool,
    limit: int = 5,
) -> List[Dict[str, str]]:
    category_key = nrm(category)
    require_anchor = category_key in STRICT_RELAXED_CATEGORIES
    min_anchor_hits = 2 if require_anchor else 1
    if require_anchor and anchor_terms:
        min_anchor_hits = min(min_anchor_hits, len(anchor_terms))
    min_specific_hits = min_specific_item_hits(category, specific_item_terms)
    if require_anchor and len(anchor_terms) < min_anchor_hits:
        return []

    ranked: List[Tuple[float, Dict[str, str]]] = []
    seen_urls: set[str] = set()
    page_numbers = candidate_pages_for_category(category)
    for keyword in unique_keywords(keywords)[:max(1, MAX_RESCUE_KEYWORDS)]:
        for lang in (RESCUE_LANGS or ("EN",)):
            for page_no in page_numbers:
                try:
                    resp = product_query(keyword, lang=lang, page_no=page_no, use_cache=use_cache)
                except Exception as exc:
                    print(f"  [relaxed-error] kw='{keyword}' lang={lang} page={page_no} - {exc}")
                    continue
                prods = extract_products(resp)
                if not prods:
                    continue

                for p in prods:
                    title = p.get("product_title") or ""
                    if not title:
                        continue
                    if product_fingerprint(p) in rejected_fingerprints:
                        continue
                    tt = nrm(title)
                    if looks_bad(tt):
                        continue
                    if is_deceptive_title(title, category):
                        continue
                    if vertical and not title_matches_vertical(title, vertical):
                        continue
                    if category != "nuevo" and vertical and looks_like_complete_product_for_category(title, vertical, category, part_terms):
                        continue
                    if brand and not title_has_required_brand(title, brand):
                        continue
                    has_part_term = any(pt in tt for pt in part_terms)
                    if not has_part_term:
                        if not (category == "accesorios" and specific_item_terms and count_anchor_hits(title, specific_item_terms) >= 1):
                            continue
                    active_must_not = effective_must_not_terms(title, category, [], must_not_include)
                    if active_must_not and contains_any(tt, active_must_not):
                        continue
                    if not title_matches_category_signals(title, category, specific_item_terms):
                        continue
                    if anchor_terms and count_anchor_hits(title, anchor_terms) < min_anchor_hits:
                        continue
                    if min_specific_hits and count_anchor_hits(title, specific_item_terms) < min_specific_hits:
                        continue

                    url = str(p.get("promotion_link") or p.get("product_detail_url") or "").strip()
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    score = (
                        sum(1 for anchor in anchor_terms if anchor in nrm(str(title))) * 4.0
                        + get_orders(p) * 0.015
                        + get_commission_rate(p) * 0.4
                    )
                    ranked.append((
                        score,
                        {
                            "url": url,
                            "image_url": str(p.get("product_main_image_url") or "").strip(),
                            "sale_price": str(p.get("sale_price") or "").strip(),
                            "sale_price_currency": str(p.get("sale_price_currency") or "").strip(),
                            "original_price": str(p.get("original_price") or "").strip(),
                            "discount": str(p.get("discount") or "").strip(),
                            "product_title": str(title).strip(),
                            "matched_query": keyword,
                            "candidate_tier": "rescue",
                        },
                    ))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return [cand for _, cand in ranked[:max(1, min(limit, MAX_SHORTLIST_CANDIDATES))]]


def collect_wide_ai_candidates(
    keywords: List[str],
    *,
    brand: str,
    model_hint: str,
    category: str,
    vertical: str,
    part_terms: List[str],
    must_not_include: List[str],
    specific_item_terms: List[str],
    rejected_fingerprints: set[str],
    use_cache: bool,
    limit: int = 8,
) -> List[Dict[str, str]]:
    req_models = model_tokens_from_ctx(model_hint)
    ranked: List[Tuple[float, Dict[str, str]]] = []
    seen_urls: set[str] = set()

    for keyword in unique_keywords(keywords)[:max(1, MAX_WIDE_KEYWORDS)]:
        for lang in (RESCUE_LANGS or ("EN", "ES")):
            for page_no in candidate_pages_for_category(category):
                try:
                    resp = product_query(keyword, lang=lang, page_no=page_no, use_cache=use_cache)
                except Exception as exc:
                    print(f"  [wide-error] kw='{keyword}' lang={lang} page={page_no} - {exc}")
                    continue
                prods = extract_products(resp)
                if not prods:
                    continue

                for p in prods:
                    title = p.get("product_title") or ""
                    if not title:
                        continue
                    if product_fingerprint(p) in rejected_fingerprints:
                        continue
                    tt = nrm(title)
                    if looks_bad(tt):
                        continue
                    if is_deceptive_title(title, category):
                        continue
                    if category != "nuevo" and vertical and looks_like_complete_product_for_category(title, vertical, category, part_terms):
                        continue
                    active_must_not = effective_must_not_terms(title, category, req_models, must_not_include)
                    if active_must_not and contains_any(tt, active_must_not):
                        continue

                    brand_hit = title_has_required_brand(title, brand) if brand else True
                    model_hit = title_has_required_model(tt, req_models) if req_models else False
                    part_hit = any(pt in tt for pt in part_terms) if part_terms else False
                    specific_hit = count_anchor_hits(title, specific_item_terms) if specific_item_terms else 0
                    vertical_hit = title_matches_vertical(title, vertical) if vertical else True
                    if not vertical_hit and not (brand_hit and model_hit):
                        continue
                    if not (model_hit or (brand_hit and (part_hit or specific_hit))):
                        continue

                    url = str(p.get("promotion_link") or p.get("product_detail_url") or "").strip()
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    score = (
                        (10.0 if model_hit else 0.0)
                        + (5.0 if brand_hit else 0.0)
                        + (3.0 if part_hit else 0.0)
                        + specific_hit * 2.5
                        + get_orders(p) * 0.01
                        + get_commission_rate(p) * 0.25
                    )
                    ranked.append((
                        score,
                        {
                            "url": url,
                            "image_url": str(p.get("product_main_image_url") or "").strip(),
                            "sale_price": str(p.get("sale_price") or "").strip(),
                            "sale_price_currency": str(p.get("sale_price_currency") or "").strip(),
                            "original_price": str(p.get("original_price") or "").strip(),
                            "discount": str(p.get("discount") or "").strip(),
                            "product_title": str(title).strip(),
                            "matched_query": keyword,
                            "candidate_tier": "wide",
                        },
                    ))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return [cand for _, cand in ranked[:max(1, min(limit, MAX_SHORTLIST_CANDIDATES))]]


def pick_best_promotion_link(
    keyword: str,
    must_brand: str,
    model_hint: str,
    part_terms: List[str],
    must_include: List[str],
    must_not_include: List[str],
    model_tokens_override: List[str],
    vertical: str,
    category: str,
    strict_anchor_terms: List[str],
    specific_item_terms: List[str],
    rejected_fingerprints: set[str],
    use_cache: bool,
) -> Optional[Dict[str, str]]:
    req_models = model_tokens_override[:] if model_tokens_override else model_tokens_from_ctx(model_hint)
    strict_category = nrm(category) in STRICT_EXACT_MATCH_CATEGORIES
    min_anchor_hits = 2 if strict_category else 1
    min_specific_hits = min_specific_item_hits(category, specific_item_terms)

    page_numbers = candidate_pages_for_category(category)
    for lang in ("EN", "ES"):
        for page_no in page_numbers:
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
                if product_fingerprint(p) in rejected_fingerprints:
                    continue

                tt = nrm(title)
                if looks_bad(tt):
                    continue
                if is_deceptive_title(title, category):
                    continue
                if vertical and not title_matches_vertical(title, vertical):
                    continue
                if category != "nuevo" and vertical and looks_like_complete_product_for_category(title, vertical, category, part_terms):
                    continue
                if category == "nuevo" and is_low_quality_new_title(title):
                    continue

                if category != "nuevo" and must_brand and not title_has_required_brand(title, must_brand):
                    continue

                if req_models and not title_has_required_model(tt, req_models):
                    continue

                if part_terms:
                    has_part_term = any(pt in tt for pt in part_terms)
                    if not has_part_term:
                        if not (category == "accesorios" and specific_item_terms and count_anchor_hits(title, specific_item_terms) >= 1):
                            continue

                if must_include and not must_include_satisfied(title, must_include, category):
                    continue

                if must_not_include and contains_any(tt, must_not_include):
                    continue
                if not title_matches_category_signals(title, category, specific_item_terms):
                    continue

                if strict_anchor_terms and count_anchor_hits(title, strict_anchor_terms) < min_anchor_hits:
                    continue
                if min_specific_hits and count_anchor_hits(title, specific_item_terms) < min_specific_hits:
                    continue

                candidates.append(p)

            if not candidates:
                continue

            if category == "nuevo":
                candidates.sort(
                    key=lambda p: score_new_product(
                        p.get("product_title") or "",
                        p,
                        must_brand=nrm(must_brand),
                        req_models=req_models,
                        vertical=vertical,
                    ),
                    reverse=True,
                )
            else:
                candidates.sort(
                    key=lambda p: score_product(
                        p.get("product_title") or "",
                        p,
                        must_brand=nrm(must_brand),
                        part_terms=part_terms,
                        req_models=req_models,
                        category=category,
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


def collect_exact_candidates(
    keywords: List[str],
    *,
    must_brand: str,
    model_hint: str,
    part_terms: List[str],
    must_include: List[str],
    must_not_include: List[str],
    model_tokens_override: List[str],
    vertical: str,
    category: str,
    strict_anchor_terms: List[str],
    specific_item_terms: List[str],
    rejected_fingerprints: set[str],
    use_cache: bool,
    limit: int = 5,
) -> List[Dict[str, str]]:
    req_models = model_tokens_override[:] if model_tokens_override else model_tokens_from_ctx(model_hint)
    strict_category = nrm(category) in STRICT_EXACT_MATCH_CATEGORIES
    min_anchor_hits = 2 if strict_category else 1
    if nrm(category) in EXACT_SHARED_COMPAT_CATEGORIES:
        min_anchor_hits = 1
    min_specific_hits = min_specific_item_hits(category, specific_item_terms)
    ranked: List[Tuple[float, Dict[str, str]]] = []
    seen_urls: set[str] = set()

    page_numbers = candidate_pages_for_category(category)
    for keyword in keywords[:max(1, MAX_EXACT_KEYWORDS)]:
        for lang in (EXACT_LANGS or ("EN", "ES")):
            for page_no in page_numbers:
                try:
                    resp = product_query(keyword, lang=lang, page_no=page_no, use_cache=use_cache)
                except Exception as exc:
                    print(f"  [query-error] kw='{keyword}' lang={lang} page={page_no} - {exc}")
                    continue
                prods = extract_products(resp)
                if not prods:
                    continue

                for p in prods:
                    title = p.get("product_title") or ""
                    if not title:
                        continue
                    if product_fingerprint(p) in rejected_fingerprints:
                        continue
                    tt = nrm(title)
                    if looks_bad(tt):
                        continue
                    if is_deceptive_title(title, category):
                        continue
                    if vertical and not title_matches_vertical(title, vertical):
                        continue
                    if category != "nuevo" and vertical and looks_like_complete_product_for_category(title, vertical, category, part_terms):
                        continue
                    if category == "nuevo" and is_low_quality_new_title(title):
                        continue
                    if category != "nuevo" and must_brand and not title_has_required_brand(title, must_brand):
                        continue
                    if req_models and not title_has_required_model(tt, req_models):
                        continue
                    if part_terms:
                        has_part_term = any(pt in tt for pt in part_terms)
                        if not has_part_term:
                            if not (category == "accesorios" and specific_item_terms and count_anchor_hits(title, specific_item_terms) >= 1):
                                continue
                    if must_include and not must_include_satisfied(title, must_include, category):
                        continue
                    active_must_not = effective_must_not_terms(title, category, req_models, must_not_include)
                    if active_must_not and contains_any(tt, active_must_not):
                        continue
                    if not title_matches_category_signals(title, category, specific_item_terms):
                        continue
                    if strict_anchor_terms and count_anchor_hits(title, strict_anchor_terms) < min_anchor_hits:
                        continue
                    if min_specific_hits and count_anchor_hits(title, specific_item_terms) < min_specific_hits:
                        continue

                    url = str(p.get("promotion_link") or p.get("product_detail_url") or "").strip()
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    score = (
                        score_new_product(title, p, must_brand=nrm(must_brand), req_models=req_models, vertical=vertical)
                        if category == "nuevo"
                        else score_product(title, p, must_brand=nrm(must_brand), part_terms=part_terms, req_models=req_models, category=category)
                    )
                    if score <= -1e8:
                        continue
                    ranked.append((
                        score,
                        {
                            "url": url,
                            "image_url": str(p.get("product_main_image_url") or "").strip(),
                            "sale_price": str(p.get("sale_price") or "").strip(),
                            "sale_price_currency": str(p.get("sale_price_currency") or "").strip(),
                            "original_price": str(p.get("original_price") or "").strip(),
                            "discount": str(p.get("discount") or "").strip(),
                            "product_title": str(title).strip(),
                            "matched_query": keyword,
                            "candidate_tier": "exact",
                        },
                    ))

    ranked.sort(key=lambda item: item[0], reverse=True)
    return [cand for _, cand in ranked[:max(1, min(limit, MAX_SHORTLIST_CANDIDATES))]]


# =========================
# Vertical fallback (buy-new genÃ©rico por vertical)
# =========================
VERTICAL_FALLBACK_QUERIES = {
    "aspiradores":          "cordless stick vacuum cleaner household",
    "afeitadoras":          "electric shaver men foil rotary",
    "aspiradoras-normales": "upright canister vacuum cleaner",
    "auriculares":          "wireless earbuds bluetooth noise cancelling",
    "cafeteras":            "automatic espresso coffee machine household",
    "cepillos":             "electric toothbrush sonic",
    "secadores-pelo":       "professional hair dryer ionic household",
    "planchas-pelo":        "hair straightener ceramic flat iron",
    "aspiradores-mano":     "handheld vacuum cleaner portable cordless",
    "vaporetas":            "steam cleaner steam mop household",
    "centros-planchado":    "steam generator iron household",
    "freidoras":            "air fryer household digital 6l 8l",
    "herramientas":         "cordless drill brushless household tool set",
    "lavadoras":            "washing machine front load fully automatic",
    "mascotas":             "pet groomer dog clipper vacuum",
    "osmosis":              "reverse osmosis water purifier system household",
    "patinetes-electricos": "electric scooter adult 350w 500w",
    "robots-cristales":     "window cleaning robot automatic",
    "robots-fregar":        "floor washing robot mop self cleaning",
    "robots-piscina":       "robotic pool cleaner automatic",
}

VERTICAL_FALLBACK_LABELS = {
    "aspiradores":          "Ver aspiradoras en AliExpress",
    "afeitadoras":          "Ver afeitadoras en AliExpress",
    "aspiradoras-normales": "Ver aspiradoras en AliExpress",
    "auriculares":          "Ver auriculares en AliExpress",
    "cafeteras":            "Ver cafeteras en AliExpress",
    "cepillos":             "Ver cepillos elÃ©ctricos en AliExpress",
    "secadores-pelo":       "Ver secadores en AliExpress",
    "planchas-pelo":        "Ver planchas de pelo en AliExpress",
    "aspiradores-mano":     "Ver aspiradores de mano en AliExpress",
    "vaporetas":            "Ver vaporetas en AliExpress",
    "centros-planchado":    "Ver centros de planchado en AliExpress",
    "freidoras":            "Ver freidoras de aire en AliExpress",
    "herramientas":         "Ver herramientas en AliExpress",
    "lavadoras":            "Ver lavadoras en AliExpress",
    "mascotas":             "Ver productos mascotas en AliExpress",
    "osmosis":              "Ver sistemas osmosis en AliExpress",
    "patinetes-electricos": "Ver patinetes elÃ©ctricos en AliExpress",
    "robots-cristales":     "Ver robots limpiacristales en AliExpress",
    "robots-fregar":        "Ver robots friegasuelos en AliExpress",
    "robots-piscina":       "Ver robots limpiapiscinas en AliExpress",
}


def pick_vertical_best(keyword: str, vertical: str, use_cache: bool) -> Optional[Dict[str, str]]:
    """Busca el producto con mejor comisiÃ³n para un keyword genÃ©rico de vertical.
    Exige que el tÃ­tulo contenga al menos un tÃ©rmino del vertical para evitar
    productos de automociÃ³n u otras categorÃ­as que se cuelan por comisiÃ³n alta.
    Devuelve dict con url y product_title, o None si no encuentra nada."""
    required_terms = VERTICAL_REQUIRED_TERMS.get(vertical, [])

    for lang in ("EN", "ES"):
        try:
            resp = product_query(keyword, lang=lang, page_no=1, use_cache=use_cache)
        except Exception as exc:
            print(f"  [vertical-error] kw='{keyword}' lang={lang} - {exc}")
            continue
        prods = extract_products(resp)
        if not prods:
            continue
        candidates = []
        for p in prods:
            title = p.get("product_title") or ""
            tt = nrm(title)
            if looks_bad(tt):
                continue
            if is_low_quality_new_title(title):
                continue
            # Exigir al menos un tÃ©rmino del vertical en el tÃ­tulo
            if required_terms and not any(nrm(t) in tt for t in required_terms):
                continue
            if not is_complete_new_product(tt, vertical):
                continue
            candidates.append(p)
        if not candidates:
            continue
        # Prioridad: comisiÃ³n absoluta estimada (precio * %comisiÃ³n), ventas como desempate
        candidates.sort(
            key=lambda p: score_new_product(
                p.get("product_title") or "",
                p,
                must_brand="",
                req_models=[],
                vertical=vertical,
            ),
            reverse=True,
        )
        best = candidates[0]
        url = best.get("promotion_link") or best.get("product_detail_url")
        if url:
            return {
                "url": str(url).strip(),
                "product_title": str(best.get("product_title") or "").strip(),
                "image_url": str(best.get("product_main_image_url") or "").strip(),
                "sale_price": str(best.get("sale_price") or "").strip(),
                "sale_price_currency": str(best.get("sale_price_currency") or "").strip(),
                "original_price": str(best.get("original_price") or "").strip(),
                "discount": str(best.get("discount") or "").strip(),
            }
    return None


def sync_vertical_defaults(verticals: List[str], force: bool, use_cache: bool) -> None:
    """
    Para cada vertical, si buy_new_url estÃ¡ vacÃ­o (o --force), busca en AliExpress
    el producto con mejor comisiÃ³n y actualiza data/vertical_defaults.yaml.
    """
    vd = load_yaml(VERTICAL_DEFAULTS_YAML)
    changed = False

    for vertical in verticals:
        entry = vd.get(vertical)
        if not isinstance(entry, dict):
            entry = {}
            vd[vertical] = entry

        current_url = str(entry.get("buy_new_url") or "").strip()
        if current_url and not force:
            print(f"  [vertical-default] {vertical}: ya tiene URL, saltando (usa --force para refrescar)")
            continue

        query = VERTICAL_FALLBACK_QUERIES.get(vertical)
        if not query:
            print(f"  [vertical-default] {vertical}: sin query definida, saltando")
            continue

        print(f"  [vertical-default] {vertical}: buscando â†’ '{query}'")
        result = pick_vertical_best(query, vertical=vertical, use_cache=use_cache)
        if result:
            entry["buy_new_url"] = result["url"]
            entry["buy_new_label"] = VERTICAL_FALLBACK_LABELS.get(vertical, "Ver productos en AliExpress")
            entry["buy_new_product_title"] = result["product_title"]
            if result.get("image_url"):
                entry["buy_new_image_url"] = result["image_url"]
            if result.get("sale_price"):
                entry["buy_new_sale_price"] = result["sale_price"]
                entry["buy_new_sale_price_currency"] = result.get("sale_price_currency", "EUR")
            if result.get("original_price"):
                entry["buy_new_original_price"] = result["original_price"]
            if result.get("discount"):
                entry["buy_new_discount"] = result["discount"]
            entry["updated_at"] = datetime.now().date().isoformat()
            vd[vertical] = entry
            changed = True
            print(f"  [vertical-default] {vertical}: OK â†’ {result['url'][:80]}")
            if result["product_title"]:
                print(f"  [vertical-default] {vertical}: tÃ­tulo â†’ {result['product_title'][:80]}")
        else:
            print(f"  [vertical-default] {vertical}: sin resultado en AliExpress")

    if changed:
        dump_yaml(VERTICAL_DEFAULTS_YAML, vd)
        print(f"  vertical_defaults.yaml actualizado")


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Sincroniza data/ofertas.json desde data/<vertical>.yaml + AliExpress API")
    ap.add_argument("--clear-cache", action="store_true", help="Borra el cache y termina")
    ap.add_argument("--no-cache", action="store_true", help="Ignora cache (hace llamadas frescas)")
    ap.add_argument("--only-sku", action="append", default=[], help="Solo procesa este SKU (puedes repetir)")
    ap.add_argument("--force", action="store_true", help="Fuerza lookup incluso si ya hay URL no-default")
    ap.add_argument("--vertical", default="all", help="Vertical a sincronizar: una, varias separadas por comas, o 'all' (default)")
    ap.add_argument("--only-stale", type=int, default=0, metavar="DAYS",
                    help="Solo procesa SKUs cuyo updated_at tiene mÃ¡s de DAYS dÃ­as (0 = ignorar). Ãštil para refrescar enlaces sin relanzar todo.")
    ap.add_argument("--batch-size", type=int, default=0, metavar="N",
                    help="Procesa como mÃ¡ximo N SKUs por ejecuciÃ³n, los mÃ¡s antiguos primero (0 = todos). Combina con --only-stale para repartir en dÃ­as.")
    ap.add_argument("--max-minutes", type=float, default=0, metavar="MIN",
                    help="Detiene el procesado cuando se acerca a MIN minutos de ejecuciÃ³n, guarda y sale limpiamente (0 = sin lÃ­mite).")
    ap.add_argument("--skip-vertical-defaults", action="store_true",
                    help="No actualiza vertical_defaults.yaml al final. Ãštil para la Action horaria o pruebas puntuales.")
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

    if github_pause_enabled():
        try:
            github_pause_previous = github_pause_sync_workflow()

            def _restore_github_actions_pause() -> None:
                try:
                    github_restore_sync_workflow(github_pause_previous)
                except Exception as exc:
                    print(f"  WARN: no se pudo restaurar la GitHub Action: {exc}")

            atexit.register(_restore_github_actions_pause)
        except Exception as exc:
            print(f"  WARN: no se pudo pausar la GitHub Action: {exc}")

    use_cache = not args.no_cache

    selected_verticals = resolve_verticals(args.vertical)
    sku_ctx = sku_records_from_verticals(selected_verticals)
    want = set(sku_ctx.keys())

    offers_doc = load_offers_doc()
    offers = offers_doc.get("offers")
    if not isinstance(offers, dict):
        offers = {}
    offers_doc["offers"] = offers

    only = [str(x).strip() for x in args.only_sku if str(x).strip()]
    if only:
        want = set([s for s in only if s in sku_ctx])

    def sku_updated_at(sku: str) -> str:
        return str(ensure_offer_obj(offers.get(sku)).get("updated_at") or "0000-00-00")

    def sku_last_attempted_at(sku: str) -> str:
        return str(ensure_offer_obj(offers.get(sku)).get("last_attempted_at") or "0000-00-00")

    def sku_refresh_priority(sku: str) -> Tuple[int, str]:
        offer_obj = ensure_offer_obj(offers.get(sku))
        status = str(offer_obj.get("compatibility_status") or derive_compatibility_status(offer_obj)).strip()
        if status in {"fallback_buy_new", "pending_ai_validation"} or offer_obj.get("needs_url") is True:
            return (compatibility_priority(status), sku_last_attempted_at(sku))
        return (compatibility_priority(status), sku_updated_at(sku))

    # --only-stale: filtra SKUs cuyo updated_at supera N dÃ­as
    if not only and args.only_stale > 0:
        cutoff = (datetime.now().date() - timedelta(days=args.only_stale)).isoformat()
        stale = {
            sku for sku in want
            if (
                str(ensure_offer_obj(offers.get(sku)).get("ai_validation_status") or "").strip() == "pending"
                or ensure_offer_obj(offers.get(sku)).get("needs_url") is True
                or str(ensure_offer_obj(offers.get(sku)).get("compatibility_status") or "").strip() == "fallback_buy_new"
                or str(ensure_offer_obj(offers.get(sku)).get("updated_at") or "").strip() < cutoff
            )
        }
        print(f"  --only-stale {args.only_stale}d: {len(stale)}/{len(want)} SKUs sin actualizar desde {cutoff}")
        if stale:
            want = stale
        else:
            print("  --only-stale: no hay SKUs vencidos; se renuevan enlaces existentes por antigÃ¼edad")

    # --batch-size: toma los N mÃ¡s antiguos para no saturar la API en una sola pasada
    if not only and args.batch_size > 0 and len(want) > args.batch_size:
        sorted_want = sorted(want, key=sku_refresh_priority)
        want = set(sorted_want[:args.batch_size])
        print(f"  --batch-size {args.batch_size}: procesando {args.batch_size} SKUs con peor cobertura y mayor antigÃ¼edad")

    added = 0
    updated = 0
    orphaned = 0
    un_orphaned = 0
    changed_urls_to_default = 0
    filled_from_aliexpress = 0
    failed_skus = 0
    ai_exact_candidates = 0
    ai_relaxed_candidates = 0
    ai_wide_candidates = 0
    ai_buy_new_direct = 0
    ai_validated_run = 0
    ai_rejected_run = 0
    ai_doubtful_run = 0
    ai_pending_run = 0
    ai_pending_retries_exhausted = 0
    ai_pending_reasons_run: Dict[str, int] = {}
    _processed = 0
    SAVE_EVERY = 25  # guarda progreso cada N SKUs

    today = datetime.now().date().isoformat()
    init_ai_budget_state(offers_doc, today)
    _t0 = time.time()
    _total_want = len(want)
    _time_budget_s = args.max_minutes * 60 if args.max_minutes > 0 else 0
    ordered_want = sorted(want, key=sku_refresh_priority)

    print(f"  Iniciando procesado de {_total_want} SKUs...")

    def note_ai_pending_reason(reason: str) -> None:
        key = reason_key(reason or "ai_pending_unknown")
        ai_pending_reasons_run[key] = ai_pending_reasons_run.get(key, 0) + 1

    for sku in ordered_want:
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

            # Para SKUs -nuevo: usar required_terms del vertical como filtro de tipo de producto.
            # AsÃ­ "Philips EP3221" busca cafeteras, no vÃ¡lvulas de Bosch.
            if category == "nuevo":
                vertical = ctx.get("vertical") or ""
                vert_terms = VERTICAL_REQUIRED_TERMS.get(vertical, [])
                if vert_terms:
                    part_terms = vert_terms

            must_not_default = cat_negative_terms(category)

            query, must_include, must_not_override, model_tokens_override = merge_overrides(
                sku=sku,
                ctx=ctx,
                offers_obj=obj,
            )
            must_not_combined = [*must_not_default, *must_not_override]
            effective_model_tokens = model_tokens_override[:] if model_tokens_override else model_tokens_from_ctx(model)
            strict_anchor_terms = extract_strict_anchor_terms(ctx, must_include, effective_model_tokens)
            specific_item_terms = expand_specific_item_terms(
                extract_specific_item_terms(ctx, must_include, effective_model_tokens)
            )
            ai_ctx = dict(ctx)
            ai_ctx["must_include"] = must_include
            ai_ctx["must_not_include"] = must_not_combined
            ai_ctx["model_tokens"] = effective_model_tokens

            kws = build_search_keywords(ctx, query, must_include)
            rescue_kws = build_ai_rescue_keywords(ctx, query, must_include)
            wide_kws = build_ai_wide_keywords(ctx, query, must_include)
            fallback_search_query = kws[0] if kws else choose_fallback_search_query(ctx, query)
            fallback_search_label = choose_fallback_search_label(ctx, fallback_search_query)
            require_ai_validation = ai_validation_enabled()
            # Politica de seguridad:
            # - sin IA: solo matching duro, nada de tiers relajados
            # - con IA: se puede relajar, pero cualquier candidato seleccionado
            #   debe pasar validacion final antes de consolidarse
            relaxed_allowed = (
                require_ai_validation
                and nrm(category) != "nuevo"
                and rescue_category_enabled(category)
            )
            rejected_fps = rejected_candidate_fingerprints(obj)
            found = None
            matched_kw = ""
            ai_pending = False
            ai_rejected = False

            pending_candidate = obj.get("ai_pending_candidate")
            pending_status = str(obj.get("ai_validation_status") or "").strip()
            if require_ai_validation and pending_status == "pending" and isinstance(pending_candidate, dict) and pending_candidate:
                pending_result = validate_candidate_with_ai(ai_ctx, pending_candidate)
                pending_reason = pending_result.get("reason") or ""
                pending_attempts = int(obj.get("ai_pending_attempts") or 0)
                if pending_result.get("status") == "validated":
                    found = dict(pending_candidate)
                    matched_kw = str(found.get("matched_query") or "")
                    ai_validated_run += 1
                    obj["ai_validation_status"] = "validated"
                    obj["ai_validation_reason"] = pending_reason
                    obj["ai_validation_model"] = effective_ai_validation_model()
                    obj["ai_validation_at"] = today
                    obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(found)
                    obj.pop("ai_pending_candidate", None)
                elif pending_result.get("status") == "doubtful":
                    found = dict(pending_candidate)
                    matched_kw = str(found.get("matched_query") or "")
                    ai_doubtful_run += 1
                    apply_doubtful_candidate(
                        obj,
                        found,
                        matched_query=matched_kw or fallback_search_query,
                        fallback_search_query=fallback_search_query,
                        fallback_search_label=fallback_search_label,
                        reason=pending_reason,
                        today=today,
                    )
                    obj.pop("ai_pending_candidate", None)
                elif pending_result.get("status") == "rejected":
                    ai_rejected_run += 1
                    append_rejected_candidate_fingerprint(obj, candidate_fingerprint(pending_candidate))
                    obj["ai_validation_status"] = "rejected"
                    obj["ai_validation_reason"] = pending_reason
                    obj["ai_validation_model"] = effective_ai_validation_model()
                    obj["ai_validation_at"] = today
                    obj["ai_validation_candidate_fingerprint"] = ""
                    obj.pop("ai_pending_candidate", None)
                    ai_rejected = True
                else:
                    note_ai_pending_reason(pending_reason)
                    if pending_attempts >= 3:
                        ai_rejected_run += 1
                        ai_pending_retries_exhausted += 1
                        append_rejected_candidate_fingerprint(obj, candidate_fingerprint(pending_candidate))
                        obj["ai_validation_status"] = "rejected"
                        obj["ai_validation_reason"] = pending_reason or "ai_pending_retries_exhausted"
                        obj["ai_validation_model"] = effective_ai_validation_model()
                        obj["ai_validation_at"] = today
                        obj["ai_validation_candidate_fingerprint"] = ""
                        clear_ai_pending_candidate(obj)
                        ai_rejected = True
                    else:
                        ai_pending_run += 1
                        obj["ai_validation_status"] = "pending"
                        obj["ai_validation_reason"] = pending_reason or str(obj.get("ai_validation_reason") or "ai_pending_retry")
                        obj["ai_pending_reason"] = pending_reason or "ai_pending_retry"
                        obj["ai_pending_attempts"] = pending_attempts + 1
                        obj["ai_validation_model"] = effective_ai_validation_model()
                        obj["ai_validation_at"] = today
                        obj["updated_at"] = UNRESOLVED_UPDATED_AT
                        obj["last_attempted_at"] = today
                        ai_pending = True

            if require_ai_validation and not found and not ai_pending:
                exact_candidates = collect_exact_candidates(
                    kws,
                    must_brand=brand,
                    model_hint=model,
                    part_terms=part_terms,
                    must_include=must_include,
                    must_not_include=must_not_combined,
                    model_tokens_override=model_tokens_override,
                    vertical=str(ctx.get("vertical") or ""),
                    category=category,
                    strict_anchor_terms=strict_anchor_terms,
                    specific_item_terms=specific_item_terms,
                    rejected_fingerprints=rejected_fps,
                    use_cache=use_cache,
                    limit=MAX_SHORTLIST_CANDIDATES,
                )
                ai_exact_candidates += len(exact_candidates)
                if exact_candidates:
                    ai_result = choose_best_candidate_with_ai(ai_ctx, exact_candidates)
                    ai_status = ai_result.get("status") or ""
                    ai_reason = ai_result.get("reason") or ""
                    if ai_status == "validated":
                        found = dict(ai_result.get("candidate") or {})
                        matched_kw = str(found.get("matched_query") or "")
                        ai_validated_run += 1
                        obj["ai_validation_status"] = "validated"
                        obj["ai_validation_reason"] = ai_reason
                        obj["ai_validation_model"] = effective_ai_validation_model()
                        obj["ai_validation_at"] = today
                        obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(found)
                        obj.pop("ai_pending_candidate", None)
                    elif ai_status == "doubtful":
                        found = dict(ai_result.get("candidate") or {})
                        matched_kw = str(found.get("matched_query") or "")
                        ai_doubtful_run += 1
                        obj["ai_validation_status"] = "doubtful"
                        obj["ai_validation_reason"] = ai_reason
                        obj["ai_validation_model"] = effective_ai_validation_model()
                        obj["ai_validation_at"] = today
                        obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(found)
                        obj.pop("ai_pending_candidate", None)
                    elif ai_status == "rejected":
                        ai_rejected_run += 1
                        for candidate in exact_candidates:
                            fp = candidate_fingerprint(candidate)
                            append_rejected_candidate_fingerprint(obj, fp)
                            rejected_fps.add(fp)
                        obj["ai_validation_status"] = "rejected"
                        obj["ai_validation_reason"] = ai_reason
                        obj["ai_validation_model"] = effective_ai_validation_model()
                        obj["ai_validation_at"] = today
                        obj["ai_validation_candidate_fingerprint"] = ""
                        obj.pop("ai_pending_candidate", None)
                        ai_rejected = True
                    else:
                        ai_pending_run += 1
                        note_ai_pending_reason(ai_reason)
                        stage_candidate_for_ai(obj, exact_candidates[0], reason=ai_reason, today=today)
                        ai_pending = True
                else:
                    found = None
            else:
                for kw in kws:
                    found = pick_best_promotion_link(
                        keyword=kw,
                        must_brand=brand,
                        model_hint=model,
                        part_terms=part_terms,
                        must_include=must_include,
                        must_not_include=must_not_combined,
                        model_tokens_override=model_tokens_override,
                        vertical=str(ctx.get("vertical") or ""),
                        category=category,
                        strict_anchor_terms=strict_anchor_terms,
                        specific_item_terms=specific_item_terms,
                        rejected_fingerprints=rejected_fps,
                        use_cache=use_cache,
                    )
                    if found:
                        matched_kw = kw
                        break

            if found:
                if str(obj.get("ai_validation_status") or "").strip() == "doubtful":
                    apply_doubtful_candidate(
                        obj,
                        found,
                        matched_query=matched_kw or fallback_search_query,
                        fallback_search_query=fallback_search_query,
                        fallback_search_label=fallback_search_label,
                        reason=str(obj.get("ai_validation_reason") or "ai_doubtful_match"),
                        today=today,
                    )
                else:
                    apply_offer_candidate(
                        obj,
                        found,
                        match_type="exact_or_best_match",
                        matched_query=matched_kw or fallback_search_query,
                        fallback_search_query=fallback_search_query,
                        fallback_search_label=fallback_search_label,
                        today=today,
                    )
                filled_from_aliexpress += 1
            elif not ai_pending:
                anchor_terms = extract_relaxed_anchor_terms(ctx, must_include)
                relaxed = None
                if relaxed_allowed:
                    rescue_candidates = collect_relaxed_candidates(
                        rescue_kws,
                        brand=brand,
                        category=category,
                        vertical=str(ctx.get("vertical") or ""),
                        part_terms=part_terms,
                        must_not_include=must_not_combined,
                        anchor_terms=anchor_terms,
                        specific_item_terms=specific_item_terms,
                        rejected_fingerprints=rejected_fps,
                        use_cache=use_cache,
                        limit=MAX_SHORTLIST_CANDIDATES,
                    )
                    ai_relaxed_candidates += len(rescue_candidates)
                    if rescue_candidates:
                        ai_result = choose_best_candidate_with_ai(ai_ctx, rescue_candidates)
                        ai_status = ai_result.get("status") or ""
                        ai_reason = ai_result.get("reason") or ""
                        if ai_status == "validated":
                            relaxed = dict(ai_result.get("candidate") or {})
                            ai_validated_run += 1
                            obj["ai_validation_status"] = "validated"
                            obj["ai_validation_reason"] = ai_reason
                            obj["ai_validation_model"] = effective_ai_validation_model()
                            obj["ai_validation_at"] = today
                            obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(relaxed)
                            obj.pop("ai_pending_candidate", None)
                        elif ai_status == "doubtful":
                            relaxed = dict(ai_result.get("candidate") or {})
                            ai_doubtful_run += 1
                            obj["ai_validation_status"] = "doubtful"
                            obj["ai_validation_reason"] = ai_reason
                            obj["ai_validation_model"] = effective_ai_validation_model()
                            obj["ai_validation_at"] = today
                            obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(relaxed)
                            obj.pop("ai_pending_candidate", None)
                        elif ai_status == "rejected":
                            ai_rejected_run += 1
                            for candidate in rescue_candidates:
                                fp = candidate_fingerprint(candidate)
                                append_rejected_candidate_fingerprint(obj, fp)
                                rejected_fps.add(fp)
                            obj["ai_validation_status"] = "rejected"
                            obj["ai_validation_reason"] = ai_reason
                            obj["ai_validation_model"] = effective_ai_validation_model()
                            obj["ai_validation_at"] = today
                            obj["ai_validation_candidate_fingerprint"] = ""
                            obj.pop("ai_pending_candidate", None)
                            ai_rejected = True
                        else:
                            ai_pending_run += 1
                            note_ai_pending_reason(ai_reason)
                            stage_candidate_for_ai(obj, rescue_candidates[0], reason=ai_reason, today=today)
                            ai_pending = True
                    if not relaxed and not ai_pending and not ai_rejected:
                        wide_candidates = collect_wide_ai_candidates(
                            wide_kws,
                            brand=brand,
                            model_hint=model,
                            category=category,
                            vertical=str(ctx.get("vertical") or ""),
                            part_terms=part_terms,
                            must_not_include=must_not_combined,
                            specific_item_terms=specific_item_terms,
                            rejected_fingerprints=rejected_fps,
                            use_cache=use_cache,
                            limit=MAX_SHORTLIST_CANDIDATES,
                        )
                        ai_wide_candidates += len(wide_candidates)
                        if wide_candidates:
                            ai_result = choose_best_candidate_with_ai(ai_ctx, wide_candidates)
                            ai_status = ai_result.get("status") or ""
                            ai_reason = ai_result.get("reason") or ""
                            if ai_status == "validated":
                                relaxed = dict(ai_result.get("candidate") or {})
                                ai_validated_run += 1
                                obj["ai_validation_status"] = "validated"
                                obj["ai_validation_reason"] = ai_reason
                                obj["ai_validation_model"] = effective_ai_validation_model()
                                obj["ai_validation_at"] = today
                                obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(relaxed)
                                obj.pop("ai_pending_candidate", None)
                            elif ai_status == "doubtful":
                                relaxed = dict(ai_result.get("candidate") or {})
                                ai_doubtful_run += 1
                                obj["ai_validation_status"] = "doubtful"
                                obj["ai_validation_reason"] = ai_reason
                                obj["ai_validation_model"] = effective_ai_validation_model()
                                obj["ai_validation_at"] = today
                                obj["ai_validation_candidate_fingerprint"] = candidate_fingerprint(relaxed)
                                obj.pop("ai_pending_candidate", None)
                            elif ai_status == "rejected":
                                ai_rejected_run += 1
                                for candidate in wide_candidates:
                                    fp = candidate_fingerprint(candidate)
                                    append_rejected_candidate_fingerprint(obj, fp)
                                    rejected_fps.add(fp)
                                obj["ai_validation_status"] = "rejected"
                                obj["ai_validation_reason"] = ai_reason
                                obj["ai_validation_model"] = effective_ai_validation_model()
                                obj["ai_validation_at"] = today
                                obj["ai_validation_candidate_fingerprint"] = ""
                                obj.pop("ai_pending_candidate", None)
                                ai_rejected = True
                            else:
                                ai_pending_run += 1
                                note_ai_pending_reason(ai_reason)
                                stage_candidate_for_ai(obj, wide_candidates[0], reason=ai_reason, today=today)
                                ai_pending = True
                if relaxed:
                    if str(obj.get("ai_validation_status") or "").strip() == "doubtful":
                        apply_doubtful_candidate(
                            obj,
                            relaxed,
                            matched_query=str(relaxed.get("matched_query") or ""),
                            fallback_search_query=fallback_search_query,
                            fallback_search_label=fallback_search_label,
                            reason=str(obj.get("ai_validation_reason") or "ai_doubtful_match"),
                            today=today,
                        )
                    else:
                        apply_offer_candidate(
                            obj,
                            relaxed,
                            match_type="relaxed_fallback",
                            matched_query="",
                            fallback_search_query=fallback_search_query,
                            fallback_search_label=fallback_search_label,
                            today=today,
                        )
                    filled_from_aliexpress += 1
                elif not ai_pending:
                    ai_buy_new_direct += 1
                    if str(obj.get("url") or "").strip():
                        changed_urls_to_default += 1
                    obj["url"] = ""
                    for stale_key in (
                        "image_url",
                        "sale_price",
                        "sale_price_currency",
                        "original_price",
                        "discount",
                        "product_title",
                        "matched_query",
                    ):
                        obj.pop(stale_key, None)

                    obj.pop("ai_pending_candidate", None)
                    obj["needs_url"] = True
                    obj["match_type"] = "fallback_buy_new"
                    obj["fallback_search_query"] = fallback_search_query
                    obj["fallback_search_label"] = fallback_search_label
                    obj["compatibility_status"] = derive_compatibility_status(obj)
                    obj["compatibility_note"] = derive_compatibility_note(obj["compatibility_status"])
                    obj["updated_at"] = UNRESOLVED_UPDATED_AT
                    obj["last_attempted_at"] = today
                    obj["debug_last_query"] = fallback_search_query
                    obj["debug_model_tokens"] = effective_model_tokens
                    obj["debug_must_include"] = must_include
                    obj["debug_must_not_include"] = must_not_combined
                    obj["debug_specific_item_terms"] = specific_item_terms
                    obj["debug_relaxed_allowed"] = relaxed_allowed
                    obj["debug_failure_stage"] = "ai_rejected_all_candidates" if ai_rejected else ("relaxed_disabled" if not relaxed_allowed else "no_candidate_after_exact_and_relaxed")

        else:
            obj.pop("needs_url", None)
            if "match_type" not in obj:
                obj["match_type"] = "manual_or_existing"
            obj["compatibility_status"] = derive_compatibility_status(obj)
            obj["compatibility_note"] = derive_compatibility_note(obj["compatibility_status"])

        if sku not in offers:
            offers[sku] = obj
            added += 1
        else:
            offers[sku] = obj
            if before != obj:
                updated += 1

        if needs_lookup and obj.get("updated_at") != today:
            obj["last_attempted_at"] = today

        _processed += 1
        _elapsed = time.time() - _t0
        _rate = _processed / _elapsed if _elapsed > 0 else 0
        _remaining = _total_want - _processed
        _eta_s = int(_remaining / _rate) if _rate > 0 else 0
        _eta_str = f"{_eta_s//3600:02d}h{(_eta_s%3600)//60:02d}m{_eta_s%60:02d}s" if _eta_s >= 3600 else f"{_eta_s//60:02d}m{_eta_s%60:02d}s"
        _status = "OK" if obj.get("url") and obj.get("url") != DEFAULT_URL else "~"
        print(f"  [{_processed:4d}/{_total_want}] {_status} {sku[:55]:<55} | elapsed {int(_elapsed//60):02d}m{int(_elapsed%60):02d}s ETA {_eta_str}", flush=True)
        if _processed % SAVE_EVERY == 0:
            offers_doc["offers"] = offers
            dump_offers_doc(offers_doc)
            print(f"  --- checkpoint guardado ({filled_from_aliexpress} AliExpress, {added} nuevos, {updated} actualizados) ---", flush=True)

    if not only and set(selected_verticals) == set(available_verticals()):
        for sku, o in list(offers.items()):
            if sku not in set(sku_ctx.keys()):
                o = ensure_offer_obj(o)
                if o.get("orphaned") is not True:
                    o["orphaned"] = True
                    offers[sku] = o
                    orphaned += 1

    offers_doc["offers"] = offers
    dump_offers_doc(offers_doc)

    # Sincronizar URLs de fallback por vertical (buy-new genÃ©rico)
    if args.skip_vertical_defaults:
        print("\n  --- Saltando vertical_defaults (--skip-vertical-defaults) ---")
    else:
        print("\n  --- Sincronizando vertical_defaults (buy-new fallback) ---")
        sync_vertical_defaults(selected_verticals, force=args.force, use_cache=use_cache)

    _total_elapsed = time.time() - _t0
    _avg_api = (_api_time_real / _api_calls_real) if _api_calls_real else 0.0

    # Calcular pendientes restantes tras el proceso
    _cutoff = (datetime.now().date() - timedelta(days=args.only_stale)).isoformat() if args.only_stale > 0 else None
    _needs_url = sum(1 for s, o in offers.items() if ensure_offer_obj(o).get("needs_url") and not ensure_offer_obj(o).get("orphaned"))
    _still_stale = sum(1 for s in sku_ctx if _cutoff and str(ensure_offer_obj(offers.get(s)).get("updated_at") or "").strip() < _cutoff) if _cutoff else 0
    _status_counts: Dict[str, int] = {}
    _ai_status_counts: Dict[str, int] = {}
    _validated_links = 0
    _links_total = 0
    for s in sku_ctx.keys():
        offer_obj = ensure_offer_obj(offers.get(s))
        status = str(offer_obj.get("compatibility_status") or derive_compatibility_status(offer_obj)).strip() or "sin_cobertura"
        _status_counts[status] = _status_counts.get(status, 0) + 1
        ai_status = str(offer_obj.get("ai_validation_status") or "").strip() or "none"
        _ai_status_counts[ai_status] = _ai_status_counts.get(ai_status, 0) + 1
        if str(offer_obj.get("url") or "").strip():
            _links_total += 1
            if ai_status == "validated":
                _validated_links += 1
    _pending_ai = _ai_status_counts.get("pending", 0)
    _rejected_ai = _ai_status_counts.get("rejected", 0)
    _validated_ai = _ai_status_counts.get("validated", 0)
    _doubtful_ai = _ai_status_counts.get("doubtful", 0)
    _ai_budget = format_ai_budget_status()
    if ai_pending_reasons_run:
        _pending_reasons_summary = ",".join(
            f"{reason}:{count}"
            for reason, count in sorted(ai_pending_reasons_run.items(), key=lambda item: item[1], reverse=True)[:3]
        )
    else:
        _pending_reasons_summary = "none"
    _missing_new: List[str] = []
    for s in sorted(sku_ctx.keys()):
        ctx = sku_ctx.get(s) or {}
        if str(ctx.get("category") or "") != "nuevo":
            continue
        offer_obj = ensure_offer_obj(offers.get(s))
        if offer_obj.get("orphaned"):
            continue
        url_now = str(offer_obj.get("url") or "").strip()
        if url_now and not offer_obj.get("needs_url"):
            continue
        brand = normalize(str(ctx.get("brand") or ""))
        model = normalize(str(ctx.get("model") or s))
        vertical = normalize(str(ctx.get("vertical") or ""))
        _missing_new.append(f"{vertical}: {brand} {model}".strip())
    if _missing_new:
        _missing_new_preview = _missing_new[:5]
        _missing_new_more = len(_missing_new) - len(_missing_new_preview)
        _missing_new_sample = " | ".join(_missing_new_preview)
        if _missing_new_more > 0:
            _missing_new_sample = f"{_missing_new_sample} | +{_missing_new_more} mas"
    else:
        _missing_new_sample = "none"

    print("OK: sync_ofertas (AliExpress autolinks + catalog overrides + cache flags)")
    print(f"  Verticales:            {', '.join(selected_verticals)}")
    print(f"  Cache:                 {'ON' if use_cache else 'OFF'} (TTL={CACHE_TTL_SECONDS}s)")
    print(f"  SKUs en catÃ¡logo:       {len(set(sku_ctx.keys()))}")
    print(f"  Procesados ahora:       {len(want)}")
    print(f"  Offers total:           {len(offers)}")
    print(f"  AÃ±adidos:               {added}")
    print(f"  Actualizados:           {updated}")
    print(f"  Rellenados AliExpress:  {filled_from_aliexpress}")
    print(f"  URLs a DEFAULT:         {changed_urls_to_default}")
    print(f"  Rehabilitados:          {un_orphaned}")
    print(f"  Marcados huÃ©rfano:      {orphaned}")
    print(f"  Cache dir:              {CACHE_DIR}")
    print(f"  --- Rendimiento API ---")
    print(f"  Llamadas reales API:    {_api_calls_real}")
    print(f"  Tiempo total API:       {_api_time_real:.1f}s")
    print(f"  Media por llamada:      {_avg_api:.2f}s")
    print(f"  Tiempo total script:    {_total_elapsed:.1f}s")
    print(f"  --- Pendientes restantes ---")
    print(f"  Sin URL (needs_url):    {_needs_url}")
    print(f"  Nuevo sin enlace:       {len(_missing_new)}")
    print(f"  Compat alto:            {_status_counts.get('compatible_alto', 0)}")
    print(f"  Compat probable:        {_status_counts.get('compatible_probable', 0)}")
    print(f"  Dudosos:                {_status_counts.get('dudoso', 0)}")
    print(f"  Pendiente IA:           {_status_counts.get('pending_ai_validation', 0)}")
    print(f"  Fallback buy-new:       {_status_counts.get('fallback_buy_new', 0)}")
    print(f"  Sin cobertura:          {_status_counts.get('sin_cobertura', 0)}")
    print(f"  Stale >{args.only_stale}d:            {_still_stale}")
    print(f"  --- Validacion IA ---")
    print(f"  Checks esta ejecucion:  {_ai_validation_calls}")
    print(f"  Presupuesto diario:     {_ai_budget}")
    print(f"  Candidatos exactos IA:  {ai_exact_candidates}")
    print(f"  Candidatos rescue IA:   {ai_relaxed_candidates}")
    print(f"  Candidatos wide IA:     {ai_wide_candidates}")
    print(f"  Buy-new directos:       {ai_buy_new_direct}")
    print(f"  Enlaces IA validados:   {_validated_links}/{_links_total}")
    print(f"  SKUs IA validados:      {_validated_ai}")
    print(f"  SKUs IA dudosos:        {_doubtful_ai}")
    print(f"  SKUs IA pendientes:     {_pending_ai}")
    print(f"  SKUs IA rechazados:     {_rejected_ai}")
    print(f"  Reintentos IA agotados: {ai_pending_retries_exhausted}")
    print(f"  Razones pending IA:     {_pending_reasons_summary}")
    print(f"STATS: api_calls={_api_calls_real} avg_call={_avg_api:.2f}s total={_total_elapsed:.0f}s skus={len(want)} needs_url={_needs_url} stale={_still_stale}")
    print(
        "EXEC_SUMMARY: "
        f"processed={_processed} "
        f"added={added} "
        f"updated={updated} "
        f"filled={filled_from_aliexpress} "
        f"ai_candidates={ai_exact_candidates + ai_relaxed_candidates + ai_wide_candidates} "
        f"ai_validated_run={ai_validated_run} "
        f"to_buy_new={_status_counts.get('fallback_buy_new', 0)} "
        f"buy_new_direct={ai_buy_new_direct} "
        f"pending_ai={_status_counts.get('pending_ai_validation', 0)} "
        f"needs_url={_needs_url}"
    )
    print(f"MISSING_NEW: count={len(_missing_new)} sample={_missing_new_sample}")
    print(
        "COMPAT_STATUS: "
        f"alto={_status_counts.get('compatible_alto', 0)} "
        f"probable={_status_counts.get('compatible_probable', 0)} "
        f"dudoso={_status_counts.get('dudoso', 0)} "
        f"pending_ai={_status_counts.get('pending_ai_validation', 0)} "
        f"buy_new={_status_counts.get('fallback_buy_new', 0)} "
        f"sin_cobertura={_status_counts.get('sin_cobertura', 0)}"
    )
    print(
        "AI_STATUS: "
        f"calls={_ai_validation_calls} "
        f"exact_candidates={ai_exact_candidates} "
        f"rescue_candidates={ai_relaxed_candidates} "
        f"wide_candidates={ai_wide_candidates} "
        f"validated_run={ai_validated_run} "
        f"doubtful_run={ai_doubtful_run} "
        f"pending_run={ai_pending_run} "
        f"rejected_run={ai_rejected_run} "
        f"pending_retry_exhausted={ai_pending_retries_exhausted} "
        f"pending_reasons={_pending_reasons_summary} "
        f"validated_total={_validated_ai} "
        f"doubtful_total={_doubtful_ai} "
        f"pending_total={_pending_ai} "
        f"rejected_total={_rejected_ai} "
        f"verified_links={_validated_links}/{_links_total} "
        f"budget={_ai_budget}"
    )


if __name__ == "__main__":
    main()
