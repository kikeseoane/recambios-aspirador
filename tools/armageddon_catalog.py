# tools/armageddon_catalog.py
# -*- coding: utf-8 -*-
#Desde raíz del repo:

#Validar catálogo:

#python tools/armageddon_catalog.py --validate


#Regenerar stubs (sin borrar contenido manual):

#python tools/armageddon_catalog.py --generate-stubs --force


#Sincronizar ofertas (fresh):

#python tools/armageddon_catalog.py --sync-offers --no-cache --force-lookup


#Todo en uno:

#python tools/armageddon_catalog.py --all --force --clean-all --no-cache --force-lookup*/
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
try:
    from dotenv import load_dotenv  # type: ignore
    load_dotenv()
except Exception:
    pass

import requests
import yaml


# =========================
# Paths / constants
# =========================
ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "data" / "aspiradores.yaml"
OFFERS = ROOT / "data" / "ofertas.yaml"
CONTENT = ROOT / "content"

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


# =========================
# YAML helpers
# =========================
def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise SystemExit(f"No existe {path}")
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def dump_yaml(path: Path, data: dict) -> None:
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True).strip() + "\n"
    path.write_text(text, encoding="utf-8")


def normalize(s: str) -> str:
    return " ".join((s or "").strip().split())


def nrm(s: str) -> str:
    return " ".join((s or "").lower().split())


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


# =========================
# Catalog resolution (inline recambios OR recambios_ref)
# =========================
def catalog_categories(db: dict) -> List[str]:
    cats = []
    g = db.get("globals") or {}
    for c in (g.get("categorias_recambios") or []):
        if isinstance(c, dict) and c.get("key"):
            cats.append(str(c["key"]).strip().lower())
    return list(dict.fromkeys(cats))


def resolve_model_recambios(db: dict, model_obj: dict) -> Dict[str, List[dict]]:
    """
    Retrocompatible:
      - si model_obj.recambios es dict => devuelve ese
      - si model_obj.recambios_ref => busca db.catalog_recambios[ref]
    """
    rec = model_obj.get("recambios")
    if isinstance(rec, dict):
        out: Dict[str, List[dict]] = {}
        for k, v in rec.items():
            if isinstance(v, list):
                out[str(k).strip().lower()] = [x for x in v if isinstance(x, dict)]
        return out

    ref = str(model_obj.get("recambios_ref") or "").strip()
    if not ref:
        return {}

    cat_db = db.get("catalog_recambios") or {}
    block = cat_db.get(ref)
    if not isinstance(block, dict):
        return {}

    out2: Dict[str, List[dict]] = {}
    for k, v in block.items():
        if isinstance(v, list):
            out2[str(k).strip().lower()] = [x for x in v if isinstance(x, dict)]
    return out2


# =========================
# VALIDATION (hard fail early)
# =========================
def validate_catalog(db: dict) -> None:
    brands = db.get("brands")
    if not isinstance(brands, dict) or not brands:
        raise SystemExit("ERROR: aspiradores.yaml -> falta 'brands' (dict).")

    cats = set(catalog_categories(db))
    if not cats:
        raise SystemExit("ERROR: globals.categorias_recambios vacío o inválido.")

    seen_model_slugs: set[str] = set()
    seen_skus: set[str] = set()

    for brand_key, brand in brands.items():
        if not isinstance(brand, dict):
            continue
        models = brand.get("models") or []
        if not isinstance(models, list):
            raise SystemExit(f"ERROR: brands.{brand_key}.models debe ser lista.")

        for m in models:
            if not isinstance(m, dict):
                continue
            slug = str(m.get("slug") or "").strip()
            name = str(m.get("model") or "").strip()
            if not slug or not name:
                raise SystemExit(f"ERROR: modelo sin slug/model en marca {brand_key}.")

            if slug in seen_model_slugs:
                raise SystemExit(f"ERROR: slug duplicado: {slug}")
            seen_model_slugs.add(slug)

            rec = resolve_model_recambios(db, m)
            # rec puede estar vacío, pero si existe debe tener cats válidas
            for cat_key, items in rec.items():
                if cat_key not in cats:
                    raise SystemExit(f"ERROR: cat '{cat_key}' no existe en globals.categorias_recambios (modelo {slug}).")
                for it in items:
                    sku = str(it.get("sku") or "").strip()
                    title = str(it.get("title") or "").strip()
                    if not sku or not title:
                        raise SystemExit(f"ERROR: item sin sku/title en modelo {slug} cat {cat_key}.")
                    if sku in seen_skus:
                        raise SystemExit(f"ERROR: sku duplicado global: {sku}")
                    seen_skus.add(sku)

    # OK
    print(f"OK: catálogo válido. Modelos={len(seen_model_slugs)} SKUs={len(seen_skus)} Categorías={len(cats)}")


# =========================
# STUB GENERATION (based on your generar.py, improved)
# =========================
def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s)
    return s.strip("-")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_file(path: Path, content: str, force: bool = False) -> None:
    if path.exists() and not force:
        return
    ensure_dir(path.parent)
    if not content.endswith("\n"):
        content += "\n"
    path.write_text(content, encoding="utf-8", newline="\n")


def fm(*, title: str, slug: str | None = None, kind: str | None = None, extra: dict | None = None, generated: bool = True) -> str:
    data: dict = {"title": title, "draft": False}
    if slug is not None:
        data["slug"] = slug
    if generated:
        data["generated"] = True
    if kind:
        data["type"] = kind
    if extra:
        data.update(extra)
    body = yaml.safe_dump(data, sort_keys=False, allow_unicode=True, default_flow_style=False).strip()
    return f"---\n{body}\n---\n"


def is_generated_file(path: Path) -> bool:
    if not path.exists() or not path.is_file():
        return False
    txt = path.read_text(encoding="utf-8", errors="ignore")
    return "generated: true" in txt.lower()


def safe_clean_section(section_dir: Path) -> None:
    if not section_dir.exists():
        return
    for child in section_dir.iterdir():
        if child.is_file() and child.name == "_index.md":
            continue
        if child.is_dir():
            idx_leaf = child / "index.md"
            idx_branch = child / "_index.md"
            if is_generated_file(idx_leaf) or is_generated_file(idx_branch):
                for p in sorted(child.rglob("*"), reverse=True):
                    if p.is_file():
                        p.unlink()
                    else:
                        p.rmdir()
                child.rmdir()
            continue
        if child.is_file() and child.suffix.lower() == ".md":
            if is_generated_file(child):
                child.unlink()


def ensure_model_branch_bundle(model_dir: Path) -> Path:
    ensure_dir(model_dir)
    leaf = model_dir / "index.md"
    branch = model_dir / "_index.md"
    if leaf.exists() and branch.exists():
        raise SystemExit(f"ERROR: Inconsistencia crítica en {model_dir}: existen index.md y _index.md")
    if leaf.exists() and not branch.exists():
        leaf.rename(branch)
    return branch


def clean_model_name(brand_name: str, model_name: str) -> str:
    mn = (model_name or "").strip()
    bn = (brand_name or "").strip()
    if mn and bn and mn.lower().startswith(bn.lower()):
        mn = mn[len(bn):].strip()
    return mn


def cat_title_es_from_globals(db: dict, cat_key: str) -> str:
    g = db.get("globals") or {}
    for c in (g.get("categorias_recambios") or []):
        if isinstance(c, dict) and str(c.get("key") or "").strip().lower() == cat_key:
            return str(c.get("label") or cat_key).strip()
    # fallback
    return cat_key.title()


def generate_stubs(db: dict, force: bool, clean_all: bool) -> None:
    if clean_all:
        safe_clean_section(CONTENT / "modelos")
        safe_clean_section(CONTENT / "marcas")
        safe_clean_section(CONTENT / "guias")
        print("OK: limpieza segura (generated:true) ejecutada.")

    # Secciones
    write_file(CONTENT / "marcas" / "_index.md", fm(title="Marcas", slug=None, kind=None, extra=None), force=force)
    write_file(CONTENT / "modelos" / "_index.md", fm(title="Modelos", slug=None, kind=None, extra=None), force=force)
    write_file(CONTENT / "guias" / "_index.md", fm(title="Guías", slug=None, kind=None, extra=None), force=force)

    # Guías genéricas
    write_file(CONTENT / "guias" / "seguridad.md", fm(title="Seguridad", slug="seguridad", kind="guia", extra={"guideKey": "seguridad"}), force=force)
    write_file(CONTENT / "guias" / "mantenimiento.md", fm(title="Mantenimiento", slug="mantenimiento", kind="guia", extra={"guideKey": "mantenimiento"}), force=force)
    write_file(CONTENT / "guias" / "compra.md", fm(title="Cómo elegir recambio", slug="compra", kind="guia", extra={"guideKey": "compra"}), force=force)

    brands = db.get("brands") or {}
    cats = catalog_categories(db)

    for brand_key, brand in (brands.items() if isinstance(brands, dict) else []):
        if not isinstance(brand, dict):
            continue
        brand_name = brand.get("name") or brand_key
        brand_slug = slugify(brand_key)

        # Marca
        write_file(
            CONTENT / "marcas" / brand_slug / "_index.md",
            fm(title=str(brand_name), slug=None, kind=None, extra={"brandKey": brand_key}),
            force=force,
        )

        # Modelos
        for m in (brand.get("models", []) or []):
            if not isinstance(m, dict):
                continue

            model_name_raw = (m.get("model") or "").strip()
            model_name = clean_model_name(str(brand_name), model_name_raw)
            model_slug = (m.get("slug") or "").strip() or slugify(f"{brand_key}-{model_name}")
            title = f"{brand_name} {model_name}".strip()

            model_dir = CONTENT / "modelos" / model_slug
            model_index = ensure_model_branch_bundle(model_dir)

            write_file(
                model_index,
                fm(title=title, slug=None, kind=None, extra={"brandKey": brand_key, "modelSlug": model_slug}),
                force=force,
            )

            # ✅ Hubs por categoría: GENERA SIEMPRE según globals.categorias_recambios
            for cat_key in cats:
                hub_dir = CONTENT / "modelos" / model_slug / slugify(cat_key)
                hub_title = f"{brand_name} {model_name} · {cat_title_es_from_globals(db, cat_key)}"
                write_file(
                    hub_dir / "index.md",
                    fm(
                        title=hub_title,
                        slug=None,
                        kind=None,
                        extra={"brandKey": brand_key, "modelSlug": model_slug, "catKey": slugify(cat_key), "layout": "recambio"},
                    ),
                    force=force,
                )

            # Problemas
            problems = (m.get("problemas") or [])
            if isinstance(problems, list) and problems:
                write_file(
                    CONTENT / "modelos" / model_slug / "problemas" / "_index.md",
                    fm(
                        title=f"Problemas frecuentes de {title}",
                        slug=None,
                        kind=None,
                        extra={"brandKey": brand_key, "modelSlug": model_slug, "layout": "problemas"},
                    ),
                    force=force,
                )
                for p in problems:
                    if not isinstance(p, dict):
                        continue
                    pkey = slugify(p.get("key") or "")
                    ptitle = (p.get("title") or "").strip()
                    if not pkey or not ptitle:
                        continue
                    pdir = CONTENT / "modelos" / model_slug / "problemas" / pkey
                    write_file(
                        pdir / "index.md",
                        fm(
                            title=ptitle,
                            slug=None,
                            kind=None,
                            extra={"brandKey": brand_key, "modelSlug": model_slug, "problemKey": pkey, "layout": "problema"},
                        ),
                        force=force,
                    )

    print("OK: stubs generados.")


# =========================
# OFFERS SYNC (ported from your sync_ofertas.py, but supports recambios_ref too)
# =========================
def is_empty(value: object | None) -> bool:
    return value is None or str(value).strip() == ""


def is_placeholder(value: object | None, placeholder: str) -> bool:
    if is_empty(value):
        return True
    return str(value).strip() == placeholder


def ensure_offer_obj(existing: object | None) -> dict:
    return existing if isinstance(existing, dict) else {}


def guess_brand_name(brand_key: str, brand_obj: dict) -> str:
    return normalize(str(brand_obj.get("name") or brand_obj.get("title") or brand_key))


def guess_model_name(model_obj: dict) -> str:
    return normalize(str(model_obj.get("name") or model_obj.get("title") or model_obj.get("model") or ""))


def guess_item_title(item_obj: dict) -> str:
    return normalize(str(item_obj.get("title") or item_obj.get("name") or item_obj.get("label") or ""))


def sku_records_from_catalog(db: dict) -> Dict[str, Dict[str, Any]]:
    """
    Lee SKUs desde recambios inline O recambios_ref resuelto
    """
    out: Dict[str, Dict[str, Any]] = {}
    brands = db.get("brands") or {}
    if not isinstance(brands, dict):
        return out

    for brand_key, brand in brands.items():
        brand = brand or {}
        brand_name = guess_brand_name(str(brand_key), brand)

        models = brand.get("models") or []
        if not isinstance(models, list):
            continue

        for m in models:
            if not isinstance(m, dict):
                continue
            model_name = guess_model_name(m)

            rec = resolve_model_recambios(db, m)
            if not isinstance(rec, dict):
                continue

            for cat, items in rec.items():
                if not items or not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
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
                        "query": normalize(str(it.get("query") or "")),
                        "must_include": ensure_list_str(it.get("must_include")),
                        "must_not_include": ensure_list_str(it.get("must_not_include")),
                        "model_tokens": [nrm(x) for x in ensure_list_str(it.get("model_tokens"))],
                    }
    return out


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


def ali_call_flat(method: str, biz_params: Dict[str, Any], use_cache: bool = True) -> Dict[str, Any]:
    if not APP_KEY or not APP_SECRET:
        raise SystemExit("Faltan variables de entorno: ALI_APP_KEY y/o ALI_APP_SECRET")

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
        raise RuntimeError(f"AliExpress error_response: code={er.get('code')} msg={er.get('msg')} sub_msg={er.get('sub_msg')}")

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


def score_product(title: str, p: Dict[str, Any], must_brand: str, part_terms: List[str], req_models: List[str]) -> float:
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


def merge_overrides(sku: str, ctx: Dict[str, Any], offers_obj: Dict[str, Any]) -> Tuple[str, List[str], List[str], List[str]]:
    query = normalize(str(ctx.get("query") or ""))
    if not query:
        query = normalize(str(offers_obj.get("query") or ""))
    must_include = ensure_list_str(ctx.get("must_include")) or ensure_list_str(offers_obj.get("must_include"))
    must_not_include = ensure_list_str(ctx.get("must_not_include")) or ensure_list_str(offers_obj.get("must_not_include"))
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
            return str(url).strip()

    return None


def sync_offers(db: dict, no_cache: bool, force_lookup: bool, only_sku: List[str]) -> None:
    use_cache = not no_cache

    sku_ctx = sku_records_from_catalog(db)
    want = set(sku_ctx.keys())

    offers_doc = {}
    if OFFERS.exists():
        offers_doc = yaml.safe_load(OFFERS.read_text(encoding="utf-8")) or {}
    offers = offers_doc.get("offers")
    if not isinstance(offers, dict):
        offers = {}

    only = [str(x).strip() for x in only_sku if str(x).strip()]
    if only:
        want = set([s for s in only if s in sku_ctx])

    added = updated = orphaned = un_orphaned = changed_urls_to_default = filled_from_aliexpress = 0
    today = datetime.now().date().isoformat()

    for sku in sorted(want):
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
            force_lookup
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

            query, must_include, must_not_override, model_tokens_override = merge_overrides(sku, ctx, obj)
            must_not_combined = [*must_not_default, *must_not_override]

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

    if not only:
        for sku, o in list(offers.items()):
            if sku not in set(sku_ctx.keys()):
                o = ensure_offer_obj(o)
                if o.get("orphaned") is not True:
                    o["orphaned"] = True
                    offers[sku] = o
                    orphaned += 1

    dump_yaml(OFFERS, {"offers": offers})

    print("OK: sync_ofertas (armageddon unified)")
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


# =========================
# CLI
# =========================
def parse_args() -> argparse.Namespace:
    ap = argparse.ArgumentParser(description="Armageddon catalog: validate + generate stubs + sync offers")
    ap.add_argument("--validate", action="store_true")
    ap.add_argument("--generate-stubs", action="store_true")
    ap.add_argument("--sync-offers", action="store_true")
    ap.add_argument("--all", action="store_true")
    ap.add_argument("--force", action="store_true", help="Sobrescribe stubs generados")
    ap.add_argument("--clean-all", action="store_true", help="Limpia stubs generados (generated:true) en secciones")
    ap.add_argument("--no-cache", action="store_true", help="Ignora cache AliExpress")
    ap.add_argument("--only-sku", action="append", default=[], help="Solo procesa este SKU (repetible)")
    ap.add_argument("--force-lookup", action="store_true", help="Fuerza lookup incluso si ya hay URL no-default")
    return ap.parse_args()


def main() -> None:
    args = parse_args()
    db = load_yaml(CATALOG)

    do_all = args.all or (not args.validate and not args.generate_stubs and not args.sync_offers)

    if args.validate or do_all:
        validate_catalog(db)

    if args.generate_stubs or do_all:
        generate_stubs(db, force=args.force, clean_all=args.clean_all)

    if args.sync_offers or do_all:
        sync_offers(db, no_cache=args.no_cache, force_lookup=args.force_lookup, only_sku=args.only_sku)


if __name__ == "__main__":
    main()
