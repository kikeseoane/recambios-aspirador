# tools/build_catalog.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import argparse
import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[1]
PARTS_YAML = ROOT / "data" / "catalog_parts.yaml"
BRANDS_YAML = ROOT / "data" / "catalog_brands.yaml"
SKUS_YAML = ROOT / "data" / "catalog_skus.yaml"
VERTICALS_YAML = ROOT / "data" / "verticals.yaml"

SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")

# Packs por defecto según sku_pack (se puede sobreescribir por modelo en catalog_brands.yaml)
SKU_PACK_DEFAULTS: Dict[str, Dict[str, str]] = {
    "stick10": {
        "compatibility_pack": "cordless_base",
        "problem_pack": "cordless_base",
        "faq_pack": "recambio_base",
    },
    "robot10": {
        "compatibility_pack": "robot_base",
        "problem_pack": "robot_base",
        "faq_pack": "robot_base",
    },
    "shaver10": {
        "compatibility_pack": "shaver_base",
        "problem_pack": "shaver_base",
        "faq_pack": "shaver_base",
    },
    "cafetera10": {
        "compatibility_pack": "cafetera_base",
        "problem_pack": "cafetera_base",
        "faq_pack": "cafetera_base",
    },
    "power-tool10": {
        "compatibility_pack": "power_tool_base",
        "problem_pack": "power_tool_base",
        "faq_pack": "power_tool_base",
    },
    "toothbrush10": {
        "compatibility_pack": "toothbrush_base",
        "problem_pack": "toothbrush_base",
        "faq_pack": "toothbrush_base",
    },
    "airfryer10": {
        "compatibility_pack": "airfryer_base",
        "problem_pack": "airfryer_base",
        "faq_pack": "airfryer_base",
    },
}


def load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def dump_yaml(path: Path, data: dict) -> None:
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True).strip() + "\n"
    path.write_text(text, encoding="utf-8")


def nrm(s: str) -> str:
    return " ".join((s or "").strip().split())


def assert_slug(slug: str, where: str) -> None:
    if not slug or not SLUG_RE.match(slug):
        raise SystemExit(f"ERROR: slug inválido '{slug}' en {where}. Usa kebab-case (a-z0-9-).")


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


def first_model_token(model_tokens: List[str], model_name: str) -> str:
    # Preferimos tokens cortos tipo v11 / s7 / 3090, si existen
    for t in model_tokens:
        tt = t.strip().lower()
        if re.fullmatch(r"[a-z]*\d[\w\.\-]*", tt):
            return tt
    # Fallback: primera palabra del modelo
    return (model_name.split()[:1] or ["model"])[0].lower()


def build_sku_id(brand_key: str, model_slug: str, cat_key: str, suffix: str) -> str:
    # Mantén estable y determinista
    return f"{brand_key}-{model_slug}-{cat_key}-{suffix}"


def apply_tpl(text: str, model: str, model_token: str) -> str:
    return (text or "").replace("{model}", model).replace("{model_token}", model_token)


def apply_tpl_list(items: List[str], model: str, model_token: str) -> List[str]:
    out: List[str] = []
    for x in items or []:
        s = apply_tpl(str(x), model, model_token).strip()
        if s:
            out.append(s)
    return out


def compile_problem_pack(pack_items: List[dict], model_name: str, model_token: str) -> List[dict]:
    out: List[dict] = []
    for p in pack_items or []:
        if not isinstance(p, dict):
            continue
        obj: Dict[str, Any] = {}
        for k in ("key", "intent", "cta_cat", "fix_hint"):
            if p.get(k):
                obj[k] = apply_tpl(str(p[k]), model_name, model_token).strip()
        if p.get("title"):
            obj["title"] = apply_tpl(str(p["title"]), model_name, model_token).strip()
        for lk in ("symptoms", "causes", "checks"):
            vals = apply_tpl_list(ensure_list_str(p.get(lk)), model_name, model_token)
            if vals:
                obj[lk] = vals
        if obj:
            out.append(obj)
    return out


def compile_faq_pack(pack_items: List[dict], model_name: str, model_token: str) -> List[dict]:
    out: List[dict] = []
    for p in pack_items or []:
        if not isinstance(p, dict):
            continue
        q = apply_tpl(str(p.get("q") or ""), model_name, model_token).strip()
        a = apply_tpl(str(p.get("a") or ""), model_name, model_token).strip()
        if q and a:
            out.append({"q": q, "a": a})
    return out


def compile_catalog(vertical: str = "aspiradores") -> dict:
    parts = load_yaml(PARTS_YAML)
    # Brand loading: per-brand directory > vertical-specific file > main file
    brands_dir = ROOT / "data" / "brands" / vertical
    brands_path_specific = ROOT / "data" / f"catalog_brands_{vertical}.yaml"

    if brands_dir.exists() and any(brands_dir.glob("*.yaml")):
        # Load from per-brand files
        brands_raw: Dict[str, Any] = {}
        for bf in sorted(brands_dir.glob("*.yaml")):
            bd = yaml.safe_load(bf.read_text(encoding="utf-8")) or {}
            bk = bd.get("brand_key") or bf.stem
            brand_data = {k: v for k, v in bd.items() if k != "brand_key"}
            brands_raw[bk] = brand_data
        brands_doc = {"brands": brands_raw}
        using_vertical_file = True
    elif brands_path_specific.exists():
        brands_doc = load_yaml(brands_path_specific)
        using_vertical_file = True
    else:
        brands_doc = load_yaml(BRANDS_YAML)
        using_vertical_file = False
    skus_doc = load_yaml(SKUS_YAML)

    globals_obj = parts.get("globals") or {}
    sku_packs = parts.get("sku_packs") or {}
    problem_packs = parts.get("problem_packs") or {}
    compatibility_packs = parts.get("compatibility_packs") or {}
    faq_packs = parts.get("faq_packs") or {}
    categories = globals_obj.get("categorias_recambios") or []
    valid_cats = {str(c.get("key")).strip() for c in categories if isinstance(c, dict) and c.get("key")}

    model_overrides = (skus_doc.get("model_overrides") or {}) if isinstance(skus_doc.get("model_overrides"), dict) else {}
    sku_overrides = (skus_doc.get("sku_overrides") or {}) if isinstance(skus_doc.get("sku_overrides"), dict) else {}

    out: Dict[str, Any] = {}
    out["globals"] = globals_obj
    out["brands"] = {}

    brands = brands_doc.get("brands") or {}
    if not isinstance(brands, dict):
        raise SystemExit("ERROR: data/catalog_brands.yaml: 'brands' debe ser un dict.")

    # Validaciones base
    seen_model_slugs: set[str] = set()

    for brand_key, brand_obj in list(brands.items()):
        if not isinstance(brand_obj, dict):
            continue
        # If using main brands file, filter by vertical
        if not using_vertical_file:
            brand_vertical = brand_obj.get("vertical", "aspiradores")
            if brand_vertical != vertical:
                continue

        brand_name = nrm(str(brand_obj.get("name") or brand_key))
        b_out: Dict[str, Any] = {
            "name": brand_name,
        }
        if brand_obj.get("country_hint"):
            b_out["country_hint"] = brand_obj["country_hint"]
        if brand_obj.get("notes_brand"):
            b_out["notes_brand"] = brand_obj["notes_brand"]

        models = brand_obj.get("models") or []
        if not isinstance(models, list):
            models = []

        compiled_models: List[dict] = []

        for m in models:
            if not isinstance(m, dict):
                continue

            model_name = nrm(str(m.get("model") or ""))
            model_slug = nrm(str(m.get("slug") or ""))
            if not model_name or not model_slug:
                continue

            assert_slug(model_slug, f"brand={brand_key} model={model_name}")
            if model_slug in seen_model_slugs:
                raise SystemExit(f"ERROR: slug de modelo duplicado: {model_slug}")
            seen_model_slugs.add(model_slug)

            sku_pack = nrm(str(m.get("sku_pack") or ""))
            if sku_pack and sku_pack not in sku_packs:
                raise SystemExit(f"ERROR: sku_pack '{sku_pack}' no existe en {PARTS_YAML}")

            model_tokens = ensure_list_str(m.get("model_tokens"))
            model_token = first_model_token(model_tokens, model_name)

            # Derivar packs de contenido editorial (explícito en YAML > defecto por sku_pack)
            pack_defaults = SKU_PACK_DEFAULTS.get(sku_pack, {})
            compatibility_pack = nrm(str(m.get("compatibility_pack") or pack_defaults.get("compatibility_pack") or ""))
            faq_pack = nrm(str(m.get("faq_pack") or pack_defaults.get("faq_pack") or ""))
            problem_pack = nrm(str(m.get("problem_pack") or pack_defaults.get("problem_pack") or ""))

            if compatibility_pack and compatibility_pack not in compatibility_packs:
                raise SystemExit(f"ERROR: compatibility_pack '{compatibility_pack}' no existe en {PARTS_YAML}")
            if faq_pack and faq_pack not in faq_packs:
                raise SystemExit(f"ERROR: faq_pack '{faq_pack}' no existe en {PARTS_YAML}")
            if problem_pack and problem_pack not in problem_packs:
                raise SystemExit(f"ERROR: problem_pack '{problem_pack}' no existe en {PARTS_YAML}")

            m_out: Dict[str, Any] = dict(m)  # copia campos (seo/specs/problemas/etc)
            m_out["model"] = model_name
            m_out["slug"] = model_slug

            # Normaliza listas claves
            if model_tokens:
                m_out["model_tokens"] = [t.strip() for t in model_tokens]

            # recambios autogenerados
            recambios: Dict[str, List[dict]] = {}

            if sku_pack:
                pack_items = sku_packs.get(sku_pack) or []
                if not isinstance(pack_items, list):
                    pack_items = []

                # overrides por modelo
                mo = model_overrides.get(model_slug) if isinstance(model_overrides.get(model_slug), dict) else {}
                extra_model_tokens = ensure_list_str(mo.get("extra_model_tokens"))
                extra_must_include = ensure_list_str(mo.get("extra_must_include"))
                extra_must_not_include = ensure_list_str(mo.get("extra_must_not_include"))
                query_hint = nrm(str(mo.get("query_hint") or ""))

                effective_model_tokens = list(dict.fromkeys([*model_tokens, *extra_model_tokens]))
                effective_model_token = first_model_token(effective_model_tokens, model_name)

                for p in pack_items:
                    if not isinstance(p, dict):
                        continue

                    cat_key = nrm(str(p.get("catKey") or ""))
                    suffix = nrm(str(p.get("sku_suffix") or ""))
                    if not cat_key or not suffix:
                        continue

                    assert_slug(cat_key, f"catKey en pack {sku_pack}")
                    if cat_key not in valid_cats:
                        raise SystemExit(f"ERROR: catKey '{cat_key}' no está en globals.categorias_recambios")

                    sku = build_sku_id(str(brand_key), model_slug, cat_key, suffix)

                    title_tpl = nrm(str(p.get("title_tpl") or "Recambio compatible para {model}"))
                    title = apply_tpl(title_tpl, model_name, effective_model_token)

                    # query: hint por modelo > plantilla simple
                    query = query_hint or f"{brand_name} {model_name} {cat_key} {title}".strip()
                    query = query[:120]

                    must_include = []
                    for x in ensure_list_str(p.get("must_include_tpl")):
                        must_include.append(apply_tpl(x, model_name, effective_model_token))
                    must_include = [x.strip() for x in must_include if x.strip()]
                    must_include = [*must_include, *extra_must_include]

                    must_not_include = ensure_list_str(p.get("must_not_include"))
                    must_not_include = [*must_not_include, *extra_must_not_include]

                    item = {
                        "sku": sku,
                        "title": title,
                        "intent": p.get("intent") or "compra",
                        "query": query,
                    }
                    if must_include:
                        item["must_include"] = must_include
                    if must_not_include:
                        item["must_not_include"] = must_not_include
                    if effective_model_tokens:
                        item["model_tokens"] = effective_model_tokens

                    # overrides por SKU final
                    so = sku_overrides.get(sku) if isinstance(sku_overrides.get(sku), dict) else None
                    if so:
                        if so.get("title"):
                            item["title"] = nrm(str(so["title"]))
                        if so.get("query"):
                            item["query"] = nrm(str(so["query"]))[:120]
                        if so.get("must_include") is not None:
                            item["must_include"] = ensure_list_str(so.get("must_include"))
                        if so.get("must_not_include") is not None:
                            item["must_not_include"] = ensure_list_str(so.get("must_not_include"))
                        if so.get("model_tokens") is not None:
                            item["model_tokens"] = ensure_list_str(so.get("model_tokens"))

                    recambios.setdefault(cat_key, []).append(item)

            # Validación: 10 SKUs/modelo si hay sku_pack
            if sku_pack:
                n_skus = sum(len(v) for v in recambios.values())
                if n_skus != 10:
                    raise SystemExit(f"ERROR: modelo {model_slug} debería tener 10 SKUs (pack={sku_pack}) y tiene {n_skus}")

            m_out["recambios"] = recambios

            # Token efectivo para plantillas de contenido editorial
            tpl_token = effective_model_token if sku_pack else model_token

            # Compilar compatibilidad (siempre, es campo nuevo)
            if compatibility_pack:
                compiled_compat = apply_tpl_list(
                    ensure_list_str(compatibility_packs.get(compatibility_pack)),
                    model_name,
                    tpl_token,
                )
                if compiled_compat:
                    m_out["compatibilidad"] = compiled_compat

            # Compilar problemas desde pack solo si el modelo no tiene problemas manuales
            if problem_pack and not m_out.get("problemas"):
                compiled_probs = compile_problem_pack(
                    problem_packs.get(problem_pack) or [],
                    model_name,
                    tpl_token,
                )
                if compiled_probs:
                    m_out["problemas"] = compiled_probs

            # Compilar FAQs (siempre, es campo nuevo)
            if faq_pack:
                compiled_faqs = compile_faq_pack(
                    faq_packs.get(faq_pack) or [],
                    model_name,
                    tpl_token,
                )
                if compiled_faqs:
                    m_out["faqs"] = compiled_faqs

            compiled_models.append(m_out)

        b_out["models"] = compiled_models
        out["brands"][brand_key] = b_out

    return out


def main() -> None:
    ap = argparse.ArgumentParser(description="Compila el catálogo de recambios para un vertical")
    ap.add_argument("--vertical", default="aspiradores", help="Vertical a compilar (default: aspiradores)")
    args = ap.parse_args()

    out_path = ROOT / "data" / f"{args.vertical}.yaml"
    compiled = compile_catalog(vertical=args.vertical)
    dump_yaml(out_path, compiled)
    print(f"OK: catálogo compilado -> {out_path}")


if __name__ == "__main__":
    main()
