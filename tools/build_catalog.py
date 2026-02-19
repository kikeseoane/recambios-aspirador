# tools/build_catalog.py
# -*- coding: utf-8 -*-
from __future__ import annotations

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import yaml

ROOT = Path(__file__).resolve().parents[1]
PARTS_YAML = ROOT / "data" / "catalog_parts.yaml"
BRANDS_YAML = ROOT / "data" / "catalog_brands.yaml"
SKUS_YAML = ROOT / "data" / "catalog_skus.yaml"
OUT_YAML = ROOT / "data" / "aspiradores.yaml"

SLUG_RE = re.compile(r"^[a-z0-9]+(?:-[a-z0-9]+)*$")


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


def compile_catalog() -> dict:
    parts = load_yaml(PARTS_YAML)
    brands_doc = load_yaml(BRANDS_YAML)
    skus_doc = load_yaml(SKUS_YAML)

    globals_obj = parts.get("globals") or {}
    sku_packs = parts.get("sku_packs") or {}
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

    for brand_key, brand_obj in brands.items():
        if not isinstance(brand_obj, dict):
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
            compiled_models.append(m_out)

        b_out["models"] = compiled_models
        out["brands"][brand_key] = b_out

    return out


def main() -> None:
    compiled = compile_catalog()
    dump_yaml(OUT_YAML, compiled)
    print(f"OK: catálogo compilado -> {OUT_YAML}")


if __name__ == "__main__":
    main()
