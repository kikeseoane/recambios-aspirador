from __future__ import annotations

import argparse
import re
import shutil
from pathlib import Path

try:
    import yaml
except ImportError:
    raise SystemExit("Falta PyYAML. Instala con: pip install pyyaml")

ROOT = Path(__file__).resolve().parents[1]
DATA = ROOT / "data" / "aspiradores.yaml"
CONTENT = ROOT / "content"

def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s)
    return s.strip("-")

def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)

def write_if_missing(path: Path, content: str) -> None:
    if path.exists():
        return
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")

def fm(title: str, slug: str, kind: str, extra_lines: list[str] | None = None) -> str:
    extra_lines = extra_lines or []
    extra = "\n".join(extra_lines).strip()
    if extra:
        extra = "\n" + extra + "\n"
    return f"""---
title: "{title}"
slug: "{slug}"
type: "{kind}"
draft: false
generated: true{extra}---
"""

def read_yaml() -> dict:
    if not DATA.exists():
        raise SystemExit(f"No existe {DATA}")
    return yaml.safe_load(DATA.read_text(encoding="utf-8")) or {}

def get_model_identity(brand_key: str, m: dict) -> tuple[str, str, list[str]]:
    model_name = (m.get("model") or "").strip()
    m_slug = (m.get("slug") or "").strip()
    canonical = (m.get("canonical_slug") or "").strip()

    if not canonical:
        canonical = m_slug if m_slug else slugify(f"{brand_key}-{model_name}")

    model_id = (m.get("id") or "").strip() or canonical

    aliases: list[str] = []
    raw_aliases = m.get("aliases") or []
    if isinstance(raw_aliases, list):
        aliases.extend([str(a).strip() for a in raw_aliases if str(a).strip()])
    elif isinstance(raw_aliases, str) and raw_aliases.strip():
        aliases.extend([x.strip() for x in raw_aliases.split(",") if x.strip()])

    if m_slug and m_slug != canonical and m_slug not in aliases:
        aliases.append(m_slug)

    canonical = slugify(canonical)
    model_id = slugify(model_id)

    aliases = [slugify(a) for a in aliases if a]
    aliases = [a for a in aliases if a and a != canonical]
    aliases = list(dict.fromkeys(aliases))

    if not canonical:
        raise SystemExit(f"Modelo sin slug canónico válido en brand={brand_key} model={model_name}")

    return model_id, canonical, aliases

def build_index(db: dict) -> dict:
    brands = (db.get("brands") or {}) if isinstance(db.get("brands"), dict) else {}

    seen_canonicals: dict[str, str] = {}
    seen_ids: dict[str, str] = {}
    problems: list[str] = []

    for brand_key, brand in brands.items():
        for m in (brand or {}).get("models", []) or []:
            model_id, canonical, _aliases = get_model_identity(brand_key, m)

            if canonical in seen_canonicals and seen_canonicals[canonical] != model_id:
                problems.append(f"Duplicado canonical_slug='{canonical}' para ids: {seen_canonicals[canonical]} y {model_id}")

            if model_id in seen_ids and seen_ids[model_id] != canonical:
                problems.append(f"Duplicado id='{model_id}' apunta a canonicals: {seen_ids[model_id]} y {canonical}")

            seen_canonicals[canonical] = model_id
            seen_ids[model_id] = canonical

    if problems:
        msg = "ERRORES de deduplicación en YAML:\n- " + "\n- ".join(problems)
        raise SystemExit(msg)

    return {"brands": brands}

def is_generated_index(path: Path) -> bool:
    candidates = [path / "_index.md", path / "index.md"]
    for idx in candidates:
        if not idx.exists():
            continue
        txt = idx.read_text(encoding="utf-8", errors="ignore")
        if "generated: true" in txt:
            return True
    return False

def clean_duplicate_model_dirs(canonical_slugs: set[str], dry_run: bool = True) -> None:
    modelos_dir = CONTENT / "modelos"
    if not modelos_dir.exists():
        return

    for p in modelos_dir.iterdir():
        if not p.is_dir():
            continue
        slug = p.name
        if slug in canonical_slugs:
            continue

        if is_generated_index(p):
            if dry_run:
                print(f"[DRY-RUN] Borraría duplicado generado: {p}")
            else:
                print(f"Borrando duplicado generado: {p}")
                shutil.rmtree(p, ignore_errors=True)
        else:
            print(f"[SKIP] No borro (no parece generado): {p}")

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--clean", action="store_true")
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()

    db = read_yaml()
    idx = build_index(db)
    brands = idx["brands"]

    write_if_missing(CONTENT / "_index.md", fm("Inicio", "", "home"))
    write_if_missing(CONTENT / "marcas" / "_index.md", fm("Marcas", "marcas", "marcas"))
    write_if_missing(CONTENT / "modelos" / "_index.md", fm("Modelos", "modelos", "modelos"))
    write_if_missing(CONTENT / "guias" / "_index.md", fm("Guías", "guias", "guias"))

    write_if_missing(CONTENT / "guias" / "seguridad.md", fm("Seguridad", "seguridad", "guia", ['guideKey: "seguridad"']))
    write_if_missing(CONTENT / "guias" / "mantenimiento.md", fm("Mantenimiento", "mantenimiento", "guia", ['guideKey: "mantenimiento"']))
    write_if_missing(CONTENT / "guias" / "compra.md", fm("Cómo elegir recambio", "compra", "guia", ['guideKey: "compra"']))

    canonical_slugs: set[str] = set()

    for brand_key, brand in brands.items():
        brand_name = (brand or {}).get("name") or brand_key
        brand_slug = slugify(brand_key)

        write_if_missing(
            CONTENT / "marcas" / brand_slug / "_index.md",
            fm(f"{brand_name}", brand_slug, "marca", [f'brandKey: "{brand_key}"'])
        )

        for m in (brand or {}).get("models", []) or []:
            model = (m.get("model") or "").strip()
            model_id, canonical, aliases = get_model_identity(brand_key, m)

            canonical_slugs.add(canonical)

            title = f"{brand_name} {model}".strip()

            extra_lines = [
                f'brandKey: "{brand_key}"',
                f'modelId: "{model_id}"',
                f'canonicalSlug: "{canonical}"',
            ]
            if aliases:
                extra_lines.append("aliases:")
                extra_lines.extend([f'  - "{a}"' for a in aliases])

            write_if_missing(
                CONTENT / "modelos" / canonical / "index.md",
                fm(title, canonical, "modelo", extra_lines)
            )

    if args.clean:
        clean_duplicate_model_dirs(canonical_slugs, dry_run=args.dry_run)

    print("OK: stubs creados (solo si faltaban).")
    if args.clean:
        print("OK: limpieza ejecutada." if not args.dry_run else "OK: limpieza simulada (dry-run).")

if __name__ == "__main__":
    main()

