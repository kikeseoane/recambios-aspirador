from __future__ import annotations
import os
from pathlib import Path
import re

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

def fm(title: str, slug: str, kind: str, extra: str = "") -> str:
    extra = extra.strip()
    if extra:
        extra = "\n" + extra + "\n"
    return f"""---
title: "{title}"
slug: "{slug}"
type: "{kind}"
draft: false
{extra}---
"""

def main() -> None:
    if not DATA.exists():
        raise SystemExit(f"No existe {DATA}")

    db = yaml.safe_load(DATA.read_text(encoding="utf-8")) or {}
    brands = db.get("brands", {}) or {}

    # Home + secciones
    write_if_missing(CONTENT / "_index.md", fm("Inicio", "", "home"))
    write_if_missing(CONTENT / "marcas" / "_index.md", fm("Marcas", "marcas", "marcas"))
    write_if_missing(CONTENT / "modelos" / "_index.md", fm("Modelos", "modelos", "modelos"))
    write_if_missing(CONTENT / "guias" / "_index.md", fm("Guías", "guias", "guias"))

    # Guías genéricas (stubs)
    write_if_missing(CONTENT / "guias" / "seguridad.md", fm("Seguridad", "seguridad", "guia", 'guideKey: "seguridad"'))
    write_if_missing(CONTENT / "guias" / "mantenimiento.md", fm("Mantenimiento", "mantenimiento", "guia", 'guideKey: "mantenimiento"'))
    write_if_missing(CONTENT / "guias" / "compra.md", fm("Cómo elegir recambio", "compra", "guia", 'guideKey: "compra"'))

    # Marcas + modelos
    for brand_key, brand in brands.items():
        brand_name = (brand or {}).get("name") or brand_key
        brand_slug = slugify(brand_key)

        # página de marca
        write_if_missing(
            CONTENT / "marcas" / brand_slug / "_index.md",
            fm(f"{brand_name}", brand_slug, "marca", f'brandKey: "{brand_key}"')
        )

        # modelos de esa marca
        for m in (brand or {}).get("models", []) or []:
            model = m.get("model", "").strip()
            model_slug = (m.get("slug") or "").strip()
            if not model_slug:
                model_slug = slugify(f"{brand_key}-{model}")
            title = f"{brand_name} {model}".strip()

            write_if_missing(
                CONTENT / "modelos" / model_slug / "_index.md",
                fm(title, model_slug, "modelo", f'brandKey: "{brand_key}"\nmodelSlug: "{model_slug}"')
            )

    print("OK: stubs creados (solo si faltaban).")

if __name__ == "__main__":
    main()
