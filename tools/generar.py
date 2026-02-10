from __future__ import annotations

import argparse
import re
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


def write_file(path: Path, content: str, force: bool = False) -> None:
    """Write content to path. If force=False, only write if missing."""
    if path.exists() and not force:
        return
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")


def fm(
    *,
    title: str,
    slug: str,
    kind: str | None = None,
    extra: dict | None = None,
    generated: bool = True,
) -> str:
    """
    Front matter YAML delimitado por ---.

    - kind: si None, NO se escribe 'type' (Hugo usará section por defecto).
    - extra: dict adicional a volcar en YAML.
    """
    data: dict = {
        "title": title,
        "slug": slug,
        "draft": False,
    }
    if generated:
        data["generated"] = True

    if kind:  # solo si lo queremos explícito
        data["type"] = kind

    if extra:
        data.update(extra)

    # YAML "bonito" y estable
    body = yaml.safe_dump(
        data,
        sort_keys=False,
        allow_unicode=True,
        default_flow_style=False,
    ).strip()

    return f"---\n{body}\n---\n"


def load_db() -> dict:
    if not DATA.exists():
        raise SystemExit(f"No existe {DATA}")
    return yaml.safe_load(DATA.read_text(encoding="utf-8")) or {}


# -------------------------------
# Limpiezas
# -------------------------------

def clean_modelos_dir() -> None:
    """
    Limpieza "bruta" (legacy):
    borra todo content/modelos excepto modelos/_index.md.
    """
    modelos = CONTENT / "modelos"
    if modelos.exists():
        for child in modelos.iterdir():
            if child.name == "_index.md":
                continue
            if child.is_dir():
                for p in sorted(child.rglob("*"), reverse=True):
                    if p.is_file():
                        p.unlink()
                    else:
                        p.rmdir()
                child.rmdir()
            elif child.is_file():
                child.unlink()


def is_generated_file(path: Path) -> bool:
    """
    Detecta si un .md fue generado por el sistema.
    Criterio: contiene 'generated: true' en el front matter.
    """
    if not path.exists() or not path.is_file():
        return False
    txt = path.read_text(encoding="utf-8", errors="ignore")
    return "generated: true" in txt.lower()


def safe_clean_section(section_dir: Path) -> None:
    """
    Limpia SOLO stubs generados (generated:true) dentro de una sección.
    Soporta:
      - leaf bundle:  content/<sec>/<slug>/index.md
      - branch bundle: content/<sec>/<slug>/_index.md
      - leaf pages sueltas .md (p.ej. guias/seguridad.md)
    Mantiene cualquier contenido no generado.
    """
    if not section_dir.exists():
        return

    for child in section_dir.iterdir():
        # mantenemos el _index.md de la sección (se puede regenerar con --force)
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
            # si no parece generado, no tocamos
            continue

        # archivos .md sueltos
        if child.is_file() and child.suffix.lower() == ".md":
            if is_generated_file(child):
                child.unlink()


# -------------------------------
# Main
# -------------------------------

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="Sobrescribe stubs generados")

    ap.add_argument(
        "--clean-modelos",
        action="store_true",
        help="(Legacy) Limpia content/modelos (excepto modelos/_index.md) antes de generar",
    )

    ap.add_argument(
        "--clean-all",
        action="store_true",
        help="Limpia stubs generados (generated:true) en content/modelos, content/marcas y content/guias",
    )

    args = ap.parse_args()

    db = load_db()
    brands = (db.get("brands", {}) or {})

    if args.clean_all:
        safe_clean_section(CONTENT / "modelos")
        safe_clean_section(CONTENT / "marcas")
        safe_clean_section(CONTENT / "guias")
        print("OK: limpieza segura (generated:true) ejecutada.")

    if args.clean_modelos:
        clean_modelos_dir()
        print("OK: limpieza modelos (bruta) ejecutada.")

    # HOME + secciones (branch bundles => _index.md)
    write_file(
        CONTENT / "_index.md",
        fm(title="Inicio", slug="", kind="home", extra=None),
        force=args.force,
    )
    write_file(
        CONTENT / "marcas" / "_index.md",
        fm(title="Marcas", slug="marcas", kind="marcas"),
        force=args.force,
    )
    write_file(
        CONTENT / "modelos" / "_index.md",
        fm(title="Modelos", slug="modelos", kind="modelos"),
        force=args.force,
    )
    write_file(
        CONTENT / "guias" / "_index.md",
        fm(title="Guías", slug="guias", kind="guias"),
        force=args.force,
    )

    # Guías genéricas (leaf pages => .md normal)
    write_file(
        CONTENT / "guias" / "seguridad.md",
        fm(
            title="Seguridad",
            slug="seguridad",
            kind="guia",
            extra={"guideKey": "seguridad"},
        ),
        force=args.force,
    )
    write_file(
        CONTENT / "guias" / "mantenimiento.md",
        fm(
            title="Mantenimiento",
            slug="mantenimiento",
            kind="guia",
            extra={"guideKey": "mantenimiento"},
        ),
        force=args.force,
    )
    write_file(
        CONTENT / "guias" / "compra.md",
        fm(
            title="Cómo elegir recambio",
            slug="compra",
            kind="guia",
            extra={"guideKey": "compra"},
        ),
        force=args.force,
    )

    # Marcas + modelos
    for brand_key, brand in brands.items():
        brand = brand or {}
        brand_name = brand.get("name") or brand_key
        brand_slug = slugify(brand_key)

        # página de marca (branch bundle)
        # CLAVE: NO forzamos type aquí. Hugo usará section = "marcas" -> layouts/marcas/single.html
        write_file(
            CONTENT / "marcas" / brand_slug / "_index.md",
            fm(
                title=brand_name,
                slug=brand_slug,
                kind=None,  # <-- NO type
                extra={"brandKey": brand_key},
            ),
            force=args.force,
        )

        # modelos de esa marca (leaf bundle: content/modelos/<slug>/index.md)
        for m in (brand.get("models", []) or []):
            model_name = (m.get("model") or "").strip()
            model_slug = (m.get("slug") or "").strip() or slugify(f"{brand_key}-{model_name}")
            title = f"{brand_name} {model_name}".strip()

            # CLAVE: NO forzamos type aquí.
            # Hugo usará section = "modelos" -> layouts/modelos/single.html
            write_file(
                CONTENT / "modelos" / model_slug / "index.md",
                fm(
                    title=title,
                    slug=model_slug,
                    kind=None,  # <-- NO type
                    extra={
                        "brandKey": brand_key,
                        "modelSlug": model_slug,
                    },
                ),
                force=args.force,
            )
            # ---- hubs por categoría (solo si hay items) ----
            rec = (m.get("recambios") or {})
            if isinstance(rec, dict):
                for cat_key, items in rec.items():
                    if not items:
                        continue
                    # crea /modelos/<model_slug>/<cat_key>/index.md
                    hub_dir = CONTENT / "modelos" / model_slug / slugify(cat_key)
                    hub_slug = f"{model_slug}/{slugify(cat_key)}"
                    hub_title = f"{brand_name} {model_name} · {cat_key.title()}"
                    write_file(
                        hub_dir / "index.md",
                        fm(
                            title=hub_title,
                            slug=hub_slug,
                            kind=None,
                            extra={
                                "brandKey": brand_key,
                                "modelSlug": model_slug,
                                "catKey": slugify(cat_key),
                                "layout": "recambio",
                            },
                        ),
                        force=args.force,
                    )

    print("OK: stubs generados.")


if __name__ == "__main__":
    main()
