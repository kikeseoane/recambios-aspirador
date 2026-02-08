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


def clean_modelos_dir() -> None:
    modelos = CONTENT / "modelos"
    if modelos.exists():
        # Borramos solo el contenido interno (manteniendo carpeta base)
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


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--force", action="store_true", help="Sobrescribe stubs generados")
    ap.add_argument(
        "--clean-modelos",
        action="store_true",
        help="Limpia content/modelos (excepto modelos/_index.md) antes de generar",
    )
    args = ap.parse_args()

    db = load_db()
    brands = (db.get("brands", {}) or {})

    if args.clean_modelos:
        clean_modelos_dir()
        print("OK: limpieza ejecutada.")

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
        write_file(
            CONTENT / "marcas" / brand_slug / "_index.md",
            fm(
                title=brand_name,
                slug=brand_slug,
                kind="marca",
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

    print("OK: stubs generados.")


if __name__ == "__main__":
    main()
