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
    """
    Escribe content en path.
    - Si force=False: solo crea si no existe.
    - Si force=True: sobreescribe.
    """
    if path.exists() and not force:
        return
    ensure_dir(path.parent)
    if not content.endswith("\n"):
        content += "\n"
    path.write_text(content, encoding="utf-8", newline="\n")


def ensure_model_branch_bundle(model_dir: Path) -> Path:
    """
    Garantiza que el MODELO sea branch bundle:
      content/modelos/<slug>/_index.md

    Reglas:
      - Si existe index.md y NO existe _index.md -> renombra index.md -> _index.md
      - Si existen ambos -> ERROR (inconsistencia crítica)
      - Devuelve la ruta al _index.md
    """
    ensure_dir(model_dir)

    leaf = model_dir / "index.md"
    branch = model_dir / "_index.md"

    if leaf.exists() and branch.exists():
        raise SystemExit(
            f"ERROR: Inconsistencia crítica en {model_dir}: existen index.md y _index.md"
        )

    if leaf.exists() and not branch.exists():
        leaf.rename(branch)

    return branch


def fm(
    *,
    title: str,
    slug: str | None = None,
    kind: str | None = None,
    extra: dict | None = None,
    generated: bool = True,
) -> str:
    """
    Front matter YAML delimitado por ---.

    REGLAS IMPORTANTES:
    - Para _index.md de secciones: NO uses slug ni type (evita duplicados).
    - Para home: NO generes content/_index.md (home viene de layouts/index.html).
    - Para leaf .md tipo guía: sí usamos type="guia" porque tienes layouts/guia/*.
    """
    data: dict = {
        "title": title,
        "draft": False,
    }

    if slug is not None:
        data["slug"] = slug

    if generated:
        data["generated"] = True

    if kind:
        data["type"] = kind  # solo cuando realmente lo necesitas

    if extra:
        data.update(extra)

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
    Mantiene cualquier contenido no generado.
    """
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


# -------------------------------
# Helpers SEO / títulos
# -------------------------------

def clean_model_name(brand_name: str, model_name: str) -> str:
    """
    Evita títulos tipo "Dyson Dyson V10" si en YAML el model ya incluye la marca.
    """
    mn = (model_name or "").strip()
    bn = (brand_name or "").strip()
    if mn and bn and mn.lower().startswith(bn.lower()):
        mn = mn[len(bn):].strip()
    return mn


def cat_title_es(cat_key: str) -> str:
    ck = (cat_key or "").strip().lower()
    m = {
        "bateria": "Batería",
        "filtro": "Filtro",
        "cepillo": "Cepillo",
        "cargador": "Cargador",
        "accesorios": "Accesorios",
    }
    return m.get(ck, cat_key.title())


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

    # Secciones (IMPORTANTE: sin slug ni type)
    write_file(
        CONTENT / "marcas" / "_index.md",
        fm(title="Marcas", slug=None, kind=None, extra=None),
        force=args.force,
    )
    write_file(
        CONTENT / "modelos" / "_index.md",
        fm(title="Modelos", slug=None, kind=None, extra=None),
        force=args.force,
    )
    write_file(
        CONTENT / "guias" / "_index.md",
        fm(title="Guías", slug=None, kind=None, extra=None),
        force=args.force,
    )

    # Guías genéricas (leaf pages) -> aquí SÍ usamos type="guia"
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
    for brand_key, brand in (brands.items() if isinstance(brands, dict) else []):
        brand = brand or {}
        brand_name = brand.get("name") or brand_key
        brand_slug = slugify(brand_key)

        # Marca (branch bundle) -> _index.md
        write_file(
            CONTENT / "marcas" / brand_slug / "_index.md",
            fm(
                title=brand_name,
                slug=None,
                kind=None,
                extra={"brandKey": brand_key},
            ),
            force=args.force,
        )

        # Modelos
        for m in (brand.get("models", []) or []):
            if not isinstance(m, dict):
                continue

            model_name_raw = (m.get("model") or "").strip()
            model_name = clean_model_name(brand_name, model_name_raw)

            # slug canónico del modelo
            model_slug = (m.get("slug") or "").strip() or slugify(f"{brand_key}-{model_name}")
            title = f"{brand_name} {model_name}".strip()

            # ✅ MODELO = BRANCH bundle (_index.md)
            model_dir = CONTENT / "modelos" / model_slug
            model_index = ensure_model_branch_bundle(model_dir)

            write_file(
                model_index,
                fm(
                    title=title,
                    slug=None,
                    kind=None,
                    extra={"brandKey": brand_key, "modelSlug": model_slug},
                ),
                force=args.force,
            )

            # Hubs por categoría (solo si hay items) -> leaf bundle index.md
            rec = (m.get("recambios") or {})
            if isinstance(rec, dict):
                for cat_key, items in rec.items():
                    if not items or not isinstance(items, list):
                        continue

                    cat_slug = slugify(cat_key)
                    hub_dir = CONTENT / "modelos" / model_slug / cat_slug
                    hub_title = f"{brand_name} {model_name} · {cat_title_es(cat_key)}"

                    write_file(
                        hub_dir / "index.md",
                        fm(
                            title=hub_title,
                            slug=None,
                            kind=None,
                            extra={
                                "brandKey": brand_key,
                                "modelSlug": model_slug,
                                "catKey": cat_slug,
                                "layout": "recambio",
                            },
                        ),
                        force=args.force,
                    )

            # Problemas (solo si existen)
            problems = (m.get("problemas") or [])
            if isinstance(problems, list) and len(problems) > 0:
                # HUB /modelos/<model>/problemas/ (branch bundle) -> _index.md
                write_file(
                    CONTENT / "modelos" / model_slug / "problemas" / "_index.md",
                    fm(
                        title=f"Problemas frecuentes de {title}",
                        slug=None,
                        kind=None,
                        extra={
                            "brandKey": brand_key,
                            "modelSlug": model_slug,
                            "layout": "problemas",
                        },
                    ),
                    force=args.force,
                )

                # Problemas individuales (leaf bundles) -> index.md
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
                            extra={
                                "brandKey": brand_key,
                                "modelSlug": model_slug,
                                "problemKey": pkey,
                                "layout": "problema",
                            },
                        ),
                        force=args.force,
                    )

    print("OK: stubs generados.")


if __name__ == "__main__":
    main()
