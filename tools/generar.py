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


# ---------- Utils ----------
def slugify(s: str) -> str:
    s = (s or "").strip().lower()
    s = re.sub(r"[^\w\s-]", "", s, flags=re.UNICODE)
    s = re.sub(r"[\s_-]+", "-", s)
    return s.strip("-")


def ensure_dir(p: Path) -> None:
    p.mkdir(parents=True, exist_ok=True)


def write_file(path: Path, content: str, force: bool = False) -> None:
    """
    Escribe archivo si no existe, o si force=True.
    """
    if path.exists() and not force:
        return
    ensure_dir(path.parent)
    path.write_text(content, encoding="utf-8")


def is_generated_stub(path: Path) -> bool:
    """
    True si el archivo contiene 'generated: true' en el front matter.
    """
    if not path.exists():
        return False
    try:
        txt = path.read_text(encoding="utf-8", errors="ignore")
    except Exception:
        return False
    return "generated: true" in txt


def fm(title: str, slug: str, kind: str, extra_lines: list[str] | None = None) -> str:
    """
    Front matter SIEMPRE bien cerrado.
    """
    extra_lines = extra_lines or []
    extra = "\n".join([ln.rstrip() for ln in extra_lines if ln.strip()])
    if extra:
        extra = "\n" + extra + "\n"
    return (
        "---\n"
        f'title: "{title}"\n'
        f'slug: "{slug}"\n'
        f'type: "{kind}"\n'
        "draft: false\n"
        "generated: true\n"
        f"{extra}"
        "---\n"
    )


def read_yaml() -> dict:
    if not DATA.exists():
        raise SystemExit(f"No existe {DATA}")
    return yaml.safe_load(DATA.read_text(encoding="utf-8")) or {}


# ---------- Cleaning ----------
def clean_model_dirs(valid_slugs: set[str], dry_run: bool) -> None:
    """
    Borra directorios content/modelos/<slug>/ que NO estén en valid_slugs,
    solo si dentro hay un index.md generado.
    """
    modelos_dir = CONTENT / "modelos"
    if not modelos_dir.exists():
        return

    for d in modelos_dir.iterdir():
        if not d.is_dir():
            continue
        slug = d.name
        if slug in valid_slugs:
            continue

        idx = d / "index.md"
        old = d / "_index.md"

        # solo borrar si es generado (index o _index)
        if is_generated_stub(idx) or is_generated_stub(old):
            if dry_run:
                print(f"[DRY-RUN] Borraría: {d}")
            else:
                print(f"Borrando: {d}")
                shutil.rmtree(d, ignore_errors=True)
        else:
            print(f"[SKIP] No borro (no parece generado): {d}")


# ---------- Main ----------
def main() -> None:
    ap = argparse.ArgumentParser(description="Genera stubs Hugo desde data/aspiradores.yaml")
    ap.add_argument("--force", action="store_true", help="Sobrescribe stubs generados (útil si se rompió el front matter).")
    ap.add_argument("--clean-modelos", action="store_true", help="Elimina carpetas de modelos generadas que ya no existan en YAML.")
    ap.add_argument("--dry-run", action="store_true", help="Con --clean-modelos: simula sin borrar.")
    args = ap.parse_args()

    db = read_yaml()
    brands = db.get("brands") or {}
    if not isinstance(brands, dict):
        raise SystemExit("ERROR: brands debe ser un mapa/dict en YAML")

    # ---- Secciones (branch bundles) ----
    # Sobrescribimos solo si --force y el archivo era generado
    def write_section(path: Path, content: str) -> None:
        if path.exists() and not args.force:
            return
        if path.exists() and args.force and not is_generated_stub(path):
            # No machacamos páginas manuales
            return
        write_file(path, content, force=True)

    write_section(CONTENT / "_index.md", fm("Inicio", "", "home"))
    write_section(CONTENT / "guias" / "_index.md", fm("Guías", "guias", "guias"))
    write_section(CONTENT / "marcas" / "_index.md", fm("Marcas", "marcas", "marcas"))
    write_section(CONTENT / "modelos" / "_index.md", fm("Modelos", "modelos", "modelos"))

    # ---- Guías (páginas sueltas) ----
    # Estas son stubs: si están rotas y son generated, con --force se arreglan.
    def write_stub(path: Path, content: str) -> None:
        if path.exists() and not args.force:
            return
        if path.exists() and args.force and not is_generated_stub(path):
            return
        write_file(path, content, force=True)

    write_stub(CONTENT / "guias" / "seguridad.md", fm("Seguridad", "seguridad", "guia", ['guideKey: "seguridad"']))
    write_stub(CONTENT / "guias" / "mantenimiento.md", fm("Mantenimiento", "mantenimiento", "guia", ['guideKey: "mantenimiento"']))
    write_stub(CONTENT / "guias" / "compra.md", fm("Cómo elegir recambio", "compra", "guia", ['guideKey: "compra"']))

    valid_model_slugs: set[str] = set()

    # ---- Marcas + modelos ----
    for brand_key, brand in brands.items():
        if not isinstance(brand, dict):
            continue

        brand_name = (brand.get("name") or brand_key).strip()
        brand_slug = slugify(brand_key)

        # Marca: branch bundle
        write_stub(
            CONTENT / "marcas" / brand_slug / "_index.md",
            fm(brand_name, brand_slug, "marca", [f'brandKey: "{brand_key}"'])
        )

        models = brand.get("models") or []
        if not isinstance(models, list):
            continue

        for m in models:
            if not isinstance(m, dict):
                continue

            model_name = (m.get("model") or "").strip()
            model_slug = (m.get("slug") or "").strip()
            if not model_slug:
                model_slug = slugify(f"{brand_key}-{model_name}")

            model_slug = slugify(model_slug)
            valid_model_slugs.add(model_slug)

            title = f"{brand_name} {model_name}".strip()

            # Modelo: leaf bundle => index.md (clave para single)
            write_stub(
                CONTENT / "modelos" / model_slug / "index.md",
                fm(title, model_slug, "modelo", [
                    f'brandKey: "{brand_key}"',
                    f'modelSlug: "{model_slug}"'
                ])
            )

    # ---- Limpieza opcional ----
    if args.clean_modelos:
        clean_model_dirs(valid_model_slugs, dry_run=args.dry_run)

    print("OK: stubs generados.")
    if args.clean_modelos:
        print("OK: limpieza ejecutada." if not args.dry_run else "OK: limpieza simulada.")


if __name__ == "__main__":
    main()
