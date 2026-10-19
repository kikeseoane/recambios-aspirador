"""Validate Grow's small, evidence-backed model intake backlog.

This tool never calls AliExpress and never writes offer outputs.  It checks the
evidence Grow records before a candidate may become a catalog model; the hourly
offer workflow remains responsible for resolving the actual affiliate links.
"""
from __future__ import annotations

import argparse
import subprocess
from datetime import date, datetime
from pathlib import Path
from urllib.parse import urlsplit

import yaml


ROOT = Path(__file__).resolve().parents[1]
BACKLOG = Path("data/growth-model-backlog.yaml")
BRANDS = Path("data/brands/aspiradores")
ACTIVE = {"ready_for_catalog", "incorporated"}
VALID_STATUS = {"researching", "rejected", *ACTIVE}
MAX_LISTING_AGE_DAYS = 30


def load_yaml(path: Path) -> dict:
    if not path.exists():
        raise ValueError(f"Falta {path.as_posix()}")
    value = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
    if not isinstance(value, dict):
        raise ValueError(f"{path.as_posix()} debe ser un objeto YAML")
    return value


def text(value) -> str:
    return str(value or "").strip()


def parsed_date(value, field: str) -> date:
    if isinstance(value, datetime):
        return value.date()
    if isinstance(value, date):
        return value
    try:
        return date.fromisoformat(text(value))
    except ValueError as error:
        raise ValueError(f"{field} debe usar YYYY-MM-DD") from error


def direct_aliexpress_item(url: str) -> bool:
    parsed = urlsplit(url)
    host = parsed.hostname or ""
    return (parsed.scheme == "https" and (host == "aliexpress.com" or host.endswith(".aliexpress.com"))
            and "/item/" in parsed.path and bool(parsed.path.rsplit("/item/", 1)[1]))


def catalog_model_details(root: Path) -> dict[tuple[str, str], dict]:
    found: dict[tuple[str, str], dict] = {}
    for path in (root / BRANDS).glob("*.yaml"):
        doc = load_yaml(path)
        brand = text(doc.get("brand_key") or path.stem)
        for model in doc.get("models") or []:
            if isinstance(model, dict) and text(model.get("slug")):
                found[(brand, text(model["slug"]))] = model
    return found


def catalog_models(root: Path) -> set[tuple[str, str]]:
    return set(catalog_model_details(root))


def validate_candidate(candidate: dict, index: int, models: dict[tuple[str, str], dict], today: date,
                       require_fresh_listing: bool = False) -> list[str]:
    where = f"candidates[{index}]"
    errors: list[str] = []
    if not isinstance(candidate, dict):
        return [f"{where} debe ser un objeto"]
    status = text(candidate.get("status"))
    if status not in VALID_STATUS:
        return [f"{where}.status invalido: {status or '(vacio)'}"]
    for field in ("brand_key", "model", "slug", "next_step"):
        if not text(candidate.get(field)):
            errors.append(f"{where}.{field} es obligatorio")
    try:
        checked = parsed_date(candidate.get("last_checked"), f"{where}.last_checked")
        if checked > today:
            errors.append(f"{where}.last_checked no puede estar en el futuro")
    except ValueError as error:
        errors.append(str(error))

    if status == "rejected" and not text(candidate.get("rejection_reason")):
        errors.append(f"{where}.rejection_reason es obligatorio para un descarte")
    if status not in ACTIVE:
        return errors

    if not text(candidate.get("sku_pack")):
        errors.append(f"{where}.sku_pack es obligatorio para una incorporacion")
    tokens = candidate.get("model_tokens")
    if not isinstance(tokens, list) or not any(text(token) for token in tokens):
        errors.append(f"{where}.model_tokens debe contener el identificador de modelo")
    elif not any(any(char.isdigit() for char in text(token)) for token in tokens):
        errors.append(f"{where}.model_tokens necesita un identificador con digitos")
    anchors = [text(token).casefold() for token in tokens or []
               if any(char.isdigit() for char in text(token))]

    manufacturer = candidate.get("manufacturer")
    if not isinstance(manufacturer, dict) or not text(manufacturer.get("url")) or not text(manufacturer.get("reference")):
        errors.append(f"{where}.manufacturer necesita url y referencia de variante")
    demand = candidate.get("demand")
    if not isinstance(demand, dict) or not all(text(demand.get(key)) for key in ("source", "url", "evidence")):
        errors.append(f"{where}.demand necesita fuente, url y evidencia")
    else:
        try:
            parsed_date(demand.get("observed_on"), f"{where}.demand.observed_on")
        except ValueError as error:
            errors.append(str(error))

    parts = candidate.get("aliexpress_parts")
    if not isinstance(parts, list) or len(parts) < 3:
        errors.append(f"{where}.aliexpress_parts necesita al menos tres recambios")
    else:
        categories = set()
        for part_index, part in enumerate(parts):
            part_where = f"{where}.aliexpress_parts[{part_index}]"
            if not isinstance(part, dict):
                errors.append(f"{part_where} debe ser un objeto")
                continue
            category = text(part.get("category"))
            if not category:
                errors.append(f"{part_where}.category es obligatorio")
            elif category in categories:
                errors.append(f"{where} repite la categoria {category}")
            categories.add(category)
            url = text(part.get("url"))
            if not direct_aliexpress_item(url):
                errors.append(f"{part_where}.url debe ser un anuncio directo https://*.aliexpress.com/item/...")
            for field in ("listing_title", "compatibility_evidence"):
                if not text(part.get(field)):
                    errors.append(f"{part_where}.{field} es obligatorio")
            stated = f"{text(part.get('listing_title'))} {text(part.get('compatibility_evidence'))}".casefold()
            if anchors and not any(anchor in stated for anchor in anchors):
                errors.append(f"{part_where} debe citar un identificador de modelo en el anuncio o la evidencia")
            try:
                verified = parsed_date(part.get("verified_on"), f"{part_where}.verified_on")
                if verified > today:
                    errors.append(f"{part_where}.verified_on no puede estar en el futuro")
                elif (status == "ready_for_catalog" or require_fresh_listing) and (today - verified).days > MAX_LISTING_AGE_DAYS:
                    errors.append(f"{part_where}.verified_on supera los {MAX_LISTING_AGE_DAYS} dias; verifica el anuncio otra vez")
            except ValueError as error:
                errors.append(str(error))
        if len(categories) < 3:
            errors.append(f"{where}.aliexpress_parts debe cubrir tres categorias distintas")

    identity = (text(candidate.get("brand_key")), text(candidate.get("slug")))
    if status == "incorporated":
        source = models.get(identity)
        if source is None:
            errors.append(f"{where} figura incorporado pero falta {identity[0]}/{identity[1]} en data/brands/aspiradores")
        else:
            if text(source.get("model")) != text(candidate.get("model")):
                errors.append(f"{where}.model no coincide con la ficha fuente del catalogo")
            if text(source.get("sku_pack")) != text(candidate.get("sku_pack")):
                errors.append(f"{where}.sku_pack no coincide con la ficha fuente del catalogo")
    return errors


def validate(root: Path = ROOT, today: date | None = None,
             fresh_incorporated: set[tuple[str, str]] | None = None) -> list[str]:
    today = today or date.today()
    doc = load_yaml(root / BACKLOG)
    if doc.get("version") != 1:
        return ["data/growth-model-backlog.yaml debe declarar version: 1"]
    candidates = doc.get("candidates")
    if not isinstance(candidates, list):
        return ["data/growth-model-backlog.yaml.candidates debe ser una lista"]
    models = catalog_model_details(root)
    fresh_incorporated = fresh_incorporated or set()
    errors: list[str] = []
    identities: set[tuple[str, str]] = set()
    for index, candidate in enumerate(candidates):
        identity = (text(candidate.get("brand_key")), text(candidate.get("slug"))) if isinstance(candidate, dict) else ("", "")
        errors.extend(validate_candidate(candidate, index, models, today, identity in fresh_incorporated))
        if isinstance(candidate, dict):
            identity = (text(candidate.get("brand_key")), text(candidate.get("slug")))
            if identity in identities:
                errors.append(f"Candidato repetido: {identity[0]}/{identity[1]}")
            identities.add(identity)
    return errors


def git_file_at(ref: str, path: Path, cwd: Path) -> str | None:
    result = subprocess.run(["git", "show", f"{ref}:{path.as_posix()}"], cwd=cwd,
                            text=True, encoding="utf-8", capture_output=True)
    return result.stdout if result.returncode == 0 else None


def models_at_head(root: Path, ref: str) -> set[tuple[str, str]]:
    result = subprocess.run(["git", "ls-tree", "-r", "--name-only", ref, BRANDS.as_posix()], cwd=root,
                            text=True, encoding="utf-8", capture_output=True, check=True)
    models: set[tuple[str, str]] = set()
    for relative in result.stdout.splitlines():
        source = git_file_at(ref, Path(relative), root)
        if source is None:
            continue
        doc = yaml.safe_load(source) or {}
        brand = text(doc.get("brand_key") or Path(relative).stem)
        for model in doc.get("models") or []:
            if isinstance(model, dict) and text(model.get("slug")):
                models.add((brand, text(model["slug"])))
    return models


def newly_incorporated(root: Path, base: str) -> list[tuple[str, str]]:
    return sorted(catalog_models(root) - models_at_head(root, base))


def validate_daily_limit(root: Path = ROOT, base: str = "HEAD") -> list[str]:
    added = newly_incorporated(root, base)
    if len(added) > 1:
        names = ", ".join(f"{brand}/{slug}" for brand, slug in added)
        return [f"Grow solo puede incorporar un modelo por ejecucion diaria; se agregaron {names}"]
    if not added:
        return []
    doc = load_yaml(root / BACKLOG)
    incorporated = {
        (text(item.get("brand_key")), text(item.get("slug")))
        for item in doc.get("candidates", []) if isinstance(item, dict) and item.get("status") == "incorporated"
    }
    brand, slug = added[0]
    if (brand, slug) not in incorporated:
        return [f"El nuevo modelo {brand}/{slug} necesita una ficha incorporada en data/growth-model-backlog.yaml"]
    return []


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(description="Valida el backlog diario de modelos de Grow")
    parser.add_argument("--root", type=Path, default=ROOT)
    parser.add_argument("--base", help="Commit base para limitar nuevas incorporaciones")
    args = parser.parse_args(argv)
    fresh_incorporated = set(newly_incorporated(args.root, args.base)) if args.base else set()
    errors = validate(args.root, fresh_incorporated=fresh_incorporated)
    if args.base:
        errors.extend(validate_daily_limit(args.root, args.base))
    if errors:
        raise SystemExit("\n".join(f"ERROR: {error}" for error in errors))
    print("OK: backlog de modelos Grow validado")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
