from __future__ import annotations

from pathlib import Path
from typing import Any
import yaml

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "data" / "aspiradores.yaml"
OFFERS = ROOT / "data" / "ofertas.yaml"

PLACEHOLDER_URL = "RELLENAR_URL_ALIEXPRESS"
PLACEHOLDER_EST = "RELLENAR_COSTE_ESTIMADO"
PLACEHOLDER_BADGE = "RELLENAR_BADGES"


def load_yaml(path: Path) -> dict[str, Any]:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def dump_yaml(path: Path, data: dict[str, Any]) -> None:
    text = (
        yaml.safe_dump(
            data,
            sort_keys=False,
            allow_unicode=True,
            default_flow_style=False,
            width=120,
        ).strip()
        + "\n"
    )
    path.write_text(text, encoding="utf-8")


def collect_skus(db: dict[str, Any]) -> set[str]:
    skus: set[str] = set()
    brands = db.get("brands") or {}
    if not isinstance(brands, dict):
        return skus

    for _bk, brand in brands.items():
        if not isinstance(brand, dict):
            continue
        models = brand.get("models") or []
        if not isinstance(models, list):
            continue

        for m in models:
            if not isinstance(m, dict):
                continue
            rec = m.get("recambios") or {}
            if not isinstance(rec, dict):
                continue

            for _cat, items in rec.items():
                if not isinstance(items, list):
                    continue
                for it in items:
                    if not isinstance(it, dict):
                        continue
                    sku = it.get("sku")
                    if sku:
                        skus.add(str(sku).strip())
    return skus


def is_blank(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return v.strip() == ""
    return False


def is_placeholder(v: Any, placeholder: str) -> bool:
    if is_blank(v):
        return True
    if isinstance(v, str):
        return v.strip() == placeholder
    return False


def ensure_offer_obj(existing: Any) -> dict[str, Any]:
    return existing if isinstance(existing, dict) else {}


def normalize_badges(badges: Any) -> list[str]:
    # Si no hay lista válida, pone placeholder
    if not isinstance(badges, list):
        return [PLACEHOLDER_BADGE]

    cleaned: list[str] = []
    for b in badges:
        if not isinstance(b, str):
            continue
        s = b.strip()
        if not s:
            continue
        cleaned.append(s)

    # Si hay badges reales además del placeholder, quitamos el placeholder
    if len(cleaned) == 0:
        return [PLACEHOLDER_BADGE]

    real = [b for b in cleaned if b != PLACEHOLDER_BADGE]
    if len(real) > 0:
        return real

    return [PLACEHOLDER_BADGE]


def main() -> None:
    catalog = load_yaml(CATALOG)
    want = collect_skus(catalog)

    offers_doc = load_yaml(OFFERS)
    offers_raw = offers_doc.get("offers")
    offers: dict[str, Any] = offers_raw if isinstance(offers_raw, dict) else {}

    added = 0
    updated = 0
    orphaned = 0
    un_orphaned = 0

    # 1) Añadir / completar estructura sin pisar valores ya rellenos
    for sku in sorted(want):
        before = offers.get(sku)
        obj = ensure_offer_obj(before)

        changed = False

        # url
        if is_placeholder(obj.get("url"), PLACEHOLDER_URL):
            if obj.get("url") != PLACEHOLDER_URL:
                obj["url"] = PLACEHOLDER_URL
                changed = True
            else:
                # ya estaba como placeholder, no cuenta como cambio real
                obj["url"] = PLACEHOLDER_URL

        # estimated price range
        if is_placeholder(obj.get("estimated_price_range"), PLACEHOLDER_EST):
            if obj.get("estimated_price_range") != PLACEHOLDER_EST:
                obj["estimated_price_range"] = PLACEHOLDER_EST
                changed = True
            else:
                obj["estimated_price_range"] = PLACEHOLDER_EST

        # badges
        new_badges = normalize_badges(obj.get("badges"))
        if obj.get("badges") != new_badges:
            obj["badges"] = new_badges
            changed = True

        # si antes era huérfano, lo desmarcamos
        if obj.get("orphaned") is True:
            obj.pop("orphaned", None)
            changed = True
            un_orphaned += 1

        if sku not in offers:
            offers[sku] = obj
            added += 1
        else:
            offers[sku] = obj
            if changed:
                updated += 1

    # 2) Marcar huérfanos (no borrar)
    for sku, obj_any in list(offers.items()):
        if sku not in want:
            obj = ensure_offer_obj(obj_any)
            if obj.get("orphaned") is not True:
                obj["orphaned"] = True
                offers[sku] = obj
                orphaned += 1

    # 3) Guardar ordenado por SKU para diffs limpios
    offers_sorted = {k: offers[k] for k in sorted(offers.keys())}
    out = {"offers": offers_sorted}
    dump_yaml(OFFERS, out)

    print("OK: sync_ofertas")
    print(f"  SKUs en catálogo:  {len(want)}")
    print(f"  Offers total:      {len(offers_sorted)}")
    print(f"  Añadidos:          {added}")
    print(f"  Actualizados:      {updated}")
    print(f"  Rehabilitados:     {un_orphaned}")
    print(f"  Marcados huérfano: {orphaned}")


if __name__ == "__main__":
    main()
