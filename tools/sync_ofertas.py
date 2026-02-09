from __future__ import annotations

from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "data" / "aspiradores.yaml"
OFFERS = ROOT / "data" / "ofertas.yaml"

PLACEHOLDER_URL = "RELLENAR_URL_ALIEXPRESS"
PLACEHOLDER_EST = "RELLENAR_COSTE_ESTIMADO"
PLACEHOLDER_BADGE = "RELLENAR_BADGES"


def load_yaml(path: Path) -> dict:
    if not path.exists():
        return {}
    return yaml.safe_load(path.read_text(encoding="utf-8")) or {}


def dump_yaml(path: Path, data: dict) -> None:
    text = yaml.safe_dump(data, sort_keys=False, allow_unicode=True).strip() + "\n"
    path.write_text(text, encoding="utf-8")


def collect_skus(db: dict) -> set[str]:
    skus: set[str] = set()
    brands = db.get("brands") or {}
    if not isinstance(brands, dict):
        return skus

    for _bk, brand in brands.items():
        brand = brand or {}
        for m in (brand.get("models") or []):
            m = m or {}
            rec = m.get("recambios") or {}
            if not isinstance(rec, dict):
                continue
            for _cat, items in rec.items():
                if not items:
                    continue
                for it in items:
                    it = it or {}
                    sku = it.get("sku")
                    if sku:
                        skus.add(str(sku).strip())
    return skus


def is_placeholder(value: str | None, placeholder: str) -> bool:
    if value is None:
        return True
    v = str(value).strip()
    return v == "" or v == placeholder


def ensure_offer_obj(existing: object | None) -> dict:
    if isinstance(existing, dict):
        return existing
    return {}


def main() -> None:
    catalog = load_yaml(CATALOG)
    want = collect_skus(catalog)

    offers_doc = load_yaml(OFFERS)
    offers = offers_doc.get("offers")
    if not isinstance(offers, dict):
        offers = {}

    added = 0
    updated = 0
    orphaned = 0

    # 1) Añadir / completar estructura (sin pisar valores ya rellenos)
    for sku in sorted(want):
        obj = ensure_offer_obj(offers.get(sku))

        # url
        if is_placeholder(obj.get("url"), PLACEHOLDER_URL):
            obj["url"] = PLACEHOLDER_URL

        # estimated price (opcional; si no lo quieres, puedes borrar estas 2 líneas)
        if is_placeholder(obj.get("estimated_price_range"), PLACEHOLDER_EST):
            obj["estimated_price_range"] = PLACEHOLDER_EST

        # badges
        badges = obj.get("badges")
        if not isinstance(badges, list) or len(badges) == 0:
            obj["badges"] = [PLACEHOLDER_BADGE]
        else:
            # si tiene exactamente el placeholder, lo dejamos
            pass

        # housekeeping
        if obj.get("orphaned") is True:
            obj.pop("orphaned", None)

        if sku not in offers:
            offers[sku] = obj
            added += 1
        else:
            offers[sku] = obj
            updated += 1

    # 2) Marcar huérfanos (no borrar)
    for sku, obj in list(offers.items()):
        if sku not in want:
            obj = ensure_offer_obj(obj)
            if obj.get("orphaned") is not True:
                obj["orphaned"] = True
                offers[sku] = obj
                orphaned += 1

    offers_doc["offers"] = offers
    dump_yaml(OFFERS, offers_doc)

    print(f"OK sync_ofertas.py -> nuevos={added} actualizados={updated} orphaned_marcados={orphaned} total={len(offers)}")
    print(f"Archivo: {OFFERS}")


if __name__ == "__main__":
    main()
