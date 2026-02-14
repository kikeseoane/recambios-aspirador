from __future__ import annotations

from pathlib import Path
import yaml

ROOT = Path(__file__).resolve().parents[1]
CATALOG = ROOT / "data" / "aspiradores.yaml"
OFFERS = ROOT / "data" / "ofertas.yaml"

DEFAULT_URL = "https://s.click.aliexpress.com/e/_c3VfQRLt"

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


def is_empty(value: object | None) -> bool:
    return value is None or str(value).strip() == ""


def is_placeholder(value: object | None, placeholder: str) -> bool:
    if is_empty(value):
        return True
    return str(value).strip() == placeholder


def ensure_offer_obj(existing: object | None) -> dict:
    return existing if isinstance(existing, dict) else {}


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
    un_orphaned = 0
    changed_urls_to_default = 0

    for sku in sorted(want):
        prev = offers.get(sku)
        obj = ensure_offer_obj(prev)
        before = dict(obj)  # snapshot simple

        # url: si falta/vacía/placeholder -> DEFAULT + flag needs_url
        if is_placeholder(obj.get("url"), PLACEHOLDER_URL):
            if obj.get("url") != DEFAULT_URL:
                obj["url"] = DEFAULT_URL
                changed_urls_to_default += 1
            obj["needs_url"] = True
        else:
            # si está rellena de verdad, quitamos flag si existía
            obj.pop("needs_url", None)

        # estimated_price_range: si falta/vacía/placeholder -> placeholder
        if is_placeholder(obj.get("estimated_price_range"), PLACEHOLDER_EST):
            obj["estimated_price_range"] = PLACEHOLDER_EST

        # badges: si falta o lista vacía -> placeholder list
        badges = obj.get("badges")
        if not isinstance(badges, list) or len(badges) == 0:
            obj["badges"] = [PLACEHOLDER_BADGE]

        # si antes era huérfano, lo desmarcamos
        if obj.get("orphaned") is True:
            obj.pop("orphaned", None)
            un_orphaned += 1

        if sku not in offers:
            offers[sku] = obj
            added += 1
        else:
            offers[sku] = obj
            if before != obj:
                updated += 1

    # marcar huérfanos
    for sku, obj in list(offers.items()):
        if sku not in want:
            obj = ensure_offer_obj(obj)
            if obj.get("orphaned") is not True:
                obj["orphaned"] = True
                offers[sku] = obj
                orphaned += 1

    dump_yaml(OFFERS, {"offers": offers})

    print("OK: sync_ofertas")
    print(f"  SKUs en catálogo: {len(want)}")
    print(f"  Offers total:     {len(offers)}")
    print(f"  Añadidos:         {added}")
    print(f"  Actualizados:     {updated}")
    print(f"  URLs a DEFAULT:   {changed_urls_to_default}")
    print(f"  Rehabilitados:    {un_orphaned}")
    print(f"  Marcados huérfano:{orphaned}")


if __name__ == "__main__":
    main()
