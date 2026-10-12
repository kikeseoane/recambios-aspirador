"""Read-only commercial coverage audit; does not query or mutate AliExpress."""
import argparse
import csv
import json
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[1]


def reliable(offer):
    return bool(offer.get("url") and not offer.get("needs_url")
                and not offer.get("orphaned")
                and offer.get("compatibility_status") in
                ("compatible_alto", "compatible_probable"))


def report(gsc_dir):
    catalog = yaml.safe_load((ROOT / "data/aspiradores.yaml").read_text(encoding="utf-8"))
    offers = json.loads((ROOT / "data/ofertas.json").read_text(encoding="utf-8"))["offers"]
    categories, empty_models = {}, []
    model_count = references = accepted = 0
    for brand_key, brand in catalog["brands"].items():
        for model in brand.get("models", []):
            model_count += 1
            model_accepted = 0
            for cat, items in model.get("recambios", {}).items():
                count = sum(reliable(offers.get(item["sku"], {})) for item in items)
                categories[f"/marcas/{brand_key}/{model['slug']}/{cat}/"] = count
                references += len(items)
                accepted += count
                model_accepted += count
            if not model_accepted:
                empty_models.append(f"{brand_key}/{model['slug']}")
    lines = [
        f"# Auditoria comercial: {date.today()}",
        "", "Datos locales: no se comprueba stock ni destino en vivo.",
        f"- Modelos: {model_count}; sin ofertas aceptadas: {len(empty_models)}.",
        f"- Referencias aceptadas: {accepted}/{references}.",
        f"- Categorias sin cobertura: {sum(n == 0 for n in categories.values())}/{len(categories)}.",
        "", "## Modelos sin cobertura", "",
        *[f"- {name}" for name in empty_models],
        "", "## Prioridades con trafico", "",
    ]
    pages_file = gsc_dir / "Páginas.csv"
    if pages_file.exists():
        with pages_file.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        lines.append("Exportacion historica: confirmar periodo. No sumar totales de paginas y consultas.")
        for row in sorted(rows, key=lambda r: int(r.get("Clics", 0)), reverse=True)[:30]:
            path = urlsplit(next(iter(row.values()))).path
            count = categories.get(path)
            flag = f"; ofertas aceptadas={count}" if count is not None else ""
            lines.append(f"- {path}: {row.get('Clics', 0)} clics; {row.get('Impresiones', 0)} impresiones{flag}")
    else:
        lines.append("Sin exportacion de Search Console: no inferir demanda ni posiciones.")
    lines.extend([
        "", "## Decision siguiente", "",
        "- Verificar recambios en paginas con trafico y cero ofertas antes de ampliar catalogo.",
        "- Incorporar modelos solo con demanda documentada y anuncios compatibles actuales.",
        "- Contrastar affiliate_click en GA4 con pedidos y comision aprobada en AliExpress.",
        "- Comparar periodos de 28 dias y descontar costes de automatizacion del beneficio.",
    ])
    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--gsc-dir", type=Path, default=ROOT)
    parser.add_argument("--output", type=Path)
    args = parser.parse_args()
    result = report(args.gsc_dir)
    if args.output:
        args.output.parent.mkdir(parents=True, exist_ok=True)
        args.output.write_text(result, encoding="utf-8")
    else:
        print(result)
