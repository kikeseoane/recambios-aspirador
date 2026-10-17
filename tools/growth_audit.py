"""Read-only commercial coverage audit; does not query or mutate AliExpress."""
import argparse
import csv
import json
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[1]


def analytics_snapshot():
    path = ROOT / ".git" / "growth-analytics.json"
    if not path.exists():
        return None
    try:
        return json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError):
        return None


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
    analytics = analytics_snapshot()
    gsc = (analytics or {}).get("search_console", {})
    pages_file = gsc_dir / "Páginas.csv"
    if gsc.get("status") == "ok":
        period = analytics["period"]
        lines.append(f"Search Console API, periodo cerrado {period['start_date']} a {period['end_date']}.")
        for row in gsc.get("pages", []):
            path = urlsplit(row["page"]).path
            count = categories.get(path)
            flag = f"; ofertas aceptadas={count}" if count is not None else ""
            lines.append(f"- {path}: {row['clicks']} clics; {row['impressions']} impresiones; "
                         f"CTR {float(row['ctr']):.1%}; posicion {float(row['position']):.1f}{flag}")
        if not gsc.get("pages"):
            lines.append("La API no devolvio paginas en este periodo; no inferir demanda.")
    elif pages_file.exists():
        with pages_file.open(encoding="utf-8-sig", newline="") as stream:
            rows = list(csv.DictReader(stream))
        lines.append("Exportacion historica: confirmar periodo. No sumar totales de paginas y consultas.")
        for row in sorted(rows, key=lambda r: int(r.get("Clics", 0)), reverse=True)[:30]:
            path = urlsplit(next(iter(row.values()))).path
            count = categories.get(path)
            flag = f"; ofertas aceptadas={count}" if count is not None else ""
            lines.append(f"- {path}: {row.get('Clics', 0)} clics; {row.get('Impresiones', 0)} impresiones{flag}")
    else:
        reason = gsc.get("reason", "sin conexion configurada")
        lines.append(f"Search Console no disponible ({reason}); no inferir demanda ni posiciones.")
    ga4 = (analytics or {}).get("ga4", {})
    lines.extend(["", "## GA4", ""])
    if ga4.get("status") == "ok":
        lines.append(f"GA4 API: eventos affiliate_click={ga4.get('affiliate_clicks', 'no disponibles')}.")
        for row in ga4.get("pages", []):
            lines.append(f"- {row['page']}: {row.get('sessions', '0')} sesiones; "
                         f"{row.get('screenPageViews', '0')} vistas.")
    else:
        lines.append(f"GA4 no disponible ({ga4.get('reason', 'sin conexion configurada')}).")
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
