"""Read-only commercial coverage audit; does not query or mutate AliExpress."""
import argparse
import csv
import json
from datetime import date
from pathlib import Path
from urllib.parse import urlsplit

import yaml

ROOT = Path(__file__).resolve().parents[1]


def intake_backlog():
    """Return the versioned Grow research queue without treating it as offers."""
    path = ROOT / "data" / "growth-model-backlog.yaml"
    if not path.exists():
        return []
    try:
        doc = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return [item for item in doc.get("candidates", []) if isinstance(item, dict)]
    except (OSError, ValueError, yaml.YAMLError):
        return []


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


def model_coverage_line(model, gsc, ga4):
    name = f"{model['brand']}/{model['slug']}"
    prefix = f"/marcas/{model['brand']}/{model['slug']}/"
    parts = [f"- {name}: 0/{model['references']} ofertas aceptadas; "
             f"fallback_buy_new={model['buy_new']}; needs_url={model['pending']}"]
    if gsc.get("status") == "ok":
        pages = [row for row in gsc.get("pages", [])
                 if urlsplit(row.get("page", "")).path.startswith(prefix)]
        parts.append(f"GSC paginas listadas={len(pages)}, clics={sum(float(row['clicks']) for row in pages):g}, "
                     f"impresiones={sum(float(row['impressions']) for row in pages):g}")
    if ga4.get("status") == "ok":
        pages = [row for row in ga4.get("pages", [])
                 if urlsplit(row.get("page", "")).path.startswith(prefix)]
        parts.append(f"GA4 paginas listadas={len(pages)}, sesiones={sum(float(row['sessions']) for row in pages):g}")
    return "; ".join(parts) + "."


def report(gsc_dir):
    catalog = yaml.safe_load((ROOT / "data/aspiradores.yaml").read_text(encoding="utf-8"))
    offers = json.loads((ROOT / "data/ofertas.json").read_text(encoding="utf-8"))["offers"]
    categories, empty_models, buy_new_models = {}, [], []
    model_count = references = accepted = 0
    for brand_key, brand in catalog["brands"].items():
        for model in brand.get("models", []):
            model_count += 1
            model_accepted = 0
            model_references = 0
            model_pending = 0
            model_buy_new = 0
            for cat, items in model.get("recambios", {}).items():
                count = sum(reliable(offers.get(item["sku"], {})) for item in items)
                categories[f"/marcas/{brand_key}/{model['slug']}/{cat}/"] = count
                references += len(items)
                model_references += len(items)
                model_pending += sum(bool(offers.get(item["sku"], {}).get("needs_url")) for item in items)
                model_buy_new += sum(offers.get(item["sku"], {}).get("compatibility_status") == "fallback_buy_new" for item in items)
                accepted += count
                model_accepted += count
            model_state = {"brand": brand_key, "slug": model["slug"],
                           "references": model_references, "pending": model_pending,
                           "buy_new": model_buy_new}
            if not model_accepted:
                empty_models.append(model_state)
            if model_references and model_buy_new == model_references:
                buy_new_models.append(model_state)
    backlog = intake_backlog()
    analytics = analytics_snapshot()
    gsc = (analytics or {}).get("search_console", {})
    ga4 = (analytics or {}).get("ga4", {})
    lines = [
        f"# Auditoria comercial: {date.today()}",
        "", "Datos locales: no se comprueba stock ni destino en vivo.",
        f"- Modelos: {model_count}; sin ofertas aceptadas: {len(empty_models)}.",
        f"- Referencias aceptadas: {accepted}/{references}.",
        f"- Categorias sin cobertura: {sum(n == 0 for n in categories.values())}/{len(categories)}.",
        f"- Modelos con TODOS los recambios en buy new: {len(buy_new_models)}.",
        "", "## Regla de retirada: todos los recambios en buy new", "",
        "Revisar tras sync completo e historial; retirar el modelo fuente y sus paginas "
        "generadas, con redireccion solo si existe un destino pertinente.",
        "",
        *[model_coverage_line(model, gsc, ga4) for model in buy_new_models],
        "", "## Modelos sin cobertura: candidatos para investigar", "",
        "Sin aparecer en las paginas destacadas de GSC/GA4 no significa cero trafico. "
        "needs_url puede ser un fallo temporal del sincronizador: no retirar solo por este recuento.",
        "",
        *[model_coverage_line(model, gsc, ga4) for model in empty_models],
        "", "## Backlog diario de modelos", "",
        (f"- Candidatos conservados: {len(backlog)}; "
         f"pendientes: {sum(item.get('status') == 'researching' for item in backlog)}; "
         f"listos: {sum(item.get('status') == 'ready_for_catalog' for item in backlog)}; "
         f"incorporados: {sum(item.get('status') == 'incorporated' for item in backlog)}."),
        *[f"- {item.get('brand_key', '?')}/{item.get('slug', '?')}: "
          f"{item.get('status', 'sin estado')}; siguiente paso: {item.get('next_step', 'sin registrar')}"
          for item in backlog[-12:]],
        "", "## Prioridades con trafico", "",
    ]
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
        "- Avanzar como maximo un candidato del backlog a catalogo en esta ejecucion.",
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
