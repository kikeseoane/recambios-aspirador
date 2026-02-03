import csv
import os
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]  # repo root
CONTENT = ROOT / "content"
DATA = ROOT / "data" / "matriz.csv"

# Ajusta esto si tu site usa otra base final (debe coincidir con config.toml)
SITE = "https://recambios-aspirador.com"

PART_LABELS = {
    "bateria": "Batería",
    "filtro": "Filtro",
    "cepillo": "Cepillo/rodillo",
    "cargador": "Cargador",
}

def norm_model(model: str) -> str:
    model = (model or "").strip().lower()
    if model.startswith("dyson-"):
        return model.replace("dyson-", "")
    return model

def front_matter(title: str, description: str, slug: str, canonical: str) -> str:
    # Hugo TOML front matter
    return f"""+++
title = "{title}"
description = "{description}"
slug = "{slug.strip("/")}"
canonical = "{canonical}"
robots = "index,follow"
+++

"""

def model_page(model: str) -> str:
    m = model.upper()
    # enlaces internos relativos (evitas líos de canonical)
    return f"""## Qué encontrarás aquí

En esta guía de **Dyson {m}** tienes una selección de recambios compatibles y soluciones a problemas habituales.

### Recambios más buscados para Dyson {m}
- [Batería compatible](/recambios/bateria/dyson-{model}/)
- [Filtro compatible](/recambios/filtro/dyson-{model}/)
- [Cepillo/rodillo compatible](/recambios/cepillo/dyson-{model}/)
- [Cargador compatible](/recambios/cargador/dyson-{model}/)

### Problemas comunes en Dyson {m}
- [No carga](/problemas/dyson-{model}-no-carga/)
- [Pierde succión](/problemas/dyson-{model}-pierde-succion/)
- [Se apaga](/problemas/dyson-{model}-se-apaga/)
- [Huele mal](/problemas/dyson-{model}-huele-mal/)
- [Hace ruido](/problemas/dyson-{model}-hace-ruido/)
"""

def recambio_page(model: str, part: str) -> str:
    m = model.upper()
    part_name = PART_LABELS.get(part, part)
    return f"""## Cómo elegir {part_name.lower()} compatible para Dyson {m}

Checklist rápido:
- Compatibilidad exacta con **Dyson {m}**
- Calidad de materiales y encaje
- Política de devoluciones
- Opiniones recientes (importante en compatibles)

### Opciones recomendadas
{{{{< aff title="Ver opciones de {part_name} para Dyson {m}" url="https://example.com" >}}}}

### Relacionado
- [Guía de recambios Dyson {m}](/modelos/dyson-{model}/)
- Otros recambios: [batería](/recambios/bateria/dyson-{model}/), [filtro](/recambios/filtro/dyson-{model}/), [cepillo](/recambios/cepillo/dyson-{model}/), [cargador](/recambios/cargador/dyson-{model}/)

### Problemas donde suele ayudar
- [No carga](/problemas/dyson-{model}-no-carga/)
- [Se apaga](/problemas/dyson-{model}-se-apaga/)
"""

def problema_page(model: str, problem: str) -> str:
    m = model.upper()
    # map básico problem -> recambio sugerido
    suggest = {
        "no-carga": ("cargador", "Cargador"),
        "se-apaga": ("bateria", "Batería"),
        "pierde-succion": ("filtro", "Filtro"),
        "huele-mal": ("filtro", "Filtro"),
        "hace-ruido": ("cepillo", "Cepillo/rodillo"),
    }.get(problem, ("filtro", "Filtro"))
    part, part_name = suggest
    return f"""## Diagnóstico rápido (3 pasos)

1) Limpia y revisa obstrucciones (tubo/cepillo/depósito).  
2) Revisa filtros y estado general.  
3) Si persiste, revisa el recambio relacionado (batería/cargador/cepillo).

### Solución habitual
Muchas veces se resuelve con **{part_name.lower()}** adecuado:

{{{{< aff title="Ver {part_name} compatible para Dyson {m}" url="https://example.com" >}}}}

### Enlaces útiles
- [Guía de recambios Dyson {m}](/modelos/dyson-{model}/)
- [Batería](/recambios/bateria/dyson-{model}/) · [Filtro](/recambios/filtro/dyson-{model}/) · [Cepillo](/recambios/cepillo/dyson-{model}/) · [Cargador](/recambios/cargador/dyson-{model}/)
"""

def write_md(path: Path, content: str) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(content, encoding="utf-8")

def main():
    if not DATA.exists():
        raise SystemExit(f"No existe {DATA}")

    created = 0

    with DATA.open(newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        for row in reader:
            t = (row.get("type") or "").strip().lower()
            model = norm_model(row.get("model") or "")
            part = (row.get("part") or "").strip().lower()
            problem = (row.get("problem") or "").strip().lower()

            slug = (row.get("slug") or "").strip()
            title = (row.get("title") or "").strip().replace('"', "'")
            description = (row.get("description") or "").strip().replace('"', "'")

            if not slug.startswith("/"):
                raise SystemExit(f"Slug debe empezar por '/': {slug}")

            canonical = SITE.rstrip("/") + slug

            # Calcula ruta de salida
            if t == "modelo":
                out = CONTENT / "modelos" / f"dyson-{model}" / "index.md"
                body = model_page(model)
                fm = front_matter(title, description, f"modelos/dyson-{model}", canonical)

            elif t == "recambio":
                out = CONTENT / "recambios" / part / f"dyson-{model}" / "index.md"
                body = recambio_page(model, part)
                fm = front_matter(title, description, f"recambios/{part}/dyson-{model}", canonical)

            elif t == "problema":
                out = CONTENT / "problemas" / f"dyson-{model}-{problem}" / "index.md"
                body = problema_page(model, problem)
                fm = front_matter(title, description, f"problemas/dyson-{model}-{problem}", canonical)

            else:
                raise SystemExit(f"type inválido: {t} (usa modelo|recambio|problema)")

            if out.exists():
                # No machacamos si ya existe (seguro)
                continue

            write_md(out, fm + body)
            created += 1

    print(f"OK: creados {created} ficheros nuevos")

if __name__ == "__main__":
    main()
