# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Hugo static site for vacuum cleaner replacement parts (recambios-aspirador.com). Content is generated from YAML catalogs via a Python toolchain. The site uses AliExpress affiliate links for product offers.

## Build Pipeline

The pipeline must be run in this exact order:

```bash
# 1. Compile aspiradores.yaml from catalog source files
python tools/build_catalog.py

# 2. Generate/update Hugo content stubs from aspiradores.yaml
python tools/generar.py --force

# 3. Sync AliExpress affiliate links into ofertas.yaml
python tools/sync_ofertas.py --no-cache --force
```

**`generar.py` flags:**
- `--force`: Overwrite existing generated stubs (files with `generated: true` in front matter)
- `--clean-all`: Remove all generated stubs before regenerating
- `--clean-modelos`: Legacy brute-force clean of `content/modelos/`

**`sync_ofertas.py` flags:**
- `--no-cache`: Skip local cache, make fresh AliExpress API calls
- `--force`: Re-lookup URLs even if already filled
- `--only-sku <sku>`: Process a single SKU

**`sync_ofertas.py` requires environment variables:** `ALI_APP_KEY`, `ALI_APP_SECRET` (and optionally `ALI_TRACKING_ID`, `ALI_API_URL`, `ALI_SHIP_TO`, `ALI_CURRENCY`)

**Build Hugo site:**
```bash
hugo
hugo server  # local dev
```

**Dump project state for review (PowerShell):**
```powershell
powershell -ExecutionPolicy Bypass -File .\tools\dump_state.ps1 > kk.txt
```

## Data Architecture

### Source of Truth (hand-edited)
- `data/catalog_parts.yaml` — global config, part categories (`globals.categorias_recambios`), and `sku_packs` (templates for generating 10 SKUs per model)
- `data/catalog_brands.yaml` — brands and models, each model references a `sku_pack`
- `data/catalog_skus.yaml` — `model_overrides` and `sku_overrides` for fine-tuning individual SKUs or models

### Generated (do not hand-edit)
- `data/aspiradores.yaml` — compiled catalog (output of `build_catalog.py`)
- `data/ofertas.yaml` — AliExpress affiliate URLs keyed by SKU (output of `sync_ofertas.py`)
- `content/modelos/`, `content/marcas/`, `content/guias/` — Hugo content stubs (output of `generar.py`; stubs are marked `generated: true`)

### SKU ID Format
`{brand_key}-{model_slug}-{cat_key}-{sku_suffix}` — deterministic and stable.

### Invariant: 10 SKUs per model
Every model with a `sku_pack` must produce exactly 10 SKU items across all categories. `build_catalog.py` enforces this with a hard error.

## Content & Layout Architecture

Hugo reads `data/aspiradores.yaml` and `data/ofertas.yaml` at build time. Layouts look up brand/model data directly from `site.Data.aspiradores`.

### URL structure
- `/marcas/<brand>/` → `content/marcas/<brand>/_index.md` (branch bundle)
- `/modelos/<model-slug>/` → `content/modelos/<model-slug>/_index.md` (branch bundle, uses `layouts/modelos/list.html`)
- `/modelos/<model-slug>/<cat>/` → `content/modelos/<model-slug>/<cat>/index.md` (leaf bundle, uses `layouts/modelos/recambio.html` via `layout: recambio` in front matter)
- `/modelos/<model-slug>/problemas/<key>/` → leaf bundle, uses `layouts/modelos/problema.html`

### Key layouts
- `layouts/modelos/list.html` — handles both `/modelos/` root and `/modelos/<model>/` model pages (distinguished by presence of `brandKey` param)
- `layouts/modelos/recambio.html` — category hub page listing SKU items with affiliate buy buttons
- `layouts/partials/offer_btn.html` — renders the buy button by looking up SKU in `site.Data.ofertas`
- `layouts/partials/model_crosslinks.html` — cross-links between categories for a model

### Thin content protection
Model pages without recambios, problemas, or sufficient intro text are automatically marked `noindex` in `list.html`.

## Part Categories (catKey values)
`bateria`, `filtro`, `cepillo`, `cargador`, `soporte`, `accesorios` — defined in `catalog_parts.yaml` under `globals.categorias_recambios`. All catKeys must be from this list.

## Important Conventions
- All slugs must be kebab-case (`a-z0-9-`); `build_catalog.py` validates this.
- Model `_index.md` files must be branch bundles; category `index.md` files must be leaf bundles. `generar.py` auto-migrates `index.md → _index.md` if needed.
- Hand-edited content files (without `generated: true`) are preserved by `--clean-all` and `generar.py --force`.
- `ofertas.yaml` entries with no real URL are set to the default fallback URL and flagged with `needs_url: true`.
- Orphaned SKUs in `ofertas.yaml` (removed from catalog) are flagged `orphaned: true` rather than deleted.
