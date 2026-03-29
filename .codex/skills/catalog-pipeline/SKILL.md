---
name: catalog-pipeline
description: Use this skill when changing catalog source YAML, SKU generation, generated content rules, or deciding whether build_catalog and generar must be run in this repository.
---

# Catalog Pipeline

Use this skill for changes around `data/catalog_*.yaml`, `tools/build_catalog.py`, `tools/generar.py`, generated stubs, or SKU-shape invariants.

## Working Rules

- Treat `data/catalog_parts.yaml`, `data/catalog_brands.yaml`, and `data/catalog_skus.yaml` as source of truth.
- Treat `data/aspiradores.yaml` and generated content under `content/` as derived output.
- Prefer changing generator logic or catalog source data instead of editing generated files by hand.
- Preserve slug stability and existing SKU identity unless the user explicitly asks for a breaking catalog change.

## Pipeline Decisions

- Run `python tools/build_catalog.py` when source catalog YAML or catalog compilation logic changes.
- Run `python tools/generar.py --force` when content stubs, front matter shape, or generated section structure may change.
- Skip both steps for pure layout-only edits unless verification requires them.
- Do not run `sync_ofertas.py` from this skill unless the user explicitly asks for it.

## Checks

- Confirm the intended change was made at the source layer, not only in derived files.
- Watch for accidental edits to generated stubs and keep commits focused.
