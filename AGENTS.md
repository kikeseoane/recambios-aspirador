# AGENTS.md

## Scope

These rules apply to the whole repository.

## Project Model

- This repo generates a Hugo site from YAML catalogs and templates.
- Edit the source of truth first. Avoid patching generated output unless the user explicitly asks for it.
- Main hand-edited sources live in `data/catalog_parts.yaml`, `data/catalog_brands.yaml`, `data/catalog_skus.yaml`, `layouts/`, `tools/`, and manually maintained content files.
- Generated artifacts include `data/aspiradores.yaml`, `data/ofertas.yaml`, and generated stubs under `content/`.

## Build And Generation Rules

- Canonical pipeline order is:
  1. `python tools/build_catalog.py`
  2. `python tools/generar.py --force`
  3. `python tools/sync_ofertas.py --no-cache --force`
- Do not run `sync_ofertas.py` unless the user explicitly asks for it. The user often runs it externally to avoid long local executions.
- Before commit, run `build_catalog` and `generar` only when the change affects catalogs, generated content shape, or data consumed by generated stubs.
- For template-only or script-only changes, do not run heavy generation steps unless needed to verify the change.

## AliExpress And Offer Rules

- `data/ofertas.yaml` may be newer on remote because a GitHub Action updates it.
- If `git push` is rejected because remote is ahead, use `git pull --rebase --autostash origin main` and then push again.
- Do not commit accidental local churn in `data/ofertas.yaml` or `data/vertical_defaults.yaml` unless the user wants those updates included.
- Prefer exact or strongly anchored matches over broad AliExpress searches. Avoid relaxed fallbacks that can map unrelated products to the same SKU.
- For "buy new" fallbacks, prefer a complete product for the vertical rather than a generic spare part search.

## Commit And Push Rules

- Cloudflare deploys from `main`, so when the user asks for shipping work, finish with commit and push.
- Keep commits focused. Do not mix user-local files or generated noise into functional commits.
- Never commit `.claude/settings.local.json`.

## Working Style

- When changing affiliate logic, inspect both `tools/sync_ofertas.py` and the Hugo partials that render offer CTAs.
- When changing catalog behavior, preserve the invariant that each model produces the expected SKU set.
- When a GitHub Action or autostash causes conflicts in generated YAML, prefer keeping the remote/generated version unless the user explicitly wants the local generated result.

## SEO And Indexing Rules

- Preserve the existing SEO control points in `layouts/_default/baseof.html`, `layouts/_default/sitemap.xml`, and the head partials.
- Before changing indexing behavior, inspect canonical, robots, and noindex logic together. Do not treat them as independent tweaks.
- Thin pages, low-signal generated pages, and pages explicitly marked with `robots: noindex` must stay out of the sitemap and out of indexable templates unless the user wants the strategy changed.
- Prefer fixing indexation at the template, front matter, or generation-rule level instead of patching one page at a time.
- When a change affects URL shape, page discoverability, or internal linking, review sitemap, canonical output, and noindex side effects.

## Repo-Local Skills

- Use `.codex/skills/catalog-pipeline` for catalog source, generation, and SKU-shape changes.
- Use `.codex/skills/aliexpress-offers` for AliExpress matching, offer fallbacks, and `buy new` behavior.
- Use `.codex/skills/hugo-affiliate-layouts` for Hugo templates that render model, recambio, problema, CTA blocks, and SEO/indexation controls such as canonical, robots, noindex, and sitemap logic.
- Use `.codex/skills/problem-agent` for SEO problem pages built around real symptoms, causes, checks, solutions, and related recambio CTAs.
- Use `.codex/skills/linking-agent` for internal linking, orphan detection, and useful anchor selection across brand, model, category, and problem pages.
