---
name: aliexpress-offers
description: Use this skill when adjusting AliExpress link selection, fallback queries, vertical defaults, ofertas.yaml behavior, or the buy-new recommendation logic in this repository.
---

# AliExpress Offers

Use this skill for `tools/sync_ofertas.py`, `data/ofertas.yaml`, `data/vertical_defaults.yaml`, and matching logic that feeds Hugo offer buttons.

## Priorities

- Product relevance beats commission and sales volume.
- Exact model matches beat relaxed matches.
- Strongly anchored part matches beat generic brand-plus-category matches.
- For `nuevo`, prefer a full product from the same vertical when the exact model is not available.

## Guardrails

- Avoid relaxed fallbacks that can attach the same generic listing to many unrelated SKUs.
- Require anchor terms for weak or broad categories such as accessories, supports, baskets, bags, seals, and similar buckets.
- Use negative terms aggressively when a category is prone to cross-category contamination.
- Treat `vertical_defaults.yaml` as the fallback layer for "buy new" vertical recommendations.

## Execution Rules

- Do not run `sync_ofertas.py` unless the user explicitly asks for it.
- If the user runs only one SKU externally, remember that the script may still refresh `vertical_defaults.yaml`.
- Do not commit `ofertas.yaml` or `vertical_defaults.yaml` unless the user wants those generated updates included.

## Review Focus

- Check bad-match risk first.
- Then check fallback quality.
- Then check rendering fields needed by templates: URL, image, price, title, label.
