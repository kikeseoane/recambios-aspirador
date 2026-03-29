---
name: hugo-affiliate-layouts
description: Use this skill when changing Hugo templates that render model pages, recambio pages, problema pages, offer buttons, buy-new affiliate blocks, or SEO and indexation controls in this repository.
---

# Hugo Affiliate Layouts

Use this skill for work in `layouts/`, especially model pages, offer partials, and SEO-sensitive template output.

## Key Files

- `layouts/modelos/list.html`
- `layouts/modelos/recambio.html`
- `layouts/modelos/problema.html`
- `layouts/partials/offer_btn.html`
- `layouts/partials/buy_new.html`
- `layouts/_default/baseof.html`
- `layouts/_default/sitemap.xml`
- `layouts/partials/head/canonical.html`
- `layouts/partials/is_noindex.html`

## Working Rules

- Follow the data flow from `site.Data.aspiradores` and `site.Data.ofertas` before editing markup.
- Prefer fixing labels, fallback selection, and rendering conditions in templates instead of hardcoding content in generated pages.
- Keep CTA copy short and commercially direct.
- If an offer has image and price data, render them consistently for recambios and buy-new blocks.
- Review canonical, robots, noindex, and sitemap behavior together when touching indexable page templates.
- Treat thin-content protection as part of the SEO strategy, not as a cosmetic toggle.

## Validation Focus

- Check the exact template that serves the target URL type.
- Verify fallbacks when `url` is missing.
- Watch for thin-content or `noindex` side effects on model pages.
- If URL shape or internal linking changes, verify canonical and sitemap consequences.
