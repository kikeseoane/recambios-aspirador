# SEO And Indexing Notes

Use this reference when the task affects crawlability, canonicalization, or indexable page selection.

## Existing Control Points

- `layouts/_default/baseof.html` sets the robots meta tag and includes the canonical partial.
- `layouts/partials/is_noindex.html` centralizes the noindex decision.
- `layouts/_default/sitemap.xml` excludes pages marked with `robots: noindex`.
- Model templates can mark thin pages as non-indexable.

## Repo Strategy

- Not every generated page should be indexable.
- Thin pages and low-signal pages should stay out of the sitemap and out of index.
- Canonical, robots, sitemap, and internal linking should stay aligned.

## Before Shipping SEO Changes

- Confirm the intended page type should rank.
- Check whether canonical output still matches the stable URL.
- Check whether the page should appear in `sitemap.xml`.
- Check whether the template accidentally creates duplicate or thin variants.
