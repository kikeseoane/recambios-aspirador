---
name: linking-agent
description: Use this skill when improving internal linking in this repository, especially links between brand and model pages, model and category pages, problem and product pages, orphan detection, and anchor text suggestions.
---

# Linking Agent

Use this skill for internal linking improvements that should help both users and SEO without creating noisy or artificial links.

## Objective

Improve internal linking with meaningful paths between discovery pages, problem pages, and buying-intent pages.

## Required Linking Priorities

- brand -> model
- model -> categories
- problem -> products

## Rules

- Do not create links that feel forced or semantically weak.
- Prioritize user experience first, then crawlability.
- Prefer links that help the user continue the task they are already on.
- Use useful anchors, not generic anchors like `haz clic aquí`, `ver más`, or repeated boilerplate.
- Detect orphan pages and surface them clearly.

## Anchor Guidance

Good anchors usually include:

- model name
- specific category
- specific symptom or part
- clear intent such as `batería compatible`, `filtro para`, `solución si no carga`

Avoid anchors that are:

- too generic
- too long
- repeated mechanically across many pages
- misleading about the destination

## Workflow

1. Map the page type involved: brand, model, category, problem, or guide.
2. Check whether nearby pages that the user would logically want next are linked.
3. Detect orphan pages or weakly connected pages.
4. Suggest or add only links with clear navigational value.
5. Review anchors for specificity and readability.

## Validation Focus

- Does each model page lead naturally to its key category pages?
- Do problem pages link to the recambios that can plausibly solve the issue?
- Are brand pages helping users reach the most relevant models?
- Are there orphan pages with no meaningful inbound links?
- Are anchors descriptive without sounding spammy?

## Required Output Format

When reporting results, include:

- `Páginas huérfanas`
- `Enlaces recomendados`
- `Anchors sugeridos`

## Reference

- Read `references/linking-checklist.md` for the repo-specific review checklist and anchor heuristics.
