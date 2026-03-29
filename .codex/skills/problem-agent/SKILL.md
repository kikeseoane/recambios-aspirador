---
name: problem-agent
description: Use this skill when creating or reviewing SEO long-tail problem pages for aspiradores and related verticals in this repository, especially pages built around real symptoms, causes, checks, solutions, and recambio CTAs.
---

# Problem Agent

Use this skill for problem pages that target real user intent such as `no carga`, `pierde potencia`, `filtro obstruido`, or similar failure patterns.

## Objective

Generate useful long-tail pages based on real product problems, not filler content.

## Required Structure

Each page must include:

- symptom
- causes
- checks
- solution
- CTA to related recambios when it makes sense

## Rules

- Content must be useful, concrete, and diagnostic.
- Always connect the problem to related products when the fix plausibly involves a recambio.
- Do not force a CTA if the problem is clearly not solved by a product replacement.
- Avoid duplicates between very similar problems. Merge or differentiate them clearly.
- Do not invent faults, causes, or repair claims that are not supported by the model/category context in repo data.

## Writing Standard

- Lead with the real symptom the user experiences.
- Prefer actionable checks before replacement advice.
- Keep causes plausible and ordered by likelihood.
- Make the solution specific to the model or category when possible.
- Keep the page aligned with the available `cta_cat` and recambio categories.

## Duplicate Control

Treat as possible duplicates when:

- two problems describe the same user symptom with different wording
- one problem is only a broader or vaguer version of another
- both pages would lead to the same checks, causes, and CTA

When this happens:

- keep one page and strengthen it
- or split them only if user intent is genuinely different

## Validation Focus

- Does the problem reflect a real symptom?
- Are causes and checks coherent with the model and category?
- Is the CTA tied to a sensible recambio category?
- Would this page cannibalize another problem page on the same model?

## Reference

- Read `references/problem-patterns.md` for the repo-specific checklist before creating or deduplicating problem pages.
