"""Retire catalog models only when every SKU is marked fallback_buy_new."""
import argparse
import json
import re
import shutil
from pathlib import Path

import yaml

ROOT = Path(__file__).resolve().parents[1]


def candidates(root=ROOT):
    catalog = yaml.safe_load((root / "data/aspiradores.yaml").read_text(encoding="utf-8"))
    offers = json.loads((root / "data/ofertas.json").read_text(encoding="utf-8"))["offers"]
    found = []
    for brand, data in catalog["brands"].items():
        for model in data.get("models", []):
            skus = [item["sku"] for items in model.get("recambios", {}).values() for item in items]
            if skus and all(offers.get(sku, {}).get("compatibility_status") == "fallback_buy_new"
                            for sku in skus):
                found.append((brand, model["slug"], len(skus)))
    return found


def without_models(text, slugs):
    lines = text.splitlines(keepends=True)
    starts = [i for i, line in enumerate(lines) if line.startswith("  - model:")]
    if not starts:
        raise ValueError("No model blocks in source YAML")
    result = lines[:starts[0]]
    removed = set()
    for start, end in zip(starts, starts[1:] + [len(lines)]):
        block = "".join(lines[start:end])
        match = re.search(r'(?m)^    slug:\s*["\']?([a-z0-9-]+)', block)
        if not match:
            raise ValueError("Model block without slug")
        if match.group(1) in slugs:
            removed.add(match.group(1))
        else:
            result.extend(lines[start:end])
    if removed != slugs:
        raise ValueError(f"Missing source models: {sorted(slugs - removed)}")
    updated = "".join(result).rstrip() + "\n"
    parsed = yaml.safe_load(updated)
    if parsed.get("models") is None:
        raise ValueError("Retirement would leave brand without models")
    return updated


def generated_directory(root, brand, slug):
    content_root = (root / "content/marcas").resolve()
    path = (content_root / brand / slug).resolve()
    if not path.is_relative_to(content_root) or not path.is_dir():
        raise ValueError(f"Missing or unsafe model directory: {brand}/{slug}")
    files = list(path.rglob("*"))
    files = [item for item in files if item.is_file()]
    if not files or any(item.suffix != ".md" or "generated: true" not in
                        item.read_text(encoding="utf-8", errors="ignore").lower()
                        for item in files):
        raise ValueError(f"Manual or unknown content in {brand}/{slug}")
    return path


def updated_redirects(text, selected):
    # Keep old /modelos/ aliases from pointing at deleted pages.
    updated = []
    for line in text.splitlines():
        fields = line.split()
        if len(fields) >= 3:
            for brand, slug, _ in selected:
                if fields[1].startswith(f"/marcas/{brand}/{slug}/"):
                    fields[1] = f"/marcas/{brand}/"
                    line = " ".join(fields)
                    break
        updated.append(line)
    direct = ["# Modelos retirados: todos sus recambios son buy new"]
    for brand, slug, _ in selected:
        direct.extend((f"/marcas/{brand}/{slug}/ /marcas/{brand}/ 301",
                       f"/marcas/{brand}/{slug}/* /marcas/{brand}/ 301"))
    return "\n".join(direct + updated) + "\n"


def retire(root, selected):
    by_brand = {}
    directories = []
    for brand, slug, _ in selected:
        by_brand.setdefault(brand, set()).add(slug)
        directories.append(generated_directory(root, brand, slug))
    sources = {}
    for brand, slugs in by_brand.items():
        path = root / "data/brands/aspiradores" / f"{brand}.yaml"
        sources[path] = without_models(path.read_text(encoding="utf-8"), slugs)
    redirects = root / "static/_redirects"
    redirect_text = updated_redirects(redirects.read_text(encoding="utf-8"), selected)
    for path, text in sources.items():
        path.write_text(text, encoding="utf-8")
    redirects.write_text(redirect_text, encoding="utf-8")
    for path in directories:
        shutil.rmtree(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--model", help="Retire one brand/slug (for bounded Grow runs)")
    args = parser.parse_args()
    selected = candidates()
    if args.model:
        selected = [row for row in selected if f"{row[0]}/{row[1]}" == args.model]
        if not selected:
            raise SystemExit(f"Not an all-buy-new candidate: {args.model}")
    for brand, slug, count in selected:
        print(f"{brand}/{slug}: {count} of {count} buy new")
    if args.apply and selected:
        retire(ROOT, selected)
        print("Retired source models and generated pages. Run build_catalog.py and generar.py --force.")


if __name__ == "__main__":
    main()
