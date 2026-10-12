"""Check rendered sitemap/robots, coverage and affiliate CTAs after Hugo build."""
import json
import re
import xml.etree.ElementTree as ET
from html.parser import HTMLParser
from pathlib import Path
from urllib.parse import urlsplit

import yaml
from growth_audit import ROOT, reliable


class Page(HTMLParser):
    def __init__(self):
        super().__init__()
        self.robots = ""
        self.canonical = ""
        self.links = []

    def handle_starttag(self, tag, attrs):
        attrs = dict(attrs)
        if tag == "meta" and attrs.get("name") == "robots":
            self.robots = attrs.get("content", "")
        if tag == "link" and attrs.get("rel") == "canonical":
            self.canonical = attrs.get("href", "")
        if tag == "a":
            self.links.append(attrs)


def main():
    public = ROOT / "public"
    sitemap = ET.parse(public / "sitemap.xml")
    urls = {node.text for node in sitemap.findall(".//{*}loc")}
    offers = json.loads((ROOT / "data/ofertas.json").read_text(encoding="utf-8"))["offers"]
    catalog = yaml.safe_load((ROOT / "data/aspiradores.yaml").read_text(encoding="utf-8"))
    empty = set()
    for brand, data in catalog["brands"].items():
        for model in data["models"]:
            for cat, items in model.get("recambios", {}).items():
                if not any(reliable(offers.get(item["sku"], {})) for item in items):
                    empty.add(f"/marcas/{brand}/{model['slug']}/{cat}/")
    errors = []
    checked = clicks = 0
    for file in public.rglob("index.html"):
        path = "/" + file.parent.relative_to(public).as_posix().strip(".") + "/"
        path = re.sub("/+", "/", path)
        page = Page()
        page.feed(file.read_text(encoding="utf-8"))
        if not page.canonical:
            continue  # Legacy redirect files are not content pages.
        checked += 1
        if page.canonical in urls and "noindex" in page.robots.lower():
            errors.append(f"Noindex in sitemap: {path}")
        if urlsplit(page.canonical).path != path:
            errors.append(f"Canonical mismatch: {path}")
        if path in empty and ("noindex" not in page.robots.lower() or page.canonical in urls):
            errors.append(f"Empty category indexable: {path}")
        for link in page.links:
            if link.get("data-affiliate-placement") == "recambio":
                clicks += 1
                sku = link.get("data-affiliate-sku", "")
                if not reliable(offers.get(sku, {})):
                    errors.append(f"Unreliable CTA: {path} {sku}")
                if link.get("href") != offers.get(sku, {}).get("url"):
                    errors.append(f"Wrong CTA destination: {path} {sku}")
                if "sponsored" not in link.get("rel", ""):
                    errors.append(f"Missing sponsored: {path}")
    for url in urls:
        file = public / urlsplit(url).path.strip("/") / "index.html"
        if not file.exists():
            errors.append(f"Sitemap URL missing: {url}")
    if not checked or not clicks:
        errors.append("No rendered content or tracked CTAs found")
    if errors:
        raise SystemExit("\n".join(errors[:40]))
    print(f"OK: {checked} pages; {len(urls)} sitemap URLs; {len(empty)} empty categories excluded; {clicks} accepted tracked CTAs.")


if __name__ == "__main__":
    main()
