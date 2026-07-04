# tools/clean_orphans.py
# -*- coding: utf-8 -*-
"""
Cleans orphaned SKUs from ofertas.json that have been inactive for N days.
Moves them to ofertas_archive.json for safety.

Usage:
    python tools/clean_orphans.py --dry-run          # Preview what would be removed
    python tools/clean_orphans.py --days 30          # Remove orphans older than 30 days
    python tools/clean_orphans.py --days 30 --purge  # Remove without archiving
"""
from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timedelta
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
OFFERS = ROOT / "data" / "ofertas.json"
ARCHIVE = ROOT / "data" / "ofertas_archive.json"

DEFAULT_URL = "https://s.click.aliexpress.com/e/_c3VfQRLt"


def load_json(path: Path) -> dict:
    if not path.exists():
        return {}
    return json.loads(path.read_text(encoding="utf-8"))


def save_json(path: Path, data: dict) -> None:
    path.write_text(json.dumps(data, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    ap = argparse.ArgumentParser(description="Clean orphaned SKUs from ofertas.json")
    ap.add_argument("--days", type=int, default=30,
                    help="Remove orphans flagged for more than N days (default: 30)")
    ap.add_argument("--dry-run", action="store_true",
                    help="Only show what would be removed, don't modify files")
    ap.add_argument("--purge", action="store_true",
                    help="Delete orphans permanently (don't archive)")
    ap.add_argument("--also-default-url", action="store_true",
                    help="Also clean non-orphan entries that only have the default fallback URL and needs_url=true")
    args = ap.parse_args()

    if not OFFERS.exists():
        print(f"ERROR: {OFFERS} not found")
        sys.exit(1)

    offers_doc = load_json(OFFERS)
    offers = offers_doc.get("offers", {})
    today = datetime.now().date()
    cutoff = (today - timedelta(days=args.days)).isoformat()

    # Identify orphans to remove
    to_remove = []
    for sku, offer in offers.items():
        if not isinstance(offer, dict):
            continue
        if offer.get("orphaned") is True:
            updated = str(offer.get("updated_at") or "0000-00-00")
            if updated <= cutoff:
                to_remove.append(sku)
        elif args.also_default_url:
            # Also clean entries with only default URL that have been stale
            url = str(offer.get("url") or "")
            if (url == DEFAULT_URL or url == "") and offer.get("needs_url") is True:
                updated = str(offer.get("updated_at") or "0000-00-00")
                if updated <= cutoff:
                    to_remove.append(sku)

    if not to_remove:
        print(f"No orphans older than {args.days} days found. Nothing to clean.")
        return

    # Calculate size savings
    removed_json = json.dumps({sku: offers[sku] for sku in to_remove}, ensure_ascii=False)
    size_mb = len(removed_json.encode("utf-8")) / (1024 * 1024)

    print(f"Found {len(to_remove)} entries to remove (older than {cutoff})")
    print(f"  Estimated size reduction: {size_mb:.2f} MB")
    print(f"  Orphaned: {sum(1 for s in to_remove if isinstance(offers[s], dict) and offers[s].get('orphaned'))}")
    if args.also_default_url:
        non_orphan = sum(1 for s in to_remove if isinstance(offers[s], dict) and not offers[s].get('orphaned'))
        print(f"  Default-URL stale: {non_orphan}")

    if args.dry_run:
        print("\n[DRY RUN] No changes made. Sample of SKUs that would be removed:")
        for sku in sorted(to_remove)[:20]:
            offer = offers[sku]
            updated = offer.get("updated_at", "?") if isinstance(offer, dict) else "?"
            print(f"    {sku} (updated: {updated})")
        if len(to_remove) > 20:
            print(f"    ... and {len(to_remove) - 20} more")
        return

    # Archive removed entries (unless --purge)
    if not args.purge:
        archive = load_json(ARCHIVE)
        if "archived" not in archive:
            archive["archived"] = {}
            archive["archive_meta"] = {
                "description": "Archived orphan SKUs removed from ofertas.json",
                "created_at": today.isoformat(),
            }
        for sku in to_remove:
            archive["archived"][sku] = offers[sku]
            if isinstance(archive["archived"][sku], dict):
                archive["archived"][sku]["archived_at"] = today.isoformat()
        save_json(ARCHIVE, archive)
        print(f"  Archived {len(to_remove)} entries to {ARCHIVE.name}")

    # Remove from main offers
    for sku in to_remove:
        del offers[sku]

    offers_doc["offers"] = offers
    save_json(OFFERS, offers_doc)

    # Report final state
    remaining = len(offers)
    remaining_orphaned = sum(1 for o in offers.values() if isinstance(o, dict) and o.get("orphaned"))
    print(f"\n  ofertas.json updated:")
    print(f"    Removed: {len(to_remove)} entries")
    print(f"    Remaining: {remaining} entries ({remaining_orphaned} still orphaned)")
    print(f"    New file size: {OFFERS.stat().st_size / (1024*1024):.2f} MB")


if __name__ == "__main__":
    main()
