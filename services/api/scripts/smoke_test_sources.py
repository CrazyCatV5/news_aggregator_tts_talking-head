#!/usr/bin/env python3
"""Quick smoke test for source parsers.

Usage (from repo root):
  python -m services.api.scripts.smoke_test_sources --source "TASS RSS v2" --limit 5

This script performs live HTTP requests.
"""

from __future__ import annotations

import argparse
import sys

from services.api.app.sources import list_source_names, get_parser


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--source", default="TASS RSS v2", help="Exact source name from sources.json")
    ap.add_argument("--limit", type=int, default=5, help="Limit for HTML sources (RSS ignores)")
    args = ap.parse_args()

    if args.source not in list_source_names():
        print("Unknown source. Available:")
        for n in list_source_names():
            print(" -", n)
        return 2

    parser = get_parser(args.source)
    print(f"Source: {parser.config.name} ({parser.config.kind})")
    print(f"Listing URL: {parser.config.url}")

    items = parser.fetch_items(limit_per_html_source=args.limit)
    print(f"Fetched items: {len(items)}")

    for i, it in enumerate(items[: min(3, len(items))], 1):
        body = (it.get("body") or "").strip()
        title = (it.get("title") or "").strip()
        url = it.get("url") or ""
        print("\n---")
        print(f"#{i}: {title}")
        print(url)
        print(f"body_len={len(body)}")
        print(body[:500].replace("\n", " "))

        # basic quality checks
        if len(body) < 120:
            print("[WARN] body is very short; likely paywall or extraction failure")
        for bad in ["Новости партнеров", "LiveInternet", "Все материалы"]:
            if bad in body:
                print(f"[WARN] noise marker still present: {bad}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
