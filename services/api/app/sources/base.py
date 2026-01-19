from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List

from concurrent.futures import ThreadPoolExecutor, as_completed

from ..config import settings
from ..extractors import fetch_rss, fetch_html_index, fetch_article
from ..utils import canonicalize_url

@dataclass(frozen=True)
class SourceConfig:
    name: str
    kind: str  # "rss" | "html"
    url: str

class SourceParser:
    config: SourceConfig

    def __init__(self, config: SourceConfig):
        self.config = config


class RssSource(SourceParser):
    def fetch_items(self, limit_per_html_source: int = 500) -> List[Dict[str, Any]]:
        """Fetch RSS entries and *enrich* them by downloading the article pages.

        We keep RSS as the listing source (stable link + basic metadata), but we prefer
        full-text extracted from the article page when available.
        """
        # limit_per_html_source is ignored for RSS sources
        items = fetch_rss(self.config.url)
        if not items:
            return []

        # Enrich concurrently, but keep within reasonable bounds
        workers = int(getattr(settings, "max_workers", 16))
        workers = max(1, workers)
        workers = min(workers, 64)

        out: List[Dict[str, Any]] = []

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(fetch_article, it.get("url") or ""): it for it in items if (it.get("url") or "")}
            for fut in as_completed(futs):
                it = futs[fut]
                link = it.get("url") or ""
                try:
                    art = fut.result()
                except Exception:
                    art = {}

                # Merge strategy: keep RSS title/date if page parsing fails, but replace body with full text if longer.
                rss_title = it.get("title") or ""
                rss_body = it.get("body") or ""
                rss_pub = it.get("published_at")

                page_title = (art.get("title") or "").strip()
                page_body = (art.get("body") or "").strip()
                page_pub = art.get("published_at")

                title = page_title if len(page_title) >= 5 else rss_title
                published_at = page_pub or rss_pub

                body = rss_body
                if len(page_body) >= max(120, len(rss_body) + 40):
                    body = page_body

                out.append({
                    "url": link,
                    "url_canon": it.get("url_canon") or canonicalize_url(link),
                    "title": title,
                    "body": body,
                    "published_at": published_at,
                    # Pass-through optional taxonomy from RSS (ignored by ingest if not needed)
                    "tags": it.get("tags"),
                    "section": it.get("section"),
                })

        # Keep stable ordering similar to the RSS feed where possible (fallback: by published_at desc).
        # Since futures complete out of order, we can re-sort by published_at when available.
        def _sort_key(x: Dict[str, Any]):
            return (x.get("published_at") or "", x.get("url") or "")
        out.sort(key=_sort_key, reverse=True)
        return out

class HtmlSource(SourceParser):
    def fetch_index(self, limit_links: int = 20) -> List[str]:
        # Default behavior (simple HTML index parse)
        return fetch_html_index(self.config.url, limit_links=limit_links)

    def fetch_items(self, limit_per_html_source: int = 500) -> List[Dict[str, Any]]:
        links = self.fetch_index(limit_links=limit_per_html_source)
        out: List[Dict[str, Any]] = []
        if not links:
            return out

        workers = max(1, int(getattr(settings, "article_concurrency", 16)))
        workers = min(workers, 64)

        with ThreadPoolExecutor(max_workers=workers) as ex:
            futs = {ex.submit(fetch_article, link): link for link in links}
            for fut in as_completed(futs):
                link = futs[fut]
                art = fut.result()
                out.append({
                    "url": link,
                    "url_canon": canonicalize_url(link),
                    "title": art.get("title") or "",
                    "body": art.get("body") or "",
                    "published_at": art.get("published_at"),
                })
        return out

