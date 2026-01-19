from __future__ import annotations
from datetime import datetime, timezone

import re
from typing import Iterable, List, Set
from urllib.parse import urljoin, urlparse

from bs4 import BeautifulSoup

from .base import HtmlSource, SourceConfig
from ..extractors import fetch_article, http_fetch, normalize_url


def _extract_eastrussia_article_links(section_url: str, html: str) -> List[str]:
    soup = BeautifulSoup(html, "lxml")

    base = "https://www.eastrussia.ru"
    allowed_prefixes = (
        base + "/news/",
        base + "/economics/",
        base + "/business/",
        base + "/peoples/",
        base + "/material/",
    )

    seen: Set[str] = set()
    out: List[str] = []

    for a in soup.select("a[href]"):
        href = a.get("href") or ""
        if not href or href.startswith("javascript:"):
            continue

        url = urljoin(section_url, href)
        url = normalize_url(url)
        if not url.startswith(base):
            continue

        p = urlparse(url)
        if p.query or p.fragment:
            continue

        # оставляем только /section/<slug>/ (ровно 2 сегмента)
        parts = [x for x in p.path.split("/") if x]
        if len(parts) != 2:
            continue

        if parts[1] in {"rss", "archive"}:
            continue

        if not url.startswith(allowed_prefixes):
            continue

        # ВАЖНО: сохраняем порядок как на странице
        if url not in seen:
            seen.add(url)
            out.append(url)

    return out



def _clean_eastrussia_body(text: str, title: str | None = None) -> str:
    """Remove navigation/related blocks that often leak into extracted text."""

    t = (text or "").strip()
    if not t:
        return ""

    # The generic extractor already collapses whitespace, so we operate on a mostly single-line string.
    t = re.sub(r"\s+", " ", t).strip()

    # Drop repeated UI words.
    t = re.sub(r"\bПоделиться\b", "", t)
    # Remove breadcrumb/menu tokens only if they appear at the very beginning.
    t = re.sub(
        r"^(?:\s*(?:Новости|Экономика|Бизнес|Люди|Культура|Туризм)\b\s*)+",
        "",
        t,
        flags=re.IGNORECASE,
    )

    if title:
        # Sometimes the title is duplicated at the beginning.
        t = re.sub(rf"^(?:{re.escape(title)}\s+)+", "", t).strip()

    # Cut off "tails" appended after the main article text.
    # Strong markers that almost always denote the end of the article body.
    strong_tail_re = re.compile(r"\s+(Теги:|Новости по теме:)\b", re.IGNORECASE)
    m_strong = strong_tail_re.search(t)
    if m_strong and m_strong.start() > 80:
        t = t[: m_strong.start()].rstrip()
    else:
        # Weaker markers can appear in headers/menus, so require a later position.
        tail_re = re.compile(
            r"\s+(Картина дня|Вся лента|Больше материалов|Читать полностью)\b",
            re.IGNORECASE,
        )
        cut_pos = None
        for m in tail_re.finditer(t):
            if m.start() > 200:
                cut_pos = m.start()
                break
        if cut_pos is not None:
            t = t[:cut_pos].rstrip()

    # Final whitespace cleanup after removals.
    t = re.sub(r"\s{2,}", " ", t).strip()
    return t


class EastRussiaSource(HtmlSource):
    _DT_RE = re.compile(r"\b(\d{2}\.\d{2}\.\d{4})\s+(\d{2}:\d{2})\b")

    def fetch_index(self, limit_links: int = 20) -> List[str]:
        section_urls = [
            "https://www.eastrussia.ru/news/",
            "https://www.eastrussia.ru/economics/",
            "https://www.eastrussia.ru/business/",
            "https://www.eastrussia.ru/peoples/",
        ]

        seen: Set[str] = set()
        out: List[str] = []

        for sec in section_urls:
            r = http_fetch(sec)

            if isinstance(r, str):
                html = r
            else:
                status = getattr(r, "status_code", 200)
                if status != 200:
                    continue
                html = getattr(r, "text", "") or ""

            links = _extract_eastrussia_article_links(sec, html)

            # ВАЖНО: не режем "по 5 на секцию".
            for u in links:
                if u in seen:
                    continue
                seen.add(u)
                out.append(u)
                if len(out) >= limit_links:
                    return out

        return out


    def fetch_items(self, limit_per_html_source: int = 500):
        """Fetch and clean EastRussia articles.

        EastRussia pages embed large navigation blocks ("Картина дня", "Новости по теме",
        sidebars) that the generic extractor sometimes merges into the article body.
        """

        links = self.fetch_index(limit_links=limit_per_html_source)
        items = []

        for url in links:
            try:
                art = fetch_article(url)
            except Exception:
                continue

            title = (art.get("title") or "").strip()
            body = (art.get("body") or "").strip()
            body = _clean_eastrussia_body(body, title=title)

            published_at = art.get("published_at")
            if not published_at:
                published_at = datetime.now(timezone.utc).isoformat()

            items.append(
                {
                    "url": art.get("url") or url,
                    "url_canon": art.get("url_canon") or url,
                    "title": title,
                    "body": body,
                    "published_at": published_at,
                }
            )

        return items


PARSER = EastRussiaSource(
    SourceConfig(
        name="EastRussia",
        url="https://www.eastrussia.ru/news/",
        kind="html",
    )
)
