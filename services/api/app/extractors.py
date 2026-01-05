import httpx
import feedparser
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from typing import Dict, List, Optional, Any
from .config import settings
from .utils import canonicalize_url, normalize_whitespace

def _client():
    return httpx.Client(
        headers={"User-Agent": settings.user_agent},
        timeout=settings.request_timeout,
        follow_redirects=True,
    )

def fetch_rss(url: str) -> List[Dict[str, Any]]:
    with _client() as c:
        r = c.get(url)
        r.raise_for_status()
        data = r.content
    feed = feedparser.parse(data)
    items = []
    for e in feed.entries:
        link = getattr(e, "link", None) or ""
        title = getattr(e, "title", "") or ""
        summary = getattr(e, "summary", "") or ""
        published = getattr(e, "published", None) or getattr(e, "updated", None) or None
        published_at = None
        if published:
            try:
                published_at = dtparser.parse(published).astimezone().isoformat()
            except Exception:
                published_at = None
        items.append({
            "url": link,
            "url_canon": canonicalize_url(link),
            "title": normalize_whitespace(title),
            "body": normalize_whitespace(summary),
            "published_at": published_at,
        })
    return items

def _text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()

    candidates = []
    for sel in ["article", "main", "div[itemprop='articleBody']", "div.article__text", "div[itemprop='text']"]:
        for node in soup.select(sel):
            txt = normalize_whitespace(node.get_text(" ", strip=True))
            if len(txt) > 250:
                candidates.append(txt)
    if candidates:
        candidates.sort(key=len, reverse=True)
        return candidates[0]

    return normalize_whitespace(soup.get_text(" ", strip=True))

def fetch_html_index(url: str, limit_links: int = 30) -> List[str]:
    with _client() as c:
        r = c.get(url)
        r.raise_for_status()
        html = r.text
    soup = BeautifulSoup(html, "lxml")
    links = []
    for a in soup.select("a[href]"):
        href = a.get("href")
        if not href:
            continue
        if href.startswith("//"):
            href = "https:" + href
        elif href.startswith("/"):
            from urllib.parse import urljoin
            href = urljoin(url, href)
        if href.startswith("http"):
            links.append(href)

    seen = set()
    out = []
    for u in links:
        cu = canonicalize_url(u)
        if cu in seen:
            continue
        seen.add(cu)
        if any(x in cu for x in ["/video", "/photo", "/tag/", "/tags/", "/auth", "/login", "/subscribe", "/special", "/project", "#"]):
            continue
        out.append(cu)
        if len(out) >= limit_links:
            break
    return out

def fetch_article(url: str) -> Dict[str, Optional[str]]:
    with _client() as c:
        r = c.get(url)
        r.raise_for_status()
        html = r.text
    soup = BeautifulSoup(html, "lxml")

    title = ""
    for sel in ["meta[property='og:title']", "meta[name='title']", "h1", "title"]:
        node = soup.select_one(sel)
        if not node:
            continue
        if sel.startswith("meta"):
            title = node.get("content") or ""
        else:
            title = node.get_text(strip=True) or ""
        if title:
            break

    published_at = None
    for sel in ["meta[property='article:published_time']", "time[datetime]", "meta[itemprop='datePublished']"]:
        node = soup.select_one(sel)
        if not node:
            continue
        val = node.get("content") if sel.startswith("meta") else node.get("datetime")
        if val:
            try:
                published_at = dtparser.parse(val).astimezone().isoformat()
            except Exception:
                published_at = None
            break

    body = _text_from_html(html)
    return {"title": normalize_whitespace(title), "body": normalize_whitespace(body), "published_at": published_at}
