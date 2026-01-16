import httpx
from bs4 import BeautifulSoup
from dateutil import parser as dtparser
from typing import Dict, List, Optional, Any
from .config import settings
from .utils import canonicalize_url, normalize_whitespace

# NOTE: Do not depend on the optional third-party `feedparser` package.
# Some deployments run without it (ModuleNotFoundError). We therefore parse
# RSS/Atom with the standard library XML parser.
import xml.etree.ElementTree as ET
import urllib.parse as up

def _client():
    return httpx.Client(
        headers={"User-Agent": settings.user_agent},
        timeout=settings.request_timeout,
        follow_redirects=True,
    )

def _rss_url_variants(url: str) -> List[str]:
    """Generate conservative URL variants for RSS endpoints.

    Some publishers intermittently refuse connections on certain schemes/hosts
    (e.g., HTTPS vs HTTP, with/without 'www'). We try a small, deterministic set
    of alternatives before failing.

    IMPORTANT: build URLs using urllib.parse to avoid httpx.URL query-bytes issues.
    """
    url = (url or "").strip()
    if not url:
        return []

    parts = up.urlsplit(url)
    scheme = (parts.scheme or "https").lower()
    netloc = parts.netloc or ""
    path = parts.path or ""
    query = parts.query or ""
    # fragments are never needed for HTTP requests
    fragment = ""

    # If the input accidentally omitted scheme, urlsplit may place it into path.
    # In that case, just return the original.
    if not netloc and parts.scheme and not parts.netloc:
        return [url]

    def with_netloc(nl: str, sch: str | None = None, pth: str | None = None) -> str:
        return up.urlunsplit((
            (sch or scheme),
            nl,
            (pth if pth is not None else path),
            query,
            fragment,
        ))

    def ensure_trailing_slash(pth: str) -> str:
        if not pth:
            return "/"
        return pth if pth.endswith("/") else pth + "/"

    variants: List[str] = [url]

    # Parse host/port from netloc for safe www-flip
    host = netloc
    port = ""
    if "@" in host:
        # strip userinfo if any (rare)
        host = host.split("@", 1)[1]
    if ":" in host:
        host, port = host.rsplit(":", 1)
        if port:
            port = ":" + port

    host_l = host.lower()

    def nl(h: str) -> str:
        return h + port

    # Scheme flip
    if scheme == "https":
        variants.append(with_netloc(netloc, sch="http"))
    elif scheme == "http":
        variants.append(with_netloc(netloc, sch="https"))

    # www flip
    if host_l.startswith("www."):
        h2 = host_l[4:]
        variants.append(with_netloc(nl(h2)))
        variants.append(with_netloc(nl(h2), sch="http"))
    else:
        h2 = "www." + host_l if host_l else host_l
        if h2:
            variants.append(with_netloc(nl(h2)))
            variants.append(with_netloc(nl(h2), sch="http"))

    # Trailing slash variants (path-only)
    variants2: List[str] = []
    for v in variants:
        pv = up.urlsplit(v)
        variants2.append(v)
        p2 = ensure_trailing_slash(pv.path or "")
        if p2 != (pv.path or ""):
            variants2.append(up.urlunsplit((pv.scheme, pv.netloc, p2, pv.query, "")))

    # Deduplicate preserving order
    out: List[str] = []
    seen: set[str] = set()
    for v in variants2:
        if v not in seen:
            seen.add(v)
            out.append(v)
    return out
def fetch_rss(url: str) -> List[Dict[str, Any]]:
    last_err: Optional[Exception] = None
    data: Optional[bytes] = None
    with _client() as c:
        for u in _rss_url_variants(url):
            try:
                r = c.get(u)
                r.raise_for_status()
                data = r.content
                break
            except (httpx.ConnectError, httpx.ConnectTimeout, httpx.ReadTimeout) as e:
                last_err = e
                continue
            except httpx.HTTPStatusError as e:
                # For RSS endpoints we don't want to spam retries on 4xx/5xx.
                last_err = e
                break
    if data is None:
        raise last_err or RuntimeError(f"RSS fetch failed: {url}")

    def _strip_ns(tag: str) -> str:
        return tag.split('}', 1)[1] if tag.startswith('{') and '}' in tag else tag

    def _text(node: Optional[ET.Element]) -> str:
        if node is None or node.text is None:
            return ""
        return normalize_whitespace(node.text)

    def _find_child(parent: ET.Element, names: List[str]) -> Optional[ET.Element]:
        for ch in list(parent):
            if _strip_ns(ch.tag) in names:
                return ch
        return None

    def _find_children(parent: ET.Element, name: str) -> List[ET.Element]:
        out: List[ET.Element] = []
        for ch in list(parent):
            if _strip_ns(ch.tag) == name:
                out.append(ch)
        return out

    def _parse_datetime(raw: str) -> Optional[str]:
        raw = (raw or "").strip()
        if not raw:
            return None
        try:
            return dtparser.parse(raw).astimezone().isoformat()
        except Exception:
            return None

    root = ET.fromstring(data)
    root_tag = _strip_ns(root.tag)

    entries: List[ET.Element] = []
    if root_tag == "rss":
        channel = _find_child(root, ["channel"]) or root
        entries = _find_children(channel, "item")
    elif root_tag == "feed":
        entries = _find_children(root, "entry")
    else:
        # fallback: try common containers
        entries = _find_children(root, "item") or _find_children(root, "entry")

    items: List[Dict[str, Any]] = []
    for e in entries:
        title = _text(_find_child(e, ["title"]))

        # link: RSS <link>text</link>, Atom <link href="..."/>
        link = ""
        link_el = _find_child(e, ["link"]) 
        if link_el is not None:
            href = (link_el.attrib.get("href") or "").strip()
            link = href or _text(link_el)

        # summary/body: RSS description/summary/content:encoded, Atom summary/content
        summary = ""
        for name in ["description", "summary", "content"]:
            val = _text(_find_child(e, [name]))
            if val:
                summary = val
                break
        # some feeds store HTML in <content:encoded> (namespaced)
        if not summary:
            for ch in list(e):
                if _strip_ns(ch.tag) == "encoded":
                    summary = _text(ch)
                    if summary:
                        break

        # published/updated
        published_raw = ""
        for name in ["published", "pubDate", "updated"]:
            v = _text(_find_child(e, [name]))
            if v:
                published_raw = v
                break
        published_at = _parse_datetime(published_raw)

        # taxonomy tags/categories
        tags: List[str] = []
        for ch in list(e):
            tname = _strip_ns(ch.tag)
            if tname in ("category", "tag"):
                term = (ch.attrib.get("term") or ch.attrib.get("label") or "").strip()
                txt = term or _text(ch)
                if txt:
                    tags.append(txt)
        if tags:
            seen = set()
            tags = [x for x in tags if not (x in seen or seen.add(x))]

        items.append({
            "url": link,
            "url_canon": canonicalize_url(link),
            "title": title,
            "body": summary,
            "published_at": published_at,
            "tags": tags,
            "section": tags[0] if tags else None,
        })

    return items

def _text_from_html(html: str) -> str:
    soup = BeautifulSoup(html, "lxml")
    for tag in soup(["script", "style", "noscript", "header", "footer", "nav", "aside"]):
        tag.decompose()

    candidates = []
    for sel in [
        "article",
        "main",
        "div[itemprop='articleBody']",
        "div.article__text",
        "div[itemprop='text']",
        # Common regional news CMS blocks
        "div.news__text",
        "div.news__body",
        "div.post__text",
        "div.entry-content",
    ]:
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

def _clean_tass_body(text: str) -> str:
    """Remove common non-article tails/blocks from tass.ru pages."""
    if not text:
        return text
    # Cut off typical copyright / service blocks if present
    cutoff_markers = [
        "© Информационное агентство ТАСС",
        "© Информационное агентство ТАСС",
        "Свидетельство о регистрации СМИ",
        "На информационном ресурсе применяются",
        "Правила цитирования",
        "Правовая информация",
        "Присоединяйтесь к нам",
        "RSS-лента",
    ]
    for m in cutoff_markers:
        idx = text.find(m)
        if idx != -1 and idx > 200:
            text = text[:idx].rstrip()
            break
    # Remove inline noise lines
    noise_phrases = [
        "Новости партнеров",
        "Все материалы",
        "рекомендательные технологии",
    ]
    for ph in noise_phrases:
        text = text.replace(ph, " ")
    return normalize_whitespace(text)


def _clean_dvnovosti_body(text: str) -> str:
    """Remove common non-article blocks from dvnovosti.ru pages."""
    if not text:
        return text
    # Cut off typical "read also" and footer-like blocks.
    cutoff_markers = [
        "Читайте также",
        "Новости партнеров",
        "Подписывайтесь",
        "Поделиться",
        "Комментарии",
    ]
    for m in cutoff_markers:
        idx = text.find(m)
        if idx != -1 and idx > 200:
            text = text[:idx].rstrip()
            break
    noise_phrases = [
        "Новости партнеров",
        "Читайте также",
    ]
    for ph in noise_phrases:
        text = text.replace(ph, " ")
    return normalize_whitespace(text)



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
    body_norm = normalize_whitespace(body)
    if "tass.ru/" in url:
        body_norm = _clean_tass_body(body_norm)
    if "dvnovosti.ru/" in url:
        body_norm = _clean_dvnovosti_body(body_norm)
    return {"title": normalize_whitespace(title), "body": body_norm, "published_at": published_at}

DEFAULT_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    ),
    "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
    "Accept-Language": "ru-RU,ru;q=0.9,en-US;q=0.8,en;q=0.7",
}

def http_fetch(
    url: str,
    *,
    timeout: float = 20.0,
    headers: dict | None = None,
) -> str:
    """
    Скачивает страницу и возвращает text (HTML).
    Исключения не глушит — source сам решает, что с ними делать.
    """
    req_headers = DEFAULT_HEADERS.copy()
    if headers:
        req_headers.update(headers)

    with httpx.Client(
        timeout=timeout,
        headers=req_headers,
        follow_redirects=True,
    ) as client:
        r = client.get(url)
        r.raise_for_status()
        return r.text

from urllib.parse import urljoin, urlparse, urlunparse

def normalize_url(url: str, base: str | None = None) -> str:
    """
    Нормализует URL:
    - склеивает с base, если url относительный
    - убирает fragment (#...)
    - сохраняет query (важно для некоторых источников)
    """
    if base:
        url = urljoin(base, url)

    p = urlparse(url)

    # Убираем fragment, остальное сохраняем
    normalized = urlunparse((
        p.scheme,
        p.netloc,
        p.path,
        p.params,
        p.query,
        "",
    ))

    return normalized