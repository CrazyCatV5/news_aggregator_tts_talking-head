import re
import hashlib
from urllib.parse import urlparse, urlunparse, parse_qsl, urlencode

def canonicalize_url(url: str) -> str:
    try:
        p = urlparse(url)
        q = [(k, v) for k, v in parse_qsl(p.query, keep_blank_values=True)
             if not k.lower().startswith("utm_") and k.lower() not in ("yclid", "gclid", "fbclid")]
        new = p._replace(query=urlencode(q, doseq=True), fragment="")
        return urlunparse(new)
    except Exception:
        return url

def fingerprint(title: str, url: str) -> str:
    s = (title.strip().lower() + "|" + url.strip().lower()).encode("utf-8", errors="ignore")
    return hashlib.sha256(s).hexdigest()

def normalize_whitespace(text: str) -> str:
    return re.sub(r"\s+", " ", text or "").strip()
