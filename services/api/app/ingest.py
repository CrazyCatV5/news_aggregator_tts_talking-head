from __future__ import annotations

import datetime as dt
from typing import Any, Dict, List, Optional

import json
from .config import settings
from .db import connect
from .utils import fingerprint, canonicalize_url, normalize_whitespace
from .scoring import score
from .jobs import init_sources, update_source, push_error, set_job, incr_job, finalize_job_if_complete
from .sources import get_parser, list_source_names


def ingest_source(job_id: str, source_name: str, limit_per_html_source: int = 200) -> Dict[str, Any]:
    """Ingest exactly one source.

    This is designed to be executed by a per-source queue/worker so that:
    - one broken source does not block other sources
    - backpressure and retries can be handled independently per source
    """
    parser = get_parser(source_name)
    now = dt.datetime.now(dt.timezone.utc).isoformat()

    update_source(job_id, source_name, state="running", fetched_at=now, errors=0, links=0, articles_ok=0, inserted=0)

    # Fetch
    try:
        items = parser.fetch_items(limit_per_html_source=limit_per_html_source)
    except Exception as e:
        push_error(job_id, source_name, "fetch", parser.config.url, str(e))
        update_source(job_id, source_name, state="error", errors=1)
        incr_job(job_id, errors_count=1, done_sources=1)
        finalize_job_if_complete(job_id)
        return {"ok": False, "source": source_name, "error": str(e)}

    links_n = len(items)
    update_source(job_id, source_name, links=links_n)

    # Normalize + basic validation + store
    inserted = 0
    ok = 0

    commit_every = max(1, int(settings.db_commit_every))
    pending = 0

    with connect() as con:
        for it in items:
            try:
                it_norm = {
                    "url": it.get("url") or "",
                    "url_canon": it.get("url_canon") or canonicalize_url(it.get("url") or ""),
                    "title": normalize_whitespace(it.get("title") or ""),
                    "body": normalize_whitespace(it.get("body") or ""),
                    "published_at": it.get("published_at"),
                }
                if len(it_norm["title"]) < 5:
                    continue
                # RSS often has short descriptions; keep the existing threshold but be less strict for RSS.
                min_body = 80 if parser.config.kind == "rss" else 150
                if len(it_norm["body"]) < min_body:
                    continue

                ok += 1
                if _insert_item(con, source_name, it_norm, fetched_at=now):
                    inserted += 1
                    incr_job(job_id, ingested=1)
                pending += 1
                if pending >= commit_every:
                    con.commit()
                    pending = 0
                if ok % 10 == 0:
                    update_source(job_id, source_name, articles_ok=ok, inserted=inserted)
            except Exception as e:
                push_error(job_id, source_name, "store", it.get("url") or parser.config.url, str(e))
                incr_job(job_id, errors_count=1)
        if pending:
            con.commit()

    update_source(job_id, source_name, articles_ok=ok, inserted=inserted, state="done")
    incr_job(job_id, done_sources=1, links_total=links_n, articles_total=ok)
    finalize_job_if_complete(job_id)
    return {"ok": True, "source": source_name, "links": links_n, "articles_ok": ok, "inserted": inserted}


def ingest_job_init(job_id: str, sources: Optional[List[Dict[str, Any]]] = None) -> Dict[str, Any]:
    """Initialize job and source status structures in Redis."""
    if sources is None:
        # Build sources list from registry (canonical)
        sources = [{"name": n, "kind": get_parser(n).config.kind, "url": get_parser(n).config.url} for n in list_source_names()]

    now = int(dt.datetime.now(dt.timezone.utc).timestamp())
    set_job(
        job_id,
        status="queued",
        created_at=now,
        updated_at=now,
        fetched_at=dt.datetime.now(dt.timezone.utc).isoformat(),
        ingested=0,
        errors_count=0,
        total_sources=len(sources),
        done_sources=0,
        links_total=0,
        articles_total=0,
        message="Queued",
    )
    init_sources(job_id, sources)
    return {"ok": True, "job_id": job_id, "total_sources": len(sources)}


def _safe_int(v: Any, default: int = 0) -> int:
    try: return int(v)
    except Exception: return default

def _insert_item(con, source_name: str, it: Dict[str, Any], fetched_at: str) -> int:
    url = it.get("url") or it.get("url_canon") or ""
    url_canon = it.get("url_canon") or canonicalize_url(url)
    title = normalize_whitespace(it.get("title") or "")
    body = normalize_whitespace(it.get("body") or "")
    published_at = it.get("published_at")

    if not title:
        return 0

    # Stable fingerprint + dedup by canonical URL.
    fp = fingerprint(title, url_canon)

    # scoring.score(text) -> (business_score, dfo_score, has_company, reasons_dict)
    business_score, dfo_score, has_company, reasons = score(f"{title} {body}")
    reasons_json = json_dumps(reasons)

    cur = con.execute(
        """
        INSERT OR IGNORE INTO items (
            source_name, url, url_canon, title, body,
            published_at, fetched_at,
            fingerprint, business_score, dfo_score,
            has_company, reasons
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_name, url, url_canon, title, body,
            published_at, fetched_at,
            fp, _safe_int(business_score), _safe_int(dfo_score),
            _safe_int(has_company), reasons_json,
        ),
    )
    return 1


def json_dumps(obj: Any) -> str:
    import json
    return json.dumps(obj, ensure_ascii=False)
