import json
import datetime as dt
from typing import Any, Dict, List, Tuple, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from queue import Queue, Empty
import threading

from .config import settings
from .db import connect
from .extractors import fetch_rss, fetch_html_index, fetch_article
from .utils import fingerprint, canonicalize_url, normalize_whitespace
from .scoring import score
from .jobs import set_job, init_sources, update_source, push_error

def load_sources() -> List[Dict[str, Any]]:
    with open(settings.sources_path, "r", encoding="utf-8") as f:
        return json.load(f)

def ingest_run(job_id: str, limit_per_html_source: int = 20) -> Dict[str, Any]:
    sources = load_sources()
    now = dt.datetime.now(dt.timezone.utc).isoformat()

    init_sources(job_id, sources)
    set_job(
        job_id,
        status="running",
        fetched_at=now,
        ingested=0,
        errors_count=0,
        total_sources=len(sources),
        done_sources=0,
        links_total=0,
        articles_total=0,
        articles_done=0,
        message="Starting",
    )

    # Writer thread: single DB connection, consumes queue of normalized items
    q: "Queue[Optional[Tuple[str, Dict[str, Any]]]]" = Queue(maxsize=2000)
    ingested_counter = {"n": 0}
    per_source_inserted = {}
    stop_token = object()

    def writer():
        commit_every = max(1, settings.db_commit_every)
        pending = 0
        with connect() as con:
            while True:
                item = q.get()
                if item is None:
                    break
                src_name, it = item
                inserted = _insert_item(con, src_name, it, fetched_at=now)
                if inserted:
                    ingested_counter["n"] += 1
                    per_source_inserted[src_name] = per_source_inserted.get(src_name, 0) + 1
                    update_source(job_id, src_name, inserted=per_source_inserted[src_name])
                pending += 1
                if pending >= commit_every:
                    con.commit()
                    pending = 0
                    set_job(job_id, ingested=ingested_counter["n"], message="Writing to DB")
            if pending:
                con.commit()

    wt = threading.Thread(target=writer, daemon=True)
    wt.start()

    errors_count = 0
    done_sources = 0

    def handle_rss(src: Dict[str, Any]):
        name, url = src["name"], src["url"]
        update_source(job_id, name, state="fetching")
        try:
            items = fetch_rss(url)
            # Push directly to writer queue
            for it in items:
                q.put((name, it))
            update_source(job_id, name, state="done", links=len(items), articles_ok=len(items))
            return {"name": name, "done": True, "links": len(items), "articles_ok": len(items)}
        except Exception as e:
            push_error(job_id, name, "rss", url, str(e))
            update_source(job_id, name, state="error", errors=1)
            return {"name": name, "done": True, "error": str(e)}

    def handle_html(src: Dict[str, Any]):
        name, url = src["name"], src["url"]
        update_source(job_id, name, state="indexing")
        try:
            links = fetch_html_index(url, limit_links=limit_per_html_source)
            update_source(job_id, name, links=len(links), state="fetching")
        except Exception as e:
            push_error(job_id, name, "index", url, str(e))
            update_source(job_id, name, state="error", errors=1)
            return {"name": name, "done": True, "error": str(e)}

        # Fetch articles in parallel
        ok = 0
        with ThreadPoolExecutor(max_workers=settings.article_concurrency) as ex2:
            futs = {ex2.submit(fetch_article, link): link for link in links}
            for fut in as_completed(futs):
                link = futs[fut]
                try:
                    art = fut.result()
                    it = {
                        "url": link,
                        "url_canon": canonicalize_url(link),
                        "title": art.get("title") or "",
                        "body": art.get("body") or "",
                        "published_at": art.get("published_at"),
                    }
                    if len(it["title"]) >= 5 and len(it["body"]) >= 150:
                        q.put((name, it))
                        ok += 1
                        update_source(job_id, name, articles_ok=ok)
                except Exception as e:
                    push_error(job_id, name, "article", link, str(e))
        update_source(job_id, name, state="done")
        return {"name": name, "done": True, "links": len(links), "articles_ok": ok}

    # Source-level parallelism
    with ThreadPoolExecutor(max_workers=settings.fetch_concurrency) as ex:
        futs = {}
        for s in sources:
            if s["kind"] == "rss":
                futs[ex.submit(handle_rss, s)] = s
            else:
                futs[ex.submit(handle_html, s)] = s

        for fut in as_completed(futs):
            s = futs[fut]
            name = s["name"]
            res = fut.result()
            done_sources += 1
            if "error" in res:
                errors_count += 1
                set_job(job_id, errors_count=errors_count)
            set_job(job_id, done_sources=done_sources, message=f"Completed source: {name}")

    # finalize
    q.put(None)
    wt.join(timeout=120)

    set_job(job_id, status="done", ingested=ingested_counter["n"], errors_count=errors_count, message="Completed")
    return {"ok": True, "job_id": job_id, "ingested": ingested_counter["n"], "errors_count": errors_count, "fetched_at": now}

def _insert_item(con, source_name: str, it: Dict[str, Any], fetched_at: str) -> int:
    url = it.get("url") or it.get("url_canon") or ""
    url_canon = it.get("url_canon") or canonicalize_url(url)
    title = normalize_whitespace(it.get("title") or "")
    body = normalize_whitespace(it.get("body") or "")
    published_at = it.get("published_at")

    if not title:
        return 0

    fp = fingerprint(title, url_canon)
    fulltext = f"{title}. {body}"
    biz, dfo, has_company, reasons = score(fulltext)

    before = con.total_changes
    con.execute(
        """
        INSERT OR IGNORE INTO items
        (source_name, url, url_canon, title, body, published_at, fetched_at, fingerprint,
         business_score, dfo_score, has_company, reasons)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            source_name, url, url_canon, title, body, published_at, fetched_at, fp,
            int(biz), int(dfo), int(has_company), json.dumps(reasons, ensure_ascii=False),
        ),
    )
    return 1 if con.total_changes > before else 0
