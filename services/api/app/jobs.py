import time, uuid, json
from typing import Any, Dict
from .redis_client import get_redis

JOB_PREFIX = "dfo:job:"
JOB_ERRORS = "dfo:joberr:"
JOB_SOURCES = "dfo:jobsources:"

def new_job_id() -> str:
    return uuid.uuid4().hex

def set_job(job_id: str, **fields):
    r = get_redis()
    key = JOB_PREFIX + job_id
    fields.setdefault("updated_at", str(int(time.time())))
    r.hset(key, mapping={k: str(v) for k, v in fields.items()})
    r.expire(key, 60 * 60 * 12)


def incr_job(job_id: str, **increments):
    """Atomically increment integer fields in job hash."""
    r = get_redis()
    key = JOB_PREFIX + job_id
    for k, v in increments.items():
        try:
            r.hincrby(key, k, int(v))
        except Exception:
            # fallback: set as string if not int
            r.hset(key, k, str(v))
    r.hset(key, mapping={"updated_at": str(int(time.time()))})

def get_job(job_id: str) -> Dict[str, Any]:
    r = get_redis()
    key = JOB_PREFIX + job_id
    data = r.hgetall(key)
    if not data:
        return {"job_id": job_id, "status": "not_found"}
    data["job_id"] = job_id
    for k in ["total_sources", "done_sources", "ingested", "errors_count", "links_total", "articles_total", "articles_done"]:
        if k in data:
            try: data[k] = int(data[k])
            except: pass
    return data

def init_sources(job_id: str, sources: list):
    r = get_redis()
    key = JOB_SOURCES + job_id
    # Per-source progress structure consumed by UI (/api/v1/jobs/{job_id}/sources)
    payload = {
        s["name"]: {
            "kind": s.get("kind"),
            "url": s.get("url"),
            "links": 0,
            "articles_ok": 0,
            "inserted": 0,
            "errors": 0,
            "state": "queued",
            "fetched_at": None,
        }
        for s in sources
    }
    r.set(key, json.dumps(payload, ensure_ascii=False))
    r.expire(key, 60 * 60 * 12)

def update_source(job_id: str, source_name: str, **patch):
    r = get_redis()
    key = JOB_SOURCES + job_id
    raw = r.get(key) or "{}"
    data = json.loads(raw)
    cur = data.get(source_name, {})
    cur.update(patch)
    data[source_name] = cur
    r.set(key, json.dumps(data, ensure_ascii=False))
    r.expire(key, 60 * 60 * 12)

def get_sources(job_id: str) -> Dict[str, Any]:
    r = get_redis()
    raw = r.get(JOB_SOURCES + job_id)
    return json.loads(raw) if raw else {}

def finalize_job_if_complete(job_id: str):
    """
    If all sources are completed (done_sources >= total_sources),
    set a final job status:
      - done             (no errors)
      - done_with_errors (any errors_count > 0 OR any source.state == 'error')
    This prevents jobs from being stuck in 'running' when workers finish.
    """
    job = get_job(job_id)
    try:
        total = int(job.get("total_sources", 0) or 0)
        done = int(job.get("done_sources", 0) or 0)
        errs = int(job.get("errors_count", 0) or 0)
    except Exception:
        return

    if total <= 0 or done < total:
        return

    sources = get_sources(job_id) or {}
    any_source_error = any((s or {}).get("state") == "error" for s in sources.values())

    final_status = "done_with_errors" if (errs > 0 or any_source_error) else "done"
    msg = job.get("message") or ""
    suffix = f"finalized: {done}/{total}, errors={errs}"
    if suffix not in msg:
        msg = (msg + " | " + suffix).strip(" |")

    set_job(job_id, status=final_status, message=msg)

def finalize_job_if_complete(job_id: str):
    """
    If all sources are completed (done_sources >= total_sources),
    set a final job status:
      - done             (no errors)
      - done_with_errors (any errors_count > 0 OR any source.state == 'error')
    This prevents jobs from being stuck in 'running' when workers finish.
    """
    job = get_job(job_id)
    try:
        total = int(job.get("total_sources", 0) or 0)
        done = int(job.get("done_sources", 0) or 0)
        errs = int(job.get("errors_count", 0) or 0)
    except Exception:
        return

    if total <= 0 or done < total:
        return

    sources = get_sources(job_id) or {}
    any_source_error = any((s or {}).get("state") == "error" for s in sources.values())

    final_status = "done_with_errors" if (errs > 0 or any_source_error) else "done"
    msg = job.get("message") or ""
    suffix = f"finalized: {done}/{total}, errors={errs}"
    if suffix not in msg:
        msg = (msg + " | " + suffix).strip(" |")

    set_job(job_id, status=final_status, message=msg)

def push_error(job_id: str, source: str, stage: str, url: str, error: str):
    r = get_redis()
    key = JOB_ERRORS + job_id
    r.rpush(key, json.dumps({"source": source, "stage": stage, "url": url, "error": error}, ensure_ascii=False))
    r.ltrim(key, -200, -1)  # keep last 200
    r.expire(key, 60 * 60 * 12)

def get_errors(job_id: str, limit: int = 200):
    r = get_redis()
    key = JOB_ERRORS + job_id
    items = r.lrange(key, max(0, -limit), -1)
    out = []
    for x in items:
        try: out.append(json.loads(x))
        except: pass
    return out


def list_jobs(limit: int = 20, offset: int = 0):
    """List recent jobs.

    Jobs are stored as Redis hashes under JOB_PREFIX + job_id.
    We scan keys and sort by created_at desc (fallback updated_at).
    """
    r = get_redis()

    keys = []
    # scan_iter is incremental
    for k in r.scan_iter(match=JOB_PREFIX + "*"):
        try:
            ks = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
            keys.append(ks)
        except Exception:
            pass

    items = []
    for key in keys:
        job_id = key.replace(JOB_PREFIX, "", 1)
        data = r.hgetall(key) or {}
        # normalize
        def _int(v, fb=0):
            try:
                return int(v)
            except Exception:
                return fb

        created_at = _int(data.get("created_at") or data.get("updated_at") or "0", 0)
        updated_at = _int(data.get("updated_at") or data.get("created_at") or "0", 0)
        items.append({
            "job_id": job_id,
            "status": data.get("status", "—"),
            "created_at": created_at,
            "updated_at": updated_at,
            "ingested": _int(data.get("ingested", 0), 0),
            "errors_count": _int(data.get("errors_count", 0), 0),
        })

    items.sort(key=lambda x: x.get("created_at", 0), reverse=True)
    total = len(items)
    off = max(0, int(offset))
    lim = max(1, int(limit))
    sliced = items[off:off+lim]
    return {"ok": True, "total": total, "items": sliced}
