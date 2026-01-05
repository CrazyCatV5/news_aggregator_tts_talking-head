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
    payload = {s["name"]: {"kind": s["kind"], "url": s["url"], "links": 0, "articles_ok": 0, "inserted": 0, "errors": 0, "state": "queued"} for s in sources}
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
