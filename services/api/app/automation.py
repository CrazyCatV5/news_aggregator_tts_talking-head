from __future__ import annotations

import asyncio
import datetime as dt
import json
import threading
import time
import uuid
from typing import Any, Dict, List, Optional

from fastapi import APIRouter, HTTPException, Query

from .redis_client import get_redis
from .ingest import ingest_job_init
from .jobs import get_job
from .llm_queue import enqueue_candidates
from .sources import list_source_names, queue_key_for_source
from .daily_digests import DigestParams, create_or_refill_daily_digest
from .tts_api import tts_daily_render, tts_daily_status
from .video_api import video_daily_render, video_daily_status


router = APIRouter(prefix="/auto", tags=["automation"])


# ---------------------------------------------------------------------------
# Redis schema
# ---------------------------------------------------------------------------

K_STATE = "dfo:auto:state"              # hash
K_RUNS_Z = "dfo:auto:runs"              # zset score=created_at_ts
K_RUN_PREFIX = "dfo:auto:run:"          # hash: dfo:auto:run:<run_id>
K_LOG_PREFIX = "dfo:auto:runlog:"       # list: dfo:auto:runlog:<run_id>


def _utc_ts() -> int:
    return int(time.time())


def _utc_iso() -> str:
    return dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()


def _new_run_id() -> str:
    return uuid.uuid4().hex


def _run_key(run_id: str) -> str:
    return K_RUN_PREFIX + run_id


def _log_key(run_id: str) -> str:
    return K_LOG_PREFIX + run_id


def _log(run_id: str, msg: str) -> None:
    r = get_redis()
    line = f"{_utc_iso()} | {msg}"
    r.rpush(_log_key(run_id), line)
    r.ltrim(_log_key(run_id), -800, -1)
    r.expire(_log_key(run_id), 60 * 60 * 72)


def _set_run(run_id: str, **fields: Any) -> None:
    r = get_redis()
    fields.setdefault("updated_at", _utc_iso())
    r.hset(_run_key(run_id), mapping={k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v) for k, v in fields.items()})
    r.expire(_run_key(run_id), 60 * 60 * 72)


def _get_run(run_id: str) -> Dict[str, Any]:
    r = get_redis()
    raw = r.hgetall(_run_key(run_id)) or {}
    out: Dict[str, Any] = {"run_id": run_id}
    for k, v in raw.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        # best-effort json
        if vv and vv[:1] in "[{":
            try:
                out[kk] = json.loads(vv)
                continue
            except Exception:
                pass
        out[kk] = vv
    return out


def _tail_log(run_id: str, limit: int = 200) -> List[str]:
    r = get_redis()
    items = r.lrange(_log_key(run_id), max(0, -int(limit)), -1)
    out: List[str] = []
    for x in items:
        try:
            out.append(x.decode() if isinstance(x, (bytes, bytearray)) else str(x))
        except Exception:
            pass
    return out


def _set_state(**fields: Any) -> None:
    r = get_redis()
    fields.setdefault("updated_at", _utc_iso())
    r.hset(K_STATE, mapping={k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v) for k, v in fields.items()})
    r.expire(K_STATE, 60 * 60 * 72)


def _get_state() -> Dict[str, Any]:
    r = get_redis()
    raw = r.hgetall(K_STATE) or {}
    out: Dict[str, Any] = {}
    for k, v in raw.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        if vv and vv[:1] in "[{":
            try:
                out[kk] = json.loads(vv)
                continue
            except Exception:
                pass
        out[kk] = vv
    return out


def _add_run(run_id: str) -> None:
    r = get_redis()
    r.zadd(K_RUNS_Z, {run_id: float(_utc_ts())})
    r.zremrangebyrank(K_RUNS_Z, 0, -501)  # keep last 500
    r.expire(K_RUNS_Z, 60 * 60 * 72)


def _list_runs(limit: int = 30, offset: int = 0) -> Dict[str, Any]:
    r = get_redis()
    limit = max(1, min(int(limit), 200))
    offset = max(0, int(offset))
    total = int(r.zcard(K_RUNS_Z) or 0)
    # newest first
    ids = r.zrevrange(K_RUNS_Z, offset, offset + limit - 1)
    run_ids = [(x.decode() if isinstance(x, (bytes, bytearray)) else str(x)) for x in (ids or [])]
    return {"ok": True, "total": total, "limit": limit, "offset": offset, "items": run_ids}


# ---------------------------------------------------------------------------
# Pipeline execution
# ---------------------------------------------------------------------------

def _today_utc() -> str:
    return dt.datetime.now(dt.timezone.utc).date().isoformat()


def _step(run_id: str, name: str, status: str, **extra: Any) -> None:
    run = _get_run(run_id)
    steps = run.get("steps") if isinstance(run.get("steps"), dict) else {}
    steps = dict(steps) if isinstance(steps, dict) else {}
    steps[name] = {"status": status, "ts": _utc_iso(), **extra}
    _set_run(run_id, steps=steps)


def _run_ingest(run_id: str, limit_per_html_source: int = 500) -> str:
    from .jobs import new_job_id
    from .redis_client import get_redis

    job_id = new_job_id()
    _log(run_id, f"ingest: init job_id={job_id}")
    ingest_job_init(job_id)

    r = get_redis()
    for src_name in list_source_names():
        r.lpush(queue_key_for_source(src_name), f"{job_id}:{int(limit_per_html_source)}")

    _log(run_id, f"ingest: enqueued sources={len(list_source_names())}")
    return job_id


def _wait_job_done(run_id: str, job_id: str, timeout_s: int = 900, poll_s: float = 2.0) -> Dict[str, Any]:
    t0 = time.time()
    last_status = None
    while True:
        job = get_job(job_id)
        status = job.get("status")
        if status != last_status:
            _log(run_id, f"ingest: job status={status}")
            last_status = status

        if status in {"done", "done_with_errors", "failed"}:
            return job
        if (time.time() - t0) > timeout_s:
            _log(run_id, f"ingest: timeout waiting job_id={job_id}")
            return job
        time.sleep(poll_s)


def _run_llm_enqueue(run_id: str, window_hours: int = 6, min_business: int = 2, min_dfo: int = 2, exclude_war: bool = True, limit: int = 500) -> int:
    n = enqueue_candidates(
        window_hours=window_hours,
        min_business=min_business,
        min_dfo=min_dfo,
        require_company=False,
        exclude_war=exclude_war,
        limit=limit,
    )
    _log(run_id, f"llm: enqueued={n} (window_hours={window_hours})")
    return int(n)


async def _run_daily_assets_async(run_id: str, day: str, language: str = "ru", force_digest: bool = False, force_script: bool = False, force_tts: bool = False, force_video: bool = False) -> Dict[str, Any]:
    # 1) digest
    params = DigestParams()
    d = create_or_refill_daily_digest(day, params, refill=True, force=force_digest)
    _log(run_id, f"daily: digest status={d.get('status')} items={d.get('items_count')} day={day}")

    # 2) script
    from .daily_digests import generate_digest_script
    try:
        scr = await generate_digest_script(day=day, force=force_script)
        _log(run_id, f"daily: script ok day={day}")
    except Exception as e:
        _log(run_id, f"daily: script FAILED day={day}: {e}")
        raise

    # 3) tts (idempotent)
    if not force_tts:
        st = tts_daily_status(day=day, language=language)
        if isinstance(st, dict) and st.get("exists"):
            _log(run_id, f"daily: tts exists file={st.get('file_name')}")
        else:
            force_tts = True

    if force_tts:
        await tts_daily_render(day=day, language=language, voice_wav=None, force_script=False)
        st2 = tts_daily_status(day=day, language=language)
        _log(run_id, f"daily: tts rendered file={st2.get('file_name') if isinstance(st2, dict) else '—'}")

    # 4) video (idempotent)
    if not force_video:
        vst = video_daily_status(day=day, language=language)
        if isinstance(vst, dict) and vst.get("exists"):
            _log(run_id, f"daily: video exists file={vst.get('file_name')}")
        else:
            force_video = True

    if force_video:
        await video_daily_render(day=day, language=language, force_tts=False, image=None)
        vst2 = video_daily_status(day=day, language=language)
        _log(run_id, f"daily: video rendered file={vst2.get('file_name') if isinstance(vst2, dict) else '—'}")

    return {"ok": True, "day": day, "language": language}


def _run_pipeline(run_id: str, pipeline: str, day: Optional[str], wait_ingest: bool) -> None:
    day = day or _today_utc()
    _set_run(run_id, status="running", pipeline=pipeline, day=day, started_at=_utc_iso(), steps={})
    _set_state(running_run_id=run_id, running_pipeline=pipeline, running_day=day)

    try:
        if pipeline in {"ingest", "full"}:
            _step(run_id, "ingest", "running")
            job_id = _run_ingest(run_id)
            _step(run_id, "ingest", "queued", job_id=job_id)
            if wait_ingest:
                job = _wait_job_done(run_id, job_id)
                _step(run_id, "ingest", "done", job=job)

        if pipeline in {"llm", "full"}:
            _step(run_id, "llm", "running")
            n = _run_llm_enqueue(run_id)
            _step(run_id, "llm", "done", enqueued=n)

        if pipeline in {"daily", "full"}:
            _step(run_id, "daily", "running")
            asyncio.run(_run_daily_assets_async(run_id, day=day))
            _step(run_id, "daily", "done")

        _set_run(run_id, status="done", finished_at=_utc_iso())
        _set_state(last_ok_run_id=run_id, last_ok_at=_utc_iso())
        _log(run_id, "pipeline: DONE")
    except Exception as e:
        _set_run(run_id, status="failed", finished_at=_utc_iso(), error=str(e)[:800])
        _set_state(last_failed_run_id=run_id, last_failed_at=_utc_iso(), last_failed_error=str(e)[:800])
        _log(run_id, f"pipeline: FAILED: {e}")
    finally:
        # clear running marker
        st = _get_state()
        if st.get("running_run_id") == run_id:
            _set_state(running_run_id="", running_pipeline="", running_day="")


def _start_thread(run_id: str, pipeline: str, day: Optional[str], wait_ingest: bool) -> None:
    t = threading.Thread(target=_run_pipeline, args=(run_id, pipeline, day, wait_ingest), daemon=True)
    t.start()


# ---------------------------------------------------------------------------
# API endpoints
# ---------------------------------------------------------------------------


@router.get("/state")
def auto_state():
    return {"ok": True, "state": _get_state()}


@router.get("/runs")
def auto_runs(limit: int = Query(30, ge=1, le=200), offset: int = Query(0, ge=0, le=5000)):
    return _list_runs(limit=limit, offset=offset)


@router.get("/runs/{run_id}")
def auto_run_detail(run_id: str, log_limit: int = Query(200, ge=0, le=800)):
    run = _get_run(run_id)
    if not run or (run.get("status") is None and len(run.keys()) <= 1):
        raise HTTPException(status_code=404, detail="run not found")
    return {"ok": True, "run": run, "log": _tail_log(run_id, limit=log_limit)}


@router.post("/run")
def auto_run(
    pipeline: str = Query("full", pattern=r"^(ingest|llm|daily|full)$"),
    day: Optional[str] = Query(None, pattern=r"^\d{4}-\d{2}-\d{2}$"),
    wait_ingest: bool = Query(False),
):
    # single-flight: prevent multiple concurrent runs unless user explicitly starts another (we keep simple: reject)
    st = _get_state()
    if (st.get("running_run_id") or "").strip():
        raise HTTPException(status_code=409, detail=f"another run is already running: {st.get('running_run_id')}")

    run_id = _new_run_id()
    _add_run(run_id)
    _set_run(run_id, status="queued", pipeline=pipeline, day=day or _today_utc(), created_at=_utc_iso(), steps={})
    _log(run_id, f"pipeline: QUEUED pipeline={pipeline} day={day or _today_utc()} wait_ingest={wait_ingest}")
    _start_thread(run_id, pipeline=pipeline, day=day, wait_ingest=wait_ingest)
    return {"ok": True, "run_id": run_id}
