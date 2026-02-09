from __future__ import annotations

import datetime as dt
import os
import sys

import httpx
from apscheduler.schedulers.blocking import BlockingScheduler


API_BASE = os.getenv("API_BASE_URL", "http://api:8088").rstrip("/")
TZ = os.getenv("TZ", "Europe/Riga")


def _log(msg: str) -> None:
    ts = dt.datetime.now(dt.timezone.utc).replace(microsecond=0).isoformat()
    print(f"{ts} | scheduler | {msg}", flush=True)


def _call_auto_run(pipeline: str) -> None:
    url = f"{API_BASE}/auto/run"
    params = {"pipeline": pipeline}
    try:
        with httpx.Client(timeout=20.0) as client:
            r = client.post(url, params=params)
        if r.status_code == 409:
            _log(f"skip {pipeline}: already running")
            return
        if r.status_code >= 400:
            _log(f"ERROR {pipeline}: {r.status_code} {r.text[:500]}")
            return
        data = r.json()
        _log(f"queued {pipeline}: run_id={data.get('run_id')}")
    except Exception as e:
        _log(f"ERROR {pipeline}: {e}")


def job_ingest() -> None:
    _call_auto_run("ingest")


def job_llm() -> None:
    _call_auto_run("llm")


def job_daily() -> None:
    _call_auto_run("daily")


def main() -> None:
    # Defaults: ingest hourly, LLM every 3 hours, daily at 08:10 Riga time.
    ingest_minutes = int(os.getenv("INGEST_EVERY_MIN", "60"))
    llm_minutes = int(os.getenv("LLM_EVERY_MIN", "180"))
    daily_time = os.getenv("DAILY_AT", "08:10")  # local time in TZ

    try:
        hh, mm = daily_time.split(":", 1)
        daily_h = int(hh)
        daily_m = int(mm)
    except Exception:
        _log(f"Invalid DAILY_AT={daily_time}. Expected HH:MM")
        sys.exit(2)

    sched = BlockingScheduler(timezone=TZ)

    sched.add_job(job_ingest, "interval", minutes=max(5, ingest_minutes), id="ingest")
    sched.add_job(job_llm, "interval", minutes=max(15, llm_minutes), id="llm")
    sched.add_job(job_daily, "cron", hour=daily_h, minute=daily_m, id="daily")

    _log(
        "started "
        + f"API_BASE_URL={API_BASE} TZ={TZ} "
        + f"INGEST_EVERY_MIN={ingest_minutes} LLM_EVERY_MIN={llm_minutes} DAILY_AT={daily_time}"
    )

    # Kick once on start if requested
    if os.getenv("RUN_ON_START", "0") == "1":
        _log("RUN_ON_START=1 -> triggering ingest + llm")
        job_ingest()
        job_llm()

    sched.start()


if __name__ == "__main__":
    main()
