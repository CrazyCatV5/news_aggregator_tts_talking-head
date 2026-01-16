from __future__ import annotations

import os
import time

from .redis_client import get_redis
from .ingest import ingest_source
from .jobs import set_job
from .sources import queue_keys, source_name_from_queue_key, queue_key_for_source

def main():
    r = get_redis()

    # Optional: run a dedicated worker for a single source.
    # Example: SOURCE_NAME="TASS RSS v2"
    source_name = os.getenv("SOURCE_NAME")
    if source_name:
        listen_keys = [queue_key_for_source(source_name)]
        worker_label = f"worker:{source_name}"
    else:
        listen_keys = queue_keys()
        worker_label = "worker:all"

    while True:
        item = r.brpop(listen_keys, timeout=5)
        if not item:
            continue

        key_b, payload_b = item
        key = key_b.decode() if isinstance(key_b, (bytes, bytearray)) else str(key_b)
        payload = payload_b.decode() if isinstance(payload_b, (bytes, bytearray)) else str(payload_b)

        try:
            job_id, limit_s = payload.split(":", 1)
            limit = int(limit_s)
        except Exception:
            continue

        try:
            src = source_name or source_name_from_queue_key(key)
        except Exception:
            continue

        try:
            # Do not overwrite job totals here; orchestrator sets them.
            set_job(job_id, status="running", message=f"{worker_label} started {src}")
            ingest_source(job_id=job_id, source_name=src, limit_per_html_source=limit)
        except Exception as e:
            set_job(job_id, status="failed", message=f"{worker_label} failed {src}: {e}")
        time.sleep(0.05)

if __name__ == "__main__":
    main()
