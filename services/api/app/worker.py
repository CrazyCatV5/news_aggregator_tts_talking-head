import time
from .redis_client import get_redis
from .ingest import ingest_run
from .jobs import set_job

QUEUE_KEY = "dfo:queue"

def main():
    r = get_redis()
    while True:
        item = r.brpop(QUEUE_KEY, timeout=5)
        if not item:
            continue
        _, payload = item
        try:
            job_id, limit_s = payload.split(":", 1)
            limit = int(limit_s)
        except Exception:
            continue

        try:
            set_job(job_id, status="running", message="Worker started")
            ingest_run(job_id=job_id, limit_per_html_source=limit)
        except Exception as e:
            set_job(job_id, status="failed", message=str(e))
        time.sleep(0.1)

if __name__ == "__main__":
    main()
