# DFO Business News Aggregator — v3 (Full Parallelism + Live UI Progress)

What’s new vs v2:
- **Full parallelism**: per-source and per-article fetching in parallel.
- **Single DB writer thread** with an internal queue (avoids SQLite write contention).
- **Incremental ingest**: items become available in `/news` during `running`.
- **Live UI**: per-source progress table (links found, articles fetched, items inserted, errors).

## Start

```bash
docker compose up -d --build
```

- UI: http://localhost:8088/ui
- Swagger: http://localhost:8088/docs

## Run ingest

PowerShell:
```powershell
Invoke-RestMethod -Method Post -Uri "http://localhost:8088/ingest/run?limit_per_html_source=20"
```

Check job:
```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8088/jobs/<job_id>"
Invoke-RestMethod -Method Get -Uri "http://localhost:8088/jobs/<job_id>/detail"
```

News / Digest:
```powershell
Invoke-RestMethod -Method Get -Uri "http://localhost:8088/news?window_hours=24&min_business=1&min_dfo=1"
Invoke-RestMethod -Method Get -Uri "http://localhost:8088/digest?window_hours=24&min_business=1&min_dfo=1"
```

Notes:
- SQLite is a prototype choice. For production, migrate to Postgres + proper task queue.
