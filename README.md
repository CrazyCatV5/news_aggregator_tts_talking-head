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


## Очереди по источникам (per-source queues)

Начиная с версии **3.1**, каждый источник обслуживается **своей Redis-очередью**:

- `dfo:queue:vedomosti_economics`
- `dfo:queue:tass_rss_v2`
- `dfo:queue:rbc_export`
- `dfo:queue:rg_doc`
- `dfo:queue:forbes_russia`
- `dfo:queue:eastrussia`
- `dfo:queue:dvnovosti`

Эндпоинт `POST /ingest/run` создаёт job и кладёт **по одному заданию на источник** в соответствующую очередь.

### Worker

В `docker-compose.yml` добавлен сервис `worker`, который по умолчанию слушает **все** очереди.

При необходимости можно поднять *выделенный воркер на конкретный источник*, задав переменную окружения:

- `SOURCE_NAME="TASS RSS v2"` (значение должно совпадать с `name` в `sources.json`)

Пример запуска вне compose:

```bash
SOURCE_NAME="TASS RSS v2" python -m app.worker
```


## LLM Digest (новый слой)

Добавлены контейнеры:
- `ollama` — локальный inference runtime
- `llm` — небольшой FastAPI-обёртка с промптом и chunking для длинных статей
- `llm_worker` — воркер, который забирает item_id из Redis и пишет результаты в SQLite (`llm_analyses`)

### Быстрый старт

1) Поднять всё:
```bash
docker compose up -d --build
```

2) Один раз скачать модель в `ollama` (пример для Qwen 2.5 14B Instruct, GGUF):
```bash
docker compose exec -T ollama ollama pull qwen2.5:14b-instruct-q4_K_M
```

3) Открыть UI:
- `http://localhost:8088/ui` → вкладка **LLM Digest**

4) Нажать **Enqueue (24h)**, затем **Refresh** через 10–60 секунд (в зависимости от CPU/GPU).

### API

- `POST /llm/enqueue` — выбрать кандидатов (exclude war + dfo ≥ 2 + business ≥ 2) и поставить их в очередь
- `GET /llm/items` — список проанализированных новостей
- `GET /llm/items/{item_id}` — детали анализа (включая summary/bulletin/why)

Примечание: модель и контекст задаются переменными окружения `LLM_MODEL`, `LLM_NUM_CTX`.
