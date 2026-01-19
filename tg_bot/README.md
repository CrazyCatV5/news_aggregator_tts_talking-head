# tg_bot (Pyrogram)

Telegram-бот для проекта "агрегатор новостей бот". Работает внутри Docker Compose и общается с FastAPI по внутренней сети.

## ENV

Минимально:

- `TG_API_ID`
- `TG_API_HASH`
- `TG_BOT_TOKEN`
- `API_BASE_URL` (по умолчанию `http://api:8088`)
- `TZ` (по умолчанию `Europe/Riga`)

Опционально:

- `DEFAULT_LANGUAGE` (по умолчанию `ru`)
- `TG_ALLOWED_USER_IDS` (например: `123,456`)

## Команды

- `/start`, `/help`
- `/today`
- `/day YYYY-MM-DD`
- `/news [YYYY-MM-DD]`
- `/tts [YYYY-MM-DD]`, `/tts_render [YYYY-MM-DD]`
- `/video [YYYY-MM-DD]`, `/video_render [YYYY-MM-DD]`
- `/days`

## Запуск в compose

```bash
docker compose build tg_bot
docker compose up -d tg_bot
docker compose logs -f tg_bot
```
