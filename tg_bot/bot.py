from __future__ import annotations

import os
import sys
import json
import logging
from typing import Any, Dict

from pyrogram import Client

from api_client import ApiClient
from handlers import register_handlers


def _setup_logging() -> None:
    level = os.getenv("LOG_LEVEL", "INFO").upper().strip() or "INFO"
    logging.basicConfig(level=level, format="%(message)s")

    class JsonFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:
            payload: Dict[str, Any] = {
                "level": record.levelname,
                "logger": record.name,
                "msg": record.getMessage(),
            }
            return json.dumps(payload, ensure_ascii=False)

    root = logging.getLogger()
    for h in root.handlers:
        h.setFormatter(JsonFormatter())


def main() -> None:
    _setup_logging()
    log = logging.getLogger("tg_bot")

    api_id = os.getenv("TG_API_ID", "").strip()
    api_hash = os.getenv("TG_API_HASH", "").strip()
    bot_token = os.getenv("TG_BOT_TOKEN", "").strip()

    if not api_id or not api_hash or not bot_token:
        log.error("Missing required env vars: TG_API_ID, TG_API_HASH, TG_BOT_TOKEN")
        sys.exit(2)

    try:
        api_id_int = int(api_id)
    except Exception:
        log.error("TG_API_ID must be an integer")
        sys.exit(2)

    api = ApiClient()

    app = Client(
        name="dfo_news_bot",
        api_id=api_id_int,
        api_hash=api_hash,
        bot_token=bot_token,
        workdir="/tmp",
        in_memory=True,
    )

    register_handlers(app, api)

    log.info("Bot starting")
    try:
        app.run()
    finally:
        api.close()


if __name__ == "__main__":
    main()
