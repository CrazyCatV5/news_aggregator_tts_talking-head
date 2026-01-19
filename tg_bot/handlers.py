from __future__ import annotations

import os
import re
import time
import tempfile
import logging
import datetime as dt
from zoneinfo import ZoneInfo
from typing import Optional, Dict, Any

from pyrogram import Client, filters
from pyrogram.types import (
    Message,
    ReplyKeyboardMarkup,
    KeyboardButton,
    InlineKeyboardMarkup,
    InlineKeyboardButton,
    CallbackQuery,
)

from api_client import ApiClient, ApiError
from formatters import split_message, format_digest_text, format_news_list, format_days_list

logger = logging.getLogger(__name__)

TZ = ZoneInfo(os.getenv("TZ", "Europe/Riga"))
DEFAULT_LANGUAGE = os.getenv("DEFAULT_LANGUAGE", "ru").strip() or "ru"

_RE_DAY = re.compile(r"^\d{4}-\d{2}-\d{2}$")

# --- Menu buttons (ReplyKeyboard) ---
BTN_TODAY = "📌 Сегодня"
BTN_YESTERDAY = "🕘 Вчера"
BTN_DAYS = "📅 Дни"
BTN_NEWS = "📰 Новости"
BTN_TTS = "🎧 Аудио"
BTN_VIDEO = "🎥 Видео"
BTN_HELP = "❓ Помощь"


def _allowed_user_ids() -> Optional[set[int]]:
    raw = (os.getenv("TG_ALLOWED_USER_IDS") or "").strip()
    if not raw:
        return None
    out: set[int] = set()
    for p in raw.split(","):
        p = p.strip()
        if not p:
            continue
        try:
            out.add(int(p))
        except Exception:
            continue
    return out or None


ALLOWED = _allowed_user_ids()


def _is_allowed(message: Message) -> bool:
    if ALLOWED is None:
        return True
    uid = getattr(message.from_user, "id", None)
    return uid is not None and int(uid) in ALLOWED


def _today_str() -> str:
    return dt.datetime.now(TZ).date().isoformat()


def _yesterday_str() -> str:
    return (dt.datetime.now(TZ).date() - dt.timedelta(days=1)).isoformat()


def parse_day_arg(arg: Optional[str]) -> str:
    if not arg:
        return _today_str()
    s = arg.strip().lower()
    if s in ("today", "сегодня"):
        return _today_str()
    if s in ("yesterday", "вчера"):
        return _yesterday_str()
    if _RE_DAY.match(s):
        return s
    return _today_str()


def main_menu_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        [
            [KeyboardButton(BTN_TODAY), KeyboardButton(BTN_YESTERDAY), KeyboardButton(BTN_DAYS)],
            [KeyboardButton(BTN_NEWS), KeyboardButton(BTN_TTS), KeyboardButton(BTN_VIDEO)],
            [KeyboardButton(BTN_HELP)],
        ],
        resize_keyboard=True,
        one_time_keyboard=False,
        selective=False,
    )


def day_actions_keyboard(day: str) -> InlineKeyboardMarkup:
    lang = DEFAULT_LANGUAGE
    # callback_data must be <= 64 bytes, keep it short
    # Format: act|YYYY-MM-DD|lang
    return InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("📰 Новости", callback_data=f"news|{day}|{lang}"),
                InlineKeyboardButton("🎧 Аудио", callback_data=f"tts|{day}|{lang}"),
                InlineKeyboardButton("🎥 Видео", callback_data=f"video|{day}|{lang}"),
            ]
        ]
    )


def register_handlers(app: Client, api: ApiClient) -> None:
    @app.on_message(filters.command(["start"]))
    def start(_, message: Message):
        if not _is_allowed(message):
            return
        text = (
            "Привет. Я бот деловых новостей ДФО.\n\n"
            "Выбирай действия кнопками внизу.\n\n"
            "Команды (если нужно):\n"
            "/today — дайджест за сегодня\n"
            "/day YYYY-MM-DD — дайджест за дату\n"
            "/news [YYYY-MM-DD] — список новостей\n"
            "/tts [YYYY-MM-DD] — аудио\n"
            "/video [YYYY-MM-DD] — видео\n"
            "/days — доступные дни\n\n"
            "Подсказка: можно писать 'сегодня'/'вчера' в аргументах."
        )
        message.reply_text(text, reply_markup=main_menu_keyboard())

    @app.on_message(filters.command(["help"]))
    def help_cmd(_, message: Message):
        if not _is_allowed(message):
            return
        message.reply_text(
            "Команды:\n"
            "/today\n"
            "/day YYYY-MM-DD\n"
            "/news [YYYY-MM-DD]\n"
            "/tts [YYYY-MM-DD]\n"
            "/video [YYYY-MM-DD]\n"
            "/days",
            reply_markup=main_menu_keyboard(),
        )

    @app.on_message(filters.command(["today"]))
    def today(_, message: Message):
        if not _is_allowed(message):
            return
        _send_digest(message, api, _today_str())

    @app.on_message(filters.command(["day"]))
    def day_cmd(_, message: Message):
        if not _is_allowed(message):
            return
        parts = message.text.split(maxsplit=1)
        day = parse_day_arg(parts[1] if len(parts) > 1 else None)
        _send_digest(message, api, day)

    @app.on_message(filters.command(["news"]))
    def news_cmd(_, message: Message):
        if not _is_allowed(message):
            return
        parts = message.text.split(maxsplit=1)
        day = parse_day_arg(parts[1] if len(parts) > 1 else None)
        _send_news(message, api, day)

    @app.on_message(filters.command(["days"]))
    def days_cmd(_, message: Message):
        if not _is_allowed(message):
            return
        _send_days(message, api)

    @app.on_message(filters.command(["tts"]))
    def tts_cmd(_, message: Message):
        if not _is_allowed(message):
            return
        parts = message.text.split(maxsplit=1)
        day = parse_day_arg(parts[1] if len(parts) > 1 else None)
        _send_tts(message, api, day, lang=DEFAULT_LANGUAGE)

    @app.on_message(filters.command(["video"]))
    def video_cmd(_, message: Message):
        if not _is_allowed(message):
            return
        parts = message.text.split(maxsplit=1)
        day = parse_day_arg(parts[1] if len(parts) > 1 else None)
        _send_video(message, api, day, lang=DEFAULT_LANGUAGE)

    # --- ReplyKeyboard menu handling (text buttons) ---
    @app.on_message(filters.text & ~filters.command(["start", "help", "today", "day", "news", "days", "tts", "video"]))
    def menu_buttons(_, message: Message):
        if not _is_allowed(message):
            return

        txt = (message.text or "").strip()

        if txt == BTN_TODAY:
            _send_digest(message, api, _today_str())
            return
        if txt == BTN_YESTERDAY:
            _send_digest(message, api, _yesterday_str())
            return
        if txt == BTN_DAYS:
            _send_days(message, api)
            return
        if txt == BTN_NEWS:
            _send_news(message, api, _today_str())
            return
        if txt == BTN_TTS:
            _send_tts(message, api, _today_str(), lang=DEFAULT_LANGUAGE)
            return
        if txt == BTN_VIDEO:
            _send_video(message, api, _today_str(), lang=DEFAULT_LANGUAGE)
            return
        if txt == BTN_HELP:
            help_cmd(_, message)
            return

        # default: keep UX tight
        message.reply_text("Выбери действие кнопками внизу.", reply_markup=main_menu_keyboard())

    # --- Inline buttons under digest ---
    @app.on_callback_query()
    def on_inline_button(_, cq: CallbackQuery):
        msg = cq.message
        if msg is None:
            return
        if not _is_allowed(msg):
            cq.answer("Доступ запрещён.", show_alert=True)
            return

        data = (cq.data or "").strip()
        cq.answer()

        # --- Days list actions ---
        if data == "days_refresh":
            try:
                out = api.list_digests(limit=14, offset=0)
                items = out.get("items") or []
                if not items:
                    msg.reply_text("Доступных дней нет. Попробуй позже.", reply_markup=main_menu_keyboard())
                    return

                # Пытаемся отредактировать сообщение, если можно
                try:
                    msg.edit_text("Выбери день:", reply_markup=days_keyboard(items, limit=14))
                except Exception:
                    msg.reply_text("Выбери день:", reply_markup=days_keyboard(items, limit=14))
            except ApiError as e:
                _reply_api_error(msg, e)
            return

        if data.startswith("pickday|"):
            day = data.split("|", 1)[1].strip()
            if not day:
                return
            # Показываем меню дня
            try:
                try:
                    msg.edit_text(f"День: {day}\nВыбери действие:", reply_markup=day_menu_keyboard(day))
                except Exception:
                    msg.reply_text(f"День: {day}\nВыбери действие:", reply_markup=day_menu_keyboard(day))
            except Exception:
                msg.reply_text(f"День: {day}\nВыбери действие:", reply_markup=day_menu_keyboard(day))
            return

        # --- Day actions (digest/news/tts/video) ---
        # Format: act|YYYY-MM-DD|lang
        try:
            act, day, lang = data.split("|", 2)
        except Exception:
            cq.answer("Некорректная кнопка.", show_alert=True)
            return

        if act == "digest":
            _send_digest(msg, api, day)
            return

        if act == "news":
            _send_news(msg, api, day)
            return

        if act == "tts":
            _send_tts(msg, api, day, lang=lang)
            return

        if act == "video":
            _send_video(msg, api, day, lang=lang)
            return


def _send_days(message: Message, api: ApiClient) -> None:
    try:
        out = api.list_digests(limit=14, offset=0)
        items = out.get("items") or []
        if not items:
            message.reply_text("Доступных дней нет. Попробуй позже.", reply_markup=main_menu_keyboard())
            return

        text = "Выбери день:"
        message.reply_text(
            text,
            reply_markup=days_keyboard(items, limit=14),
        )
    except ApiError as e:
        _reply_api_error(message, e)
def days_keyboard(items: list[dict], limit: int = 14) -> InlineKeyboardMarkup:
    rows: list[list[InlineKeyboardButton]] = []
    chunk: list[InlineKeyboardButton] = []

    for it in (items or [])[:limit]:
        day = (it.get("day") or "").strip()
        if not day:
            continue

        status = (it.get("status") or "").strip()
        n_items = it.get("n_items") or it.get("items") or it.get("count") or it.get("n") or 0

        # Лейбл кнопки: "2026-01-17 (5)" или "2026-01-17"
        label = f"{day} ({n_items})" if n_items else day
        if status and status != "ready":
            label = f"{label} · {status}"

        # callback: pickday|YYYY-MM-DD
        btn = InlineKeyboardButton(label, callback_data=f"pickday|{day}")

        chunk.append(btn)
        if len(chunk) == 2:  # 2 кнопки в строке
            rows.append(chunk)
            chunk = []

    if chunk:
        rows.append(chunk)

    # нижняя строка: обновить
    rows.append([InlineKeyboardButton("🔄 Обновить", callback_data="days_refresh")])

    return InlineKeyboardMarkup(rows)


def day_menu_keyboard(day: str) -> InlineKeyboardMarkup:
    lang = DEFAULT_LANGUAGE
    return InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("📌 Дайджест", callback_data=f"digest|{day}|{lang}")],
            [
                InlineKeyboardButton("📰 Новости", callback_data=f"news|{day}|{lang}"),
                InlineKeyboardButton("🎧 Аудио", callback_data=f"tts|{day}|{lang}"),
            ],
            [InlineKeyboardButton("🎥 Видео", callback_data=f"video|{day}|{lang}")],
            [InlineKeyboardButton("⬅️ К списку дней", callback_data="days_refresh")],
        ]
    )



def _send_news(message: Message, api: ApiClient, day: str) -> None:
    try:
        data = api.get_digest(day)
        if not data.get("exists"):
            message.reply_text(f"Дайджест за {day} не найден. Попробуй {BTN_DAYS} или /days.", reply_markup=main_menu_keyboard())
            return
        digest = data.get("digest") or {}
        text = format_news_list(day, digest)
        for chunk in split_message(text):
            message.reply_text(chunk, disable_web_page_preview=True, reply_markup=main_menu_keyboard())
    except ApiError as e:
        _reply_api_error(message, e)


def _send_digest(message: Message, api: ApiClient, day: str) -> None:
    try:
        data = api.get_digest(day)
        if not data.get("exists"):
            message.reply_text(f"Дайджест за {day} не найден. Попробуй {BTN_DAYS} или /days.", reply_markup=main_menu_keyboard())
            return

        digest = data.get("digest") or {}
        text = format_digest_text(day, digest, max_bullets=10)

        chunks = split_message(text)
        for i, chunk in enumerate(chunks):
            if i == len(chunks) - 1:
                message.reply_text(
                    chunk,
                    disable_web_page_preview=True,
                    reply_markup=day_actions_keyboard(day),
                )
            else:
                message.reply_text(chunk, disable_web_page_preview=True)
    except ApiError as e:
        _reply_api_error(message, e)


def _send_tts(message: Message, api: ApiClient, day: str, *, lang: str) -> None:
    try:
        st = api.tts_status(day, language=lang)
        if not st.get("exists"):
            message.reply_text(
                f"Аудио за {day} ещё не подготовлено.\n"
                f"Выбери другой день через {BTN_DAYS} или /days.",
                reply_markup=main_menu_keyboard(),
            )
            return

        _send_file_from_api(
            message,
            api,
            download_url=st["download_url"],
            filename=st.get("file_name") or f"tts_{day}.wav",
            kind="audio",
            caption=f"TTS {day} ({lang})",
        )
    except ApiError as e:
        _reply_api_error(message, e)


def _send_video(message: Message, api: ApiClient, day: str, *, lang: str) -> None:
    try:
        st = api.video_status(day, language=lang)
        if not st.get("exists"):
            message.reply_text(
                f"Видео за {day} ещё не подготовлено.\n"
                f"Выбери другой день через {BTN_DAYS} или /days.",
                reply_markup=main_menu_keyboard(),
            )
            return

        _send_file_from_api(
            message,
            api,
            download_url=st["download_url"],
            filename=st.get("file_name") or f"video_{day}.mp4",
            kind="video",
            caption=f"Видео {day} ({lang})",
        )
    except ApiError as e:
        _reply_api_error(message, e)


def _send_file_from_api(
    message: Message,
    api: ApiClient,
    *,
    download_url: str,
    filename: str,
    kind: str,
    caption: str,
) -> None:
    safe_name = os.path.basename(filename) or ("file.wav" if kind == "audio" else "file.mp4")

    with tempfile.TemporaryDirectory(prefix="tg_bot_") as td:
        path = os.path.join(td, safe_name)
        api.download_to_file(download_url, path)

        try:
            if kind == "audio":
                message.reply_audio(path, caption=caption)
            elif kind == "video":
                message.reply_video(path, caption=caption, supports_streaming=True)
            else:
                message.reply_document(path, caption=caption)
        except Exception:
            message.reply_document(path, caption=caption)


def _reply_api_error(message: Message, e: ApiError) -> None:
    # Do not claim "no data" for every 404; it can be "file not found".
    detail = (e.detail or "").strip()

    if e.status_code == 404:
        if detail:
            message.reply_text(f"Не найдено: {detail}\nПопробуй {BTN_DAYS} или /days.", reply_markup=main_menu_keyboard())
        else:
            message.reply_text("Не найдено. Попробуй выбрать день через /days.", reply_markup=main_menu_keyboard())
        return

    message.reply_text(
        f"Ошибка API: {detail}" if detail else "Ошибка при обращении к API. Попробуй ещё раз позже.",
        reply_markup=main_menu_keyboard(),
    )
