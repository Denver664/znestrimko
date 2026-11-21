import asyncio
import html
import json
import logging
import os
from collections import deque
from datetime import datetime, timedelta
from pathlib import Path
from typing import Any
from zoneinfo import ZoneInfo

import aiohttp
from aiogram import BaseMiddleware, Bot, Dispatcher, F
from aiogram.client.default import DefaultBotProperties
from aiogram.enums import ParseMode
from aiogram.filters import Command, CommandStart
from aiogram.types import (
    CallbackQuery,
    InlineKeyboardButton,
    InlineKeyboardMarkup,
    KeyboardButton,
    Message,
    ReplyKeyboardMarkup,
)

try:
    from dotenv import load_dotenv
except ImportError:  # pragma: no cover
    load_dotenv = None


BASE_DIR = Path(__file__).resolve().parent
if load_dotenv:
    load_dotenv(BASE_DIR / ".env")

DB_PATH = Path(os.getenv("BOT_DB_PATH", BASE_DIR / "bot.sqlite3"))
API_URL = "https://be-svitlo.oe.if.ua/schedule-by-queue?queue={}"
VALID_QUEUES = {f"{number}.{group}" for number in range(1, 7) for group in range(1, 3)}
POLL_INTERVAL_SECONDS = 60
REMINDER_MINUTES = 10
TIMEOUT_SECONDS = 30
KYIV = ZoneInfo("Europe/Kyiv")

FORCE_REFRESH_REQUESTS: asyncio.Queue[str] = asyncio.Queue()
FORCE_REFRESH_EVENT = asyncio.Event()
BOT_RUNNING = False
LAST_CHECK_AT: str | None = None
LAST_SUCCESS_AT: str | None = None

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(name)s %(message)s")
logger = logging.getLogger(__name__)
ADMIN_LOGS: deque[str] = deque(maxlen=250)


class AdminLogBuffer(logging.Handler):
    def emit(self, record: logging.LogRecord) -> None:
        ADMIN_LOGS.append(self.format(record))


admin_log_handler = AdminLogBuffer(logging.INFO)
admin_log_handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s — %(message)s", "%H:%M:%S"))
logging.getLogger().addHandler(admin_log_handler)


def connect_db():
    import sqlite3

    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    connection = sqlite3.connect(DB_PATH)
    connection.row_factory = sqlite3.Row
    connection.execute(
        "CREATE TABLE IF NOT EXISTS subscriptions (chat_id INTEGER NOT NULL, queue TEXT NOT NULL, "
        "PRIMARY KEY (chat_id, queue))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS known_chats (chat_id INTEGER PRIMARY KEY, first_seen TEXT NOT NULL, last_seen TEXT NOT NULL)"
    )
    known_now = datetime.now(KYIV).isoformat(timespec="seconds")
    connection.execute(
        "INSERT OR IGNORE INTO known_chats(chat_id, first_seen, last_seen) "
        "SELECT DISTINCT chat_id, ?, ? FROM subscriptions",
        (known_now, known_now),
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schedules (queue TEXT NOT NULL, event_date TEXT NOT NULL, "
        "payload TEXT NOT NULL, fetched_at TEXT NOT NULL, PRIMARY KEY (queue, event_date))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS sent_reminders (chat_id INTEGER NOT NULL, queue TEXT NOT NULL, "
        "event_date TEXT NOT NULL, shutdown_hours TEXT NOT NULL, PRIMARY KEY (chat_id, queue, event_date, shutdown_hours))"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS bot_settings (key TEXT PRIMARY KEY, value TEXT NOT NULL)"
    )
    connection.execute(
        "CREATE TABLE IF NOT EXISTS schedule_overrides (queue TEXT NOT NULL, event_date TEXT NOT NULL, "
        "payload TEXT NOT NULL, updated_at TEXT NOT NULL, updated_by INTEGER NOT NULL, "
        "PRIMARY KEY (queue, event_date))"
    )
    connection.execute(
        "INSERT OR IGNORE INTO bot_settings(key, value) VALUES ('notifications_enabled', '1')"
    )
    connection.commit()
    return connection


def normalize_queue(value: str) -> str | None:
    value = value.strip().replace(" ", "")
    if "." not in value:
        value = f"{value}.1"
    return value if value in VALID_QUEUES else None


def admin_user_ids() -> set[int]:
    ids: set[int] = set()
    for value in os.getenv("ADMIN_USER_IDS", "").split(","):
        try:
            ids.add(int(value.strip()))
        except ValueError:
            continue
    return ids


def notifications_enabled() -> bool:
    with connect_db() as db:
        row = db.execute("SELECT value FROM bot_settings WHERE key = 'notifications_enabled'").fetchone()
        return not row or row["value"] == "1"


def set_notifications_enabled(enabled: bool) -> None:
    with connect_db() as db:
        db.execute(
            "INSERT INTO bot_settings(key, value) VALUES ('notifications_enabled', ?) "
            "ON CONFLICT(key) DO UPDATE SET value=excluded.value",
            ("1" if enabled else "0",),
        )
        db.commit()


def get_schedule_override(queue: str, event_date: str) -> dict[str, Any] | None:
    with connect_db() as db:
        row = db.execute(
            "SELECT payload FROM schedule_overrides WHERE queue = ? AND event_date = ?",
            (queue, event_date),
        ).fetchone()
    if not row:
        return None
    try:
        return json.loads(row["payload"])
    except (TypeError, json.JSONDecodeError):
        logger.exception("Не вдалося прочитати ручну зміну графіка: queue=%s date=%s", queue, event_date)
        return None


def save_schedule_override(queue: str, payload: dict[str, Any], admin_id: int) -> None:
    event_date = payload["eventDate"]
    now = datetime.now(KYIV).isoformat(timespec="seconds")
    with connect_db() as db:
        db.execute(
            "INSERT INTO schedule_overrides(queue, event_date, payload, updated_at, updated_by) VALUES (?, ?, ?, ?, ?) "
            "ON CONFLICT(queue, event_date) DO UPDATE SET payload=excluded.payload, "
            "updated_at=excluded.updated_at, updated_by=excluded.updated_by",
            (queue, event_date, json.dumps(payload, ensure_ascii=False), now, admin_id),
        )
        db.commit()
    logger.info("Адміністратор змінив графік у боті: user_id=%s queue=%s date=%s", admin_id, queue, event_date)


def remove_schedule_override(queue: str, event_date: str, admin_id: int) -> None:
    with connect_db() as db:
        db.execute("DELETE FROM schedule_overrides WHERE queue = ? AND event_date = ?", (queue, event_date))
        db.commit()
    logger.info("Адміністратор скасував ручну зміну графіка: user_id=%s queue=%s date=%s", admin_id, queue, event_date)


async def apply_manual_schedule(
    telegram_bot: Bot,
    queue: str,
    event_date: str,
    intervals: list[dict[str, Any]],
    admin_id: int,
    note: str = "",
) -> tuple[int, int, bool]:
    now = datetime.now(KYIV)
    payload: dict[str, Any] = {
        "eventDate": event_date,
        "createdAt": now.isoformat(timespec="minutes"),
        "queues": {queue: intervals},
    }
    if note:
        payload["adminMessage"] = note
    save_schedule_override(queue, payload, admin_id)
    save_and_check_changed(queue, payload)
    recipients = subscribed_chats(queue)
    enabled = notifications_enabled()
    sent = 0
    if enabled:
        text = format_schedule(queue, payload, "Графік змінено адміністратором")
        for chat_id in recipients:
            try:
                await telegram_bot.send_message(chat_id, text)
                sent += 1
            except Exception as exc:
                logger.warning("Не вдалося надіслати змінений графік: chat_id=%s queue=%s error=%s", chat_id, queue, exc)
    else:
        logger.info("Графік змінено, розсилку пропущено через вимкнені сповіщення: queue=%s date=%s", queue, event_date)
    FORCE_REFRESH_EVENT.set()
    logger.info(
        "Ручний графік розіслано: queue=%s date=%s intervals=%s recipients=%s sent=%s note=%s",
        queue, event_date, len(intervals), len(recipients), sent, bool(note),
    )
    return sent, len(recipients), enabled


def known_chat_ids() -> list[int]:
    with connect_db() as db:
        rows = db.execute("SELECT chat_id FROM known_chats ORDER BY chat_id").fetchall()
        return [row["chat_id"] for row in rows]


class ChatTrackingMiddleware(BaseMiddleware):
    async def __call__(self, handler, event, data):
        if isinstance(event, Message):
            now = datetime.now(KYIV).isoformat(timespec="seconds")
            with connect_db() as db:
                db.execute(
                    "INSERT INTO known_chats(chat_id, first_seen, last_seen) VALUES (?, ?, ?) "
                    "ON CONFLICT(chat_id) DO UPDATE SET last_seen=excluded.last_seen",
                    (event.chat.id, now, now),
                )
        return await handler(event, data)


def parse_manual_intervals(value: str) -> list[dict[str, Any]] | None:
    if value.strip().lower() in {"немає", "нема", "0"}:
        return []
    intervals: list[dict[str, Any]] = []
    for part in value.split(","):
        try:
            start_value, end_value = (piece.strip() for piece in part.split("-", maxsplit=1))
            start = datetime.strptime(start_value, "%H:%M").strftime("%H:%M")
            end = datetime.strptime(end_value, "%H:%M").strftime("%H:%M")
        except (ValueError, TypeError):
            return None
        if start == end:
            return None
        intervals.append({"status": 1, "shutdownHours": f"{start}-{end}", "from": start, "to": end})
        if len(intervals) > 12:
            return None
    intervals.sort(key=lambda item: item["from"])
    return intervals or None


def admin_main_keyboard() -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📊 Графіки", callback_data="admin:schedules"),
         InlineKeyboardButton(text="🔔 Сповіщення", callback_data="admin:notifications")],
        [InlineKeyboardButton(text="📢 Оголошення всім", callback_data="admin:announcement"),
         InlineKeyboardButton(text="✏️ Змінити графік", callback_data="admin:edit")],
        [InlineKeyboardButton(text="🔄 Оновити всі графіки", callback_data="admin:refresh:all")],
        [InlineKeyboardButton(text="📋 Останні логи", callback_data="admin:logs")],
    ])


def admin_queue_keyboard() -> InlineKeyboardMarkup:
    queues = sorted(VALID_QUEUES, key=lambda value: (int(value.split(".")[0]), int(value.split(".")[1])))
    rows = [[InlineKeyboardButton(text=queue, callback_data=f"admin:queue:{queue}")
             for queue in queues[index:index + 3]] for index in range(0, len(queues), 3)]
    rows.append([InlineKeyboardButton(text="⬅️ До меню", callback_data="admin:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_edit_queue_keyboard() -> InlineKeyboardMarkup:
    queues = sorted(VALID_QUEUES, key=lambda value: (int(value.split(".")[0]), int(value.split(".")[1])))
    rows = [[InlineKeyboardButton(text=queue, callback_data=f"admin:edit_queue:{queue}")
             for queue in queues[index:index + 3]] for index in range(0, len(queues), 3)]
    rows.append([InlineKeyboardButton(text="⬅️ До меню", callback_data="admin:menu")])
    return InlineKeyboardMarkup(inline_keyboard=rows)


def admin_queue_detail_keyboard(queue: str) -> InlineKeyboardMarkup:
    return InlineKeyboardMarkup(inline_keyboard=[
        [InlineKeyboardButton(text="📅 На завтра", callback_data=f"admin:tomorrow:{queue}"),
         InlineKeyboardButton(text="🔄 Оновити", callback_data=f"admin:refresh:{queue}")],
        [InlineKeyboardButton(text="⬅️ До черг", callback_data="admin:schedules"),
         InlineKeyboardButton(text="🏠 Меню", callback_data="admin:menu")],
    ])


def admin_log_text() -> str:
    lines = [html.escape(line[:240]) for line in list(ADMIN_LOGS)[-14:]]
    if not lines:
        lines = ["Поки що немає подій у журналі."]
    return "📋 <b>Останні події бота</b>\n<blockquote>" + "\n".join(lines) + "</blockquote>"


def parse_date(value: str) -> datetime:
    return datetime.strptime(value, "%d.%m.%Y")


def format_intervals(items: list[dict[str, Any]]) -> str:
    active = [item.get("shutdownHours") for item in items if item.get("status") == 1]
    return ", ".join(active) if active else "без планових відключень"


def today_schedule_status(items: list[dict[str, Any]], event_date: str) -> tuple[str, str]:
    planned = [item for item in items if item.get("status") == 1 and item.get("shutdownHours")]
    if not planned:
        return "🟢 Планових відключень немає", "без планових відключень"
    if event_date != datetime.now(KYIV).strftime("%d.%m.%Y"):
        return "🔴 Є планові відключення", format_intervals(items)

    now = datetime.now(KYIV)
    states: list[str] = []
    all_finished = True
    has_current = False
    for item in planned:
        try:
            start_value, end_value = item["shutdownHours"].split("-", maxsplit=1)
            start = datetime.combine(now.date(), datetime.strptime(start_value, "%H:%M").time(), KYIV)
            end = datetime.combine(now.date(), datetime.strptime(end_value, "%H:%M").time(), KYIV)
            if end <= start:
                end += timedelta(days=1)
            if now < start:
                all_finished = False
                states.append(f"{item['shutdownHours']} — заплановано")
            elif now < end:
                all_finished = False
                has_current = True
                states.append(f"{item['shutdownHours']} — триває")
            else:
                states.append(f"{item['shutdownHours']} — завершено")
        except (KeyError, ValueError):
            all_finished = False
            states.append(str(item.get("shutdownHours")))

    if all_finished:
        return "✅ Відключення на сьогодні вже пройшли", ", ".join(states)
    if has_current:
        return "🔴 Відключення триває", ", ".join(states)
    return "🟠 Є заплановані відключення", ", ".join(states)


def format_schedule(queue: str, payload: dict[str, Any], title: str = "Графік") -> str:
    event_date = payload.get("eventDate", "невідома дата")
    intervals = payload.get("queues", {}).get(queue, [])
    status, _ = today_schedule_status(intervals, event_date)
    lines = [
        f"⚡ <b>{title}</b>",
        "",
        f"🗓 Дата: <b>{html.escape(str(event_date))}</b>",
        f"🚦 Черга: <b>{html.escape(queue)}</b>",
        f"📌 Статус: <b>{html.escape(status)}</b>",
    ]
    outage_quotes = format_outage_quotes(intervals, event_date)
    if outage_quotes:
        lines.extend(["", *outage_quotes])
    else:
        lines.extend(["", "🟢 Запланованих відключень немає"])
    lines.extend([
        "",
        f"📊 Загалом без світла за графіком: <b>{human_duration(scheduled_blackout_seconds(intervals))}</b>",
    ])
    created = payload.get("createdAt")
    approved = payload.get("scheduleApprovedSince")
    if created:
        lines.extend(["", f"🔄 Оновлено: {html.escape(str(created))}"])
    if approved:
        lines.append(f"✅ Затверджено з: {html.escape(str(approved))}")
    admin_message = payload.get("adminMessage")
    if admin_message:
        lines.extend(["", "📢 <b>Повідомлення адміністратора</b>", html.escape(str(admin_message))])
    return "\n".join(lines)


def format_outage_quotes(items: list[dict[str, Any]], event_date: str) -> list[str]:
    """Formats every planned outage as its own short Telegram quote."""
    planned = [item for item in items if item.get("status") == 1 and item.get("shutdownHours")]
    now = datetime.now(KYIV)
    quotes = []
    for item in planned:
        hours = str(item["shutdownHours"])
        state = "за графіком"
        duration_text = ""
        try:
            start_value, end_value = hours.split("-", maxsplit=1)
            start_time = datetime.strptime(start_value, "%H:%M").time()
            end_time = datetime.strptime(end_value, "%H:%M").time()
            start = datetime.combine(now.date(), start_time, KYIV)
            end = datetime.combine(now.date(), end_time, KYIV)
            if end <= start:
                if event_date == now.strftime("%d.%m.%Y") and now < end:
                    start -= timedelta(days=1)
                else:
                    end += timedelta(days=1)
            duration_text = human_duration(int((end - start).total_seconds()))
            if event_date == now.strftime("%d.%m.%Y"):
                if now < start:
                    state = "заплановано"
                elif now < end:
                    state = "триває"
                else:
                    state = "завершено"
        except ValueError:
            pass

        display_hours = html.escape(hours.replace("-", "–"))
        details = f" · {duration_text}" if duration_text else ""
        quotes.append(f"<blockquote>⚡ <b>{display_hours}</b>{details} · {state}</blockquote>")
    return quotes


def human_duration(seconds: int) -> str:
    minutes = max(0, seconds) // 60
    hours, minutes = divmod(minutes, 60)
    if hours and minutes:
        return f"{hours} год {minutes} хв"
    if hours:
        return f"{hours} год"
    return f"{minutes} хв"


def scheduled_blackout_seconds(items: list[dict[str, Any]]) -> int:
    """Сумарна тривалість усіх запланованих відключень у секундах."""
    total = 0
    for item in items:
        if item.get("status") != 1 or not item.get("shutdownHours"):
            continue
        try:
            start_value, end_value = item["shutdownHours"].split("-", maxsplit=1)
            start = datetime.strptime(start_value, "%H:%M")
            end = datetime.strptime(end_value, "%H:%M")
            duration = (end - start).total_seconds()
            if duration <= 0:
                duration += 24 * 60 * 60
            total += int(duration)
        except (KeyError, ValueError):
            continue
    return total


def claim_reminder(chat_id: int, queue: str, event_date: str, hours: str) -> bool:
    """Позначає нагадування надісланим; не дублює його після перезапуску."""
    with connect_db() as db:
        cursor = db.execute(
            "INSERT OR IGNORE INTO sent_reminders(chat_id, queue, event_date, shutdown_hours) VALUES (?, ?, ?, ?)",
            (chat_id, queue, event_date, hours),
        )
        return cursor.rowcount == 1


async def send_upcoming_reminders(bot: Bot, queue: str, payload: dict[str, Any]) -> None:
    if not notifications_enabled():
        return
    event_date = payload.get("eventDate", "")
    if event_date != datetime.now(KYIV).strftime("%d.%m.%Y"):
        return
    now = datetime.now(KYIV)
    for item in payload.get("queues", {}).get(queue, []):
        hours = item.get("shutdownHours")
        if item.get("status") != 1 or not hours:
            continue
        try:
            start_value, _ = hours.split("-", maxsplit=1)
            start = datetime.combine(now.date(), datetime.strptime(start_value, "%H:%M").time(), KYIV)
        except (ValueError, TypeError):
            continue
        remaining = (start - now).total_seconds()
        # A one-minute polling loop with a 10–11 minute window catches each event once.
        if not (REMINDER_MINUTES * 60 <= remaining < (REMINDER_MINUTES + 1) * 60):
            continue
        for chat_id in subscribed_chats(queue):
            with connect_db() as db:
                already_sent = db.execute(
                    "SELECT 1 FROM sent_reminders WHERE chat_id = ? AND queue = ? AND event_date = ? AND shutdown_hours = ?",
                    (chat_id, queue, event_date, hours),
                ).fetchone()
            if already_sent:
                continue
            try:
                await bot.send_message(
                    chat_id,
                    f"🔔 <b>Нагадування</b>\nЧерга {queue}: за 10 хвилин вимкнуть світло.\nПеріод: <b>{hours}</b>",
                )
                claim_reminder(chat_id, queue, event_date, hours)
                logger.info("Надіслано нагадування: chat_id=%s queue=%s date=%s period=%s", chat_id, queue, event_date, hours)
            except Exception as exc:
                logger.warning("Не вдалося надіслати нагадування chat_id=%s: %s", chat_id, exc)


def countdown_text(queue: str, payload: dict[str, Any]) -> str:
    today = datetime.now(KYIV).date()
    event_date = parse_date(payload.get("eventDate", "01.01.1900")).date()
    if event_date != today:
        return f"⚡ <b>Черга {queue}</b>\n\nℹ️ На сьогодні графік ще не опубліковано."

    now = datetime.now(KYIV)
    intervals = []
    for item in payload.get("queues", {}).get(queue, []):
        if item.get("status") != 1:
            continue
        try:
            start_value, end_value = item["shutdownHours"].split("-", maxsplit=1)
            start = datetime.combine(today, datetime.strptime(start_value, "%H:%M").time(), KYIV)
            end = datetime.combine(today, datetime.strptime(end_value, "%H:%M").time(), KYIV)
            if end <= start:
                end += timedelta(days=1)
            intervals.append((start, end, item["shutdownHours"]))
        except (KeyError, ValueError):
            continue

    intervals.sort(key=lambda item: item[0])
    total_seconds = scheduled_blackout_seconds(payload.get("queues", {}).get(queue, []))
    total_line = f"\n📊 Загалом без світла за графіком: <b>{human_duration(total_seconds)}</b>"
    for start, end, hours in intervals:
        if now < start:
            return (f"⚡ <b>Черга {queue}</b>\n\n🟢 Світло зараз є\n"
                    f"⏳ До відключення: <b>{human_duration(int((start - now).total_seconds()))}</b>\n"
                    f"🕒 Період: {hours}{total_line}")
        if start <= now < end:
            return (f"⚡ <b>Черга {queue}</b>\n\n🔴 Світла зараз немає\n"
                    f"⏳ До включення: <b>{human_duration(int((end - now).total_seconds()))}</b>\n"
                    f"🕒 Поточне відключення: {hours}{total_line}")
    return f"⚡ <b>Черга {queue}</b>\n\n🟢 Світло зараз є\n📌 Сьогодні запланованих відключень більше немає.{total_line}"


async def fetch_schedule(session: aiohttp.ClientSession, queue: str) -> dict[str, Any] | None:
    today_text = datetime.now(KYIV).strftime("%d.%m.%Y")
    tomorrow_text = (datetime.now(KYIV) + timedelta(days=1)).strftime("%d.%m.%Y")
    if override := get_schedule_override(queue, tomorrow_text):
        return override
    if override := get_schedule_override(queue, today_text):
        return override
    try:
        async with session.get(API_URL.format(queue), timeout=TIMEOUT_SECONDS) as response:
            response.raise_for_status()
            data = await response.json(content_type=None)
        if not isinstance(data, list) or not data:
            return None
        candidates = [item for item in data if isinstance(item, dict) and item.get("eventDate")]
        if not candidates:
            return None
        tomorrow = (datetime.now(KYIV) + timedelta(days=1)).date()
        dated = [(parse_date(item["eventDate"]).date(), item) for item in candidates]
        exact_tomorrow = [item for date, item in dated if date == tomorrow]
        if exact_tomorrow:
            payload = exact_tomorrow[-1]
            return get_schedule_override(queue, payload["eventDate"]) or payload
        future = [item for date, item in dated if date > tomorrow]
        if future:
            payload = min(future, key=lambda item: parse_date(item["eventDate"]))
            return get_schedule_override(queue, payload["eventDate"]) or payload
        payload = max(candidates, key=lambda item: parse_date(item["eventDate"]))
        return get_schedule_override(queue, payload["eventDate"]) or payload
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
        logger.warning("Не вдалося отримати чергу %s: %s", queue, exc)
        return None


async def fetch_today_schedule(session: aiohttp.ClientSession, queue: str) -> dict[str, Any] | None:
    today_text = datetime.now(KYIV).strftime("%d.%m.%Y")
    if override := get_schedule_override(queue, today_text):
        return override
    try:
        async with session.get(API_URL.format(queue), timeout=TIMEOUT_SECONDS) as response:
            response.raise_for_status()
            data = await response.json(content_type=None)
        today = datetime.now(KYIV).date()
        for item in data if isinstance(data, list) else []:
            if isinstance(item, dict) and item.get("eventDate") and parse_date(item["eventDate"]).date() == today:
                return item
        return None
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
        logger.warning("Не вдалося отримати сьогоднішню чергу %s: %s", queue, exc)
        return None


async def fetch_tomorrow_schedule(session: aiohttp.ClientSession, queue: str) -> dict[str, Any] | None:
    tomorrow_text = (datetime.now(KYIV) + timedelta(days=1)).strftime("%d.%m.%Y")
    if override := get_schedule_override(queue, tomorrow_text):
        return override
    try:
        async with session.get(API_URL.format(queue), timeout=TIMEOUT_SECONDS) as response:
            response.raise_for_status()
            data = await response.json(content_type=None)
        tomorrow = (datetime.now(KYIV) + timedelta(days=1)).date()
        for item in data if isinstance(data, list) else []:
            if isinstance(item, dict) and item.get("eventDate") and parse_date(item["eventDate"]).date() == tomorrow:
                return item
        return None
    except (aiohttp.ClientError, asyncio.TimeoutError, ValueError, TypeError) as exc:
        logger.warning("Не вдалося отримати завтрашню чергу %s: %s", queue, exc)
        return None


def save_and_check_changed(queue: str, payload: dict[str, Any]) -> bool:
    event_date = payload["eventDate"]
    serialized = json.dumps(payload, ensure_ascii=False, sort_keys=True)
    now = datetime.now(KYIV).isoformat(timespec="seconds")
    with connect_db() as db:
        old = db.execute(
            "SELECT payload FROM schedules WHERE queue = ? AND event_date = ?", (queue, event_date)
        ).fetchone()
        db.execute(
            "INSERT INTO schedules(queue, event_date, payload, fetched_at) VALUES (?, ?, ?, ?) "
            "ON CONFLICT(queue, event_date) DO UPDATE SET payload=excluded.payload, fetched_at=excluded.fetched_at",
            (queue, event_date, serialized, now),
        )
        if old is None:
            logger.info("Графік додано до локального кешу: queue=%s date=%s", queue, event_date)
            return False
        try:
            previous_payload = json.loads(old["payload"])
        except (TypeError, json.JSONDecodeError):
            return False
        changed = schedule_signature(previous_payload, queue) != schedule_signature(payload, queue)
        if changed:
            logger.info("Змінився графік: queue=%s date=%s", queue, event_date)
        return changed


def schedule_signature(payload: dict[str, Any], queue: str) -> list[tuple[str, str, str, int]]:
    """Повертає тільки дані про години конкретної черги, без дат і службових полів."""
    intervals = payload.get("queues", {}).get(queue, [])
    signature = []
    for item in intervals:
        signature.append(
            (
                str(item.get("shutdownHours", "")),
                str(item.get("from", "")),
                str(item.get("to", "")),
                int(item.get("status", 0)),
            )
        )
    return sorted(signature)


def subscribed_chats(queue: str) -> list[int]:
    with connect_db() as db:
        rows = db.execute("SELECT chat_id FROM subscriptions WHERE queue = ?", (queue,)).fetchall()
        return [row["chat_id"] for row in rows]


def queue_subscriptions(chat_id: int) -> list[str]:
    with connect_db() as db:
        rows = db.execute("SELECT queue FROM subscriptions WHERE chat_id = ? ORDER BY queue", (chat_id,)).fetchall()
        return [row["queue"] for row in rows]


def add_subscription(chat_id: int, queue: str) -> None:
    with connect_db() as db:
        db.execute("INSERT OR IGNORE INTO subscriptions(chat_id, queue) VALUES (?, ?)", (chat_id, queue))
        db.commit()


def remove_subscription(chat_id: int, queue: str) -> None:
    with connect_db() as db:
        db.execute("DELETE FROM subscriptions WHERE chat_id = ? AND queue = ?", (chat_id, queue))
        db.commit()


def main_keyboard() -> ReplyKeyboardMarkup:
    return ReplyKeyboardMarkup(
        keyboard=[
            [KeyboardButton(text="➕ Підписатися"), KeyboardButton(text="➖ Відписатися")],
            [KeyboardButton(text="📋 Мої черги"), KeyboardButton(text="📅 Графік зараз")],
            [KeyboardButton(text="📅 Графік на завтра")],
            [KeyboardButton(text="⏱ До зміни світла")],
            [KeyboardButton(text="ℹ️ Допомога")],
        ],
        resize_keyboard=True,
        is_persistent=True,
    )


def queue_keyboard() -> ReplyKeyboardMarkup:
    queues = sorted(VALID_QUEUES, key=lambda value: (int(value.split(".")[0]), value))
    rows = [[KeyboardButton(text=queue) for queue in queues[index:index + 3]] for index in range(0, len(queues), 3)]
    rows.append([KeyboardButton(text="⬅️ Назад")])
    return ReplyKeyboardMarkup(keyboard=rows, resize_keyboard=True, is_persistent=True)


async def get_queue_arg(message: Message) -> str | None:
    parts = (message.text or "").split(maxsplit=1)
    if len(parts) != 2:
        await message.answer("Вкажіть чергу, наприклад: <code>/subscribe 5.2</code>")
        return None
    queue = normalize_queue(parts[1])
    if not queue:
        await message.answer("Черга має бути у форматі X.G, де X — 1–6, G — 1–2. Наприклад: <code>5.2</code>")
    return queue


async def send_schedule(message: Message, queue: str, session: aiohttp.ClientSession, title: str = "Графік") -> None:
    payload = await fetch_schedule(session, queue)
    if payload is None:
        await message.answer("Не вдалося отримати графік. Спробуйте трохи пізніше.")
        return
    await message.answer(format_schedule(queue, payload, title))


async def send_countdown(message: Message, queue: str, session: aiohttp.ClientSession) -> None:
    payload = await fetch_today_schedule(session, queue)
    if payload is None:
        await message.answer(
            f"<b>Черга {queue}</b>\nНе вдалося отримати сьогоднішній графік.",
            reply_markup=main_keyboard(),
        )
        return
    await message.answer(countdown_text(queue, payload), reply_markup=main_keyboard())


async def send_today_schedule(message: Message, queue: str, session: aiohttp.ClientSession) -> None:
    payload = await fetch_today_schedule(session, queue)
    if payload is None:
        await message.answer(
            f"⚡ <b>Черга {queue}</b>\n\nℹ️ Сьогоднішній графік ще не опубліковано.",
            reply_markup=main_keyboard(),
        )
        return
    await message.answer(format_schedule(queue, payload, "Графік на сьогодні"), reply_markup=main_keyboard())


async def send_tomorrow_schedule(message: Message, queue: str, session: aiohttp.ClientSession) -> None:
    payload = await fetch_tomorrow_schedule(session, queue)
    if payload is None:
        await message.answer(
            "📅 <b>Графіку на завтра ще немає.</b>\n\n☀️ Насолоджуйтесь світлом!",
            reply_markup=main_keyboard(),
        )
        return
    await message.answer(format_schedule(queue, payload, "Графік на завтра"), reply_markup=main_keyboard())


async def main() -> None:
    global BOT_RUNNING, LAST_CHECK_AT, LAST_SUCCESS_AT
    token = os.getenv("BOT_TOKEN") or os.getenv("TELEGRAM_BOT_TOKEN")
    if not token:
        raise RuntimeError("Не задано BOT_TOKEN у файлі .env")

    connect_db().close()
    bot = Bot(token, default=DefaultBotProperties(parse_mode=ParseMode.HTML))
    dispatcher = Dispatcher()
    dispatcher.message.middleware(ChatTrackingMiddleware())
    pending_action: dict[int, str] = {}
    admin_pending_actions: dict[int, dict[str, Any]] = {}
    BOT_RUNNING = True
    logger.info("Telegram-бот запускається; черг моніторингу: %s", len(VALID_QUEUES))

    @dispatcher.message(Command("myid"))
    async def my_id(message: Message) -> None:
        if message.from_user:
            await message.answer(f"Ваш Telegram ID: <code>{message.from_user.id}</code>")

    @dispatcher.message(Command("admin"))
    async def admin_command(message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if user_id not in admin_user_ids():
            logger.warning("Відмовлено в доступі до /admin: user_id=%s", user_id)
            await message.answer("⛔ Доступ до меню адміністратора закритий.")
            return
        admin_pending_actions.pop(user_id, None)
        logger.info("Адміністратор відкрив меню: user_id=%s", user_id)
        await message.answer("⚙️ <b>Панель керування ботом</b>\nОберіть дію:", reply_markup=admin_main_keyboard())

    @dispatcher.callback_query(F.data.startswith("admin:"))
    async def admin_callback(callback: CallbackQuery) -> None:
        user_id = callback.from_user.id if callback.from_user else None
        if user_id not in admin_user_ids():
            logger.warning("Відмовлено в callback адмінки: user_id=%s", user_id)
            await callback.answer("⛔ Немає доступу", show_alert=True)
            return
        await callback.answer()
        if not callback.message:
            return

        data = callback.data or ""
        if data == "admin:menu":
            admin_pending_actions.pop(user_id, None)
            await callback.message.edit_text("⚙️ <b>Панель керування ботом</b>\nОберіть дію:", reply_markup=admin_main_keyboard())
            return
        if data == "admin:cancel_input":
            admin_pending_actions.pop(user_id, None)
            await callback.message.edit_text("⚙️ <b>Панель керування ботом</b>\nДію скасовано.", reply_markup=admin_main_keyboard())
            return
        if data == "admin:announcement":
            admin_pending_actions[user_id] = {"type": "announcement"}
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Скасувати", callback_data="admin:cancel_input")],
            ])
            await callback.message.edit_text(
                "📢 <b>Оголошення всім користувачам бота</b>\n\nНадішліть наступним повідомленням текст. Його отримають усі чати, які вже писали боту.",
                reply_markup=keyboard,
            )
            return
        if data == "admin:edit":
            admin_pending_actions.pop(user_id, None)
            await callback.message.edit_text("✏️ <b>Зміна графіка в боті</b>\nОберіть чергу:", reply_markup=admin_edit_queue_keyboard())
            return
        if data == "admin:edit_skip_note":
            state = admin_pending_actions.get(user_id, {})
            if state.get("type") != "edit_schedule_note":
                await callback.message.edit_text("Дія завершилася. Відкрийте /admin ще раз.", reply_markup=admin_main_keyboard())
                return
            admin_pending_actions.pop(user_id, None)
            queue = str(state["queue"])
            day_key = str(state["day"])
            event_date = (datetime.now(KYIV) + (timedelta(days=1) if day_key == "tomorrow" else timedelta())).strftime("%d.%m.%Y")
            sent, recipients, enabled = await apply_manual_schedule(
                bot, queue, event_date, state["intervals"], user_id,
            )
            notice = f"Повідомлено підписників: {sent} з {recipients}." if enabled else "Сповіщення вимкнені, розсилки не було."
            await callback.message.edit_text(
                f"✅ Графік для черги <b>{queue}</b> на <b>{event_date}</b> збережено без додаткового повідомлення. {notice}",
                reply_markup=admin_main_keyboard(),
            )
            return
        if data.startswith("admin:edit_queue:"):
            queue = normalize_queue(data.split(":", maxsplit=2)[2])
            if not queue:
                await callback.message.edit_text("Невідома черга.", reply_markup=admin_edit_queue_keyboard())
                return
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Сьогодні", callback_data=f"admin:edit_date:today:{queue}"),
                 InlineKeyboardButton(text="Завтра", callback_data=f"admin:edit_date:tomorrow:{queue}")],
                [InlineKeyboardButton(text="⬅️ До черг", callback_data="admin:edit"),
                 InlineKeyboardButton(text="🏠 Меню", callback_data="admin:menu")],
            ])
            await callback.message.edit_text(f"✏️ <b>Черга {queue}</b>\nНа який день змінити графік?", reply_markup=keyboard)
            return
        if data.startswith("admin:edit_date:"):
            _, _, day_key, queue_value = data.split(":", maxsplit=3)
            queue = normalize_queue(queue_value)
            if not queue or day_key not in {"today", "tomorrow"}:
                await callback.message.edit_text("Некоректна черга або дата.", reply_markup=admin_edit_queue_keyboard())
                return
            day_label = "сьогодні" if day_key == "today" else "завтра"
            admin_pending_actions[user_id] = {"type": "edit_schedule", "queue": queue, "day": day_key}
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="Скасувати", callback_data="admin:cancel_input")],
            ])
            await callback.message.edit_text(
                f"✏️ <b>Новий графік · {queue} · {day_label}</b>\n\nНадішліть періоди через кому, наприклад:\n<code>12:00-15:30, 19:00-21:00</code>\n\nЩоб указати відсутність відключень, напишіть <code>немає</code>. Щоб повернути графік джерела, напишіть <code>офіційний</code>.",
                reply_markup=keyboard,
            )
            return
        if data == "admin:schedules":
            await callback.message.edit_text("📊 <b>Графіки</b>\nОберіть чергу:", reply_markup=admin_queue_keyboard())
            return
        if data.startswith("admin:queue:"):
            queue = normalize_queue(data.split(":", maxsplit=2)[2])
            if not queue:
                await callback.message.edit_text("Невідома черга.", reply_markup=admin_queue_keyboard())
                return
            async with aiohttp.ClientSession() as session:
                payload = await fetch_today_schedule(session, queue)
            logger.info("Адміністратор переглянув графік: user_id=%s queue=%s", user_id, queue)
            text = format_schedule(queue, payload, "Графік на сьогодні") if payload else (
                f"📊 <b>Черга {queue}</b>\n\nНа сьогодні графік ще не опублікований."
            )
            await callback.message.edit_text(text, reply_markup=admin_queue_detail_keyboard(queue))
            return
        if data.startswith("admin:tomorrow:"):
            queue = normalize_queue(data.split(":", maxsplit=2)[2])
            if not queue:
                await callback.message.edit_text("Невідома черга.", reply_markup=admin_queue_keyboard())
                return
            async with aiohttp.ClientSession() as session:
                payload = await fetch_tomorrow_schedule(session, queue)
            text = format_schedule(queue, payload, "Графік на завтра") if payload else (
                f"📅 <b>Черга {queue}</b>\n\nГрафіку на завтра ще немає."
            )
            await callback.message.edit_text(text, reply_markup=admin_queue_detail_keyboard(queue))
            return
        if data.startswith("admin:refresh:"):
            requested = data.split(":", maxsplit=2)[2]
            queue = "*" if requested == "all" else normalize_queue(requested)
            if not queue:
                await callback.message.edit_text("Невідома черга.", reply_markup=admin_main_keyboard())
                return
            FORCE_REFRESH_REQUESTS.put_nowait(queue)
            FORCE_REFRESH_EVENT.set()
            logger.info("Адміністратор запросив примусове оновлення: user_id=%s queue=%s", user_id, "усі" if queue == "*" else queue)
            label = "усіх черг" if queue == "*" else f"черги {queue}"
            delivery = "Графік буде надіслано підписникам." if notifications_enabled() else "Сповіщення вимкнені — графік оновиться без розсилки."
            await callback.message.edit_text(
                f"🔄 Примусове оновлення {label} поставлено в чергу. Результат з’явиться в логах. {delivery}",
                reply_markup=admin_main_keyboard(),
            )
            return
        if data == "admin:notifications":
            enabled = notifications_enabled()
            button = InlineKeyboardButton(
                text="🔕 Вимкнути сповіщення" if enabled else "🔔 Увімкнути сповіщення",
                callback_data="admin:notifications:toggle",
            )
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [button], [InlineKeyboardButton(text="⬅️ До меню", callback_data="admin:menu")],
            ])
            state = "увімкнені" if enabled else "вимкнені"
            await callback.message.edit_text(
                f"🔔 <b>Сповіщення {state}</b>\n\nКерування впливає на нагадування за 10 хвилин і повідомлення про зміну графіка для підписників.",
                reply_markup=keyboard,
            )
            return
        if data == "admin:notifications:toggle":
            enabled = not notifications_enabled()
            set_notifications_enabled(enabled)
            logger.info("Адміністратор %s сповіщення: enabled=%s", "увімкнув" if enabled else "вимкнув", enabled)
            state = "увімкнені" if enabled else "вимкнені"
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="🔕 Вимкнути" if enabled else "🔔 Увімкнути", callback_data="admin:notifications:toggle")],
                [InlineKeyboardButton(text="⬅️ До меню", callback_data="admin:menu")],
            ])
            await callback.message.edit_text(f"🔔 <b>Сповіщення {state}</b>", reply_markup=keyboard)
            return
        if data in {"admin:logs", "admin:logs:refresh"}:
            logger.info("Адміністратор переглянув журнал: user_id=%s", user_id)
            keyboard = InlineKeyboardMarkup(inline_keyboard=[
                [InlineKeyboardButton(text="↻ Оновити журнал", callback_data="admin:logs:refresh")],
                [InlineKeyboardButton(text="⬅️ До меню", callback_data="admin:menu")],
            ])
            await callback.message.edit_text(admin_log_text(), reply_markup=keyboard)
            return
        await callback.message.edit_text("Невідома дія.", reply_markup=admin_main_keyboard())

    @dispatcher.message(CommandStart())
    async def start(message: Message) -> None:
        await message.answer(
            "Привіт! Я стежу за графіками відключень.\n\n"
            "Користуйтеся кнопками внизу екрана. Також доступні команди "
            "<code>/subscribe 5.2</code>, <code>/unsubscribe 5.2</code>, "
            "<code>/tomorrow 5.2</code> і <code>/subscriptions</code>.\n"
            "Для адміністратора: <code>/admin</code>; дізнатися свій ID: <code>/myid</code>.",
            reply_markup=main_keyboard(),
        )

    @dispatcher.message(Command("help"))
    async def help_command(message: Message) -> None:
        await start(message)

    @dispatcher.message(Command("subscribe"))
    async def subscribe(message: Message) -> None:
        queue = await get_queue_arg(message)
        if not queue:
            return
        add_subscription(message.chat.id, queue)
        logger.info("Нова підписка: chat_id=%s queue=%s", message.chat.id, queue)
        await message.answer(f"✅ Підписку на чергу <b>{queue}</b> увімкнено.\n🔄 Перевіряю оновлення щохвилини.")
        async with aiohttp.ClientSession() as session:
            await send_schedule(message, queue, session, "Поточний графік")

    @dispatcher.message(F.text == "➕ Підписатися")
    async def subscribe_button(message: Message) -> None:
        pending_action[message.chat.id] = "subscribe"
        await message.answer("Оберіть чергу:", reply_markup=queue_keyboard())

    @dispatcher.message(Command("unsubscribe"))
    async def unsubscribe(message: Message) -> None:
        queue = await get_queue_arg(message)
        if not queue:
            return
        remove_subscription(message.chat.id, queue)
        logger.info("Підписку видалено: chat_id=%s queue=%s", message.chat.id, queue)
        await message.answer(f"🔕 Підписку на чергу <b>{queue}</b> вимкнено.")

    @dispatcher.message(F.text == "➖ Відписатися")
    async def unsubscribe_button(message: Message) -> None:
        queues = queue_subscriptions(message.chat.id)
        if not queues:
            await message.answer("У вас немає активних підписок.", reply_markup=main_keyboard())
            return
        pending_action[message.chat.id] = "unsubscribe"
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=queue) for queue in queues], [KeyboardButton(text="⬅️ Назад")]],
            resize_keyboard=True,
            is_persistent=True,
        )
        await message.answer("Оберіть чергу для відписки:", reply_markup=keyboard)

    @dispatcher.message(Command("subscriptions"))
    async def subscriptions(message: Message) -> None:
        queues = queue_subscriptions(message.chat.id)
        await message.answer(
            "Ваші підписки: " + (", ".join(f"<b>{q}</b>" for q in queues) if queues else "немає"),
            reply_markup=main_keyboard(),
        )

    @dispatcher.message(F.text == "📋 Мої черги")
    async def subscriptions_button(message: Message) -> None:
        await subscriptions(message)

    @dispatcher.message(Command("tomorrow"))
    async def tomorrow(message: Message) -> None:
        queue = await get_queue_arg(message)
        if not queue:
            return
        async with aiohttp.ClientSession() as session:
            await send_tomorrow_schedule(message, queue, session)

    @dispatcher.message(F.text == "📅 Графік на завтра")
    async def tomorrow_button(message: Message) -> None:
        queues = queue_subscriptions(message.chat.id)
        if not queues:
            await message.answer("Спочатку підпишіться хоча б на одну чергу.", reply_markup=main_keyboard())
            return
        if len(queues) == 1:
            async with aiohttp.ClientSession() as session:
                await send_tomorrow_schedule(message, queues[0], session)
            return
        pending_action[message.chat.id] = "tomorrow"
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=queue) for queue in queues], [KeyboardButton(text="⬅️ Назад")]],
            resize_keyboard=True,
            is_persistent=True,
        )
        await message.answer("Оберіть чергу:", reply_markup=keyboard)

    @dispatcher.message(F.text == "📅 Графік зараз")
    async def today_button(message: Message) -> None:
        queues = queue_subscriptions(message.chat.id)
        if not queues:
            await message.answer("Спочатку підпишіться хоча б на одну чергу.", reply_markup=main_keyboard())
            return
        if len(queues) == 1:
            async with aiohttp.ClientSession() as session:
                await send_today_schedule(message, queues[0], session)
            return
        pending_action[message.chat.id] = "today"
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=queue) for queue in queues], [KeyboardButton(text="⬅️ Назад")]],
            resize_keyboard=True,
            is_persistent=True,
        )
        await message.answer("Оберіть чергу:", reply_markup=keyboard)

    @dispatcher.message(F.text == "⏱ До зміни світла")
    async def countdown_button(message: Message) -> None:
        queues = queue_subscriptions(message.chat.id)
        if not queues:
            await message.answer("Спочатку підпишіться хоча б на одну чергу.", reply_markup=main_keyboard())
            return
        if len(queues) == 1:
            async with aiohttp.ClientSession() as session:
                await send_countdown(message, queues[0], session)
            return
        pending_action[message.chat.id] = "countdown"
        keyboard = ReplyKeyboardMarkup(
            keyboard=[[KeyboardButton(text=queue) for queue in queues], [KeyboardButton(text="⬅️ Назад")]],
            resize_keyboard=True,
            is_persistent=True,
        )
        await message.answer("Оберіть чергу:", reply_markup=keyboard)

    @dispatcher.message(F.text == "ℹ️ Допомога")
    async def help_button(message: Message) -> None:
        await help_command(message)

    @dispatcher.message(F.text == "⬅️ Назад")
    async def back_button(message: Message) -> None:
        pending_action.pop(message.chat.id, None)
        await message.answer("Головне меню.", reply_markup=main_keyboard())

    async def handle_admin_text(message: Message, user_id: int, state: dict[str, Any]) -> None:
        text = (message.text or "").strip()
        action_type = state.get("type")
        if text.lower() in {"/cancel", "скасувати", "відміна"}:
            admin_pending_actions.pop(user_id, None)
            await message.answer("Дію скасовано.", reply_markup=admin_main_keyboard())
            return

        if action_type == "announcement":
            if not text:
                await message.answer("Надішліть текст оголошення або натисніть «Скасувати».")
                return
            if len(text) > 3500:
                await message.answer("Оголошення задовге. Скоротіть його до 3500 символів.")
                return
            admin_pending_actions.pop(user_id, None)
            recipients = known_chat_ids()
            sent = 0
            failed = 0
            for chat_id in recipients:
                try:
                    await bot.send_message(
                        chat_id,
                        f"📢 <b>Оголошення</b>\n\n<blockquote>{html.escape(text)}</blockquote>",
                    )
                    sent += 1
                    if sent % 20 == 0:
                        await asyncio.sleep(1)
                except Exception as exc:
                    failed += 1
                    logger.warning("Не вдалося доставити оголошення: chat_id=%s error=%s", chat_id, exc)
            logger.info("Масове оголошення завершено: admin_id=%s recipients=%s sent=%s failed=%s", user_id, len(recipients), sent, failed)
            await message.answer(
                f"📢 Розсилку завершено. Доставлено: <b>{sent}</b> із {len(recipients)} чатів; помилок: {failed}.",
                reply_markup=admin_main_keyboard(),
            )
            return

        if action_type == "edit_schedule_note":
            if not text:
                await message.answer("Надішліть текст повідомлення або натисніть «Без повідомлення».")
                return
            if len(text) > 1500:
                await message.answer("Повідомлення задовге. Скоротіть його до 1500 символів або пропустіть.")
                return
            queue = str(state["queue"])
            day_key = str(state["day"])
            event_date = (datetime.now(KYIV) + (timedelta(days=1) if day_key == "tomorrow" else timedelta())).strftime("%d.%m.%Y")
            sent, recipients, enabled = await apply_manual_schedule(
                bot, queue, event_date, state["intervals"], user_id, note=text,
            )
            admin_pending_actions.pop(user_id, None)
            delivery = f"Повідомлено підписників: {sent} з {recipients}." if enabled else "Сповіщення вимкнені — розсилки не було."
            await message.answer(
                f"✅ Графік для черги <b>{queue}</b> на <b>{event_date}</b> збережено разом із повідомленням. {delivery}",
                reply_markup=admin_main_keyboard(),
            )
            return

        if action_type != "edit_schedule":
            admin_pending_actions.pop(user_id, None)
            await message.answer("Не вдалося визначити дію. Відкрийте /admin ще раз.", reply_markup=admin_main_keyboard())
            return

        queue = str(state["queue"])
        day_key = str(state["day"])
        now = datetime.now(KYIV)
        event_date = (now + (timedelta(days=1) if day_key == "tomorrow" else timedelta())).strftime("%d.%m.%Y")
        if text.lower() in {"офіційний", "скинути", "reset"}:
            admin_pending_actions.pop(user_id, None)
            remove_schedule_override(queue, event_date, user_id)
            async with aiohttp.ClientSession() as session:
                official = await (fetch_today_schedule(session, queue) if day_key == "today"
                                 else fetch_tomorrow_schedule(session, queue))
            sent = 0
            if official:
                save_and_check_changed(queue, official)
                if notifications_enabled():
                    for chat_id in subscribed_chats(queue):
                        try:
                            await bot.send_message(chat_id, format_schedule(queue, official, "Графік повернуто до джерела"))
                            sent += 1
                        except Exception as exc:
                            logger.warning("Не вдалося повідомити про повернення графіка: chat_id=%s queue=%s error=%s", chat_id, queue, exc)
            FORCE_REFRESH_EVENT.set()
            if official:
                await message.answer(
                    f"✅ Ручну зміну скасовано. Отримано офіційний графік; повідомлено підписників: {sent}.",
                    reply_markup=admin_main_keyboard(),
                )
            else:
                await message.answer(
                    "✅ Ручну зміну скасовано, але офіційний графік зараз недоступний. Бот підхопить його під час наступної перевірки.",
                    reply_markup=admin_main_keyboard(),
                )
            return

        intervals = parse_manual_intervals(text)
        if intervals is None:
            await message.answer(
                "Не розпізнав періоди. Формат: <code>12:00-15:30, 19:00-21:00</code>, або напишіть <code>немає</code> чи <code>офіційний</code>.",
            )
            return
        admin_pending_actions[user_id] = {
            "type": "edit_schedule_note", "queue": queue, "day": day_key, "intervals": intervals,
        }
        keyboard = InlineKeyboardMarkup(inline_keyboard=[
            [InlineKeyboardButton(text="Без повідомлення", callback_data="admin:edit_skip_note")],
            [InlineKeyboardButton(text="Скасувати", callback_data="admin:cancel_input")],
        ])
        await message.answer(
            f"✅ Періоди прийнято. Тепер надішліть коротке повідомлення, яке додати до графіка <b>{queue}</b> на <b>{event_date}</b>, або пропустіть цей крок.",
            reply_markup=keyboard,
        )

    @dispatcher.message(F.text)
    async def queue_choice(message: Message) -> None:
        user_id = message.from_user.id if message.from_user else None
        if user_id is not None and user_id in admin_pending_actions and user_id in admin_user_ids():
            await handle_admin_text(message, user_id, admin_pending_actions[user_id])
            return
        action = pending_action.get(message.chat.id)
        queue = normalize_queue(message.text or "")
        if not action or not queue:
            return
        pending_action.pop(message.chat.id, None)
        if action == "subscribe":
            add_subscription(message.chat.id, queue)
            logger.info("Нова підписка через меню: chat_id=%s queue=%s", message.chat.id, queue)
            await message.answer(
                f"✅ Підписку на чергу <b>{queue}</b> увімкнено.\n🔄 Перевіряю оновлення щохвилини.",
                reply_markup=main_keyboard(),
            )
            async with aiohttp.ClientSession() as session:
                await send_schedule(message, queue, session, "Поточний графік")
        elif action == "unsubscribe":
            remove_subscription(message.chat.id, queue)
            logger.info("Підписку видалено через меню: chat_id=%s queue=%s", message.chat.id, queue)
            await message.answer(f"🔕 Підписку на чергу <b>{queue}</b> вимкнено.", reply_markup=main_keyboard())
        elif action == "tomorrow":
            async with aiohttp.ClientSession() as session:
                await send_tomorrow_schedule(message, queue, session)
        elif action == "countdown":
            async with aiohttp.ClientSession() as session:
                await send_countdown(message, queue, session)
        elif action == "today":
            async with aiohttp.ClientSession() as session:
                await send_today_schedule(message, queue, session)

    async def monitor() -> None:
        global LAST_CHECK_AT, LAST_SUCCESS_AT
        logger.info("Монітор графіків запущено з інтервалом %s сек.", POLL_INTERVAL_SECONDS)
        async with aiohttp.ClientSession() as session:
            while True:
                forced_queues: set[str] = set()
                while not FORCE_REFRESH_REQUESTS.empty():
                    requested = FORCE_REFRESH_REQUESTS.get_nowait()
                    forced_queues.update(VALID_QUEUES if requested == "*" else {requested})
                FORCE_REFRESH_EVENT.clear()
                queues = {queue for chat_id in all_chats() for queue in queue_subscriptions(chat_id)} | forced_queues
                for queue in queues:
                    LAST_CHECK_AT = datetime.now(KYIV).isoformat(timespec="seconds")
                    forced = queue in forced_queues
                    if forced:
                        logger.info("Починаю примусове оновлення графіка: queue=%s", queue)
                    try:
                        today_payload = await fetch_today_schedule(session, queue)
                        if today_payload:
                            await send_upcoming_reminders(bot, queue, today_payload)
                        tomorrow_payload = await fetch_tomorrow_schedule(session, queue)
                        if today_payload or tomorrow_payload:
                            LAST_SUCCESS_AT = datetime.now(KYIV).isoformat(timespec="seconds")
                        if not today_payload and not tomorrow_payload:
                            logger.warning("Графік недоступний: queue=%s forced=%s", queue, forced)
                            continue
                        for payload, title in (
                            (today_payload, "Графік на сьогодні оновлено"),
                            (tomorrow_payload, "Графік на завтра оновлено"),
                        ):
                            if not payload:
                                continue
                            changed = save_and_check_changed(queue, payload)
                            if forced or changed:
                                if not notifications_enabled():
                                    logger.info("Розсилку пропущено: сповіщення вимкнені, queue=%s date=%s forced=%s", queue, payload.get("eventDate"), forced)
                                    continue
                                if forced:
                                    title = "Графік примусово оновлено"
                                text = format_schedule(queue, payload, title)
                                recipients = subscribed_chats(queue)
                                sent = 0
                                for chat_id in recipients:
                                    try:
                                        await bot.send_message(chat_id, text)
                                        sent += 1
                                    except Exception:
                                        logger.exception("Помилка розсилки графіка: chat_id=%s queue=%s", chat_id, queue)
                                logger.info(
                                    "Оновлення графіка завершено: queue=%s date=%s forced=%s recipients=%s sent=%s changed=%s",
                                    queue, payload.get("eventDate"), forced, len(recipients), sent, changed,
                                )
                        if forced:
                            logger.info("Примусове оновлення завершено: queue=%s", queue)
                    except Exception:
                        logger.exception("Помилка циклу графіка: queue=%s forced=%s", queue, forced)
                try:
                    await asyncio.wait_for(FORCE_REFRESH_EVENT.wait(), timeout=POLL_INTERVAL_SECONDS)
                except asyncio.TimeoutError:
                    pass

    def all_chats() -> list[int]:
        with connect_db() as db:
            rows = db.execute("SELECT DISTINCT chat_id FROM subscriptions").fetchall()
            return [row["chat_id"] for row in rows]

    monitor_task = asyncio.create_task(monitor())
    try:
        await dispatcher.start_polling(bot)
    finally:
        BOT_RUNNING = False
        monitor_task.cancel()
        await asyncio.gather(monitor_task, return_exceptions=True)
        await bot.session.close()
        logger.info("Telegram-бот зупинений")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    except KeyboardInterrupt:
        pass
