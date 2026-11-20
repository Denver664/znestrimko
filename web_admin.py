"""React admin API and FadeHost entry point for the Telegram bot."""

import asyncio
import hmac
import json
import logging
import os
import threading
from collections import deque
from contextlib import asynccontextmanager
from datetime import datetime
from pathlib import Path
from typing import Any

import uvicorn
from fastapi import Depends, FastAPI, Header, HTTPException, WebSocket, WebSocketDisconnect
from fastapi.responses import FileResponse
from pydantic import BaseModel

import bot


BASE_DIR = Path(__file__).resolve().parent
ADMIN_DIR = BASE_DIR / "admin"
logger = logging.getLogger("admin")


class LiveLogHandler(logging.Handler):
    """Bounded in-memory log history shared by the API and live WebSocket."""

    def __init__(self) -> None:
        super().__init__(logging.INFO)
        self.records: deque[dict[str, Any]] = deque(maxlen=1000)
        self.records_lock = threading.Lock()
        self.next_id = 1

    def emit(self, record: logging.LogRecord) -> None:
        message = self.format(record)
        lowered = message.lower()
        if any(word in lowered for word in ("нагадув", "сповіщ", "notification")):
            category = "notifications"
        elif any(word in lowered for word in ("графік", "черг", "schedule", "оновлення")):
            category = "schedules"
        else:
            category = "system"
        with self.records_lock:
            entry = {
                "id": self.next_id,
                "time": datetime.now().astimezone().isoformat(timespec="seconds"),
                "level": record.levelname,
                "category": category,
                "message": message,
            }
            self.next_id += 1
            self.records.append(entry)

    def since(self, after_id: int = 0, limit: int = 200) -> list[dict[str, Any]]:
        with self.records_lock:
            return [item for item in self.records if item["id"] > after_id][-limit:]

    def latest(self, limit: int = 200) -> list[dict[str, Any]]:
        with self.records_lock:
            return list(self.records)[-limit:]


live_logs = LiveLogHandler()
live_logs.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(name)s — %(message)s"))
logging.getLogger().addHandler(live_logs)


def admin_token() -> str:
    return os.getenv("ADMIN_TOKEN", "")


async def require_admin(x_admin_token: str | None = Header(default=None)) -> None:
    expected = admin_token()
    if not expected:
        raise HTTPException(status_code=503, detail="ADMIN_TOKEN не налаштовано на сервері")
    if not x_admin_token or not hmac.compare_digest(x_admin_token, expected):
        raise HTTPException(status_code=401, detail="Невірний токен адміністратора")


async def run_bot() -> None:
    try:
        await bot.main()
    except asyncio.CancelledError:
        raise
    except Exception:
        logger.exception("Telegram-бот аварійно завершився")


@asynccontextmanager
async def lifespan(_: FastAPI):
    if not admin_token():
        logger.error("Адмінпанель заблокована: додайте ADMIN_TOKEN у змінні середовища")
    bot_task = asyncio.create_task(run_bot(), name="telegram-bot")
    logger.info("Вебадмінку запущено; адреса слухає PORT=%s", os.getenv("PORT", "8080"))
    try:
        yield
    finally:
        bot_task.cancel()
        await asyncio.gather(bot_task, return_exceptions=True)
        logger.info("Вебадмінку зупинено")


app = FastAPI(title="Знеструмко — адмінпанель", lifespan=lifespan)


class LoginRequest(BaseModel):
    token: str


class RefreshRequest(BaseModel):
    queue: str = "*"


@app.get("/")
async def index() -> FileResponse:
    return FileResponse(ADMIN_DIR / "index.html")


@app.get("/admin.js")
async def javascript() -> FileResponse:
    return FileResponse(ADMIN_DIR / "admin.js", media_type="text/javascript")


@app.get("/admin.css")
async def stylesheet() -> FileResponse:
    return FileResponse(ADMIN_DIR / "admin.css", media_type="text/css")


@app.post("/api/login")
async def login(data: LoginRequest) -> dict[str, bool]:
    expected = admin_token()
    if not expected:
        raise HTTPException(status_code=503, detail="На сервері не задано ADMIN_TOKEN")
    if not hmac.compare_digest(data.token, expected):
        logger.warning("Відхилено невдалу спробу входу в адмінпанель")
        raise HTTPException(status_code=401, detail="Невірний токен адміністратора")
    logger.info("Адміністратор увійшов у панель")
    return {"ok": True}


@app.get("/api/status", dependencies=[Depends(require_admin)])
async def status() -> dict[str, Any]:
    subscriptions: dict[str, int] = {}
    try:
        with bot.connect_db() as db:
            rows = db.execute("SELECT queue, COUNT(*) AS total FROM subscriptions GROUP BY queue").fetchall()
            subscriptions = {row["queue"]: row["total"] for row in rows}
    except Exception:
        logger.exception("Не вдалося прочитати підписки для адмінпанелі")
    return {
        "bot_running": bot.BOT_RUNNING,
        "last_check_at": bot.LAST_CHECK_AT,
        "last_success_at": bot.LAST_SUCCESS_AT,
        "queues_total": len(bot.VALID_QUEUES),
        "subscriptions_total": sum(subscriptions.values()),
        "queues_subscribed": len(subscriptions),
        "database_path": str(bot.DB_PATH),
    }


@app.get("/api/schedules", dependencies=[Depends(require_admin)])
async def schedules() -> dict[str, Any]:
    try:
        with bot.connect_db() as db:
            rows = db.execute(
                "SELECT queue, event_date, payload, fetched_at FROM schedules ORDER BY fetched_at DESC"
            ).fetchall()
            subscriptions = db.execute(
                "SELECT queue, COUNT(*) AS total FROM subscriptions GROUP BY queue"
            ).fetchall()
        subscription_counts = {row["queue"]: row["total"] for row in subscriptions}
        items = []
        seen: set[tuple[str, str]] = set()
        for row in rows:
            key = (row["queue"], row["event_date"])
            if key in seen:
                continue
            seen.add(key)
            payload = json.loads(row["payload"])
            intervals = payload.get("queues", {}).get(row["queue"], [])
            planned = [item.get("shutdownHours") for item in intervals if item.get("status") == 1]
            items.append({
                "queue": row["queue"],
                "date": row["event_date"],
                "fetched_at": row["fetched_at"],
                "intervals": planned,
                "total_seconds": bot.scheduled_blackout_seconds(intervals),
                "subscribers": subscription_counts.get(row["queue"], 0),
            })
        return {"items": items, "queues": sorted(bot.VALID_QUEUES)}
    except Exception:
        logger.exception("Не вдалося завантажити графіки для адмінпанелі")
        raise HTTPException(status_code=500, detail="Не вдалося прочитати графіки")


@app.get("/api/logs", dependencies=[Depends(require_admin)])
async def logs(limit: int = 200) -> dict[str, Any]:
    return {"items": live_logs.latest(max(1, min(limit, 500)))}


@app.post("/api/refresh", status_code=202, dependencies=[Depends(require_admin)])
async def refresh(data: RefreshRequest) -> dict[str, Any]:
    queue = data.queue.strip()
    if queue != "*" and queue not in bot.VALID_QUEUES:
        raise HTTPException(status_code=400, detail="Невідома черга")
    if not bot.BOT_RUNNING:
        raise HTTPException(status_code=503, detail="Telegram-бот зараз не працює")
    bot.FORCE_REFRESH_REQUESTS.put_nowait(queue)
    bot.FORCE_REFRESH_EVENT.set()
    logger.info("Адміністратор запросив примусове оновлення: queue=%s", "усі черги" if queue == "*" else queue)
    return {"accepted": True, "queue": queue}


@app.websocket("/api/live")
async def live(websocket: WebSocket) -> None:
    await websocket.accept()
    try:
        hello = await asyncio.wait_for(websocket.receive_json(), timeout=10)
    except (asyncio.TimeoutError, WebSocketDisconnect):
        await websocket.close(code=4401)
        return
    token = str(hello.get("token", "")) if isinstance(hello, dict) else ""
    expected = admin_token()
    if not expected or not hmac.compare_digest(token, expected):
        await websocket.close(code=4401)
        return
    cursor = 0
    for item in live_logs.latest(200):
        await websocket.send_json(item)
        cursor = max(cursor, item["id"])
    try:
        while True:
            await asyncio.sleep(0.5)
            for item in live_logs.since(cursor, 100):
                await websocket.send_json(item)
                cursor = item["id"]
    except (WebSocketDisconnect, RuntimeError):
        return


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=int(os.getenv("PORT", "8080")), log_config=None)
