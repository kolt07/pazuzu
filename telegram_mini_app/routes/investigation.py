# -*- coding: utf-8 -*-
"""
API маршрути для агента-інвестігейтора Flx (нове розслідування).

Endpoints:
- POST   /api/llm/investigation/start                          → стартує сесію + перший крок
- POST   /api/llm/investigation/{session_id}/answer            → відповідь користувача (ask_user)
- POST   /api/llm/investigation/{session_id}/cancel            → скасування
- GET    /api/llm/investigation/{session_id}                   → стан сесії
- GET    /api/llm/investigation                                → список сесій користувача
- GET    /api/llm/investigation/{session_id}/events            → SSE-стрім подій (status/note/question/done)
- GET    /api/llm/investigation/{session_id}/report            → 302 → /api/files/artifact/{aid}?token=...
"""

import json
import logging
import time
from typing import Optional

from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import RedirectResponse, StreamingResponse
from pydantic import BaseModel

from telegram_mini_app.auth import validate_telegram_init_data

logger = logging.getLogger(__name__)
router = APIRouter(prefix="/api/llm/investigation", tags=["investigation"])

# Поліінг SSE: якщо немає нових подій, надсилаємо ping раз на 15с щоб тримати з'єднання живим.
SSE_POLL_INTERVAL_SECONDS = 1.5
SSE_PING_INTERVAL_SECONDS = 15.0
SSE_MAX_LIFETIME_SECONDS = 600  # 10 хв; UI може реконектитись з ?since=seq


def _get_user_id(request: Request) -> int:
    init_data = request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        raise HTTPException(status_code=403, detail="X-Telegram-Init-Data required")
    token = getattr(request.app.state, "bot_token", None)
    if not token:
        raise HTTPException(status_code=503, detail="Mini app not configured")
    validated = validate_telegram_init_data(init_data, token)
    if not validated:
        raise HTTPException(status_code=403, detail="Invalid init data")
    user_obj = validated.get("user") or {}
    user_id = user_obj.get("id")
    if user_id is None:
        raise HTTPException(status_code=403, detail="User id missing")
    user_service = request.app.state.user_service
    if not user_service.is_user_authorized(int(user_id)):
        raise HTTPException(status_code=403, detail="User not authorized")
    return int(user_id)


def _get_service(request: Request):
    svc = getattr(request.app.state, "investigation_service", None)
    if svc is None:
        from business.services.investigation_service import InvestigationService

        svc = InvestigationService(request.app.state.settings)
        request.app.state.investigation_service = svc
    return svc


class StartRequest(BaseModel):
    text: str
    chat_id: Optional[str] = None


@router.post("/start")
def start_investigation(request: Request, body: StartRequest):
    user_id = _get_user_id(request)
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Текст запиту не може бути порожнім")
    svc = _get_service(request)
    result = svc.start(
        user_id=str(user_id),
        chat_id=body.chat_id,
        query=text,
    )
    if not result.get("ok"):
        raise HTTPException(status_code=500, detail=result.get("error") or "Не вдалося стартувати розслідування")
    return result


class AnswerRequest(BaseModel):
    answer: str


@router.post("/{session_id}/answer")
def submit_answer(request: Request, session_id: str, body: AnswerRequest):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    state = svc.get_state(session_id)
    if not state.get("ok"):
        raise HTTPException(status_code=404, detail="session_not_found")
    if str(state["session"].get("user_id")) != str(user_id):
        raise HTTPException(status_code=403, detail="Немає доступу")
    result = svc.submit_user_answer(session_id, body.answer or "")
    if not result.get("ok"):
        raise HTTPException(status_code=400, detail=result.get("error") or "answer_rejected")
    return result


@router.post("/{session_id}/cancel")
def cancel_investigation(request: Request, session_id: str):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    state = svc.get_state(session_id)
    if not state.get("ok"):
        raise HTTPException(status_code=404, detail="session_not_found")
    if str(state["session"].get("user_id")) != str(user_id):
        raise HTTPException(status_code=403, detail="Немає доступу")
    return svc.cancel(session_id)


@router.get("")
def list_investigations(request: Request, limit: int = 50):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    items = svc.list_for_user(str(user_id), limit=int(limit))
    # Прибираємо громіздкі поля з кожного запису (plan/notes тримаємо легкими)
    out = []
    for s in items:
        out.append({
            "session_id": s.get("session_id"),
            "query": s.get("query"),
            "state": s.get("state"),
            "step_index": s.get("step_index"),
            "report_artifact_id": s.get("report_artifact_id"),
            "report_artifact_token": s.get("report_artifact_token"),
            "updated_at": s.get("updated_at").isoformat() if s.get("updated_at") else None,
            "created_at": s.get("created_at").isoformat() if s.get("created_at") else None,
        })
    return {"items": out}


@router.get("/{session_id}")
def get_investigation(request: Request, session_id: str):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    state = svc.get_state(session_id)
    if not state.get("ok"):
        raise HTTPException(status_code=404, detail="session_not_found")
    sess = state["session"]
    if str(sess.get("user_id")) != str(user_id):
        raise HTTPException(status_code=403, detail="Немає доступу")
    # Серіалізуємо datetime
    for k in ("updated_at", "created_at"):
        if sess.get(k) is not None and not isinstance(sess[k], str):
            try:
                sess[k] = sess[k].isoformat()
            except Exception:
                sess[k] = str(sess[k])
    return {"session": sess}


def _sse_line(data: dict) -> str:
    return f"data: {json.dumps(data, ensure_ascii=False, default=str)}\n\n"


@router.get("/{session_id}/events")
def stream_events(request: Request, session_id: str, since: int = 0):
    user_id = _get_user_id(request)
    svc = _get_service(request)
    state = svc.get_state(session_id)
    if not state.get("ok"):
        raise HTTPException(status_code=404, detail="session_not_found")
    if str(state["session"].get("user_id")) != str(user_id):
        raise HTTPException(status_code=403, detail="Немає доступу")

    def event_generator():
        last_seq = int(since or 0)
        last_ping = time.time()
        started = time.time()
        while True:
            now = time.time()
            if now - started > SSE_MAX_LIFETIME_SECONDS:
                # Користувач має реконектитись з ?since=seq для продовження
                yield _sse_line({"type": "reconnect", "since_seq": last_seq})
                break
            try:
                events = svc.fetch_events(session_id, since_seq=last_seq, limit=200)
            except Exception as e:
                yield _sse_line({"type": "error", "message": str(e)})
                break
            terminal = False
            for ev in events:
                last_seq = max(last_seq, int(ev.get("seq") or 0))
                payload = {
                    "type": ev.get("type"),
                    "seq": ev.get("seq"),
                    "payload": ev.get("payload") or {},
                }
                yield _sse_line(payload)
                if ev.get("type") in ("done", "error"):
                    terminal = True
            if terminal:
                break
            if time.time() - last_ping >= SSE_PING_INTERVAL_SECONDS:
                yield _sse_line({"type": "ping", "ts": int(time.time())})
                last_ping = time.time()
            time.sleep(SSE_POLL_INTERVAL_SECONDS)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )


@router.get("/{session_id}/report")
def open_report(request: Request, session_id: str):
    """Зручний редірект на артефакт HTML-звіту (з токеном)."""
    user_id = _get_user_id(request)
    svc = _get_service(request)
    state = svc.get_state(session_id)
    if not state.get("ok"):
        raise HTTPException(status_code=404, detail="session_not_found")
    sess = state["session"]
    if str(sess.get("user_id")) != str(user_id):
        raise HTTPException(status_code=403, detail="Немає доступу")
    aid = sess.get("report_artifact_id")
    token = sess.get("report_artifact_token")
    if not aid:
        raise HTTPException(status_code=404, detail="Звіт ще не готовий")
    url = f"/api/files/artifact/{aid}"
    if token:
        url += f"?token={token}"
    return RedirectResponse(url=url, status_code=302)
