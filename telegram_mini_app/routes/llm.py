# -*- coding: utf-8 -*-
"""
API: чат з LLM-асистентом. Використовує MultiAgentService (безпека + мультиагентний пайплайн).
"""

import asyncio
import concurrent.futures
import json
import queue
import uuid
from fastapi import APIRouter, Request, HTTPException
from fastapi.responses import StreamingResponse
from pydantic import BaseModel
from telegram_mini_app.auth import validate_telegram_init_data
import re
from typing import Any, Dict, List
from utils.link_formatter import format_message_links_for_mini_app


def _infer_quick_actions_from_response(response: str, user_query: str) -> List[Dict[str, Any]]:
    """
    Визначає кнопки швидких дій, якщо відповідь містить уточнення або пропозицію варіантів.
    """
    if not response or not isinstance(response, str):
        return []
    r_lower = response.lower()
    actions = []
    if re.search(r"(показати|вивести)\s+(тут|в\s+чату|текстом)", r_lower) or "показати тут" in r_lower:
        actions.append({"label": "Показати тут", "prompt": (user_query or "").strip() + " Покажи результати в чаті."})
    if re.search(r"(вивантажити|експорт|завантажити|зберегти)\s+(в\s+)?(файл|excel)", r_lower) or "у файл" in r_lower:
        actions.append({"label": "Вивантажити в Excel", "prompt": (user_query or "").strip() + " Вивантаж результати в Excel."})
    return actions[:8]

router = APIRouter(prefix="/api/llm", tags=["llm"])


class ChatRequest(BaseModel):
    text: str
    chat_id: str | None = None  # ідентифікатор чату для контексту діалогу (Mini App)
    listing_context: dict | None = None  # { page_url, summary } — контекст оголошення з посиланням (без заголовка в text)
    reply_to_text: str | None = None
    intent: str | None = None   # explicit_intent: report_last_day | report_last_week | export_data
    params: dict | None = None  # explicit_params: period_days, collections, region_filter, need_update_first


def _get_user_id_and_services(request: Request):
    init_data = request.headers.get("X-Telegram-Init-Data")
    if not init_data:
        raise HTTPException(status_code=403, detail="X-Telegram-Init-Data required")
    token = getattr(request.app.state, "bot_token", None)
    if not token:
        raise HTTPException(status_code=503, detail="Mini app not configured")
    validated = validate_telegram_init_data(init_data, token)
    if not validated:
        raise HTTPException(status_code=403, detail="Invalid init data")
    user_obj = validated.get("user")
    if not user_obj or not isinstance(user_obj, dict):
        raise HTTPException(status_code=403, detail="User data missing")
    user_id = user_obj.get("id")
    if user_id is None:
        raise HTTPException(status_code=403, detail="User id missing")
    user_service = request.app.state.user_service
    if not user_service.is_user_authorized(int(user_id)):
        raise HTTPException(status_code=403, detail="User not authorized")
    agent = getattr(request.app.state, "multi_agent_service", None)
    if agent is None:
        try:
            from business.services.multi_agent_service import MultiAgentService
            logging_svc = request.app.state.logging_service

            def notify_admins_fn(msg: str, uid=None, det=None):
                detail = f" User: {uid}" if uid else ""
                if det:
                    detail += f" Details: {det}"
                logging_svc.log_app_event(
                    message=f"[Mini App security] {msg}{detail}",
                    event_type="security_incident",
                )
            request.app.state.multi_agent_service = MultiAgentService(
                request.app.state.settings,
                user_service=request.app.state.user_service,
                notify_admins_fn=notify_admins_fn,
            )
            agent = request.app.state.multi_agent_service
        except Exception as e:
            raise HTTPException(status_code=503, detail=f"LLM agent not available: {e!s}")
    return int(user_id), agent, request.app.state.logging_service


@router.post("/chat")
def chat(request: Request, body: ChatRequest):
    """
    Відправляє повідомлення LLM-асистенту та повертає текстову відповідь.
    Обробка через MultiAgentService: перевірка безпеки, мультиагентний пайплайн або LangChain-агент.
    """
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(
            status_code=400,
            detail="Текст повідомлення не може бути порожнім. Введіть запит.",
        )
    user_id, agent, logging_service = _get_user_id_and_services(request)
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    request_id = str(uuid.uuid4())
    
    def run_query():
        return agent.process_query(
            user_query=text,
            user_id=str(user_id),
            chat_id=body.chat_id,
            listing_context=body.listing_context,
            stream_callback=None,
            reply_to_text=body.reply_to_text,
            request_id=request_id,
            explicit_intent=body.intent,
            explicit_params=body.params,
        )

    with concurrent.futures.ThreadPoolExecutor() as pool:
        future = pool.submit(run_query)
        response = future.result(timeout=300)

    logging_service.log_user_action(
        user_id=user_id,
        action="llm_query",
        message=f"Mini App запит: {text[:100]}...",
        metadata={"query_length": len(text), "response_length": len(response), "request_id": request_id},
    )

    excel_files = agent.get_last_excel_files()

    # Форматуємо посилання: клікабельні, з підписом «Посилання» замість URL
    formatted_response = format_message_links_for_mini_app(response)

    # Зберігаємо Excel у артефакти, повертаємо посилання для завантаження (без base64)
    artifact_service = getattr(request.app.state, "artifact_service", None)
    if not artifact_service:
        from business.services.artifact_service import ArtifactService
        request.app.state.artifact_service = ArtifactService()
        artifact_service = request.app.state.artifact_service

    out_excel_files = []
    for f in (excel_files or []):
        content_b64 = f.get("file_base64")
        filename = f.get("filename") or "report.xlsx"
        if not content_b64:
            continue
        try:
            reg = artifact_service.register_with_token(
                user_id=str(user_id),
                artifact_type="excel",
                content_base64=content_b64,
                metadata={"filename": filename},
            )
            out_excel_files.append({
                "artifact_id": reg["artifact_id"],
                "download_token": reg["download_token"],
                "filename": filename,
                "rows_count": f.get("rows_count"),
                "columns_count": f.get("columns_count"),
            })
        except Exception:
            continue

    return {
        "response": formatted_response,
        "request_id": request_id,
        "excel_files": out_excel_files,
    }


def _sse_line(data: dict) -> str:
    """Формує рядок SSE: data: {...}"""
    return f"data: {json.dumps(data, ensure_ascii=False)}\n\n"


@router.post("/chat-stream")
def chat_stream(request: Request, body: ChatRequest):
    """
    Відправляє повідомлення LLM-асистенту та повертає SSE-потік зі статусами та фінальною відповіддю.
    """
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(
            status_code=400,
            detail="Текст повідомлення не може бути порожнім. Введіть запит.",
        )
    user_id, agent, logging_service = _get_user_id_and_services(request)
    try:
        asyncio.get_event_loop()
    except RuntimeError:
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)

    request_id = str(uuid.uuid4())
    events_queue = queue.Queue()

    def status_callback(msg: str) -> None:
        events_queue.put({"type": "status", "message": msg})

    def run_query() -> None:
        try:
            response = agent.process_query(
                user_query=text,
                user_id=str(user_id),
                chat_id=body.chat_id,
                listing_context=body.listing_context,
                stream_callback=None,
                status_callback=status_callback,
                reply_to_text=body.reply_to_text,
                request_id=request_id,
                explicit_intent=body.intent,
                explicit_params=body.params,
            )
            excel_files = agent.get_last_excel_files()
            formatted_response = format_message_links_for_mini_app(response)
            logging_service.log_user_action(
                user_id=user_id,
                action="llm_query",
                message=f"Mini App запит (stream): {text[:100]}...",
                metadata={"query_length": len(text), "response_length": len(response), "request_id": request_id},
            )
            artifact_service = getattr(request.app.state, "artifact_service", None)
            if not artifact_service:
                from business.services.artifact_service import ArtifactService
                request.app.state.artifact_service = ArtifactService()
                artifact_service = request.app.state.artifact_service
            out_excel_files = []
            for f in (excel_files or []):
                content_b64 = f.get("file_base64")
                filename = f.get("filename") or "report.xlsx"
                if not content_b64:
                    continue
                try:
                    reg = artifact_service.register_with_token(
                        user_id=str(user_id),
                        artifact_type="excel",
                        content_base64=content_b64,
                        metadata={"filename": filename},
                    )
                    out_excel_files.append({
                        "artifact_id": reg["artifact_id"],
                        "download_token": reg["download_token"],
                        "filename": filename,
                        "rows_count": f.get("rows_count"),
                        "columns_count": f.get("columns_count"),
                    })
                except Exception:
                    continue
            quick_actions = _infer_quick_actions_from_response(response, text)
            events_queue.put({
                "type": "done",
                "response": formatted_response,
                "request_id": request_id,
                "excel_files": out_excel_files,
                "quick_actions": quick_actions,
            })
        except Exception as e:
            events_queue.put({"type": "error", "message": str(e)})

    def event_generator():
        pool = concurrent.futures.ThreadPoolExecutor(max_workers=1)
        future = pool.submit(run_query)
        while True:
            try:
                item = events_queue.get(timeout=2.0)
            except queue.Empty:
                yield _sse_line({"type": "ping"})
                continue
            yield _sse_line(item)
            if item.get("type") in ("done", "error"):
                break
        future.result(timeout=1)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
        },
    )
