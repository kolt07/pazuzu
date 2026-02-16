# -*- coding: utf-8 -*-
"""
API: обробка фідбеку користувачів про відповіді LLM.
"""

import logging
from datetime import datetime, timezone
from typing import Dict, Any
from fastapi import APIRouter, Request, HTTPException
from pydantic import BaseModel
from telegram_mini_app.auth import validate_telegram_init_data
from data.repositories.feedback_repository import FeedbackRepository
from data.repositories.agent_activity_log_repository import AgentActivityLogRepository

router = APIRouter(prefix="/api/feedback", tags=["feedback"])
logger = logging.getLogger(__name__)


class FeedbackRequest(BaseModel):
    request_id: str
    feedback_type: str  # "like" | "dislike"
    user_query: str | None = None
    response_text: str | None = None
    chat_id: str | None = None  # для завантаження повної бесіди при dislike


def _get_user_id_and_services(request: Request):
    """Отримує user_id та необхідні сервіси."""
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
    return int(user_id)


@router.post("/submit")
def submit_feedback(request: Request, body: FeedbackRequest):
    """
    Обробляє фідбек користувача про відповідь LLM.
    Якщо feedback_type == "dislike", запускає самодіагностику.
    """
    user_id = _get_user_id_and_services(request)
    
    if body.feedback_type not in ["like", "dislike"]:
        raise HTTPException(
            status_code=400,
            detail="feedback_type має бути 'like' або 'dislike'"
        )
    
    feedback_repo = FeedbackRepository()
    activity_log_repo = AgentActivityLogRepository()
    
    # Отримуємо дані запиту з логів, якщо вони не передані
    user_query = body.user_query
    response_text = body.response_text
    
    if not user_query or not response_text:
        # Спробуємо отримати з логів активності
        logs = activity_log_repo.get_by_request_id(body.request_id)
        for log_entry in logs:
            payload = log_entry.get("payload", {})
            step = log_entry.get("step")
            agent_name = log_entry.get("agent_name", "")
            
            # Шукаємо user_query в різних місцях
            if not user_query:
                if step == "intent" and "user_query" in payload:
                    user_query = payload["user_query"]
                elif "query" in payload:
                    user_query = payload["query"]
                elif "text" in payload:
                    user_query = payload["text"]
            
            # response_text зазвичай не зберігається в логах (тільки response_length)
            # Але можемо спробувати знайти в payload
            if not response_text:
                if "response_text" in payload:
                    response_text = payload["response_text"]
                elif "response" in payload:
                    response_text = payload["response"]
    
    # Якщо все ще немає даних, використовуємо значення за замовчуванням
    user_query = user_query or "Не вказано"
    response_text = response_text or "Не вказано"
    
    diagnostic_result = None
    
    # Якщо дизлайк — завантажуємо повну бесіду з ChatSessionRepository
    conversation = None
    if body.feedback_type == "dislike" and body.chat_id:
        try:
            from data.repositories.chat_session_repository import ChatSessionRepository
            chat_repo = ChatSessionRepository()
            session = chat_repo.get(str(user_id), body.chat_id)
            messages = session.get("messages") or []
            if messages:
                conversation = [
                    {"role": m.get("role", ""), "content": (m.get("content", ""))[:10000]}
                    for m in messages
                ]
                logger.info("Завантажено бесіду для dislike: %s повідомлень", len(conversation))
        except Exception as e:
            logger.warning("Не вдалося завантажити бесіду для dislike: %s", e)
    
    # Якщо дизлайк - запускаємо самодіагностику
    if body.feedback_type == "dislike":
        logger.info("Запуск самодіагностики для request_id=%s, user_id=%s", body.request_id, user_id)
        diagnostic_result = _run_self_diagnostics(body.request_id, user_query, response_text)
    
    # Зберігаємо фідбек
    feedback_id = feedback_repo.save_feedback(
        request_id=body.request_id,
        user_id=str(user_id),
        user_query=user_query,
        response_text=response_text,
        feedback_type=body.feedback_type,
        diagnostic_result=diagnostic_result,
        conversation=conversation,
    )
    
    logger.info("Збережено фідбек: feedback_id=%s, request_id=%s, type=%s", 
                feedback_id, body.request_id, body.feedback_type)
    
    return {
        "success": True,
        "feedback_id": feedback_id,
        "diagnostic_result": diagnostic_result if body.feedback_type == "dislike" else None
    }


def _run_self_diagnostics(request_id: str, user_query: str, response_text: str) -> Dict[str, Any]:
    """
    Запускає самодіагностику для виявлення проблеми з відповіддю.
    
    Returns:
        Словник з результатами діагностики:
        - issues: список виявлених проблем
        - suggestions: рекомендації для виправлення
        - execution_context: контекст виконання запиту
    """
    issues = []
    suggestions = []
    execution_context = {}
    
    try:
        activity_log_repo = AgentActivityLogRepository()
        logs = activity_log_repo.get_by_request_id(request_id)
        
        # Аналізуємо логи для виявлення проблем
        execution_steps = []
        for log_entry in logs:
            step_info = {
                "agent": log_entry.get("agent_name"),
                "step": log_entry.get("step"),
                "payload": log_entry.get("payload", {})
            }
            execution_steps.append(step_info)
        
        execution_context["steps"] = execution_steps
        execution_context["total_steps"] = len(execution_steps)
        
        # Перевіряємо наявність кроків
        step_types = [log.get("step") for log in logs]
        agent_names = [log.get("agent_name") for log in logs]
        
        # Перевірка 1: Чи був визначений intent?
        if "intent" not in step_types:
            issues.append("Не визначено намір користувача (intent detection не виконано)")
            suggestions.append("Перевірити роботу IntentDetectorAgent")
        
        # Перевірка 2: Чи був побудований пайплайн?
        pipeline_built = any(
            log.get("agent_name") == "PipelineBuilderAgent" 
            for log in logs
        )
        if not pipeline_built:
            issues.append("Пайплайн не був побудований")
            suggestions.append("Перевірити роботу PipelineBuilderAgent")
        
        # Перевірка 3: Чи були результати?
        has_results = False
        for log in logs:
            payload = log.get("payload", {})
            if "results" in payload or "count" in payload:
                result_count = payload.get("count", 0) or payload.get("results_count", 0)
                if result_count > 0:
                    has_results = True
                    break
        
        if not has_results:
            issues.append("Запит не повернув результатів")
            suggestions.append("Перевірити фільтри та умови запиту")
            suggestions.append("Перевірити наявність даних у колекціях")
        
        # Перевірка 4: Чи відповідь порожня або занадто коротка?
        if len(response_text) < 50:
            issues.append("Відповідь занадто коротка або порожня")
            suggestions.append("Перевірити роботу AnswerComposerService")
        
        # Перевірка 5: Аналізуємо помилки в логах
        error_logs = [
            log for log in logs 
            if log.get("payload", {}).get("error") or 
               log.get("payload", {}).get("success") == False
        ]
        if error_logs:
            issues.append(f"Виявлено {len(error_logs)} помилок під час виконання")
            for error_log in error_logs[:3]:  # Перші 3 помилки
                error_msg = error_log.get("payload", {}).get("error", "Невідома помилка")
                suggestions.append(f"Помилка в {error_log.get('agent_name')}: {error_msg}")
        
        # Перевірка 6: Чи відповідає відповідь запиту?
        # Простий перевірка: чи містить відповідь ключові слова з запиту?
        query_words = set(user_query.lower().split())
        response_words = set(response_text.lower().split())
        common_words = query_words.intersection(response_words)
        if len(common_words) < 2 and len(query_words) > 3:
            issues.append("Відповідь не відповідає запиту (мало спільних слів)")
            suggestions.append("Перевірити правильність інтерпретації запиту")
        
        # Перевірка 7: Чи є діагностична інформація в відповіді?
        if "діагностик" not in response_text.lower() and "результатів не знайдено" in response_text.lower():
            issues.append("Відсутня діагностична інформація про причину відсутності результатів")
            suggestions.append("Додати детальну діагностику в AnswerComposerService")
        
    except Exception as e:
        logger.exception("Помилка під час самодіагностики: %s", e)
        issues.append(f"Помилка самодіагностики: {str(e)}")
        suggestions.append("Перевірити логи системи")
    
    return {
        "issues": issues,
        "suggestions": suggestions,
        "execution_context": execution_context,
        "diagnostic_timestamp": datetime.now(timezone.utc).isoformat()
    }
