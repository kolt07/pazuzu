# -*- coding: utf-8 -*-
"""
API: адміністрування (додати/заблокувати користувача, ProZorro config, оновлення даних).
"""

import threading
import uuid
from fastapi import APIRouter, Request, HTTPException, UploadFile, File, Query
from fastapi.responses import Response
from pydantic import BaseModel
from telegram_mini_app.auth import validate_telegram_init_data
import yaml

router = APIRouter(prefix="/api/admin", tags=["admin"])

# Задачі оновлення даних: task_id -> {status, message, ...}
_data_update_tasks: dict = {}


def _get_admin_user(request: Request):
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
    user_id = int(user_obj.get("id", 0))
    user_service = request.app.state.user_service
    if not user_service.is_admin(user_id):
        raise HTTPException(status_code=403, detail="Admin only")
    return user_id, user_service, request.app.state.prozorro_service, request.app.state.logging_service


class AddUserRequest(BaseModel):
    user_id: int
    role: str  # 'user' | 'admin'
    nickname: str


class BlockUserRequest(BaseModel):
    user_id: int


class TraceQueryRequest(BaseModel):
    text: str


class CreateSchedulerEventRequest(BaseModel):
    """Параметри для створення події планового оновлення даних."""
    hour: int = 6
    minute: int = 0
    days: int = 7
    sources: str = "all"


@router.post("/trace")
def trace_query(request: Request, body: TraceQueryRequest):
    """
    Теоретичне опрацювання запиту: IntentDetector → QueryStructure → PipelineBuilder
    без виконання пайплайну. Повертає зведення для дебагу (тільки для адмінів).
    """
    admin_id, _, _, _ = _get_admin_user(request)
    text = (body.text or "").strip()
    if not text:
        raise HTTPException(status_code=400, detail="Текст запиту не може бути порожнім.")
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
    trace_result = agent.trace_query_processing(
        user_query=text,
        user_id=str(admin_id),
    )
    return trace_result


@router.post("/add-user")
def add_user(request: Request, body: AddUserRequest):
    """Додає користувача або адміністратора."""
    admin_id, user_service, _, logging_service = _get_admin_user(request)
    if body.role not in ("user", "admin"):
        raise HTTPException(status_code=400, detail="role must be 'user' or 'admin'")
    success = user_service.add_user(body.user_id, body.role, body.nickname, admin_id)
    if success:
        logging_service.log_user_action(
            user_id=admin_id,
            action="admin_action",
            message=f"Додано користувача {body.nickname} (ID: {body.user_id}) як {body.role}",
            metadata={"action": "add_user", "target_user_id": body.user_id, "role": body.role},
        )
        return {"success": True, "message": f"Користувач {body.nickname} додано."}
    raise HTTPException(status_code=400, detail="Користувач вже існує або помилка збереження.")


@router.post("/block-user")
def block_user(request: Request, body: BlockUserRequest):
    """Блокує користувача."""
    admin_id, user_service, _, logging_service = _get_admin_user(request)
    success = user_service.block_user(body.user_id, admin_id)
    if success:
        logging_service.log_user_action(
            user_id=admin_id,
            action="admin_action",
            message=f"Заблоковано користувача (ID: {body.user_id})",
            metadata={"action": "block_user", "target_user_id": body.user_id},
        )
        return {"success": True, "message": "Користувача заблоковано."}
    raise HTTPException(status_code=400, detail="Не вдалося заблокувати користувача.")


@router.get("/prozorro-config")
def get_prozorro_config(request: Request, download: bool = False):
    """
    Якщо download=1 — повертає YAML-файл для завантаження.
    Інакше — JSON з конфігурацією.
    """
    _get_admin_user(request)
    prozorro = request.app.state.prozorro_service
    config = prozorro.get_classification_codes_config()
    if download:
        yaml_str = yaml.dump(config, allow_unicode=True, default_flow_style=False, sort_keys=False)
        return Response(
            content=yaml_str.encode("utf-8"),
            media_type="application/x-yaml",
            headers={"Content-Disposition": 'attachment; filename="ProZorro_clasification_codes.yaml"'},
        )
    return config


@router.post("/data-update")
def start_data_update(request: Request, days: int = Query(1, description="1 або 7 днів")):
    """Запускає оновлення даних ProZorro + OLX. Повертає task_id для перевірки статусу."""
    _get_admin_user(request)
    if days not in (1, 7):
        raise HTTPException(status_code=400, detail="days must be 1 or 7")
    task_id = str(uuid.uuid4())
    prozorro = request.app.state.prozorro_service
    settings = request.app.state.settings

    _data_update_tasks[task_id] = {
        "status": "running",
        "message": "Запуск оновлення...",
        "days": days,
    }

    def run_update():
        try:
            _data_update_tasks[task_id]["message"] = "ProZorro: оновлення аукціонів..."
            result_prozorro = prozorro.fetch_and_save_real_estate_auctions(days=days)
            p_ok = result_prozorro.get("success", False)
            p_msg = result_prozorro.get("message", "OK") if p_ok else result_prozorro.get("message", "помилка")
            _data_update_tasks[task_id]["message"] = "OLX: оновлення оголошень..."
            result_olx = {}
            try:
                from scripts.olx_scraper.run_update import run_olx_update
                result_olx = run_olx_update(settings=settings, days=days)
                o_ok = result_olx.get("success", False)
                o_msg = "OK" if o_ok else result_olx.get("message", "помилка")
                if o_ok and result_olx.get("total_listings") is not None:
                    o_msg += f" (оголошень: {result_olx.get('total_listings', 0)})"
            except Exception as olx_err:
                o_ok = False
                o_msg = str(olx_err)
            try:
                from business.services.collection_knowledge_service import refresh_knowledge_after_sources
                refresh_knowledge_after_sources(["prozorro", "olx"])
            except Exception:
                pass
            try:
                from business.services.price_analytics_service import PriceAnalyticsService
                analytics = PriceAnalyticsService()
                analytics.rebuild_all()
            except Exception:
                pass
            summary = f"ProZorro: {'✓ ' + p_msg if p_ok else '✗ ' + p_msg}\nOLX: {'✓ ' + o_msg if o_ok else '✗ ' + o_msg}"
            _data_update_tasks[task_id]["status"] = "done"
            _data_update_tasks[task_id]["message"] = summary
            _data_update_tasks[task_id]["prozorro"] = result_prozorro
            _data_update_tasks[task_id]["olx"] = result_olx.get("success", False)
        except Exception as e:
            _data_update_tasks[task_id]["status"] = "error"
            _data_update_tasks[task_id]["message"] = f"Помилка: {e!s}"

    threading.Thread(target=run_update, daemon=True, name="DataUpdate").start()
    return {"task_id": task_id, "status": "started", "days": days}


@router.get("/data-update/status")
def data_update_status(request: Request, task_id: str):
    """Повертає статус задачі оновлення даних."""
    _get_admin_user(request)
    if task_id not in _data_update_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    t = _data_update_tasks[task_id]
    return {
        "task_id": task_id,
        "status": t["status"],
        "message": t.get("message"),
        "days": t.get("days"),
    }


@router.get("/scheduler/events")
def list_scheduler_events(request: Request):
    """Список запланованих подій оновлення даних (тільки data_update, scope=system)."""
    _get_admin_user(request)
    from data.repositories.scheduled_events_repository import (
        EVENT_TYPE_DATA_UPDATE,
        SCOPE_SYSTEM,
        ScheduledEventsRepository,
    )
    repo = ScheduledEventsRepository()
    events = repo.get_active_events(event_type=EVENT_TYPE_DATA_UPDATE)
    result = []
    for ev in events:
        schedule = ev.get("schedule") or {}
        payload = ev.get("payload") or {}
        result.append({
            "id": str(ev.get("_id", "")),
            "hour": schedule.get("hour", 0),
            "minute": schedule.get("minute", 0),
            "days": payload.get("days", 1),
            "sources": payload.get("sources", "all"),
            "title": ev.get("title"),
            "last_run_at": ev.get("last_run_at").isoformat() if ev.get("last_run_at") else None,
        })
    return {"events": result}


@router.post("/scheduler/events")
def create_scheduler_event(request: Request, body: CreateSchedulerEventRequest):
    """Створює подію планового оновлення даних."""
    admin_id, user_service, _, _ = _get_admin_user(request)
    if body.hour < 0 or body.hour > 23:
        raise HTTPException(status_code=400, detail="hour must be 0-23")
    if body.minute < 0 or body.minute > 59:
        raise HTTPException(status_code=400, detail="minute must be 0-59")
    if body.days not in (1, 7):
        raise HTTPException(status_code=400, detail="days must be 1 or 7")
    if body.sources not in ("all", "prozorro", "olx"):
        raise HTTPException(status_code=400, detail="sources must be all, prozorro, or olx")
    from data.repositories.scheduled_events_repository import (
        EVENT_TYPE_DATA_UPDATE,
        SCOPE_SYSTEM,
        ScheduledEventsRepository,
    )
    repo = ScheduledEventsRepository()
    schedule = {
        "type": "cron",
        "minute": body.minute,
        "hour": body.hour,
    }
    payload = {"days": body.days, "sources": body.sources}
    title = f"Оновлення даних щодня о {body.hour:02d}:{body.minute:02d} ({body.days} дн., {body.sources})"
    event_id = repo.create_event(
        event_type=EVENT_TYPE_DATA_UPDATE,
        scope=SCOPE_SYSTEM,
        schedule=schedule,
        payload=payload,
        created_by=admin_id,
        title=title,
    )
    return {"success": True, "id": event_id, "message": "Подію додано. Застосується після перезапуску бота."}


@router.delete("/scheduler/events/{event_id}")
def delete_scheduler_event(request: Request, event_id: str):
    """Видаляє (деактивує) подію планового оновлення."""
    _get_admin_user(request)
    from data.repositories.scheduled_events_repository import (
        EVENT_TYPE_DATA_UPDATE,
        ScheduledEventsRepository,
    )
    repo = ScheduledEventsRepository()
    ev = repo.get_event_by_id(event_id)
    if not ev:
        raise HTTPException(status_code=404, detail="Event not found")
    if ev.get("event_type") != EVENT_TYPE_DATA_UPDATE:
        raise HTTPException(status_code=400, detail="Can only delete data_update events")
    repo.deactivate(event_id)
    return {"success": True, "message": "Подію вимкнено."}


@router.get("/feedback/dislikes")
def get_feedback_dislikes(
    request: Request,
    limit: int = Query(50, ge=1, le=200),
    days: int = Query(14, ge=1, le=90),
):
    """Список дизлайків з повною бесідою для перегляду в панелі адміністратора."""
    _get_admin_user(request)
    from data.repositories.feedback_repository import FeedbackRepository
    repo = FeedbackRepository()
    items = repo.get_recent_dislikes(limit=limit, days=days)
    return {"items": items, "count": len(items)}


@router.post("/prozorro-config")
async def upload_prozorro_config(request: Request, file: UploadFile = File(...)):
    """Завантажує файл конфігурації ProZorro (YAML)."""
    _get_admin_user(request)
    if not file.filename or not (file.filename.endswith(".yaml") or file.filename.endswith(".yml")):
        raise HTTPException(status_code=400, detail="Файл має бути .yaml або .yml")
    content = await file.read()
    try:
        config = yaml.safe_load(content.decode("utf-8"))
    except Exception as e:
        raise HTTPException(status_code=400, detail=f"Помилка парсингу YAML: {e}")
    if not isinstance(config, dict) or "classification_codes" not in config:
        raise HTTPException(status_code=400, detail="Файл має містити ключ classification_codes")
    if not isinstance(config["classification_codes"], list):
        raise HTTPException(status_code=400, detail="classification_codes має бути списком")
    for idx, item in enumerate(config["classification_codes"]):
        if not isinstance(item, dict) or "code" not in item:
            raise HTTPException(status_code=400, detail=f"Елемент {idx + 1}: потрібен словник з полем code")
    prozorro = request.app.state.prozorro_service
    success = prozorro.save_classification_codes_config(config)
    if not success:
        raise HTTPException(status_code=500, detail="Не вдалося зберегти конфігурацію")
    return {"success": True, "message": f"Завантажено {len(config['classification_codes'])} кодів."}
