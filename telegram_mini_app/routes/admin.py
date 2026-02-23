# -*- coding: utf-8 -*-
"""
API: адміністрування (додати/заблокувати користувача, ProZorro config, оновлення даних).
"""

import threading
import uuid
from typing import Optional
from fastapi import APIRouter, Request, HTTPException, UploadFile, File, Query
from fastapi.responses import Response
from pydantic import BaseModel
from telegram_mini_app.auth import validate_telegram_init_data
import yaml

router = APIRouter(prefix="/api/admin", tags=["admin"])

# Задачі оновлення даних: task_id -> {status, message, ...}
_data_update_tasks: dict = {}

# Задачі скрапера кадастру: task_id -> {status, message, cells_processed, parcels_saved, ...}
_cadastral_scraper_tasks: dict = {}

# Задачі індексації та кластеризації кадастру
_cadastral_index_tasks: dict = {}
_cadastral_clusters_tasks: dict = {}


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
def start_data_update(
    request: Request,
    days: Optional[int] = Query(None, description="1, 7 або 30 днів"),
    mode: Optional[str] = Query(None, description="full_olx | full_prozorro | period"),
):
    """
    Запускає оновлення даних. Повертає task_id для перевірки статусу.

    Режими:
    - days=1|7|30 (без mode) — оновлення за період (ProZorro + OLX)
    - mode=full_olx — OLX повністю (всі сторінки пошуку)
    - mode=full_prozorro — ProZorro повністю (вся історія)
    - mode=period & days=30 — оновлення за місяць
    """
    _get_admin_user(request)
    task_id = str(uuid.uuid4())
    prozorro = request.app.state.prozorro_service
    settings = request.app.state.settings

    # Визначаємо режим
    run_prozorro = False
    run_olx = False
    olx_full = False
    prozorro_full = False
    effective_days = days or 7

    if mode == "full_olx":
        run_olx = True
        olx_full = True
    elif mode == "full_prozorro":
        run_prozorro = True
        prozorro_full = True
    elif mode == "period" and days:
        if days not in (1, 7, 30):
            raise HTTPException(status_code=400, detail="days must be 1, 7 or 30 for mode=period")
        run_prozorro = True
        run_olx = True
        effective_days = days
    else:
        # Класичний режим: days=1|7|30 (за замовчуванням 7)
        effective_days = days if days in (1, 7, 30) else 7
        if days is not None and days not in (1, 7, 30):
            raise HTTPException(status_code=400, detail="days must be 1, 7 or 30")
        run_prozorro = True
        run_olx = True

    _data_update_tasks[task_id] = {
        "status": "running",
        "message": "Запуск оновлення...",
        "days": effective_days,
        "mode": mode,
    }

    def run_update():
        try:
            result_prozorro = {}
            result_olx = {}
            p_ok = True
            o_ok = True
            p_msg = "пропущено"
            o_msg = "пропущено"

            if run_prozorro:
                _data_update_tasks[task_id]["message"] = (
                    "ProZorro: повне оновлення історії..." if prozorro_full
                    else f"ProZorro: оновлення за {effective_days} дн..."
                )
                result_prozorro = prozorro.fetch_and_save_real_estate_auctions(
                    days=None if prozorro_full else effective_days,
                    full=prozorro_full,
                )
                p_ok = result_prozorro.get("success", False)
                p_msg = result_prozorro.get("message", "помилка") if p_ok else result_prozorro.get("message", "помилка")

            if run_olx:
                _data_update_tasks[task_id]["message"] = (
                    "OLX: повне оновлення..." if olx_full
                    else f"OLX: оновлення за {effective_days} дн..."
                )
                try:
                    from scripts.olx_scraper.run_update import run_olx_update
                    result_olx = run_olx_update(
                        settings=settings,
                        days=None if olx_full else effective_days,
                        full=olx_full,
                    )
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


@router.post("/reformat-listing")
def reformat_listing(
    request: Request,
    source: str = Query(..., description="Джерело: olx або prozorro"),
    source_id: str = Query(..., description="ID в джерелі (URL для OLX, auction_id для ProZorro)"),
):
    """
    Переформатує оголошення: повторний LLM-парсинг, геокодування, перерахунок метрик.
    Доступно лише адміністраторам.
    """
    _get_admin_user(request)
    try:
        from business.services.listing_reformat_service import ListingReformatService
        svc = ListingReformatService(settings=request.app.state.settings)
        result = svc.reformat_listing(source.strip().lower(), source_id)
        return result
    except Exception as e:
        return {"success": False, "message": str(e), "updated": False}


@router.post("/process-anomalous-prices")
def process_anomalous_prices(
    request: Request,
    limit: int = Query(50, ge=1, le=200, description="Максимум оголошень для перевірки"),
):
    """
    Знаходить оголошення з аномальними цінами (глобально, потім по місцевості).
    Доступно лише адміністраторам.
    """
    _get_admin_user(request)
    try:
        from business.services.price_anomaly_service import PriceAnomalyService
        svc = PriceAnomalyService()
        result = svc.process_anomalous_prices(limit=limit)
        return result
    except Exception as e:
        return {"found": 0, "items": [], "error": str(e)}


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


@router.post("/cadastral-scraper/start")
def start_cadastral_scraper(
    request: Request,
    max_cells: int = Query(None, description="Максимум комірок за запуск (None — без обмеження)"),
):
    """Запускає скрапер кадастрової карти kadastrova-karta.com. Повертає task_id."""
    _get_admin_user(request)
    task_id = str(uuid.uuid4())
    settings = request.app.state.settings

    # Ініціалізація MongoDB до запуску потоку (fork-safety при uvicorn workers)
    from data.database.connection import MongoDBConnection
    MongoDBConnection.initialize(settings)

    _cadastral_scraper_tasks[task_id] = {
        "status": "running",
        "message": "Запуск скрапера кадастру...",
        "cells_processed": 0,
        "parcels_saved": 0,
        "total_parcels": 0,
        "cells_total": 0,
        "cells_done": 0,
        "current_cell_id": "",
        "errors": 0,
    }

    def run_scraper():
        try:
            from scripts.cadastral_scraper.run_scraper import run_cadastral_scraper

            def log_fn(msg: str):
                _cadastral_scraper_tasks[task_id]["message"] = msg

            def progress_fn(progress: dict):
                t = _cadastral_scraper_tasks[task_id]
                t["cells_processed"] = progress.get("cells_processed", 0)
                t["parcels_saved"] = progress.get("parcels_saved", 0)
                t["total_parcels"] = progress.get("total_parcels", 0)
                t["cells_total"] = progress.get("cells_total", 0)
                t["cells_done"] = progress.get("cells_done", 0)
                t["current_cell_id"] = progress.get("current_cell_id", "")
                cells_total = progress.get("cells_total") or 0
                pct = f" ({100 * progress.get('cells_done', 0) // max(1, cells_total)}%)" if cells_total else ""
                t["message"] = (
                    f"Тайлів: {progress.get('cells_done', 0)}/{cells_total}{pct} | "
                    f"Ділянок: {progress.get('total_parcels', 0)} | "
                    f"За сесію: +{progress.get('parcels_saved', 0)}"
                )

            result = run_cadastral_scraper(
                settings=settings,
                max_cells=max_cells,
                log_fn=log_fn,
                progress_callback=progress_fn,
            )
            _cadastral_scraper_tasks[task_id]["status"] = "done"
            _cadastral_scraper_tasks[task_id]["message"] = result.get("message", "Готово.")
            _cadastral_scraper_tasks[task_id]["cells_processed"] = result.get("cells_processed", 0)
            _cadastral_scraper_tasks[task_id]["parcels_saved"] = result.get("parcels_saved", 0)
            _cadastral_scraper_tasks[task_id]["total_parcels"] = result.get("total_parcels", 0)
            _cadastral_scraper_tasks[task_id]["errors"] = result.get("errors", 0)
        except Exception as e:
            _cadastral_scraper_tasks[task_id]["status"] = "error"
            _cadastral_scraper_tasks[task_id]["message"] = f"Помилка: {e!s}"

    threading.Thread(target=run_scraper, daemon=True, name="CadastralScraper").start()
    return {"task_id": task_id, "status": "started", "max_cells": max_cells}


@router.get("/cadastral-scraper/status")
def cadastral_scraper_status(request: Request, task_id: str):
    """Повертає статус задачі скрапера кадастру з прогресом."""
    _get_admin_user(request)
    if task_id not in _cadastral_scraper_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    t = _cadastral_scraper_tasks[task_id]
    return {
        "task_id": task_id,
        "status": t["status"],
        "message": t.get("message"),
        "cells_processed": t.get("cells_processed", 0),
        "parcels_saved": t.get("parcels_saved", 0),
        "total_parcels": t.get("total_parcels", 0),
        "cells_total": t.get("cells_total", 0),
        "cells_done": t.get("cells_done", 0),
        "current_cell_id": t.get("current_cell_id", ""),
        "errors": t.get("errors", 0),
    }


@router.get("/cadastral-scraper/db-info")
def cadastral_scraper_db_info(request: Request):
    """Діагностика: БД, хост, кількість ділянок (для перевірки контексту)."""
    _get_admin_user(request)
    from data.database.connection import MongoDBConnection
    from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository

    try:
        MongoDBConnection.initialize(request.app.state.settings)
        s = request.app.state.settings
        total = CadastralParcelsRepository().count_total()
        return {
            "database": s.mongodb_database_name,
            "host": f"{s.mongodb_host}:{s.mongodb_port}",
            "total_parcels": total,
        }
    except Exception as e:
        return {"error": str(e), "database": None, "total_parcels": 0}


@router.get("/cadastral-scraper/stats")
def cadastral_scraper_stats(request: Request):
    """Загальна статистика скрапера: всього ділянок, комірок, індексу, кластерів."""
    _get_admin_user(request)
    from data.database.connection import MongoDBConnection
    from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository
    from data.repositories.cadastral_scraper_progress_repository import CadastralScraperProgressRepository
    from data.repositories.cadastral_parcel_location_index_repository import CadastralParcelLocationIndexRepository
    from data.repositories.cadastral_parcel_clusters_repository import CadastralParcelClustersRepository

    try:
        MongoDBConnection.initialize(request.app.state.settings)
        s = request.app.state.settings
        parcels_repo = CadastralParcelsRepository()
        progress_repo = CadastralScraperProgressRepository()
        index_repo = CadastralParcelLocationIndexRepository()
        clusters_repo = CadastralParcelClustersRepository()
        total_parcels = parcels_repo.count_total()
        cell_stats = progress_repo.get_stats()
        index_count = index_repo.count_total()
        clusters_count = clusters_repo.count_total()
        return {
            "total_parcels": total_parcels,
            "cells": cell_stats,
            "location_index_count": index_count,
            "clusters_count": clusters_count,
            "db_info": f"{s.mongodb_database_name} @ {s.mongodb_host}:{s.mongodb_port}",
        }
    except Exception as e:
        return {
            "total_parcels": 0,
            "cells": {},
            "location_index_count": 0,
            "clusters_count": 0,
            "db_info": None,
            "error": str(e),
        }


@router.post("/cadastral-scraper/reset-stale")
def reset_cadastral_stale_cells(request: Request):
    """
    Скидає комірки зі статусом processing, що зависли (старші 1 хв).
    Повертає їх у pending для повторної обробки.
    """
    _get_admin_user(request)
    from data.database.connection import MongoDBConnection
    from data.repositories.cadastral_scraper_progress_repository import CadastralScraperProgressRepository

    try:
        MongoDBConnection.initialize(request.app.state.settings)
        progress_repo = CadastralScraperProgressRepository()
        reset = progress_repo.reset_stale_processing(max_age_minutes=1)
        return {"success": True, "reset_count": reset}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cadastral/clusters/clear")
def cadastral_clear_clusters(request: Request):
    """Очищає колекцію cadastral_parcel_clusters."""
    _get_admin_user(request)
    from data.database.connection import MongoDBConnection
    from data.repositories.cadastral_parcel_clusters_repository import CadastralParcelClustersRepository

    try:
        MongoDBConnection.initialize(request.app.state.settings)
        repo = CadastralParcelClustersRepository()
        deleted = repo.clear_all()
        return {"success": True, "deleted": deleted}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


@router.post("/cadastral/index/build")
def cadastral_build_index(request: Request):
    """Запускає побудову індексу місцезнаходження з cadastral_parcels. Повертає task_id."""
    _get_admin_user(request)
    task_id = str(uuid.uuid4())
    from data.database.connection import MongoDBConnection
    MongoDBConnection.initialize(request.app.state.settings)

    _cadastral_index_tasks[task_id] = {
        "status": "running",
        "message": "Запуск індексації...",
        "indexed": 0,
        "skipped": 0,
        "errors": 0,
    }

    def run_index():
        try:
            from business.services.cadastral_location_index_service import CadastralLocationIndexService

            def progress_fn(idx, skip, err):
                _cadastral_index_tasks[task_id]["indexed"] = idx
                _cadastral_index_tasks[task_id]["skipped"] = skip
                _cadastral_index_tasks[task_id]["errors"] = err
                _cadastral_index_tasks[task_id]["message"] = f"Індексовано: {idx}, пропущено: {skip}"

            service = CadastralLocationIndexService()
            result = service.build_index_from_parcels(batch_size=2000, progress_callback=progress_fn)
            _cadastral_index_tasks[task_id]["status"] = "done"
            _cadastral_index_tasks[task_id]["message"] = (
                f"Готово. Індексовано: {result['indexed']}, пропущено: {result['skipped']}, помилок: {result['errors']}"
            )
            _cadastral_index_tasks[task_id]["indexed"] = result["indexed"]
            _cadastral_index_tasks[task_id]["skipped"] = result["skipped"]
            _cadastral_index_tasks[task_id]["errors"] = result["errors"]
        except Exception as e:
            _cadastral_index_tasks[task_id]["status"] = "error"
            _cadastral_index_tasks[task_id]["message"] = f"Помилка: {e!s}"

    threading.Thread(target=run_index, daemon=True, name="CadastralIndex").start()
    return {"task_id": task_id, "status": "started"}


@router.get("/cadastral/index/status")
def cadastral_index_status(request: Request, task_id: str):
    """Статус побудови індексу місцезнаходження."""
    _get_admin_user(request)
    if task_id not in _cadastral_index_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    t = _cadastral_index_tasks[task_id]
    return {
        "task_id": task_id,
        "status": t["status"],
        "message": t.get("message"),
        "indexed": t.get("indexed", 0),
        "skipped": t.get("skipped", 0),
        "errors": t.get("errors", 0),
    }


@router.post("/cadastral/clusters/build")
def cadastral_build_clusters(request: Request):
    """Запускає побудову кластерів ділянок. Повертає task_id."""
    _get_admin_user(request)
    task_id = str(uuid.uuid4())
    from data.database.connection import MongoDBConnection
    MongoDBConnection.initialize(request.app.state.settings)

    _cadastral_clusters_tasks[task_id] = {
        "status": "running",
        "message": "Запуск кластеризації...",
        "parcels_processed": 0,
        "clusters_created": 0,
        "parcels_in_clusters": 0,
    }

    def run_clusters():
        try:
            from business.services.cadastral_clustering_service import CadastralClusteringService

            def progress_fn(processed, clusters):
                _cadastral_clusters_tasks[task_id]["parcels_processed"] = processed
                _cadastral_clusters_tasks[task_id]["clusters_created"] = clusters
                _cadastral_clusters_tasks[task_id]["message"] = f"Оброблено: {processed}, кластерів: {clusters}"

            service = CadastralClusteringService()
            result = service.build_clusters(
                max_parcels=None,
                min_cluster_size=2,
                progress_callback=progress_fn,
            )
            _cadastral_clusters_tasks[task_id]["status"] = "done"
            _cadastral_clusters_tasks[task_id]["message"] = (
                f"Готово. Ділянок: {result['parcels_processed']}, кластерів: {result['clusters_created']}, "
                f"ділянок у кластерах: {result['parcels_in_clusters']}"
            )
            _cadastral_clusters_tasks[task_id]["parcels_processed"] = result["parcels_processed"]
            _cadastral_clusters_tasks[task_id]["clusters_created"] = result["clusters_created"]
            _cadastral_clusters_tasks[task_id]["parcels_in_clusters"] = result["parcels_in_clusters"]
        except Exception as e:
            _cadastral_clusters_tasks[task_id]["status"] = "error"
            _cadastral_clusters_tasks[task_id]["message"] = f"Помилка: {e!s}"

    threading.Thread(target=run_clusters, daemon=True, name="CadastralClusters").start()
    return {"task_id": task_id, "status": "started"}


@router.get("/cadastral/clusters/status")
def cadastral_clusters_status(request: Request, task_id: str):
    """Статус побудови кластерів."""
    _get_admin_user(request)
    if task_id not in _cadastral_clusters_tasks:
        raise HTTPException(status_code=404, detail="Task not found")
    t = _cadastral_clusters_tasks[task_id]
    return {
        "task_id": task_id,
        "status": t["status"],
        "message": t.get("message"),
        "parcels_processed": t.get("parcels_processed", 0),
        "clusters_created": t.get("clusters_created", 0),
        "parcels_in_clusters": t.get("parcels_in_clusters", 0),
    }


@router.post("/cadastral-scraper/reset-cells")
def reset_cadastral_scraper_cells(request: Request):
    """
    Очищає колекцію cadastral_scraper_cells. Після цього при наступному запуску
    скрапера буде створена нова сітка (zoom 12, center-first порядок).
    """
    _get_admin_user(request)
    from data.database.connection import MongoDBConnection
    from data.repositories.cadastral_scraper_progress_repository import CadastralScraperProgressRepository

    try:
        MongoDBConnection.initialize(request.app.state.settings)
        progress_repo = CadastralScraperProgressRepository()
        deleted = progress_repo.collection.delete_many({}).deleted_count
        return {"success": True, "deleted_cells": deleted}
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))


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


def _fill_days_with_zeros(by_day: list, days: int) -> list:
    """Доповнює by_day усіма датами в діапазоні, для відсутніх — count=0."""
    from datetime import datetime, timedelta
    if not by_day and days <= 0:
        return []
    date_to_count = {d["date"]: d["count"] for d in by_day}
    result = []
    today = datetime.utcnow().date()
    for i in range(days - 1, -1, -1):
        d = today - timedelta(days=i)
        date_str = d.strftime("%Y-%m-%d")
        result.append({"date": date_str, "count": date_to_count.get(date_str, 0)})
    return result


@router.get("/usage-stats")
def get_usage_stats(
    request: Request,
    days: int = Query(60, ge=7, le=365, description="Кількість днів для графіка"),
):
    """
    Статистика використання: запити LLM та виклики Google Geocoding API по днях,
    плюс загальна кількість та за останній місяць.
    """
    _get_admin_user(request)
    from data.repositories.logs_repository import LogsRepository
    from data.repositories.geocode_cache_repository import GeocodeCacheRepository

    try:
        logs_repo = LogsRepository()

        # LLM: api_usage (усі виклики) + llm_query (запити користувачів з Mini App)
        llm_api_by_day = logs_repo.count_api_usage_by_day(service="llm", days=days)
        llm_api_total = logs_repo.count_api_usage_total(service="llm")
        llm_api_last_month = logs_repo.count_api_usage_last_month(service="llm")
        llm_user_by_day = logs_repo.count_llm_queries_by_day(days=days)
        llm_user_total = logs_repo.count_llm_queries_total()
        llm_user_last_month = logs_repo.count_llm_queries_last_month()

        # Geocoding: api_usage (from_cache=False = реальні виклики API; from_cache=True = з кешу)
        geocode_api_by_day = logs_repo.count_api_usage_by_day(
            service="geocoding", days=days, from_cache_only=False
        )
        geocode_api_total = logs_repo.count_api_usage_total(
            service="geocoding", from_cache_only=False
        )
        geocode_api_last_month = logs_repo.count_api_usage_last_month(
            service="geocoding", from_cache_only=False
        )
        geocode_cache_by_day = logs_repo.count_api_usage_by_day(
            service="geocoding", days=days, from_cache_only=True
        )
        geocode_cache_total = logs_repo.count_api_usage_total(
            service="geocoding", from_cache_only=True
        )

        llm_by_day = _fill_days_with_zeros(llm_api_by_day, days)
        geocode_by_day = _fill_days_with_zeros(geocode_api_by_day, days)

        return {
            "llm": {
                "by_day": llm_by_day,
                "total": llm_api_total,
                "last_month": llm_api_last_month,
                "user_queries_total": llm_user_total,
                "user_queries_last_month": llm_user_last_month,
            },
            "geocoding": {
                "by_day": geocode_by_day,
                "total": geocode_api_total,
                "last_month": geocode_api_last_month,
                "cache_hits_total": geocode_cache_total,
            },
        }
    except Exception as e:
        return {
            "llm": {"by_day": [], "total": 0, "last_month": 0},
            "geocoding": {"by_day": [], "total": 0, "last_month": 0},
            "error": str(e),
        }


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


@router.get("/integrity/check")
def integrity_check(request: Request):
    """Перевірка цілісності даних (схема, колекції)."""
    _get_admin_user(request)
    from business.services.data_integrity_service import DataIntegrityService
    service = DataIntegrityService()
    return service.check()


@router.get("/export/config")
def export_config(request: Request):
    """Експорт конфігураційного bundle (ZIP)."""
    _get_admin_user(request)
    from config.config_export_service import build_config_zip
    zip_bytes, filename = build_config_zip()
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/export/data")
def export_data(request: Request, limit: int = Query(10000, ge=1, le=50000)):
    """Експорт даних з основних колекцій (ZIP з JSON)."""
    _get_admin_user(request)
    from config.config_export_service import build_data_zip
    zip_bytes, filename = build_data_zip(limit_per_collection=limit)
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.get("/export/full")
def export_full(request: Request, limit: int = Query(5000, ge=1, le=20000)):
    """Експорт конфігу + даних в одному архіві."""
    _get_admin_user(request)
    from config.config_export_service import build_full_zip
    zip_bytes, filename = build_full_zip(limit_per_collection=limit)
    return Response(
        content=zip_bytes,
        media_type="application/zip",
        headers={"Content-Disposition": f'attachment; filename="{filename}"'},
    )


@router.post("/import/config")
async def import_config(request: Request, file: UploadFile = File(...)):
    """Імпорт конфігурації з ZIP."""
    admin_id, _, _, logging_service = _get_admin_user(request)
    if not file.filename or not file.filename.lower().endswith(".zip"):
        raise HTTPException(status_code=400, detail="Файл має бути .zip")
    content = await file.read()
    from config.config_export_service import import_config_from_zip
    success, message = import_config_from_zip(content)
    if success:
        logging_service.log_user_action(
            user_id=admin_id,
            action="admin_action",
            message=f"Імпортовано конфігурацію: {message}",
            metadata={"action": "import_config"},
        )
        return {"success": True, "message": message}
    raise HTTPException(status_code=400, detail=message)


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
