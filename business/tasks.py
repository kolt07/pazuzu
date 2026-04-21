# -*- coding: utf-8 -*-
"""
Celery tasks для brokered source-load та LLM processing.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, Optional

from celery import current_task

from business.celery_app import celery_app
from business.services.source_data_load_service import process_prozorro_llm_auction, run_full_pipeline
from business.services.task_queue_service import TaskQueueService
from config.settings import Settings
from data.database.connection import MongoDBConnection

logger = logging.getLogger(__name__)


def _init_runtime() -> Settings:
    settings = Settings()
    MongoDBConnection.initialize(settings)
    return settings


def _queue_service(settings: Settings) -> TaskQueueService:
    return TaskQueueService(settings)


def _log_llm_queue_progress(settings: Settings) -> None:
    """Короткий знімок RabbitMQ + Mongo для оцінки «скільки лишилось» у llm_processing."""
    if not getattr(settings, "task_queue_enabled", False):
        return
    try:
        snap = TaskQueueService(settings).get_llm_queue_snapshot()
        r = snap.get("rabbit_messages")
        ma = snap.get("mongo_active_tasks")
        mq = snap.get("mongo_queued_only")
        parts = []
        if r is not None:
            parts.append(f"у RabbitMQ очікує ≈{r} повідомлень")
        if ma is not None:
            parts.append(f"у БД активних задач ≈{ma}")
        if mq is not None:
            parts.append(f"з них queued/received ≈{mq}")
        if parts:
            logger.info("[llm-processing] Черга LLM: %s", " | ".join(parts))
    except Exception:
        logger.debug("LLM queue snapshot failed", exc_info=True)


def _log_llm_batch_progress(queue: TaskQueueService, task_id: str, stage: str) -> None:
    """Логує прогрес batch-черги у форматі «оброблено X з Y»."""
    if not task_id:
        return
    try:
        doc = queue.get_task_doc(task_id) or {}
        meta = doc.get("metadata") or {}
        batch_id = str(meta.get("llm_batch_id") or "").strip()
        if not batch_id:
            return
        total_hint = meta.get("llm_batch_total")
        source = str(meta.get("llm_batch_source") or "").strip() or "unknown"
        progress = queue.get_llm_batch_progress(batch_id, total_hint=total_hint)
        logger.info(
            "[llm-processing] Черга LLM (%s, source=%s): оброблено %s з %s "
            "(success=%s, failed=%s, in_progress=%s)",
            stage,
            source,
            progress["processed"],
            progress["total"],
            progress["success"],
            progress["failed"],
            progress["in_progress"],
        )
    except Exception:
        logger.debug("LLM batch progress failed", exc_info=True)


def _current_task_id() -> str:
    req = getattr(current_task, "request", None)
    return str(getattr(req, "id", "") or "")


def _heartbeat_logger(queue: TaskQueueService, task_id: str):
    def _log(message: str) -> None:
        logger.info("%s", message)
        if task_id:
            try:
                queue.heartbeat(task_id, patch={"message": str(message)[:1000]})
            except Exception:
                logger.debug("Task heartbeat update failed (task_id=%s)", task_id, exc_info=True)

    return _log


def _safe_queue_heartbeat(queue: TaskQueueService, task_id: str, patch: Dict[str, Any]) -> None:
    if not task_id:
        return
    try:
        queue.heartbeat(task_id, patch=patch)
    except Exception:
        logger.debug("Task heartbeat update failed (task_id=%s)", task_id, exc_info=True)


@celery_app.task(bind=True, name="business.tasks.run_source_load_pipeline_task")
def run_source_load_pipeline_task(
    self,
    days: Optional[int] = None,
    sources: Optional[list] = None,
    regions: Optional[list] = None,
    listing_types: Optional[list] = None,
    use_browser_olx: Optional[bool] = None,
    olx_phase1_max_threads: Optional[int] = None,
) -> Dict[str, Any]:
    settings = _init_runtime()
    queue = _queue_service(settings)
    task_id = _current_task_id()
    control_state = queue.get_queue_control_state(TaskQueueService.SOURCE_LOAD_QUEUE)
    if control_state == "paused":
        if task_id:
            queue.heartbeat(task_id, patch={"phase": "paused_by_admin", "queue_control_state": "paused"})
        raise self.retry(countdown=20, max_retries=None)
    if control_state == "disabled":
        msg = "Queue source_load is disabled by admin."
        if task_id:
            queue.mark_task_failed(task_id, msg)
        return {"success": False, "skipped": True, "reason": msg}
    if task_id:
        queue.mark_task_started(task_id)
        queue.heartbeat(task_id, patch={"phase": "source_load_started"})
    try:
        log_fn = _heartbeat_logger(queue, task_id)
        result = run_full_pipeline(
            settings=settings,
            sources=list(sources or []),
            days=days,
            regions=list(regions or []) or None,
            listing_types=list(listing_types or []) or None,
            use_browser_olx=use_browser_olx,
            olx_phase1_max_threads=olx_phase1_max_threads,
            use_brokered_llm=True,
            log_fn=log_fn,
            llm_wait_heartbeat_fn=lambda: _safe_queue_heartbeat(
                queue,
                task_id,
                {"phase": "waiting_llm_tasks"},
            ),
            run_phase3=False,
        )
        logger.info(
            "[source_load] Core pipeline completed (task_id=%s): raw + promote/main + LLM done. "
            "Task marked SUCCESS. Phase 3 analytics is decoupled from this completion.",
            task_id or "—",
        )
        if task_id:
            _safe_queue_heartbeat(
                queue,
                task_id,
                {
                    "phase": "core_completed",
                    "message": "Core pipeline completed: raw + promote/main + LLM done",
                },
            )
            queue.mark_task_success(task_id, result=result)
        return result
    except Exception as e:
        logger.exception("Source-load task failed: %s", e)
        if task_id:
            queue.mark_task_failed(task_id, str(e))
        raise


@celery_app.task(bind=True, name="business.tasks.process_olx_llm_task")
def process_olx_llm_task(self, listing_url: str) -> Dict[str, Any]:
    settings = _init_runtime()
    queue = _queue_service(settings)
    task_id = _current_task_id()
    control_state = queue.get_queue_control_state(TaskQueueService.LLM_QUEUE)
    if control_state == "paused":
        if task_id:
            queue.heartbeat(task_id, patch={"phase": "paused_by_admin", "queue_control_state": "paused"})
        raise self.retry(countdown=20, max_retries=None)
    if control_state == "disabled":
        msg = "Queue llm_processing is disabled by admin."
        if task_id:
            queue.mark_task_failed(task_id, msg)
        return {"success": False, "skipped": True, "reason": msg, "listing_url": listing_url}
    wid = str(getattr(getattr(self, "request", None), "id", "") or task_id or "")
    logger.info(
        "[llm-processing] OLX LLM: старт listing_url=%s celery_id=%s",
        listing_url[:400] + ("…" if len(listing_url) > 400 else ""),
        wid or "—",
    )
    _log_llm_queue_progress(settings)
    if task_id:
        queue.mark_task_started(task_id)
        queue.heartbeat(task_id, patch={"phase": "llm_started", "listing_url": listing_url})
        _log_llm_batch_progress(queue, task_id, stage="start")
    try:
        from business.services.llm_service import _get_vllm_orchestrator
        from business.services.olx_llm_extractor_service import OlxLLMExtractorService
        from business.services.geocoding_service import GeocodingService
        from business.services.unified_listings_service import UnifiedListingsService
        from business.services.currency_rate_service import CurrencyRateService
        from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
        from data.repositories.olx_listings_repository import OlxListingsRepository
        from scripts.olx_scraper.run_update import _process_single_llm_pending_url

        log_fn = _heartbeat_logger(queue, task_id)
        raw_repo = RawOlxListingsRepository()
        main_repo = OlxListingsRepository()
        llm_extractor = OlxLLMExtractorService(settings)
        geocoding_service = GeocodingService(settings)
        unified_service = UnifiedListingsService(settings)
        try:
            usd_rate = CurrencyRateService(settings).get_today_usd_rate(allow_fetch=True)
        except Exception:
            usd_rate = None

        runtime = _get_vllm_orchestrator()
        if runtime.is_enabled():
            runtime.schedule_forced_healthcheck("llm_task_start_olx")
        ok = _process_single_llm_pending_url(
            listing_url=listing_url,
            raw_repo=raw_repo,
            main_repo=main_repo,
            llm_extractor=llm_extractor,
            geocoding_service=geocoding_service,
            unified_service=unified_service,
            usd_rate=usd_rate,
            log_fn=log_fn,
        )
        result = {"success": bool(ok), "listing_url": listing_url}
        logger.info(
            "[llm-processing] OLX LLM: кінець ok=%s celery_id=%s url=%s",
            ok,
            wid or "—",
            listing_url[:200] + ("…" if len(listing_url) > 200 else ""),
        )
        if task_id:
            if ok:
                queue.mark_task_success(task_id, result=result)
            else:
                queue.mark_task_failed(task_id, f"OLX LLM processing returned false for {listing_url}")
            _log_llm_batch_progress(queue, task_id, stage="finish")
        return result
    except Exception as e:
        logger.exception("OLX LLM task failed for %s: %s", listing_url, e)
        if task_id:
            queue.mark_task_failed(task_id, str(e))
        raise


@celery_app.task(bind=True, name="business.tasks.process_prozorro_llm_task")
def process_prozorro_llm_task(self, auction_id: str) -> Dict[str, Any]:
    settings = _init_runtime()
    queue = _queue_service(settings)
    task_id = _current_task_id()
    control_state = queue.get_queue_control_state(TaskQueueService.LLM_QUEUE)
    if control_state == "paused":
        if task_id:
            queue.heartbeat(task_id, patch={"phase": "paused_by_admin", "queue_control_state": "paused"})
        raise self.retry(countdown=20, max_retries=None)
    if control_state == "disabled":
        msg = "Queue llm_processing is disabled by admin."
        if task_id:
            queue.mark_task_failed(task_id, msg)
        return {"success": False, "skipped": True, "reason": msg, "auction_id": auction_id}
    wid = str(getattr(getattr(self, "request", None), "id", "") or task_id or "")
    logger.info("[llm-processing] ProZorro LLM: старт auction_id=%s celery_id=%s", auction_id, wid or "—")
    _log_llm_queue_progress(settings)
    if task_id:
        queue.mark_task_started(task_id)
        queue.heartbeat(task_id, patch={"phase": "llm_started", "auction_id": auction_id})
        _log_llm_batch_progress(queue, task_id, stage="start")
    try:
        from business.services.llm_service import _get_vllm_orchestrator

        runtime = _get_vllm_orchestrator()
        if runtime.is_enabled():
            runtime.schedule_forced_healthcheck("llm_task_start_prozorro")
        ok = process_prozorro_llm_auction(
            auction_id,
            settings=settings,
            log_fn=_heartbeat_logger(queue, task_id),
        )
        result = {"success": bool(ok), "auction_id": auction_id}
        logger.info(
            "[llm-processing] ProZorro LLM: кінець ok=%s auction_id=%s celery_id=%s",
            ok,
            auction_id,
            wid or "—",
        )
        if task_id:
            if ok:
                queue.mark_task_success(task_id, result=result)
            else:
                queue.mark_task_failed(task_id, f"ProZorro LLM processing returned false for {auction_id}")
            _log_llm_batch_progress(queue, task_id, stage="finish")
        return result
    except Exception as e:
        logger.exception("ProZorro LLM task failed for %s: %s", auction_id, e)
        if task_id:
            queue.mark_task_failed(task_id, str(e))
        raise
