# -*- coding: utf-8 -*-
"""
Старт/зупинка VastRuntimeSupervisor тільки в Celery-воркері llm_processing.

Оренда / idle-drain / прогрів Vast — лише тут, щоб не дублювати оркестратор з pazuzu-app
(різні процеси = різні process-singleton VllmRuntimeOrchestrator).
"""

from __future__ import annotations

import logging
import os
from typing import Any, Optional

logger = logging.getLogger(__name__)

_VAST_SUPERVISOR: Any = None


def register_vast_runtime_supervisor_hooks() -> None:
    """Підключає worker_init / worker_shutdown до celery_app (викликати після import celery_app)."""
    from celery import signals
    from celery.app.base import Celery

    def _is_enabled() -> bool:
        raw = (os.getenv("PAZUZU_VAST_ORCHESTRATOR_IN_WORKER") or "").strip().lower()
        return raw in ("1", "true", "yes", "on")

    def _start_supervisor_if_needed(stage: str) -> None:
        global _VAST_SUPERVISOR
        if not _is_enabled():
            logger.info("Vast runtime supervisor: skip at %s (PAZUZU_VAST_ORCHESTRATOR_IN_WORKER=false).", stage)
            return
        if _VAST_SUPERVISOR is not None:
            return
        try:
            from config.settings import Settings
            from data.database.connection import MongoDBConnection
            from business.services.vast_runtime_supervisor_service import VastRuntimeSupervisorService

            settings = Settings()
            MongoDBConnection.initialize(settings)
            sup = VastRuntimeSupervisorService(
                settings,
                notify_admins_fn=None,
                orchestrate_vast_rentals=True,
            )
            sup.start()
            _VAST_SUPERVISOR = sup
            logger.info("Vast runtime supervisor: started in Celery worker at %s.", stage)
        except Exception as e:
            logger.warning("Vast runtime supervisor: failed to start at %s: %s", stage, e)

    @signals.worker_init.connect
    def _on_worker_init(sender: Optional[Celery] = None, **kwargs: Any) -> None:
        _start_supervisor_if_needed("worker_init")

    @signals.worker_ready.connect
    def _on_worker_ready(sender: Optional[Celery] = None, **kwargs: Any) -> None:
        # fallback: на частині конфігів worker_init може відпрацювати занадто рано/тихо
        _start_supervisor_if_needed("worker_ready")

    # Жорсткий fallback: для середовищ, де Celery signals поводяться нестабільно,
    # стартуємо supervisor одразу після реєстрації hooks.
    _start_supervisor_if_needed("register_hooks")

    @signals.worker_shutdown.connect
    def _on_worker_shutdown(sender: Optional[Celery] = None, **kwargs: Any) -> None:
        global _VAST_SUPERVISOR
        if _VAST_SUPERVISOR is None:
            return
        try:
            _VAST_SUPERVISOR.stop()
        except Exception as e:
            logger.debug("Vast runtime supervisor stop: %s", e)
        _VAST_SUPERVISOR = None
