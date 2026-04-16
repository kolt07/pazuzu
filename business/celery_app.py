# -*- coding: utf-8 -*-
"""
Ініціалізація Celery для brokered background tasks.
"""

from __future__ import annotations

try:
    from celery import Celery
except ImportError:  # pragma: no cover - fallback for environments without Celery installed yet.
    Celery = None

from config.settings import Settings


def _build_broker_url(settings: Settings) -> str:
    return settings.task_queue_broker_url


def _build_backend_url(settings: Settings) -> str:
    return settings.task_queue_result_backend or "rpc://"


def create_celery_app(settings: Settings | None = None) -> Celery:
    if Celery is None:
        raise RuntimeError("Celery is not installed. Install requirements to enable RabbitMQ task queue.")
    app_settings = settings or Settings()
    app = Celery(
        "pazuzu",
        broker=_build_broker_url(app_settings),
        backend=_build_backend_url(app_settings),
        include=["business.tasks"],
    )
    app.conf.update(
        task_default_exchange="pazuzu",
        task_default_exchange_type="direct",
        task_default_routing_key="default",
        task_serializer="json",
        result_serializer="json",
        accept_content=["json"],
        timezone="UTC",
        enable_utc=True,
        worker_prefetch_multiplier=1,
        task_acks_late=True,
        task_reject_on_worker_lost=True,
        task_track_started=True,
        result_expires=3600,
        task_routes={
            "business.tasks.run_source_load_pipeline_task": {"queue": "source_load"},
            "business.tasks.process_olx_llm_task": {"queue": "llm_processing"},
            "business.tasks.process_prozorro_llm_task": {"queue": "llm_processing"},
        },
    )
    return app


celery_app = create_celery_app()
