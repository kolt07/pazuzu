# -*- coding: utf-8 -*-
"""Orchestration backend adapters (Celery now, Temporal-compatible)."""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Any, Dict, Optional

from config.settings import Settings


class OrchestrationBackend(ABC):
    @abstractmethod
    def enqueue(self, session_id: str, countdown_sec: int = 0) -> None:
        raise NotImplementedError

    @abstractmethod
    def heartbeat(self, session_id: str, payload: Dict[str, Any]) -> None:
        raise NotImplementedError

    @abstractmethod
    def name(self) -> str:
        raise NotImplementedError


class CeleryBackend(OrchestrationBackend):
    def __init__(self, settings: Settings):
        self.settings = settings

    def enqueue(self, session_id: str, countdown_sec: int = 0) -> None:
        from business.tasks import run_investigation_step  # type: ignore

        run_investigation_step.apply_async(
            args=[session_id],
            countdown=max(0, int(countdown_sec)),
            queue="flx_investigation",
        )

    def heartbeat(self, session_id: str, payload: Dict[str, Any]) -> None:
        _ = session_id
        _ = payload

    def name(self) -> str:
        return "celery"


class TemporalBackend(OrchestrationBackend):
    """Compatibility stub for future migration."""

    def __init__(self, settings: Settings):
        self.settings = settings

    def enqueue(self, session_id: str, countdown_sec: int = 0) -> None:
        raise NotImplementedError("Temporal backend is not wired yet.")

    def heartbeat(self, session_id: str, payload: Dict[str, Any]) -> None:
        _ = session_id
        _ = payload

    def name(self) -> str:
        return "temporal"


def get_orchestration_backend(settings: Settings, preferred: Optional[str] = None) -> OrchestrationBackend:
    name = str(preferred or getattr(settings, "flx_orchestration_backend", "celery")).strip().lower()
    if name == "temporal":
        return TemporalBackend(settings)
    return CeleryBackend(settings)
