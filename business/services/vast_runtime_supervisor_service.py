# -*- coding: utf-8 -*-
"""
Фоновий supervisor для Vast runtime:
- стартує разом із застосунком;
- перевіряє чергу LLM-задач;
- прогріває runtime за потреби;
- виконує idle-drain, коли черга порожня.
"""

from __future__ import annotations

import logging
import threading
from typing import Optional

from config.settings import Settings
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from business.services.vllm_runtime_orchestrator import get_shared_vllm_runtime_orchestrator

logger = logging.getLogger(__name__)


class VastRuntimeSupervisorService:
    """Окремий фон-процес (daemon thread) керування Vast runtime."""

    def __init__(self, settings: Settings, poll_interval_sec: int = 10) -> None:
        self.settings = settings
        self.poll_interval_sec = max(3, int(poll_interval_sec))
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._orchestrator = get_shared_vllm_runtime_orchestrator()
        self._olx_repo = OlxListingsRepository()
        self._prozorro_repo = ProZorroAuctionsRepository()

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._stop_event.clear()
        self._thread = threading.Thread(
            target=self._loop,
            daemon=True,
            name="VastRuntimeSupervisor",
        )
        self._thread.start()
        logger.info("Vast runtime supervisor started.")

    def stop(self, timeout_sec: float = 5.0) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=max(0.5, float(timeout_sec)))
        logger.info("Vast runtime supervisor stopped.")

    def _loop(self) -> None:
        while not self._stop_event.is_set():
            try:
                if not self._is_vast_parsing_mode():
                    # Якщо парсинг переключили з Vast, не тримаємо інстанс активним.
                    self._orchestrator.force_shutdown("runtime_supervisor_disabled")
                    self._wait_next_tick()
                    continue

                if not self._orchestrator.is_enabled():
                    self._wait_next_tick()
                    continue

                if self._has_pending_llm_tasks():
                    self._orchestrator.ensure_runtime_ready()
                else:
                    self._orchestrator.handle_pool_drain(self._has_pending_llm_tasks)
            except Exception as e:
                logger.warning("Vast runtime supervisor tick failed: %s", e)
            self._wait_next_tick()

    def _wait_next_tick(self) -> None:
        self._stop_event.wait(self.poll_interval_sec)

    def _is_vast_parsing_mode(self) -> bool:
        provider = (getattr(self.settings, "llm_parsing_provider", "") or "").strip().lower()
        return provider == "vllm_remote"

    def _has_pending_llm_tasks(self) -> bool:
        try:
            if self._olx_repo.count({"detail.llm_pending": True}) > 0:
                return True
        except Exception:
            pass
        try:
            if self._prozorro_repo.count({"llm": {"$exists": False}, "auction_data.description": {"$exists": True}}) > 0:
                return True
        except Exception:
            pass
        return False
