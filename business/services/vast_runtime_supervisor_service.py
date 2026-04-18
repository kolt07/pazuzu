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
import time
from typing import Callable, Optional

from config.settings import Settings
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from business.services.source_data_load_service import is_source_load_running
from business.services.task_queue_service import TaskQueueService
from business.services.vast_ai_client import VastAiClient
from business.services.vllm_runtime_orchestrator import get_shared_vllm_runtime_orchestrator

logger = logging.getLogger(__name__)


class VastRuntimeSupervisorService:
    """Окремий фон-процес (daemon thread) керування Vast runtime."""

    LOW_BALANCE_THRESHOLD_USD = 1.0
    BALANCE_CHECK_INTERVAL_SEC = 60

    def __init__(
        self,
        settings: Settings,
        poll_interval_sec: int = 10,
        notify_admins_fn: Optional[Callable[[str], bool]] = None,
    ) -> None:
        self.settings = settings
        self.poll_interval_sec = max(3, int(poll_interval_sec))
        self.notify_admins_fn = notify_admins_fn
        self._stop_event = threading.Event()
        self._thread: Optional[threading.Thread] = None
        self._orchestrator = get_shared_vllm_runtime_orchestrator()
        self._task_queue = TaskQueueService(settings)
        self._olx_repo = OlxListingsRepository()
        self._prozorro_repo = ProZorroAuctionsRepository()
        self._last_balance_check_ts: float = 0.0
        self._low_balance_alert_active = False

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

                self._check_low_balance()
                has_pending = self._has_pending_llm_tasks()
                source_load_active = self._has_active_source_load_tasks()
                if source_load_active:
                    self._orchestrator.mark_source_load_activity()
                has_instance = bool(self._orchestrator.get_observability_status().get("instance_id"))

                if has_pending:
                    self._orchestrator.ensure_runtime_ready()
                elif source_load_active and not has_instance:
                    # Прогрів на старті source-load: інстанс піднімаємо наперед.
                    self._orchestrator.ensure_runtime_ready()
                else:
                    self._orchestrator.handle_pool_drain(
                        self._has_pending_llm_tasks,
                        is_source_load_running,
                    )
            except Exception as e:
                logger.warning("Vast runtime supervisor tick failed: %s", e)
            self._wait_next_tick()

    def _wait_next_tick(self) -> None:
        self._stop_event.wait(self.poll_interval_sec)

    def _is_vast_parsing_mode(self) -> bool:
        provider = (getattr(self.settings, "llm_parsing_provider", "") or "").strip().lower()
        return provider == "vllm_remote"

    def _check_low_balance(self) -> None:
        now = time.time()
        if now - self._last_balance_check_ts < self.BALANCE_CHECK_INTERVAL_SEC:
            return
        self._last_balance_check_ts = now
        cfg = self._orchestrator._settings_svc.get_settings()
        api_key = str(cfg.get("vast_api_key") or "").strip()
        if not api_key:
            self._low_balance_alert_active = False
            return
        try:
            client = VastAiClient(api_key=api_key, timeout_sec=30)
            user_payload = client.get_current_user()
            balance = self._extract_available_balance_usd(user_payload)
        except Exception as e:
            logger.debug("Vast balance check failed: %s", e)
            return

        threshold = float(self.LOW_BALANCE_THRESHOLD_USD)
        if balance >= threshold:
            if self._low_balance_alert_active:
                logger.info("Vast balance recovered above threshold: %.2f USD", balance)
            self._low_balance_alert_active = False
            return

        if self._low_balance_alert_active:
            return

        logger.warning("Vast balance is low: %.2f USD", balance)
        message = (
            "⚠️ Низький баланс Vast.ai.\n"
            f"Поточний баланс: ${balance:.2f}\n"
            f"Поріг сповіщення: ${threshold:.2f}\n"
            "Поповніть баланс, щоб уникнути автоматичної зупинки або втрати runtime."
        )
        delivered = False
        if callable(self.notify_admins_fn):
            try:
                delivered = bool(self.notify_admins_fn(message))
            except Exception as e:
                logger.warning("Failed to notify admins about low Vast balance: %s", e)
        self._low_balance_alert_active = delivered

    @staticmethod
    def _extract_available_balance_usd(user_payload: dict) -> float:
        """
        Vast може повертати одночасно `balance` і `credit`.
        Для runtime-алерта використовуємо фактично доступний кредит, якщо він є.
        """
        credit = user_payload.get("credit")
        if credit is not None:
            return float(credit)
        return float(user_payload.get("balance") or 0.0)

    def _has_pending_llm_tasks(self) -> bool:
        if self._task_queue.is_enabled():
            return self._task_queue.has_pending_llm_tasks()
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

    def _has_active_source_load_tasks(self) -> bool:
        if is_source_load_running():
            return True
        if self._task_queue.is_enabled():
            return self._task_queue.has_active_source_load_tasks()
        return False
