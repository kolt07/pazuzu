# -*- coding: utf-8 -*-
"""
Сервіс-планувальник подій на майбутнє.
Події: оновлення даних з джерел (тільки для адмінів), регламентні звіти, нагадування (разові/постійні).
"""

import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from apscheduler.schedulers.background import BackgroundScheduler
from apscheduler.triggers.cron import CronTrigger
from apscheduler.triggers.date import DateTrigger

from config.settings import Settings
from data.repositories.scheduled_events_repository import (
    EVENT_TYPE_DATA_UPDATE,
    EVENT_TYPE_DATA_PROFILE,
    EVENT_TYPE_REMINDER,
    EVENT_TYPE_SCHEDULED_REPORT,
    SCOPE_SYSTEM,
    SCOPE_USER,
    ScheduledEventsRepository,
)
from utils.date_utils import KYIV_TZ

logger = logging.getLogger(__name__)


class SchedulerNotifier:
    """
    Інтерфейс для відправки повідомлень/файлів користувачу з контексту планувальника.
    Реалізація (наприклад, через Telegram) має викликати send_message/send_document у потоку бота.
    """

    def send_message(self, chat_id: int, text: str) -> None:
        """Надіслати текстове повідомлення. chat_id — Telegram user_id (chat_id)."""
        raise NotImplementedError

    def send_document(self, chat_id: int, file_path: str, filename: str, caption: Optional[str] = None) -> None:
        """Надіслати файл. file_path — шлях до файлу на диску."""
        raise NotImplementedError


class TelegramSchedulerNotifier(SchedulerNotifier):
    """Реалізація SchedulerNotifier через TelegramBotService (виклик з потоку планувальника)."""

    def __init__(self, telegram_bot_service: Any):
        self._bot = telegram_bot_service

    def send_message(self, chat_id: int, text: str) -> None:
        self._bot.send_message_to_chat_sync(chat_id, text)

    def send_document(self, chat_id: int, file_path: str, filename: str, caption: Optional[str] = None) -> None:
        self._bot.send_document_to_chat_sync(chat_id, file_path, filename, caption=caption)


class SchedulerService:
    """
    Планувальник подій: завантажує активні події з БД, додає їх у APScheduler,
    виконує дії (оновлення даних, звіт, нагадування) і оновлює last_run/next_run.
    """

    def __init__(
        self,
        settings: Settings,
        notifier: Optional[SchedulerNotifier] = None,
    ):
        self.settings = settings
        self.notifier = notifier
        self.repo = ScheduledEventsRepository()
        self.user_service = None  # встановлюється ззовні для перевірки is_admin
        self._scheduler = BackgroundScheduler(timezone=KYIV_TZ)
        self._job_id_to_event_id: Dict[str, str] = {}

    def set_user_service(self, user_service: Any) -> None:
        """Встановити UserService для перевірки прав (is_admin)."""
        self.user_service = user_service

    def _job_cleanup_expired_artifacts(self) -> None:
        """Видаляє артефакти з простроченим TTL (наприклад, Excel з чату старші 10 днів)."""
        try:
            from business.services.artifact_service import ArtifactService
            svc = ArtifactService()
            deleted = svc.delete_expired()
            if deleted:
                logger.info("Видалено прострочених артефактів: %d", deleted)
        except Exception as e:
            logger.exception("Помилка очищення артефактів: %s", e)

    def start(self) -> None:
        """Запускає планувальник і завантажує активні події з БД."""
        if self._scheduler.running:
            return
        self._load_jobs()
        self._scheduler.add_job(
            self._job_cleanup_expired_artifacts,
            trigger=CronTrigger(hour=3, minute=0, timezone=KYIV_TZ),
            id="cleanup_expired_artifacts",
            replace_existing=True,
        )
        self._scheduler.start()
        logger.info("Планувальник подій запущено")

    def shutdown(self, wait: bool = True) -> None:
        """Зупиняє планувальник."""
        if not self._scheduler.running:
            return
        self._scheduler.shutdown(wait=wait)
        self._job_id_to_event_id.clear()
        logger.info("Планувальник подій зупинено")

    def _load_jobs(self) -> None:
        """Завантажує всі активні події та додає їх у APScheduler."""
        events = self.repo.get_active_events()
        for ev in events:
            try:
                self._add_job_for_event(ev)
            except Exception as e:
                logger.exception("Не вдалося додати задачу для події %s: %s", ev.get("_id"), e)

    def _add_job_for_event(self, event: Dict[str, Any]) -> None:
        """Додає одну подію в APScheduler."""
        event_id = event["_id"]
        schedule = event.get("schedule") or {}
        trigger = None

        if schedule.get("type") == "once":
            run_at = schedule.get("run_at")
            if isinstance(run_at, str):
                run_at = datetime.fromisoformat(run_at.replace("Z", "+00:00"))
            if run_at and run_at.tzinfo is None:
                run_at = run_at.replace(tzinfo=timezone.utc)
            if run_at:
                trigger = DateTrigger(run_time=run_at, timezone=KYIV_TZ)
        else:
            # cron
            trigger = self._cron_trigger(schedule)

        if trigger is None:
            logger.warning("Подія %s: невизначений розклад, пропущено", event_id)
            return

        job_id = f"ev_{event_id}"
        self._scheduler.add_job(
            self._run_event,
            trigger=trigger,
            id=job_id,
            args=[event_id],
            replace_existing=True,
        )
        self._job_id_to_event_id[job_id] = event_id

    def _cron_trigger(self, schedule: Dict[str, Any]) -> CronTrigger:
        """Будує CronTrigger з полів schedule (minute, hour, day_of_week, day, month)."""
        return CronTrigger(
            minute=schedule.get("minute", 0),
            hour=schedule.get("hour", 0),
            day_of_week=schedule.get("day_of_week"),
            day=schedule.get("day"),
            month=schedule.get("month"),
            timezone=KYIV_TZ,
        )

    def _run_event(self, event_id: str) -> None:
        """Виконує одну подію за ID."""
        event = self.repo.get_event_by_id(event_id)
        if not event or not event.get("is_active"):
            return
        event_type = event.get("event_type")
        payload = event.get("payload") or {}
        user_id = event.get("user_id")
        chat_id = user_id  # для Telegram user_id == chat_id в особистих чатах

        try:
            if event_type == EVENT_TYPE_DATA_UPDATE:
                self._execute_data_update(event_id, payload)
            elif event_type == EVENT_TYPE_SCHEDULED_REPORT:
                self._execute_scheduled_report(event_id, payload, chat_id)
            elif event_type == EVENT_TYPE_REMINDER:
                self._execute_reminder(event_id, payload, chat_id)
            elif event_type == EVENT_TYPE_DATA_PROFILE:
                self._execute_data_profile(event_id, payload)
            else:
                logger.warning("Невідомий тип події: %s", event_type)
                return
        except Exception as e:
            logger.exception("Помилка виконання події %s: %s", event_id, e)
            if chat_id and self.notifier:
                self.notifier.send_message(chat_id, f"Помилка виконання запланованої події: {e}")
        finally:
            self._update_after_run(event_id, event)

    def _execute_data_update(self, event_id: str, payload: Dict[str, Any]) -> None:
        """Оновлення даних з джерел (ProZorro, OLX)."""
        from business.services.prozorro_service import ProZorroService
        from scripts.olx_scraper.run_update import run_olx_update

        days = payload.get("days", 1)
        sources = payload.get("sources", "all")
        if isinstance(sources, str) and sources == "all":
            sources = ["prozorro", "olx"]
        elif isinstance(sources, str):
            sources = [sources]

        if "prozorro" in sources:
            prozorro = ProZorroService(self.settings)
            prozorro.fetch_and_save_real_estate_auctions(days=days)
        if "olx" in sources:
            run_olx_update(settings=self.settings, days=days)

        try:
            from business.services.collection_knowledge_service import refresh_knowledge_after_sources
            refresh_knowledge_after_sources(sources)
        except Exception as e:
            logger.debug("Оновлення знань про колекції після data_update: %s", e)

        try:
            from business.services.price_analytics_service import PriceAnalyticsService
            analytics = PriceAnalyticsService()
            counts = analytics.rebuild_all()
            logger.info("Price analytics оновлено після data_update: %s", counts)
        except Exception as e:
            logger.warning("Помилка оновлення price analytics після data_update: %s", e)

        logger.info("Подія data_update %s виконано (days=%s)", event_id, days)

    def _execute_data_profile(self, event_id: str, payload: Dict[str, Any]) -> None:
        """Дослідження даних: профілювання колекцій (статистика по полях, топ значень)."""
        from business.services.collection_knowledge_service import CollectionKnowledgeService

        collection_names = payload.get("collection_names")
        sample_size = payload.get("sample_size")
        service = CollectionKnowledgeService(
            sample_size=sample_size or 5000,
        )
        result = service.run_profiling(collection_names=collection_names)
        logger.info("Подія data_profile %s виконано: %s", event_id, result)

    def _execute_scheduled_report(
        self,
        event_id: str,
        payload: Dict[str, Any],
        chat_id: Optional[int],
    ) -> None:
        """Формує звіт і надсилає файлом або текстом."""
        from utils.report_generator import ReportGenerator

        if not chat_id or not self.notifier:
            logger.warning("Регламентний звіт без chat_id або notifier, подія %s", event_id)
            return

        report_request = payload.get("report_request")
        if not report_request:
            self.notifier.send_message(chat_id, "Помилка: у події не задано report_request.")
            return

        delivery = payload.get("delivery", "file")
        title = payload.get("title", "Регламентний звіт")

        if delivery == "text":
            gen = ReportGenerator()
            result = gen.get_report_data(report_request)
            if not result.get("success"):
                self.notifier.send_message(chat_id, f"Помилка звіту: {result.get('error', 'Невідома помилка')}")
                return
            data = result.get("data", [])
            columns = result.get("columns", [])
            lines = [title, ""]
            for row in data:
                parts = [f"{row.get(col, '')}" for col in columns]
                lines.append(" | ".join(str(p) for p in parts))
            text = "\n".join(lines)
            self.notifier.send_message(chat_id, text[:4000] if len(text) > 4000 else text)
        else:
            gen = ReportGenerator()
            result = gen.generate_report(report_request, return_base64=False)
            if not result.get("success"):
                self.notifier.send_message(chat_id, f"Помилка звіту: {result.get('error', 'Невідома помилка')}")
                return
            file_path = result.get("url")
            filename = result.get("filename", "report.xlsx")
            if file_path and Path(file_path).exists():
                self.notifier.send_document(chat_id, file_path, filename, caption=title)
            else:
                self.notifier.send_message(chat_id, "Помилка: файл звіту не створено.")

        logger.info("Регламентний звіт події %s надіслано користувачу %s", event_id, chat_id)

    def _execute_reminder(
        self,
        event_id: str,
        payload: Dict[str, Any],
        chat_id: Optional[int],
    ) -> None:
        """Надсилає нагадування; якщо разове — деактивує подію."""
        if not chat_id or not self.notifier:
            logger.warning("Нагадування без chat_id або notifier, подія %s", event_id)
            return
        text = payload.get("text", "Нагадування")
        self.notifier.send_message(chat_id, text)
        if not payload.get("recurring", True):
            self.repo.deactivate(event_id)
        logger.info("Нагадування події %s надіслано", event_id)

    def _update_after_run(self, event_id: str, event: Dict[str, Any]) -> None:
        """Оновлює last_run_at та для cron — next_run_at у БД (для відображення)."""
        schedule = event.get("schedule") or {}
        next_run = None
        if schedule.get("type") == "cron":
            trigger = self._cron_trigger(schedule)
            now = datetime.now(KYIV_TZ)
            next_run = trigger.get_next_fire_time(None, now)
            if next_run and next_run.tzinfo is None:
                next_run = next_run.replace(tzinfo=KYIV_TZ)
        self.repo.set_last_run(event_id, next_run_at=next_run)

    def create_event(
        self,
        event_type: str,
        scope: str,
        schedule: Dict[str, Any],
        payload: Dict[str, Any],
        user_id: Optional[int] = None,
        created_by: Optional[int] = None,
        title: Optional[str] = None,
    ) -> tuple[bool, str]:
        """
        Створює заплановану подію.
        Для event_type=data_update перевіряє, що created_by — адміністратор.

        Returns:
            (success, event_id або повідомлення про помилку)
        """
        if event_type == EVENT_TYPE_DATA_UPDATE and self.user_service and created_by is not None:
            if not self.user_service.is_admin(created_by):
                return False, "Регламентне оновлення даних можуть налаштовувати лише адміністратори."

        event_id = self.repo.create_event(
            event_type=event_type,
            scope=scope,
            schedule=schedule,
            payload=payload,
            user_id=user_id,
            created_by=created_by,
            title=title,
        )
        event = self.repo.get_event_by_id(event_id)
        if event:
            try:
                self._add_job_for_event(event)
            except Exception as e:
                logger.exception("Не вдалося додати задачу для нової події %s: %s", event_id, e)
        return True, event_id

    def deactivate_event(self, event_id: str) -> bool:
        """Вимикає подію та видаляє її з планувальника."""
        ok = self.repo.deactivate(event_id)
        if ok:
            job_id = f"ev_{event_id}"
            try:
                self._scheduler.remove_job(job_id)
            except Exception:
                pass
            self._job_id_to_event_id.pop(job_id, None)
        return ok

    def list_events_for_user(self, user_id: int, include_system: bool = False) -> List[Dict[str, Any]]:
        """Список активних подій для користувача."""
        return self.repo.list_for_user(user_id, include_system=include_system)
