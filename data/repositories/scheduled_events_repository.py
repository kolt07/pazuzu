# -*- coding: utf-8 -*-
"""
Репозиторій для запланованих подій (планувальник).
Події: оновлення даних з джерел, регламентні звіти, нагадування.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


# Типи подій
EVENT_TYPE_DATA_UPDATE = "data_update"
EVENT_TYPE_SCHEDULED_REPORT = "scheduled_report"
EVENT_TYPE_REMINDER = "reminder"
EVENT_TYPE_DATA_PROFILE = "data_profile"

# Область: система або користувач
SCOPE_SYSTEM = "system"
SCOPE_USER = "user"


class ScheduledEventsRepository(BaseRepository):
    """
    Репозиторій запланованих подій.
    Поля: event_type, scope, user_id, created_by, is_active, schedule, payload, last_run_at, next_run_at, created_at, updated_at.
    """

    def __init__(self):
        super().__init__("scheduled_events")
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("is_active")
            self.collection.create_index([("user_id", 1), ("is_active", 1)])
            self.collection.create_index("next_run_at")
            self._indexes_created = True
        except Exception:
            pass

    def create_event(
        self,
        event_type: str,
        scope: str,
        schedule: Dict[str, Any],
        payload: Dict[str, Any],
        user_id: Optional[int] = None,
        created_by: Optional[int] = None,
        title: Optional[str] = None,
    ) -> str:
        """
        Створює заплановану подію.

        Args:
            event_type: data_update | scheduled_report | reminder
            scope: system | user
            schedule: { type: "cron"|"once", cron: {...} або run_at: datetime }
            payload: параметри події (залежать від event_type)
            user_id: для scope=user — кому належить подія (Telegram chat_id)
            created_by: хто створив (для перевірки прав; для data_update — має бути admin)
            title: описова назва події (опціонально)

        Returns:
            ID створеного документа
        """
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        next_run = self._compute_next_run(schedule)
        # для cron next_run залишається None — планувальник оновить після першого запуску
        doc = {
            "event_type": event_type,
            "scope": scope,
            "user_id": user_id,
            "created_by": created_by,
            "title": title,
            "is_active": True,
            "schedule": schedule,
            "payload": payload,
            "last_run_at": None,
            "next_run_at": next_run,
            "created_at": now,
            "updated_at": now,
        }
        return self.create(doc)

    def _compute_next_run(self, schedule: Dict[str, Any]) -> Optional[datetime]:
        """Обчислює наступний час запуску за розкладом. Для cron повертає None (планувальник сам рахує)."""
        if schedule.get("type") == "once" and schedule.get("run_at"):
            run_at = schedule["run_at"]
            if isinstance(run_at, str):
                run_at = datetime.fromisoformat(run_at.replace("Z", "+00:00"))
            if run_at.tzinfo is None:
                run_at = run_at.replace(tzinfo=timezone.utc)
            return run_at
        return None

    def get_active_events(
        self,
        user_id: Optional[int] = None,
        event_type: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """
        Повертає активні події, опційно за user_id та/або event_type.
        """
        self._ensure_indexes()
        filt: Dict[str, Any] = {"is_active": True}
        if user_id is not None:
            filt["user_id"] = user_id
        if event_type is not None:
            filt["event_type"] = event_type
        return self.find_many(filter=filt, sort=[("next_run_at", 1)])

    def get_event_by_id(self, event_id: str) -> Optional[Dict[str, Any]]:
        """Повертає подію за ID."""
        return self.find_by_id(event_id)

    def set_last_run(self, event_id: str, next_run_at: Optional[datetime] = None) -> bool:
        """Оновлює last_run_at та опційно next_run_at після виконання."""
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        update: Dict[str, Any] = {"$set": {"last_run_at": now, "updated_at": now}}
        if next_run_at is not None:
            update["$set"]["next_run_at"] = next_run_at
        return self.collection.update_one(
            {"_id": self._oid(event_id)},
            update,
        ).modified_count > 0

    def _oid(self, event_id: str):
        from bson import ObjectId
        return ObjectId(event_id) if isinstance(event_id, str) else event_id

    def deactivate(self, event_id: str) -> bool:
        """Вимикає подію (наприклад після разового нагадування)."""
        return self.update_by_id(
            event_id,
            {"$set": {"is_active": False, "updated_at": datetime.now(timezone.utc)}},
        )

    def update_next_run(self, event_id: str, next_run_at: datetime) -> bool:
        """Оновлює час наступного запуску (для cron після кожного виконання)."""
        return self.update_by_id(
            event_id,
            {"$set": {"next_run_at": next_run_at, "updated_at": datetime.now(timezone.utc)}},
        )

    def list_for_user(self, user_id: int, include_system: bool = False) -> List[Dict[str, Any]]:
        """
        Список подій для користувача: його власні + опційно системні (scope=system).
        """
        self._ensure_indexes()
        if include_system:
            filt = {"is_active": True, "$or": [{"user_id": user_id}, {"scope": SCOPE_SYSTEM}]}
        else:
            filt = {"is_active": True, "user_id": user_id}
        return self.find_many(filter=filt, sort=[("next_run_at", 1)])
