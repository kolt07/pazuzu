# -*- coding: utf-8 -*-
"""
Міграція 008: Колекція scheduled_events для планувальника подій.
Події: оновлення даних з джерел, регламентні звіти, нагадування.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.scheduled_events_repository import ScheduledEventsRepository


def run_migration() -> bool:
    """Створює колекцію та індекси для запланованих подій."""
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = ScheduledEventsRepository()
        repo._ensure_indexes()
        print("Міграція 008: колекція scheduled_events та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 008:", e)
        return False
