# -*- coding: utf-8 -*-
"""
Міграція 007: Колекція agent_activity_log для логування діяльності мультиагентної системи.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.agent_activity_log_repository import AgentActivityLogRepository


def run_migration() -> bool:
    """Створює колекцію та індекс по request_id для швидкої виборки логів запиту."""
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = AgentActivityLogRepository()
        repo.collection.create_index("request_id")
        repo.collection.create_index([("created_at", 1)])
        print("Міграція 007: колекція agent_activity_log та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 007:", e)
        return False
