# -*- coding: utf-8 -*-
"""
Міграція 062: колекція user_activity_log для журналу активності користувачів Mini App.

Запуск: py scripts/migrations/062_user_activity_log_collection.py
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.user_activity_log_repository import UserActivityLogRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = UserActivityLogRepository()
        repo._ensure_indexes()
        print("Міграція 062: колекція user_activity_log та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 062:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
