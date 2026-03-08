# -*- coding: utf-8 -*-
"""
Міграція 010: Колекція export_daily_count для ліміту експортів на користувача за добу.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.export_daily_count_repository import ExportDailyCountRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = ExportDailyCountRepository()
        repo.ensure_indexes()
        print("Міграція 010: колекція export_daily_count та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 010:", e)
        return False
