# -*- coding: utf-8 -*-
"""
Міграція 006: Колекція agent_temp_exports для тимчасових вибірок агента (експорт у файл з тимчасової колекції).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.agent_temp_exports_repository import AgentTempExportsRepository


def run_migration() -> bool:
    """Створює колекцію та індекс по batch_id для швидкого вибору та видалення."""
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = AgentTempExportsRepository()
        repo.collection.create_index("batch_id")
        print("Міграція 006: колекція agent_temp_exports та індекс batch_id створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 006:", e)
        return False
