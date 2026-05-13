# -*- coding: utf-8 -*-
"""
Міграція 044: колекція agent_runtime_settings (прапорці LLM-агента для Mini App).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.agent_runtime_settings_repository import AgentRuntimeSettingsRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = AgentRuntimeSettingsRepository()
        repo.collection.create_index("updated_at")
        print("Міграція 044: колекція agent_runtime_settings та індекси перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 044:", e)
        return False


if __name__ == "__main__":
    import sys

    sys.exit(0 if run_migration() else 1)
