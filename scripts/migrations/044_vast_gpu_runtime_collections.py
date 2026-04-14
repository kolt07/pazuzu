# -*- coding: utf-8 -*-
"""
Міграція 044: колекції для Vast.ai runtime settings та GPU runtime sessions.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.gpu_runtime_sessions_repository import GpuRuntimeSessionsRepository
from data.repositories.vast_runtime_settings_repository import VastRuntimeSettingsRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        VastRuntimeSettingsRepository()._ensure_indexes()
        GpuRuntimeSessionsRepository()._ensure_indexes()
        print("Міграція 044: vast_runtime_settings + gpu_runtime_sessions готові.")
        return True
    except Exception as e:
        print("Помилка міграції 044:", e)
        return False


if __name__ == "__main__":
    import sys

    sys.exit(0 if run_migration() else 1)
