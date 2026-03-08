# -*- coding: utf-8 -*-
"""
Міграція 009: Колекція artifacts для збереження згенерованих файлів з TTL.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.artifact_repository import ArtifactRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = ArtifactRepository()
        repo.ensure_indexes()
        print("Міграція 009: колекція artifacts та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 009:", e)
        return False
