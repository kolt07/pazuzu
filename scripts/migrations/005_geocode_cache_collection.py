# -*- coding: utf-8 -*-
"""
Міграція 005: Колекція geocode_cache для кешу результатів Google Geocoding API.

Створює колекцію geocode_cache та унікальний індекс по полю query_hash.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.geocode_cache_repository import GeocodeCacheRepository


def run_migration() -> bool:
    """Виконує міграцію: ініціалізація підключення та створення індексу."""
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = GeocodeCacheRepository()
        repo.collection.create_index("query_hash", unique=True)
        print("Міграція 005: колекція geocode_cache та індекс по query_hash створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 005:", e)
        return False
