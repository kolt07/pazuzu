# -*- coding: utf-8 -*-
"""
Міграція 050: колекція cadastral_polygon_query_cache.

Кешує списки cadastral_number після полігон-пошуку для LLM (пагінація по query_id).
"""

from __future__ import annotations

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.cadastral_polygon_query_cache_repository import (
    CadastralPolygonQueryCacheRepository,
)


def run_migration() -> bool:
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        repo = CadastralPolygonQueryCacheRepository()
        repo._ensure_indexes()
        print("Міграція 050: cadastral_polygon_query_cache ok.")
        return True
    except Exception as e:
        print("Помилка міграції 050:", e)
        return False


if __name__ == "__main__":
    import sys

    sys.exit(0 if run_migration() else 1)
