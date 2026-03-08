# -*- coding: utf-8 -*-
"""
Репозиторій кешу геокодування (Google Maps Geocoding API).
Ключ — хеш текстового запиту; значення — список результатів (координати, formatted_address тощо).
"""

from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "geocode_cache"


class GeocodeCacheRepository(BaseRepository):
    """Кеш результатів геокодування: query_hash -> results."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("query_hash", unique=True)
            self.collection.create_index("created_at")
            self._indexes_created = True
        except Exception:
            pass

    def find_by_query_hash(self, query_hash: str) -> Optional[Dict[str, Any]]:
        """Повертає кешований результат за хешем запиту."""
        self._ensure_indexes()
        doc = self.collection.find_one({"query_hash": query_hash})
        if doc and "_id" in doc and hasattr(doc["_id"], "binary"):
            doc["_id"] = str(doc["_id"])
        return doc

    def save_result(self, query_hash: str, query_text: str, result: List[Dict[str, Any]]) -> str:
        """Зберігає або оновлює результат геокодування."""
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        existing = self.find_by_query_hash(query_hash)
        document = {
            "query_hash": query_hash,
            "query_text": query_text,
            "result": result,
            "created_at": now,
        }
        if existing:
            self.collection.update_one(
                {"query_hash": query_hash},
                {"$set": {"result": result, "query_text": query_text, "created_at": now}},
            )
            return str(existing["_id"])
        return self.create(document)

    def count_api_calls_by_day(self, days: int = 60) -> List[Dict[str, Any]]:
        """
        Агрегація запитів до Google Geocoding API по днях за останні N днів.
        Кожен новий запис у кеші = один виклик API (cache miss).
        Повертає список {date: "YYYY-MM-DD", count: N}.
        """
        self._ensure_indexes()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=days)
            pipeline = [
                {"$match": {"created_at": {"$gte": cutoff}}},
                {
                    "$group": {
                        "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$created_at"}},
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"_id": 1}},
                {"$project": {"date": "$_id", "count": 1, "_id": 0}},
            ]
            cursor = self.collection.aggregate(pipeline)
            return list(cursor)
        except Exception:
            return []

    def count_total(self) -> int:
        """Загальна кількість записів у кеші (унікальних запитів до API)."""
        self._ensure_indexes()
        try:
            return self.collection.count_documents({})
        except Exception:
            return 0

    def count_last_month(self) -> int:
        """Кількість нових записів (викликів API) за останні 30 днів."""
        self._ensure_indexes()
        try:
            cutoff = datetime.now(timezone.utc) - timedelta(days=30)
            return self.collection.count_documents({"created_at": {"$gte": cutoff}})
        except Exception:
            return 0
