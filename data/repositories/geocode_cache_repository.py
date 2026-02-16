# -*- coding: utf-8 -*-
"""
Репозиторій кешу геокодування (Google Maps Geocoding API).
Ключ — хеш текстового запиту; значення — список результатів (координати, formatted_address тощо).
"""

from datetime import datetime, timezone
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
