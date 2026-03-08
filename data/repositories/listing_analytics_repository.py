# -*- coding: utf-8 -*-
"""
Репозиторій для збереження LLM-згенерованої аналітики оголошення.
Ключ: source + source_id (olx:url, prozorro:auction_id).
Аналітика: ціна за одиницю площі, місцезнаходження, оточення.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "listing_analytics"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class ListingAnalyticsRepository(BaseRepository):
    """Зберігання та отримання LLM-аналітики оголошення."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_source_id(self, source: str, source_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить аналітику за джерелом та ID."""
        doc = self.collection.find_one({"source": source, "source_id": source_id})
        return _normalize_doc(doc)

    def upsert(
        self,
        source: str,
        source_id: str,
        analysis_text: str,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """Зберігає або оновлює аналітику."""
        if not source or not source_id or not analysis_text:
            return False
        now = datetime.now(timezone.utc)
        doc = {
            "source": source,
            "source_id": source_id,
            "analysis_text": analysis_text.strip(),
            "metadata": metadata or {},
            "analysis_at": now,
            "updated_at": now,
        }
        result = self.collection.update_one(
            {"source": source, "source_id": source_id},
            {"$set": doc},
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def ensure_index(self) -> None:
        """Створює індекси."""
        try:
            self.collection.create_index(
                [("source", 1), ("source_id", 1)],
                unique=True,
            )
            self.collection.create_index("analysis_at")
        except Exception:
            pass
