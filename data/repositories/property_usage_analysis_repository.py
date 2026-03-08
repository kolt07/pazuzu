# -*- coding: utf-8 -*-
"""
Репозиторій для збереження попереднього аналізу використання об'єкта оголошення.
Ключ: source + source_id (olx:url, prozorro:auction_id).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "property_usage_analysis"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class PropertyUsageAnalysisRepository(BaseRepository):
    """Зберігання та отримання аналізу використання об'єкта."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_source_id(self, source: str, source_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить аналіз за джерелом та ID."""
        doc = self.collection.find_one({"source": source, "source_id": source_id})
        return _normalize_doc(doc)

    def upsert(
        self,
        source: str,
        source_id: str,
        existing_usage: List[str],
        geo_analysis: Dict[str, Any],
        usage_suggestions: List[Dict[str, Any]],
        address_for_geocode: Optional[str] = None,
    ) -> bool:
        """Зберігає або оновлює аналіз."""
        if not source or not source_id:
            return False
        now = datetime.now(timezone.utc)
        doc = {
            "source": source,
            "source_id": source_id,
            "existing_usage": existing_usage,
            "geo_analysis": geo_analysis,
            "usage_suggestions": usage_suggestions,
            "address_for_geocode": address_for_geocode,
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
