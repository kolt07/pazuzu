# -*- coding: utf-8 -*-
"""
Репозиторій сирих оголошень OLX (колекція raw_olx_listings).

Phase 1 pipeline: запис сирих даних без LLM. Поля fetch_filters, approximate_region.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository
from utils.olx_url import normalize_olx_listing_url
from utils.hash_utils import calculate_search_data_hash

COLLECTION_NAME = "raw_olx_listings"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class RawOlxListingsRepository(BaseRepository):
    """Робота з колекцією сирих оголошень OLX (без LLM-обробки)."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Знаходить сирий запис за URL."""
        if not url or not url.strip():
            return None
        url = (normalize_olx_listing_url(url.strip()) or url.strip()).strip()
        doc = self.collection.find_one({"url": url})
        return _normalize_doc(doc)

    def upsert_raw(
        self,
        url: str,
        search_data: Dict[str, Any],
        detail: Optional[Dict[str, Any]] = None,
        fetch_filters: Optional[Dict[str, Any]] = None,
        approximate_region: Optional[str] = None,
    ) -> bool:
        """
        Створює або оновлює сирий запис. Повертає True якщо запис додано/оновлено.
        """
        if not url or not url.strip():
            return False
        url = (normalize_olx_listing_url(url.strip()) or url.strip()).strip()
        now = datetime.now(timezone.utc)
        search_data_dict = search_data or {}
        search_data_hash = calculate_search_data_hash(search_data_dict)
        doc = {
            "url": url,
            "search_data": search_data_dict,
            "search_data_hash": search_data_hash,
            "detail": detail,
            "loaded_at": now,
            "updated_at": now,
        }
        if fetch_filters is not None:
            doc["fetch_filters"] = fetch_filters
        if approximate_region is not None:
            doc["approximate_region"] = approximate_region

        set_fields: Dict[str, Any] = {
            "search_data": doc["search_data"],
            "search_data_hash": doc["search_data_hash"],
            "detail": doc["detail"],
            "loaded_at": doc["loaded_at"],
            "updated_at": doc["updated_at"],
        }
        if fetch_filters is not None:
            set_fields["fetch_filters"] = fetch_filters
        if approximate_region is not None:
            set_fields["approximate_region"] = approximate_region

        result = self.collection.update_one(
            {"url": url},
            {"$set": set_fields},
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def get_by_urls(self, urls: List[str]) -> List[Dict[str, Any]]:
        """Повертає сирі записи за списком URL."""
        if not urls:
            return []
        normalized = []
        for u in urls:
            s = (u or "").strip()
            if s:
                normalized.append((normalize_olx_listing_url(s) or s).strip())
        if not normalized:
            return []
        cursor = self.collection.find({"url": {"$in": list(set(normalized))}})
        return [_normalize_doc(d) for d in cursor]

    def ensure_index(self) -> None:
        """Створює індекси (дубль міграції для явного виклику)."""
        self.collection.create_index("url", unique=True)
        self.collection.create_index("loaded_at")
        self.collection.create_index("approximate_region")

    def count_by_source(self, source: str) -> int:
        """Повертає кількість записів із fetch_filters.source == source (напр. 'clicker')."""
        return self.collection.count_documents({"fetch_filters.source": source})

    def get_recent_by_source(
        self,
        source: str,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Повертає останні записи з fetch_filters.source == source, відсортовані за loaded_at.
        Повертає url, search_data.title, loaded_at.
        """
        cursor = (
            self.collection.find(
                {"fetch_filters.source": source},
                {"url": 1, "search_data.title": 1, "loaded_at": 1},
            )
            .sort("loaded_at", -1)
            .limit(max(limit, 1))
        )
        return [_normalize_doc(d) for d in cursor]

    def get_recent_for_health_check(
        self,
        days: int = 7,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """
        Повертає останні raw-оголошення для діагностики роботи скрапера.

        Args:
            days: Глибина в днях по полю loaded_at.
            limit: Максимальна кількість документів.
        """
        from datetime import datetime, timezone, timedelta

        now = datetime.now(timezone.utc)
        cutoff = now - timedelta(days=max(days, 1))
        cursor = (
            self.collection.find(
                {"loaded_at": {"$gte": cutoff}},
                {"url": 1, "detail": 1, "loaded_at": 1},
            )
            .sort("loaded_at", -1)
            .limit(max(limit, 1))
        )
        return [_normalize_doc(d) for d in cursor]
