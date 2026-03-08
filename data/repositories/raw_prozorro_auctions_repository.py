# -*- coding: utf-8 -*-
"""
Репозиторій сирих аукціонів ProZorro (колекція raw_prozorro_auctions).

Phase 1 pipeline: запис сирих даних з API без LLM. Поля fetch_context, approximate_region.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "raw_prozorro_auctions"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class RawProzorroAuctionsRepository(BaseRepository):
    """Робота з колекцією сирих аукціонів ProZorro (без LLM-обробки)."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_auction_id(self, auction_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить сирий запис за auction_id."""
        if not auction_id or not str(auction_id).strip():
            return None
        doc = self.collection.find_one({"auction_id": str(auction_id).strip()})
        return _normalize_doc(doc)

    def upsert_raw(
        self,
        auction_id: str,
        auction_data: Dict[str, Any],
        fetch_context: Optional[Dict[str, Any]] = None,
        approximate_region: Optional[str] = None,
    ) -> bool:
        """
        Створює або оновлює сирий запис. Повертає True якщо запис додано/оновлено.
        """
        if not auction_id or not str(auction_id).strip():
            return False
        auction_id = str(auction_id).strip()
        now = datetime.now(timezone.utc)
        set_fields: Dict[str, Any] = {
            "auction_id": auction_id,
            "auction_data": auction_data or {},
            "loaded_at": now,
            "updated_at": now,
        }
        if fetch_context is not None:
            set_fields["fetch_context"] = fetch_context
        if approximate_region is not None:
            set_fields["approximate_region"] = approximate_region

        result = self.collection.update_one(
            {"auction_id": auction_id},
            {"$set": set_fields},
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def get_by_auction_ids(self, auction_ids: List[str]) -> List[Dict[str, Any]]:
        """Повертає сирі записи за списком auction_id."""
        if not auction_ids:
            return []
        ids = [str(i).strip() for i in auction_ids if i]
        if not ids:
            return []
        cursor = self.collection.find({"auction_id": {"$in": list(set(ids))}})
        return [_normalize_doc(d) for d in cursor]

    def ensure_index(self) -> None:
        """Створює індекси (дубль міграції для явного виклику)."""
        self.collection.create_index("auction_id", unique=True)
        self.collection.create_index("loaded_at")
        self.collection.create_index("approximate_region")
