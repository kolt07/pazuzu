# -*- coding: utf-8 -*-
"""
Репозиторій оголошень OLX: збереження даних зі сторінки пошуку та опційно зі сторінки оголошення (detail).
Ідентифікатор — URL оголошення.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "olx_listings"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Повертає документ з _id у вигляді рядка (якщо це ObjectId)."""
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class OlxListingsRepository(BaseRepository):
    """Робота з колекцією оголошень OLX (пошук + деталі)."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Знаходить оголошення за URL (url зберігається в полі url або як _id)."""
        if not url or not url.strip():
            return None
        doc = self.collection.find_one({"url": url.strip()})
        return _normalize_doc(doc)

    def upsert_listing(
        self,
        url: str,
        search_data: Dict[str, Any],
        detail: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Створює або оновлює оголошення. url — унікальний ідентифікатор.
        search_data — дані зі сторінки пошуку; detail — опційно дані зі сторінки оголошення.
        """
        if not url or not url.strip():
            return False
        url = url.strip()
        now = datetime.now(timezone.utc)
        update: Dict[str, Any] = {
            "$set": {
                "url": url,
                "search_data": search_data,
                "updated_at": now,
            }
        }
        if detail is not None:
            update["$set"]["detail"] = detail
        update["$setOnInsert"] = {"created_at": now}
        result = self.collection.update_one(
            {"url": url},
            update,
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def ensure_index(self) -> None:
        """Створює унікальний індекс по url якщо його ще немає."""
        self.collection.create_index("url", unique=True)

    def get_all_for_export(self, limit: Optional[int] = None) -> List[Dict[str, Any]]:
        """Повертає всі оголошення для експорту (найновіші спочатку)."""
        cursor = self.collection.find({}).sort("updated_at", -1)
        if limit is not None:
            cursor = cursor.limit(limit)
        docs = list(cursor)
        return [_normalize_doc(d) for d in docs]

    def get_by_ids(self, ids: List[str]) -> List[Dict[str, Any]]:
        """
        Повертає оголошення за списком ідентифікаторів.
        Ідентифікатор може бути URL оголошення або _id (ObjectId у вигляді рядка).

        Args:
            ids: Список URL або _id

        Returns:
            Список документів (порядок може не збігатися з порядком ids)
        """
        if not ids:
            return []
        from bson import ObjectId
        by_url = []
        by_object_id = []
        for i in ids:
            s = (i or "").strip()
            if not s:
                continue
            if s.startswith("http://") or s.startswith("https://"):
                by_url.append(s)
            elif len(s) == 24:
                try:
                    by_object_id.append(ObjectId(s))
                except Exception:
                    pass
            else:
                by_url.append(s)
        criteria = None
        if by_url and by_object_id:
            criteria = {"$or": [{"url": {"$in": by_url}}, {"_id": {"$in": by_object_id}}]}
        elif by_url:
            criteria = {"url": {"$in": by_url}}
        elif by_object_id:
            criteria = {"_id": {"$in": by_object_id}}
        else:
            return []
        docs = list(self.collection.find(criteria))
        return [_normalize_doc(d) for d in docs]
