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


def _olx_url_variants(url: str) -> list:
    """Повертає варіанти URL для пошуку (різні домени, протоколи, без query)."""
    variants = [url]
    u = url.strip()
    if "?" in u:
        variants.append(u.split("?")[0])
    if u.startswith("https://"):
        variants.append("http://" + u[8:])
    elif u.startswith("http://"):
        variants.append("https://" + u[7:])
    if "www.olx.ua" in u:
        variants.append(u.replace("www.olx.ua", "olx.ua"))
    elif "olx.ua" in u and "www." not in u:
        variants.append(u.replace("olx.ua", "www.olx.ua"))
    if u.endswith("/"):
        variants.append(u.rstrip("/"))
    elif not u.endswith("/"):
        variants.append(u + "/")
    return list(dict.fromkeys(variants))


class OlxListingsRepository(BaseRepository):
    """Робота з колекцією оголошень OLX (пошук + деталі)."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_url(self, url: str) -> Optional[Dict[str, Any]]:
        """Знаходить оголошення за URL (url зберігається в полі url). Пробує варіанти нормалізації."""
        if not url or not url.strip():
            return None
        url = url.strip()
        doc = self.collection.find_one({"url": url})
        if doc:
            return _normalize_doc(doc)
        variants = _olx_url_variants(url)
        for v in variants:
            if v != url:
                doc = self.collection.find_one({"url": v})
                if doc:
                    return _normalize_doc(doc)
        return None

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
        url_set: set = set()
        by_object_id = []
        for i in ids:
            s = (i or "").strip()
            if not s:
                continue
            if s.startswith("http://") or s.startswith("https://"):
                url_set.update(_olx_url_variants(s))
            elif len(s) == 24:
                try:
                    by_object_id.append(ObjectId(s))
                except Exception:
                    pass
            else:
                url_set.add(s)
        by_url = list(url_set)
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
