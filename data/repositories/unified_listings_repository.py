# -*- coding: utf-8 -*-
"""
Репозиторій для зведеної таблиці оголошень (unified_listings).
Об'єднує дані з OLX та ProZorro в єдину структуру.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository
from utils.olx_url import normalize_olx_listing_url

COLLECTION_NAME = "unified_listings"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Повертає документ з _id у вигляді рядка (якщо це ObjectId)."""
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class UnifiedListingsRepository(BaseRepository):
    """Репозиторій для роботи зі зведеною таблицею оголошень."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            # Унікальний індекс по джерелу та ID в джерелі
            self.collection.create_index(
                [("source", 1), ("source_id", 1)], unique=True
            )
            # Індекси для швидкого пошуку
            self.collection.create_index("source")
            self.collection.create_index("status")
            self.collection.create_index("property_type")
            self.collection.create_index("source_updated_at")
            self.collection.create_index("system_updated_at")
            # Root geo для геопошуку
            self.collection.create_index("region")
            self.collection.create_index("city")
            self.collection.create_index("oblast_raion")
            self.collection.create_index("city_district")
            self._indexes_created = True
        except Exception:
            pass

    def find_by_source_id(self, source: str, source_id: str) -> Optional[Dict[str, Any]]:
        """
        Знаходить оголошення за джерелом та ID в джерелі.
        Для OLX: приймає URL з query (search_reason) — шукає за канонічним.
        
        Args:
            source: Джерело даних ("olx" або "prozorro")
            source_id: ID в джерелі (URL для OLX, auction_id для ProZorro)
            
        Returns:
            Документ або None, якщо не знайдено
        """
        self._ensure_indexes()
        doc = self.collection.find_one({"source": source, "source_id": source_id})
        if doc:
            return _normalize_doc(doc)
        if source.lower() == "olx" and source_id and "?" in source_id:
            canonical = normalize_olx_listing_url(source_id)
            if canonical:
                doc = self.collection.find_one({"source": "olx", "source_id": canonical})
        return _normalize_doc(doc)

    def upsert_listing(self, listing_data: Dict[str, Any]) -> bool:
        """
        Створює або оновлює оголошення в зведеній таблиці.
        
        Args:
            listing_data: Словник з даними оголошення (має містити source та source_id)
            
        Returns:
            True якщо успішно
        """
        if not listing_data.get("source") or not listing_data.get("source_id"):
            return False
        
        self._ensure_indexes()
        source = listing_data["source"]
        source_id = listing_data["source_id"]
        now = datetime.now(timezone.utc)
        
        # Перевіряємо, чи існує запис
        existing = self.find_by_source_id(source, source_id)
        
        if existing:
            # Оновлюємо існуючий
            update_data = {
                "$set": {
                    **listing_data,
                    "system_updated_at": now,
                }
            }
            # Видаляємо _id з update_data, якщо він там є
            if "_id" in update_data["$set"]:
                del update_data["$set"]["_id"]
            result = self.collection.update_one(
                {"source": source, "source_id": source_id},
                update_data,
            )
            return result.modified_count > 0
        else:
            # Створюємо новий
            listing_data["system_updated_at"] = now
            listing_data["created_at"] = now
            self.collection.insert_one(listing_data)
            return True

    def find_many(
        self,
        filter: Optional[Dict[str, Any]] = None,
        sort: Optional[List[tuple]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """Знаходить кілька документів з нормалізацією _id."""
        self._ensure_indexes()
        docs = super().find_many(filter=filter, sort=sort, limit=limit, skip=skip)
        return [_normalize_doc(d) for d in docs]

    def get_by_ids(self, ids: List[str]) -> List[Dict[str, Any]]:
        """
        Повертає оголошення за списком ідентифікаторів.
        Ідентифікатор може бути _id (ObjectId у вигляді рядка) або комбінація source:source_id.
        
        Args:
            ids: Список _id або "source:source_id"
        
        Returns:
            Список документів
        """
        if not ids:
            return []
        
        from bson import ObjectId
        
        by_object_id = []
        by_source_id = []
        
        for i in ids:
            s = (i or "").strip()
            if not s:
                continue
            
            # Перевіряємо формат "source:source_id"
            if ":" in s and not s.startswith("http"):
                parts = s.split(":", 1)
                if len(parts) == 2:
                    by_source_id.append({"source": parts[0], "source_id": parts[1]})
                    continue
            
            # Перевіряємо ObjectId
            if len(s) == 24:
                try:
                    by_object_id.append(ObjectId(s))
                except Exception:
                    pass
        
        criteria = None
        if by_object_id and by_source_id:
            criteria = {
                "$or": [
                    {"_id": {"$in": by_object_id}},
                    {"$or": [{"source": d["source"], "source_id": d["source_id"]} for d in by_source_id]}
                ]
            }
        elif by_object_id:
            criteria = {"_id": {"$in": by_object_id}}
        elif by_source_id:
            criteria = {
                "$or": [{"source": d["source"], "source_id": d["source_id"]} for d in by_source_id]
            }
        else:
            return []
        
        self._ensure_indexes()
        docs = list(self.collection.find(criteria))
        return [_normalize_doc(d) for d in docs]

    def delete_by_source_id(self, source: str, source_id: str) -> int:
        """
        Видаляє оголошення за джерелом та ID в джерелі.
        Для OLX: приймає URL, шукає за канонічним.
        Повертає кількість видалених документів (0 або 1).
        """
        if not source or not source_id:
            return 0
        result = self.collection.delete_one({"source": source, "source_id": source_id})
        if result.deleted_count > 0:
            return result.deleted_count
        if source.lower() == "olx" and source_id:
            canonical = normalize_olx_listing_url(source_id)
            if canonical and canonical != source_id:
                result = self.collection.delete_one({"source": "olx", "source_id": canonical})
        return result.deleted_count

    def update_real_estate_object_refs(
        self,
        source: str,
        source_id: str,
        refs: List[Dict[str, Any]],
    ) -> bool:
        """
        Оновлює посилання на об'єкти нерухомого майна (ОНМ).

        Args:
            source: Джерело (olx, prozorro)
            source_id: ID в джерелі
            refs: Масив {object_id: str, role: str}

        Returns:
            True якщо оновлено
        """
        if not refs:
            return False
        result = self.collection.update_one(
            {"source": source, "source_id": source_id},
            {"$set": {"real_estate_object_refs": refs}},
        )
        return result.modified_count > 0
