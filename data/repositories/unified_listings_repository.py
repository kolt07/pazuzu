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
            self.collection.create_index("deal_type")
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
            # modified_count==0, якщо BSON збігся з попереднім (напр. швидкі повторні sync);
            # ОНМ та інші побічні кроки все одно мають мати змогу відпрацювати.
            return bool(result.acknowledged and result.matched_count > 0)
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

    def find_by_source_keys(self, keys: List[Dict[str, str]]) -> List[Dict[str, Any]]:
        """Документи за списком {source, source_id}."""
        if not keys:
            return []
        olx_ids = [k.get("source_id") for k in keys if (k or {}).get("source") == "olx" and k.get("source_id")]
        pz_ids = [k.get("source_id") for k in keys if (k or {}).get("source") == "prozorro" and k.get("source_id")]
        ors = []
        if olx_ids:
            ors.append({"source": "olx", "source_id": {"$in": list(dict.fromkeys(olx_ids))}})
        if pz_ids:
            ors.append({"source": "prozorro", "source_id": {"$in": list(dict.fromkeys(pz_ids))}})
        if not ors:
            return []
        self._ensure_indexes()
        docs = list(self.collection.find({"$or": ors} if len(ors) > 1 else ors[0]))
        return [_normalize_doc(d) for d in docs]

    def find_listings(
        self,
        region: Optional[str] = None,
        city: Optional[str] = None,
        property_type: Optional[str] = None,
        property_type_contains: Optional[str] = None,
        city_contains: Optional[str] = None,
        region_contains: Optional[str] = None,
        status: Optional[str] = None,
        source: Optional[str] = None,
        building_area_min: Optional[float] = None,
        building_area_max: Optional[float] = None,
        land_area_min: Optional[float] = None,
        land_area_max: Optional[float] = None,
        price_uah_min: Optional[float] = None,
        price_uah_max: Optional[float] = None,
        tags: Optional[List[str]] = None,
        limit: int = 50,
    ) -> List[Dict[str, Any]]:
        """
        Універсальна вибірка з unified_listings з опційними точними та substring-фільтрами.

        В реальних даних `property_type` зберігається як «комерційна нерухомість»,
        «земельна ділянка» тощо — повними фразами. Тому корисніше шукати через
        `property_type_contains` (substring case-insensitive), а не точне значення.
        Це і використовує `listings.find_with_fallback` для стратегії `relaxed_*`.
        """
        import re as _re

        criteria: Dict[str, Any] = {}
        if status:
            criteria["status"] = str(status).strip()
        if source:
            criteria["source"] = str(source).strip()
        if region:
            criteria["region"] = str(region).strip()
        if city:
            criteria["city"] = str(city).strip()
        if property_type:
            criteria["property_type"] = str(property_type).strip()
        if region_contains:
            criteria["region"] = {"$regex": _re.escape(str(region_contains).strip()), "$options": "i"}
        if city_contains:
            criteria["city"] = {"$regex": _re.escape(str(city_contains).strip()), "$options": "i"}
        if property_type_contains:
            criteria["property_type"] = {"$regex": _re.escape(str(property_type_contains).strip()), "$options": "i"}
        if tags:
            valid_tags = [str(t).strip() for t in tags if str(t).strip()]
            if valid_tags:
                criteria["tags"] = {"$in": valid_tags}

        if building_area_min is not None or building_area_max is not None:
            r: Dict[str, Any] = {}
            try:
                if building_area_min is not None:
                    r["$gte"] = float(building_area_min)
            except (TypeError, ValueError):
                pass
            try:
                if building_area_max is not None:
                    r["$lte"] = float(building_area_max)
            except (TypeError, ValueError):
                pass
            if r:
                criteria["building_area_sqm"] = r

        if land_area_min is not None or land_area_max is not None:
            r = {}
            try:
                if land_area_min is not None:
                    r["$gte"] = float(land_area_min)
            except (TypeError, ValueError):
                pass
            try:
                if land_area_max is not None:
                    r["$lte"] = float(land_area_max)
            except (TypeError, ValueError):
                pass
            if r:
                criteria["land_area_sqm"] = r

        if price_uah_min is not None or price_uah_max is not None:
            r = {}
            try:
                if price_uah_min is not None:
                    r["$gte"] = float(price_uah_min)
            except (TypeError, ValueError):
                pass
            try:
                if price_uah_max is not None:
                    r["$lte"] = float(price_uah_max)
            except (TypeError, ValueError):
                pass
            if r:
                criteria["price_uah"] = r

        docs = list(self.collection.find(criteria).limit(max(1, int(limit or 50))))
        return [_normalize_doc(d) for d in docs]

    def landscape_summary(
        self,
        region: Optional[str] = None,
        city: Optional[str] = None,
        status: Optional[str] = None,
        source: Optional[str] = None,
        region_contains: Optional[str] = None,
        city_contains: Optional[str] = None,
        max_per_field: int = 20,
    ) -> Dict[str, Any]:
        """
        Швидкий «landscape»: розподіл по property_type / region / city / source
        у межах базового scope (без area/price-фільтрів). Використовується
        `listings.find_with_fallback` коли точні стратегії дали 0, щоб
        повертати агенту реальні значення з БД.
        """
        import re as _re

        criteria: Dict[str, Any] = {}
        if status:
            criteria["status"] = str(status).strip()
        if source:
            criteria["source"] = str(source).strip()
        if region:
            criteria["region"] = str(region).strip()
        if city:
            criteria["city"] = str(city).strip()
        if region_contains:
            criteria["region"] = {"$regex": _re.escape(str(region_contains).strip()), "$options": "i"}
        if city_contains:
            criteria["city"] = {"$regex": _re.escape(str(city_contains).strip()), "$options": "i"}

        total = self.collection.count_documents(criteria)
        if total == 0:
            return {"total": 0, "by_property_type": [], "by_region": [], "by_city": [], "by_source": []}

        def _agg(field: str) -> List[Dict[str, Any]]:
            pipeline = [
                {"$match": criteria},
                {"$group": {"_id": f"${field}", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
                {"$limit": max(1, int(max_per_field))},
            ]
            out: List[Dict[str, Any]] = []
            for r in self.collection.aggregate(pipeline):
                key = r.get("_id")
                if key is None or key == "":
                    continue
                out.append({"value": key, "count": int(r.get("count") or 0)})
            return out

        return {
            "total": int(total),
            "by_property_type": _agg("property_type"),
            "by_region": _agg("region"),
            "by_city": _agg("city"),
            "by_source": _agg("source"),
        }

    def set_vector_indexed(self, source: str, source_id: str) -> bool:
        """Позначає документ як проіндексований у векторній БД (Qdrant)."""
        if not source or not source_id:
            return False
        from datetime import datetime, timezone

        now = datetime.now(timezone.utc)
        result = self.collection.update_one(
            {"source": source, "source_id": source_id},
            {"$set": {"vector_indexed_at": now}},
        )
        return bool(result.modified_count) or bool(result.matched_count)

    def find_without_vector_index(
        self,
        limit: int = 500,
        source: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Повертає документи без поля `vector_indexed_at` (або зі старішим за оновлення)."""
        criteria: Dict[str, Any] = {
            "$or": [
                {"vector_indexed_at": {"$exists": False}},
                {"vector_indexed_at": None},
            ],
        }
        if source:
            criteria["source"] = source
        docs = list(self.collection.find(criteria).limit(max(1, int(limit))))
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
        result = self.collection.update_one(
            {"source": source, "source_id": source_id},
            {"$set": {"real_estate_object_refs": refs or []}},
        )
        return result.matched_count > 0
