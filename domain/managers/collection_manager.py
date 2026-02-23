# -*- coding: utf-8 -*-
"""
CollectionManager: менеджер колекції — робота з усією колекцією одразу.
Вся робота з додавання, видалення, пошуком здійснюється через цей шар.
Агенти та інструменти НЕ мають прямого доступу до БД — тільки через CollectionManager.
"""

import logging
import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

import pandas as pd

from domain.models.filter_models import (
    FilterElement,
    FilterGroup,
    FilterGroupType,
    FilterOperator,
    FindQuery,
    GeoFilter,
)
from domain.services.geo_filter_service import GeoFilterService

logger = logging.getLogger(__name__)


class BaseCollectionManager:
    """
    Базовий менеджер колекції.
    Підкласи реалізують конкретні колекції (unified_listings, prozorro_auctions, olx_listings).
    """

    # Перевизначити у підкласах
    COLLECTION_NAME: str = ""
    ENTITY_CLASS = None  # Клас домен-сутності

    def __init__(self):
        self._geo_filter_service = GeoFilterService()
        self._field_values_cache: Dict[str, List[Any]] = {}
        self._field_structure_cache: Optional[Dict[str, Any]] = None

    def add(self):
        """
        Додати — повертається пустий об'єкт колекції.
        """
        if self.ENTITY_CLASS:
            return self.ENTITY_CLASS({})
        return {}

    def delete(self, object_id: str) -> bool:
        """
        Видалити — безпечне видалення об'єкта та пов'язаних сутностей.
        Перевизначити у підкласах для каскадного видалення.
        """
        raise NotImplementedError

    def get_available_field_values(
        self,
        field: str,
        force_refresh: bool = False
    ) -> List[Dict[str, Any]]:
        """
        Отримати доступні значення поля — кешована процедура.
        При першому запуску формує масив можливих значень.
        force_refresh=True — примусове оновлення (викликати лише з update_cache).
        
        Returns:
            Список значень. Якщо значення — посилання на іншу таблицю:
            {"id": "...", "representation": "...", "target_collection": "..."}
        """
        cache_key = field
        if not force_refresh and cache_key in self._field_values_cache:
            return self._field_values_cache[cache_key]
        
        values = self._fetch_field_values(field)
        self._field_values_cache[cache_key] = values
        return values

    def _fetch_field_values(self, field: str) -> List[Dict[str, Any]]:
        """Отримує значення поля з БД через репозиторій. Перевизначити у підкласах."""
        raise NotImplementedError

    def get_field_structure(self) -> Dict[str, Any]:
        """
        Отримати структуру полів — максимально повна структура з описом, типами.
        """
        if self._field_structure_cache is not None:
            return self._field_structure_cache
        structure = self._load_field_structure()
        self._field_structure_cache = structure
        return structure

    def _load_field_structure(self) -> Dict[str, Any]:
        """Завантажує структуру з data_dictionary. Перевизначити у підкласах."""
        raise NotImplementedError

    def update_cache(self) -> None:
        """
        Оновити кеш — викликати після оновлення даних з джерел.
        Скидає кеш полів та примусово оновлює довідкові дані.
        """
        self._field_values_cache.clear()
        self._field_structure_cache = None
        logger.info("CollectionManager %s: кеш оновлено", self.COLLECTION_NAME)

    def find(
        self,
        query: FindQuery
    ) -> pd.DataFrame:
        """
        Знайти — фільтри, сортування, групування, поля, геофільтри.
        Повертає DataFrame з даними (не об'єктами).
        Якщо fields не задані — повертає всі поля.
        """
        raise NotImplementedError

    def get_object(self, object_id: str):
        """
        Отримати об'єкт — по ідентифікатору отримує domain-сутність.
        """
        raise NotImplementedError

    def get_total_count(self) -> Optional[int]:
        """
        Повертає загальну кількість документів у колекції (для діагностики).
        Перевизначити у підкласах.
        """
        return None

    def _filter_group_to_mongo(self, group: FilterGroup) -> Dict[str, Any]:
        """Рекурсивно перетворює FilterGroup на MongoDB query."""
        if not group.items:
            return {}
        
        mongo_items = []
        for item in group.items:
            if isinstance(item, FilterElement):
                mongo_items.append(self._filter_element_to_mongo(item))
            elif isinstance(item, FilterGroup):
                mongo_items.append(self._filter_group_to_mongo(item))
        
        if not mongo_items:
            return {}
        
        if group.group_type == FilterGroupType.AND:
            return {"$and": mongo_items} if len(mongo_items) > 1 else mongo_items[0]
        if group.group_type == FilterGroupType.OR:
            return {"$or": mongo_items} if len(mongo_items) > 1 else mongo_items[0]
        if group.group_type == FilterGroupType.NOT:
            return {"$nor": mongo_items} if len(mongo_items) > 1 else {"$nor": [mongo_items[0]]}
        return {}

    def _filter_element_to_mongo(self, elem: FilterElement) -> Dict[str, Any]:
        """Перетворює FilterElement на MongoDB умову."""
        field = elem.field
        value = elem.value
        op = elem.operator
        
        mongo_op_map = {
            FilterOperator.EQ: "$eq",
            FilterOperator.NE: "$ne",
            FilterOperator.GT: "$gt",
            FilterOperator.GTE: "$gte",
            FilterOperator.LT: "$lt",
            FilterOperator.LTE: "$lte",
            FilterOperator.IN: "$in",
            FilterOperator.NIN: "$nin",
            FilterOperator.CONTAINS: "$regex",
            FilterOperator.FILLED: "$exists",
            FilterOperator.EMPTY: "$exists",
        }
        
        if op == FilterOperator.FILLED:
            return {"$and": [{field: {"$exists": True}}, {field: {"$nin": [None, ""]}}]}
        if op == FilterOperator.EMPTY:
            return {"$or": [
                {field: {"$exists": False}},
                {field: None},
                {field: ""}
            ]}
        if op == FilterOperator.CONTAINS:
            return {field: {"$regex": str(value), "$options": "i"}}

        # Для полів дат: конвертуємо ISO-рядки в datetime (BSON Date)
        if op in (FilterOperator.GTE, FilterOperator.LTE, FilterOperator.GT, FilterOperator.LT):
            value = self._coerce_date_value(field, value)

        mongo_op = mongo_op_map.get(op, "$eq")
        return {field: {mongo_op: value}}

    def _is_date_field(self, field: str) -> bool:
        """Чи є поле полем дати (BSON Date). Перевизначити у підкласах."""
        return False

    def _coerce_date_value(self, field: str, value: Any) -> Any:
        """Конвертує ISO-рядок у datetime для полів дат."""
        if not self._is_date_field(field) or not isinstance(value, str):
            return value
        # ISO 8601: 2026-02-05T10:25:17.000Z або 2026-02-05T10:25:17
        if not re.match(r"^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}", value):
            return value
        try:
            s = value.replace("Z", "+00:00")
            return datetime.fromisoformat(s)
        except (ValueError, TypeError):
            return value


class UnifiedListingsCollectionManager(BaseCollectionManager):
    """
    Менеджер колекції unified_listings.
    Використовує UnifiedListingsRepository для доступу до даних.
    """

    COLLECTION_NAME = "unified_listings"
    DATE_FIELDS = frozenset({"source_updated_at", "system_updated_at", "system_created_at"})

    def _is_date_field(self, field: str) -> bool:
        return field in self.DATE_FIELDS

    def __init__(self):
        super().__init__()
        from data.repositories.unified_listings_repository import UnifiedListingsRepository
        from domain.entities.unified_listing import UnifiedListing
        self._repo = UnifiedListingsRepository()
        self.ENTITY_CLASS = UnifiedListing

    def delete(self, object_id: str) -> bool:
        """Безпечне видалення. unified_listings — не каскадує на інші колекції."""
        try:
            from bson import ObjectId
            if len(object_id) == 24:
                r = self._repo.collection.delete_one({"_id": ObjectId(object_id)})
                return r.deleted_count > 0
            if ":" in object_id:
                parts = object_id.split(":", 1)
                if len(parts) == 2:
                    r = self._repo.collection.delete_one({"source": parts[0], "source_id": parts[1]})
                    return r.deleted_count > 0
        except Exception as e:
            logger.warning("UnifiedListingsCollectionManager.delete: %s", e)
        return False

    def _fetch_field_values(self, field: str) -> List[Dict[str, Any]]:
        """Отримує унікальні значення поля через aggregation або distinct."""
        from utils.source_field_mapper import SourceFieldMapper
        path = field
        if field in ("region", "city"):
            path = SourceFieldMapper.get_region_field(self.COLLECTION_NAME) if field == "region" else SourceFieldMapper.get_city_field(self.COLLECTION_NAME)
        
        try:
            if "." in path:
                parts = path.split(".")
                from data.database.connection import MongoDBConnection
                db = MongoDBConnection.get_database()
                coll = db[self.COLLECTION_NAME]
                cursor = coll.aggregate([
                    {"$unwind": {"path": f"${parts[0]}", "preserveNullAndEmptyArrays": True}},
                    {"$group": {"_id": f"${path}"}},
                    {"$match": {"_id": {"$nin": [None, ""]}}},
                    {"$sort": {"_id": 1}},
                    {"$limit": 500}
                ])
                vals = [{"value": d["_id"], "representation": str(d["_id"])} for d in cursor]
            else:
                vals_raw = self._repo.collection.distinct(path)
                vals = [{"value": v, "representation": str(v)} for v in vals_raw if v is not None and v != ""][:500]
            return vals
        except Exception as e:
            logger.warning("UnifiedListingsCollectionManager._fetch_field_values: %s", e)
            return []

    def _load_field_structure(self) -> Dict[str, Any]:
        """Завантажує з data_dictionary."""
        try:
            import yaml
            from pathlib import Path
            dd_path = Path(__file__).resolve().parents[2] / "config" / "data_dictionary.yaml"
            with open(dd_path, "r", encoding="utf-8") as f:
                dd = yaml.safe_load(f)
            coll_config = dd.get("collections", {}).get(self.COLLECTION_NAME, {})
            return {
                "description": coll_config.get("description", ""),
                "fields": coll_config.get("fields", {}),
                "indexes": coll_config.get("indexes", []),
            }
        except Exception as e:
            logger.warning("UnifiedListingsCollectionManager._load_field_structure: %s", e)
            return {"description": "", "fields": {}, "indexes": []}

    def find(self, query: FindQuery) -> pd.DataFrame:
        """Знайти записи. Перетворює FindQuery на MongoDB pipeline і виконує."""
        from utils.query_builder import QueryBuilder
        from utils.source_field_mapper import SourceFieldMapper
        
        qb = QueryBuilder()
        mongo_filter: Dict[str, Any] = {}
        
        # Звичайні фільтри
        if query.filters:
            mongo_filter = self._filter_group_to_mongo(query.filters)
        
        # Геофільтри — перетворюємо GeoFilter на MongoDB
        if query.geo_filters:
            geo_mongo = self._geo_filter_to_mongo(query.geo_filters)
            if geo_mongo:
                mongo_filter = {"$and": [mongo_filter, geo_mongo]} if mongo_filter else geo_mongo
        
        pipeline: List[Dict[str, Any]] = []
        
        # $unwind для addresses (гео-пошук) — має бути перед $match
        if query.geo_filters and self.COLLECTION_NAME == "unified_listings":
            pipeline.append({"$unwind": {"path": "$addresses", "preserveNullAndEmptyArrays": True}})
        
        # $match
        if mongo_filter:
            logger.info("Final Mongo $match: %s", mongo_filter)
            pipeline.append({"$match": mongo_filter})
        
        # $sort
        if query.sort:
            sort_spec = {}
            for s in query.sort:
                f = s.get("field") or s.get("field_path")
                o = s.get("order", s.get("direction", -1))
                if isinstance(o, str) and o.lower() in ("asc", "desc"):
                    o = 1 if o.lower() == "asc" else -1
                if f:
                    sort_spec[f] = o
            if sort_spec:
                pipeline.append({"$sort": sort_spec})
        
        # $skip, $limit
        if query.skip:
            pipeline.append({"$skip": query.skip})
        if query.limit:
            pipeline.append({"$limit": query.limit})
        
        # $project (поля)
        if query.fields:
            project = {f: 1 for f in query.fields}
            project["_id"] = 1
            pipeline.append({"$project": project})
        
        result = qb.execute_aggregation(
            collection_name=self.COLLECTION_NAME,
            pipeline=pipeline,
            limit=query.limit
        )
        
        if not result.get("success"):
            logger.warning("UnifiedListingsCollectionManager.find: %s", result.get("error"))
            return pd.DataFrame()
        
        data = result.get("results", result.get("data", []))
        return pd.DataFrame(data) if data else pd.DataFrame()

    def get_total_count(self) -> Optional[int]:
        """Повертає загальну кількість документів у колекції."""
        try:
            return self._repo.collection.count_documents({})
        except Exception as e:
            logger.warning("UnifiedListingsCollectionManager.get_total_count: %s", e)
            return None

    def _geo_filter_to_mongo(self, geo_filter: GeoFilter) -> Dict[str, Any]:
        """Перетворює GeoFilter на MongoDB умову для unified_listings."""
        import re
        from domain.models.filter_models import GeoFilterElement, GeoFilterGroup, GeoFilterOperator
        root = geo_filter.root
        
        def process_element(elem: GeoFilterElement) -> Dict[str, Any]:
            if elem.operator == GeoFilterOperator.EQ:
                if elem.geo_type == "settlement":
                    # Підтримка "Київ" та "м. Київ"
                    escaped = re.escape(str(elem.value))
                    pattern = f"^(м\\.\\s*)?{escaped}"
                    return {"addresses.settlement": {"$regex": pattern, "$options": "i"}}
                if elem.geo_type == "region":
                    escaped = re.escape(str(elem.value))
                    return {"addresses.region": {"$regex": f"^{escaped}", "$options": "i"}}
            if elem.operator == GeoFilterOperator.NE:
                # «Не в місті» / «Не в області» — документи, де жодна адреса не відповідає
                if elem.geo_type == "settlement":
                    escaped = re.escape(str(elem.value))
                    pattern = f"^(м\\.\\s*)?{escaped}"
                    return {"addresses": {"$not": {"$elemMatch": {"settlement": {"$regex": pattern, "$options": "i"}}}}}
                if elem.geo_type == "region":
                    escaped = re.escape(str(elem.value))
                    return {"addresses": {"$not": {"$elemMatch": {"region": {"$regex": f"^{escaped}", "$options": "i"}}}}}
            if elem.operator == GeoFilterOperator.IN_RADIUS and elem.geo_type == "coordinates":
                # $geoWithin $centerSphere
                lat = elem.value.get("latitude")
                lon = elem.value.get("longitude")
                km = elem.radius_km or 10
                if lat is not None and lon is not None:
                    # радіус в радіанах: km / 6378.1
                    rad = km / 6378.1
                    return {"addresses.coordinates": {"$geoWithin": {"$centerSphere": [[lon, lat], rad]}}}
            return {}
        
        def process_group(gr: GeoFilterGroup) -> Dict[str, Any]:
            items = []
            for it in gr.items:
                if isinstance(it, GeoFilterElement):
                    c = process_element(it)
                    if c:
                        items.append(c)
                elif isinstance(it, GeoFilterGroup):
                    c = process_group(it)
                    if c:
                        items.append(c)
            if not items:
                return {}
            if gr.group_type == FilterGroupType.OR:
                return {"$or": items} if len(items) > 1 else items[0]
            if gr.group_type == FilterGroupType.AND:
                return {"$and": items} if len(items) > 1 else items[0]
            return items[0] if items else {}
        
        if isinstance(root, GeoFilterElement):
            return process_element(root)
        if isinstance(root, GeoFilterGroup):
            return process_group(root)
        return {}

    def get_object(self, object_id: str):
        """Отримує UnifiedListing за ідентифікатором."""
        from domain.gateways.listing_gateway import ListingGateway
        gw = ListingGateway()
        return gw.get_unified_listing_by_id(object_id)


class ListingAnalyticsCollectionManager(BaseCollectionManager):
    """
    Менеджер колекції listing_analytics.
    LLM-аналітика оголошень. Зв'язок з оголошенням через source+source_id.
    """

    COLLECTION_NAME = "listing_analytics"
    DATE_FIELDS = frozenset({"analysis_at", "updated_at"})

    def _is_date_field(self, field: str) -> bool:
        return field in self.DATE_FIELDS

    def __init__(self):
        super().__init__()
        from data.repositories.listing_analytics_repository import ListingAnalyticsRepository
        from domain.entities.listing_analytics import ListingAnalytics
        self._repo = ListingAnalyticsRepository()
        self.ENTITY_CLASS = ListingAnalytics

    def delete(self, object_id: str) -> bool:
        """Видалення не підтримується через domain-шар (write через сервіс)."""
        return False

    def _fetch_field_values(self, field: str) -> List[Dict[str, Any]]:
        """Отримує унікальні значення поля."""
        try:
            if field == "source":
                return [{"value": "olx", "representation": "OLX"}, {"value": "prozorro", "representation": "ProZorro"}]
            vals_raw = self._repo.collection.distinct(field)
            return [{"value": v, "representation": str(v)} for v in vals_raw if v is not None and v != ""][:500]
        except Exception as e:
            logger.warning("ListingAnalyticsCollectionManager._fetch_field_values: %s", e)
            return []

    def _load_field_structure(self) -> Dict[str, Any]:
        """Завантажує з data_dictionary."""
        try:
            import yaml
            from pathlib import Path
            dd_path = Path(__file__).resolve().parents[2] / "config" / "data_dictionary.yaml"
            with open(dd_path, "r", encoding="utf-8") as f:
                dd = yaml.safe_load(f)
            coll_config = dd.get("collections", {}).get(self.COLLECTION_NAME, {})
            return {
                "description": coll_config.get("description", ""),
                "fields": coll_config.get("fields", {}),
                "indexes": coll_config.get("indexes", []),
            }
        except Exception as e:
            logger.warning("ListingAnalyticsCollectionManager._load_field_structure: %s", e)
            return {"description": "", "fields": {}, "indexes": []}

    def find(self, query: FindQuery) -> pd.DataFrame:
        """Знайти записи через QueryBuilder."""
        from utils.query_builder import QueryBuilder
        qb = QueryBuilder()
        mongo_filter: Dict[str, Any] = {}
        if query.filters:
            mongo_filter = self._filter_group_to_mongo(query.filters)
        pipeline: List[Dict[str, Any]] = []
        if mongo_filter:
            pipeline.append({"$match": mongo_filter})
        if query.sort:
            sort_spec = {}
            for s in query.sort:
                f = s.get("field") or s.get("field_path")
                o = s.get("order", s.get("direction", -1))
                if isinstance(o, str) and o.lower() in ("asc", "desc"):
                    o = 1 if o.lower() == "asc" else -1
                if f:
                    sort_spec[f] = o
            if sort_spec:
                pipeline.append({"$sort": sort_spec})
        if query.skip:
            pipeline.append({"$skip": query.skip})
        if query.limit:
            pipeline.append({"$limit": query.limit})
        if query.fields:
            project = {f: 1 for f in query.fields}
            project["_id"] = 1
            pipeline.append({"$project": project})
        result = qb.execute_aggregation(
            collection_name=self.COLLECTION_NAME,
            pipeline=pipeline,
            limit=query.limit
        )
        if not result.get("success"):
            logger.warning("ListingAnalyticsCollectionManager.find: %s", result.get("error"))
            return pd.DataFrame()
        data = result.get("results", result.get("data", []))
        return pd.DataFrame(data) if data else pd.DataFrame()

    def get_total_count(self) -> Optional[int]:
        try:
            return self._repo.collection.count_documents({})
        except Exception as e:
            logger.warning("ListingAnalyticsCollectionManager.get_total_count: %s", e)
            return None

    def get_object(self, object_id: str):
        """Отримує ListingAnalytics за source:source_id."""
        if ":" in object_id:
            parts = object_id.split(":", 1)
            if len(parts) == 2:
                doc = self._repo.find_by_source_id(parts[0], parts[1])
                if doc and self.ENTITY_CLASS:
                    return self.ENTITY_CLASS(doc)
        return None


class RealEstateObjectsCollectionManager(BaseCollectionManager):
    """
    Менеджер колекції real_estate_objects.
    Об'єкти нерухомого майна: land_plot, building, premises.
    """

    COLLECTION_NAME = "real_estate_objects"
    DATE_FIELDS = frozenset({"created_at", "updated_at"})

    def _is_date_field(self, field: str) -> bool:
        return field in self.DATE_FIELDS

    def __init__(self):
        super().__init__()
        from data.repositories.real_estate_objects_repository import RealEstateObjectsRepository
        from domain.entities.real_estate_object import RealEstateObject
        self._repo = RealEstateObjectsRepository()
        self.ENTITY_CLASS = RealEstateObject

    def delete(self, object_id: str) -> bool:
        """Видалення не підтримується через domain-шар."""
        return False

    def _fetch_field_values(self, field: str) -> List[Dict[str, Any]]:
        """Отримує унікальні значення поля."""
        try:
            if field == "type":
                return [
                    {"value": "land_plot", "representation": "Земельна ділянка"},
                    {"value": "building", "representation": "Будівля"},
                    {"value": "premises", "representation": "Приміщення"},
                ]
            path = field
            vals_raw = self._repo.collection.distinct(path)
            return [{"value": v, "representation": str(v)} for v in vals_raw if v is not None and v != ""][:500]
        except Exception as e:
            logger.warning("RealEstateObjectsCollectionManager._fetch_field_values: %s", e)
            return []

    def _load_field_structure(self) -> Dict[str, Any]:
        """Завантажує з data_dictionary."""
        try:
            import yaml
            from pathlib import Path
            dd_path = Path(__file__).resolve().parents[2] / "config" / "data_dictionary.yaml"
            with open(dd_path, "r", encoding="utf-8") as f:
                dd = yaml.safe_load(f)
            coll_config = dd.get("collections", {}).get(self.COLLECTION_NAME, {})
            return {
                "description": coll_config.get("description", ""),
                "fields": coll_config.get("fields", {}),
                "indexes": coll_config.get("indexes", []),
            }
        except Exception as e:
            logger.warning("RealEstateObjectsCollectionManager._load_field_structure: %s", e)
            return {"description": "", "fields": {}, "indexes": []}

    def find(self, query: FindQuery) -> pd.DataFrame:
        """Знайти записи через QueryBuilder."""
        from utils.query_builder import QueryBuilder
        qb = QueryBuilder()
        mongo_filter: Dict[str, Any] = {}
        if query.filters:
            mongo_filter = self._filter_group_to_mongo(query.filters)
        pipeline: List[Dict[str, Any]] = []
        if mongo_filter:
            pipeline.append({"$match": mongo_filter})
        if query.sort:
            sort_spec = {}
            for s in query.sort:
                f = s.get("field") or s.get("field_path")
                o = s.get("order", s.get("direction", -1))
                if isinstance(o, str) and o.lower() in ("asc", "desc"):
                    o = 1 if o.lower() == "asc" else -1
                if f:
                    sort_spec[f] = o
            if sort_spec:
                pipeline.append({"$sort": sort_spec})
        if query.skip:
            pipeline.append({"$skip": query.skip})
        if query.limit:
            pipeline.append({"$limit": query.limit})
        if query.fields:
            project = {f: 1 for f in query.fields}
            project["_id"] = 1
            pipeline.append({"$project": project})
        result = qb.execute_aggregation(
            collection_name=self.COLLECTION_NAME,
            pipeline=pipeline,
            limit=query.limit
        )
        if not result.get("success"):
            logger.warning("RealEstateObjectsCollectionManager.find: %s", result.get("error"))
            return pd.DataFrame()
        data = result.get("results", result.get("data", []))
        return pd.DataFrame(data) if data else pd.DataFrame()

    def get_total_count(self) -> Optional[int]:
        try:
            return self._repo.collection.count_documents({})
        except Exception as e:
            logger.warning("RealEstateObjectsCollectionManager.get_total_count: %s", e)
            return None

    def get_object(self, object_id: str):
        """Отримує RealEstateObject за _id."""
        doc = self._repo.find_by_id(object_id)
        if doc and self.ENTITY_CLASS:
            return self.ENTITY_CLASS(doc)
        return None
