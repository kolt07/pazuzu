# -*- coding: utf-8 -*-
"""
PipelineService: формування контексту для створення пайплайнів.
Отримує інтент з асистента та попередньо виокремлені дані з запиту,
збирає структуру полів та кеші значень колекцій для контексту агенту створення пайплайнів.
"""

import logging
from typing import Any, Dict, List, Optional, Callable

from config.settings import Settings
from domain.managers.collection_manager import (
    BaseCollectionManager,
    UnifiedListingsCollectionManager,
    ListingAnalyticsCollectionManager,
    RealEstateObjectsCollectionManager,
)

logger = logging.getLogger(__name__)

# Джерела даних: зведена таблиця — основна, джерела — лише за потреби
UNIFIED_COLLECTION = "unified_listings"
SOURCE_COLLECTIONS = ["prozorro_auctions", "olx_listings"]
# Колекції, що підтримуються пайплайном (domain-шар)
PIPELINE_COLLECTIONS = [
    UNIFIED_COLLECTION,
    "listing_analytics",
    "real_estate_objects",
] + SOURCE_COLLECTIONS


def get_collection_manager(collection: str) -> Optional[BaseCollectionManager]:
    """Повертає CollectionManager для колекції."""
    if collection == UNIFIED_COLLECTION:
        return UnifiedListingsCollectionManager()
    if collection == "listing_analytics":
        return ListingAnalyticsCollectionManager()
    if collection == "real_estate_objects":
        return RealEstateObjectsCollectionManager()
    return None


class PipelineService:
    """
    Сервіс пайплайнів: формує контекст для PipelineBuilderAgent.
    
    Контекст включає:
    - структуру полів колекцій
    - кеші значень (унікальні значення полів для фільтрів)
    - методи менеджерів колекцій
    """

    def __init__(
        self,
        settings: Settings,
        run_tool_fn: Optional[Callable[[str, Optional[Dict[str, Any]]], Any]] = None,
    ):
        self.settings = settings
        self.run_tool_fn = run_tool_fn

    def build_pipeline_context(
        self,
        intent_info: Dict[str, Any],
        extracted_data: Dict[str, Any],
        user_query: str,
    ) -> Dict[str, Any]:
        """
        Формує контекст для агента створення пайплайнів.
        
        Args:
            intent_info: результат IntentDetectorAgent (intent, response_format, тощо)
            extracted_data: результат QueryStructureAgent (query_structure) — sources, filter_metrics, sort_metrics, limit тощо
            user_query: оригінальний запит користувача
        
        Returns:
            Контекст з полями:
            - intent_info
            - extracted_data
            - user_query
            - field_structure: структура полів по колекціях
            - collection_value_caches: кеші унікальних значень для фільтрів
            - collection_manager_methods: опис методів менеджерів колекцій
        """
        sources = extracted_data.get("sources", [UNIFIED_COLLECTION])
        filter_metrics = extracted_data.get("filter_metrics", {})
        
        # Завжди включаємо unified_listings як основну колекцію
        collections_to_load = set(sources) | {UNIFIED_COLLECTION}
        
        field_structure = {}
        collection_value_caches = {}
        
        for coll_name in collections_to_load:
            mgr = get_collection_manager(coll_name)
            if mgr:
                try:
                    struct = mgr.get_field_structure()
                    field_structure[coll_name] = struct
                except Exception as e:
                    logger.warning("PipelineService: не вдалося отримати структуру %s: %s", coll_name, e)
                    field_structure[coll_name] = {}
            
            # Кеші значень для полів фільтрації (region, city, property_type тощо)
            for field in filter_metrics.keys():
                if field in ("region", "city"):
                    try:
                        vals = self._get_field_values(coll_name, field)
                        if vals:
                            cache_key = f"{coll_name}.{field}"
                            collection_value_caches[cache_key] = vals
                    except Exception as e:
                        logger.debug("PipelineService: кеш для %s.%s: %s", coll_name, field, e)
        
        # Fallback: якщо run_tool_fn є — отримуємо через MCP
        if not field_structure and self.run_tool_fn:
            for coll_name in collections_to_load:
                try:
                    info = self.run_tool_fn("get_collection_info", {"collection_name": coll_name})
                    if info.get("success"):
                        coll_data = info.get("collection", {})
                        field_structure[coll_name] = {
                            "schema": coll_data.get("schema", {}),
                            "fields": coll_data.get("fields", []),
                        }
                except Exception as e:
                    logger.debug("PipelineService: get_collection_info для %s: %s", coll_name, e)
        
        # Опис методів менеджерів колекцій для агента
        collection_manager_methods = {
            "unified_listings": {
                "description": "Зведена таблиця — основне джерело. Об'єднує OLX та ProZorro.",
                "methods": ["find", "get_available_field_values", "get_field_structure"],
                "fields_available": list(self._get_unified_fields_from_structure(field_structure.get(UNIFIED_COLLECTION, {}))),
            },
            "prozorro_auctions": {
                "description": "Джерело даних ProZorro. Використовувати лише якщо потрібні поля, що не містяться в зведеній таблиці.",
                "methods": ["find", "get_available_field_values"],
            },
            "olx_listings": {
                "description": "Джерело даних OLX. Використовувати лише якщо потрібні поля, що не містяться в зведеній таблиці.",
                "methods": ["find", "get_available_field_values"],
            },
            "listing_analytics": {
                "description": "LLM-аналітика оголошень (ціна за одиницю, місцезнаходження, оточення). Зв'язок через source+source_id.",
                "methods": ["find", "get_available_field_values", "get_field_structure"],
            },
            "real_estate_objects": {
                "description": "Об'єкти нерухомого майна (ОНМ): land_plot, building, premises. Зв'язок з unified_listings через real_estate_refs.",
                "methods": ["find", "get_available_field_values", "get_field_structure"],
            },
        }
        
        return {
            "intent_info": intent_info,
            "extracted_data": extracted_data,
            "user_query": user_query,
            "field_structure": field_structure,
            "collection_value_caches": collection_value_caches,
            "collection_manager_methods": collection_manager_methods,
            "main_collection": UNIFIED_COLLECTION,
            "source_collections": SOURCE_COLLECTIONS,
        }

    def _get_field_values(self, collection: str, field: str) -> List[Dict[str, Any]]:
        """Отримує кеш унікальних значень поля."""
        mgr = get_collection_manager(collection)
        if mgr:
            return mgr.get_available_field_values(field)
        if self.run_tool_fn:
            from utils.source_field_mapper import SourceFieldMapper
            path = SourceFieldMapper.get_region_field(collection) if field == "region" else SourceFieldMapper.get_city_field(collection)
            try:
                result = self.run_tool_fn("get_distinct_values", {
                    "collection_name": collection,
                    "field_path": path,
                    "limit": 100,
                })
                if result.get("success"):
                    vals = result.get("values", [])
                    return [{"value": v, "representation": str(v)} for v in vals[:100]]
            except Exception:
                pass
        return []

    def _get_unified_fields_from_structure(self, struct: Dict[str, Any]) -> set:
        """Витягує логічні назви полів зі структури unified_listings."""
        fields = set()
        if "fields" in struct:
            fields_dict = struct["fields"]
            if isinstance(fields_dict, dict):
                for k in fields_dict.keys():
                    if not k.startswith("_") and k not in ("source", "source_id"):
                        fields.add(k)
        return fields or {"price_uah", "addresses", "region", "city", "property_type", "building_area_sqm", "land_area_ha", "status"}
