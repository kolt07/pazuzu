# -*- coding: utf-8 -*-
"""
GeoFilterBuilder: централізована побудова географічних фільтрів для MongoDB.
Автоматично визначає структуру колекції, додає $unwind для масивів, використовує regex,
та будує OR логіку для "та" (місто та область).
"""

import logging
from typing import Dict, Any, Optional, List, Tuple
from utils.source_field_mapper import SourceFieldMapper
from utils.schema_filter_resolver import REGION_SYNONYMS

logger = logging.getLogger(__name__)


class GeoFilterBuilder:
    """
    Будівник географічних фільтрів для MongoDB.
    
    Кроки роботи:
    1. Перевірка структури колекції через get_collection_info
    2. Автоматичний $unwind якщо address_refs - масив
    3. Завжди використовує regex для пошуку
    4. Автоматична OR логіка для city + region
    5. Self-healing fallback якщо результатів 0
    """
    
    def __init__(self, run_tool_fn=None):
        """
        Args:
            run_tool_fn: Функція для виклику MCP tools (get_collection_info)
        """
        self.run_tool_fn = run_tool_fn
    
    def build_geo_filter(
        self,
        geo_filters: Dict[str, str],  # {"city": "Київ", "region": "Київська область"}
        collection: str,
        pipeline: List[Dict[str, Any]]
    ) -> Tuple[List[Dict[str, Any]], Dict[str, Any], Dict[str, Any]]:
        """
        Будує географічний фільтр для MongoDB pipeline.
        
        Args:
            geo_filters: Логічні фільтри {"city": "Київ", "region": "Київська область"}
            collection: Назва колекції
            pipeline: Поточний MongoDB pipeline (може бути модифікований)
            
        Returns:
            Tuple:
            - pipeline: Оновлений pipeline (з $unwind якщо потрібно)
            - match_filter: Фільтр для $match
            - metadata: Метадані про структуру (для self-healing)
        """
        if not geo_filters or not collection:
            return pipeline, {}, {}
        
        city_value = geo_filters.get("city")
        region_value = geo_filters.get("region")
        
        # Пропускаємо непідставлені параметри ($region, {{city}} тощо)
        def _is_substituted(val: Any) -> bool:
            if val is None:
                return False
            if isinstance(val, str) and (val.startswith("$") or val.startswith("{{")):
                return False
            return True
        
        if not _is_substituted(city_value):
            city_value = None
        if not _is_substituted(region_value):
            region_value = None
        
        if not city_value and not region_value:
            return pipeline, {}, {}
        
        # Крок 1: Перевірка структури колекції
        collection_info = self._get_collection_info(collection)
        address_refs_info = self._analyze_address_refs_structure(collection_info, collection)
        
        metadata = {
            "has_address_refs": address_refs_info["has_address_refs"],
            "is_array": address_refs_info["is_array"],
            "address_refs_path": address_refs_info["path"],
            "geo_filter_applied": True
        }
        
        # Крок 2: $unwind тільки якщо не використовуємо root geo
        # unified_listings має root region, city — без $unwind
        if not SourceFieldMapper.uses_root_geo(collection):
            if address_refs_info["is_array"] and address_refs_info["has_address_refs"]:
                unwind_path = address_refs_info["path"]
                unwind_path_mongo = unwind_path if unwind_path.startswith("$") else f"${unwind_path}"
                has_unwind = any(
                    stage.get("$unwind") == unwind_path_mongo or
                    stage.get("$unwind") == unwind_path or
                    (isinstance(stage.get("$unwind"), dict) and stage["$unwind"].get("path") in (unwind_path, unwind_path_mongo))
                    for stage in pipeline
                )
                if not has_unwind:
                    pipeline.insert(0, {"$unwind": unwind_path_mongo})
                    logger.info("GeoFilterBuilder: Додано $unwind для %s", unwind_path_mongo)
        
        # Крок 3: Будуємо фільтр з regex
        match_filter = self._build_regex_filter(
            city_value,
            region_value,
            collection,
            address_refs_info
        )
        
        return pipeline, match_filter, metadata
    
    def _get_collection_info(self, collection: str) -> Dict[str, Any]:
        """
        Отримує інформацію про структуру колекції через get_collection_info.
        """
        if not self.run_tool_fn:
            # Fallback: використовуємо знання про структуру
            return self._get_default_collection_info(collection)
        
        try:
            result = self.run_tool_fn("get_collection_info", {
                "collection_name": collection
            })
            if result.get("success"):
                return result.get("collection", {})
        except Exception as e:
            logger.warning("GeoFilterBuilder: Помилка отримання collection_info: %s", e)
        
        return self._get_default_collection_info(collection)
    
    def _get_default_collection_info(self, collection: str) -> Dict[str, Any]:
        """Fallback інформація про структуру колекції."""
        if collection == "prozorro_auctions":
            return {
                "fields": [
                    {"name": "auction_data.address_refs", "type": "array"},
                    {"name": "auction_data.items", "type": "array"}
                ]
            }
        elif collection == "olx_listings":
            return {
                "fields": [
                    {"name": "detail.address_refs", "type": "array"},
                    {"name": "detail.resolved_locations", "type": "array"}
                ]
            }
        elif collection == "unified_listings":
            return {
                "fields": [
                    {"name": "addresses", "type": "array"},
                ]
            }
        return {}
    
    def _analyze_address_refs_structure(
        self,
        collection_info: Dict[str, Any],
        collection: str
    ) -> Dict[str, Any]:
        """
        Аналізує структуру address_refs/addresses в колекції.
        
        Returns:
            {
                "has_address_refs": bool,
                "is_array": bool,
                "path": str  # Шлях до масиву адрес
            }
        """
        # unified_listings: root geo (region, city) — без $unwind
        if collection == "unified_listings":
            return {
                "has_address_refs": True,
                "is_array": False,
                "path": "",
                "use_root_geo": True,
            }
        
        # Отримуємо шлях до address_refs через SourceFieldMapper
        region_field = SourceFieldMapper.get_region_field(collection)
        
        # Визначаємо шлях до address_refs (прибираємо .region.name)
        if "address_refs" in region_field:
            parts = region_field.split(".")
            refs_idx = next((i for i, p in enumerate(parts) if p == "address_refs"), None)
            if refs_idx is not None:
                refs_path = ".".join(parts[:refs_idx + 1])
            else:
                refs_path = region_field
        else:
            refs_path = region_field
        
        # Перевіряємо в collection_info, чи це масив
        is_array = False
        if collection_info:
            fields = collection_info.get("fields", [])
            for field in fields:
                field_name = field.get("name", "")
                if "address_refs" in field_name and field.get("type") == "array":
                    is_array = True
                    break
        
        # За замовчуванням вважаємо, що address_refs - масив
        if not collection_info:
            is_array = True
        
        return {
            "has_address_refs": True,  # Припускаємо, що є
            "is_array": is_array,
            "path": refs_path
        }
    
    def _build_regex_filter(
        self,
        city_value: Optional[str],
        region_value: Optional[str],
        collection: str,
        address_refs_info: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Будує MongoDB фільтр з використанням regex.
        Завжди використовує regex, ніколи exact match.
        """
        match_filter = {}
        refs_path = address_refs_info["path"]
        region_key, city_key = SourceFieldMapper.get_geo_match_keys(collection)
        
        # Нормалізуємо значення
        city_variants = self._normalize_city_value(city_value) if city_value else []
        region_variants = self._normalize_region_value(region_value) if region_value else []
        
        # Крок 4: Автоматична OR логіка
        # Якщо є і city, і region - це означає OR (або місто, або область)
        or_conditions = []
        
        # Умови для міста
        prefix = f"{refs_path}." if refs_path else ""
        if city_variants:
            for variant in city_variants:
                regex_pattern = self._build_regex_pattern(variant)
                field_path = f"{prefix}{city_key}" if prefix else city_key
                or_conditions.append({
                    field_path: {
                        "$regex": regex_pattern,
                        "$options": "i"
                    }
                })
                
                # Fallback поля (unified_listings не має fallback)
                city_fallback = SourceFieldMapper.get_city_fallback_field(collection)
                if city_fallback:
                    if "items" in city_fallback:
                        # Для prozorro_auctions
                        or_conditions.append({
                            "auction_data.items": {
                                "$elemMatch": {
                                    "address.locality.uk_UA": {
                                        "$regex": regex_pattern,
                                        "$options": "i"
                                    }
                                }
                            }
                        })
                    elif "resolved_locations" in city_fallback:
                        # Для olx_listings
                        or_conditions.append({
                            city_fallback: {
                                "$regex": regex_pattern,
                                "$options": "i"
                            }
                        })
                    elif "location" in city_fallback:
                        # Для olx_listings - search_data.location
                        or_conditions.append({
                            city_fallback: {
                                "$regex": regex_pattern,
                                "$options": "i"
                            }
                        })
        
        # Умови для регіону
        if region_variants:
            for variant in region_variants:
                regex_pattern = self._build_regex_pattern(variant)
                field_path = f"{prefix}{region_key}" if prefix else region_key
                or_conditions.append({
                    field_path: {
                        "$regex": regex_pattern,
                        "$options": "i"
                    }
                })
                
                # Fallback поля (unified_listings не має fallback)
                region_fallback = SourceFieldMapper.get_region_fallback_field(collection)
                if region_fallback:
                    if "items" in region_fallback:
                        # Для prozorro_auctions
                        or_conditions.append({
                            "auction_data.items": {
                                "$elemMatch": {
                                    "address.region.uk_UA": {
                                        "$regex": regex_pattern,
                                        "$options": "i"
                                    }
                                }
                            }
                        })
                    elif "location" in region_fallback:
                        # Для olx_listings
                        or_conditions.append({
                            region_fallback: {
                                "$regex": regex_pattern,
                                "$options": "i"
                            }
                        })
        
        # Якщо є умови, об'єднуємо через $or
        if or_conditions:
            if len(or_conditions) == 1:
                match_filter.update(or_conditions[0])
            else:
                match_filter["$or"] = or_conditions
        
        return match_filter
    
    def _build_regex_pattern(self, value: str) -> str:
        """
        Будує regex pattern для пошуку.
        Завжди використовує regex, ніколи exact match.
        """
        # Екрануємо спеціальні символи regex
        import re
        escaped = re.escape(value)
        # Додаємо можливість часткового збігу на початку
        return f"^{escaped}"
    
    def _normalize_region_value(self, region_value: str) -> List[str]:
        """
        Нормалізує значення регіону, додаючи варіанти з "область" та "обл.".
        Також використовує синоніми.
        """
        if not region_value:
            return []
        
        variants = [region_value]
        
        # Додаємо варіанти з "область" та "обл." якщо їх немає
        if not region_value.endswith(" область") and not region_value.endswith(" обл."):
            variants.append(region_value + " область")
            variants.append(region_value + " обл.")
        
        # Використовуємо синоніми
        if region_value in REGION_SYNONYMS:
            synonym = REGION_SYNONYMS[region_value]
            if synonym not in variants:
                variants.append(synonym)
                # Також додаємо варіанти синоніма
                if not synonym.endswith(" область") and not synonym.endswith(" обл."):
                    variants.append(synonym + " область")
                    variants.append(synonym + " обл.")
        
        return variants
    
    def _normalize_city_value(self, city_value: str) -> List[str]:
        """
        Нормалізує значення міста.
        """
        if not city_value:
            return []
        
        variants = [city_value]
        
        # Для міста зазвичай не потрібні додаткові варіанти,
        # але можна додати "м. " префікс якщо потрібно
        if not city_value.startswith("м. "):
            variants.append("м. " + city_value)
        
        return variants
    
    def build_self_healing_fallback(
        self,
        original_geo_filters: Dict[str, str],
        collection: str,
        pipeline: List[Dict[str, Any]],
        result_count: int,
        total_docs: int,
        metadata: Dict[str, Any]
    ) -> Optional[Tuple[List[Dict[str, Any]], Dict[str, Any]]]:
        """
        Крок 5: Self-healing fallback.
        
        Якщо:
        - count == 0
        - total_docs > 0
        - geo_filter_applied == True
        
        → повторити запит без region
        → якщо з'явились результати — проблема в region
        
        Returns:
            Tuple (pipeline, match_filter) або None якщо fallback не потрібен
        """
        if result_count > 0:
            return None  # Результати є, fallback не потрібен
        
        if total_docs == 0:
            return None  # Колекція порожня
        
        if not metadata.get("geo_filter_applied"):
            return None  # Гео-фільтр не застосовувався
        
        # Спробуємо без region (залишаємо тільки city)
        city_value = original_geo_filters.get("city")
        if not city_value:
            return None  # Немає city для fallback
        
        logger.info("GeoFilterBuilder: Self-healing fallback - спроба без region")
        
        # Будуємо новий фільтр тільки з city
        fallback_geo = {"city": city_value}
        fallback_pipeline = pipeline.copy()
        fallback_pipeline, fallback_match, _ = self.build_geo_filter(fallback_geo, collection, fallback_pipeline)
        
        return fallback_pipeline, fallback_match
