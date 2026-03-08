# -*- coding: utf-8 -*-
"""
FieldMappingService: єдиний рівень маппінгу логічних полів на фізичні шляхи БД.
Обгортає SourceFieldMapper, додає бізнес-логіку та нормалізацію.
"""

import logging
from typing import Dict, Any, Optional, Tuple

from utils.source_field_mapper import SourceFieldMapper

logger = logging.getLogger(__name__)

# Geo-поля залишаються логічними для GeoFilterBuilder (він сам мапить через SourceFieldMapper)
GEO_LOGICAL_KEYS = frozenset({"city", "region"})


class FieldMappingService:
    """
    Єдиний сервіс маппінгу: logical → physical.
    PipelineBuilder/PipelineInterpreter використовують тільки його, не SourceFieldMapper напряму.
    """

    @classmethod
    def map_logical_to_physical(
        cls,
        collection: str,
        logical_filters: Dict[str, Any]
    ) -> Dict[str, Any]:
        """
        Перетворює логічні фільтри у фізичні шляхи.

        Geo-поля (city, region) залишаються як є — їх обробляє GeoFilterBuilder.
        Інші поля мапляться на фізичні шляхи через SourceFieldMapper.

        Args:
            collection: Назва колекції
            logical_filters: Логічні фільтри {city: "Київ", price: 100000}

        Returns:
            Словник: geo-фільтри (city, region) + фізичні шляхи для інших
        """
        if not logical_filters:
            return {}

        result = {}
        for key, value in logical_filters.items():
            if key in GEO_LOGICAL_KEYS:
                result[key] = value
            else:
                physical_path = SourceFieldMapper.get_field_path(key, collection)
                if physical_path != key:
                    result[physical_path] = value
                else:
                    result[key] = value
                    logger.debug(
                        "FieldMappingService: Немає маппінгу для '%s' в %s, залишаємо як є",
                        key, collection
                    )
        return result

    @classmethod
    def get_field_path(cls, collection: str, logical_field: str) -> str:
        """Повертає фізичний шлях для логічного поля. Делегує до SourceFieldMapper."""
        return SourceFieldMapper.get_field_path(logical_field, collection)

    @classmethod
    def map_sort_field(cls, collection: str, logical_field: str) -> str:
        """
        Повертає фізичний шлях для сортування.

        Args:
            collection: Назва колекції
            logical_field: Логічна назва (price, date, area)

        Returns:
            Фізичний шлях (price_uah, source_updated_at, building_area_sqm)
        """
        return SourceFieldMapper.get_field_path(logical_field, collection)

    @classmethod
    def get_addresses_array_path(cls, collection: str) -> Optional[str]:
        """Делегує до SourceFieldMapper."""
        return SourceFieldMapper.get_addresses_array_path(collection)

    @classmethod
    def get_geo_match_keys(cls, collection: str) -> Tuple[str, str]:
        """Делегує до SourceFieldMapper. Повертає (region_key, city_key)."""
        return SourceFieldMapper.get_geo_match_keys(collection)

    @classmethod
    def normalize_physical_filters(
        cls,
        physical_filters: Dict[str, Any],
        collection: str
    ) -> Dict[str, Any]:
        """
        Видаляє дублікати, поля без mapping, неіснуючі поля.

        Args:
            physical_filters: Фізичні фільтри (можуть містити дублікати або невалідні ключі)
            collection: Колекція для перевірки mapping

        Returns:
            Нормалізований словник
        """
        if not physical_filters:
            return {}

        valid_paths = set(GEO_LOGICAL_KEYS)
        if SourceFieldMapper.is_valid_source(collection):
            valid_paths.update(SourceFieldMapper.get_all_fields_for_source(collection).values())
            for k in SourceFieldMapper.FIELD_MAP.get(collection, {}):
                if not k.endswith("_fallback"):
                    valid_paths.add(SourceFieldMapper.get_field_path(k, collection))

        seen = set()
        result = {}
        for key, value in physical_filters.items():
            if key in seen:
                logger.debug("FieldMappingService: Пропуск дубліката ключа %s", key)
                continue
            if key in ("$or", "$and"):
                if key not in result:
                    result[key] = value
                else:
                    if isinstance(result[key], list) and isinstance(value, list):
                        result[key] = result[key] + value
                    else:
                        result[key] = value
                seen.add(key)
                continue
            # Пропускаємо MongoDB оператори як ключі верхнього рівня
            if key.startswith("$"):
                continue
            # Перевіряємо, чи ключ відомий для цієї колекції; для невідомих колекцій — дозволяємо все
            is_valid = (
                key in valid_paths or "." in key
                or (not SourceFieldMapper.is_valid_source(collection))
            )
            if is_valid:
                seen.add(key)
                result[key] = value
            else:
                logger.warning(
                    "FieldMappingService: Пропуск невідомого поля '%s' для %s",
                    key, collection
                )
        return result
