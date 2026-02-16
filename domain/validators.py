# -*- coding: utf-8 -*-
"""
Domain validators for logical layer. Ensure agents return only logical fields.
"""

from typing import Dict, Any, FrozenSet

from domain.exceptions import LogicalLayerViolation

# Логічні поля, дозволені на рівні агентів. Без крапок, без фізичних шляхів.
ALLOWED_LOGICAL_FILTER_KEYS: FrozenSet[str] = frozenset({
    "city",
    "region",
    "price",
    "date",
    "status",
    "source",
    "property_type",
    "area",
    "building_area_sqm",
    "land_area_ha",
    "bids_count",
    "bidders_count",
})


def validate_logical_filters(filters: Dict[str, Any], context: str = "") -> None:
    """
    Валідує, що фільтри містять тільки логічні поля.

    Args:
        filters: Словник фільтрів (filter_metrics, filters, region_filter)
        context: Контекст для повідомлення про помилку (наприклад, "QueryStructureAgent")

    Raises:
        LogicalLayerViolation: Якщо знайдено фізичне поле (з крапкою або не з allow-list)
    """
    if not filters or not isinstance(filters, dict):
        return

    for key in filters:
        if "." in key:
            raise LogicalLayerViolation(
                f"Physical field detected in logical layer: '{key}'. "
                f"Use only logical fields: city, region, price, date, status, source, property_type, area. "
                f"Context: {context}"
            )
        if key not in ALLOWED_LOGICAL_FILTER_KEYS:
            raise LogicalLayerViolation(
                f"Unknown logical field: '{key}'. "
                f"Allowed: {', '.join(sorted(ALLOWED_LOGICAL_FILTER_KEYS))}. "
                f"Context: {context}"
            )
