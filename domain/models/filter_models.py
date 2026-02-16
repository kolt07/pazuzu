# -*- coding: utf-8 -*-
"""
Моделі фільтрів та геофільтрів для домен-шару.
Підтримка груп (AND, OR, NOT) та вкладеності.
"""

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional, Union


class FilterGroupType(str, Enum):
    """Тип логічної групи фільтрів."""
    AND = "and"
    OR = "or"
    NOT = "not"


class FilterOperator(str, Enum):
    """Оператори порівняння для елементів фільтра."""
    EQ = "eq"           # дорівнює
    NE = "ne"           # не дорівнює
    GT = "gt"           # більше
    GTE = "gte"         # більше або дорівнює
    LT = "lt"           # менше
    LTE = "lte"         # менше або дорівнює
    IN = "in"           # міститься в списку
    NIN = "nin"         # не міститься в списку
    CONTAINS = "contains"  # містить (рядок)
    FILLED = "filled"   # заповнено
    EMPTY = "empty"     # не заповнено
    COUNT = "count"     # кількість (для масивів)


class GeoFilterOperator(str, Enum):
    """Оператори для геофільтрів."""
    IN_HIERARCHY = "in_hierarchy"  # в ієрархії (міста області, вулиці міста)
    EQ = "eq"           # дорівнює
    NE = "ne"           # не дорівнює (напр. «не в Києві»)
    IN_RADIUS = "in_radius"  # в радіусі (км) — тільки для координат


@dataclass
class FilterElement:
    """Елемент фільтра: поле, оператор, значення."""
    field: str
    operator: FilterOperator
    value: Any
    # Ліве значення — зазвичай поле (field), праве — value


@dataclass
class FilterGroup:
    """Група фільтрів (може містити елементи та вкладені групи)."""
    group_type: FilterGroupType
    items: List[Union["FilterElement", "FilterGroup"]] = field(default_factory=list)


@dataclass
class GeoFilterElement:
    """
    Елемент геофільтра.
    Може містити: назву населеного пункту, області, вулиці або координати.
    """
    operator: GeoFilterOperator
    # Тип: "settlement" | "region" | "street" | "coordinates"
    geo_type: str
    value: Any  # рядок (назва) або dict {"latitude": x, "longitude": y}
    radius_km: Optional[float] = None  # для IN_RADIUS


@dataclass
class GeoFilterGroup:
    """Група геофільтрів (логіка як у FilterGroup)."""
    group_type: FilterGroupType
    items: List[Union[GeoFilterElement, "GeoFilterGroup"]] = field(default_factory=list)


@dataclass
class GeoFilter:
    """Повний геофільтр — група або один елемент."""
    root: Union[GeoFilterElement, GeoFilterGroup]


@dataclass
class FindQuery:
    """
    Параметри пошуку для методу CollectionManager.find().
    """
    filters: Optional[FilterGroup] = None
    geo_filters: Optional[GeoFilter] = None
    sort: Optional[List[Dict[str, Any]]] = None  # [{"field": "price_uah", "order": -1}]
    group_by: Optional[List[str]] = None
    fields: Optional[List[str]] = None  # None = всі поля
    limit: Optional[int] = None
    skip: Optional[int] = None
    collection: Optional[str] = None  # для контексту
