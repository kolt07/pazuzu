# -*- coding: utf-8 -*-
"""
Schema-aware filter resolver: будує MongoDB $match для фільтрів за регіоном/містом
з урахуванням схеми колекції (address_refs, fallback на resolved_locations/search_data.location).
Використовується PlannerAgent та AnalysisPlannerAgent.
"""

from typing import Dict, Any, List, Optional

# Синоніми для назв областей (коротка форма ↔ повна)
REGION_SYNONYMS: Dict[str, str] = {
    "Київська": "Київська область",
    "Київська область": "Київська",
    "Львівська": "Львівська область",
    "Львівська область": "Львівська",
    "Харківська": "Харківська область",
    "Харківська область": "Харківська",
    "Одеська": "Одеська область",
    "Одеська область": "Одеська",
    "Дніпропетровська": "Дніпропетровська область",
    "Дніпропетровська область": "Дніпропетровська",
    "Вінницька": "Вінницька область",
    "Вінницька область": "Вінницька",
    "Полтавська": "Полтавська область",
    "Полтавська область": "Полтавська",
    "Житомирська": "Житомирська область",
    "Житомирська область": "Житомирська",
    "Черкаська": "Черкаська область",
    "Черкаська область": "Черкаська",
    "Чернігівська": "Чернігівська область",
    "Чернігівська область": "Чернігівська",
    "Сумська": "Сумська область",
    "Сумська область": "Сумська",
    "Тернопільська": "Тернопільська область",
    "Тернопільська область": "Тернопільська",
    "Івано-Франківська": "Івано-Франківська область",
    "Івано-Франківська область": "Івано-Франківська",
    "Волинська": "Волинська область",
    "Волинська область": "Волинська",
    "Рівненська": "Рівненська область",
    "Рівненська область": "Рівненська",
    "Закарпатська": "Закарпатська область",
    "Закарпатська область": "Закарпатська",
    "Миколаївська": "Миколаївська область",
    "Миколаївська область": "Миколаївська",
    "Кіровоградська": "Кіровоградська область",
    "Кіровоградська область": "Кіровоградська",
    "Херсонська": "Херсонська область",
    "Херсонська область": "Херсонська",
    "Запорізька": "Запорізька область",
    "Запорізька область": "Запорізька",
    "Донецька": "Донецька область",
    "Донецька область": "Донецька",
    "Луганська": "Луганська область",
    "Луганська область": "Луганська",
    "АР Крим": "АР Крим",
}


def _normalize_region_value(value: str) -> List[str]:
    """Повертає список варіантів для пошуку (оригінал + синонім якщо є)."""
    v = (value or "").strip()
    if not v:
        return []
    variants = [v]
    syn = REGION_SYNONYMS.get(v)
    if syn and syn != v:
        variants.append(syn)
    return variants


def resolve_geo_filter(filters: Dict[str, Any], entity: str) -> Optional[Dict[str, Any]]:
    """
    Будує MongoDB $match (або частину) для фільтрів за регіоном та містом.

    Args:
        filters: словник з ключами city (список або рядок), region (список або рядок), location.
        entity: "olx_listings" | "prozorro_auctions"

    Returns:
        Словник для додавання в $match (наприклад {"$or": [...]}) або None, якщо немає гео-фільтрів.
    """
    if not filters or not isinstance(filters, dict):
        return None

    cities = filters.get("city")
    regions = filters.get("region")
    if not cities and not regions:
        return None

    def _to_list(x: Any) -> List[str]:
        if x is None:
            return []
        if isinstance(x, str):
            return [x.strip()] if x.strip() else []
        if isinstance(x, list):
            return [str(v).strip() for v in x if v]
        return []

    city_values = _to_list(cities)
    region_values = _to_list(regions)

    from utils.settlement_geo_match import (
        resolve_olx_geo_filter,
        resolve_prozorro_geo_filter,
        resolve_unified_listings_geo_filter,
    )

    if entity == "unified_listings":
        return resolve_unified_listings_geo_filter(city_values, region_values)
    if entity == "olx_listings":
        return resolve_olx_geo_filter(city_values, region_values)
    if entity == "prozorro_auctions":
        return resolve_prozorro_geo_filter(city_values, region_values)
    return None


def region_filter_to_geo_filter(region_filter: Optional[Dict[str, str]]) -> Optional[Dict[str, Any]]:
    """
    Перетворює region_filter з інтерпретатора (ключі region, city — один рядок)
    у формат filters для resolve_geo_filter (списки або рядки).
    """
    if not region_filter or not isinstance(region_filter, dict):
        return None
    out: Dict[str, Any] = {}
    if region_filter.get("region"):
        out["region"] = region_filter["region"]
    if region_filter.get("city"):
        out["city"] = region_filter["city"]
    return out if out else None
