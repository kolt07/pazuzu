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

    if entity == "unified_listings":
        return _resolve_geo_filter_unified(city_values, region_values)
    if entity == "olx_listings":
        return _resolve_geo_filter_olx(city_values, region_values)
    if entity == "prozorro_auctions":
        return _resolve_geo_filter_prozorro(city_values, region_values)
    return None


def _resolve_geo_filter_unified(city_values: List[str], region_values: List[str]) -> Optional[Dict[str, Any]]:
    """
    unified_listings: addresses з $elemMatch (region, settlement).
    """
    or_conditions: List[Dict[str, Any]] = []

    for region in region_values:
        for rv in _normalize_region_value(region):
            or_conditions.append({
                "addresses": {"$elemMatch": {"region": {"$regex": rv, "$options": "i"}}}
            })

    for city in city_values:
        c = city.strip()
        if not c:
            continue
        or_conditions.append({
            "addresses": {"$elemMatch": {"settlement": {"$regex": c, "$options": "i"}}}
        })

    if not or_conditions:
        return None
    return {"$or": or_conditions}


def _resolve_geo_filter_olx(city_values: List[str], region_values: List[str]) -> Optional[Dict[str, Any]]:
    """
    OLX: пріоритет detail.address_refs з $elemMatch (region.name, city.name),
    fallback — detail.resolved_locations та search_data.location.
    """
    or_conditions: List[Dict[str, Any]] = []

    for region in region_values:
        for rv in _normalize_region_value(region):
            or_conditions.append({
                "detail.address_refs": {"$elemMatch": {"region.name": {"$regex": rv, "$options": "i"}}}
            })
        or_conditions.append({"search_data.location": {"$regex": region, "$options": "i"}})
        or_conditions.append({
            "detail.resolved_locations": {
                "$elemMatch": {"results.address_structured.region": {"$regex": region, "$options": "i"}}
            }
        })

    for city in city_values:
        c = city.strip()
        if not c:
            continue
        or_conditions.append({
            "detail.address_refs": {"$elemMatch": {"city.name": {"$regex": c, "$options": "i"}}}
        })
        or_conditions.append({"search_data.location": {"$regex": c, "$options": "i"}})
        or_conditions.append({
            "detail.resolved_locations": {
                "$elemMatch": {"$or": [{"results.address_structured.city": {"$regex": c, "$options": "i"}}, {"results.address_structured.settlement": {"$regex": c, "$options": "i"}}]}
            }
        })

    if not or_conditions:
        return None
    return {"$or": or_conditions}


def _resolve_geo_filter_prozorro(city_values: List[str], region_values: List[str]) -> Optional[Dict[str, Any]]:
    """
    ProZorro: auction_data.address_refs з $elemMatch (region.name, city.name).
    Fallback — llm_result.result.addresses (region, settlement) або auction_data.items[].address.
    """
    or_conditions: List[Dict[str, Any]] = []

    for region in region_values:
        for rv in _normalize_region_value(region):
            # Пріоритет: address_refs
            or_conditions.append({
                "auction_data.address_refs": {"$elemMatch": {"region.name": {"$regex": rv, "$options": "i"}}}
            })
        # Fallback 1: llm_result.result.addresses (якщо є join з llm_cache)
        or_conditions.append({
            "llm_result.result.addresses": {"$elemMatch": {"region": {"$regex": region, "$options": "i"}}}
        })
        # Fallback 2: auction_data.items[].address (основний fallback згідно з правилами)
        for rv in _normalize_region_value(region):
            or_conditions.append({
                "auction_data.items": {
                    "$elemMatch": {
                        "address.region.uk_UA": {"$regex": rv, "$options": "i"}
                    }
                }
            })

    for city in city_values:
        c = city.strip()
        if not c:
            continue
        # Пріоритет: address_refs
        or_conditions.append({
            "auction_data.address_refs": {"$elemMatch": {"city.name": {"$regex": c, "$options": "i"}}}
        })
        # Fallback 1: llm_result.result.addresses (якщо є join з llm_cache)
        or_conditions.append({
            "llm_result.result.addresses": {"$elemMatch": {"settlement": {"$regex": c, "$options": "i"}}}
        })
        # Fallback 2: auction_data.items[].address (основний fallback згідно з правилами)
        or_conditions.append({
            "auction_data.items": {
                "$elemMatch": {
                    "address.locality.uk_UA": {"$regex": c, "$options": "i"}
                }
            }
        })

    if not or_conditions:
        return None
    return {"$or": or_conditions}


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
