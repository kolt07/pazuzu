# -*- coding: utf-8 -*-
"""
Централізована логіка геопошуку за населеним пунктом.

Усі шляхи (unified_listings, OLX, ProZorro, GeoFilterBuilder, API пошуку)
мають використовувати цей модуль, щоб однаково враховувати:
- префікси типу НП (смт., с., м., …);
- аліаси з довідника cities;
- city_id з UI;
- область для гомонімів (AND region+settlement, не OR).
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

from utils.settlement_normalizer import (
    SETTLEMENT_PREFIX_REGEX,
    format_settlement_display_name,
    normalize_settlement_name,
)
from utils.ukraine_regions import (
    build_region_search_regex,
    is_special_city_region,
    special_city_from_region,
)


def get_settlement_match_names(
    *,
    city_id: Optional[str] = None,
    settlement_name: Optional[str] = None,
    region: Optional[str] = None,
) -> List[str]:
    """
    Усі варіанти назви НП для пошуку в оголошеннях: канонічна назва, аліаси, введена назва.
    """
    from data.repositories.geography_repository import CitiesRepository, RegionsRepository
    from utils.ukraine_regions import normalize_region_for_repository_lookup

    cities_repo = CitiesRepository()
    regions_repo = RegionsRepository()
    names: List[str] = []
    seen: set[str] = set()

    def _add(raw: Optional[str]) -> None:
        if not raw:
            return
        for candidate in (
            str(raw).strip(),
            normalize_settlement_name(str(raw)),
            format_settlement_display_name(str(raw)),
        ):
            if not candidate:
                continue
            key = candidate.casefold()
            if key in seen:
                continue
            seen.add(key)
            names.append(candidate)

    def _region_id(region_name: Optional[str]) -> Optional[str]:
        if not region_name:
            return None
        lookup = normalize_region_for_repository_lookup(region_name) or region_name
        reg = regions_repo.find_by_name(lookup)
        return str(reg["_id"]) if reg else None

    try:
        if city_id:
            doc = cities_repo.find_by_id(city_id)
            if doc:
                _add(doc.get("name"))
                for alias in doc.get("search_aliases") or []:
                    _add(alias)

        if settlement_name:
            region_id = _region_id(region)
            if region_id:
                found = cities_repo.find_by_name_and_region(settlement_name, region_id)
                if found:
                    _add(found.get("name"))
                    for alias in found.get("search_aliases") or []:
                        _add(alias)
    except Exception:
        pass

    if settlement_name:
        _add(settlement_name)

    return names


def settlement_regex(name: str) -> Dict[str, Any]:
    """MongoDB regex для однієї назви НП (з опційними префіксами типу)."""
    escaped = re.escape(str(name))
    pattern = f"^{SETTLEMENT_PREFIX_REGEX}{escaped}"
    return {"$regex": pattern, "$options": "i"}


def settlement_regex_for_names(names: List[str]) -> Dict[str, Any]:
    """Один regex для кількох варіантів назви (канон + аліаси)."""
    cleaned = [str(n).strip() for n in names if n and str(n).strip()]
    if not cleaned:
        return {"$regex": "^$", "$options": "i"}
    if len(cleaned) == 1:
        return settlement_regex(cleaned[0])
    alt = "|".join(re.escape(n) for n in cleaned)
    pattern = f"^{SETTLEMENT_PREFIX_REGEX}(?:{alt})"
    return {"$regex": pattern, "$options": "i"}


def region_regex_mongo(region: Optional[str]) -> Optional[Dict[str, Any]]:
    if not region:
        return None
    region_pattern = build_region_search_regex(str(region)) or re.escape(str(region))
    return {"$regex": region_pattern, "$options": "i"}


def build_unified_listings_settlement_match(
    settlement_value: str,
    *,
    region: Optional[str] = None,
    city_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    MongoDB-умова для unified_listings: root city/region + addresses + city_id.
    """
    if str(settlement_value) == "__NO_MATCH__":
        return {"_id": {"$exists": False}}

    or_parts: List[Dict[str, Any]] = []
    cid = str(city_id).strip() if city_id else ""
    region_re = region_regex_mongo(region)

    if cid:
        or_parts.append({"city_id": cid})
        addr_by_id: Dict[str, Any] = {"city_id": cid}
        if region_re:
            addr_by_id["region"] = region_re
        or_parts.append({"addresses": {"$elemMatch": addr_by_id}})

    names = get_settlement_match_names(
        city_id=city_id,
        settlement_name=settlement_value,
        region=region,
    )
    if not names:
        names = [str(settlement_value)]
    settlement_re = settlement_regex_for_names(names)

    if region_re:
        or_parts.append({"$and": [{"city": settlement_re}, {"region": region_re}]})
        or_parts.append({
            "addresses": {
                "$elemMatch": {
                    "settlement": settlement_re,
                    "region": region_re,
                }
            }
        })
    else:
        or_parts.append({"city": settlement_re})
        or_parts.append({"addresses": {"$elemMatch": {"settlement": settlement_re}}})

    if not or_parts:
        return {"_id": {"$exists": False}}
    return {"$or": or_parts} if len(or_parts) > 1 else or_parts[0]


def build_unified_listings_region_match(region: str) -> Dict[str, Any]:
    """Фільтр лише за областю."""
    region_re = region_regex_mongo(region)
    if not region_re:
        return {"_id": {"$exists": False}}
    return {
        "$or": [
            {"region": region_re},
            {"addresses": {"$elemMatch": {"region": region_re}}},
        ]
    }


def build_unified_listings_city_region_filter(
    *,
    region: Optional[str] = None,
    city: Optional[str] = None,
    city_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Плоский геофільтр для API (unified_listings): region+city разом → AND, не OR.
    """
    r = (region or "").strip() or None
    c = (city or "").strip() or None

    if r and is_special_city_region(r):
        city_name = special_city_from_region(r) or r
        return build_unified_listings_settlement_match(city_name, region=None, city_id=city_id)

    if c and r:
        return build_unified_listings_settlement_match(c, region=r, city_id=city_id)
    if c:
        return build_unified_listings_settlement_match(c, city_id=city_id)
    if r:
        return build_unified_listings_region_match(r)
    return {}


def build_address_refs_settlement_conditions(
    refs_path: str,
    *,
    settlement_name: str,
    region: Optional[str] = None,
    city_id: Optional[str] = None,
    settlement_key: str = "city.name",
    region_key: str = "region.name",
) -> List[Dict[str, Any]]:
    """
    Умови для масиву address_refs (OLX / ProZorro).
    refs_path: напр. 'detail.address_refs' або 'auction_data.address_refs'.
    """
    names = get_settlement_match_names(
        city_id=city_id,
        settlement_name=settlement_name,
        region=region,
    )
    if not names:
        names = [settlement_name]
    settlement_re = settlement_regex_for_names(names)
    region_re = region_regex_mongo(region)

    conditions: List[Dict[str, Any]] = []
    elem: Dict[str, Any] = {}
    parts = settlement_key.split(".")
    if len(parts) == 2:
        elem[parts[0]] = {parts[1]: settlement_re}
    else:
        elem[settlement_key] = settlement_re
    if region_re:
        rparts = region_key.split(".")
        if len(rparts) == 2:
            elem[rparts[0]] = {rparts[1]: region_re}
        else:
            elem[region_key] = region_re
    conditions.append({refs_path: {"$elemMatch": elem}})
    return conditions


def resolve_unified_listings_geo_filter(
    city_values: List[str],
    region_values: List[str],
) -> Optional[Dict[str, Any]]:
    """Mongo $match для unified_listings (агенти, аналітика)."""
    cities = [c.strip() for c in city_values if c and str(c).strip()]
    regions = [r.strip() for r in region_values if r and str(r).strip()]
    if not cities and not regions:
        return None

    or_conditions: List[Dict[str, Any]] = []

    if cities and regions:
        for city in cities:
            for region in regions:
                or_conditions.append(
                    build_unified_listings_settlement_match(city, region=region)
                )
    elif cities:
        for city in cities:
            or_conditions.append(
                build_unified_listings_settlement_match(city)
            )
    else:
        for region in regions:
            or_conditions.append(build_unified_listings_region_match(region))

    if not or_conditions:
        return None
    return {"$or": or_conditions} if len(or_conditions) > 1 else or_conditions[0]


def _legacy_region_variants(region: str) -> List[str]:
    """Варіанти назви області (коротка ↔ повна) для legacy resolver."""
    from utils.schema_filter_resolver import REGION_SYNONYMS

    v = (region or "").strip()
    if not v:
        return []
    variants = [v]
    syn = REGION_SYNONYMS.get(v)
    if syn and syn != v:
        variants.append(syn)
    return variants


def resolve_olx_geo_filter(
    city_values: List[str],
    region_values: List[str],
) -> Optional[Dict[str, Any]]:
    or_conditions: List[Dict[str, Any]] = []
    cities = [c.strip() for c in city_values if c and str(c).strip()]
    regions = [r.strip() for r in region_values if r and str(r).strip()]

    if cities and regions:
        for city in cities:
            for region in regions:
                or_conditions.extend(
                    build_address_refs_settlement_conditions(
                        "detail.address_refs",
                        settlement_name=city,
                        region=region,
                    )
                )
    elif cities:
        for city in cities:
            names = get_settlement_match_names(settlement_name=city)
            if not names:
                names = [city]
            settlement_re = settlement_regex_for_names(names)
            or_conditions.append({
                "detail.address_refs": {"$elemMatch": {"city.name": settlement_re}}
            })
            or_conditions.append({"search_data.location": settlement_re})
            or_conditions.append({
                "detail.resolved_locations": {
                    "$elemMatch": {
                        "$or": [
                            {"results.address_structured.city": settlement_re},
                            {"results.address_structured.settlement": settlement_re},
                        ]
                    }
                }
            })
    else:
        for region in regions:
            for rv in _legacy_region_variants(region):
                or_conditions.append({
                    "detail.address_refs": {
                        "$elemMatch": {"region.name": {"$regex": rv, "$options": "i"}}
                    }
                })
            or_conditions.append({"search_data.location": {"$regex": region, "$options": "i"}})
            or_conditions.append({
                "detail.resolved_locations": {
                    "$elemMatch": {
                        "results.address_structured.region": {
                            "$regex": region,
                            "$options": "i",
                        }
                    }
                }
            })

    if not or_conditions:
        return None
    return {"$or": or_conditions}


def resolve_prozorro_geo_filter(
    city_values: List[str],
    region_values: List[str],
) -> Optional[Dict[str, Any]]:
    or_conditions: List[Dict[str, Any]] = []
    cities = [c.strip() for c in city_values if c and str(c).strip()]
    regions = [r.strip() for r in region_values if r and str(r).strip()]

    if cities and regions:
        for city in cities:
            for region in regions:
                or_conditions.extend(
                    build_address_refs_settlement_conditions(
                        "auction_data.address_refs",
                        settlement_name=city,
                        region=region,
                    )
                )
    elif cities:
        for city in cities:
            names = get_settlement_match_names(settlement_name=city)
            if not names:
                names = [city]
            settlement_re = settlement_regex_for_names(names)
            or_conditions.append({
                "auction_data.address_refs": {
                    "$elemMatch": {"city.name": settlement_re}
                }
            })
            or_conditions.append({
                "llm_result.result.addresses": {
                    "$elemMatch": {"settlement": settlement_re}
                }
            })
            or_conditions.append({
                "auction_data.items": {
                    "$elemMatch": {"address.locality.uk_UA": settlement_re}
                }
            })
    else:
        for region in regions:
            for rv in _legacy_region_variants(region):
                or_conditions.append({
                    "auction_data.address_refs": {
                        "$elemMatch": {"region.name": {"$regex": rv, "$options": "i"}}
                    }
                })
            or_conditions.append({
                "llm_result.result.addresses": {
                    "$elemMatch": {"region": {"$regex": region, "$options": "i"}}
                }
            })
            for rv in _legacy_region_variants(region):
                or_conditions.append({
                    "auction_data.items": {
                        "$elemMatch": {
                            "address.region.uk_UA": {"$regex": rv, "$options": "i"}
                        }
                    }
                })

    if not or_conditions:
        return None
    return {"$or": or_conditions}
