# -*- coding: utf-8 -*-
"""
Збагачення геоконтексту адрес після LLM-парсингу.

Визначає головний населений пункт/область оголошення та підставляє їх
у адреси, де вказано лише вулицю — для точного геокодування.
"""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional

_LOCATION_CONTEXT_KEYS = (
    "region",
    "district",
    "settlement_type",
    "settlement",
    "hromada",
)


def _strip_str(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, str):
        return value.strip()
    return str(value).strip()


def normalize_region_name(region: str) -> str:
    """«Київська область» / «Київська обл.» → «Київська»."""
    text = _strip_str(region)
    if not text:
        return ""
    return re.sub(r"\s*(?:область|обл\.?)\s*$", "", text, flags=re.I).strip()


def detect_geo_present(addr: Dict[str, Any]) -> Dict[str, bool]:
    """Чи містить адреса явно вказані компоненти регіону, НП та вулиці."""
    return {
        "region": bool(_strip_str(addr.get("region"))),
        "settlement": bool(_strip_str(addr.get("settlement"))),
        "street": bool(_strip_str(addr.get("street"))),
    }


def _extract_location_context(llm_result: Dict[str, Any]) -> Dict[str, str]:
    raw = llm_result.get("location_context")
    if not isinstance(raw, dict):
        return {key: "" for key in _LOCATION_CONTEXT_KEYS}
    return {
        "region": normalize_region_name(raw.get("region") or ""),
        "district": _strip_str(raw.get("district")),
        "settlement_type": _strip_str(raw.get("settlement_type")),
        "settlement": _strip_str(raw.get("settlement")),
        "hromada": _strip_str(raw.get("hromada")),
    }


def _merge_context(*contexts: Optional[Dict[str, Any]]) -> Dict[str, str]:
    merged = {key: "" for key in _LOCATION_CONTEXT_KEYS}
    for ctx in contexts:
        if not isinstance(ctx, dict):
            continue
        for key in _LOCATION_CONTEXT_KEYS:
            value = _strip_str(ctx.get(key))
            if key == "region" and value:
                value = normalize_region_name(value)
            if value:
                merged[key] = value
    return merged


def _context_from_addresses(addresses: List[Dict[str, Any]]) -> Dict[str, str]:
    found: Dict[str, str] = {}
    for addr in addresses:
        if not isinstance(addr, dict):
            continue
        if not found.get("settlement"):
            settlement = _strip_str(addr.get("settlement"))
            if settlement:
                found["settlement"] = settlement
                stype = _strip_str(addr.get("settlement_type"))
                if stype:
                    found["settlement_type"] = stype
        if not found.get("region"):
            region = normalize_region_name(addr.get("region") or "")
            if region:
                found["region"] = region
        if not found.get("district"):
            district = _strip_str(addr.get("district"))
            if district:
                found["district"] = district
        if not found.get("hromada"):
            hromada = _strip_str(addr.get("hromada"))
            if hromada:
                found["hromada"] = hromada
    return found


def _apply_special_city_region(ctx: Dict[str, str]) -> Dict[str, str]:
    """
    Для спецміст (Київ, Севастополь) картка часто має region=«Київська область».
    Це ламає вибір геокоду (prefer oblast) — підміняємо на регіон-місто.
    """
    from utils.ukraine_regions import is_special_city_region

    settlement = _strip_str(ctx.get("settlement"))
    region = _strip_str(ctx.get("region"))
    if not settlement:
        return ctx

    settlement_l = settlement.casefold()
    special_by_settlement = {
        "київ": "м. Київ",
        "м. київ": "м. Київ",
        "м київ": "м. Київ",
        "місто київ": "м. Київ",
        "севастополь": "м. Севастополь",
        "м. севастополь": "м. Севастополь",
        "м севастополь": "м. Севастополь",
        "місто севастополь": "м. Севастополь",
    }
    canonical = special_by_settlement.get(settlement_l)
    if not canonical:
        return ctx

    if is_special_city_region(region):
        return ctx

    region_norm = normalize_region_name(region).casefold()
    # «Київська» / порожньо → регіон спецміста; інші області не чіпаємо
    if canonical == "м. Київ":
        if not region_norm or region_norm.startswith("київськ"):
            out = dict(ctx)
            out["region"] = canonical
            return out
    elif canonical == "м. Севастополь":
        if not region_norm or "севастопол" in region_norm:
            out = dict(ctx)
            out["region"] = canonical
            return out
    return ctx


def _parse_card_city_value(raw_city: str) -> Dict[str, str]:
    """«Київ, Голосіївський» → settlement + district (район міста)."""
    from utils.district_normalizer import split_city_and_district

    city, district = split_city_and_district(raw_city)
    out: Dict[str, str] = {}
    if city:
        out["settlement"] = city
    if district:
        out["district"] = district
    return out


def normalize_olx_detail_location(loc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """
    Нормалізує збережений detail.location:
    city=«Київ, Голосіївський» → city=Київ, city_district=Голосіївський;
    для м. Київ region «Київська область» → «місто Київ».
    """
    if not isinstance(loc, dict):
        return loc
    out = dict(loc)
    city_raw = _strip_str(out.get("city"))
    parsed = _parse_card_city_value(city_raw) if city_raw else {}
    if parsed.get("settlement"):
        out["city"] = parsed["settlement"]
    if parsed.get("district"):
        out["city_district"] = parsed["district"]

    city = _strip_str(out.get("city"))
    region = _strip_str(out.get("region"))
    city_l = city.casefold()
    if city_l in ("київ", "м. київ", "м київ", "місто київ"):
        region_norm = normalize_region_name(region).casefold()
        if not region_norm or region_norm.startswith("київськ"):
            out["region"] = "місто Київ"
    elif city_l in ("севастополь", "м. севастополь", "м севастополь"):
        region_norm = normalize_region_name(region).casefold()
        if not region_norm or "севастопол" in region_norm:
            out["region"] = "м. Севастополь"

    return out


def build_listing_context(
    search_data: Optional[Dict[str, Any]] = None,
    detail_data: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Витягує стабільний геоконтекст з полів картки/деталей оголошення OLX."""
    search_data = search_data or {}
    detail_data = detail_data or {}
    ctx: Dict[str, str] = {}

    loc = detail_data.get("location")
    if isinstance(loc, dict):
        loc = normalize_olx_detail_location(loc) or loc
        region = normalize_region_name(loc.get("region") or "")
        city = _strip_str(loc.get("city"))
        if region:
            ctx["region"] = region
        if city:
            parsed = _parse_card_city_value(city)
            ctx["settlement"] = parsed.get("settlement") or city
            if parsed.get("district") and not ctx.get("district"):
                ctx["district"] = parsed["district"]
        city_district = _strip_str(loc.get("city_district"))
        if city_district and not ctx.get("district"):
            ctx["district"] = city_district

    search_loc = _strip_str(search_data.get("location"))
    if search_loc:
        parts = [part.strip() for part in search_loc.split(",") if part.strip()]
        if parts and not ctx.get("settlement"):
            parsed = _parse_card_city_value(parts[0])
            ctx["settlement"] = parsed.get("settlement") or parts[0]
            if parsed.get("district") and not ctx.get("district"):
                ctx["district"] = parsed["district"]
        if len(parts) >= 2 and not ctx.get("district"):
            from utils.district_normalizer import normalize_district_for_kyiv

            maybe_district = normalize_district_for_kyiv(parts[1])
            settlement_l = _strip_str(ctx.get("settlement")).casefold()
            if maybe_district and settlement_l in ("київ", "м. київ", "м київ"):
                ctx["district"] = maybe_district
        for part in parts[1:]:
            lowered = part.lower()
            if "област" in lowered or re.search(r"\bобл\.?\b", lowered):
                region = normalize_region_name(part)
                if region:
                    ctx["region"] = region

    ctx = {key: value for key, value in ctx.items() if value}
    return _apply_special_city_region(ctx)


def enrich_address_with_context(
    addr: Dict[str, Any],
    default_context: Dict[str, str],
) -> Dict[str, Any]:
    """Підставляє region/settlement у адресу, якщо вони відсутні, але є в контексті."""
    enriched = dict(addr)
    geo = enriched.get("geo_present")
    if not isinstance(geo, dict):
        geo = detect_geo_present(enriched)

    if not geo.get("settlement") and default_context.get("settlement"):
        enriched["settlement"] = default_context["settlement"]
        if default_context.get("settlement_type") and not _strip_str(enriched.get("settlement_type")):
            enriched["settlement_type"] = default_context["settlement_type"]
    if not geo.get("region") and default_context.get("region"):
        enriched["region"] = default_context["region"]
    if not _strip_str(enriched.get("district")) and default_context.get("district"):
        enriched["district"] = default_context["district"]
    if not _strip_str(enriched.get("hromada")) and default_context.get("hromada"):
        enriched["hromada"] = default_context["hromada"]

    enriched["geo_present"] = detect_geo_present(enriched)
    return enriched


def enrich_llm_geo_result(
    llm_result: Optional[Dict[str, Any]],
    listing_context: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Збагачує результат LLM-парсингу:
    - нормалізує location_context;
    - підставляє населений пункт/область у адреси без них.
    """
    if not isinstance(llm_result, dict):
        return llm_result or {}

    addresses_raw = llm_result.get("addresses") or []
    addresses: List[Dict[str, Any]] = [
        dict(addr) for addr in addresses_raw if isinstance(addr, dict)
    ]

    default_context = _merge_context(
        _extract_location_context(llm_result),
        _context_from_addresses(addresses),
        listing_context or {},
    )

    from utils.district_normalizer import sanitize_llm_address_districts

    enriched_addresses = [
        sanitize_llm_address_districts(enrich_address_with_context(addr, default_context))
        for addr in addresses
    ]

    result = dict(llm_result)
    result["addresses"] = enriched_addresses
    result["location_context"] = default_context
    return result


def _address_has_street_without_settlement(addr: Dict[str, Any]) -> bool:
    if not _strip_str(addr.get("street")):
        return False
    geo = addr.get("geo_present")
    if isinstance(geo, dict):
        return bool(geo.get("street")) and not geo.get("settlement")
    return not _strip_str(addr.get("settlement"))


def analyze_geo_reprocess_candidate(
    search_data: Optional[Dict[str, Any]],
    detail_data: Optional[Dict[str, Any]],
) -> Dict[str, Any]:
    """
    Аналізує, чи потребує оголошення повторного геокодування з урахуванням контексту НП.
    """
    search_data = search_data or {}
    detail_data = detail_data or {}
    listing_context = build_listing_context(search_data, detail_data)
    llm = detail_data.get("llm") if isinstance(detail_data.get("llm"), dict) else {}
    addresses = [
        addr for addr in (llm.get("addresses") or []) if isinstance(addr, dict)
    ]

    reasons: List[str] = []
    if not listing_context.get("settlement") and not listing_context.get("region"):
        return {
            "needs_reprocess": False,
            "reasons": ["no_listing_context"],
            "listing_context": listing_context,
            "current_query": "",
            "enriched_query": "",
        }

    raw_loc = detail_data.get("location")
    if isinstance(raw_loc, dict):
        raw_city = _strip_str(raw_loc.get("city"))
        if raw_city and "," in raw_city:
            from utils.district_normalizer import split_city_and_district

            city_part, district_part = split_city_and_district(raw_city)
            if city_part and district_part and city_part != raw_city:
                reasons.append("glued_city_district")

    search_loc = _strip_str(search_data.get("location"))
    if "," in search_loc:
        from utils.district_normalizer import split_city_and_district

        city_part, district_part = split_city_and_district(search_loc)
        if city_part and district_part and city_part != search_loc:
            if "glued_city_district" not in reasons:
                reasons.append("card_location_city_district")

    if not addresses and not reasons:
        return {
            "needs_reprocess": False,
            "reasons": ["no_llm_addresses"],
            "listing_context": listing_context,
            "current_query": "",
            "enriched_query": "",
        }

    if addresses and any(_address_has_street_without_settlement(addr) for addr in addresses):
        reasons.append("street_without_settlement")

    enriched_llm = enrich_llm_geo_result(dict(llm), listing_context=listing_context)
    enriched_addresses = enriched_llm.get("addresses") or []
    current_query = format_address_line_from_llm(addresses[0]) if addresses else ""
    enriched_query = (
        format_address_line_from_llm(enriched_addresses[0])
        if enriched_addresses and isinstance(enriched_addresses[0], dict)
        else ""
    )
    if current_query != enriched_query and enriched_query:
        reasons.append("geocode_query_would_change")

    resolved = detail_data.get("resolved_locations") or []
    settlement = listing_context.get("settlement", "")
    if settlement and resolved:
        first_query = _strip_str((resolved[0] or {}).get("query_text")).lower()
        if (
            addresses
            and _strip_str(addresses[0].get("street"))
            and settlement.lower() not in first_query
        ):
            reasons.append("geocode_query_missing_settlement")

        settlement_l = settlement.casefold()
        bare = settlement_l.replace("м. ", "").replace("м ", "")
        has_city_hit = False
        for loc in resolved:
            if not isinstance(loc, dict):
                continue
            for r in loc.get("results") or []:
                if not isinstance(r, dict):
                    continue
                struct = r.get("address_structured") or {}
                city = _strip_str(struct.get("city")).casefold()
                if city == settlement_l or city == bare:
                    has_city_hit = True
                    break
            if has_city_hit:
                break
        if has_city_hit and (
            "glued_city_district" in reasons
            or "card_location_city_district" in reasons
            or bare in ("київ", "севастополь")
        ):
            # Є city-hit у resolved, але склейка/старий вибір міг спорожнити root city
            reasons.append("resolved_city_needs_resync")

    unique_reasons = list(dict.fromkeys(reasons))
    return {
        "needs_reprocess": bool(unique_reasons),
        "reasons": unique_reasons,
        "listing_context": listing_context,
        "current_query": current_query,
        "enriched_query": enriched_query,
    }


def listing_needs_geo_reprocess(
    search_data: Optional[Dict[str, Any]],
    detail_data: Optional[Dict[str, Any]],
) -> bool:
    """Чи потребує оголошення повторного геокодування з підстановкою НП/області."""
    return analyze_geo_reprocess_candidate(search_data, detail_data)["needs_reprocess"]


def build_olx_geo_reprocess_mongo_query(region: Optional[str] = None) -> Dict[str, Any]:
    """Mongo-запит для попереднього відбору кандидатів на переобробку геоконтексту."""
    street_only = {
        "detail.llm.addresses": {
            "$elemMatch": {
                "street": {"$exists": True, "$nin": [None, ""]},
                "$or": [
                    {"settlement": {"$exists": False}},
                    {"settlement": None},
                    {"settlement": ""},
                ],
            }
        },
        "$or": [
            {"detail.location.city": {"$exists": True, "$nin": [None, ""]}},
            {"search_data.location": {"$exists": True, "$nin": [None, ""]}},
        ],
    }
    # Будь-яке місто: «Одеса, Приморський» / «Харків, Шевченківський» тощо
    glued_city_card = {
        "$or": [
            {"detail.location.city": {"$regex": r".+,"}},
            {"search_data.location": {"$regex": r"^[^,]+,\s*\S"}},
        ]
    }
    query: Dict[str, Any] = {"$or": [street_only, glued_city_card]}
    if region:
        region_norm = normalize_region_name(region)
        query = {
            "$and": [
                query,
                {
                    "$or": [
                        {"detail.location.region": {"$regex": region_norm, "$options": "i"}},
                        {"search_data.location": {"$regex": region_norm, "$options": "i"}},
                        {"search_data.location": {"$regex": r"^Київ", "$options": "i"}},
                    ]
                },
            ]
        }
    return query


def build_address_from_llm_or_context(
    *,
    search_data: Optional[Dict[str, Any]] = None,
    detail_data: Optional[Dict[str, Any]] = None,
    location_context: Optional[Dict[str, Any]] = None,
) -> List[Dict[str, Any]]:
    """
    Fallback-адреси без Google: LLM addresses або контекст картки (НП/область/район).

    Гарантує root city у unified, коли resolved_locations порожні
    (типово для великих міст зі склейкою «Місто, район»).
    """
    detail_data = detail_data or {}
    search_data = search_data or {}
    ctx = location_context if isinstance(location_context, dict) else resolve_listing_location_context(
        search_data=search_data,
        detail_data=detail_data,
    )
    llm = detail_data.get("llm") if isinstance(detail_data.get("llm"), dict) else {}
    out: List[Dict[str, Any]] = []

    for raw in llm.get("addresses") or []:
        if not isinstance(raw, dict):
            continue
        settlement = _strip_str(raw.get("settlement")) or _strip_str(ctx.get("settlement"))
        region = _strip_str(raw.get("region")) or _strip_str(ctx.get("region"))
        street = _strip_str(raw.get("street"))
        if not settlement and not street:
            continue
        district = _strip_str(raw.get("district"))
        city_district = _strip_str(raw.get("city_district") or raw.get("settlement_district"))
        # Район міста зі склейки картки, якщо LLM лишив його порожнім
        ctx_district = _strip_str(ctx.get("district"))
        if not city_district and ctx_district and settlement:
            if _settlements_compatible(settlement, ctx.get("settlement") or settlement):
                city_district = ctx_district
        addr = {
            "region": region or None,
            "district": district or None,
            "settlement": settlement or None,
            "city_district": city_district or None,
            "street_type": _strip_str(raw.get("street_type")) or None,
            "street": street or None,
            "building": _strip_str(raw.get("building")) or None,
            "is_complete": bool(_strip_str(raw.get("building"))),
            "source": "llm_fallback",
        }
        if city_district and district and city_district.casefold() == district.casefold():
            # Не дублювати міський район як район області
            addr["district"] = None
        out.append(addr)

    if not out:
        settlement = _strip_str(ctx.get("settlement"))
        if settlement:
            out.append(
                {
                    "region": _strip_str(ctx.get("region")) or None,
                    "district": None,
                    "settlement": settlement,
                    "city_district": _strip_str(ctx.get("district")) or None,
                    "street_type": None,
                    "street": None,
                    "building": None,
                    "is_complete": False,
                    "source": "card_context_fallback",
                }
            )

    return filter_addresses_by_location_context(out, ctx)


def format_address_line_from_llm(addr: Dict[str, Any]) -> str:
    """Збирає рядок адреси для геокодування (region → settlement → street → building)."""
    from utils.district_normalizer import sanitize_llm_address_districts

    addr = sanitize_llm_address_districts(addr if isinstance(addr, dict) else {})
    parts: List[str] = []
    region = _strip_str(addr.get("region"))
    if region:
        parts.append(region)
    district = _strip_str(addr.get("district"))
    if district:
        parts.append(district)
    settlement_type = _strip_str(addr.get("settlement_type"))
    settlement = _strip_str(addr.get("settlement"))
    if settlement:
        parts.append(f"{settlement_type} {settlement}".strip() if settlement_type else settlement)
    settlement_district = _strip_str(addr.get("settlement_district"))
    if settlement_district:
        parts.append(settlement_district)
    street_type = _strip_str(addr.get("street_type"))
    street = _strip_str(addr.get("street"))
    if street:
        parts.append(f"{street_type} {street}".strip() if street_type else street)
    building = _strip_str(addr.get("building"))
    if building:
        parts.append(building)
    building_part = _strip_str(addr.get("building_part"))
    if building_part:
        parts.append(building_part)
    room = _strip_str(addr.get("room"))
    if room:
        parts.append(room)
    return ", ".join(parts)


def _settlement_key(value: Any) -> str:
    from utils.settlement_normalizer import normalize_settlement_name

    raw = _strip_str(value)
    if not raw:
        return ""
    normalized = normalize_settlement_name(raw) or raw
    return normalized.casefold()


def _region_key(value: Any) -> str:
    raw = normalize_region_name(_strip_str(value))
    if not raw:
        return ""
    # «місто Київ» / «м. Київ» → порівнюємо як окремий ключ регіону-міста
    lowered = raw.casefold()
    lowered = re.sub(r"^(?:місто|м\.?)\s+", "", lowered).strip()
    return lowered


def _regions_compatible(ctx_region: str, addr_region: str) -> bool:
    """Чи узгоджуються область контексту і область з геокоду."""
    a = _region_key(ctx_region)
    b = _region_key(addr_region)
    if not a or not b:
        return True
    if a == b:
        return True
    # Спецмісто «Київ» ≠ область «Київська» (substring «київ»∈«київська» — хибний збіг)
    special = {"київ", "киев", "севастополь"}
    def _is_oblast_of_special(short: str, other: str) -> bool:
        if short not in special or other == short:
            return False
        return other.startswith(short) and (
            other.endswith("ська") or other.endswith("ской") or "област" in other
        )

    if _is_oblast_of_special(a, b) or _is_oblast_of_special(b, a):
        return False
    # «Одеська» ↔ «Одеська область» уже нормалізовані; також допускаємо префікс
    if a in b or b in a:
        return True
    if a in ("київ", "киев") and b in ("київ", "киев"):
        return True
    return False


def _settlements_compatible(ctx_settlement: str, addr_settlement: str) -> bool:
    a = _settlement_key(ctx_settlement)
    b = _settlement_key(addr_settlement)
    if not a or not b:
        return True
    if a == b:
        return True
    # Склеєні значення з картки («Київ, Печерський») vs геокод («Київ»)
    from utils.district_normalizer import split_city_and_district

    for left, right in ((ctx_settlement, addr_settlement), (addr_settlement, ctx_settlement)):
        city, _district = split_city_and_district(left)
        if city and _settlement_key(city) == _settlement_key(right):
            return True
    return False


def address_contradicts_location_context(
    addr: Optional[Dict[str, Any]],
    location_context: Optional[Dict[str, Any]],
) -> bool:
    """
    True, якщо адреса/геокод явно суперечить location_context оголошення.

    Приклад: контекст Затока/Одеська, а геокод повернув Київ через «Вокзальна».
    """
    if not isinstance(addr, dict) or not isinstance(location_context, dict):
        return False

    ctx_settlement = _strip_str(location_context.get("settlement"))
    ctx_region = normalize_region_name(location_context.get("region") or "")
    if not ctx_settlement and not ctx_region:
        return False

    addr_settlement = _strip_str(
        addr.get("settlement") or addr.get("city") or addr.get("locality")
    )
    addr_region = normalize_region_name(addr.get("region") or "")

    if ctx_settlement and addr_settlement and not _settlements_compatible(ctx_settlement, addr_settlement):
        return True
    if ctx_region and addr_region and not _regions_compatible(ctx_region, addr_region):
        return True
    return False


def geocode_result_contradicts_location_context(
    result: Optional[Dict[str, Any]],
    location_context: Optional[Dict[str, Any]],
) -> bool:
    """Перевірка одного нормалізованого результату Google Geocoding."""
    if not isinstance(result, dict):
        return False
    struct = result.get("address_structured")
    if not isinstance(struct, dict):
        struct = {}
    addr = {
        "settlement": struct.get("city") or struct.get("sublocality") or struct.get("settlement"),
        "region": struct.get("region"),
        "city": struct.get("city"),
    }
    return address_contradicts_location_context(addr, location_context)


def filter_geocode_results_by_location_context(
    results: Optional[List[Dict[str, Any]]],
    location_context: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """
    Залишає лише геокод-результати, узгоджені з location_context.
    Якщо всі суперечать — повертає [] (краще нічого, ніж хибний Київ/Львів).
    """
    items = [r for r in (results or []) if isinstance(r, dict)]
    if not items:
        return []
    if not isinstance(location_context, dict):
        return items
    if not _strip_str(location_context.get("settlement")) and not _strip_str(
        location_context.get("region")
    ):
        return items

    kept = [
        r for r in items
        if not geocode_result_contradicts_location_context(r, location_context)
    ]
    return kept


def filter_addresses_by_location_context(
    addresses: Optional[List[Dict[str, Any]]],
    location_context: Optional[Dict[str, Any]],
) -> List[Dict[str, Any]]:
    """Відкидає збережені адреси, що суперечать location_context."""
    items = [a for a in (addresses or []) if isinstance(a, dict)]
    if not items:
        return []
    return [
        a for a in items
        if not address_contradicts_location_context(a, location_context)
    ]


def resolve_listing_location_context(
    *,
    search_data: Optional[Dict[str, Any]] = None,
    detail_data: Optional[Dict[str, Any]] = None,
    llm_result: Optional[Dict[str, Any]] = None,
) -> Dict[str, str]:
    """Єдиний location_context для фільтрації геокоду/адрес (картка OLX має пріоритет)."""
    detail_data = detail_data or {}
    llm = llm_result if isinstance(llm_result, dict) else detail_data.get("llm")
    if not isinstance(llm, dict):
        llm = {}
    return _merge_context(
        _context_from_addresses(
            [a for a in (llm.get("addresses") or []) if isinstance(a, dict)]
        ),
        _extract_location_context(llm),
        build_listing_context(search_data, detail_data),
    )
