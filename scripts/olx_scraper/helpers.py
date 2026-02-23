# -*- coding: utf-8 -*-
"""
Спільні допоміжні функції для скрапера OLX (run_prototype, run_update).
"""

import re
from typing import Optional


def _has_region_in_text(text: str) -> bool:
    """Перевіряє, чи текст містить згадку області (для пріоритету над короткими топонімами)."""
    if not text or not isinstance(text, str):
        return False
    t = text.strip().lower()
    return bool(re.search(r"область|обл\.|обл\b", t))


def _should_skip_ambiguous_short_location(short: str, existing_queries: list) -> bool:
    """
    Пропускати короткі неоднозначні топоніми (наприклад "Іванівка"), якщо вже є
    більш специфічна адреса з регіоном, що містить цей топонім.
    Уникає помилкового геокодування (напр. Іванівка Тернопільська замість Житомирської).
    """
    if not short or len(short.strip()) < 4:
        return False
    s = short.strip()
    for q in existing_queries:
        if not q or q == s:
            continue
        q_lower = q.strip().lower()
        s_lower = s.lower()
        if s_lower in q_lower and _has_region_in_text(q):
            return True
    return False


def search_data_from_listing(item: dict) -> dict:
    """Формує словник search_data для збереження з полів картки пошуку."""
    return {
        "title": item.get("title"),
        "price_text": item.get("price_text"),
        "price_value": item.get("price_value"),
        "currency": item.get("currency"),
        "location": item.get("location"),
        "date_text": item.get("date_text"),
        "listed_at_iso": item.get("listed_at_iso"),
        "area_m2": item.get("area_m2"),
        "raw_snippet": item.get("raw_snippet"),
    }


def _address_line_from_llm_address(addr: dict) -> str:
    """Збирає один рядок адреси з об'єкта LLM (region, settlement, street, building тощо)."""
    parts = []
    if addr.get("region"):
        parts.append(addr["region"])
    if addr.get("district"):
        parts.append(addr["district"])
    st = addr.get("settlement_type") or ""
    settlement = (addr.get("settlement") or "").strip()
    if settlement:
        parts.append((st + " " + settlement).strip() if st else settlement)
    if addr.get("settlement_district"):
        parts.append(addr["settlement_district"])
    street_type = addr.get("street_type") or ""
    street = (addr.get("street") or "").strip()
    if street:
        parts.append((street_type + " " + street).strip() if street_type else street)
    if addr.get("building"):
        parts.append(addr["building"])
    if addr.get("building_part"):
        parts.append(addr["building_part"])
    if addr.get("room"):
        parts.append(addr["room"])
    return ", ".join(p for p in parts if p).strip()


def _collect_and_geocode_locations(
    search_data: dict,
    detail_data: dict,
    geocoding_service: "GeocodingService",
    geography_service: Optional["GeographyService"] = None,
):
    """
    Збирає рядки адрес/топонімів з search_data та detail (включно з llm.addresses),
    геокодує кожен унікальний рядок, повертає (geocode_query_hashes, resolved_locations, address_refs).
    
    Пріоритет: LLM-адреси (найповніші, з регіоном) → raw → city → search location.
    Дедуплікація: не геокодувати короткі неоднозначні топоніми (напр. "Іванівка"), якщо є
    повніша адреса з регіоном — уникнення помилок (Іванівка Тернопільська замість Житомирської).
    
    Якщо geography_service надано, також створює посилання на топоніми.
    """
    seen = set()
    query_strings = []

    # 1. LLM-адреси — пріоритет, бо містять регіон з тексту оголошення
    llm = detail_data.get("llm") or {}
    for addr in llm.get("addresses") or []:
        if not isinstance(addr, dict):
            continue
        line = _address_line_from_llm_address(addr)
        if line and line not in seen:
            seen.add(line)
            query_strings.append(line)

    # 2. detail.location.raw
    loc = detail_data.get("location") or {}
    if isinstance(loc, dict):
        raw = (loc.get("raw") or "").strip()
        if raw and raw not in seen:
            seen.add(raw)
            query_strings.append(raw)

    # 3. detail.location.city — пропускати, якщо вже є повніша адреса з регіоном
    if isinstance(loc, dict):
        city = (loc.get("city") or "").strip()
        if city and city not in seen:
            if not _should_skip_ambiguous_short_location(city, query_strings):
                seen.add(city)
                query_strings.append(city)

    # 4. search_data.location
    loc_text = (search_data.get("location") or "").strip()
    if loc_text and loc_text not in seen:
        if not _should_skip_ambiguous_short_location(loc_text, query_strings):
            seen.add(loc_text)
            query_strings.append(loc_text)

    geocode_hashes = []
    resolved_locations = []
    address_refs_list = []
    
    for q in query_strings:
        try:
            out = geocoding_service.geocode(query=q, region="ua", caller="olx_scraper")
            geocode_hashes.append(out["query_hash"])
            resolved_locations.append({
                "query_hash": out["query_hash"],
                "query_text": out["query_text"],
                "results": out["results"],
            })
        except Exception:
            pass
    
    # Обробка адрес з LLM для створення посилань
    if geography_service:
        for addr in llm.get("addresses") or []:
            if not isinstance(addr, dict):
                continue
            try:
                address_refs = geography_service.resolve_address(addr)
                if address_refs.get("region_id") or address_refs.get("city_id"):
                    address_refs_list.append(address_refs["address_refs"])
            except Exception:
                pass
    
    return geocode_hashes, resolved_locations, address_refs_list


def search_data_changed(stored: dict, current: dict) -> bool:
    """
    Порівнює дані пошуку: чи відрізняються значення, що впливають на контент оголошення.
    date_text та listed_at_iso не враховуються — вони змінюються з часом («Вчора» → «Сьогодні»)
    і не повинні призводити до повторного завантаження деталей та виклику LLM.
    """
    keys = ("title", "price_text", "price_value", "currency", "location", "area_m2")
    for k in keys:
        v1 = stored.get(k)
        v2 = current.get(k)
        if v1 != v2:
            if isinstance(v1, (int, float)) and isinstance(v2, (int, float)):
                if abs((v1 or 0) - (v2 or 0)) < 1e-6:
                    continue
            return True
    return False
