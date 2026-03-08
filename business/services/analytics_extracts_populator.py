# -*- coding: utf-8 -*-
"""
Популяція колекції analytics_extracts з unified_listings.

Викликається під час перерахунку аналітики (PriceAnalyticsService.rebuild_all).
Витягує плоскі поля з оголошень для швидких агрегацій.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.analytics_extracts_repository import AnalyticsExtractsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository

logger = logging.getLogger(__name__)


def _extract_from_listing(doc: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Витягує плоский документ для analytics_extracts з unified_listings.

    Використовує root-поля та першу адресу з addresses.
    """
    source = doc.get("source")
    source_id = doc.get("source_id")
    if not source or not source_id:
        return None

    # Перша адреса (пріоритет — з is_complete)
    addresses = doc.get("addresses") or []
    addr = None
    for a in addresses:
        if isinstance(a, dict) and a.get("is_complete"):
            addr = a
            break
    if not addr and addresses:
        addr = addresses[0] if isinstance(addresses[0], dict) else None

    # street може бути dict {street_type, name} або рядок
    street_type = ""
    street = ""
    if addr:
        st = addr.get("street")
        if isinstance(st, dict):
            street_type = st.get("street_type") or ""
            street = st.get("name") or st.get("street") or ""
        elif isinstance(st, str):
            street = st

    # settlement_type — тип населеного пункту або територіального об'єкту
    settlement_type = addr.get("settlement_type") if addr else ""

    # land_area_sqm — зберігається в м² у unified_listings
    land_sqm = doc.get("land_area_sqm")
    if land_sqm is not None:
        try:
            v = float(land_sqm)
            if v <= 0:
                land_sqm = None
            else:
                land_sqm = v
        except (TypeError, ValueError):
            land_sqm = None

    # source_date — дата з джерела (ISO рядок для фільтрів)
    source_updated = doc.get("source_updated_at")
    source_date = None
    if source_updated:
        if isinstance(source_updated, datetime):
            source_date = source_updated.strftime("%Y-%m-%d")
        elif isinstance(source_updated, str):
            source_date = source_updated[:10] if len(source_updated) >= 10 else source_updated

    extract = {
        "source": source,
        "source_id": source_id,
        "listing_url": doc.get("page_url") or doc.get("identifier") or "",
        "property_type": doc.get("property_type") or "",
        "source_date": source_date,
        "region": doc.get("region") or (addr.get("region") if addr else "") or "",
        "oblast_raion": doc.get("oblast_raion") or (addr.get("district") if addr else "") or "",
        "settlement_type": settlement_type or "",
        "settlement": doc.get("city") or (addr.get("settlement") if addr else "") or "",
        "city": doc.get("city") or (addr.get("settlement") if addr else "") or "",
        "city_district": doc.get("city_district") or (addr.get("city_district") if addr else "") or "",
        "street_type": street_type or "",
        "street": street or "",
        "building": (addr.get("building") if addr else "") or "",
        "floor": doc.get("floor"),
        "land_area_sqm": land_sqm,
        "price_per_ha_uah": doc.get("price_per_ha_uah"),
        "price_per_ha_usd": doc.get("price_per_ha_usd"),
        "building_area_sqm": doc.get("building_area_sqm"),
        "price_per_m2_uah": doc.get("price_per_m2_uah"),
        "price_per_m2_usd": doc.get("price_per_m2_usd"),
        "price_uah": doc.get("price_uah"),
        "price_usd": doc.get("price_usd"),
    }

    # Прибираємо None для числових полів (залишаємо 0 для порівняння)
    for k, v in list(extract.items()):
        if v is None and k not in ("region", "oblast_raion", "settlement", "city", "city_district", "street", "building", "listing_url", "property_type", "settlement_type", "street_type"):
            pass  # Залишаємо None — MongoDB не індексує їх
        elif v == "" and k in ("floor", "land_area_sqm", "price_per_ha_uah", "price_per_ha_usd", "building_area_sqm", "price_per_m2_uah", "price_per_m2_usd", "price_uah", "price_usd"):
            extract[k] = None

    return extract


def rebuild_analytics_extracts() -> int:
    """
    Повністю перезаповнює analytics_extracts з unified_listings.

    Викликати після оновлення даних з джерел (разом з price analytics).

    Returns:
        Кількість записів у analytics_extracts
    """
    unified_repo = UnifiedListingsRepository()
    extracts_repo = AnalyticsExtractsRepository()

    try:
        extracts_repo.clear_all()
        count = 0
        cursor = unified_repo.collection.find(
            {"status": "активне"},
            projection={
                "source": 1,
                "source_id": 1,
                "page_url": 1,
                "identifier": 1,
                "property_type": 1,
                "source_updated_at": 1,
                "region": 1,
                "oblast_raion": 1,
                "city": 1,
                "city_district": 1,
                "addresses": 1,
                "floor": 1,
                "land_area_sqm": 1,
                "building_area_sqm": 1,
                "price_uah": 1,
                "price_usd": 1,
                "price_per_m2_uah": 1,
                "price_per_m2_usd": 1,
                "price_per_ha_uah": 1,
                "price_per_ha_usd": 1,
            },
        )
        batch = []
        batch_size = 500
        for doc in cursor:
            extract = _extract_from_listing(doc)
            if extract:
                batch.append(extract)
                if len(batch) >= batch_size:
                    count += extracts_repo.upsert_many(batch)
                    batch = []

        if batch:
            count += extracts_repo.upsert_many(batch)

        logger.info("analytics_extracts rebuilt: %d records", count)
        return count
    except Exception as e:
        logger.exception("Помилка перезаповнення analytics_extracts: %s", e)
        raise
