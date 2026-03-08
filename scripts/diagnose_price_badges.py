# -*- coding: utf-8 -*-
"""
Діагностика плашок ціни (price_indicator).

Перевіряє:
- кількість індикаторів у price_analytics
- структуру unified_listings (city, region, addresses)
- чому get_price_indicators_for_items не повертає індикатор для зразків
"""

import sys
from datetime import datetime, timedelta, timezone

# Додаємо корінь проекту
sys.path.insert(0, ".")

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.price_analytics_repository import (
    LISTING_TYPE_GENERAL,
    LISTING_TYPE_LAND,
    LISTING_TYPE_MIXED,
    LISTING_TYPE_REAL_ESTATE,
    PriceAnalyticsRepository,
)
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from business.services.price_analytics_service import PriceAnalyticsService


def main():
    print("=== Діагностика плашок ціни ===\n")

    MongoDBConnection.initialize(Settings())
    analytics_repo = PriceAnalyticsRepository()
    unified_repo = UnifiedListingsRepository()

    # 1. Індикатори в БД
    print("1. Індикатори в price_analytics:")
    indicators = list(analytics_repo.collection.find({"period_type": "indicator"}))
    total = len(indicators)
    print(f"   Всього: {total}")

    by_type = {}
    by_level = {"settlement": 0, "region": 0}
    regions_seen = set()
    cities_seen = set()
    for ind in indicators:
        lt = ind.get("listing_type", "?")
        by_type[lt] = by_type.get(lt, 0) + 1
        if ind.get("city"):
            by_level["settlement"] += 1
            cities_seen.add((ind.get("city"), ind.get("region", "")))
        else:
            by_level["region"] += 1
            regions_seen.add(ind.get("region", ""))

    print(f"   По типу: {by_type}")
    print(f"   По рівню: settlement={by_level['settlement']}, region={by_level['region']}")
    print(f"   Унікальних (city,region): {len(cities_seen)}, унікальних областей: {len(regions_seen)}")
    if regions_seen:
        print(f"   Приклад областей: {list(regions_seen)[:5]}")

    # 2. Зразок unified_listings (як при пошуку)
    print("\n2. Зразок unified_listings (активні, останні 60 днів):")
    cutoff = datetime.now(timezone.utc) - timedelta(days=60)
    docs = list(
        unified_repo.collection.find(
            {"status": "активне", "source_updated_at": {"$gte": cutoff}},
            limit=50,
            projection={
                "source": 1,
                "source_id": 1,
                "addresses": 1,
                "price_uah": 1,
                "price_per_m2_uah": 1,
                "price_per_ha_uah": 1,
                "building_area_sqm": 1,
                "land_area_ha": 1,
            },
        )
    )
    print(f"   Знайдено: {len(docs)} документів")

    # Нормалізуємо як при пошуку
    def norm(doc):
        region = city = None
        for addr in (doc.get("addresses") or []):
            if isinstance(addr, dict):
                if addr.get("region"):
                    region = addr["region"]
                if addr.get("settlement"):
                    city = addr["settlement"]
                if region and city:
                    break
        return {
            "source": doc.get("source"),
            "source_id": doc.get("source_id"),
            "city": city,
            "region": region,
            "addresses": doc.get("addresses", []),
            "price_uah": doc.get("price_uah"),
            "price_per_m2_uah": doc.get("price_per_m2_uah"),
            "price_per_ha_uah": doc.get("price_per_ha_uah"),
            "building_area_sqm": doc.get("building_area_sqm"),
            "land_area_ha": doc.get("land_area_ha"),
        }

    items = [norm(d) for d in docs]

    no_city = sum(1 for i in items if not i.get("city"))
    no_region = sum(1 for i in items if not i.get("region"))
    def has_metric(i):
        return (i.get("price_per_m2_uah") or 0) > 0 or (i.get("price_per_ha_uah") or 0) > 0 or (i.get("price_uah") or i.get("price") or 0) > 0

    no_metric = sum(1 for i in items if not has_metric(i))
    print(f"   Без city: {no_city}, без region: {no_region}, без метрики ціни: {no_metric}")

    # 3. Покрокова перевірка для перших 5
    print("\n3. Покрокова перевірка (перші 5 оголошень):")
    svc = PriceAnalyticsService()
    for idx, item in enumerate(items[:5]):
        print(f"\n   [{idx+1}] {item.get('source')}:{str(item.get('source_id'))[:40]}...")
        addrs = item.get("addresses") or []
        addr = addrs[0] if addrs and isinstance(addrs[0], dict) else {}
        city = item.get("city") or addr.get("settlement")
        region = item.get("region") or addr.get("region")
        print(f"       city={city!r}, region={region!r}")

        ppm2 = item.get("price_per_m2_uah")
        ppha = item.get("price_per_ha_uah")
        price = item.get("price_uah") or item.get("price")
        metric, value = None, None
        if ppm2 and ppm2 > 0:
            metric, value = "price_per_m2_uah", ppm2
        elif ppha and ppha > 0:
            metric, value = "price_per_ha_uah", ppha
        elif price and price > 0:
            metric, value = "price_uah", price
        print(f"       metric={metric}, value={value}")

        if not metric or not value:
            print("       -> Пропуск: немає метрики/значення")
            continue

        lt = svc._listing_type_from_item(item)
        print(f"       listing_type={lt}")

        # Перевірка settlement
        ind_sett = analytics_repo.get_indicator(city or "", metric, region, lt) if city else None
        ind_sett_gen = analytics_repo.get_indicator(city or "", metric, region, LISTING_TYPE_GENERAL) if city and lt != LISTING_TYPE_MIXED else None
        print(f"       settlement ind ({lt}): count={ind_sett.get('count') if ind_sett else None}")
        print(f"       settlement ind (general): count={ind_sett_gen.get('count') if ind_sett_gen else None}")

        # Перевірка region
        ind_reg = analytics_repo.get_region_indicator(region, metric, lt) if region else None
        ind_reg_gen = analytics_repo.get_region_indicator(region, metric, LISTING_TYPE_GENERAL) if region and lt != LISTING_TYPE_MIXED else None
        print(f"       region ind ({lt}): count={ind_reg.get('count') if ind_reg else None}, region_key={ind_reg.get('region') if ind_reg else None}")
        print(f"       region ind (general): count={ind_reg_gen.get('count') if ind_reg_gen else None}")

        result = svc.get_price_indicator(value, city or "", metric, region, lt)
        print(f"       -> get_price_indicator: {result!r} (indicator, source)")

    # 4. Повний прогон get_price_indicators_for_items
    print("\n4. get_price_indicators_for_items на зразку:")
    indicators_result = svc.get_price_indicators_for_items(items)
    print(f"   Вхідних: {len(items)}, отримано індикаторів: {len(indicators_result)}")
    if indicators_result:
        for k, v in list(indicators_result.items())[:3]:
            print(f"   {k[:50]}... -> {v.get('indicator')} ({v.get('source', '?')})")

    # 5. Перевірка: скільки unified_listings по типах
    print("\n5. Розподіл unified_listings по типах (зразок 500):")
    sample = list(unified_repo.collection.find({"status": "активне"}, limit=500, projection={"building_area_sqm": 1, "land_area_ha": 1}))
    land_cnt = sum(1 for d in sample if (d.get("land_area_ha") or 0) > 0 and not ((d.get("building_area_sqm") or 0) > 0))
    re_cnt = sum(1 for d in sample if (d.get("building_area_sqm") or 0) > 0 and not ((d.get("land_area_ha") or 0) > 0))
    mixed_cnt = sum(1 for d in sample if (d.get("building_area_sqm") or 0) > 0 and (d.get("land_area_ha") or 0) > 0)
    none_cnt = len(sample) - land_cnt - re_cnt - mixed_cnt
    print(f"   land: {land_cnt}, real_estate: {re_cnt}, mixed: {mixed_cnt}, без площ: {none_cnt}")

    # 6. Чи є індикатори для областей, що зустрічаються в items
    print("\n6. Відповідність областей у items та індикаторах:")
    item_regions = set()
    for i in items:
        r = i.get("region")
        if not r and i.get("addresses") and isinstance(i["addresses"][0], dict):
            r = i["addresses"][0].get("region")
        if r:
            item_regions.add(r)
    item_regions = {r for r in item_regions if r}
    for r in list(item_regions)[:5]:
        found = analytics_repo.collection.find_one(
            {"period_type": "indicator", "city": "", "region": {"$in": [r, r + " область", r.rstrip(" область")]}, "listing_type": {"$in": [LISTING_TYPE_REAL_ESTATE, LISTING_TYPE_GENERAL, LISTING_TYPE_MIXED]}}
        )
        print(f"   {r!r}: {'знайдено' if found else 'НЕ ЗНАЙДЕНО'}")

    print("\n=== Кінець діагностики ===")


if __name__ == "__main__":
    main()
