# -*- coding: utf-8 -*-
"""
Аналіз використання Google Maps та інших сервісів Google: Geocoding, Places API.
Екстраполяція на місяць, прайсинг.

Запуск:
  py scripts/google_maps_usage_analysis.py
  py scripts/google_maps_usage_analysis.py --days 90
"""

import argparse
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.logs_repository import LogsRepository
from data.repositories.geocode_cache_repository import GeocodeCacheRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository


# --- Прайсинг Google Maps Platform (березень 2025) ---
# Джерело: https://developers.google.com/maps/billing-and-pricing/pricing

# Geocoding API (Essentials): 10 000 безкоштовно, далі за 1000 подій
GEOCODING_FREE_CAP = 10_000
GEOCODING_PER_1K_0_100K = 5.00  # USD
GEOCODING_PER_1K_100K_500K = 4.00
GEOCODING_PER_1K_500K_1M = 3.00
GEOCODING_PER_1K_1M_5M = 1.50
GEOCODING_PER_1K_5M_PLUS = 0.38

# Places API Nearby Search (Pro): 5 000 безкоштовно, далі за 1000 подій
PLACES_NEARBY_FREE_CAP = 5_000
PLACES_NEARBY_PER_1K_0_100K = 32.00  # USD
PLACES_NEARBY_PER_1K_100K_500K = 25.60
PLACES_NEARBY_PER_1K_500K_1M = 19.20
PLACES_NEARBY_PER_1K_1M_5M = 9.60
PLACES_NEARBY_PER_1K_5M_PLUS = 2.40

# Оцінка: на 1 geo_assessment ≈ 1 geocode + 1 search_nearby
# На 1 оголошення OLX при оновленні: 1–3 geocode (локації, адреси з LLM)
# На 1 unified_listings sync: geocode для адрес без координат
GEOCODE_PER_OLX_LISTING_EST = 1.5  # унікальні локації на оголошення
GEOCODE_PER_PROZORRO_EST = 0.5
PLACES_PER_GEO_ASSESSMENT_EST = 1.0  # 1 search_nearby на оцінку


def _cost_geocoding(events: int) -> float:
    """Вартість Geocoding API за кількістю подій (після free cap)."""
    billable = max(0, events - GEOCODING_FREE_CAP)
    if billable <= 0:
        return 0.0
    cost = 0.0
    remaining = billable
    if remaining > 0:
        tier1 = min(remaining, 90_000)  # 10_001 - 100_000
        cost += tier1 * (GEOCODING_PER_1K_0_100K / 1000)
        remaining -= tier1
    if remaining > 0:
        tier2 = min(remaining, 400_000)
        cost += tier2 * (GEOCODING_PER_1K_100K_500K / 1000)
        remaining -= tier2
    if remaining > 0:
        tier3 = min(remaining, 500_000)
        cost += tier3 * (GEOCODING_PER_1K_500K_1M / 1000)
        remaining -= tier3
    if remaining > 0:
        tier4 = min(remaining, 4_000_000)
        cost += tier4 * (GEOCODING_PER_1K_1M_5M / 1000)
        remaining -= tier4
    if remaining > 0:
        cost += remaining * (GEOCODING_PER_1K_5M_PLUS / 1000)
    return cost


def _cost_places_nearby(events: int) -> float:
    """Вартість Places API Nearby Search за кількістю подій."""
    billable = max(0, events - PLACES_NEARBY_FREE_CAP)
    if billable <= 0:
        return 0.0
    cost = 0.0
    remaining = billable
    if remaining > 0:
        tier1 = min(remaining, 95_000)
        cost += tier1 * (PLACES_NEARBY_PER_1K_0_100K / 1000)
        remaining -= tier1
    if remaining > 0:
        tier2 = min(remaining, 400_000)
        cost += tier2 * (PLACES_NEARBY_PER_1K_100K_500K / 1000)
        remaining -= tier2
    if remaining > 0:
        tier3 = min(remaining, 500_000)
        cost += tier3 * (PLACES_NEARBY_PER_1K_500K_1M / 1000)
        remaining -= tier3
    if remaining > 0:
        tier4 = min(remaining, 4_000_000)
        cost += tier4 * (PLACES_NEARBY_PER_1K_1M_5M / 1000)
        remaining -= tier4
    if remaining > 0:
        cost += remaining * (PLACES_NEARBY_PER_1K_5M_PLUS / 1000)
    return cost


def run_analysis(days: int = 60) -> dict:
    Settings()
    MongoDBConnection.initialize(Settings())

    logs_repo = LogsRepository()
    geocode_cache_repo = GeocodeCacheRepository()
    unified_repo = UnifiedListingsRepository()
    olx_repo = OlxListingsRepository()
    prozorro_repo = ProZorroAuctionsRepository()

    cutoff = datetime.now(timezone.utc) - timedelta(days=days)

    # --- 1. Geocoding: з логів api_usage ---
    geocode_by_source = logs_repo.count_api_usage_by_source(
        service="geocoding", days=days, from_cache_only=False
    )
    geocode_api_total = sum(s.get("count", 0) for s in geocode_by_source)
    geocode_cached_total = logs_repo.count_api_usage_total(
        service="geocoding", from_cache_only=True
    )
    geocode_all_total = logs_repo.count_api_usage_total(service="geocoding")

    # Geocode cache: кількість унікальних запитів (реальні виклики API)
    geocode_cache_total = geocode_cache_repo.count_total()
    geocode_cache_last_month = geocode_cache_repo.count_last_month()
    geocode_by_day = geocode_cache_repo.count_api_calls_by_day(days)

    # Якщо logs не заповнені — використовуємо geocode_cache як fallback для API calls
    if geocode_api_total == 0 and geocode_cache_last_month > 0:
        geocode_api_est = geocode_cache_last_month
    else:
        geocode_api_est = geocode_api_total

    # --- 2. Places API: немає прямого логування, оцінка ---
    # geo_assessment: 1 geocode + 1 search_nearby. Частка geocode від langchain_agent — груба оцінка.
    geocode_langchain = sum(
        s.get("count", 0) for s in geocode_by_source
        if "langchain" in str(s.get("source", "")).lower()
    )
    geocode_property_usage = sum(
        s.get("count", 0) for s in geocode_by_source
        if "property_usage" in str(s.get("source", "")).lower()
    )
    places_est_per_month = (geocode_langchain + geocode_property_usage) * 0.8

    # --- 3. Середня кількість оголошень на добу (для екстраполяції) ---
    olx_by_day = list(
        olx_repo.collection.aggregate([
            {"$match": {"updated_at": {"$gte": cutoff}}},
            {
                "$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$updated_at"}},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ])
    )
    prozorro_by_day = list(
        prozorro_repo.collection.aggregate([
            {"$match": {"last_updated": {"$gte": cutoff}}},
            {
                "$group": {
                    "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$last_updated"}},
                    "count": {"$sum": 1},
                }
            },
            {"$sort": {"_id": 1}},
        ])
    )

    olx_total = sum(r["count"] for r in olx_by_day)
    prozorro_total = sum(r["count"] for r in prozorro_by_day)
    olx_days = len(olx_by_day) or 1
    prozorro_days = len(prozorro_by_day) or 1
    olx_avg_per_day = olx_total / olx_days if olx_by_day else 0
    prozorro_avg_per_day = prozorro_total / prozorro_days if prozorro_by_day else 0

    # --- 4. Екстраполяція на місяць ---
    days_per_month = 30
    geocode_by_day_list = geocode_by_day or []
    geocode_last_30_days = sum(d.get("count", 0) for d in geocode_by_day_list[-30:])
    geocode_per_month_est = geocode_last_30_days if geocode_last_30_days > 0 else geocode_cache_last_month
    if geocode_per_month_est <= 0:
        geocode_per_month_est = (
            olx_avg_per_day * GEOCODE_PER_OLX_LISTING_EST * days_per_month
            + prozorro_avg_per_day * GEOCODE_PER_PROZORRO_EST * days_per_month
        )

    places_per_month_est = places_est_per_month * (days_per_month / days) if days > 0 else 0

    # --- 5. Вартість ---
    cost_geocode_month = _cost_geocoding(int(geocode_per_month_est))
    cost_places_month = _cost_places_nearby(int(places_per_month_est))

    return {
        "days": days,
        "geocoding": {
            "api_calls_total": geocode_api_total,
            "api_calls_cached": geocode_cached_total,
            "api_calls_all": geocode_all_total,
            "cache_entries_total": geocode_cache_total,
            "cache_new_last_month": geocode_cache_last_month,
            "by_source": geocode_by_source,
            "by_day": geocode_by_day,
        },
        "places": {
            "api_calls_est": int(places_est_per_month * (days / 30)) if days > 0 else 0,
            "note": "Оцінка з geocode для langchain/property_usage (немає прямого логування)",
        },
        "listings": {
            "olx_total": olx_total,
            "olx_avg_per_day": round(olx_avg_per_day, 1),
            "prozorro_total": prozorro_total,
            "prozorro_avg_per_day": round(prozorro_avg_per_day, 1),
        },
        "extrapolation_month": {
            "geocoding_events": round(geocode_per_month_est, 0),
            "places_events": round(places_per_month_est, 0),
        },
        "cost_usd_month": {
            "geocoding": round(cost_geocode_month, 2),
            "places_nearby": round(cost_places_month, 2),
            "total": round(cost_geocode_month + cost_places_month, 2),
        },
        "pricing_ref": {
            "geocoding_free": GEOCODING_FREE_CAP,
            "geocoding_per_1k": GEOCODING_PER_1K_0_100K,
            "places_free": PLACES_NEARBY_FREE_CAP,
            "places_per_1k": PLACES_NEARBY_PER_1K_0_100K,
        },
    }


def print_report(data: dict) -> None:
    print("=" * 70)
    print("АНАЛІЗ ВИКОРИСТАННЯ GOOGLE MAPS ТА СЕРВІСІВ GOOGLE")
    print("=" * 70)
    print(f"Період аналізу: останні {data['days']} днів")
    print()

    print("--- 1. GEOCODING API ---")
    g = data["geocoding"]
    print(f"Викликів API (реальних, без кешу): {g['api_calls_total']}")
    print(f"З кешу: {g['api_calls_cached']}")
    print(f"Усі запити (реальні + кеш): {g['api_calls_all']}")
    print(f"Записів у geocode_cache (унікальні адреси): {g['cache_entries_total']}")
    print(f"Нових у кеші за 30 днів: {g['cache_new_last_month']}")
    if g["by_source"]:
        print("По джерелах:")
        for s in g["by_source"][:10]:
            print(f"  - {s.get('source', '?')}: {s.get('count', 0)}")
    print()

    print("--- 2. PLACES API (Nearby Search) ---")
    p = data["places"]
    print(f"Оцінка викликів (за період): {p['api_calls_est']}")
    print(f"  {p['note']}")
    print()

    print("--- 3. ОГОЛОШЕННЯ (для екстраполяції) ---")
    l = data["listings"]
    print(f"OLX:      всього {l['olx_total']}, середнє на добу: {l['olx_avg_per_day']:.1f}")
    print(f"ProZorro: всього {l['prozorro_total']}, середнє на добу: {l['prozorro_avg_per_day']:.1f}")
    print()

    print("--- 4. ЕКСТРАПОЛЯЦІЯ НА МІСЯЦЬ (30 днів) ---")
    ext = data["extrapolation_month"]
    print(f"Geocoding подій: ~{ext['geocoding_events']:,.0f}")
    print(f"Places Nearby подій: ~{ext['places_events']:,.0f}")
    print()

    print("--- 5. ПРИБЛИЗНА ВАРТІСТЬ ЗА МІСЯЦЬ (USD) ---")
    ref = data["pricing_ref"]
    print(f"Geocoding: безкоштовно до {ref['geocoding_free']:,}/міс, далі ${ref['geocoding_per_1k']}/1000")
    print(f"Places Nearby: безкоштовно до {ref['places_free']:,}/міс, далі ${ref['places_per_1k']}/1000")
    print()
    c = data["cost_usd_month"]
    print(f"Geocoding:  ${c['geocoding']:.2f}")
    print(f"Places:     ${c['places_nearby']:.2f}")
    print(f"Всього:     ${c['total']:.2f}")
    print()
    print("Джерело прайсингу: Google Maps Platform, березень 2025")
    print("https://developers.google.com/maps/billing-and-pricing/pricing")
    print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Аналіз використання Google Maps")
    parser.add_argument("--days", type=int, default=60, help="Кількість днів для аналізу")
    args = parser.parse_args()
    data = run_analysis(days=args.days)
    print_report(data)
    return 0


if __name__ == "__main__":
    sys.exit(main())
