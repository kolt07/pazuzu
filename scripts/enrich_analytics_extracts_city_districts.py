# -*- coding: utf-8 -*-
"""
Доповнення analytics_extracts районами міста (city_district) через Google Maps.

1. Вибір унікальних комбінацій Область/місто/вулиця для Києва, Одеси, Львова, Миколаєва, Харкова
2. Геокодування кожної комбінації через Google Maps
3. Оновлення записів у analytics_extracts з city_district та іншими даними

Запуск: py scripts/enrich_analytics_extracts_city_districts.py [--dry-run] [--limit N]
"""

import argparse
import sys
import time
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from typing import Any, Dict, List, Optional, Set, Tuple

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.analytics_extracts_repository import AnalyticsExtractsRepository
from business.services.geocoding_service import GeocodingService


# Міста, для яких потрібно доповнити райони (варіанти написання)
TARGET_CITIES: Dict[str, List[str]] = {
    "Київ": ["Київ", "м. Київ", "м.Київ"],
    "Одеса": ["Одеса", "м. Одеса", "м.Одеса"],
    "Львів": ["Львів", "м. Львів", "м.Львів"],
    "Миколаїв": ["Миколаїв", "м. Миколаїв", "м.Миколаїв"],
    "Харків": ["Харків", "м. Харків", "м.Харків"],
}


def get_unique_combinations(
    repo: AnalyticsExtractsRepository,
    limit: Optional[int] = None,
) -> List[Tuple[str, str, str]]:
    """
    Витягує унікальні комбінації (region, city, street) з analytics_extracts
    для цільових міст, де street не порожній.
    """
    city_variants = []
    for variants in TARGET_CITIES.values():
        city_variants.extend(variants)

    pipeline = [
        {
            "$match": {
                "city": {"$in": city_variants},
                "street": {"$exists": True, "$ne": None, "$ne": ""},
            }
        },
        {
            "$group": {
                "_id": {
                    "region": {"$ifNull": ["$region", ""]},
                    "city": "$city",
                    "street": "$street",
                },
                "count": {"$sum": 1},
            }
        },
        {"$sort": {"count": -1}},
    ]
    if limit:
        pipeline.append({"$limit": limit})

    cursor = repo.collection.aggregate(pipeline)
    combinations = []
    for doc in cursor:
        gid = doc.get("_id") or {}
        region = (gid.get("region") or "").strip()
        city = (gid.get("city") or "").strip()
        street = (gid.get("street") or "").strip()
        if region and city and street:
            combinations.append((region, city, street))
    return combinations


def _build_geocode_query(region: str, city: str, street: str) -> str:
    """Формує рядок адреси для геокодування."""
    parts = []
    if street:
        street_clean = street.strip()
        if street_clean and not any(
            street_clean.lower().startswith(p)
            for p in ("вул.", "вулиця", "пр.", "проспект", "пл.", "площа")
        ):
            street_clean = f"вул. {street_clean}"
        parts.append(street_clean)
    if city:
        parts.append(city)
    if region and region.strip():
        r = region.strip()
        if not r.endswith("область") and not r.endswith("обл."):
            r = f"{r} область"
        parts.append(r)
    parts.append("Україна")
    return ", ".join(p for p in parts if p)


def geocode_and_extract(
    geocoding_service: GeocodingService,
    region: str,
    city: str,
    street: str,
) -> Optional[Dict[str, Any]]:
    """
    Геокодує адресу та витягує city_district (sublocality) та інші поля.
    """
    query = _build_geocode_query(region, city, street)
    if not query:
        return None

    result = geocoding_service.geocode(
        query=query,
        region="ua",
        caller="enrich_analytics_extracts",
    )
    results = result.get("results") or []
    if not result.get("from_cache"):
        time.sleep(0.15)  # Rate limit: ~6 req/s для Google Geocoding
    if not results:
        return None

    first = results[0]
    addr_struct = first.get("address_structured") or {}
    city_district = addr_struct.get("sublocality")
    oblast_raion = addr_struct.get("administrative_area_level_2")

    return {
        "city_district": city_district,
        "oblast_raion": oblast_raion,
        "formatted_address": first.get("formatted_address"),
        "latitude": first.get("latitude"),
        "longitude": first.get("longitude"),
    }


def _build_match_filter(region: str, city: str, street: str) -> Dict[str, Any]:
    """Побудова фільтра для пошуку документів за комбінацією."""
    match = {"city": city, "street": street}
    if region and region.strip():
        match["region"] = region
    else:
        match["$or"] = [
            {"region": {"$exists": False}},
            {"region": None},
            {"region": ""},
        ]
    return match


def update_extracts_by_combination(
    repo: AnalyticsExtractsRepository,
    region: str,
    city: str,
    street: str,
    enriched: Dict[str, Any],
    dry_run: bool = False,
) -> int:
    """
    Оновлює всі записи analytics_extracts з даною комбінацією (region, city, street).
    """
    update_doc = {}
    if enriched.get("city_district"):
        update_doc["city_district"] = enriched["city_district"]
    if enriched.get("oblast_raion"):
        update_doc["oblast_raion"] = enriched["oblast_raion"]
    if enriched.get("formatted_address"):
        update_doc["geocode_formatted_address"] = enriched["formatted_address"]
    if enriched.get("latitude") is not None:
        update_doc["geocode_latitude"] = enriched["latitude"]
    if enriched.get("longitude") is not None:
        update_doc["geocode_longitude"] = enriched["longitude"]

    if not update_doc:
        return 0

    match_filter = _build_match_filter(region, city, street)
    if dry_run:
        return repo.collection.count_documents(match_filter)

    result = repo.collection.update_many(match_filter, {"$set": update_doc})
    return result.modified_count


def run(dry_run: bool = False, limit: Optional[int] = None) -> Dict[str, Any]:
    """Головна функція обробки."""
    settings = Settings()
    MongoDBConnection.initialize(settings)

    repo = AnalyticsExtractsRepository()
    geocoding_service = GeocodingService(settings)

    repo._ensure_indexes()

    combinations = get_unique_combinations(repo, limit=limit)
    print(f"Знайдено унікальних комбінацій (область/місто/вулиця): {len(combinations)}")

    stats = {
        "total_combinations": len(combinations),
        "geocoded": 0,
        "updated_records": 0,
        "skipped_no_result": 0,
        "errors": 0,
    }

    for i, (region, city, street) in enumerate(combinations, 1):
        try:
            enriched = geocode_and_extract(geocoding_service, region, city, street)
            if not enriched:
                stats["skipped_no_result"] += 1
                if i <= 5:
                    print(f"  [{i}] Пропущено (немає результату): {region}, {city}, {street[:40]}...")
                continue

            stats["geocoded"] += 1
            updated = update_extracts_by_combination(
                repo, region, city, street, enriched, dry_run=dry_run
            )
            stats["updated_records"] += updated

            if i <= 20 or enriched.get("city_district"):
                cd = enriched.get("city_district") or "(немає)"
                print(f"  [{i}] {city}, {street[:35]}... → city_district={cd} (оновлено {updated})")

        except Exception as e:
            stats["errors"] += 1
            print(f"  [{i}] Помилка {region}/{city}/{street[:30]}: {e}")

    return stats


def main():
    parser = argparse.ArgumentParser(
        description="Доповнення analytics_extracts районами міста через Google Maps"
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Не оновлювати БД, лише показати що буде зроблено",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=None,
        help="Обмежити кількість унікальних комбінацій для обробки",
    )
    args = parser.parse_args()

    print("=" * 60)
    print("Доповнення analytics_extracts районами міста (city_district)")
    print("Міста: Київ, Одеса, Львів, Миколаїв, Харків")
    if args.dry_run:
        print("Режим: DRY-RUN (без оновлення БД)")
    if args.limit:
        print(f"Ліміт комбінацій: {args.limit}")
    print("=" * 60)

    try:
        stats = run(dry_run=args.dry_run, limit=args.limit)
        print("\n" + "=" * 60)
        print("Підсумок:")
        print(f"  Комбінацій: {stats['total_combinations']}")
        print(f"  Геокодовано: {stats['geocoded']}")
        print(f"  Оновлено записів: {stats['updated_records']}")
        print(f"  Пропущено (немає результату): {stats['skipped_no_result']}")
        print(f"  Помилок: {stats['errors']}")
        print("=" * 60)
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    main()
