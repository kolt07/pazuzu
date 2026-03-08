# -*- coding: utf-8 -*-
"""
Діагностика заповнення analytics_extracts за містами та районами (city_district).

Виводить: кількість записів з price_per_m2_uah по містах; для Києва — розбивку по city_district
(зокрема Солом'янський). Для перевірки даних перед запитами типу «середня вартість за кв. м. у Соломянському районі».

Запуск: py scripts/diagnose_analytics_extracts_districts.py
"""

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.analytics_extracts_repository import AnalyticsExtractsRepository


def main():
    settings = Settings()
    MongoDBConnection.initialize(settings)
    repo = AnalyticsExtractsRepository()

    # Умова: є ціна за м² (для аналітики середньої вартості за м²)
    match_price_sqm = {"price_per_m2_uah": {"$exists": True, "$gt": 0}}

    print("=== analytics_extracts: кількість записів з price_per_m2_uah по містах ===\n")
    pipeline_city = [
        {"$match": match_price_sqm},
        {"$group": {"_id": {"$ifNull": ["$city", ""]}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 15},
    ]
    for doc in repo.collection.aggregate(pipeline_city):
        city = doc["_id"] or "(порожньо)"
        print(f"  {city}: {doc['count']}")

    print("\n=== Київ: розбивка по city_district (райони) ===\n")
    pipeline_kyiv = [
        {"$match": {**match_price_sqm, "city": {"$in": ["Київ", "м. Київ", "м.Київ"]}}},
        {"$group": {"_id": {"$ifNull": ["$city_district", ""]}, "count": {"$sum": 1}}},
        {"$sort": {"count": -1}},
        {"$limit": 25},
    ]
    for doc in repo.collection.aggregate(pipeline_kyiv):
        district = doc["_id"] or "(без району)"
        print(f"  {district}: {doc['count']}")

    # Перевірка саме Солом'янського (regex як у district_normalizer)
    print("\n=== Записи з city_district ~ Солом'янський ===\n")
    sol_count = repo.collection.count_documents(
        {
            **match_price_sqm,
            "city": {"$in": ["Київ", "м. Київ", "м.Київ"]},
            "city_district": {"$regex": r"^Солом'?янськ", "$options": "i"},
        }
    )
    print(f"  Записів (Солом'янський/Соломянський): {sol_count}")
    if sol_count == 0:
        print("  -> Якщо потрібні дані по Солом'янському: перезаповни analytics_extracts з unified_listings")
        print("     (rebuild) та/або запусти scripts/enrich_analytics_extracts_city_districts.py для геокодування вулиць.")

    MongoDBConnection.close()
    print("\nГотово.")


if __name__ == "__main__":
    main()
