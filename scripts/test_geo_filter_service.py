# -*- coding: utf-8 -*-
"""
Тестування GeoFilterService: вибірка оголошень за містами (Львів, Київ, Вінниця) та областями.
Пустий результат вважається провалом.
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

# Тест-кейси: (city?, region?, опис)
TEST_CASES = [
    (None, "Львівська", "область Львівська"),
    ("Львів", None, "місто Львів"),
    (None, "Київська", "область Київська"),
    ("Київ", None, "місто Київ"),
    (None, "Вінницька", "область Вінницька"),
    ("Вінниця", None, "місто Вінниця"),
    (None, "Волинській області", "Волинській області (нормалізація)"),
    (None, "Київська область", "Київська область (нормалізація)"),
]


def run_geo_filter_test(city: str | None, region: str | None) -> tuple[bool, int, str]:
    """
    Запускає тест GeoFilter через PipelineExecutor.
    Returns: (success, count, error_msg)
    """
    from domain.services.pipeline_executor import execute_pipeline
    from domain.services.geo_filter_service import GeoFilterService

    geo_dict = {}
    if city:
        geo_dict["city"] = city
    if region:
        geo_dict["region"] = region
    if not geo_dict:
        return False, 0, "Немає city/region"

    geo_filter = GeoFilterService().from_dict(geo_dict)
    if not geo_filter:
        return False, 0, "GeoFilterService.from_dict повернув None"

    steps = [
        {"type": "filter", "conditions": geo_dict},
        {"type": "limit", "count": 20},
    ]
    params = geo_dict
    result = execute_pipeline(
        steps=steps,
        collection="unified_listings",
        parameters=params,
    )
    if not result.get("success"):
        return False, 0, result.get("error", "Невідома помилка")
    count = result.get("count", 0)
    return count > 0, count, ""


def main():
    from config.settings import Settings
    from data.database.connection import MongoDBConnection

    print("=== Тестування GeoFilterService ===\n")
    settings = Settings()
    MongoDBConnection.initialize(settings)

    # Діагностика (опційно)
    try:
        total = sum(1 for _ in MongoDBConnection.get_database()["unified_listings"].find({}))
        with_addr = sum(1 for _ in MongoDBConnection.get_database()["unified_listings"].find(
            {"addresses": {"$exists": True, "$ne": []}}
        ))
        print(f"Колекція unified_listings: {total} документів, з адресами: {with_addr}\n")
    except Exception as e:
        print(f"Діагностика: {e}\n")

    passed = 0
    failed = 0
    for city, region, desc in TEST_CASES:
        ok, count, err = run_geo_filter_test(city, region)
        status = "OK" if ok else "FAIL"
        if ok:
            passed += 1
            print(f"  [{status}] {desc}: {count} результатів")
        else:
            failed += 1
            print(f"  [{status}] {desc}: 0 результатів. {err}")

    print(f"\nПідсумок: {passed} пройдено, {failed} провалено")
    if failed > 0:
        sys.exit(1)
    print("Всі тести пройдено.")


if __name__ == "__main__":
    main()
