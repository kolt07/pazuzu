# -*- coding: utf-8 -*-
"""
Міграція 021: Приведення ціни за м² та за га до правил.

- price_per_m2_uah, price_per_m2_usd — лише за умови наявності building_area_sqm
- price_per_ha_uah, price_per_ha_usd — лише за умови наявності land_area_ha

Для записів без відповідної площі — встановлює null.

Запуск: py scripts/migrations/021_unified_listings_price_per_area.py
"""

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from utils.price_metrics import compute_price_metrics


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 021: Приведення ціни за м² та за га")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)

        unified_repo = UnifiedListingsRepository()
        cursor = unified_repo.collection.find({})
        total = unified_repo.collection.count_documents({})
        print(f"Знайдено записів: {total}")

        updated_count = 0
        error_count = 0

        for doc in cursor:
            try:
                price_uah = doc.get("price_uah")
                building_area_sqm = doc.get("building_area_sqm")
                land_area_ha = doc.get("land_area_ha")
                currency_rate = doc.get("currency_rate")

                metrics = compute_price_metrics(
                    total_price_uah=price_uah,
                    building_area_sqm=building_area_sqm,
                    land_area_ha=land_area_ha,
                    uah_per_usd=currency_rate,
                )

                update_data = {
                    "$set": {
                        "price_per_m2_uah": metrics.get("price_per_m2_uah"),
                        "price_per_m2_usd": metrics.get("price_per_m2_usd"),
                        "price_per_ha_uah": metrics.get("price_per_ha_uah"),
                        "price_per_ha_usd": metrics.get("price_per_ha_usd"),
                    }
                }

                result = unified_repo.collection.update_one(
                    {"_id": doc["_id"]},
                    update_data,
                )
                if result.modified_count > 0:
                    updated_count += 1

            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"  Помилка: {e}")

        print(f"\nМіграція завершена:")
        print(f"  - Оновлено: {updated_count}")
        print(f"  - Помилок: {error_count}")

        return True
    except Exception as e:
        print(f"Помилка міграції 021: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
