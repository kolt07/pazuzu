# -*- coding: utf-8 -*-
"""
Міграція 040: Колекція analytics_extracts для виокремлених даних аналітики.

Зберігає плоскі документи з метриками та адресними полями з unified_listings.
Заповнюється під час перерахунку аналітики (PriceAnalyticsService).
Використовується для швидких агрегацій (середня ціна за м² по району міста тощо).

Запуск: py scripts/migrations/040_analytics_extracts_collection.py
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


def run_migration() -> bool:
    """Створює колекцію analytics_extracts та індекси."""
    print("=" * 60)
    print("Міграція 040: Колекція analytics_extracts")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        coll = db["analytics_extracts"]

        # Унікальний індекс по джерелу та ID
        coll.create_index([("source", 1), ("source_id", 1)], unique=True)

        # Індекси для агрегацій та пошуку
        coll.create_index("source")
        coll.create_index("property_type")
        coll.create_index("region")
        coll.create_index("oblast_raion")
        coll.create_index("settlement")
        coll.create_index("city_district")
        coll.create_index("source_date")
        coll.create_index([("region", 1), ("city", 1)])
        coll.create_index([("region", 1), ("city", 1), ("city_district", 1)])

        # Складний індекс для запиту "район міста Києва з найвищою ціною за м²"
        coll.create_index([
            ("region", 1),
            ("city", 1),
            ("city_district", 1),
            ("price_per_m2_uah", 1),
        ])

        print("Колекція analytics_extracts створена, індекси додано.")
        return True
    except Exception as e:
        print(f"Помилка міграції 040: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
