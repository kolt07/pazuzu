# -*- coding: utf-8 -*-
"""
Міграція 025: Колекція price_analytics для зведеної аналітики цін.

Зберігає агреговані метрики (середні ціни, квартилі, нормальний розподіл)
за періодами: день, тиждень (пн-нд), місяць.
Групування: джерело, тип, область, місто.

Запуск: py scripts/migrations/025_price_analytics_collection.py
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
    """Створює колекцію price_analytics та індекси."""
    print("=" * 60)
    print("Міграція 025: Колекція price_analytics")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        coll = db["price_analytics"]

        # Індекси для швидкого пошуку
        coll.create_index([("period_type", 1), ("period_key", 1)])
        coll.create_index([("city", 1), ("metric", 1)])
        coll.create_index([("region", 1), ("city", 1)])
        coll.create_index("computed_at")

        print("Колекція price_analytics створена, індекси додано.")
        return True
    except Exception as e:
        print(f"Помилка міграції 025: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
