# -*- coding: utf-8 -*-
"""
Міграція 035: Колекція real_estate_objects для об'єктів нерухомого майна.

Створює колекцію з індексами для земельних ділянок, будівель та приміщень.
Єдина колекція з полем type: land_plot, building, premises.

Запуск: py scripts/migrations/035_real_estate_objects.py
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
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 035: Колекція real_estate_objects")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()

        coll = db["real_estate_objects"]

        # Індекс type для фільтрації
        coll.create_index("type")
        # Індекс cadastral_number для land_plot (унікальний в межах type=land_plot)
        coll.create_index(
            [("type", 1), ("cadastral_info.cadastral_number", 1)],
            unique=True,
            partialFilterExpression={
                "type": "land_plot",
                "cadastral_info.cadastral_number": {"$exists": True, "$gt": ""},
            },
        )
        # Індекс для пошуку будівель за адресою
        coll.create_index("address.formatted_address")
        # Індекс building_id для premises
        coll.create_index("building_id")
        # Індекс source_listing_ids для пошуку за оголошенням
        coll.create_index("source_listing_ids.source")
        coll.create_index([("source_listing_ids.source", 1), ("source_listing_ids.source_id", 1)])
        # Індекс created_at для сортування
        coll.create_index("created_at")
        coll.create_index("updated_at")

        print("Міграція 035: колекція real_estate_objects створено/перевірено.")
        return True

    except Exception as e:
        print(f"Помилка міграції 035: {e}")
        return False


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
