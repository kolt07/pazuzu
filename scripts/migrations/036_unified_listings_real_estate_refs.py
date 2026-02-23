# -*- coding: utf-8 -*-
"""
Міграція 036: Поле real_estate_object_refs у unified_listings.

Додає поле для посилань на об'єкти нерухомого майна.
Поле опціональне — існуючі документи залишаються без змін.

Запуск: py scripts/migrations/036_unified_listings_real_estate_refs.py
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
    print("Міграція 036: Поле real_estate_object_refs у unified_listings")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()

        # Поле real_estate_object_refs додається при оновленні через сервіс.
        # Міграція лише перевіряє, що колекція існує.
        coll = db["unified_listings"]
        if coll is None:
            print("Помилка: колекція unified_listings не знайдена.")
            return False

        # Індекс для пошуку оголошень з пов'язаними об'єктами (опціонально)
        coll.create_index("real_estate_object_refs", sparse=True)

        print("Міграція 036: поле real_estate_object_refs підготовлено.")
        return True

    except Exception as e:
        print(f"Помилка міграції 036: {e}")
        return False


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
