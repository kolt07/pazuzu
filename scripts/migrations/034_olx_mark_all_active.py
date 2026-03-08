# -*- coding: utf-8 -*-
"""
Міграція 034: Позначити всі оголошення OLX як активні.

Прибираємо логіку помітки неактивності — всі оголошення OLX вважаються активними.
- olx_listings: is_active = True
- unified_listings (source=olx): status = "активне"

Запуск: py scripts/migrations/034_olx_mark_all_active.py
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
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 034: Позначити всі оголошення OLX як активні")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)

        olx_repo = OlxListingsRepository()
        unified_repo = UnifiedListingsRepository()

        # olx_listings: is_active = True для всіх
        r1 = olx_repo.collection.update_many(
            {"is_active": {"$ne": True}},
            {"$set": {"is_active": True}},
        )
        print(f"olx_listings: оновлено {r1.modified_count} документів (is_active=True)")

        # unified_listings (source=olx): status = "активне"
        r2 = unified_repo.collection.update_many(
            {"source": "olx", "status": {"$ne": "активне"}},
            {"$set": {"status": "активне"}},
        )
        print(f"unified_listings (olx): оновлено {r2.modified_count} документів (status=активне)")

        print("Міграція 034 виконано успішно.")
        return True

    except Exception as e:
        print(f"Помилка міграції: {e}")
        return False


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
