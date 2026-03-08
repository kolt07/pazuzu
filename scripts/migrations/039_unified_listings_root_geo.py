# -*- coding: utf-8 -*-
"""
Міграція 039: Додавання root geo полів (region, oblast_raion, city, city_district) в unified_listings.

Заповнює ці поля з addresses за логікою:
- Якщо всі адреси збігаються — беремо спільне значення.
- Якщо є суперечності — беремо значення з більшості адрес.
- Якщо є точна адреса (is_complete=True) — пріоритет її значенням.

Запуск: py scripts/migrations/039_unified_listings_root_geo.py
"""

import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from business.services.unified_listings_service import _compute_root_geo_from_addresses


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 039: Root geo в unified_listings")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)

        unified_repo = UnifiedListingsRepository()
        unified_repo._ensure_indexes()

        cursor = unified_repo.collection.find({})
        total = unified_repo.collection.count_documents({})
        print(f"Знайдено записів у unified_listings: {total}")

        updated_count = 0
        skipped_count = 0
        error_count = 0

        for doc in cursor:
            addresses = doc.get("addresses") or []
            if not isinstance(addresses, list):
                addresses = []

            root_geo = _compute_root_geo_from_addresses(addresses)

            try:
                update_data = {
                    "$set": {
                        "region": root_geo["region"],
                        "oblast_raion": root_geo["oblast_raion"],
                        "city": root_geo["city"],
                        "city_district": root_geo["city_district"],
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
                    print(f"  Помилка обробки {doc.get('source')}:{doc.get('source_id', '')[:50]}: {e}")

        print(f"\nМіграція завершена:")
        print(f"  - Оновлено: {updated_count}")
        print(f"  - Пропущено: {skipped_count}")
        print(f"  - Помилок: {error_count}")

        return True
    except Exception as e:
        print(f"Помилка міграції 039: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
