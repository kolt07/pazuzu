# -*- coding: utf-8 -*-
"""
Міграція 024: Виправлення property_type для записів ProZorro в unified_listings.

Раніше тип визначався за рядками "land"/"building" у classification.id,
але CPV-коди мають формат "06100000-6", "04200000-0" тощо.
Тепер використовуємо префікси CPV: 06=земля, 04=нерухомість, 05=комплекс,
а також itemProps.itemPropsType та одиниці виміру.

Запуск: py scripts/migrations/024_prozorro_property_type_fix.py
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
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from business.services.unified_listings_service import UnifiedListingsService


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 024: Виправлення property_type для ProZorro")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)

        unified_repo = UnifiedListingsRepository()
        prozorro_repo = ProZorroAuctionsRepository()
        service = UnifiedListingsService(settings)

        cursor = unified_repo.collection.find({"source": "prozorro"})
        total = unified_repo.collection.count_documents({"source": "prozorro"})
        print(f"Знайдено записів ProZorro у unified_listings: {total}")

        updated_count = 0
        skipped_count = 0
        error_count = 0
        type_changes = {}

        for doc in cursor:
            source_id = doc.get("source_id")
            if not source_id:
                skipped_count += 1
                continue

            raw_doc = prozorro_repo.find_by_auction_id(source_id)
            if not raw_doc:
                skipped_count += 1
                continue

            old_type = doc.get("property_type") or "інше"
            new_type = service._determine_property_type(raw_doc, "prozorro")

            if old_type == new_type:
                continue

            try:
                result = unified_repo.collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"property_type": new_type}},
                )
                if result.modified_count > 0:
                    updated_count += 1
                    key = f"{old_type} -> {new_type}"
                    type_changes[key] = type_changes.get(key, 0) + 1
            except Exception as e:
                error_count += 1
                if error_count <= 5:
                    print(f"  Помилка для {source_id}: {e}")

        print(f"\nМіграція завершена:")
        print(f"  - Оновлено: {updated_count}")
        print(f"  - Пропущено (немає raw doc): {skipped_count}")
        print(f"  - Помилок: {error_count}")
        if type_changes:
            print("  Зміни типів:")
            for k, v in sorted(type_changes.items(), key=lambda x: -x[1]):
                print(f"    {k}: {v}")

        return True
    except Exception as e:
        print(f"Помилка міграції 024: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
