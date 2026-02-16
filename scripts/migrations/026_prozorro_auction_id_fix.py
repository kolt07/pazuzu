# -*- coding: utf-8 -*-
"""
Міграція 026: Виправлення auction_id для ProZorro — використання auctionId замість _id.

ProZorro.Sale посилання https://prozorro.sale/auction/{id} використовує auctionId
(формат LSE001-UA-20260112-18611), а НЕ _id (MongoDB ObjectId).
Раніше extract_auction_id повертав _id, тому посилання були невалідні.

Запуск: py scripts/migrations/026_prozorro_auction_id_fix.py
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
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from utils.hash_utils import extract_auction_id


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 026: Виправлення auction_id для ProZorro (auctionId замість _id)")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)

        repo = ProZorroAuctionsRepository()
        unified_repo = UnifiedListingsRepository()
        cursor = repo.collection.find({})
        total = repo.collection.count_documents({})
        print(f"Всього документів у prozorro_auctions: {total}")

        updated_count = 0
        skipped_count = 0
        error_count = 0

        for doc in cursor:
            auction_data = doc.get("auction_data") or {}
            correct_id = extract_auction_id(auction_data)
            current_id = doc.get("auction_id")

            if not correct_id or correct_id == current_id:
                skipped_count += 1
                continue

            try:
                result = repo.collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"auction_id": correct_id}},
                )
                if result.modified_count > 0:
                    updated_count += 1
                    if updated_count <= 5:
                        print(f"  {current_id} -> {correct_id}")
                    # Оновлюємо source_id у unified_listings
                    try:
                        unified_repo.collection.update_many(
                            {"source": "prozorro", "source_id": current_id},
                            {"$set": {"source_id": correct_id}},
                        )
                    except Exception:
                        pass
            except Exception as e:
                error_count += 1
                print(f"  Помилка для {doc.get('_id')}: {e}")

        print(f"\nОновлено prozorro_auctions: {updated_count}, пропущено: {skipped_count}, помилок: {error_count}")
        return error_count == 0

    except Exception as e:
        print(f"Помилка міграції: {e}")
        return False


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
