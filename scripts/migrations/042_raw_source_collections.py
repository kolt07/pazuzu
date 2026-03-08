# -*- coding: utf-8 -*-
"""
Міграція 042: Колекції сирих даних джерел (raw_olx_listings, raw_prozorro_auctions).

Phase 1 pipeline: сирі дані з джерел зберігаються тут без LLM-обробки.
Поля: fetch_filters/fetch_context, approximate_region, loaded_at.

Запуск: py scripts/migrations/042_raw_source_collections.py
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
    print("=" * 60)
    print("Міграція 042: Колекції сирих даних джерел")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()

        # raw_olx_listings: url (unique), search_data, detail, fetch_filters, approximate_region, loaded_at
        raw_olx = db["raw_olx_listings"]
        raw_olx.create_index("url", unique=True)
        raw_olx.create_index("loaded_at")
        raw_olx.create_index("approximate_region")
        raw_olx.create_index([("fetch_filters.region_filter", 1)])
        print("raw_olx_listings: індекси створено.")

        # raw_prozorro_auctions: auction_id (unique), auction_data, fetch_context, approximate_region, loaded_at
        raw_prozorro = db["raw_prozorro_auctions"]
        raw_prozorro.create_index("auction_id", unique=True)
        raw_prozorro.create_index("loaded_at")
        raw_prozorro.create_index("approximate_region")
        print("raw_prozorro_auctions: індекси створено.")

        print("\nМіграція 042 завершена успішно.")
        return True
    except Exception as e:
        print(f"Помилка міграції 042: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
