# -*- coding: utf-8 -*-
"""
Міграція 057: метадані НП з mista.ua (індекси cities + колекція raw_mista_settlements).

Запуск: py scripts/migrations/057_mista_settlement_metadata.py
"""

from __future__ import annotations

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
    print("Міграція 057: mista settlement metadata indexes")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()

        cities = db["cities"]
        cities.create_index("population")
        cities.create_index("area_sq_km")
        cities.create_index("search_aliases")
        cities.create_index("source")
        cities.create_index("mista_id", unique=True, sparse=True)
        print("✓ cities indexes: population, area_sq_km, search_aliases, source, mista_id")

        raw = db["raw_mista_settlements"]
        raw.create_index("mista_url", unique=True)
        raw.create_index("mista_id", sparse=True)
        raw.create_index("scrape_status")
        raw.create_index("list_page")
        print("✓ raw_mista_settlements indexes")

        print("Міграція 057 завершена успішно")
        return True
    except Exception as e:
        print(f"Помилка міграції 057: {e}")
        return False


if __name__ == "__main__":
    ok = run_migration()
    sys.exit(0 if ok else 1)
