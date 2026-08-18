# -*- coding: utf-8 -*-
"""
Міграція 064: колекція source_load_runs (durable resume Phase 1/2) + індекси source_load_run_id на raw.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.source_load_run_repository import SourceLoadRunRepository
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
from data.repositories.raw_prozorro_auctions_repository import RawProzorroAuctionsRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        SourceLoadRunRepository()._ensure_indexes()
        RawOlxListingsRepository().ensure_index()
        RawProzorroAuctionsRepository().ensure_index()
        print("Міграція 064: source_load_runs + raw source_load_run_id індекси ok.")
        return True
    except Exception as e:
        print("Помилка міграції 064:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
