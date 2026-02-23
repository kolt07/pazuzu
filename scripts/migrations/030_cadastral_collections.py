# -*- coding: utf-8 -*-
"""
Міграція 030: Колекції cadastral_parcels та cadastral_scraper_cells для скрапера
кадастрової карти kadastrova-karta.com.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()

        # cadastral_parcels — земельні ділянки з кадастру
        parcels = db["cadastral_parcels"]
        parcels.create_index("cadastral_number", unique=True)
        parcels.create_index([("bounds", "2dsphere")])
        parcels.create_index("source_cell_id")
        parcels.create_index("source")
        parcels.create_index("fetched_at")

        # cadastral_scraper_cells — прогрес обробки сітки
        cells = db["cadastral_scraper_cells"]
        cells.create_index("cell_id", unique=True)
        cells.create_index("status")
        cells.create_index([("status", 1), ("processed_at", 1)])

        print("Міграція 030: колекції cadastral_parcels та cadastral_scraper_cells створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 030:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
