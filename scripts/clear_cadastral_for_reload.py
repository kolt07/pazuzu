# -*- coding: utf-8 -*-
"""
Очищення кадастрових даних для перезавантаження у новому форматі.

Видаляє всі ділянки з cadastral_parcels та скидає статус комірок у cadastral_scraper_cells
на pending, щоб скрапер перезавантажив їх з оновленими полями (наприклад, address).

Запуск: py scripts/clear_cadastral_for_reload.py
"""
import os
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
os.chdir(_PROJECT_ROOT)
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

from config.settings import Settings
from data.database.connection import MongoDBConnection


def main() -> None:
    MongoDBConnection.initialize(Settings())
    db = MongoDBConnection.get_database()

    parcels = db["cadastral_parcels"]
    cells = db["cadastral_scraper_cells"]

    parcels_count = parcels.count_documents({})
    cells_count = cells.count_documents({})

    parcels.delete_many({})
    cells.update_many(
        {},
        {
            "$set": {"status": "pending"},
            "$unset": {
                "processing_started_at": "",
                "processed_at": "",
                "error_message": "",
                "parcels_count": "",
            },
        },
    )

    print(f"Видалено ділянок: {parcels_count}")
    print(f"Скинуто комірок на pending: {cells_count}")
    print("Запустіть скрапер для перезавантаження з новим форматом (address тощо).")


if __name__ == "__main__":
    main()
