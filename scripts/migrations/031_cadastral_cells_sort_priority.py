# -*- coding: utf-8 -*-
"""
Міграція 031: Додає sort_priority до cadastral_scraper_cells для center-first порядку.
Обчислює відстань від центру України (Київ) — менше = вищий пріоритет.
"""

import math
import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.settings import Settings
from data.database.connection import MongoDBConnection

UKRAINE_CENTER_LAT = 50.45
UKRAINE_CENTER_LON = 30.52


def _distance_from_center(lat: float, lon: float) -> float:
    dlat = lat - UKRAINE_CENTER_LAT
    dlon = lon - UKRAINE_CENTER_LON
    return (dlat ** 2 + dlon ** 2) ** 0.5


def run_migration() -> bool:
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()
        cells = db["cadastral_scraper_cells"]

        updated = 0
        for doc in cells.find({"sort_priority": {"$exists": False}}):
            bbox = doc.get("bbox") or {}
            min_lat = bbox.get("min_lat")
            max_lat = bbox.get("max_lat")
            min_lon = bbox.get("min_lon")
            max_lon = bbox.get("max_lon")
            if not all(x is not None for x in (min_lat, max_lat, min_lon, max_lon)):
                continue
            center_lat = (min_lat + max_lat) / 2
            center_lon = (min_lon + max_lon) / 2
            sort_priority = round(_distance_from_center(center_lat, center_lon), 4)
            cells.update_one(
                {"_id": doc["_id"]},
                {"$set": {"sort_priority": sort_priority}},
            )
            updated += 1

        cells.create_index([("status", 1), ("sort_priority", 1), ("cell_id", 1)])
        print(f"Міграція 031: оновлено {updated} комірок, індекс створено.")
        return True
    except Exception as e:
        print("Помилка міграції 031:", e)
        return False


if __name__ == "__main__":
    sys.exit(0 if run_migration() else 1)
