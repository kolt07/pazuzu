# -*- coding: utf-8 -*-
"""
Скрипт побудови індексу місцезнаходження з кадастрового номера.
Запуск: py scripts/cadastral_build_location_index.py
"""

import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

os.chdir(_PROJECT_ROOT)

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.cadastral_location_index_service import CadastralLocationIndexService


def main() -> None:
    Settings()
    MongoDBConnection.initialize(Settings())
    service = CadastralLocationIndexService()

    def progress(idx: int, skip: int, err: int) -> None:
        if idx > 0 and idx % 1000 == 0:
            print(f"  Індексовано: {idx}, пропущено: {skip}, помилок: {err}", flush=True)

    print("Побудова індексу місцезнаходження з cadastral_parcels...", flush=True)
    result = service.build_index_from_parcels(batch_size=2000, progress_callback=progress)
    print(
        f"Готово. Індексовано: {result['indexed']}, пропущено: {result['skipped']}, помилок: {result['errors']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
