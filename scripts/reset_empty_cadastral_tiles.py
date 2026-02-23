# -*- coding: utf-8 -*-
"""
Скидає позначку опрацювання для тайлів, що були опрацьовані як порожні (0 ділянок).
Вони знову потраплять у чергу для повторної спроби.

Запуск: py scripts/reset_empty_cadastral_tiles.py
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
from data.repositories.cadastral_scraper_progress_repository import (
    CadastralScraperProgressRepository,
)


def main() -> None:
    MongoDBConnection.initialize(Settings())
    repo = CadastralScraperProgressRepository()
    count = repo.reset_empty_done_cells()
    print(f"Скинуто на pending: {count} тайлів (опрацьованих як порожні)")


if __name__ == "__main__":
    main()
