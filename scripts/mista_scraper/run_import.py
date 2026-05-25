# -*- coding: utf-8 -*-
"""
Імпорт raw_mista_settlements → cities.

Запуск:
  py scripts/mista_scraper/run_import.py
  py scripts/mista_scraper/run_import.py --dry-run --limit 20
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.mista_settlement_import_service import MistaSettlementImportService


def main() -> int:
    parser = argparse.ArgumentParser(description="Імпорт mista → cities")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    Settings()
    MongoDBConnection.initialize(Settings())
    service = MistaSettlementImportService()
    stats = service.run_import(limit=args.limit, dry_run=args.dry_run)
    print("Імпорт завершено:", stats, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
