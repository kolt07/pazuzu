# -*- coding: utf-8 -*-
"""
Заповнює cities.population / area_sq_km з raw_mista_settlements (список або parsed).

Запуск:
  py scripts/backfill_city_population_from_mista.py
  py scripts/backfill_city_population_from_mista.py --dry-run --limit 100
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.mista_settlement_import_service import MistaSettlementImportService


def main() -> int:
    parser = argparse.ArgumentParser(description="Backfill population у cities з mista raw")
    parser.add_argument("--limit", type=int, default=0)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--reimport", action="store_true", help="Повний імпорт mista→cities перед backfill")
    args = parser.parse_args()

    MongoDBConnection.initialize(Settings())
    service = MistaSettlementImportService()
    if args.reimport:
        imp = service.run_import(limit=args.limit, dry_run=args.dry_run)
        print("Імпорт:", imp, flush=True)
    stats = service.backfill_population_from_raw(limit=args.limit, dry_run=args.dry_run)
    print("Backfill population:", stats, flush=True)
    return 0


if __name__ == "__main__":
    sys.exit(main())
