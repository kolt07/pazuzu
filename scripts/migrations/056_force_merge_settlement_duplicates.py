# -*- coding: utf-8 -*-
"""
Міграція 056: примусове злиття дублів НП у cities (Любешів / ЛЮБЕШІВ / смт Любешів)
та звіт про залишкові групи.

Запуск:
  py scripts/migrations/056_force_merge_settlement_duplicates.py --dry-run
  py scripts/migrations/056_force_merge_settlement_duplicates.py
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
from business.services.settlement_deduplication_service import SettlementDeduplicationService


def run_migration(dry_run: bool = False) -> bool:
    print("=" * 60)
    print("Міграція 056: force merge settlement duplicates in cities")
    print(f"Режим: {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        svc = SettlementDeduplicationService()

        before = svc.audit_duplicate_city_groups()
        print(f"Дублікатів до злиття: {len(before)}")
        for row in before[:20]:
            print(f"  key={row['key']!r} names={row['names']}")

        merge_stats = svc.repair_cities_by_computed_key(dry_run=dry_run)
        print("Злиття cities:", merge_stats)

        norm_stats = svc.normalize_all_cities(dry_run=dry_run)
        print("Нормалізація cities:", norm_stats)

        denorm_stats = svc.normalize_denormalized_settlements(dry_run=dry_run)
        print("Denorm поля:", denorm_stats)

        after = svc.audit_duplicate_city_groups()
        print(f"Дублікатів після: {len(after)}")
        if after:
            for row in after:
                print(f"  УВАГА: {row}")
            return dry_run

        return True
    except Exception as e:
        print("Помилка міграції 056:", e)
        import traceback

        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    sys.exit(0 if run_migration(dry_run=args.dry_run) else 1)
