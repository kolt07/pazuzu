# -*- coding: utf-8 -*-
"""
Міграція 055: додаткове злиття cities за обчисленим ключем + нормалізація
рядкових city/settlement у unified_listings та analytics_extracts.

Виправляє випадки, коли в UI видно кілька «Любешів»:
- Любешів (cities)
- ЛЮБЕШІВ (denormalized поля)
- смт Любешів (старий name_normalized у cities, якщо лишився)

Запуск:
  py scripts/migrations/055_repair_settlement_variants.py --dry-run
  py scripts/migrations/055_repair_settlement_variants.py
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
    print("Міграція 055: repair settlement variants (cities + denorm fields)")
    print(f"Режим: {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        svc = SettlementDeduplicationService()

        merge_stats = svc.repair_cities_by_computed_key(dry_run=dry_run)
        print("Злиття cities за computed key:", merge_stats)

        norm_stats = svc.normalize_all_cities(dry_run=dry_run)
        print("Нормалізація cities:", norm_stats)

        denorm_stats = svc.normalize_denormalized_settlements(dry_run=dry_run)
        print("Denorm поля:", denorm_stats)

        return True
    except Exception as e:
        print("Помилка міграції 055:", e)
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
