# -*- coding: utf-8 -*-
"""
Міграція 060: аудит і виправлення зліплених адрес (гомоніми НП).

- Активує каталог cities (merged_into: null більше не виключає НП)
- Узгоджує unified_listings.addresses та city з довідником (region_id, key)
- Виправляє address_refs (region + city._id)
- Denorm поля (normalize_denormalized_settlements)

Запуск:
  py scripts/migrations/060_repair_homonym_glued_addresses.py --dry-run
  py scripts/migrations/060_repair_homonym_glued_addresses.py
"""

from __future__ import annotations

import argparse
import json
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
    print("Міграція 060: repair homonym glued addresses")
    print(f"Режим: {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        svc = SettlementDeduplicationService()

        result = svc.repair_homonym_glued_listings(dry_run=dry_run)
        print(json.dumps(result, ensure_ascii=False, indent=2, default=str))
        return True
    except Exception as e:
        print("Помилка міграції 060:", e)
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
