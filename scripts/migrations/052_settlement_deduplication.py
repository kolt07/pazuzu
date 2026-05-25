# -*- coding: utf-8 -*-
"""
Міграція 052: нормалізація населених пунктів і м'яка дедуплікація cities.

Етапи:
1. Аудит (звіт у temp/settlement_dedup_audit.json)
2. Нормалізація name/name_normalized
3. М'яке злиття дублів (merged_into + оновлення посилань)

Запуск:
  py scripts/migrations/052_settlement_deduplication.py --dry-run
  py scripts/migrations/052_settlement_deduplication.py
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


def run_migration(dry_run: bool = False, audit_only: bool = False) -> bool:
    print("=" * 60)
    print("Міграція 052: settlement deduplication")
    print(f"Режим: {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        service = SettlementDeduplicationService()

        audit_path = project_root / "temp" / "settlement_dedup_audit.json"
        report = service.run_audit(output_path=audit_path)
        print("Аудит:")
        print(f"  cities: {report['total_cities']}")
        print(f"  duplicate_groups: {report['duplicate_groups']}")
        print(f"  duplicate_records: {report['duplicate_records']}")
        print(f"  issues: {report['issues']}")
        print(f"  report: {audit_path}")

        if audit_only:
            return True

        merge_stats = service.merge_duplicates(dry_run=dry_run)
        print("\nМ'яке злиття:")
        for key, value in merge_stats.items():
            print(f"  {key}: {value}")

        norm_stats = service.normalize_all_cities(dry_run=dry_run)
        print("\nНормалізація cities:")
        for key, value in norm_stats.items():
            print(f"  {key}: {value}")

        if not dry_run:
            service.db.cities.create_index("merged_into", sparse=True)
            service.db.cities.create_index("name_normalized")
            service.db.streets.create_index("merged_into", sparse=True)

        return True
    except Exception as exc:
        print("Помилка міграції 052:", exc)
        import traceback

        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Settlement deduplication migration")
    parser.add_argument("--dry-run", action="store_true", help="Тільки показати статистику змін")
    parser.add_argument("--audit-only", action="store_true", help="Лише аудит без змін")
    args = parser.parse_args()
    sys.exit(0 if run_migration(dry_run=args.dry_run, audit_only=args.audit_only) else 1)
