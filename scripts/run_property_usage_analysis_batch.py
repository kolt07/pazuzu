# -*- coding: utf-8 -*-
"""
Скрипт для попереднього аналізу використання об'єкта для всіх оголошень та аукціонів у базі.
Запуск: py scripts/run_property_usage_analysis_batch.py [--limit N] [--source olx|prozorro|all]
"""

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
from business.services.property_usage_analysis_service import PropertyUsageAnalysisService


def main():
    parser = argparse.ArgumentParser(description="Аналіз використання об'єктів для всіх оголошень")
    parser.add_argument("--limit", type=int, default=None, help="Максимум записів для обробки")
    parser.add_argument(
        "--source",
        choices=["olx", "prozorro", "all"],
        default="all",
        help="Джерело: olx, prozorro або all",
    )
    parser.add_argument("--force", action="store_true", help="Перезаписати навіть якщо аналіз вже є")
    args = parser.parse_args()

    Settings()
    MongoDBConnection.initialize(Settings())

    service = PropertyUsageAnalysisService()
    db = MongoDBConnection.get_database()

    total = 0
    ok = 0
    err = 0

    sources_filter = ["olx", "prozorro"] if args.source == "all" else [args.source]
    coll = db["unified_listings"]
    cursor = coll.find({"source": {"$in": sources_filter}})
    if args.limit:
        cursor = cursor.limit(args.limit)

    for doc in cursor:
        total += 1
        source = doc.get("source", "")
        source_id = doc.get("source_id")
        if not source or not source_id:
            err += 1
            continue
        try:
            service.get_or_create_analysis(source, source_id, force_refresh=args.force)
            ok += 1
            if total % 10 == 0:
                print(f"  Оброблено {total}, ok={ok}, err={err}", flush=True)
        except Exception as e:
            err += 1
            sid_preview = (source_id or "")[:60]
            print(f"  Помилка {source}:{sid_preview}...: {e}", flush=True)

    print(f"\nГотово. Всього: {total}, успішно: {ok}, помилок: {err}")


if __name__ == "__main__":
    main()
