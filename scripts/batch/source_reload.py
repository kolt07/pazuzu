# -*- coding: utf-8 -*-
"""
CLI: перезавантаження оголошень з джерел (OLX / ProZorro).

Запуск:
  py scripts/batch/source_reload.py --days 7 --source both
  py scripts/batch/source_reload.py --days 30 --source olx --regions Київська,Львівська --load-new
  py scripts/batch/source_reload.py --status активне --limit 100 --dry-run
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.batch_processing.filters import BatchJobFilters
from business.services.batch_processing.source_reload_service import SourceReloadService


def _parse_regions(value: str | None) -> list[str] | None:
    if not value or not value.strip():
        return None
    parts = [x.strip() for x in value.split(",") if x.strip()]
    return parts or None


def _parse_dt(value: str | None) -> datetime | None:
    if not value:
        return None
    s = value.strip()
    if s.endswith("Z"):
        s = s.replace("Z", "+00:00")
    dt = datetime.fromisoformat(s)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt


def build_filters_from_args(args: argparse.Namespace) -> BatchJobFilters:
    return BatchJobFilters(
        source=args.source,
        regions=_parse_regions(args.regions),
        status=args.status,
        days=args.days,
        date_from=_parse_dt(args.date_from),
        date_to=_parse_dt(args.date_to),
        load_new=args.load_new,
        limit=args.limit,
        force=args.force,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Перезавантаження оголошень з джерел")
    parser.add_argument("--source", choices=["olx", "prozorro", "both"], default="both")
    parser.add_argument("--regions", type=str, default=None, help="Області через кому")
    parser.add_argument("--status", choices=["активне", "неактивне"], default=None)
    parser.add_argument("--days", type=int, default=7, help="Період за source_updated_at (дні)")
    parser.add_argument("--date-from", type=str, default=None, help="ISO дата від")
    parser.add_argument("--date-to", type=str, default=None, help="ISO дата до")
    parser.add_argument("--load-new", action="store_true", help="Після оновлення наявних — завантажити нові з джерел")
    parser.add_argument("--limit", type=int, default=None, help="Макс. кількість оголошень")
    parser.add_argument("--force", action="store_true", help="Оновлювати навіть без змін у джерелі")
    parser.add_argument("--dry-run", action="store_true", help="Лише показати кількість кандидатів")
    args = parser.parse_args()

    settings = Settings()
    MongoDBConnection.initialize(settings)
    filters = build_filters_from_args(args)
    service = SourceReloadService(settings)

    listings = service.collect_listings(filters)
    print(f"Відібрано оголошень: {len(listings)}")
    if args.dry_run:
        for i, doc in enumerate(listings[:20], start=1):
            print(f"  {i}. {doc.get('source')}:{(doc.get('source_id') or '')[:70]}")
        if len(listings) > 20:
            print(f"  ... та ще {len(listings) - 20}")
        return

    def log(msg: str) -> None:
        print(msg, flush=True)

    result = service.run(filters, log_fn=log)
    print(f"Результат: {result}")


if __name__ == "__main__":
    main()
