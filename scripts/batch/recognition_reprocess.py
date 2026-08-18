# -*- coding: utf-8 -*-
"""
CLI: повторне розпізнавання з raw-колекцій (LLM, геокодування, unified, ОНМ).

Запуск:
  py scripts/batch/recognition_reprocess.py --days 7 --source both
  py scripts/batch/recognition_reprocess.py --source olx --regions Київська --status активне
  py scripts/batch/recognition_reprocess.py --limit 200 --dry-run
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
from business.services.batch_processing.recognition_reprocess_service import RecognitionReprocessService


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
        limit=args.limit,
        force=args.force,
    )


def main() -> None:
    parser = argparse.ArgumentParser(description="Повторне розпізнавання з raw-колекцій")
    parser.add_argument("--source", choices=["olx", "prozorro", "both"], default="both")
    parser.add_argument("--regions", type=str, default=None, help="Області через кому (approximate_region)")
    parser.add_argument("--status", choices=["активне", "неактивне"], default=None, help="Фільтр за unified status")
    parser.add_argument("--days", type=int, default=7, help="Період за loaded_at у raw (дні)")
    parser.add_argument("--date-from", type=str, default=None)
    parser.add_argument("--date-to", type=str, default=None)
    parser.add_argument("--limit", type=int, default=None)
    parser.add_argument("--force", action="store_true", help="Примусово проганяти LLM навіть за наявного кешу")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()

    settings = Settings()
    MongoDBConnection.initialize(settings)
    filters = build_filters_from_args(args)
    service = RecognitionReprocessService(settings)

    olx_urls = service.collect_olx_urls(filters) if "olx" in filters.resolved_sources() else []
    prozorro_ids = service.collect_prozorro_ids(filters) if "prozorro" in filters.resolved_sources() else []
    print(f"Кандидатів: OLX {len(olx_urls)}, ProZorro {len(prozorro_ids)}")

    if args.dry_run:
        for i, u in enumerate(olx_urls[:10], start=1):
            print(f"  OLX {i}. {u[:70]}")
        for i, aid in enumerate(prozorro_ids[:10], start=1):
            print(f"  PZ {i}. {aid}")
        return

    def log(msg: str) -> None:
        print(msg, flush=True)

    result = service.run(filters, log_fn=log)
    print(f"Результат: {result}")


if __name__ == "__main__":
    main()
