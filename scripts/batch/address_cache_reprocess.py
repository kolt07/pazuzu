# -*- coding: utf-8 -*-
"""
Переобробка адрес з кешу LLM (без повторного виклику моделі).

Запуск:
  py scripts/batch/address_cache_reprocess.py --dry-run --regions Київ,Київська
  py scripts/batch/address_cache_reprocess.py --source olx --regions Київська --limit 100
  py scripts/batch/address_cache_reprocess.py --source both --days 0
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.batch_processing.filters import BatchJobFilters
from business.services.batch_processing.address_cache_reprocess_service import (
    AddressCacheReprocessService,
)


def _parse_regions(raw: str) -> list:
    if not raw or not raw.strip():
        return []
    return [p.strip() for p in raw.split(",") if p.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Переобробка адрес з llm_cache / detail.llm без виклику LLM",
    )
    parser.add_argument("--source", choices=["olx", "prozorro", "both"], default="both")
    parser.add_argument("--regions", type=str, default="", help="Області через кому")
    parser.add_argument("--status", type=str, default="", help="активне | неактивне")
    parser.add_argument(
        "--days",
        type=int,
        default=0,
        help="Період source_updated_at у днях (0 = без обмеження)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Макс. кількість (0 = усі)")
    parser.add_argument("--dry-run", action="store_true", help="Лише відбір без змін")
    parser.add_argument("--url", type=str, default="", help="Одне OLX URL")
    parser.add_argument("--auction-id", type=str, default="", help="Один ProZorro auction_id")
    args = parser.parse_args()

    settings = Settings()
    MongoDBConnection.initialize(settings)
    service = AddressCacheReprocessService(settings)

    if args.url.strip():
        result = service.reprocess_olx(args.url.strip(), log_fn=print)
        print(result)
        return
    if args.auction_id.strip():
        result = service.reprocess_prozorro(args.auction_id.strip(), log_fn=print)
        print(result)
        return

    filters = BatchJobFilters(
        source=args.source,
        regions=_parse_regions(args.regions) or None,
        status=args.status.strip() or None,
        days=args.days if args.days and args.days > 0 else None,
        limit=args.limit if args.limit and args.limit > 0 else None,
    )
    stats = service.run(filters, progress_fn=None, log_fn=print, dry_run=args.dry_run)
    print(
        "Готово. "
        f"Відібрано: {stats['selected']}, "
        f"оновлено: {stats['updated']}, "
        f"пропущено: {stats['skipped']}, "
        f"без кешу: {stats['no_cache']}, "
        f"помилок: {stats['errors']}"
    )


if __name__ == "__main__":
    main()
