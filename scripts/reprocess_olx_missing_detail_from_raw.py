# -*- coding: utf-8 -*-
"""
Переобробка OLX із raw_olx_listings, де відсутні description/parameters у detail.

Пайплайн:
raw_olx_listings -> LLM -> olx_listings -> unified_listings

Запуск:
  py scripts/reprocess_olx_missing_detail_from_raw.py --limit 400
  py scripts/reprocess_olx_missing_detail_from_raw.py --dry-run
"""

import argparse
import sys
from pathlib import Path
from typing import List

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from business.services.olx_llm_extractor_service import OlxLLMExtractorService
from business.services.geocoding_service import GeocodingService
from business.services.unified_listings_service import UnifiedListingsService
from business.services.currency_rate_service import CurrencyRateService
from scripts.olx_scraper.run_update import _process_llm_pending


def _collect_candidate_urls(raw_repo: RawOlxListingsRepository, limit: int = 400) -> List[str]:
    query = {
        "$or": [
            {"detail.description": {"$exists": False}},
            {"detail.description": None},
            {"detail.parameters": {"$exists": False}},
            {"detail.parameters": None},
            {"detail.parameters": []},
        ]
    }
    cursor = raw_repo.collection.find(query, {"url": 1}).sort("loaded_at", -1).limit(max(limit, 1))
    urls: List[str] = []
    for doc in cursor:
        url = doc.get("url")
        if url:
            urls.append(url)
    return list(dict.fromkeys(urls))


def run_reprocess(limit: int = 400, dry_run: bool = False) -> dict:
    settings = Settings()
    MongoDBConnection.initialize(settings)

    raw_repo = RawOlxListingsRepository()
    main_repo = OlxListingsRepository()

    urls = _collect_candidate_urls(raw_repo, limit=limit)
    total = len(urls)
    print(f"Кандидатів з raw для LLM-дообробки: {total}")

    if dry_run or not urls:
        if urls:
            for i, u in enumerate(urls[:20], start=1):
                print(f"  {i}. {u}")
            if total > 20:
                print(f"  ... та ще {total - 20}")
        return {"total_found": total, "processed": 0, "dry_run": dry_run}

    llm_extractor = OlxLLMExtractorService(settings)
    geocoding = GeocodingService(settings)
    unified_service = UnifiedListingsService(settings)
    try:
        usd_rate = CurrencyRateService(settings).get_today_usd_rate(allow_fetch=True)
    except Exception:
        usd_rate = None

    def log(msg: str) -> None:
        print(msg, flush=True)

    processed = _process_llm_pending(
        pending_urls=urls,
        raw_repo=raw_repo,
        main_repo=main_repo,
        llm_extractor=llm_extractor,
        geocoding_service=geocoding,
        unified_service=unified_service,
        usd_rate=usd_rate,
        log_fn=log,
        skip_activity_check=True,
    )
    print(f"Переоброблено через LLM: {processed}/{total}")
    return {"total_found": total, "processed": processed, "dry_run": False}


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Переобробка OLX із raw для кейсів без description/parameters"
    )
    parser.add_argument("--limit", type=int, default=400, help="Максимум URL для обробки")
    parser.add_argument("--dry-run", action="store_true", help="Лише показати кандидатів")
    args = parser.parse_args()
    run_reprocess(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
