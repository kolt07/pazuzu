# -*- coding: utf-8 -*-
"""
Переобробка OLX-оголошень з колекції olx_listings, у яких відсутні або порожні LLM-дані (detail.llm).

Пайплайн: читання з olx_listings → LLM-розпізнавання (Ollama) → оновлення olx_listings → sync у unified_listings.
Повторний fetch сторінки OLX не виконується (skip_activity_check), щоб не навантажувати джерело.

Запуск:
  py scripts/reprocess_olx_underprocessed.py
  py scripts/reprocess_olx_underprocessed.py --limit 50
  py scripts/reprocess_olx_underprocessed.py --dry-run
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
from data.repositories.olx_listings_repository import OlxListingsRepository
from business.services.olx_llm_extractor_service import OlxLLMExtractorService
from business.services.geocoding_service import GeocodingService
from business.services.unified_listings_service import UnifiedListingsService


def run_reprocess(limit: int = 0, dry_run: bool = False) -> dict:
    """
    Знаходить оголошення OLX без/з порожнім detail.llm, прогоняє їх через LLM та синхронізує в unified.

    Args:
        limit: Максимум URL для обробки (0 = без обмежень).
        dry_run: Тільки показати кількість та список URL, не виконувати обробку.

    Returns:
        Словник: total_found, processed, dry_run.
    """
    Settings()
    MongoDBConnection.initialize(Settings())
    settings = Settings()

    olx_repo = OlxListingsRepository()
    urls = olx_repo.find_urls_needing_llm_reprocess(limit=limit if limit > 0 else None)
    total_found = len(urls)

    if not urls:
        print("Немає оголошень OLX з відсутнім або порожнім detail.llm.")
        return {"total_found": 0, "processed": 0, "dry_run": dry_run}

    print(f"Знайдено оголошень для переобробки: {total_found}" + (f" (обмеження {limit})" if limit else ""))
    if dry_run:
        for i, u in enumerate(urls[:20], 1):
            print(f"  {i}. {u[:70]}...")
        if len(urls) > 20:
            print(f"  ... та ще {len(urls) - 20}")
        return {"total_found": total_found, "processed": 0, "dry_run": True}

    llm_extractor = OlxLLMExtractorService(settings)
    geocoding = GeocodingService(settings)
    unified_service = UnifiedListingsService(settings)
    try:
        from business.services.currency_rate_service import CurrencyRateService
        usd_rate = CurrencyRateService(settings).get_today_usd_rate(allow_fetch=True)
    except Exception:
        usd_rate = None

    def log(msg: str) -> None:
        print(msg, flush=True)

    from scripts.olx_scraper.run_update import _process_llm_pending

    processed = _process_llm_pending(
        urls,
        olx_repo,
        olx_repo,
        llm_extractor,
        geocoding,
        unified_service,
        usd_rate,
        log,
        skip_activity_check=True,
    )
    print(f"Переоброблено: {processed}/{total_found}. Оновлено olx_listings та unified_listings.")
    return {"total_found": total_found, "processed": processed, "dry_run": False}


def main() -> None:
    parser = argparse.ArgumentParser(description="Переобробка OLX з відсутнім/порожнім detail.llm (LLM → unified)")
    parser.add_argument("--limit", type=int, default=0, help="Макс. кількість оголошень (0 = всі)")
    parser.add_argument("--dry-run", action="store_true", help="Лише показати кількість та приклад URL")
    args = parser.parse_args()
    run_reprocess(limit=args.limit, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
