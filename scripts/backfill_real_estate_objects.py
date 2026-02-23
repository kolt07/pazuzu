# -*- coding: utf-8 -*-
"""
Backfill: обробка існуючих оголошень для створення об'єктів нерухомого майна (ОНМ).

Ітерує по всіх unified_listings, викликає RealEstateObjectsService.process_listing.

Запуск:
  py scripts/backfill_real_estate_objects.py
  py scripts/backfill_real_estate_objects.py --clear-llm-cache
  py scripts/backfill_real_estate_objects.py --limit 100 --batch-size 20
  py scripts/backfill_real_estate_objects.py --source prozorro  # тільки ProZorro
  py scripts/backfill_real_estate_objects.py --source olx       # тільки OLX
"""

import argparse
import sys
from pathlib import Path
from typing import Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from business.services.real_estate_objects_service import RealEstateObjectsService
from business.services.llm_cache_service import LLMCacheService


def run_backfill(
    clear_llm_cache: bool = False,
    limit: int = 0,
    batch_size: int = 50,
    source: Optional[str] = None,
    backfill_cadastral_all: bool = True,
) -> dict:
    """
    Виконує backfill ОНМ для всіх unified_listings.

    Args:
        clear_llm_cache: Очистити кеш LLM для real_estate_objects перед запуском
        limit: Максимальна кількість оголошень (0 = без обмежень)
        batch_size: Розмір батчу для логування прогресу
        source: Джерело — "olx", "prozorro" або None (обидва)

    Returns:
        Словник з статистикою: processed, errors, cleared_cache
    """
    Settings()
    MongoDBConnection.initialize(Settings())
    unified_repo = UnifiedListingsRepository()
    olx_repo = OlxListingsRepository()
    prozorro_repo = ProZorroAuctionsRepository()
    reo_service = RealEstateObjectsService()
    cache_service = LLMCacheService()

    if clear_llm_cache:
        cleared = cache_service.clear_real_estate_objects_cache()
        print(f"Очищено кеш ОНМ: {cleared} записів")
    else:
        cleared = 0

    source_label = source if source else "olx + prozorro"
    print(f"Обробка джерела: {source_label}")

    filter_query: dict = {}
    if source:
        filter_query["source"] = source
    cursor = unified_repo.collection.find(
        filter_query,
        {"source": 1, "source_id": 1},
    ).sort("system_updated_at", -1)
    if limit > 0:
        cursor = cursor.limit(limit)

    processed = 0
    errors = 0
    total_objects = 0
    cadastral_backfilled = 0

    for doc in cursor:
        source = doc.get("source")
        source_id = doc.get("source_id")
        if not source or not source_id:
            continue
        try:
            olx_doc = None
            prozorro_doc = None
            if source == "olx":
                olx_doc = olx_repo.find_by_url(source_id)
            elif source == "prozorro":
                prozorro_doc = prozorro_repo.find_by_auction_id(source_id)
            ids = reo_service.process_listing(
                source,
                source_id,
                olx_doc=olx_doc,
                prozorro_doc=prozorro_doc,
                use_cache=not clear_llm_cache,
            )
            processed += 1
            total_objects += len(ids)
            if processed % batch_size == 0:
                print(f"Оброблено: {processed}, створено/оновлено ОНМ: {total_objects}")
        except Exception as e:
            errors += 1
            print(f"Помилка {source}:{source_id[:50]}...: {e}")

    cadastral_backfilled = 0
    if backfill_cadastral_all:
        print("Перезаповнення земельних ділянок з кадастру...")
        from data.repositories.real_estate_objects_repository import RealEstateObjectsRepository
        reo_repo = RealEstateObjectsRepository()
        cursor = reo_repo.collection.find(
            {"type": "land_plot", "cadastral_info.cadastral_number": {"$exists": True, "$ne": ""}},
            {"_id": 1, "cadastral_info.cadastral_number": 1, "area_sqm": 1},
        )
        for doc in cursor:
            cad = (doc.get("cadastral_info") or {}).get("cadastral_number")
            if not cad:
                continue
            oid = str(doc["_id"])
            try:
                if reo_service._backfill_land_plot_from_cadastre(oid, cad, listing_area_sqm=doc.get("area_sqm")):
                    cadastral_backfilled += 1
            except Exception as e:
                print(f"Помилка backfill ОНМ {oid}: {e}")
        if cadastral_backfilled > 0:
            print(f"Перезаповнено з кадастру: {cadastral_backfilled} ділянок")

    print(f"Завершено. Оброблено: {processed}, помилок: {errors}, ОНМ: {total_objects}, кадастр: {cadastral_backfilled}")
    return {
        "processed": processed,
        "errors": errors,
        "total_objects": total_objects,
        "cleared_cache": cleared,
        "cadastral_backfilled": cadastral_backfilled,
    }


def main():
    parser = argparse.ArgumentParser(description="Backfill об'єктів нерухомого майна")
    parser.add_argument(
        "--clear-llm-cache",
        action="store_true",
        help="Очистити кеш LLM для ОНМ перед запуском (перезапит)",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=0,
        help="Максимальна кількість оголошень (0 = всі)",
    )
    parser.add_argument(
        "--batch-size",
        type=int,
        default=50,
        help="Розмір батчу для логування",
    )
    parser.add_argument(
        "--source",
        type=str,
        choices=["olx", "prozorro"],
        default=None,
        help="Обробляти тільки вказане джерело (olx або prozorro). За замовчуванням — обидва.",
    )
    parser.add_argument(
        "--no-cadastral-backfill",
        action="store_true",
        help="Пропустити перезаповнення всіх земельних ділянок з кадастру.",
    )
    args = parser.parse_args()
    result = run_backfill(
        clear_llm_cache=args.clear_llm_cache,
        limit=args.limit,
        batch_size=args.batch_size,
        source=args.source,
        backfill_cadastral_all=not args.no_cadastral_backfill,
    )
    sys.exit(0 if result["errors"] == 0 else 1)


if __name__ == "__main__":
    main()
