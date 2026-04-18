# -*- coding: utf-8 -*-
"""
Очищення всіх колекцій, повʼязаних з OLX, та опційно кешу LLM.

Видаляє:
  - raw_olx_listings (усі документи)
  - olx_listings (усі документи)
  - unified_listings (документи з source="olx")
  - listing_analytics (документи з source="olx")
  - real_estate_objects: прибирає посилання на OLX з source_listing_ids;
    якщо після цього source_listing_ids порожній — документ видаляється.

Опційно (--clear-llm-cache): очищає всю колекцію llm_cache.
Кеш LLM спільний для OLX і ProZorro (ключ — хеш опису), тому окремо
видалити лише «OLX-записи» неможливо без зміни схеми.

Запуск з кореня проекту:
  py scripts/clear_olx_data.py
  py scripts/clear_olx_data.py --clear-llm-cache
  py scripts/clear_olx_data.py --dry-run
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.listing_analytics_repository import ListingAnalyticsRepository
from data.repositories.real_estate_objects_repository import RealEstateObjectsRepository


def main() -> None:
    parser = argparse.ArgumentParser(description="Очищення колекцій OLX та повʼязаних даних")
    parser.add_argument("--dry-run", action="store_true", help="Лише показати, що буде зроблено, не змінювати БД")
    parser.add_argument(
        "--clear-llm-cache",
        action="store_true",
        help="Додатково очистити колекцію llm_cache (спільна з ProZorro)",
    )
    args = parser.parse_args()
    dry_run = args.dry_run

    settings = Settings()
    MongoDBConnection.initialize(settings)

    if dry_run:
        print("[DRY-RUN] Жодних змін не буде внесено.\n")

    raw_repo = RawOlxListingsRepository()
    olx_repo = OlxListingsRepository()
    unified_repo = UnifiedListingsRepository()
    analytics_repo = ListingAnalyticsRepository()
    reo_repo = RealEstateObjectsRepository()

    # 1. raw_olx_listings
    count_raw = raw_repo.count()
    print(f"raw_olx_listings: {count_raw} документів")
    if not dry_run and count_raw > 0:
        raw_repo.delete_many({})
        print("  → очищено")

    # 2. olx_listings
    count_olx = olx_repo.count()
    print(f"olx_listings: {count_olx} документів")
    if not dry_run and count_olx > 0:
        olx_repo.delete_many({})
        print("  → очищено")

    # 3. unified_listings (source=olx)
    count_unified = unified_repo.collection.count_documents({"source": "olx"})
    print(f"unified_listings (source=olx): {count_unified} документів")
    if not dry_run and count_unified > 0:
        unified_repo.delete_many({"source": "olx"})
        print("  → видалено")

    # 4. listing_analytics (source=olx)
    count_analytics = analytics_repo.collection.count_documents({"source": "olx"})
    print(f"listing_analytics (source=olx): {count_analytics} документів")
    if not dry_run and count_analytics > 0:
        analytics_repo.delete_many({"source": "olx"})
        print("  → видалено")

    # 5. real_estate_objects: прибрати OLX з source_listing_ids, потім видалити документи з порожнім масивом
    reo_with_olx = list(
        reo_repo.collection.find(
            {"source_listing_ids": {"$elemMatch": {"source": "olx"}}},
            {"_id": 1},
        )
    )
    count_reo_olx = len(reo_with_olx)
    print(f"real_estate_objects (з посиланнями на OLX): {count_reo_olx} документів")
    if not dry_run and count_reo_olx > 0:
        reo_repo.collection.update_many(
            {"source_listing_ids": {"$elemMatch": {"source": "olx"}}},
            {"$pull": {"source_listing_ids": {"source": "olx"}}},
        )
        empty_refs = reo_repo.collection.delete_many({"source_listing_ids": []})
        print(f"  → оновлено посилання, видалено {empty_refs.deleted_count} документів з порожнім source_listing_ids")

    # 6. llm_cache (опційно)
    if args.clear_llm_cache:
        from business.services.llm_cache_service import LLMCacheService
        cache = LLMCacheService()
        stats = cache.get_cache_stats()
        count_cache = stats.get("entries_count", 0)
        print(f"llm_cache: {count_cache} записів")
        if not dry_run and count_cache > 0:
            cache.clear_cache()
            print("  → очищено (кеш спільний для OLX та ProZorro)")
    else:
        print("llm_cache: не очищається (використайте --clear-llm-cache для повного очищення)")

    if dry_run:
        print("\n[DRY-RUN] Завершено. Запустіть без --dry-run для виконання.")


if __name__ == "__main__":
    main()
