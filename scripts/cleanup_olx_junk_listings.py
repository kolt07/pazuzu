# -*- coding: utf-8 -*-
"""
Очищення сміттєвих оголошень OLX з БД.

Видаляє оголошення, що містять ключові слова сміття (кепки, помада, одяг тощо)
у заголовку або описі. НЕ видаляє за property_type="інше" — це може бути валідна
нерухомість без LLM-даних.

Запуск з кореня проекту:
  py scripts/cleanup_olx_junk_listings.py
  py scripts/cleanup_olx_junk_listings.py --dry-run  # лише показати, що буде видалено
"""

import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
elif getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository

# Ключові слова сміття (товари, не нерухомість). Уникаємо "косметик"/"одяг" — можуть бути "салон краси", "магазин одягу"
JUNK_KEYWORDS = ("кепк", "помад", "шапк", "губна помада", "бейсболк")


def main() -> None:
    parser = argparse.ArgumentParser(description="Очищення сміттєвих оголошень OLX")
    parser.add_argument("--dry-run", action="store_true", help="Лише показати, що буде видалено")
    args = parser.parse_args()
    dry_run = args.dry_run

    settings = Settings()
    MongoDBConnection.initialize(settings)
    unified_repo = UnifiedListingsRepository()
    olx_repo = OlxListingsRepository()

    # 1. Шукаємо сміття в olx_listings за ключовими словами
    olx_cursor = olx_repo.collection.find(
        {"url": {"$exists": True, "$ne": ""}},
        {"url": 1, "search_data": 1, "detail": 1},
    )
    junk_urls = []
    for olx_doc in olx_cursor:
        title = ((olx_doc.get("search_data") or {}).get("title") or "").lower()
        desc = ((olx_doc.get("detail") or {}).get("description") or "").lower()
        text = f"{title} {desc}"
        if any(kw in text for kw in JUNK_KEYWORDS):
            junk_urls.append(olx_doc.get("url", ""))

    junk_urls = list(set(u for u in junk_urls if u))

    if not junk_urls:
        print("Сміттєвих оголошень (за ключовими словами) не знайдено.")
        return

    print(f"Знайдено {len(junk_urls)} сміттєвих оголошень:")
    for url in junk_urls[:15]:
        print(f"  - {url[:80]}...")
    if len(junk_urls) > 15:
        print(f"  ... та ще {len(junk_urls) - 15}")

    if dry_run:
        print("\n[DRY-RUN] Жодних змін не внесено. Запустіть без --dry-run для видалення.")
        return

    # 2. Видаляємо з unified_listings
    deleted_unified = 0
    for url in junk_urls:
        deleted_unified += unified_repo.delete_by_source_id("olx", url)
    print(f"\nВидалено з unified_listings: {deleted_unified}")

    # 3. Видаляємо з olx_listings
    deleted_olx = 0
    for url in junk_urls:
        deleted_olx += olx_repo.delete_by_url(url)
    print(f"Видалено з olx_listings: {deleted_olx}")


if __name__ == "__main__":
    main()
