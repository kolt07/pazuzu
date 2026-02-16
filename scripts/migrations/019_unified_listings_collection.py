# -*- coding: utf-8 -*-
"""
Міграція 019: Створення та заповнення зведеної таблиці unified_listings.

Створює колекцію unified_listings та заповнює її даними з olx_listings та prozorro_auctions.
Дані з джерел конвертуються в уніфікований формат через UnifiedListingsService.

Запуск: py scripts/migrations/019_unified_listings_collection.py
Або через run_migrations (викликає run_migration()).
"""

import sys
from pathlib import Path
from typing import Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from business.services.unified_listings_service import UnifiedListingsService


def migrate_olx_listings(
    unified_service: UnifiedListingsService,
    limit: Optional[int] = None,
) -> int:
    """
    Мігрує всі оголошення OLX в зведену таблицю.
    
    Args:
        unified_service: Сервіс для синхронізації
        limit: Максимальна кількість записів для обробки (None = всі)
        
    Returns:
        Кількість успішно синхронізованих записів
    """
    olx_repo = OlxListingsRepository()
    docs = olx_repo.get_all_for_export(limit=limit)
    total = len(docs)
    print(f"Обробка записів olx_listings: {total}")
    
    synced_count = 0
    error_count = 0
    
    for i, doc in enumerate(docs, 1):
        url = doc.get("url")
        if not url:
            continue
        
        try:
            success = unified_service.sync_olx_listing(url)
            if success:
                synced_count += 1
            else:
                error_count += 1
                print(f"  Попередження: не вдалося синхронізувати OLX оголошення {url}")
            
            if i % 100 == 0:
                print(f"  Оброблено {i}/{total} OLX оголошень (успішно: {synced_count}, помилок: {error_count})")
        except Exception as e:
            error_count += 1
            import traceback
            error_details = traceback.format_exc()
            print(f"  Помилка обробки OLX оголошення {url}: {e}")
            if i <= 5:  # Показуємо деталі для перших 5 помилок
                print(f"    Деталі: {error_details}")
            continue
    
    print(f"OLX: синхронізовано {synced_count} з {total} записів (помилок: {error_count})")
    return synced_count


def migrate_prozorro_auctions(
    unified_service: UnifiedListingsService,
    limit: Optional[int] = None,
) -> int:
    """
    Мігрує всі аукціони ProZorro в зведену таблицю.
    
    Args:
        unified_service: Сервіс для синхронізації
        limit: Максимальна кількість записів для обробки (None = всі)
        
    Returns:
        Кількість успішно синхронізованих записів
    """
    prozorro_repo = ProZorroAuctionsRepository()
    
    # Отримуємо всі аукціони
    filter_query = {}
    docs = prozorro_repo.find_many(filter=filter_query, sort=[("last_updated", -1)], limit=limit)
    total = len(docs)
    print(f"Обробка записів prozorro_auctions: {total}")
    
    synced_count = 0
    error_count = 0
    
    for i, doc in enumerate(docs, 1):
        auction_id = doc.get("auction_id")
        if not auction_id:
            continue
        
        try:
            success = unified_service.sync_prozorro_auction(auction_id)
            if success:
                synced_count += 1
            else:
                error_count += 1
                print(f"  Попередження: не вдалося синхронізувати ProZorro аукціон {auction_id}")
            
            if i % 100 == 0:
                print(f"  Оброблено {i}/{total} ProZorro аукціонів (успішно: {synced_count}, помилок: {error_count})")
        except Exception as e:
            error_count += 1
            import traceback
            error_details = traceback.format_exc()
            print(f"  Помилка обробки ProZorro аукціону {auction_id}: {e}")
            if i <= 5:  # Показуємо деталі для перших 5 помилок
                print(f"    Деталі: {error_details}")
            continue
    
    print(f"ProZorro: синхронізовано {synced_count} з {total} записів (помилок: {error_count})")
    return synced_count


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 019: Створення та заповнення зведеної таблиці unified_listings")
    print("=" * 60)
    
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        
        # Створюємо сервіс синхронізації
        unified_service = UnifiedListingsService(settings)
        
        # Створюємо індекси
        unified_service.unified_repo._ensure_indexes()
        print("Індекси для unified_listings створено.")
        
        # Мігруємо дані
        print("\nПочаток міграції даних...")
        
        olx_count = migrate_olx_listings(unified_service)
        print()
        prozorro_count = migrate_prozorro_auctions(unified_service)
        
        total_synced = olx_count + prozorro_count
        print(f"\nМіграція завершена. Всього синхронізовано: {total_synced} записів")
        print("  - OLX: {} записів".format(olx_count))
        print("  - ProZorro: {} записів".format(prozorro_count))
        
        return True
    except Exception as e:
        print(f"Помилка міграції 019: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
