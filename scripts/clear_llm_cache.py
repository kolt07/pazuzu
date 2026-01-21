# -*- coding: utf-8 -*-
"""
Скрипт для очищення кешу LLM.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.llm_cache_service import LLMCacheService


def clear_llm_cache():
    """Очищає кеш LLM."""
    print("=" * 60)
    print("Очищення кешу LLM")
    print("=" * 60)
    
    # Ініціалізуємо налаштування та підключення до MongoDB
    settings = Settings()
    
    try:
        MongoDBConnection.initialize(settings)
        print("✓ Підключення до MongoDB успішне")
    except Exception as e:
        print(f"✗ Помилка підключення до MongoDB: {e}")
        return False
    
    # Очищаємо кеш
    try:
        cache_service = LLMCacheService()
        stats_before = cache_service.get_cache_stats()
        print(f"\nДо очищення: {stats_before['entries_count']} записів")
        
        cache_service.clear_cache()
        
        stats_after = cache_service.get_cache_stats()
        print(f"Після очищення: {stats_after['entries_count']} записів")
        
        print("\n✓ Кеш LLM успішно очищено!")
        return True
    except Exception as e:
        print(f"✗ Помилка очищення кешу: {e}")
        return False


if __name__ == '__main__':
    success = clear_llm_cache()
    exit(0 if success else 1)
