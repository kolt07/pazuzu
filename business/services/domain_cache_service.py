# -*- coding: utf-8 -*-
"""
DomainCacheService: інвалідація кешів домен-шару.
Викликати після оновлення даних з джерел.
"""

import logging
from typing import List, Optional

logger = logging.getLogger(__name__)


def invalidate_domain_caches(sources: Optional[List[str]] = None) -> dict:
    """
    Інвалідує кеші домен-шару після оновлення даних.
    
    - Оновлює кеш CollectionManager (довідкові значення полів, структура)
    - Очищає кеш пайплайнів (pipeline_templates)
    
    Args:
        sources: Джерела даних що оновились (prozorro, olx).
                 None — оновити всі колекції та очистити пайплайни.
    
    Returns:
        {"collection_managers_updated": int, "pipelines_cleared": int}
    """
    result = {"collection_managers_updated": 0, "pipelines_cleared": 0}
    
    try:
        from domain.managers.collection_manager import UnifiedListingsCollectionManager
        
        # Оновлюємо кеш менеджерів колекцій (unified_listings залежить від olx+prozorro)
        if sources is None or "olx" in sources or "prozorro" in sources:
            mgr = UnifiedListingsCollectionManager()
            mgr.update_cache()
            result["collection_managers_updated"] += 1
    except Exception as e:
        logger.warning("DomainCacheService: помилка оновлення CollectionManager: %s", e)
    
    try:
        from data.repositories.pipeline_repository import PipelineRepository
        
        cleared = PipelineRepository().clear_cache()
        result["pipelines_cleared"] = cleared
    except Exception as e:
        logger.warning("DomainCacheService: помилка очищення кешу пайплайнів: %s", e)
    
    logger.info("DomainCacheService: кеші інвалідовані: %s", result)
    return result
