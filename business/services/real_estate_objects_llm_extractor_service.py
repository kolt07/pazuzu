# -*- coding: utf-8 -*-
"""
LLM-екстрактор об'єктів нерухомого майна (ОНМ) з опису оголошення.

Витягує земельні ділянки, будівлі та приміщення з тексту оголошення.
Використовує окремий кеш (ключі reo_*).
"""

from typing import Any, Dict, List, Optional

from config.settings import Settings
from business.services.llm_service import LLMService
from business.services.llm_cache_service import LLMCacheService
from utils.hash_utils import calculate_description_hash


class RealEstateObjectsLLMExtractorService:
    """Сервіс, що застосовує LLM для витягування ОНМ з опису оголошення."""

    CACHE_PREFIX = "reo_"

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.llm_service = None
        try:
            self.llm_service = LLMService(self.settings)
        except Exception as e:
            print(f"[RealEstateObjectsLLM] Попередження: LLM недоступний: {e}")
        self.cache_service = LLMCacheService()

    def extract_objects(
        self,
        description: str,
        use_cache: bool = True,
    ) -> List[Dict[str, Any]]:
        """
        Витягує об'єкти нерухомого майна з опису.

        Args:
            description: Текст опису оголошення
            use_cache: Чи використовувати кеш

        Returns:
            Масив об'єктів [{type, description, area_sqm, ...}, ...]
        """
        if not description or not description.strip():
            return []
        cache_key = self.CACHE_PREFIX + calculate_description_hash(description)
        if use_cache:
            cached = self.cache_service.repository.find_by_cache_key(cache_key)
            if cached and isinstance(cached.get("result"), dict):
                objs = cached["result"].get("objects")
                if isinstance(objs, list):
                    return objs
        if self.llm_service is None:
            return []
        try:
            result = self.llm_service.parse_real_estate_objects(description)
            objects = result.get("objects") or []
            if isinstance(objects, list) and use_cache:
                self.cache_service.repository.save_result_by_key(cache_key, {"objects": objects})
            return objects
        except Exception as e:
            print(f"[RealEstateObjectsLLM] Помилка: {e}")
            return []
