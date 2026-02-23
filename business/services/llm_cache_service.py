# -*- coding: utf-8 -*-
"""
Сервіс для кешування результатів парсингу описів через LLM.
"""

from typing import Dict, Any, Optional
from utils.hash_utils import calculate_description_hash
from data.repositories.llm_cache_repository import LLMCacheRepository


class LLMCacheService:
    """Сервіс для кешування результатів парсингу описів через LLM."""
    
    def __init__(self):
        """
        Ініціалізація сервісу кешування.
        """
        self.repository = LLMCacheRepository()
    
    def get_cached_result(self, description: str) -> Optional[Dict[str, Any]]:
        """
        Отримує збережений результат парсингу для опису.
        
        Args:
            description: Текст опису аукціону
            
        Returns:
            Словник з результатами парсингу або None, якщо результат не знайдено
        """
        if not description or not description.strip():
            return None
        
        description_hash = calculate_description_hash(description)
        cached_entry = self.repository.find_by_description_hash(description_hash)
        
        if cached_entry:
            return cached_entry.get('result')
        
        return None
    
    def save_result(self, description: str, result: Dict[str, Any]) -> None:
        """
        Зберігає результат парсингу для опису.
        
        Args:
            description: Текст опису аукціону
            result: Результат парсингу (словник з полями)
        """
        if not description or not description.strip():
            return
        
        description_hash = calculate_description_hash(description)
        self.repository.save_result(description_hash, result)
    
    def clear_cache(self) -> None:
        """Очищає весь кеш."""
        self.repository.delete_many({})

    def clear_real_estate_objects_cache(self) -> int:
        """Очищає кеш результатів парсингу об'єктів нерухомого майна (ключі reo_*)."""
        return self.repository.delete_by_prefix("reo_")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Отримує статистику кешу.
        
        Returns:
            Словник зі статистикою: кількість записів тощо
        """
        entries_count = self.repository.count()
        
        return {
            'entries_count': entries_count,
            'cache_type': 'mongodb'
        }
