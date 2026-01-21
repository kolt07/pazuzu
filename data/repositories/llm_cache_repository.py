# -*- coding: utf-8 -*-
"""
Репозиторій для роботи з колекцією llm_cache.
"""

from typing import Optional, Dict, Any
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class LLMCacheRepository(BaseRepository):
    """Репозиторій для роботи з кешем LLM."""
    
    def __init__(self):
        """Ініціалізація репозиторію."""
        super().__init__('llm_cache')
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index('description_hash', unique=True)
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_description_hash(self, description_hash: str) -> Optional[Dict[str, Any]]:
        """
        Знаходить кешований результат за хешем опису.
        
        Args:
            description_hash: MD5 хеш опису
            
        Returns:
            Документ з результатом або None
        """
        self._ensure_indexes()
        return self.find_one({'description_hash': description_hash})
    
    def save_result(
        self,
        description_hash: str,
        result: Dict[str, Any]
    ) -> str:
        """
        Зберігає результат парсингу.
        
        Args:
            description_hash: MD5 хеш опису
            result: Результат парсингу
            
        Returns:
            ID створеного запису
        """
        existing = self.find_by_description_hash(description_hash)
        
        document = {
            'description_hash': description_hash,
            'result': result,
            'created_at': datetime.now(timezone.utc)
        }
        
        if existing:
            # Оновлюємо існуючий
            self.update_by_id(existing['_id'], {
                '$set': {
                    'result': result,
                    'created_at': datetime.now(timezone.utc)
                }
            })
            return existing['_id']
        else:
            # Створюємо новий
            return self.create(document)
