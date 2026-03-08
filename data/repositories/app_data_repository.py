# -*- coding: utf-8 -*-
"""
Репозиторій для роботи з колекцією app_data (дати оновлень).
"""

from typing import Optional, Dict, Any
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class AppDataRepository(BaseRepository):
    """Репозиторій для роботи з даними застосунку (дати оновлень)."""
    
    def __init__(self):
        """Ініціалізація репозиторію."""
        super().__init__('app_data')
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index('key', unique=True)
            self._indexes_created = True
        except Exception:
            pass
    
    def get_update_date(self, period_days: int) -> Optional[datetime]:
        """
        Отримує дату останнього оновлення для періоду.
        
        Args:
            period_days: Кількість днів періоду (1 або 7)
            
        Returns:
            Дата останнього оновлення або None
        """
        self._ensure_indexes()
        key = f'last_update_{period_days}d'
        doc = self.find_one({'key': key})
        if doc and 'update_date' in doc:
            return doc['update_date']
        return None
    
    def set_update_date(self, period_days: int, update_date: datetime) -> bool:
        """
        Встановлює дату останнього оновлення для періоду.
        
        Args:
            period_days: Кількість днів періоду (1 або 7)
            update_date: Дата оновлення
            
        Returns:
            True якщо успішно
        """
        self._ensure_indexes()
        key = f'last_update_{period_days}d'
        
        existing = self.find_one({'key': key})
        if existing:
            return self.update_by_id(existing['_id'], {
                '$set': {
                    'update_date': update_date,
                    'updated_at': datetime.now(timezone.utc)
                }
            })
        else:
            doc = {
                'key': key,
                'update_date': update_date,
                'created_at': datetime.now(timezone.utc),
                'updated_at': datetime.now(timezone.utc)
            }
            self.create(doc)
            return True
    
    def get_all_update_dates(self) -> Dict[str, Optional[datetime]]:
        """
        Отримує всі дати оновлень.
        
        Returns:
            Словник з датами оновлень для періодів 1d та 7d
        """
        return {
            '1d': self.get_update_date(1),
            '7d': self.get_update_date(7)
        }
