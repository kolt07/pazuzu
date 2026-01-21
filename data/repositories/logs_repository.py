# -*- coding: utf-8 -*-
"""
Репозиторій для роботи з колекцією logs.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime
from data.repositories.base_repository import BaseRepository


class LogsRepository(BaseRepository):
    """Репозиторій для роботи з логами."""
    
    def __init__(self):
        """Ініціалізація репозиторію."""
        super().__init__('logs')
        # Створюємо індекси для швидкого пошуку (відкладено, щоб не падати якщо MongoDB не ініціалізовано)
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index('timestamp')
            self.collection.create_index('event_type')
            self.collection.create_index('initiator')
            self._indexes_created = True
        except Exception:
            # Якщо не вдалося створити індекси (наприклад, MongoDB не ініціалізовано), просто пропускаємо
            pass
    
    def create_log(
        self,
        event_type: str,
        message: str,
        initiator: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> str:
        """
        Створює запис логу.
        
        Args:
            event_type: Тип події (api_exchange, user_action, app_event)
            message: Повідомлення
            initiator: Ініціатор події (user_id, 'system', 'api' тощо)
            metadata: Додаткові метадані
            error: Текст помилки (якщо є)
            
        Returns:
            ID створеного запису
        """
        self._ensure_indexes()
        document = {
            'timestamp': datetime.utcnow(),
            'event_type': event_type,
            'message': message,
            'initiator': initiator,
            'metadata': metadata or {},
            'error': error
        }
        
        return self.create(document)
    
    def get_logs(
        self,
        event_type: Optional[str] = None,
        initiator: Optional[str] = None,
        limit: int = 100,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Отримує логи з фільтрацією.
        
        Args:
            event_type: Тип події для фільтрації
            initiator: Ініціатор для фільтрації
            limit: Максимальна кількість записів
            skip: Кількість записів для пропуску
            
        Returns:
            Список логів
        """
        filter_dict = {}
        if event_type:
            filter_dict['event_type'] = event_type
        if initiator:
            filter_dict['initiator'] = initiator
        
        return self.find_many(
            filter=filter_dict,
            sort=[('timestamp', -1)],  # Сортуємо за датою (нові спочатку)
            limit=limit,
            skip=skip
        )
