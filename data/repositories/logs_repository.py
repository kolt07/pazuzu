# -*- coding: utf-8 -*-
"""
Репозиторій для роботи з колекцією logs.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timedelta
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
            self.collection.create_index([('event_type', 1), ('metadata.action', 1), ('timestamp', 1)])
            self.collection.create_index([('event_type', 1), ('metadata.service', 1), ('timestamp', 1)])
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

    def count_llm_queries_by_day(self, days: int = 60) -> List[Dict[str, Any]]:
        """
        Агрегація запитів LLM (action=llm_query) по днях за останні N днів.
        Повертає список {date: "YYYY-MM-DD", count: N}.
        """
        self._ensure_indexes()
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            pipeline = [
                {
                    "$match": {
                        "event_type": "user_action",
                        "metadata.action": "llm_query",
                        "timestamp": {"$gte": cutoff},
                    }
                },
                {
                    "$group": {
                        "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"_id": 1}},
                {"$project": {"date": "$_id", "count": 1, "_id": 0}},
            ]
            cursor = self.collection.aggregate(pipeline)
            return list(cursor)
        except Exception:
            return []

    def count_llm_queries_total(self) -> int:
        """Загальна кількість записів llm_query."""
        self._ensure_indexes()
        try:
            return self.collection.count_documents({
                "event_type": "user_action",
                "metadata.action": "llm_query",
            })
        except Exception:
            return 0

    def count_llm_queries_last_month(self) -> int:
        """Кількість llm_query за останні 30 днів."""
        self._ensure_indexes()
        try:
            cutoff = datetime.utcnow() - timedelta(days=30)
            return self.collection.count_documents({
                "event_type": "user_action",
                "metadata.action": "llm_query",
                "timestamp": {"$gte": cutoff},
            })
        except Exception:
            return 0

    def count_api_usage_by_day(
        self, service: str, days: int = 60, from_cache_only: Optional[bool] = None
    ) -> List[Dict[str, Any]]:
        """
        Агрегація викликів API (api_usage) по днях.
        service: 'llm' | 'geocoding'
        from_cache_only: None = всі, True = тільки з кешу, False = тільки реальні виклики API
        """
        self._ensure_indexes()
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            match = {
                "event_type": "api_usage",
                "metadata.service": service,
                "timestamp": {"$gte": cutoff},
            }
            if from_cache_only is not None:
                match["metadata.from_cache"] = from_cache_only
            pipeline = [
                {"$match": match},
                {
                    "$group": {
                        "_id": {"$dateToString": {"format": "%Y-%m-%d", "date": "$timestamp"}},
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"_id": 1}},
                {"$project": {"date": "$_id", "count": 1, "_id": 0}},
            ]
            cursor = self.collection.aggregate(pipeline)
            return list(cursor)
        except Exception:
            return []

    def count_api_usage_total(
        self, service: str, from_cache_only: Optional[bool] = None
    ) -> int:
        """Загальна кількість викликів api_usage для service."""
        self._ensure_indexes()
        try:
            match = {"event_type": "api_usage", "metadata.service": service}
            if from_cache_only is not None:
                match["metadata.from_cache"] = from_cache_only
            return self.collection.count_documents(match)
        except Exception:
            return 0

    def count_api_usage_last_month(
        self, service: str, from_cache_only: Optional[bool] = None
    ) -> int:
        """Кількість api_usage за останні 30 днів."""
        self._ensure_indexes()
        try:
            cutoff = datetime.utcnow() - timedelta(days=30)
            match = {
                "event_type": "api_usage",
                "metadata.service": service,
                "timestamp": {"$gte": cutoff},
            }
            if from_cache_only is not None:
                match["metadata.from_cache"] = from_cache_only
            return self.collection.count_documents(match)
        except Exception:
            return 0

    def count_api_usage_by_source(
        self,
        service: str,
        days: int = 60,
        from_cache_only: Optional[bool] = None,
    ) -> List[Dict[str, Any]]:
        """
        Агрегація викликів api_usage по source (metadata.source).
        Повертає: [{"source": "llm_service.parse_auction_description", "count": N}, ...]
        """
        self._ensure_indexes()
        try:
            cutoff = datetime.utcnow() - timedelta(days=days)
            match = {
                "event_type": "api_usage",
                "metadata.service": service,
                "timestamp": {"$gte": cutoff},
            }
            if from_cache_only is not None:
                match["metadata.from_cache"] = from_cache_only
            pipeline = [
                {"$match": match},
                {
                    "$group": {
                        "_id": {"$ifNull": ["$metadata.source", "unknown"]},
                        "count": {"$sum": 1},
                    }
                },
                {"$sort": {"count": -1}},
                {"$project": {"source": "$_id", "count": 1, "_id": 0}},
            ]
            cursor = self.collection.aggregate(pipeline)
            return list(cursor)
        except Exception:
            return []
