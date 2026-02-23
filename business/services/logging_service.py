# -*- coding: utf-8 -*-
"""
Сервіс для логування подій в колекцію logs.
"""

from typing import Optional, Dict, Any
from data.repositories.logs_repository import LogsRepository


class LoggingService:
    """Сервіс для логування подій."""
    
    # Типи подій
    EVENT_TYPE_API_EXCHANGE = 'api_exchange'
    EVENT_TYPE_USER_ACTION = 'user_action'
    EVENT_TYPE_APP_EVENT = 'app_event'
    EVENT_TYPE_API_USAGE = 'api_usage'
    
    def __init__(self):
        """Ініціалізація сервісу логування."""
        self.repository = LogsRepository()
    
    def log_api_exchange(
        self,
        message: str,
        url: Optional[str] = None,
        method: Optional[str] = None,
        status_code: Optional[int] = None,
        error: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Логує обмін з API.
        
        Args:
            message: Повідомлення про подію
            url: URL запиту
            method: HTTP метод
            status_code: Код статусу відповіді
            error: Текст помилки (якщо є)
            metadata: Додаткові метадані
            
        Returns:
            ID створеного запису
        """
        api_metadata = {
            'url': url,
            'method': method,
            'status_code': status_code
        }
        
        if metadata:
            api_metadata.update(metadata)
        
        return self.repository.create_log(
            event_type=self.EVENT_TYPE_API_EXCHANGE,
            message=message,
            initiator='api',
            metadata=api_metadata,
            error=error
        )
    
    def log_user_action(
        self,
        user_id: int,
        action: str,
        message: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> str:
        """
        Логує дію користувача через бот.
        
        Args:
            user_id: Ідентифікатор користувача
            action: Тип дії (download_file, generate_file, admin_action тощо)
            message: Повідомлення про подію
            metadata: Додаткові метадані
            error: Текст помилки (якщо є)
            
        Returns:
            ID створеного запису
        """
        user_metadata = {
            'action': action
        }
        
        if metadata:
            user_metadata.update(metadata)
        
        msg = message or f"Користувач {user_id} виконав дію: {action}"
        
        return self.repository.create_log(
            event_type=self.EVENT_TYPE_USER_ACTION,
            message=msg,
            initiator=str(user_id),
            metadata=user_metadata,
            error=error
        )
    
    def log_app_event(
        self,
        message: str,
        event_type: Optional[str] = None,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None
    ) -> str:
        """
        Логує загальну подію застосунку.
        
        Args:
            message: Повідомлення про подію
            event_type: Тип події (start, stop, error тощо)
            metadata: Додаткові метадані
            error: Текст помилки (якщо є)
            
        Returns:
            ID створеного запису
        """
        app_metadata = {}
        if event_type:
            app_metadata['event_type'] = event_type
        if metadata:
            app_metadata.update(metadata)
        
        return self.repository.create_log(
            event_type=self.EVENT_TYPE_APP_EVENT,
            message=message,
            initiator='system',
            metadata=app_metadata,
            error=error
        )

    def log_api_usage(
        self,
        service: str,
        source: str,
        from_cache: bool = False,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Логує кожен виклик зовнішнього API (LLM, Google Geocoding тощо).
        Використовується для статистики та діагностики.

        Args:
            service: 'llm' | 'geocoding'
            source: джерело виклику (напр. 'langchain_agent', 'parse_auction', 'unified_listings')
            from_cache: чи результат з кешу (для geocoding — True = не було виклику API)
            metadata: додаткові метадані (query_preview, request_id, results_count тощо)

        Returns:
            ID створеного запису
        """
        usage_metadata = {
            'service': service,
            'source': source,
            'from_cache': from_cache,
        }
        if metadata:
            usage_metadata.update(metadata)
        msg = f"{service} call from {source}" + (" (cache)" if from_cache else " (API)")
        return self.repository.create_log(
            event_type=self.EVENT_TYPE_API_USAGE,
            message=msg,
            initiator='system',
            metadata=usage_metadata,
            error=None
        )
