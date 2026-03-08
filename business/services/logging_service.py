# -*- coding: utf-8 -*-
"""
Сервіс для логування подій в колекцію logs.
"""

from typing import Optional, Dict, Any
from data.repositories.logs_repository import LogsRepository
from data.repositories.llm_exchange_logs_repository import LLMExchangeLogsRepository


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
        self.llm_exchange_repo = LLMExchangeLogsRepository()
    
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

    def log_llm_exchange(
        self,
        request_text: str,
        response_text: str,
        input_tokens: int,
        output_tokens: int,
        source: str,
        request_id: Optional[str] = None,
        initiator: Optional[str] = None,
        provider: Optional[str] = None,
    ) -> str:
        """
        Логує повний обмін запит–відповідь LLM у колекцію llm_exchange_logs (Gemini, Ollama тощо).

        Args:
            request_text: Повний текст запиту (промпт або серіалізовані повідомлення).
            response_text: Повний текст відповіді LLM.
            input_tokens: Кількість вхідних токенів.
            output_tokens: Кількість вихідних токенів.
            source: Джерело виклику (напр. langchain_agent_main, llm_service.parse_auction_description).
            request_id: Ідентифікатор запиту (correlation id).
            initiator: Ініціатор (user_id або 'system').
            provider: Провайдер LLM: 'gemini', 'ollama' тощо (опційно).

        Returns:
            ID створеного запису.
        """
        import logging
        log = logging.getLogger(__name__)
        try:
            doc_id = self.llm_exchange_repo.add(
                request_text=request_text or "",
                response_text=response_text or "",
                input_tokens=int(input_tokens or 0),
                output_tokens=int(output_tokens or 0),
                source=source,
                request_id=request_id,
                initiator=initiator,
                provider=provider,
            )
            log.info("LLM exchange записано: source=%s, provider=%s, request_id=%s, id=%s", source, provider, request_id, doc_id)
            return doc_id
        except Exception as e:
            log.warning("Не вдалося записати llm_exchange (source=%s): %s", source, e)
            return ""
