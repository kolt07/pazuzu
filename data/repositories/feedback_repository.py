# -*- coding: utf-8 -*-
"""
Репозиторій для зберігання фідбеку користувачів про відповіді LLM.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class FeedbackRepository(BaseRepository):
    """
    Репозиторій для зберігання фідбеку користувачів.
    
    Документ має структуру:
    - request_id: унікальний ідентифікатор запиту
    - user_id: ідентифікатор користувача
    - user_query: оригінальний запит користувача
    - response_text: текст відповіді LLM
    - feedback_type: "like" | "dislike"
    - diagnostic_result: результат самодіагностики (якщо feedback_type == "dislike")
    - created_at: час створення фідбеку
    """

    def __init__(self):
        super().__init__("llm_feedback")
        self._indexes_created = False

    def _ensure_indexes(self):
        """Створює необхідні індекси."""
        if self._indexes_created:
            return
        
        try:
            # Індекс для пошуку за request_id
            self.collection.create_index("request_id")
            # Індекс для пошуку за user_id та часом
            self.collection.create_index([("user_id", 1), ("created_at", -1)])
            # Індекс для пошуку дизлайків з діагностикою
            self.collection.create_index([("feedback_type", 1), ("diagnostic_result", 1)])
            self._indexes_created = True
        except Exception as e:
            import logging
            logger = logging.getLogger(__name__)
            logger.warning("Помилка створення індексів для feedback_repository: %s", e)

    def save_feedback(
        self,
        request_id: str,
        user_id: str,
        user_query: str,
        response_text: str,
        feedback_type: str,  # "like" | "dislike"
        diagnostic_result: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Зберігає фідбек користувача.
        
        Args:
            request_id: унікальний ідентифікатор запиту
            user_id: ідентифікатор користувача
            user_query: оригінальний запит користувача
            response_text: текст відповіді LLM
            feedback_type: тип фідбеку ("like" або "dislike")
            diagnostic_result: результат самодіагностики (для дизлайків)
            
        Returns:
            ID збереженого документа
        """
        self._ensure_indexes()
        
        doc = {
            "request_id": request_id,
            "user_id": user_id,
            "user_query": user_query,
            "response_text": response_text[:5000],  # Обмежуємо довжину
            "feedback_type": feedback_type,
            "diagnostic_result": diagnostic_result,
            "created_at": datetime.now(timezone.utc)
        }
        
        return self.create(doc)

    def get_feedback_by_request_id(self, request_id: str) -> Optional[Dict[str, Any]]:
        """
        Отримує фідбек за request_id.
        
        Args:
            request_id: ідентифікатор запиту
            
        Returns:
            Документ з фідбеком або None
        """
        return self.find_one({"request_id": request_id})

    def get_recent_dislikes(
        self,
        limit: int = 100,
        days: int = 7
    ) -> List[Dict[str, Any]]:
        """
        Отримує останні дизлайки для аналізу.
        
        Args:
            limit: максимальна кількість записів
            days: кількість днів назад для пошуку
            
        Returns:
            Список документів з дизлайками
        """
        from datetime import timedelta
        cutoff_date = datetime.now(timezone.utc) - timedelta(days=days)
        
        return self.find_many(
            {
                "feedback_type": "dislike",
                "created_at": {"$gte": cutoff_date}
            },
            sort=[("created_at", -1)],
            limit=limit
        )
