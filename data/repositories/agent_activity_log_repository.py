# -*- coding: utf-8 -*-
"""
Репозиторій для логування діяльності мультиагентної системи.

Зберігає: намір помічника (структурований запит користувача), усі дії підпорядкованих
агентів (планувальник, аналітик, інтерпретатор, безпека) у розрізі request_id.
"""

from typing import List, Dict, Any, Optional
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class AgentActivityLogRepository(BaseRepository):
    """
    Лог активності агентів: один документ на подію (намір помічника або дія під-агента).
    Поля: request_id, user_id, agent_name, step, payload, created_at.
    """

    STEP_INTENT = "intent"      # намір помічника (структурований запит)
    STEP_ACTION = "action"      # дія під-агента (вхід/вихід)
    STEP_RESPONSE = "response"  # фінальна відповідь користувачу (короткий підсумок)

    def __init__(self):
        super().__init__("agent_activity_log")

    def log(
        self,
        request_id: str,
        user_id: Optional[str],
        agent_name: str,
        step: str,
        payload: Dict[str, Any],
    ) -> str:
        """
        Додає запис у лог.

        Args:
            request_id: Унікальний ідентифікатор обробки запиту
            user_id: Ідентифікатор користувача (Telegram)
            agent_name: Ім'я агента (assistant, planner, analyst, interpreter, security)
            step: intent | action | response
            payload: Довільний словник (наприклад intent structured, tool name + args, result summary)

        Returns:
            ID створеного документа
        """
        doc = {
            "request_id": request_id,
            "user_id": user_id,
            "agent_name": agent_name,
            "step": step,
            "payload": payload,
            "created_at": datetime.now(timezone.utc),
        }
        return self.create(doc)

    def get_by_request_id(self, request_id: str) -> List[Dict[str, Any]]:
        """
        Повертає всі записи логу для одного запиту, відсортовані за часом.

        Args:
            request_id: Ідентифікатор запиту

        Returns:
            Список документів
        """
        return self.find_many(
            {"request_id": request_id},
            sort=[("created_at", 1)],
        )
