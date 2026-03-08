# -*- coding: utf-8 -*-
"""
Короткий session state на користувача: active_collection, last_region, last_period_days.
Використовується як підказки за замовчуванням для інтерпретатора/планувальника.
"""

from typing import Dict, Any, Optional
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository

SESSION_TTL_SECONDS = 86400 * 7  # 7 днів


class SessionStateRepository(BaseRepository):
    """Один документ на user_id: user_id, state (dict), updated_at."""

    def __init__(self):
        super().__init__("session_state")

    def get(self, user_id: str) -> Dict[str, Any]:
        doc = self.collection.find_one({"user_id": user_id})
        if not doc:
            return {}
        state = doc.get("state") or {}
        if not isinstance(state, dict):
            return {}
        return state

    def set(self, user_id: str, state: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        self.collection.update_one(
            {"user_id": user_id},
            {"$set": {"state": state, "updated_at": now}},
            upsert=True,
        )

    def update_fields(self, user_id: str, **fields: Any) -> None:
        """Оновлює лише вказані поля state (active_collection, last_region, last_period_days)."""
        current = self.get(user_id)
        for k, v in fields.items():
            if v is not None:
                current[k] = v
        self.set(user_id, current)
