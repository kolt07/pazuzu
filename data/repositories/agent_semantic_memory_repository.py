# -*- coding: utf-8 -*-
"""Семантична пам'ять після задачі: дистильовані факти та евристики (не весь діалог)."""

from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository


class AgentSemanticMemoryRepository(BaseRepository):
    """Колекція `agent_semantic_memory`."""

    def __init__(self):
        super().__init__("agent_semantic_memory")

    def save_record(self, doc: Dict[str, Any]) -> str:
        return self.create(doc)

    def recent_for_user(self, user_id: str, limit: int = 10) -> List[Dict[str, Any]]:
        if not user_id:
            return []
        return self.find_many({"user_id": str(user_id)}, sort=[("created_at", -1)], limit=limit)
