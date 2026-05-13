# -*- coding: utf-8 -*-
"""Персистентний Long Chain: кроки міркування / спостережень по request_id."""

from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository


class AgentReasoningChainRepository(BaseRepository):
    """Колекція `agent_reasoning_chain` — один документ на крок (зручно для rollback-запитів)."""

    def __init__(self):
        super().__init__("agent_reasoning_chain")

    def append_step(self, doc: Dict[str, Any]) -> str:
        return self.create(doc)

    def list_by_request(self, request_id: str, limit: int = 500) -> List[Dict[str, Any]]:
        return self.find_many({"request_id": request_id}, sort=[("created_at", 1)], limit=limit)

    def mark_compressed_before(
        self,
        request_id: str,
        before_created_at: Any,
        summary_text: str,
    ) -> int:
        """Позначає старі кроки як compressed (поле compressed_summary), best-effort."""
        coll = self.collection
        if coll is None:
            return 0
        try:
            r = coll.update_many(
                {"request_id": request_id, "created_at": {"$lt": before_created_at}, "compressed": {"$ne": True}},
                {"$set": {"compressed": True, "compressed_summary": summary_text[:2000]}},
            )
            return int(getattr(r, "modified_count", 0) or 0)
        except Exception:
            return 0

    def count_by_request(self, request_id: str) -> int:
        coll = self.collection
        if coll is None:
            return 0
        try:
            return int(coll.count_documents({"request_id": request_id}, limit=10000))
        except Exception:
            return 0
