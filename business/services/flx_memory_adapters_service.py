# -*- coding: utf-8 -*-
"""Memory adapter facades for Redis/PostgreSQL/Vector migration path."""

from __future__ import annotations

from typing import Any, Dict, List, Optional


class RedisSessionAdapter:
    """Best-effort short-term memory cache facade."""

    def __init__(self):
        self._fallback: Dict[str, Dict[str, Any]] = {}

    def set_snapshot(self, session_id: str, payload: Dict[str, Any]) -> None:
        self._fallback[str(session_id)] = dict(payload or {})

    def get_snapshot(self, session_id: str) -> Optional[Dict[str, Any]]:
        item = self._fallback.get(str(session_id))
        return dict(item) if item else None


class PostgresResearchAdapter:
    """Placeholder for normalized structured analytics persistence."""

    def __init__(self):
        self._rows: List[Dict[str, Any]] = []

    def store_fact(self, session_id: str, fact: Dict[str, Any]) -> None:
        self._rows.append({"session_id": str(session_id), **dict(fact or {})})


class VectorKnowledgeAdapter:
    """Placeholder for vector memory store integration (Qdrant/Weaviate)."""

    def __init__(self):
        self._docs: List[Dict[str, Any]] = []

    def upsert_document(self, *, session_id: str, text: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        self._docs.append(
            {
                "session_id": str(session_id),
                "text": str(text or "")[:8000],
                "metadata": dict(metadata or {}),
            }
        )
