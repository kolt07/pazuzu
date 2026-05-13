# -*- coding: utf-8 -*-
"""
Репозиторій flx_lessons_learned — довгострокова пам'ять агента Flx.

Кожен запис містить узагальнення попереднього дослідження: що працювало, що ні,
які стратегії радити в схожих кейсах. Пошук — через MongoDB $text і опційно
through cosine на полі embedding (in-process).
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from data.repositories.base_repository import BaseRepository


class FlxLessonsRepository(BaseRepository):
    """Підсумки попередніх досліджувань для самовдосконалення Flx."""

    def __init__(self):
        super().__init__("flx_lessons_learned")

    def add_lesson(
        self,
        *,
        user_id: Optional[str],
        topic_tags: List[str],
        query_pattern: str,
        what_worked: List[str],
        what_failed: List[str],
        recommendations: List[str],
        embedding: Optional[List[float]] = None,
        related_session_id: Optional[str] = None,
    ) -> str:
        """Додає новий lesson та повертає його _id."""
        now = datetime.now(timezone.utc)
        doc = {
            "user_id": str(user_id) if user_id else None,
            "topic_tags": [str(t).strip() for t in (topic_tags or []) if str(t).strip()],
            "query_pattern": (query_pattern or "")[:2000],
            "what_worked": [str(x)[:1000] for x in (what_worked or [])][:20],
            "what_failed": [str(x)[:1000] for x in (what_failed or [])][:20],
            "recommendations": [str(x)[:1000] for x in (recommendations or [])][:20],
            "embedding": embedding,
            "related_session_id": related_session_id,
            "created_at": now,
            "updated_at": now,
        }
        return str(self.collection.insert_one(doc).inserted_id)

    def search_text(
        self,
        query: str,
        *,
        user_id: Optional[str] = None,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """Шукає схожі lessons через $text. user_id=None — шукати глобально + усі юзери.

        Якщо user_id вказано — пріоритет на власні lessons, потім глобальні (без user_id).
        """
        if not query or not query.strip():
            return []
        flt: Dict[str, Any] = {"$text": {"$search": query[:512]}}
        # Сортуємо за score
        try:
            cur = (
                self.collection.find(flt, {"score": {"$meta": "textScore"}})
                .sort([("score", {"$meta": "textScore"})])
                .limit(int(top_k))
            )
            out = []
            for doc in cur:
                doc["_id"] = str(doc["_id"])
                out.append(doc)
            return out
        except Exception:
            # Можливо, відсутній text-індекс — fallback на тегування
            return self.search_by_tags(query.split(), user_id=user_id, top_k=top_k)

    def search_by_tags(
        self,
        tags: List[str],
        *,
        user_id: Optional[str] = None,
        top_k: int = 5,
    ) -> List[Dict[str, Any]]:
        """Простий fallback пошук по topic_tags."""
        clean = [str(t).strip().lower() for t in (tags or []) if str(t).strip()]
        if not clean:
            return []
        flt: Dict[str, Any] = {"topic_tags": {"$in": clean}}
        cur = self.collection.find(flt).sort([("created_at", -1)]).limit(int(top_k))
        out: List[Dict[str, Any]] = []
        for doc in cur:
            doc["_id"] = str(doc["_id"])
            out.append(doc)
        return out

    def list_recent(self, user_id: Optional[str] = None, limit: int = 20) -> List[Dict[str, Any]]:
        flt: Dict[str, Any] = {}
        if user_id:
            flt["user_id"] = str(user_id)
        cur = self.collection.find(flt).sort([("created_at", -1)]).limit(int(limit))
        out: List[Dict[str, Any]] = []
        for doc in cur:
            doc["_id"] = str(doc["_id"])
            out.append(doc)
        return out
