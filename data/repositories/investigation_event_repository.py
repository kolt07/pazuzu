# -*- coding: utf-8 -*-
"""
Репозиторій investigation_events — capped-колекція з подіями досліджувань для SSE.

Воркер пушить події (status, note, question, done, error), а API-роут SSE
читає їх tail-cursor'ом за session_id, повертаючи кожну подію з її `seq`.
Capped-колекція гарантує впорядкованість і обмежений розмір.
"""

from typing import Any, Dict, Iterable, List, Optional
from datetime import datetime, timezone

import logging

from data.database.connection import MongoDBConnection
from data.repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)

ALLOWED_EVENT_TYPES = {
    "status",       # коротке інформаційне повідомлення про прогрес
    "note",         # нотатка агента (думка/спостереження тощо)
    "thinking",     # внутрішнє розмірковування (передається в UI як "thinking")
    "tool_call",    # факт виклику тулзи (стиснений)
    "question",     # потрібна відповідь користувача (ask_user)
    "answer",       # підтвердження прийняття відповіді користувача
    "report",       # фінальний артефакт звіту
    "done",         # дослідження завершено
    "error",        # помилка
}


class InvestigationEventRepository(BaseRepository):
    """Capped-колекція подій дослідження для SSE-каналу."""

    def __init__(self):
        super().__init__("investigation_events")

    def push(
        self,
        *,
        session_id: str,
        event_type: str,
        payload: Optional[Dict[str, Any]] = None,
    ) -> int:
        """Записує подію в capped-колекцію. Повертає seq події (монотонний за сесією)."""
        if event_type not in ALLOWED_EVENT_TYPES:
            raise ValueError(f"Invalid event_type: {event_type}")
        seq = self._next_seq(session_id)
        doc = {
            "session_id": session_id,
            "seq": seq,
            "type": event_type,
            "payload": payload or {},
            "ts": datetime.now(timezone.utc),
        }
        try:
            self.collection.insert_one(doc)
        except Exception as e:
            logger.warning("Failed to push investigation event: %s", e)
            return -1
        return seq

    def _next_seq(self, session_id: str) -> int:
        last = self.collection.find_one(
            {"session_id": session_id},
            sort=[("seq", -1)],
            projection={"seq": 1},
        )
        return int((last or {}).get("seq") or 0) + 1

    def fetch_since(
        self,
        session_id: str,
        since_seq: int = 0,
        limit: int = 200,
    ) -> List[Dict[str, Any]]:
        """Повертає події з seq > since_seq. Простий поліінг."""
        cur = (
            self.collection.find({"session_id": session_id, "seq": {"$gt": int(since_seq)}})
            .sort([("seq", 1)])
            .limit(int(limit))
        )
        out: List[Dict[str, Any]] = []
        for doc in cur:
            doc["_id"] = str(doc["_id"])
            out.append(doc)
        return out

    def tail(
        self,
        session_id: str,
        since_seq: int = 0,
        max_wait_seconds: float = 25.0,
    ) -> Iterable[Dict[str, Any]]:
        """Tailable-cursor по capped-колекції для SSE.

        Yields кожну нову подію цієї сесії. Якщо `max_wait_seconds` минуло без
        нових подій — генератор завершується (клієнт може перепідключитись).
        Терміна завершення дослідження не перевіряємо — це робить роут.
        """
        try:
            from pymongo import CursorType
        except Exception:
            CursorType = None  # type: ignore

        try:
            db = MongoDBConnection.get_database()
        except Exception:
            return

        coll = db[self.collection_name]
        flt = {"session_id": session_id, "seq": {"$gt": int(since_seq)}}
        try:
            if CursorType is not None:
                cur = coll.find(
                    flt,
                    cursor_type=CursorType.TAILABLE_AWAIT,
                    no_cursor_timeout=True,
                ).max_await_time_ms(int(max_wait_seconds * 1000))
            else:
                cur = coll.find(flt, no_cursor_timeout=True)
        except Exception as e:
            logger.warning("tail cursor failed, fallback to one-shot fetch: %s", e)
            for doc in self.fetch_since(session_id, since_seq):
                yield doc
            return

        try:
            for doc in cur:
                doc["_id"] = str(doc["_id"])
                yield doc
        except Exception as e:
            logger.debug("tail cursor closed: %s", e)
        finally:
            try:
                cur.close()
            except Exception:
                pass
