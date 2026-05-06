# -*- coding: utf-8 -*-
"""
Репозиторій investigation_notes — записник агента Flx.

Кожна нотатка прив'язана до session_id і має послідовний номер `seq`. Нотатки
використовуються як короткостроковий контекст: перед кожним кроком агент отримує
зведення останніх N нотаток, щоб мати спадкоємний "хід думок".
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from data.repositories.base_repository import BaseRepository

ALLOWED_KINDS = {
    "thought",       # внутрішня думка перед дією
    "observation",   # результат tool call (узагальнено)
    "hypothesis",    # припущення/версія
    "decision",      # вибір стратегії
    "question",      # питання, яке агент задає собі/користувачу
    "user_answer",   # відповідь користувача на ask_user
    "tool_call",     # лог факту виклику інструмента
    "tool_error",    # помилка виконання інструмента
}


class InvestigationNotesRepository(BaseRepository):
    """Нотатки розслідувань Flx (короткостроковий записник)."""

    def __init__(self):
        super().__init__("investigation_notes")

    def append(
        self,
        *,
        session_id: str,
        kind: str,
        text: str,
        step_index: int = 0,
        tool_call_summary: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """Додає одну нотатку. Повертає створений документ."""
        if kind not in ALLOWED_KINDS:
            raise ValueError(f"Invalid note kind: {kind}")
        now = datetime.now(timezone.utc)
        seq = self._next_seq(session_id)
        doc = {
            "session_id": session_id,
            "seq": seq,
            "step_index": int(step_index or 0),
            "kind": kind,
            "text": (text or "")[:8000],
            "tool_call_summary": tool_call_summary or None,
            "created_at": now,
        }
        result = self.collection.insert_one(doc)
        doc["_id"] = str(result.inserted_id)
        return doc

    def _next_seq(self, session_id: str) -> int:
        last = self.collection.find_one(
            {"session_id": session_id},
            sort=[("seq", -1)],
            projection={"seq": 1},
        )
        return int((last or {}).get("seq") or 0) + 1

    def list_for_session(
        self,
        session_id: str,
        limit: int = 200,
        kinds: Optional[List[str]] = None,
    ) -> List[Dict[str, Any]]:
        flt: Dict[str, Any] = {"session_id": session_id}
        if kinds:
            flt["kind"] = {"$in": list(kinds)}
        cur = self.collection.find(flt).sort([("seq", 1)]).limit(int(limit))
        out = []
        for doc in cur:
            doc["_id"] = str(doc["_id"])
            out.append(doc)
        return out

    def summary(self, session_id: str, top_k: int = 12) -> str:
        """Повертає короткий текстовий summary останніх top_k нотаток для контексту LLM.

        Формат: коротка строка по нотатці з типом і обрізаним текстом.
        """
        cur = (
            self.collection.find(
                {"session_id": session_id},
                projection={"seq": 1, "kind": 1, "text": 1, "step_index": 1, "_id": 0},
            )
            .sort([("seq", -1)])
            .limit(int(top_k))
        )
        items = list(cur)
        items.reverse()
        if not items:
            return "(немає попередніх нотаток)"
        lines: List[str] = []
        for it in items:
            kind = str(it.get("kind") or "note")
            seq = int(it.get("seq") or 0)
            step = int(it.get("step_index") or 0)
            text = str(it.get("text") or "").strip()
            if len(text) > 320:
                text = text[:320].rstrip() + "…"
            lines.append(f"[#{seq} step={step} {kind}] {text}")
        return "\n".join(lines)

    def delete_for_session(self, session_id: str) -> int:
        result = self.collection.delete_many({"session_id": session_id})
        return int(result.deleted_count or 0)
