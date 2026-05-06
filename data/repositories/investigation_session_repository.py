# -*- coding: utf-8 -*-
"""
Репозиторій investigation_sessions для агента-інвестігейтора Flx.

Зберігає стан розслідування: початковий запит, план, поточний крок, очікування відповіді
користувача, посилання на артефакт фінального звіту.
"""

from typing import Any, Dict, List, Optional
from datetime import datetime, timezone

from data.repositories.base_repository import BaseRepository


# Дозволені стани сесії розслідування.
# created — щойно створено, очікує запуск воркера.
# planning — будується початковий план (LLM Planner).
# running — виконує крок (tool call або roundtrip step).
# awaiting_user — очікує відповідь користувача (ask_user).
# awaiting_sources — очікує завершення таргетного оновлення/пошуку в джерелах.
# done — фінальний звіт сформовано.
# failed — помилка (детально у last_error).
# cancelled — скасовано користувачем.
ALLOWED_STATES = {
    "created",
    "planning",
    "running",
    "awaiting_user",
    "awaiting_sources",
    "done",
    "failed",
    "cancelled",
}


class InvestigationSessionRepository(BaseRepository):
    """Сесії розслідувань Flx."""

    def __init__(self):
        super().__init__("investigation_sessions")

    def create_session(
        self,
        *,
        session_id: str,
        user_id: str,
        chat_id: Optional[str],
        query: str,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Створює нову сесію у стані `created`."""
        now = datetime.now(timezone.utc)
        doc = {
            "session_id": session_id,
            "user_id": str(user_id),
            "chat_id": chat_id,
            "request_id": request_id,
            "query": query[:8000],
            "state": "created",
            "plan": None,
            "step_index": 0,
            "iteration": 0,
            "pending_question": None,
            "pending_answer": None,
            "report_artifact_id": None,
            "report_artifact_token": None,
            "last_error": None,
            "created_at": now,
            "updated_at": now,
        }
        self.collection.insert_one(doc)
        doc["_id"] = str(doc["_id"]) if "_id" in doc else None
        return doc

    def get_by_session_id(self, session_id: str) -> Optional[Dict[str, Any]]:
        doc = self.collection.find_one({"session_id": session_id})
        if not doc:
            return None
        doc["_id"] = str(doc["_id"])
        return doc

    def list_for_user(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        cur = (
            self.collection.find({"user_id": str(user_id)})
            .sort([("updated_at", -1)])
            .limit(int(limit))
        )
        out = []
        for doc in cur:
            doc["_id"] = str(doc["_id"])
            out.append(doc)
        return out

    def update_fields(self, session_id: str, updates: Dict[str, Any]) -> bool:
        """Оновлює довільні поля + автоматично updates['updated_at']."""
        if not updates:
            return False
        if "state" in updates and updates["state"] not in ALLOWED_STATES:
            raise ValueError(f"Invalid state: {updates['state']}")
        updates = dict(updates)
        updates["updated_at"] = datetime.now(timezone.utc)
        result = self.collection.update_one(
            {"session_id": session_id},
            {"$set": updates},
        )
        return result.matched_count > 0

    def set_state(self, session_id: str, state: str, **extra: Any) -> bool:
        """Перевід стану + опційні додаткові поля."""
        if state not in ALLOWED_STATES:
            raise ValueError(f"Invalid state: {state}")
        payload: Dict[str, Any] = {"state": state}
        payload.update(extra)
        return self.update_fields(session_id, payload)

    def set_plan(self, session_id: str, plan: Dict[str, Any]) -> bool:
        return self.update_fields(session_id, {"plan": plan, "step_index": 0})

    def increment_iteration(self, session_id: str) -> int:
        result = self.collection.find_one_and_update(
            {"session_id": session_id},
            {"$inc": {"iteration": 1}, "$set": {"updated_at": datetime.now(timezone.utc)}},
            return_document=True,
        )
        return int((result or {}).get("iteration") or 0)

    def set_pending_question(self, session_id: str, question_payload: Dict[str, Any]) -> bool:
        return self.update_fields(
            session_id,
            {
                "state": "awaiting_user",
                "pending_question": question_payload,
                "pending_answer": None,
            },
        )

    def submit_user_answer(self, session_id: str, answer: str) -> bool:
        """Записує відповідь користувача та переводить state у running."""
        return self.update_fields(
            session_id,
            {
                "state": "running",
                "pending_answer": (answer or "")[:8000],
            },
        )

    def consume_pending_answer(self, session_id: str) -> Optional[str]:
        """Атомарно витягує pending_answer і обнуляє його."""
        doc = self.collection.find_one_and_update(
            {"session_id": session_id, "pending_answer": {"$ne": None}},
            {
                "$set": {
                    "pending_answer": None,
                    "pending_question": None,
                    "updated_at": datetime.now(timezone.utc),
                }
            },
            projection={"pending_answer": 1},
            return_document=False,
        )
        if not doc:
            return None
        return doc.get("pending_answer")

    def set_report_artifact(
        self,
        session_id: str,
        artifact_id: str,
        download_token: Optional[str] = None,
    ) -> bool:
        return self.update_fields(
            session_id,
            {
                "state": "done",
                "report_artifact_id": artifact_id,
                "report_artifact_token": download_token,
            },
        )

    def fail(self, session_id: str, error: str) -> bool:
        return self.update_fields(
            session_id,
            {"state": "failed", "last_error": (error or "")[:4000]},
        )

    def cancel(self, session_id: str) -> bool:
        return self.update_fields(session_id, {"state": "cancelled"})
