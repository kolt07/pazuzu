# -*- coding: utf-8 -*-
"""
Репозиторій для збереження контексту діалогів: історія повідомлень та службові дані
(пайплайни, тимчасові вибірки, тощо) для кожного чату окремо.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository

CHAT_SESSION_TTL_DAYS = 30
MAX_MESSAGES_PER_SESSION = 50
MAX_SERVICE_DATA_ITEMS = 20


class ChatSessionRepository(BaseRepository):
    """
    Зберігання контексту діалогу на пару (user_id, chat_id).
    messages: [{role, content}, ...] — історія для контексту LLM
    service_data: {temp_collection_ids: [...], last_pipeline: {...}, ...} — службові дані
    """

    def __init__(self):
        super().__init__("chat_sessions")
        self._sidebar_indexes = False

    def ensure_sidebar_indexes(self) -> None:
        """Публічний виклик для міграцій: індекс (user_id, updated_at) для списку чатів."""
        if self._sidebar_indexes:
            return
        try:
            self.collection.create_index([("user_id", 1), ("updated_at", -1)], name="user_id_updated_desc")
            self._sidebar_indexes = True
        except Exception:
            pass

    def _ensure_sidebar_indexes(self) -> None:
        self.ensure_sidebar_indexes()

    def list_sidebar_for_user(self, user_id: str, limit: int = 200) -> List[Dict[str, Any]]:
        """Список чатів користувача для Mini App sidebar (синхронізація між пристроями)."""
        self._ensure_sidebar_indexes()
        cur = (
            self.collection.find({"user_id": str(user_id)})
            .sort([("updated_at", -1)])
            .limit(int(limit))
        )
        out: List[Dict[str, Any]] = []
        for doc in cur:
            doc["_id"] = str(doc.get("_id", ""))
            out.append(doc)
        return out

    def upsert_sidebar_state(
        self,
        *,
        user_id: str,
        chat_id: str,
        title: str,
        kind: str,
        flx: Optional[Dict[str, Any]] = None,
        ui_messages: Optional[List[Dict[str, Any]]] = None,
        listing_context: Optional[Dict[str, Any]] = None,
    ) -> None:
        """Зберігає метадані чату та останні UI-повідомлення (окремо від messages для LLM-контексту)."""
        self._ensure_sidebar_indexes()
        now = datetime.now(timezone.utc)
        uid = str(user_id)
        cid = str(chat_id)
        set_doc: Dict[str, Any] = {
            "user_id": uid,
            "chat_id": cid,
            "ui_title": (title or "")[:500],
            "ui_kind": (kind or "assistant")[:64],
            "updated_at": now,
        }
        if flx is not None:
            set_doc["ui_flx"] = dict(flx)
        if ui_messages is not None:
            safe = ui_messages[-120:] if len(ui_messages) > 120 else list(ui_messages)
            set_doc["ui_messages"] = safe
        if listing_context is not None:
            set_doc["ui_listing_context"] = dict(listing_context)
        self.collection.update_one(
            {"user_id": uid, "chat_id": cid},
            {"$set": set_doc, "$setOnInsert": {"messages": [], "service_data": {}}},
            upsert=True,
        )

    def delete_sidebar(self, user_id: str, chat_id: str) -> int:
        res = self.collection.delete_one({"user_id": str(user_id), "chat_id": str(chat_id)})
        return int(res.deleted_count)

    def _doc_id(self, user_id: str, chat_id: str) -> str:
        return f"{user_id}:{chat_id}"

    def get(self, user_id: str, chat_id: str) -> Dict[str, Any]:
        """Повертає сесію чату або порожній словник."""
        doc = self.collection.find_one({
            "user_id": user_id,
            "chat_id": chat_id,
        })
        if not doc:
            return {}
        return {
            "messages": doc.get("messages") or [],
            "service_data": doc.get("service_data") or {},
            "updated_at": doc.get("updated_at"),
        }

    def append_message(self, user_id: str, chat_id: str, role: str, content: str) -> None:
        """Додає повідомлення до історії чату."""
        now = datetime.now(timezone.utc)
        msg = {"role": role, "content": content[:50000]}  # обмеження розміру
        self.collection.update_one(
            {"user_id": user_id, "chat_id": chat_id},
            {
                "$push": {
                    "messages": {"$each": [msg], "$slice": -MAX_MESSAGES_PER_SESSION}
                },
                "$set": {"updated_at": now, "user_id": user_id, "chat_id": chat_id},
            },
            upsert=True,
        )

    def update_service_data(
        self,
        user_id: str,
        chat_id: str,
        updates: Dict[str, Any],
    ) -> None:
        """Оновлює службові дані сесії (temp_collection_ids, last_pipeline тощо)."""
        now = datetime.now(timezone.utc)
        set_updates = {f"service_data.{k}": v for k, v in updates.items()}
        set_updates["updated_at"] = now
        self.collection.update_one(
            {"user_id": user_id, "chat_id": chat_id},
            {"$set": set_updates},
            upsert=True,
        )

    def append_temp_collection(
        self,
        user_id: str,
        chat_id: str,
        temp_collection_id: str,
        source_collection: str,
        count: int,
    ) -> None:
        """Додає temp_collection_id до списку останніх вибірок (для «експортуй це»)."""
        now = datetime.now(timezone.utc)
        item = {
            "temp_collection_id": temp_collection_id,
            "source_collection": source_collection,
            "count": count,
        }
        self.collection.update_one(
            {"user_id": user_id, "chat_id": chat_id},
            {
                "$push": {
                    "service_data.temp_collections": {
                        "$each": [item],
                        "$slice": -MAX_SERVICE_DATA_ITEMS,
                    }
                },
                "$set": {"updated_at": now},
            },
            upsert=True,
        )

    def get_listing_context(self, user_id: str, chat_id: str) -> Optional[Dict[str, Any]]:
        """Повертає збережений контекст оголошення для чату (об'єкт, про який йде розмова)."""
        doc = self.get(user_id, chat_id)
        return (doc.get("service_data") or {}).get("listing_context")

    def set_listing_context(
        self,
        user_id: str,
        chat_id: str,
        listing_context: Dict[str, Any],
    ) -> None:
        """Зберігає контекст оголошення для чату. Викликати при першому повідомленні з listing_context."""
        if not listing_context or not isinstance(listing_context, dict):
            return
        self.update_service_data(user_id, chat_id, {"listing_context": listing_context})

    def get_messages_for_context(
        self,
        user_id: str,
        chat_id: str,
        max_pairs: int = 10,
    ) -> List[Dict[str, str]]:
        """Повертає останні пари (user, assistant) для контексту LLM."""
        doc = self.get(user_id, chat_id)
        messages = doc.get("messages") or []
        pairs = []
        i = len(messages) - 1
        while i >= 0 and len(pairs) < max_pairs:
            if i > 0 and messages[i].get("role") == "assistant" and messages[i - 1].get("role") == "user":
                pairs.insert(0, {
                    "user": messages[i - 1].get("content", ""),
                    "assistant": messages[i].get("content", ""),
                })
                i -= 2
            else:
                i -= 1
        return pairs
