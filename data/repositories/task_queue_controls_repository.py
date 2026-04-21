# -*- coding: utf-8 -*-
"""
Репозиторій керування станом черг задач (running/paused/disabled).
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from data.repositories.base_repository import BaseRepository


class TaskQueueControlsRepository(BaseRepository):
    """Зберігає адміністративний стан черг у MongoDB."""

    COLLECTION_NAME = "task_queue_controls"

    def __init__(self) -> None:
        super().__init__(self.COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("queue_name", unique=True)
            self.collection.create_index("state")
            self.collection.create_index("updated_at")
            self._indexes_created = True
        except Exception:
            pass

    def get_control(self, queue_name: str) -> Optional[Dict[str, Any]]:
        self._ensure_indexes()
        doc = self.collection.find_one({"queue_name": str(queue_name or "").strip()})
        if not doc:
            return None
        doc["_id"] = str(doc["_id"])
        return doc

    def set_control(
        self,
        queue_name: str,
        state: str,
        updated_by: Optional[str] = None,
        reason: str = "",
    ) -> Dict[str, Any]:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        qn = str(queue_name or "").strip()
        st = str(state or "").strip().lower() or "running"
        self.collection.update_one(
            {"queue_name": qn},
            {
                "$set": {
                    "queue_name": qn,
                    "state": st,
                    "updated_at": now,
                    "updated_by": str(updated_by or "").strip(),
                    "reason": str(reason or "").strip(),
                },
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )
        return self.get_control(qn) or {"queue_name": qn, "state": st}
