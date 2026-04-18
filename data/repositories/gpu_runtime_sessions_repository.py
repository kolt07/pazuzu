# -*- coding: utf-8 -*-
"""
Репозиторій сесій оренди GPU (Vast.ai) для обліку часу та витрат.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from data.repositories.base_repository import BaseRepository


class GpuRuntimeSessionsRepository(BaseRepository):
    """Зберігає lifecycle сесій орендованих потужностей."""

    COLLECTION_NAME = "gpu_runtime_sessions"

    def __init__(self) -> None:
        super().__init__(self.COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("started_at", -1)])
            self.collection.create_index([("finished_at", -1)])
            self.collection.create_index("state")
            self.collection.create_index("provider")
            self.collection.create_index("instance_id")
            self._indexes_created = True
        except Exception:
            pass

    def start_session(self, payload: Dict[str, Any]) -> str:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        doc = dict(payload or {})
        doc.update(
            {
                "provider": "vast.ai",
                "state": "starting",
                "started_at": now,
                "updated_at": now,
                "finished_at": None,
            }
        )
        return self.create(doc)

    def update_session(self, session_id: str, patch: Dict[str, Any]) -> bool:
        self._ensure_indexes()
        update = dict(patch or {})
        update["updated_at"] = datetime.now(timezone.utc)
        return self.update_by_id(session_id, {"$set": update})

    def finish_session(
        self,
        session_id: str,
        state: str,
        estimated_cost_usd: Optional[float] = None,
    ) -> bool:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        patch: Dict[str, Any] = {
            "state": state,
            "finished_at": now,
            "updated_at": now,
        }
        if estimated_cost_usd is not None:
            patch["estimated_cost_usd"] = float(estimated_cost_usd)
        return self.update_by_id(session_id, {"$set": patch})
