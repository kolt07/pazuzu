# -*- coding: utf-8 -*-
"""
Репозиторій станів фонових задач RabbitMQ/Celery.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository


class BackgroundTaskRepository(BaseRepository):
    """Зберігає стан, heartbeat та короткий результат фонових задач."""

    COLLECTION_NAME = "background_tasks"

    def __init__(self) -> None:
        super().__init__(self.COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("task_id", unique=True)
            self.collection.create_index("queue_name")
            self.collection.create_index("task_name")
            self.collection.create_index("state")
            self.collection.create_index("metadata.llm_batch_id")
            self.collection.create_index("updated_at")
            self.collection.create_index("heartbeat_at")
            self._indexes_created = True
        except Exception:
            pass

    def register_task(
        self,
        task_id: str,
        task_name: str,
        queue_name: str,
        payload: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> None:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        self.collection.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "task_name": task_name,
                    "queue_name": queue_name,
                    "state": "queued",
                    "payload": payload or {},
                    "metadata": metadata or {},
                    "updated_at": now,
                    "heartbeat_at": now,
                },
                "$setOnInsert": {
                    "created_at": now,
                    "started_at": None,
                    "finished_at": None,
                    "result": None,
                    "error": "",
                },
            },
            upsert=True,
        )

    def mark_started(self, task_id: str, worker_id: str = "") -> None:
        self._set_state(task_id, "running", worker_id=worker_id, started_at=datetime.now(timezone.utc))

    def heartbeat(self, task_id: str, patch: Optional[Dict[str, Any]] = None) -> None:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        update_patch = dict(patch or {})
        update_patch.update({"heartbeat_at": now, "updated_at": now})
        self.collection.update_one({"task_id": task_id}, {"$set": update_patch})

    def mark_finished(
        self,
        task_id: str,
        state: str,
        result: Optional[Dict[str, Any]] = None,
        error: str = "",
    ) -> None:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        self.collection.update_one(
            {"task_id": task_id},
            {
                "$set": {
                    "state": state,
                    "result": result,
                    "error": error,
                    "finished_at": now,
                    "heartbeat_at": now,
                    "updated_at": now,
                }
            },
        )

    def get_by_task_id(self, task_id: str) -> Optional[Dict[str, Any]]:
        self._ensure_indexes()
        doc = self.collection.find_one({"task_id": task_id})
        if not doc:
            return None
        doc["_id"] = str(doc["_id"])
        return doc

    def count_by_queue_states(self, queue_name: str, states: List[str]) -> int:
        self._ensure_indexes()
        return int(self.collection.count_documents({"queue_name": queue_name, "state": {"$in": states}}))

    def has_recent_activity(self, queue_name: str, states: List[str], within_seconds: int) -> bool:
        self._ensure_indexes()
        threshold = datetime.now(timezone.utc) - timedelta(seconds=max(1, int(within_seconds)))
        return (
            self.collection.count_documents(
                {
                    "queue_name": queue_name,
                    "state": {"$in": states},
                    "$or": [
                        {"heartbeat_at": {"$gte": threshold}},
                        {"updated_at": {"$gte": threshold}},
                    ],
                }
            )
            > 0
        )

    def list_by_task_ids(self, task_ids: List[str]) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        docs = list(self.collection.find({"task_id": {"$in": list(task_ids or [])}}))
        for doc in docs:
            doc["_id"] = str(doc["_id"])
        return docs

    def count_by_batch_id(self, batch_id: str, states: Optional[List[str]] = None) -> int:
        self._ensure_indexes()
        query: Dict[str, Any] = {"metadata.llm_batch_id": str(batch_id or "").strip()}
        if states:
            query["state"] = {"$in": list(states)}
        return int(self.collection.count_documents(query))

    def _set_state(self, task_id: str, state: str, **extra: Any) -> None:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        patch = {"state": state, "heartbeat_at": now, "updated_at": now}
        patch.update(extra)
        self.collection.update_one({"task_id": task_id}, {"$set": patch})
