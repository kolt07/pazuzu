# -*- coding: utf-8 -*-
"""Колекція market_research_runs — точкові дослідження ринку."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "market_research_runs"

STATUS_QUEUED = "queued"
STATUS_SEARCHING = "searching_sources"
STATUS_PROCESSING = "processing"
STATUS_ANALYZING = "analyzing"
STATUS_DONE = "done"
STATUS_ERROR = "error"
STATUS_CANCELLED = "cancelled"

ACTIVE_STATUSES = (STATUS_QUEUED, STATUS_SEARCHING, STATUS_PROCESSING, STATUS_ANALYZING)


class MarketResearchRepository(BaseRepository):
    """Один документ на запит дослідження ринку."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)
        self._ensure_indexes()

    def _ensure_indexes(self) -> None:
        try:
            self.collection.create_index("research_id", unique=True, name="research_id_unique")
            self.collection.create_index([("user_id", 1), ("created_at", -1)], name="user_created_idx")
            self.collection.create_index("status", name="status_idx")
            self.collection.create_index("celery_task_id", name="celery_task_id_idx")
            self.collection.create_index(
                "created_at",
                expireAfterSeconds=60 * 60 * 24 * 30,
                name="ttl_created_at",
            )
        except Exception:
            pass

    def create_run(
        self,
        *,
        user_id: str,
        filter_spec: Dict[str, Any],
        deal_types: List[str],
        depth_days: Optional[int],
        confirm_broad: bool = False,
        research_id: Optional[str] = None,
    ) -> str:
        self._ensure_indexes()
        rid = (research_id or "").strip() or uuid4().hex
        now = datetime.now(timezone.utc)
        doc = {
            "research_id": rid,
            "user_id": str(user_id),
            "status": STATUS_QUEUED,
            "phase": STATUS_QUEUED,
            "message": "У черзі",
            "filter_spec": filter_spec or {},
            "deal_types": list(deal_types or ["sale"]),
            "depth_days": depth_days,
            "confirm_broad": bool(confirm_broad),
            "listing_keys": [],
            "counts": {
                "found": 0,
                "new": 0,
                "processed": 0,
                "olx": 0,
                "prozorro": 0,
            },
            "report": None,
            "error": None,
            "celery_task_id": None,
            "created_at": now,
            "updated_at": now,
        }
        self.collection.update_one(
            {"research_id": rid},
            {"$setOnInsert": doc},
            upsert=True,
        )
        return rid

    def get_run(self, research_id: str) -> Optional[Dict[str, Any]]:
        self._ensure_indexes()
        doc = self.collection.find_one({"research_id": str(research_id or "").strip()})
        if doc and "_id" in doc and hasattr(doc["_id"], "binary"):
            doc["_id"] = str(doc["_id"])
        return doc

    def patch_run(self, research_id: str, patch: Dict[str, Any]) -> None:
        rid = str(research_id or "").strip()
        if not rid or not patch:
            return
        patch = {**patch, "updated_at": datetime.now(timezone.utc)}
        self.collection.update_one({"research_id": rid}, {"$set": patch})

    def find_active_for_user(self, user_id: str) -> Optional[Dict[str, Any]]:
        self._ensure_indexes()
        doc = self.collection.find_one(
            {"user_id": str(user_id), "status": {"$in": list(ACTIVE_STATUSES)}},
            sort=[("created_at", -1)],
        )
        if doc and "_id" in doc and hasattr(doc["_id"], "binary"):
            doc["_id"] = str(doc["_id"])
        return doc

    def list_for_user(self, user_id: str, limit: int = 30) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        cursor = (
            self.collection.find({"user_id": str(user_id)})
            .sort("created_at", -1)
            .limit(max(1, int(limit)))
        )
        out = []
        for doc in cursor:
            if "_id" in doc and hasattr(doc["_id"], "binary"):
                doc["_id"] = str(doc["_id"])
            out.append(doc)
        return out
