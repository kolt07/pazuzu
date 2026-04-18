# -*- coding: utf-8 -*-
"""
Міжпроцесна координація lifecycle GPU runtime.
"""

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, Optional

from pymongo.errors import DuplicateKeyError

from data.repositories.base_repository import BaseRepository


class GpuRuntimeCoordinationRepository(BaseRepository):
    """Singleton-документ для координації Vast runtime між процесами."""

    COLLECTION_NAME = "gpu_runtime_coordination"
    DOC_ID = "vast_vllm_runtime"

    def __init__(self) -> None:
        super().__init__(self.COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("owner_id")
            self.collection.create_index("state")
            self.collection.create_index("lease_expires_at")
            self._indexes_created = True
        except Exception:
            pass

    def get_runtime_state(self) -> Optional[Dict[str, Any]]:
        self._ensure_indexes()
        doc = self.collection.find_one({"_id": self.DOC_ID})
        if not doc:
            return None
        doc["_id"] = str(doc["_id"])
        return doc

    def try_acquire(self, owner_id: str, lease_seconds: int, payload: Optional[Dict[str, Any]] = None) -> bool:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        lease_until = now + timedelta(seconds=max(30, int(lease_seconds)))
        patch = dict(payload or {})
        patch.update(
            {
                "owner_id": owner_id,
                "lease_expires_at": lease_until,
                "updated_at": now,
            }
        )
        # Не використовуємо upsert: якщо документ з _id уже є, але фільтр не збігається
        # (інший owner з активним lease), upsert намагався б insert → E11000 duplicate key.
        filter_acquire = {
            "_id": self.DOC_ID,
            "$or": [
                {"owner_id": owner_id},
                {"owner_id": {"$exists": False}},
                {"owner_id": None},
                {"lease_expires_at": {"$lte": now}},
            ],
        }
        result = self.collection.update_one(
            filter_acquire,
            {"$set": patch},
            upsert=False,
        )
        if result.matched_count:
            return True
        if self.collection.find_one({"_id": self.DOC_ID}) is not None:
            return False
        try:
            self.collection.insert_one({"_id": self.DOC_ID, "created_at": now, **patch})
            return True
        except DuplicateKeyError:
            retry = self.collection.update_one(
                filter_acquire,
                {"$set": patch},
                upsert=False,
            )
            return bool(retry.matched_count)

    def renew(self, owner_id: str, lease_seconds: int, payload: Optional[Dict[str, Any]] = None) -> bool:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        lease_until = now + timedelta(seconds=max(30, int(lease_seconds)))
        patch = dict(payload or {})
        patch.update(
            {
                "lease_expires_at": lease_until,
                "updated_at": now,
            }
        )
        result = self.collection.update_one(
            {"_id": self.DOC_ID, "owner_id": owner_id},
            {"$set": patch},
            upsert=False,
        )
        return bool(result.matched_count)

    def update_state(self, owner_id: str, patch: Dict[str, Any]) -> bool:
        self._ensure_indexes()
        update_patch = dict(patch or {})
        update_patch["updated_at"] = datetime.now(timezone.utc)
        result = self.collection.update_one(
            {"_id": self.DOC_ID, "owner_id": owner_id},
            {"$set": update_patch},
        )
        return result.modified_count > 0

    def release(self, owner_id: str, patch: Optional[Dict[str, Any]] = None) -> bool:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        update_patch = dict(patch or {})
        update_patch.update(
            {
                "owner_id": None,
                "lease_expires_at": now,
                "updated_at": now,
            }
        )
        result = self.collection.update_one(
            {"_id": self.DOC_ID, "owner_id": owner_id},
            {"$set": update_patch},
            upsert=False,
        )
        return bool(result.matched_count)
