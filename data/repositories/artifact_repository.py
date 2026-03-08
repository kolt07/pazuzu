# -*- coding: utf-8 -*-
"""
Репозиторій артефактів: зберігання згенерованих файлів (Excel, звіти) з TTL та власником.
Клієнт отримує artifact_id і може забрати файл через API; TTL-очищення видаляє старі артефакти.
"""

from typing import Dict, Any, Optional, List
from datetime import datetime, timezone, timedelta
from data.repositories.base_repository import BaseRepository


class ArtifactRepository(BaseRepository):
    """
    Документ: artifact_id, user_id, type (excel/report), created_at, expires_at, metadata, content_base64.
    Індекс по expires_at для TTL або ручного очищення.
    """

    def __init__(self):
        super().__init__("artifacts")

    def create(
        self,
        artifact_id: str,
        user_id: Optional[str],
        artifact_type: str,
        content_base64: str,
        metadata: Optional[Dict[str, Any]] = None,
        ttl_seconds: int = 3600,
        download_token: Optional[str] = None,
    ) -> str:
        now = datetime.now(timezone.utc)
        expires_at = now + timedelta(seconds=ttl_seconds)
        meta = dict(metadata or {})
        if download_token:
            meta["download_token"] = download_token
        doc = {
            "artifact_id": artifact_id,
            "user_id": user_id,
            "type": artifact_type,
            "created_at": now,
            "expires_at": expires_at,
            "metadata": meta,
            "content_base64": content_base64,
        }
        result = self.collection.insert_one(doc)
        return str(result.inserted_id)

    def get_by_artifact_id(self, artifact_id: str) -> Optional[Dict[str, Any]]:
        doc = self.collection.find_one({"artifact_id": artifact_id})
        if not doc:
            return None
        doc["_id"] = str(doc["_id"])
        return doc

    def delete_expired(self, before: Optional[datetime] = None) -> int:
        if before is None:
            before = datetime.now(timezone.utc)
        result = self.collection.delete_many({"expires_at": {"$lt": before}})
        return result.deleted_count

    def delete_by_ids(self, artifact_ids: List[str], user_id: Optional[str] = None) -> int:
        """Видаляє артефакти за списком ID. Якщо user_id вказано — лише артефакти цього користувача."""
        if not artifact_ids:
            return 0
        query = {"artifact_id": {"$in": artifact_ids}}
        if user_id is not None:
            query["user_id"] = user_id
        result = self.collection.delete_many(query)
        return result.deleted_count

    def ensure_indexes(self) -> None:
        self.collection.create_index("artifact_id", unique=True)
        self.collection.create_index("expires_at")
