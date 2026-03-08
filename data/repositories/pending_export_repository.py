# -*- coding: utf-8 -*-
"""
Тимчасове збереження експортів, що очікують підтвердження (великі вибірки).
"""

from typing import Dict, Any, Optional
from data.repositories.base_repository import BaseRepository


class PendingExportRepository(BaseRepository):
    """Документ: request_id (унікальний), user_id, temp_collection_id, filename_prefix, format."""

    def __init__(self):
        super().__init__("pending_export")

    def create(self, request_id: str, user_id: Optional[str], temp_collection_id: str, filename_prefix: str, format: str = "xlsx") -> None:
        self.collection.update_one(
            {"request_id": request_id},
            {"$set": {
                "request_id": request_id,
                "user_id": user_id,
                "temp_collection_id": temp_collection_id,
                "filename_prefix": filename_prefix,
                "format": format,
            }},
            upsert=True,
        )

    def get(self, request_id: str) -> Optional[Dict[str, Any]]:
        doc = self.collection.find_one({"request_id": request_id})
        if not doc:
            return None
        doc.pop("_id", None)
        return doc

    def delete(self, request_id: str) -> bool:
        result = self.collection.delete_one({"request_id": request_id})
        return result.deleted_count > 0

    def delete_by_temp_collection_id(self, temp_collection_id: str) -> bool:
        result = self.collection.delete_one({"temp_collection_id": temp_collection_id})
        return result.deleted_count > 0
