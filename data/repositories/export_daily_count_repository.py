# -*- coding: utf-8 -*-
"""
Лічильник експортів по user_id за добу (для ліміту exports_per_user_per_day).
"""

from typing import Optional
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository
from pymongo import ReturnDocument


class ExportDailyCountRepository(BaseRepository):
    """Документ: user_id, date (YYYY-MM-DD), count. Індекс (user_id, date) унікальний."""

    def __init__(self):
        super().__init__("export_daily_count")

    def _today_utc(self) -> str:
        return datetime.now(timezone.utc).strftime("%Y-%m-%d")

    def get_count(self, user_id: str, date: Optional[str] = None) -> int:
        if not date:
            date = self._today_utc()
        doc = self.collection.find_one({"user_id": user_id, "date": date})
        return doc.get("count", 0) if doc else 0

    def increment(self, user_id: str, date: Optional[str] = None) -> int:
        if not date:
            date = self._today_utc()
        result = self.collection.find_one_and_update(
            {"user_id": user_id, "date": date},
            {"$inc": {"count": 1}},
            upsert=True,
            return_document=ReturnDocument.AFTER,
        )
        return result.get("count", 1)

    def ensure_indexes(self) -> None:
        self.collection.create_index([("user_id", 1), ("date", 1)], unique=True)
