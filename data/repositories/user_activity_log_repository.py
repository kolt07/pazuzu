# -*- coding: utf-8 -*-
"""Репозиторій журналу активності користувачів Mini App."""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository


class UserActivityLogRepository(BaseRepository):
    """Колекція user_activity_log — окремий журнал дій користувачів."""

    def __init__(self):
        super().__init__("user_activity_log")
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("timestamp", -1)])
            self.collection.create_index([("user_id", 1), ("timestamp", -1)])
            self.collection.create_index([("category", 1), ("timestamp", -1)])
            self.collection.create_index([("action", 1), ("timestamp", -1)])
            self._indexes_created = True
        except Exception:
            pass

    def create_entry(
        self,
        *,
        user_id: int,
        category: str,
        action: str,
        message: str,
        metadata: Optional[Dict[str, Any]] = None,
        error: Optional[str] = None,
    ) -> str:
        self._ensure_indexes()
        doc = {
            "timestamp": datetime.now(timezone.utc),
            "user_id": int(user_id),
            "category": category,
            "action": action,
            "message": message,
            "metadata": metadata or {},
            "error": error,
        }
        return self.create(doc)

    def find_entries(
        self,
        *,
        category: Optional[str] = None,
        action: Optional[str] = None,
        user_id: Optional[int] = None,
        days: int = 7,
        limit: int = 100,
        skip: int = 0,
    ) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        filt: Dict[str, Any] = {}
        if category:
            filt["category"] = category
        if action:
            filt["action"] = action
        if user_id is not None:
            filt["user_id"] = int(user_id)
        if days > 0:
            since = datetime.now(timezone.utc) - timedelta(days=int(days))
            filt["timestamp"] = {"$gte": since}
        return self.find_many(
            filter=filt,
            sort=[("timestamp", -1)],
            limit=max(1, min(int(limit), 500)),
            skip=max(0, int(skip)),
        )

    def count_entries(
        self,
        *,
        category: Optional[str] = None,
        action: Optional[str] = None,
        user_id: Optional[int] = None,
        days: int = 7,
    ) -> int:
        self._ensure_indexes()
        filt: Dict[str, Any] = {}
        if category:
            filt["category"] = category
        if action:
            filt["action"] = action
        if user_id is not None:
            filt["user_id"] = int(user_id)
        if days > 0:
            since = datetime.now(timezone.utc) - timedelta(days=int(days))
            filt["timestamp"] = {"$gte": since}
        return self.collection.count_documents(filt)

    def _build_match_filter(
        self,
        *,
        category: Optional[str] = None,
        action: Optional[str] = None,
        user_id: Optional[int] = None,
        days: int = 7,
    ) -> Dict[str, Any]:
        filt: Dict[str, Any] = {}
        if category:
            filt["category"] = category
        if action:
            filt["action"] = action
        if user_id is not None:
            filt["user_id"] = int(user_id)
        if days > 0:
            since = datetime.now(timezone.utc) - timedelta(days=int(days))
            filt["timestamp"] = {"$gte": since}
        return filt

    def count_distinct_users(
        self,
        *,
        category: Optional[str] = None,
        action: Optional[str] = None,
        user_id: Optional[int] = None,
        days: int = 7,
    ) -> int:
        self._ensure_indexes()
        filt = self._build_match_filter(
            category=category, action=action, user_id=user_id, days=days
        )
        pipeline: List[Dict[str, Any]] = [
            {"$match": filt},
            {"$group": {"_id": "$user_id"}},
            {"$count": "count"},
        ]
        rows = list(self.collection.aggregate(pipeline))
        if not rows:
            return 0
        return int(rows[0].get("count", 0))

    def find_entries_grouped_by_user(
        self,
        *,
        category: Optional[str] = None,
        action: Optional[str] = None,
        user_id: Optional[int] = None,
        days: int = 7,
        users_limit: int = 20,
        users_skip: int = 0,
        events_per_user: int = 100,
    ) -> List[Dict[str, Any]]:
        """Групує події по user_id; користувачі відсортовані за останньою активністю."""
        self._ensure_indexes()
        filt = self._build_match_filter(
            category=category, action=action, user_id=user_id, days=days
        )
        events_per_user = max(1, min(int(events_per_user), 500))
        pipeline: List[Dict[str, Any]] = [
            {"$match": filt},
            {"$sort": {"timestamp": -1}},
            {
                "$group": {
                    "_id": "$user_id",
                    "last_activity": {"$first": "$timestamp"},
                    "events_count": {"$sum": 1},
                    "events": {
                        "$push": {
                            "timestamp": "$timestamp",
                            "category": "$category",
                            "action": "$action",
                            "message": "$message",
                            "metadata": "$metadata",
                            "error": "$error",
                        }
                    },
                }
            },
            {"$sort": {"last_activity": -1}},
            {"$skip": max(0, int(users_skip))},
            {"$limit": max(1, min(int(users_limit), 100))},
            {
                "$project": {
                    "user_id": "$_id",
                    "last_activity": 1,
                    "events_count": 1,
                    "events": {"$slice": ["$events", events_per_user]},
                }
            },
        ]
        return list(self.collection.aggregate(pipeline))
