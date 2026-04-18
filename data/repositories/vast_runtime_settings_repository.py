# -*- coding: utf-8 -*-
"""
Репозиторій runtime-налаштувань Vast.ai / vLLM для parsing pipeline.
"""

from datetime import datetime, timezone
from typing import Any, Dict

from data.repositories.base_repository import BaseRepository


class VastRuntimeSettingsRepository(BaseRepository):
    """Зберігає єдиний документ налаштувань Vast.ai."""

    COLLECTION_NAME = "vast_runtime_settings"
    SETTINGS_ID = "default"

    def __init__(self) -> None:
        super().__init__(self.COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("updated_at")
            self.collection.create_index("is_enabled")
            self._indexes_created = True
        except Exception:
            pass

    def get_settings(self) -> Dict[str, Any]:
        self._ensure_indexes()
        doc = self.find_one({"_id": self.SETTINGS_ID})
        return doc or {}

    def save_settings(self, payload: Dict[str, Any]) -> str:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        doc = dict(payload or {})
        doc["_id"] = self.SETTINGS_ID
        doc["updated_at"] = now
        doc.setdefault("created_at", now)
        self.collection.replace_one({"_id": self.SETTINGS_ID}, doc, upsert=True)
        return self.SETTINGS_ID
