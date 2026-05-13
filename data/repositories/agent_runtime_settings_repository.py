# -*- coding: utf-8 -*-
"""Runtime-прапорці агента (Mini App), окремо від config.yaml."""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from data.repositories.base_repository import BaseRepository


class AgentRuntimeSettingsRepository(BaseRepository):
    """Єдиний документ `_id: default` у колекції `agent_runtime_settings`."""

    SETTINGS_ID = "default"

    def __init__(self) -> None:
        super().__init__("agent_runtime_settings")

    def get_document(self) -> Optional[Dict[str, Any]]:
        return self.find_one({"_id": self.SETTINGS_ID})

    def upsert_partial(self, fields: Dict[str, Any]) -> None:
        now = datetime.now(timezone.utc)
        existing = self.find_one({"_id": self.SETTINGS_ID}) or {}
        merged: Dict[str, Any] = {**{k: v for k, v in existing.items() if k != "_id"}, **(fields or {})}
        merged["_id"] = self.SETTINGS_ID
        merged["updated_at"] = now
        merged.setdefault("created_at", existing.get("created_at") or now)
        self.collection.replace_one({"_id": self.SETTINGS_ID}, merged, upsert=True)
