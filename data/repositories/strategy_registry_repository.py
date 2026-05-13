# -*- coding: utf-8 -*-
"""
Персистентний реєстр стратегій FLX (rolling scores, історія використання).
"""

from __future__ import annotations

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class StrategyRegistryRepository(BaseRepository):
    """Колекція `flx_strategy_registry`."""

    def __init__(self):
        super().__init__("flx_strategy_registry")
        self._indexes_ok = False

    def ensure_indexes(self) -> None:
        """Ідемпотентне створення індексів (без падіння при дублікаті)."""
        if self._indexes_ok:
            return
        try:
            self.collection.create_index(
                [("task_fingerprint", 1), ("strategy_signature", 1)],
                unique=True,
                name="task_fp_strategy_sig",
            )
            self.collection.create_index([("rolling_score", -1)], name="rolling_score_desc")
            self.collection.create_index([("task_fingerprint", 1), ("rolling_score", -1)], name="fp_roll")
        except Exception:
            logger.debug("strategy_registry index ensure failed (non-fatal)", exc_info=True)
        self._indexes_ok = True

    def upsert_aggregate(
        self,
        *,
        task_fingerprint: str,
        strategy_signature: str,
        doc_updates: Dict[str, Any],
    ) -> None:
        """Оновлює або створює запис за складним ключем."""
        now = datetime.now(timezone.utc)
        base = dict(doc_updates)
        base["task_fingerprint"] = task_fingerprint
        base["strategy_signature"] = strategy_signature
        base["updated_at"] = now

        self.collection.update_one(
            {
                "task_fingerprint": task_fingerprint,
                "strategy_signature": strategy_signature,
            },
            {
                "$set": base,
                "$setOnInsert": {"created_at": now},
            },
            upsert=True,
        )

    def find_by_task_fingerprint(
        self,
        task_fingerprint: str,
        *,
        limit: int = 12,
    ) -> List[Dict[str, Any]]:
        cur = (
            self.collection.find({"task_fingerprint": task_fingerprint})
            .sort([("rolling_score", -1), ("usage_count", -1)])
            .limit(int(limit))
        )
        out = []
        for doc in cur:
            doc["_id"] = str(doc["_id"])
            out.append(doc)
        return out

    def find_global_top(
        self,
        *,
        limit: int = 8,
        min_usage: int = 1,
    ) -> List[Dict[str, Any]]:
        """Для cold-start: найкращі стратегії загалом."""
        flt: Dict[str, Any] = {"usage_count": {"$gte": int(min_usage)}}
        cur = (
            self.collection.find(flt)
            .sort([("rolling_score", -1), ("wins", -1)])
            .limit(int(limit))
        )
        out = []
        for doc in cur:
            doc["_id"] = str(doc["_id"])
            out.append(doc)
        return out
