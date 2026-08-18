# -*- coding: utf-8 -*-
"""
Кеш добових витрат Vast.ai (instance charges) з billing API: один документ на календарний день UTC.
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from data.repositories.base_repository import BaseRepository


class VastBillingDailyRepository(BaseRepository):
    """Зберігає {date YYYY-MM-DD} → billed_usd (сума charges за день: instance+volume+serverless)."""

    COLLECTION_NAME = "vast_billing_daily"
    # 5: добові суми беруться напряму з /charges/ day-window (без додаткового prorate).
    CHARGES_SCHEMA_VERSION = 5

    def __init__(self) -> None:
        super().__init__(self.COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("updated_at")
            self._indexes_created = True
        except Exception:
            pass

    def get_billed_usd(self, date_key: str) -> Optional[float]:
        """date_key: YYYY-MM-DD. None якщо немає запису."""
        self._ensure_indexes()
        d = self.collection.find_one({"_id": date_key})
        if not d:
            return None
        try:
            return float(d.get("billed_usd") or 0.0)
        except (TypeError, ValueError):
            return None

    def get_day_entry(self, date_key: str) -> Optional[Dict[str, Any]]:
        """
        Повертає запис дня:
        { "_id", "billed_usd", "charges_schema_version", "updated_at" } або None.
        """
        self._ensure_indexes()
        d = self.collection.find_one({"_id": date_key})
        if not isinstance(d, dict):
            return None
        try:
            billed = float(d.get("billed_usd") or 0.0)
        except (TypeError, ValueError):
            billed = 0.0
        return {
            "_id": str(d.get("_id") or date_key),
            "billed_usd": billed,
            "charges_schema_version": int(d.get("charges_schema_version") or 0),
            "updated_at": d.get("updated_at"),
        }

    def upsert_day(self, date_key: str, billed_usd: float) -> None:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        self.collection.replace_one(
            {"_id": date_key},
            {
                "_id": date_key,
                "billed_usd": float(billed_usd),
                "charges_schema_version": int(self.CHARGES_SCHEMA_VERSION),
                "updated_at": now,
            },
            upsert=True,
        )
