# -*- coding: utf-8 -*-
"""
RelativeDateResolver: детермінований резолвер відносних періодів у конкретні дати.
Без LLM — тільки rule-based обчислення.
"""

from datetime import datetime, timedelta, timezone
from typing import Dict, Any, Optional


class RelativeDateResolver:
    """
    Резолвить відносні періоди в конкретні дати.
    LLM не має генерувати {{LAST_WEEK_START_DATE}} — тільки {type: "relative", value: "last_week"}.
    """

    PERIOD_MAP = {
        "last_1_day": 1,
        "last_7_days": 7,
        "last_week": 7,
        "last_30_days": 30,
        "last_month": 30,
    }

    def resolve(self, date_range: Optional[Dict[str, Any]]) -> Optional[Dict[str, str]]:
        """
        Резолвить date_range у конкретні дати.

        Args:
            date_range: Вхідний формат:
                - {type: "relative", value: "last_week"}
                - {period: "last_7_days"}
                - {period: "last_1_day"}, {period: "last_30_days"} тощо

        Returns:
            {gte: "2026-02-05T00:00:00.000Z", lte: "2026-02-12T23:59:59.999Z"}
            або None якщо не вдалося розрізнити
        """
        if not date_range or not isinstance(date_range, dict):
            return None

        days = None
        if "period" in date_range:
            period = date_range.get("period")
            days = self.PERIOD_MAP.get(period)
        elif date_range.get("type") == "relative" and "value" in date_range:
            value = date_range.get("value")
            days = self.PERIOD_MAP.get(value)

        if days is None:
            return None

        now = datetime.now(timezone.utc)
        start = now - timedelta(days=days)
        lte = now.strftime("%Y-%m-%dT%H:%M:%S.999Z")
        gte = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")

        return {"gte": gte, "lte": lte}
