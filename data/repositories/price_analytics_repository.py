# -*- coding: utf-8 -*-
"""
Репозиторій для зведеної аналітики цін (price_analytics).
Зберігає агреговані метрики за періодами та групами.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository


class PriceAnalyticsRepository(BaseRepository):
    """
    Репозиторій для зведеної аналітики цін.
    
    Документи:
    - Агреговані метрики: period_type, period_key, group_by (source, property_type, region, city),
      metrics (avg, std, q1, q2, q3, q4), count
    - Індикатори ціни: period_type="indicator", city, region, metric, q1, q2, q3, q4, count
    """

    def __init__(self):
        super().__init__("price_analytics")
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("period_type", 1), ("period_key", 1)])
            self.collection.create_index([("city", 1), ("metric", 1)])
            self.collection.create_index([("region", 1), ("city", 1)])
            self.collection.create_index("computed_at")
            self._indexes_created = True
        except Exception:
            pass

    def upsert_aggregate(self, doc: Dict[str, Any]) -> str:
        """Зберігає або оновлює документ агрегації."""
        self._ensure_indexes()
        doc["computed_at"] = datetime.now(timezone.utc)
        group = doc.get("group_by", {})
        for k, v in group.items():
            doc[f"group_{k}"] = v
        key = {
            "period_type": doc["period_type"],
            "period_key": doc["period_key"],
            "group_source": group.get("source", ""),
            "group_property_type": group.get("property_type", ""),
            "group_region": group.get("region", ""),
            "group_city": group.get("city", ""),
        }
        existing = self.collection.find_one(key)
        if existing:
            self.collection.update_one(key, {"$set": doc})
            return str(existing.get("_id", ""))
        return str(self.create(doc))

    def upsert_indicator(self, city: str, region: str, metric: str, q1: float, q2: float, q3: float, q4: float, count: int) -> str:
        """Зберігає індикатор ціни (квартилі) для міста та метрики."""
        self._ensure_indexes()
        doc = {
            "period_type": "indicator",
            "period_key": "last_30_days",
            "city": city,
            "region": region or "",
            "metric": metric,
            "q1": q1,
            "q2": q2,
            "q3": q3,
            "q4": q4,
            "count": count,
            "computed_at": datetime.now(timezone.utc),
        }
        key = {"period_type": "indicator", "city": city, "region": region or "", "metric": metric}
        existing = self.collection.find_one(key)
        if existing:
            self.collection.update_one(key, {"$set": doc})
            return str(existing.get("_id", ""))
        return str(self.create(doc))

    def get_indicator(self, city: str, metric: str, region: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Отримує індикатор ціни (квартилі) для міста та метрики."""
        self._ensure_indexes()
        query = {"period_type": "indicator", "city": city, "metric": metric}
        if region:
            query["region"] = region
        return self.find_one(query)

    def get_region_indicator(self, region: str, metric: str) -> Optional[Dict[str, Any]]:
        """Отримує індикатор ціни на рівні області (fallback, коли по місту замало даних)."""
        self._ensure_indexes()
        if not region:
            return None
        return self.find_one({
            "period_type": "indicator",
            "city": "",
            "region": region,
            "metric": metric,
        })

    def upsert_region_indicator(self, region: str, metric: str, q1: float, q2: float, q3: float, q4: float, count: int) -> str:
        """Зберігає індикатор ціни на рівні області (city='')."""
        self._ensure_indexes()
        doc = {
            "period_type": "indicator",
            "period_key": "last_30_days",
            "city": "",
            "region": region or "",
            "metric": metric,
            "q1": q1,
            "q2": q2,
            "q3": q3,
            "q4": q4,
            "count": count,
            "computed_at": datetime.now(timezone.utc),
        }
        key = {"period_type": "indicator", "city": "", "region": region or "", "metric": metric}
        existing = self.collection.find_one(key)
        if existing:
            self.collection.update_one(key, {"$set": doc})
            return str(existing.get("_id", ""))
        return str(self.create(doc))

    def get_indicators_batch(self, city_region_pairs: List[tuple], metrics: List[str]) -> Dict[str, Dict[str, Any]]:
        """
        Отримує індикатори для списку (city, region) та метрик.
        Returns: key "(city, region, metric)" -> {q1, q2, q3, q4}
        """
        self._ensure_indexes()
        result = {}
        for city, region in city_region_pairs:
            for metric in metrics:
                ind = self.get_indicator(city, metric, region)
                key = (city or "", region or "", metric)
                if ind:
                    result[key] = {"q1": ind.get("q1"), "q2": ind.get("q2"), "q3": ind.get("q3"), "q4": ind.get("q4")}
        return result

    def get_aggregates(
        self,
        period_type: str,
        period_key: Optional[str] = None,
        source: Optional[str] = None,
        property_type: Optional[str] = None,
        region: Optional[str] = None,
        city: Optional[str] = None,
        limit: int = 500,
    ) -> List[Dict[str, Any]]:
        """Отримує агреговані метрики з фільтрами."""
        self._ensure_indexes()
        query = {"period_type": period_type}
        if period_key:
            query["period_key"] = period_key
        if source:
            query["group_source"] = source
        if property_type:
            query["group_property_type"] = property_type
        if region:
            query["group_region"] = region
        if city:
            query["group_city"] = city
        return list(self.collection.find(query).sort("period_key", -1).limit(limit))

    def clear_indicators(self) -> int:
        """Видаляє всі індикатори (перед перерахунком)."""
        self._ensure_indexes()
        r = self.collection.delete_many({"period_type": "indicator"})
        return r.deleted_count

    def clear_aggregates(self, period_type: Optional[str] = None) -> int:
        """Видаляє агреговані дані (перед перерахунком)."""
        self._ensure_indexes()
        query = {"period_type": {"$ne": "indicator"}}
        if period_type:
            query["period_type"] = period_type
        r = self.collection.delete_many(query)
        return r.deleted_count
