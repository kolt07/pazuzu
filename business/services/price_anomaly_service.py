# -*- coding: utf-8 -*-
"""
Сервіс виявлення та обробки аномальних цін.

Логіка: спочатку аномальність у загальному масштабі (країна), потім по місцевості.
Ціна може бути середньою для країни, але аномальною для місцевості.
Зворотна ситуація (аномальна для країни, нормальна для місцевості) — недостатньо даних по місцевості.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.price_analytics_repository import (
    LISTING_TYPE_LAND,
    LISTING_TYPE_MIXED,
    LISTING_TYPE_REAL_ESTATE,
    PriceAnalyticsRepository,
)

logger = logging.getLogger(__name__)

# Межі IQR для визначення аномалії (1.5 * IQR за межами q1/q3)
IQR_MULTIPLIER = 1.5
# Ширші межі для змішаних оголошень (земля + нерухомість)
IQR_MULTIPLIER_MIXED = 2.5
MIN_SAMPLE_GLOBAL = 10
MIN_SAMPLE_LOCAL = 3


def _quartiles(values: List[float]) -> Tuple[float, float, float, float]:
    """Повертає (q1, q2, q3, q4)."""
    if not values:
        return 0.0, 0.0, 0.0, 0.0
    sorted_vals = sorted(v for v in values if v is not None and v > 0)
    n = len(sorted_vals)
    if n == 0:
        return 0.0, 0.0, 0.0, 0.0

    def _at(p: float) -> float:
        idx = p * (n - 1) if n > 1 else 0
        i = int(idx)
        f = idx - i
        if i >= n - 1:
            return sorted_vals[-1]
        return sorted_vals[i] * (1 - f) + sorted_vals[i + 1] * f

    return _at(0.25), _at(0.50), _at(0.75), _at(1.0)


class PriceAnomalyService:
    """
    Виявлення аномальних цін: глобально → по місцевості.
    LLM-верифікація для підтвердження або позначки «Ціна потребує перевірки».
    """

    def __init__(self):
        self.unified_repo = UnifiedListingsRepository()
        self.analytics_repo = PriceAnalyticsRepository()

    def _get_global_distribution(self, metric: str) -> Optional[Dict[str, float]]:
        """Отримує глобальний розподіл метрики з unified_listings."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        pipeline = [
            {"$match": {"status": "активне", "source_updated_at": {"$gte": cutoff}}},
            {"$match": {metric: {"$exists": True, "$ne": None, "$gt": 0}}},
            {"$group": {"_id": None, "vals": {"$push": f"${metric}"}}},
        ]
        for row in self.unified_repo.collection.aggregate(pipeline):
            vals = row.get("vals", [])
            if len(vals) < MIN_SAMPLE_GLOBAL:
                return None
            q1, q2, q3, q4 = _quartiles(vals)
            iqr = max(q3 - q1, (q2 * 0.05) if q2 else 1.0)
            lower = q1 - IQR_MULTIPLIER * iqr
            upper = q3 + IQR_MULTIPLIER * iqr
            return {"q1": q1, "q2": q2, "q3": q3, "lower": lower, "upper": upper, "count": len(vals)}
        return None

    def _get_listing_type(self, doc: Dict[str, Any]) -> str:
        """Визначає тип оголошення для аналітики."""
        if self._is_mixed_listing(doc):
            return LISTING_TYPE_MIXED
        if doc.get("land_area_sqm") and doc.get("land_area_sqm", 0) > 0:
            return LISTING_TYPE_LAND
        return LISTING_TYPE_REAL_ESTATE

    def _get_local_distribution(
        self,
        metric: str,
        city: str,
        region: Optional[str] = None,
        listing_type: str = LISTING_TYPE_REAL_ESTATE,
    ) -> Optional[Dict[str, float]]:
        """Отримує локальний розподіл по місту/області."""
        ind = self.analytics_repo.get_indicator(city, metric, region, listing_type)
        if not ind or ind.get("count", 0) < MIN_SAMPLE_LOCAL:
            if region:
                ind = self.analytics_repo.get_region_indicator(region, metric, listing_type)
            if not ind or ind.get("count", 0) < MIN_SAMPLE_LOCAL:
                return None
        q1, q3, q2 = ind.get("q1"), ind.get("q3"), ind.get("q2")
        if q1 is None or q3 is None:
            return None
        iqr = max(q3 - q1, (q2 * 0.05) if q2 else 1.0)
        lower = q1 - IQR_MULTIPLIER * iqr
        upper = q3 + IQR_MULTIPLIER * iqr
        return {"q1": q1, "q2": q2, "q3": q3, "lower": lower, "upper": upper}

    def _is_anomalous(
        self,
        value: float,
        global_dist: Optional[Dict],
        local_dist: Optional[Dict],
    ) -> Tuple[bool, Optional[str]]:
        """
        Визначає аномальність: спочатку глобально, потім по місцевості.
        Returns: (is_anomalous, "аномально низька"|"аномально висока"|None)
        """
        if value is None or value <= 0:
            return False, None
        # 1. Глобальна аномальність
        if global_dist:
            if value < global_dist["lower"]:
                return True, "аномально низька"
            if value > global_dist["upper"]:
                return True, "аномально висока"
        # 2. Локальна аномальність (ціна нормальна глобально, але аномальна для місцевості)
        if local_dist:
            if value < local_dist["lower"]:
                return True, "аномально низька"
            if value > local_dist["upper"]:
                return True, "аномально висока"
        return False, None

    def _is_mixed_listing(self, doc: Dict[str, Any]) -> bool:
        """Оголошення містить і нерухомість, і земельну ділянку."""
        b = doc.get("building_area_sqm") or 0
        l = doc.get("land_area_sqm") or 0
        return (b and b > 0) and (l and l > 0)

    def _get_global_distribution(
        self, metric: str, exclude_mixed: bool = False
    ) -> Optional[Dict[str, float]]:
        """Отримує глобальний розподіл метрики. exclude_mixed: виключити змішані оголошення."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        match: Dict[str, Any] = {
            "status": "активне",
            "source_updated_at": {"$gte": cutoff},
            metric: {"$exists": True, "$ne": None, "$gt": 0},
        }
        if exclude_mixed:
            match["$or"] = [
                {"building_area_sqm": {"$not": {"$gt": 0}}},
                {"land_area_sqm": {"$not": {"$gt": 0}}},
            ]
        pipeline = [{"$match": match}, {"$group": {"_id": None, "vals": {"$push": f"${metric}"}}}]
        for row in self.unified_repo.collection.aggregate(pipeline):
            vals = row.get("vals", [])
            if len(vals) < MIN_SAMPLE_GLOBAL:
                return None
            q1, q2, q3, q4 = _quartiles(vals)
            iqr = max(q3 - q1, (q2 * 0.05) if q2 else 1.0)
            lower = q1 - IQR_MULTIPLIER * iqr
            upper = q3 + IQR_MULTIPLIER * iqr
            return {"q1": q1, "q2": q2, "q3": q3, "lower": lower, "upper": upper, "count": len(vals)}
        return None

    def _get_global_distribution_mixed(self, metric: str) -> Optional[Dict[str, float]]:
        """Розподіл тільки для змішаних оголошень (ширші межі)."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        pipeline = [
            {"$match": {
                "status": "активне",
                "source_updated_at": {"$gte": cutoff},
                metric: {"$exists": True, "$ne": None, "$gt": 0},
                "building_area_sqm": {"$exists": True, "$gt": 0},
                "land_area_sqm": {"$exists": True, "$gt": 0},
            }},
            {"$group": {"_id": None, "vals": {"$push": f"${metric}"}}},
        ]
        for row in self.unified_repo.collection.aggregate(pipeline):
            vals = row.get("vals", [])
            if len(vals) < MIN_SAMPLE_GLOBAL:
                return None
            q1, q2, q3, q4 = _quartiles(vals)
            iqr = max(q3 - q1, (q2 * 0.05) if q2 else 1.0)
            lower = q1 - IQR_MULTIPLIER_MIXED * iqr
            upper = q3 + IQR_MULTIPLIER_MIXED * iqr
            return {"q1": q1, "q2": q2, "q3": q3, "lower": lower, "upper": upper, "count": len(vals)}
        return None

    def find_anomalous_listings(
        self, limit: int = 200
    ) -> List[Dict[str, Any]]:
        """
        Знаходить оголошення з аномальними цінами.
        Використовує ТІЛЬКИ ціну за одиницю площі (м² для нерухомості, га для землі).
        Змішані оголошення (земля+нерухомість) — окремий розподіл з ширшими межами.
        """
        cutoff = datetime.now(timezone.utc) - timedelta(days=90)
        docs = list(self.unified_repo.collection.find(
            {"status": "активне", "source_updated_at": {"$gte": cutoff}},
            limit=limit * 5,
        ))
        results = []
        global_ppm2 = self._get_global_distribution("price_per_m2_uah", exclude_mixed=True)
        global_ppha = self._get_global_distribution("price_per_ha_uah", exclude_mixed=True)
        global_ppm2_mixed = self._get_global_distribution_mixed("price_per_m2_uah")
        global_ppha_mixed = self._get_global_distribution_mixed("price_per_ha_uah")

        for doc in docs:
            if len(results) >= limit:
                break
            addrs = doc.get("addresses") or []
            addr = addrs[0] if addrs and isinstance(addrs[0], dict) else {}
            city = addr.get("settlement", "")
            region = addr.get("region", "")
            ppm2 = doc.get("price_per_m2_uah")
            ppha = doc.get("price_per_ha_uah")
            price = doc.get("price_uah")
            is_mixed = self._is_mixed_listing(doc)

            metric, value = None, None
            if ppm2 and ppm2 > 0:
                metric, value = "price_per_m2_uah", ppm2
            elif ppha and ppha > 0:
                metric, value = "price_per_ha_uah", ppha
            if not metric or not value:
                continue

            if is_mixed:
                global_dist = global_ppm2_mixed if metric == "price_per_m2_uah" else global_ppha_mixed
                local_dist = None
            else:
                global_dist = global_ppm2 if metric == "price_per_m2_uah" else global_ppha
                listing_type = self._get_listing_type(doc)
                local_dist = self._get_local_distribution(metric, city, region, listing_type) if city else None

            is_anom, anomaly_type = self._is_anomalous(value, global_dist, local_dist)
            if is_anom and anomaly_type:
                results.append({
                    "source": doc.get("source"),
                    "source_id": doc.get("source_id"),
                    "price_uah": price,
                    "price_per_m2_uah": ppm2,
                    "price_per_ha_uah": ppha,
                    "metric": metric,
                    "value": value,
                    "anomaly_type": anomaly_type,
                    "city": city,
                    "region": region,
                    "title": doc.get("title", "")[:100],
                    "is_mixed": is_mixed,
                })
        return results

    def verify_price_with_llm(
        self, title: str, description: str, price_text: str
    ) -> Dict[str, Any]:
        """
        LLM-верифікація ціни: чи це ціна за одиницю, оренда, договірна.
        Returns: {verified: bool, is_per_unit: bool, is_rent: bool, needs_review: bool}
        """
        # TODO: реалізувати LLM-промпт для верифікації
        # Поки повертаємо needs_review=True для аномалій
        return {"verified": False, "is_per_unit": False, "is_rent": False, "needs_review": True}

    def set_price_notes(self, source: str, source_id: str, notes: str) -> bool:
        """Встановлює примітку до ціни в unified_listings."""
        try:
            result = self.unified_repo.collection.update_one(
                {"source": source, "source_id": source_id},
                {"$set": {"price_notes": notes, "system_updated_at": datetime.now(timezone.utc)}},
            )
            return result.modified_count > 0
        except Exception as e:
            logger.warning("Помилка встановлення price_notes: %s", e)
            return False

    def process_anomalous_prices(self, limit: int = 50) -> Dict[str, Any]:
        """
        Обробка аномальних цін: знайти аномалії, повернути список.
        LLM-верифікація та встановлення "Ціна потребує перевірки" — у майбутньому.
        Returns: {found: int, items: list}
        """
        anomalous = self.find_anomalous_listings(limit=limit)
        return {"found": len(anomalous), "items": anomalous}
