# -*- coding: utf-8 -*-
"""
Сервіс зведеної аналітики цін.

Агрегує unified_listings за:
- Групами: джерело, тип, область, місто
- Періодами: день, тиждень (пн-нд), місяць (1-останнє число)

Метрики: середня ціна, ціна за м², ціна за га (UAH, USD).
Нормальний розподіл: mean, std, q1, q2, q3, q4.

Індикатори ціни для UI: вигідна (q1), середня (q2-q3), дорога (q4), аномально низька/висока (за межами типових меж IQR) по місту за останній місяць.
"""

import logging
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from data.repositories.price_analytics_repository import (
    LISTING_TYPE_GENERAL,
    LISTING_TYPE_LAND,
    LISTING_TYPE_MIXED,
    LISTING_TYPE_REAL_ESTATE,
    PriceAnalyticsRepository,
)
from data.repositories.unified_listings_repository import UnifiedListingsRepository

logger = logging.getLogger(__name__)

# Мінімум оголошень для розподілу по населеному пункту
MIN_SETTLEMENT_COUNT = 5
# Виключення викидів: значення за межами Q1-2*IQR та Q4+2*IQR по країні
OUTLIER_IQR_MULTIPLIER = 2.0

# Метрики для агрегації
METRICS = [
    "price_uah",
    "price_usd",
    "price_per_m2_uah",
    "price_per_m2_usd",
    "price_per_ha_uah",
    "price_per_ha_usd",
]

# Метрики для індикатора (які показуємо біля ціни)
INDICATOR_METRICS = ["price_uah", "price_per_m2_uah", "price_per_ha_uah"]

# Кількість днів для індикатора
INDICATOR_DAYS = 30


def _quartiles(values: List[float]) -> Tuple[float, float, float, float]:
    """Обчислює квартилі q1, q2 (медіана), q3, q4."""
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


def _std_dev(values: List[float]) -> float:
    """Стандартне відхилення."""
    if len(values) < 2:
        return 0.0
    mean = sum(values) / len(values)
    variance = sum((x - mean) ** 2 for x in values) / len(values)
    return variance ** 0.5


def _period_keys(dt: datetime) -> Tuple[str, str, str]:
    """Повертає (day_key, week_key, month_key) для дати."""
    day_key = dt.strftime("%Y-%m-%d")
    iso = dt.isocalendar()
    week_key = f"{iso.year}-W{iso.week:02d}"
    month_key = dt.strftime("%Y-%m")
    return day_key, week_key, month_key


class PriceAnalyticsService:
    """Сервіс зведеної аналітики цін."""

    def __init__(self):
        self.repo = PriceAnalyticsRepository()
        self.unified_repo = UnifiedListingsRepository()

    def rebuild_all(self) -> Dict[str, int]:
        """
        Повністю перераховує аналітику з unified_listings.
        Викликається після оновлення даних з джерел.
        """
        counts = {"aggregates": 0, "indicators": 0, "extracts": 0}
        try:
            # 1. Перезаповнення analytics_extracts (виокремлені дані для агрегацій)
            try:
                from business.services.analytics_extracts_populator import rebuild_analytics_extracts
                counts["extracts"] = rebuild_analytics_extracts()
            except Exception as e:
                logger.warning("Помилка перезаповнення analytics_extracts: %s", e)

            self.repo.clear_indicators()
            self.repo.clear_aggregates(period_type=None)
            counts["indicators"] = self._compute_indicators()
            counts["aggregates"] = self._compute_aggregates()
            logger.info("Price analytics rebuilt: %s", counts)
        except Exception as e:
            logger.exception("Помилка перерахунку аналітики: %s", e)
        return counts

    def _listing_type_match(self, listing_type: str) -> Dict[str, Any]:
        """Повертає $match для типу оголошення: land, real_estate, mixed, general."""
        if listing_type == LISTING_TYPE_LAND:
            return {"land_area_sqm": {"$gt": 0}, "$or": [{"building_area_sqm": {"$exists": False}}, {"building_area_sqm": {"$lte": 0}}]}
        if listing_type == LISTING_TYPE_REAL_ESTATE:
            return {"building_area_sqm": {"$gt": 0}, "$or": [{"land_area_sqm": {"$exists": False}}, {"land_area_sqm": {"$lte": 0}}]}
        if listing_type == LISTING_TYPE_MIXED:
            return {"building_area_sqm": {"$gt": 0}, "land_area_sqm": {"$gt": 0}}
        if listing_type == LISTING_TYPE_GENERAL:
            return {"$or": [
                {"land_area_sqm": {"$gt": 0}, "$or": [{"building_area_sqm": {"$exists": False}}, {"building_area_sqm": {"$lte": 0}}]},
                {"building_area_sqm": {"$gt": 0}, "$or": [{"land_area_sqm": {"$exists": False}}, {"land_area_sqm": {"$lte": 0}}]},
            ]}
        return {}

    def _get_country_quartiles(
        self, listing_type: str, cutoff: datetime
    ) -> Dict[str, Tuple[float, float, float, float, float]]:
        """Отримує країнські квартилі для фільтрації викидів (Q1-2*IQR, Q4+2*IQR)."""
        match = {"source_updated_at": {"$gte": cutoff}, "status": "активне", **self._listing_type_match(listing_type)}
        pipeline = [
            {"$match": match},
            {"$group": {
                "_id": None,
                "price_uah_vals": {"$push": "$price_uah"},
                "price_per_m2_vals": {"$push": "$price_per_m2_uah"},
                "price_per_ha_vals": {"$push": "$price_per_ha_uah"},
            }},
        ]
        result = {}
        for row in self.unified_repo.collection.aggregate(pipeline):
            for metric, vals_key in [
                ("price_uah", "price_uah_vals"),
                ("price_per_m2_uah", "price_per_m2_vals"),
                ("price_per_ha_uah", "price_per_ha_vals"),
            ]:
                vals = [v for v in row.get(vals_key, []) if v is not None and v > 0]
                if len(vals) < 3:
                    continue
                q1, q2, q3, q4 = _quartiles(vals)
                iqr = max(q3 - q1, (q2 * 0.05) if q2 else 1.0)
                result[metric] = (q1, q2, q3, q4, iqr)
        return result

    def _compute_indicators(self) -> int:
        """Обчислює індикатори ціни (3 окремі розподіли: земля, нерухомість, змішані) за останній місяць.
        Виключає викиди: значення за межами Q1-2*IQR та Q4+2*IQR по країні.
        Населений пункт (settlement): мінімум 5 оголошень. Fallback — область."""
        cutoff = datetime.now(timezone.utc) - timedelta(days=INDICATOR_DAYS)
        count = 0

        for listing_type in [LISTING_TYPE_LAND, LISTING_TYPE_REAL_ESTATE, LISTING_TYPE_MIXED, LISTING_TYPE_GENERAL]:
            type_match = self._listing_type_match(listing_type)
            if not type_match:
                continue

            country_bounds_raw = self._get_country_quartiles(listing_type, cutoff)
            country_bounds = {k: v for k, v in country_bounds_raw.items() if isinstance(v, tuple) and len(v) >= 5}

            # 1. Індикатори по населених пунктах (settlement + region)
            pipeline_settlement = [
                {"$match": {"source_updated_at": {"$gte": cutoff}, "status": "активне", **type_match}},
                {"$unwind": {"path": "$addresses", "preserveNullAndEmptyArrays": False}},
                {"$match": {"addresses.settlement": {"$exists": True, "$ne": ""}}},
                {"$group": {
                    "_id": {"city": "$addresses.settlement", "region": {"$ifNull": ["$addresses.region", ""]}},
                    "price_uah_vals": {"$push": "$price_uah"},
                    "price_per_m2_vals": {"$push": "$price_per_m2_uah"},
                    "price_per_ha_vals": {"$push": "$price_per_ha_uah"},
                }},
            ]
            for row in self.unified_repo.collection.aggregate(pipeline_settlement):
                city = row["_id"].get("city", "")
                region = row["_id"].get("region", "")
                if not city:
                    continue
                for metric, vals_key in [
                    ("price_uah", "price_uah_vals"),
                    ("price_per_m2_uah", "price_per_m2_vals"),
                    ("price_per_ha_uah", "price_per_ha_vals"),
                ]:
                    vals = [v for v in row.get(vals_key, []) if v is not None and v > 0]
                    if metric in country_bounds:
                        q1, _, _, q4, iqr = country_bounds[metric]
                        lo, hi = q1 - OUTLIER_IQR_MULTIPLIER * iqr, q4 + OUTLIER_IQR_MULTIPLIER * iqr
                        vals = [v for v in vals if lo <= v <= hi]
                    if len(vals) < MIN_SETTLEMENT_COUNT:
                        continue
                    q1, q2, q3, q4 = _quartiles(vals)
                    self.repo.upsert_indicator(city, region, metric, q1, q2, q3, q4, len(vals), listing_type)
                    count += 1

            # 2. Індикатори по областях (fallback)
            pipeline_region = [
                {"$match": {"source_updated_at": {"$gte": cutoff}, "status": "активне", **type_match}},
                {"$unwind": {"path": "$addresses", "preserveNullAndEmptyArrays": False}},
                {"$match": {"addresses.region": {"$exists": True, "$ne": ""}}},
                {"$group": {
                    "_id": {"$ifNull": ["$addresses.region", ""]},
                    "price_uah_vals": {"$push": "$price_uah"},
                    "price_per_m2_vals": {"$push": "$price_per_m2_uah"},
                    "price_per_ha_vals": {"$push": "$price_per_ha_uah"},
                }},
            ]
            for row in self.unified_repo.collection.aggregate(pipeline_region):
                rid = row["_id"]
                region = rid if isinstance(rid, str) else (rid.get("region", "") if isinstance(rid, dict) else "")
                if not region:
                    continue
                for metric, vals_key in [
                    ("price_uah", "price_uah_vals"),
                    ("price_per_m2_uah", "price_per_m2_vals"),
                    ("price_per_ha_uah", "price_per_ha_vals"),
                ]:
                    vals = [v for v in row.get(vals_key, []) if v is not None and v > 0]
                    if metric in country_bounds:
                        q1, _, _, q4, iqr = country_bounds[metric]
                        lo, hi = q1 - OUTLIER_IQR_MULTIPLIER * iqr, q4 + OUTLIER_IQR_MULTIPLIER * iqr
                        vals = [v for v in vals if lo <= v <= hi]
                    if len(vals) < self.MIN_INDICATOR_COUNT:
                        continue
                    q1, q2, q3, q4 = _quartiles(vals)
                    self.repo.upsert_region_indicator(region, metric, q1, q2, q3, q4, len(vals), listing_type)
                    count += 1

        return count

    def _compute_aggregates(self) -> int:
        """Обчислює агреговані метрики за днями, тижнями, місяцями (виключаємо змішані)."""
        pipeline = [
            {"$match": {
                "status": "активне",
                "$or": [
                    {"building_area_sqm": {"$not": {"$gt": 0}}},
                    {"land_area_sqm": {"$not": {"$gt": 0}}},
                ],
            }},
            {"$unwind": {"path": "$addresses", "preserveNullAndEmptyArrays": False}},
            {
                "$group": {
                    "_id": {
                        "source": "$source",
                        "property_type": {"$ifNull": ["$property_type", ""]},
                        "region": {"$ifNull": ["$addresses.region", ""]},
                        "city": {"$ifNull": ["$addresses.settlement", ""]},
                        "day": {"$dateToString": {"format": "%Y-%m-%d", "date": "$source_updated_at"}},
                        "week": {
                            "$dateToString": {
                                "format": "%G-W%V",
                                "date": "$source_updated_at",
                            }
                        },
                        "month": {"$dateToString": {"format": "%Y-%m", "date": "$source_updated_at"}},
                    },
                    "price_uah_vals": {"$push": "$price_uah"},
                    "price_usd_vals": {"$push": "$price_usd"},
                    "price_per_m2_uah_vals": {"$push": "$price_per_m2_uah"},
                    "price_per_m2_usd_vals": {"$push": "$price_per_m2_usd"},
                    "price_per_ha_uah_vals": {"$push": "$price_per_ha_uah"},
                    "price_per_ha_usd_vals": {"$push": "$price_per_ha_usd"},
                    "count": {"$sum": 1},
                }
            },
        ]
        cursor = self.unified_repo.collection.aggregate(pipeline)
        count = 0
        for row in cursor:
            g = row["_id"]
            group_by = {
                "source": g.get("source", ""),
                "property_type": g.get("property_type", ""),
                "region": g.get("region", ""),
                "city": g.get("city", ""),
            }
            for period_type, period_key in [
                ("day", g.get("day")),
                ("week", g.get("week")),
                ("month", g.get("month")),
            ]:
                if not period_key:
                    continue
                metrics = {}
                for m in METRICS:
                    vals_key = f"{m}_vals"
                    vals = [v for v in row.get(vals_key, []) if v is not None and v > 0]
                    if vals:
                        avg = sum(vals) / len(vals)
                        metrics[m] = {
                            "avg": round(avg, 2),
                            "std": round(_std_dev(vals), 2),
                            "count": len(vals),
                        }
                        q1, q2, q3, q4 = _quartiles(vals)
                        metrics[m]["q1"] = round(q1, 2)
                        metrics[m]["q2"] = round(q2, 2)
                        metrics[m]["q3"] = round(q3, 2)
                        metrics[m]["q4"] = round(q4, 2)
                if metrics:
                    doc = {
                        "period_type": period_type,
                        "period_key": period_key,
                        "group_by": group_by,
                        "metrics": metrics,
                        "count": row.get("count", 0),
                    }
                    self.repo.upsert_aggregate(doc)
                    count += 1
        return count

    MIN_INDICATOR_COUNT = 3

    def _listing_type_from_item(self, item: Dict[str, Any]) -> str:
        """Визначає тип оголошення: land, real_estate, mixed."""
        b = item.get("building_area_sqm") or 0
        l = item.get("land_area_sqm") or 0
        if (b and b > 0) and (l and l > 0):
            return LISTING_TYPE_MIXED
        if l and l > 0:
            return LISTING_TYPE_LAND
        return LISTING_TYPE_REAL_ESTATE

    def get_price_indicator(
        self,
        value: float,
        city: str,
        metric: str,
        region: Optional[str] = None,
        listing_type: str = LISTING_TYPE_REAL_ESTATE,
    ) -> Optional[Tuple[str, str]]:
        """
        Визначає індикатор ціни: "аномально низька" | "вигідна" | "середня" | "дорога" | "аномально висока" | None.

        Логіка: якщо по населеному пункту (settlement) 5+ оголошень схожого типу — використовуємо його розподіл.
        Інакше — fallback на аналітику по області.
        Returns: (indicator, source) де source = "city" | "region", або None.
        """
        if value is None or value <= 0:
            return None
        if metric not in INDICATOR_METRICS:
            return None
        ind = None
        used_source = None
        if city:
            ind = self.repo.get_indicator(city, metric, region, listing_type)
            if ind and ind.get("count", 0) >= MIN_SETTLEMENT_COUNT:
                used_source = "city"
            else:
                ind = None
            if not ind and listing_type != LISTING_TYPE_MIXED:
                ind = self.repo.get_indicator(city, metric, region, LISTING_TYPE_GENERAL)
                if ind and ind.get("count", 0) >= MIN_SETTLEMENT_COUNT:
                    used_source = "city"
                else:
                    ind = None
            if not ind:
                ind = self.repo.get_indicator(city, metric, region, LISTING_TYPE_MIXED)
                if ind and ind.get("count", 0) >= MIN_SETTLEMENT_COUNT:
                    used_source = "city"
                else:
                    ind = None
        if not ind and region:
            ind = self.repo.get_region_indicator(region, metric, listing_type)
            if ind:
                used_source = "region"
        if not ind and region and listing_type != LISTING_TYPE_MIXED:
            ind = self.repo.get_region_indicator(region, metric, LISTING_TYPE_GENERAL)
            if ind:
                used_source = "region"
        if not ind and region:
            ind = self.repo.get_region_indicator(region, metric, LISTING_TYPE_MIXED)
            if ind:
                used_source = "region"
        if not ind or ind.get("count", 0) < self.MIN_INDICATOR_COUNT:
            return None
        q1, q2, q3, q4 = ind.get("q1"), ind.get("q2"), ind.get("q3"), ind.get("q4")
        if q1 is None or q3 is None:
            return None
        q4 = q4 if q4 is not None else q3
        iqr = q3 - q1
        min_iqr = max(q2 * 0.05, 1.0) if q2 else 1.0
        iqr = max(iqr, min_iqr)
        lower_fence = q1 - 1.5 * iqr
        upper_fence = q3 + 1.5 * iqr
        bound_2_5 = (q2 + q3) / 2 if q2 is not None else q2
        bound_3_5 = (q3 + q4) / 2 if q4 is not None else q3
        if value < lower_fence:
            return ("аномально низька", used_source or "region")
        if value <= q1:
            return ("вигідна", used_source or "region")
        if bound_2_5 is not None and value <= bound_2_5:
            return ("нижче середньої", used_source or "region")
        if bound_3_5 is not None and value <= bound_3_5:
            return ("середня", used_source or "region")
        if value <= upper_fence:
            return ("вище середньої", used_source or "region")
        return ("аномально висока", used_source or "region")

    def get_price_indicators_for_items(
        self,
        items: List[Dict[str, Any]],
    ) -> Dict[str, Dict[str, str]]:
        """
        Повертає індикатори для списку оголошень.
        items мають містити: city/settlement, region, price_uah, price_per_m2_uah, price_per_ha_uah,
        building_area_sqm, land_area_sqm (для визначення типу).
        Логіка: settlement 5+ → локальний розподіл, інакше — область.
        Returns: composite_id -> {indicator, source} де source = "city" | "region"
        """
        result = {}
        for item in items:
            city = item.get("city") or item.get("settlement")
            addrs = item.get("addresses") or []
            if not city and addrs:
                addr = addrs[0] if isinstance(addrs[0], dict) else {}
                city = addr.get("settlement")
            region = item.get("region")
            if region is None and addrs:
                addr = addrs[0] if isinstance(addrs[0], dict) else {}
                region = addr.get("region")

            price = item.get("price_uah") or item.get("price")
            ppm2 = item.get("price_per_m2_uah")
            ppha = item.get("price_per_ha_uah")

            metric = None
            value = None
            if ppm2 and ppm2 > 0:
                metric, value = "price_per_m2_uah", ppm2
            elif ppha and ppha > 0:
                metric, value = "price_per_ha_uah", ppha
            elif price and price > 0:
                metric, value = "price_uah", price

            if metric and value:
                listing_type = self._listing_type_from_item(item)
                res = self.get_price_indicator(value, city or "", metric, region, listing_type)
                if res:
                    ind_val, ind_source = res
                    composite_id = f"{item.get('source', '')}:{item.get('source_id', '')}"
                    result[composite_id] = {"indicator": ind_val, "source": ind_source}
        return result

    def get_aggregated_analytics(
        self,
        period_type: str,
        period_key: Optional[str] = None,
        source: Optional[str] = None,
        property_type: Optional[str] = None,
        region: Optional[str] = None,
        city: Optional[str] = None,
    ) -> List[Dict[str, Any]]:
        """Отримує агреговану аналітику з фільтрами."""
        return self.repo.get_aggregates(
            period_type=period_type,
            period_key=period_key,
            source=source,
            property_type=property_type,
            region=region,
            city=city,
        )
