# -*- coding: utf-8 -*-
"""
Доменний сервіс для колекції analytics_extracts.

Надає методи агрегації по метриках, пошуку з логічними умовами,
створення похідних метрик. Доступний через MCP для агентів.
"""

import logging
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Union

from data.database.connection import MongoDBConnection
from data.repositories.analytics_extracts_repository import AnalyticsExtractsRepository

logger = logging.getLogger(__name__)

# Метрики для агрегації (числові поля)
AGGREGATABLE_METRICS = [
    "price_per_m2_uah",
    "price_per_m2_usd",
    "price_per_ha_uah",
    "price_per_ha_usd",
    "building_area_sqm",
    "land_area_sqm",
    "price_uah",
    "price_usd",
]

# Поля для групування (виміри)
GROUP_BY_FIELDS = [
    "source",
    "property_type",
    "region",
    "oblast_raion",
    "settlement_type",
    "settlement",
    "city",
    "city_district",
    "street_type",
    "street",
    "building",
    "floor",
]

# Оператори для фільтрів (логічні умови)
FILTER_OPERATORS = {
    "eq": "$eq",
    "ne": "$ne",
    "gt": "$gt",
    "gte": "$gte",
    "lt": "$lt",
    "lte": "$lte",
    "in": "$in",
    "nin": "$nin",
    "regex": "$regex",
    "exists": "$exists",
}


def _build_mongo_filter(conditions: List[Dict[str, Any]]) -> Dict[str, Any]:
    """
    Перетворює список умов у MongoDB query.

    Умови підтримують логічні оператори (AND, OR, NOT):
    - {"field": "region", "operator": "eq", "value": "Київська"}
    - {"field": "price_per_m2_uah", "operator": "gt", "value": 10000}
    - {"$or": [{"field": "city", "operator": "eq", "value": "Київ"}, {"field": "region", "operator": "eq", "value": "Київська"}]}
    - {"$and": [...]}
    """
    if not conditions:
        return {}

    def _single_condition(c: Dict[str, Any]) -> Dict[str, Any]:
        if "$or" in c:
            items = [_single_condition(x) for x in c["$or"]]
            return {"$or": items}
        if "$and" in c:
            items = [_single_condition(x) for x in c["$and"]]
            return {"$and": items}
        if "$not" in c:
            return {"$nor": [_single_condition(c["$not"])]}

        field = c.get("field")
        op = c.get("operator", "eq")
        value = c.get("value")

        if not field:
            return {}

        mongo_op = FILTER_OPERATORS.get(op, "$eq")
        if op == "exists":
            return {field: {mongo_op: bool(value)}}
        if op == "regex":
            return {field: {mongo_op: value, "$options": "i"}}
        return {field: {mongo_op: value}}

    if len(conditions) == 1 and not any(k.startswith("$") for k in conditions[0]):
        return _single_condition(conditions[0])

    mongo_items = []
    for c in conditions:
        q = _single_condition(c) if isinstance(c, dict) else c
        if q:
            mongo_items.append(q)
    return {"$and": mongo_items} if mongo_items else {}


def _build_filter_from_simple(filters: Dict[str, Any]) -> Dict[str, Any]:
    """
    Простий формат: {"region": "Київська", "city": "Київ", "price_per_m2_uah": {"$gte": 5000}}.
    """
    return dict(filters) if filters else {}


class AnalyticsExtractsService:
    """
    Доменний сервіс для analytics_extracts.

    Методи:
    - aggregate_by_metric: агрегація по метриці з групуванням
    - aggregate_avg, aggregate_sum, aggregate_min, aggregate_max
    - search: пошук з логічними умовами
    - get_distinct_values: унікальні значення поля
    """

    def __init__(self):
        self.repo = AnalyticsExtractsRepository()

    def _get_collection(self):
        return self.repo.collection

    def aggregate_by_metric(
        self,
        metric: str,
        aggregation: str = "avg",
        group_by: Optional[List[str]] = None,
        filters: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """
        Агрегує за метрикою з групуванням.

        Args:
            metric: Назва метрики (price_per_m2_uah, price_per_ha_uah, тощо)
            aggregation: avg | sum | min | max | count
            group_by: Список полів для групування (region, city, city_district, тощо)
            filters: Фільтри (простий dict або список умов з логікою)
            limit: Максимум документів у відповіді

        Returns:
            {"success": True, "results": [...], "metric": "...", "aggregation": "..."}
        """
        if metric not in AGGREGATABLE_METRICS and metric not in ("count",):
            return {
                "success": False,
                "error": f"Метрика '{metric}' не підтримується. Доступні: {', '.join(AGGREGATABLE_METRICS)}",
            }

        if isinstance(filters, list):
            match_stage = _build_mongo_filter(filters)
        else:
            match_stage = _build_filter_from_simple(filters or {})

        # Виключаємо документи з null/0 для числових метрик
        if metric in AGGREGATABLE_METRICS and metric != "count":
            metric_filter = {metric: {"$exists": True, "$gt": 0}}
            if match_stage:
                match_stage = {"$and": [match_stage, metric_filter]}
            else:
                match_stage = metric_filter

        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})

        group_id = None
        if group_by:
            gid = {}
            for f in group_by:
                if f in GROUP_BY_FIELDS:
                    gid[f] = {"$ifNull": [f"${f}", ""]}
                elif f in AGGREGATABLE_METRICS:
                    gid[f] = {"$ifNull": [f"${f}", 0]}
            if gid:
                group_id = gid

        if metric == "count":
            agg_expr = {"$sum": 1}
        elif aggregation == "avg":
            agg_expr = {"$avg": f"${metric}"}
        else:
            agg_expr = {"$" + aggregation: f"${metric}"}

        pipeline.append({"$group": {"_id": group_id, "value": agg_expr, "count": {"$sum": 1}}})
        pipeline.append({"$sort": {"value": -1}})
        pipeline.append({"$limit": limit})

        try:
            cursor = self._get_collection().aggregate(pipeline)
            results = [{"_id": r["_id"], "value": round(r["value"], 2) if isinstance(r["value"], (int, float)) else r["value"], "count": r["count"]} for r in cursor]
            return {
                "success": True,
                "results": results,
                "metric": metric,
                "aggregation": aggregation,
            }
        except Exception as e:
            logger.exception("aggregate_by_metric: %s", e)
            return {"success": False, "error": str(e)}

    def aggregate_avg(
        self,
        metric: str,
        group_by: Optional[List[str]] = None,
        filters: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Середнє значення метрики з групуванням."""
        return self.aggregate_by_metric(metric, "avg", group_by, filters, limit)

    def aggregate_sum(
        self,
        metric: str,
        group_by: Optional[List[str]] = None,
        filters: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Сума метрики з групуванням."""
        return self.aggregate_by_metric(metric, "sum", group_by, filters, limit)

    def aggregate_min(
        self,
        metric: str,
        group_by: Optional[List[str]] = None,
        filters: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Мінімальне значення метрики з групуванням."""
        return self.aggregate_by_metric(metric, "min", group_by, filters, limit)

    def aggregate_max(
        self,
        metric: str,
        group_by: Optional[List[str]] = None,
        filters: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        limit: int = 100,
    ) -> Dict[str, Any]:
        """Максимальне значення метрики з групуванням."""
        return self.aggregate_by_metric(metric, "max", group_by, filters, limit)

    def search(
        self,
        filters: Optional[Union[Dict[str, Any], List[Dict[str, Any]]]] = None,
        fields: Optional[List[str]] = None,
        sort: Optional[List[Dict[str, str]]] = None,
        limit: int = 100,
        skip: int = 0,
    ) -> Dict[str, Any]:
        """
        Пошук з логічними умовами.

        filters: простий dict або список умов з операторами (eq, gt, ne, in, $or, $and).
        sort: [{"field": "price_per_m2_uah", "order": -1}]
        """
        if isinstance(filters, list):
            match_stage = _build_mongo_filter(filters)
        else:
            match_stage = _build_filter_from_simple(filters or {})

        pipeline = []
        if match_stage:
            pipeline.append({"$match": match_stage})

        if fields:
            project = {f: 1 for f in fields}
            project["_id"] = 0
            pipeline.append({"$project": project})

        if sort:
            sort_dict = {}
            for s in sort:
                sort_dict[s["field"]] = s.get("order", 1)
            pipeline.append({"$sort": sort_dict})

        pipeline.append({"$skip": skip})
        pipeline.append({"$limit": limit})

        try:
            cursor = self._get_collection().aggregate(pipeline)
            results = list(cursor)
            return {"success": True, "results": results, "count": len(results)}
        except Exception as e:
            logger.exception("search: %s", e)
            return {"success": False, "error": str(e)}

    def get_distinct_values(self, field: str, filters: Optional[Dict[str, Any]] = None) -> Dict[str, Any]:
        """Унікальні значення поля (для дослідження перед фільтрацією)."""
        match = _build_filter_from_simple(filters or {})
        pipeline = [{"$match": match},
                    {"$group": {"_id": f"${field}"}},
                    {"$sort": {"_id": 1}},
                    {"$limit": 500}]
        try:
            cursor = self._get_collection().aggregate(pipeline)
            values = [r["_id"] for r in cursor if r["_id"] is not None]
            return {"success": True, "field": field, "values": values}
        except Exception as e:
            return {"success": False, "error": str(e)}

    def get_available_metrics(self) -> List[str]:
        """Список доступних метрик для агрегації."""
        return list(AGGREGATABLE_METRICS)

    def get_group_by_fields(self) -> List[str]:
        """Список полів для групування."""
        return list(GROUP_BY_FIELDS)
