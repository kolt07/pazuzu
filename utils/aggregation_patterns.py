# -*- coding: utf-8 -*-
"""
Каталог аналітичних патернів: intent → частина pipeline.
Кожна функція приймає параметри з Analysis Intent і повертає список stages або повний pipeline.
"""

from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional

from utils.schema_filter_resolver import resolve_geo_filter

# Маппінг логічних полів на шляхи в документі по entity
FIELD_BY_ENTITY = {
    "olx_listings": {
        "price": "search_data.price",
        "count": None,  # рахуємо документи
        "updated_at": "updated_at",
        "date": "updated_at",
        "location": "search_data.location",
        "region": "detail.address_refs.region.name",
        "city": "detail.address_refs.city.name",
        "property_type": "detail.llm.property_type",
    },
    "prozorro_auctions": {
        "price": "auction_data.value.amount",
        "count": None,
        "updated_at": "auction_data.dateModified",
        "date": "auction_data.dateModified",
        "location": "auction_data.address_refs",
        "region": "auction_data.address_refs.region.name",
        "city": "auction_data.address_refs.city.name",
        "property_type": "llm_result.result.property_type",
    },
}


def _date_range_for_time_range(time_range: Optional[str], entity: str) -> Optional[Dict[str, Any]]:
    """Повертає $match умову за датою для entity (значення в ISO або BSON залежно від колекції)."""
    if not time_range:
        return None
    now = datetime.now(timezone.utc)
    if time_range == "last_1_day":
        days = 1
    elif time_range == "last_7_days":
        days = 7
    elif time_range == "last_30_days":
        days = 30
    else:
        return None
    start = (now - timedelta(days=days)).replace(hour=0, minute=0, second=0, microsecond=0)
    end = now
    date_field = FIELD_BY_ENTITY.get(entity, {}).get("updated_at") or FIELD_BY_ENTITY.get(entity, {}).get("date")
    if not date_field:
        return None
    if entity == "olx_listings":
        return {date_field: {"$gte": start, "$lte": end}}
    if entity == "prozorro_auctions":
        start_s = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end_s = end.strftime("%Y-%m-%dT23:59:59.999Z")
        return {date_field: {"$gte": start_s, "$lte": end_s}}
    return None


def _merge_match_conditions(*conditions: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Об'єднує кілька $match умов в один словник (перетин)."""
    out: Dict[str, Any] = {}
    for c in conditions:
        if not c or not isinstance(c, dict):
            continue
        for k, v in c.items():
            if k in out:
                if k == "$or":
                    if isinstance(out[k], list) and isinstance(v, list):
                        out[k] = out[k] + v
                    else:
                        out[k] = [out[k], v]
                else:
                    out.setdefault("$and", []).append({k: v})
            else:
                out[k] = v
    if "$and" in out and len(out) == 1:
        return out["$and"][0] if len(out["$and"]) == 1 else {"$and": out["$and"]}
    return out


def _field_path(entity: str, logical_field: str) -> Optional[str]:
    """Повертає шлях до поля в документі для entity."""
    return FIELD_BY_ENTITY.get(entity, {}).get(logical_field)


def build_top_n(
    entity: str,
    dimensions: List[str],
    filters: Dict[str, Any],
    metric_spec: Dict[str, Any],
    time_range: Optional[str],
) -> List[Dict[str, Any]]:
    """
    Топ-N за полем (наприклад топ-10 найдорожчих). Повертає pipeline stages.
    metric_spec: field, aggregation="top", order, limit.
    """
    field = metric_spec.get("field") or "price"
    order = 1 if (metric_spec.get("order") or "desc").lower() == "asc" else -1
    limit = metric_spec.get("limit") or 10
    path = _field_path(entity, field)
    if not path and field != "count":
        path = FIELD_BY_ENTITY.get(entity, {}).get("price")

    stages: List[Dict[str, Any]] = []

    date_match = _date_range_for_time_range(time_range, entity)
    geo_match = resolve_geo_filter(filters or {}, entity)
    match_cond = _merge_match_conditions(date_match, geo_match)
    if match_cond:
        stages.append({"$match": match_cond})

    if path:
        stages.append({"$match": {path: {"$exists": True, "$ne": None}}})
        stages.append({"$sort": {path: order}})
    else:
        stages.append({"$sort": {"_id": order}})
    stages.append({"$limit": limit})
    return stages


def build_count(
    entity: str,
    dimensions: List[str],
    filters: Dict[str, Any],
    metric_spec: Dict[str, Any],
    time_range: Optional[str],
) -> List[Dict[str, Any]]:
    """Підрахунок документів (з опційним групуванням за dimensions)."""
    stages: List[Dict[str, Any]] = []
    date_match = _date_range_for_time_range(time_range, entity)
    geo_match = resolve_geo_filter(filters or {}, entity)
    match_cond = _merge_match_conditions(date_match, geo_match)
    if match_cond:
        stages.append({"$match": match_cond})

    if dimensions:
        group_id: Dict[str, Any] = {}
        for dim in dimensions:
            path = _field_path(entity, dim)
            if path:
                group_id[dim] = f"${path}"
            else:
                group_id[dim] = "$null"
        stages.append({"$group": {"_id": group_id, "count": {"$sum": 1}}})
        stages.append({"$sort": {"count": -1}})
    else:
        stages.append({"$count": "count"})
    return stages


def build_avg(
    entity: str,
    dimensions: List[str],
    filters: Dict[str, Any],
    metric_spec: Dict[str, Any],
    time_range: Optional[str],
) -> List[Dict[str, Any]]:
    """Середнє за полем (наприклад середня ціна), опційно по dimensions."""
    field = metric_spec.get("field") or "price"
    path = _field_path(entity, field)
    if not path:
        path = FIELD_BY_ENTITY.get(entity, {}).get("price")
    if not path:
        return []

    stages: List[Dict[str, Any]] = []
    date_match = _date_range_for_time_range(time_range, entity)
    geo_match = resolve_geo_filter(filters or {}, entity)
    match_cond = _merge_match_conditions(date_match, geo_match)
    if match_cond:
        stages.append({"$match": match_cond})
    stages.append({"$match": {path: {"$exists": True, "$ne": None, "$type": "number"}}})

    group_id: Dict[str, Any] = {}
    if dimensions:
        for dim in dimensions:
            dim_path = _field_path(entity, dim)
            if dim_path:
                group_id[dim] = f"${dim_path}"
            else:
                group_id[dim] = "$null"
    else:
        group_id = None

    if group_id is not None:
        stages.append({
            "$group": {
                "_id": group_id,
                "avg": {"$avg": f"${path}"},
                "count": {"$sum": 1},
            }
        })
        stages.append({"$sort": {"avg": -1}})
    else:
        stages.append({
            "$group": {
                "_id": None,
                "avg": {"$avg": f"${path}"},
                "count": {"$sum": 1},
            }
        })
    return stages


def build_histogram(
    entity: str,
    dimensions: List[str],
    filters: Dict[str, Any],
    metric_spec: Dict[str, Any],
    time_range: Optional[str],
) -> List[Dict[str, Any]]:
    """Розподіл (групування за полем)."""
    field = metric_spec.get("field") or "price"
    path = _field_path(entity, field)
    if not path:
        path = FIELD_BY_ENTITY.get(entity, {}).get("price")
    if not path:
        return []

    stages: List[Dict[str, Any]] = []
    date_match = _date_range_for_time_range(time_range, entity)
    geo_match = resolve_geo_filter(filters or {}, entity)
    match_cond = _merge_match_conditions(date_match, geo_match)
    if match_cond:
        stages.append({"$match": match_cond})
    stages.append({"$group": {"_id": f"${path}", "count": {"$sum": 1}}})
    stages.append({"$sort": {"count": -1}})
    limit = metric_spec.get("limit") or 50
    stages.append({"$limit": limit})
    return stages


def build_time_series(
    entity: str,
    dimensions: List[str],
    filters: Dict[str, Any],
    metric_spec: Dict[str, Any],
    time_range: Optional[str],
) -> List[Dict[str, Any]]:
    """Тренд по часу (групування за датою, опційно середнє за полем)."""
    date_field = _field_path(entity, "date") or _field_path(entity, "updated_at")
    if not date_field:
        return []

    stages: List[Dict[str, Any]] = []
    date_match = _date_range_for_time_range(time_range, entity)
    geo_match = resolve_geo_filter(filters or {}, entity)
    match_cond = _merge_match_conditions(date_match, geo_match)
    if match_cond:
        stages.append({"$match": match_cond})

    if entity == "olx_listings":
        stages.append({
            "$addFields": {
                "_date_str": {"$dateToString": {"format": "%Y-%m-%d", "date": f"${date_field}"}}
            }
        })
        group_date = "$_date_str"
    else:
        stages.append({
            "$addFields": {
                "_date_str": {"$substr": [f"${date_field}", 0, 10]}
            }
        })
        group_date = "$_date_str"

    field = metric_spec.get("field") or "price"
    path = _field_path(entity, field)
    if path:
        stages.append({
            "$group": {
                "_id": group_date,
                "count": {"$sum": 1},
                "avg": {"$avg": f"${path}"},
            }
        })
    else:
        stages.append({
            "$group": {
                "_id": group_date,
                "count": {"$sum": 1},
            }
        })
    stages.append({"$sort": {"_id": 1}})
    return stages


def build_sum(
    entity: str,
    dimensions: List[str],
    filters: Dict[str, Any],
    metric_spec: Dict[str, Any],
    time_range: Optional[str],
) -> List[Dict[str, Any]]:
    """Сума за полем, опційно по dimensions."""
    field = metric_spec.get("field") or "price"
    path = _field_path(entity, field)
    if not path:
        path = FIELD_BY_ENTITY.get(entity, {}).get("price")
    if not path:
        return []

    stages: List[Dict[str, Any]] = []
    date_match = _date_range_for_time_range(time_range, entity)
    geo_match = resolve_geo_filter(filters or {}, entity)
    match_cond = _merge_match_conditions(date_match, geo_match)
    if match_cond:
        stages.append({"$match": match_cond})
    stages.append({"$match": {path: {"$exists": True, "$ne": None, "$type": "number"}}})

    group_id: Dict[str, Any] = {}
    if dimensions:
        for dim in dimensions:
            dim_path = _field_path(entity, dim)
            if dim_path:
                group_id[dim] = f"${dim_path}"
            else:
                group_id[dim] = "$null"
    else:
        group_id = None

    if group_id is not None:
        stages.append({
            "$group": {
                "_id": group_id,
                "sum": {"$sum": f"${path}"},
                "count": {"$sum": 1},
            }
        })
        stages.append({"$sort": {"sum": -1}})
    else:
        stages.append({
            "$group": {
                "_id": None,
                "sum": {"$sum": f"${path}"},
                "count": {"$sum": 1},
            }
        })
    return stages


AGGREGATION_PATTERNS = {
    "top": build_top_n,
    "count": build_count,
    "avg": build_avg,
    "sum": build_sum,
    "distribution": build_histogram,
    "trend": build_time_series,
}
