# -*- coding: utf-8 -*-
"""
PipelineExecutor: виконання пайплайнів виключно через методи домен-шару (CollectionManager).
Жодних прямих операцій з MongoDB — тільки через CollectionManager.find(), трансформації DataFrame.
"""

import logging
from typing import Any, Dict, List, Optional

import pandas as pd

from domain.managers.collection_manager import (
    BaseCollectionManager,
    UnifiedListingsCollectionManager,
    ListingAnalyticsCollectionManager,
    RealEstateObjectsCollectionManager,
)
from domain.models.filter_models import (
    FilterElement,
    FilterGroup,
    FilterGroupType,
    FilterOperator,
    FindQuery,
)
from domain.services.geo_filter_service import GeoFilterService
from utils.source_field_mapper import SourceFieldMapper

logger = logging.getLogger(__name__)

# Поле сортування за замовчуванням (від нового до старого)
DEFAULT_SORT_FIELD = {
    "unified_listings": "source_updated_at",
    "prozorro_auctions": "auction_data.dateModified",
    "olx_listings": "updated_at",
    "listing_analytics": "analysis_at",
    "real_estate_objects": "updated_at",
}
# Фільтр за замовчуванням: лише активні аукціони (якщо користувач не вказав статус)
DEFAULT_STATUS_ACTIVE = "активне"  # unified_listings

# Маппінг логічних полів для агрегації (group_by, metrics) на фізичні колонки DataFrame.
# Для addresses.region — потрібно витягнути з масиву (спеціальна обробка в _prepare_df_for_aggregate).
AGGREGATE_FIELD_MAP = {
    "unified_listings": {
        "region": "region",  # витягується з addresses
        "city": "settlement",  # витягується з addresses
        "property_type": "property_type",
        "price": "price_uah",
        "price_per_m2": "price_per_m2_uah",
        "price_per_m2_uah": "price_per_m2_uah",
        "average_price_per_sqm": "price_per_m2_uah",
        "building_area_sqm": "building_area_sqm",
    },
}

# Маппінг логічних полів сортування/фільтрації на фізичні.
# Для unified_listings: price завжди означає price_uah (ціна в грн), якщо не вказано інакше.
SORT_FIELD_MAP = {
    "unified_listings": {
        "price": "price_uah",
        "price_per_sqm": "price_per_m2_uah",
        "price_per_m2": "price_per_m2_uah",
        "average_price_per_sqm": "price_per_m2_uah",
        "price_per_ha": "price_per_ha_uah",
    },
    "prozorro_auctions": {"price": "auction_data.value.amount"},
    "olx_listings": {"price": "search_data.price"},
    "listing_analytics": {"date": "analysis_at"},
    "real_estate_objects": {"type": "type", "area": "area_sqm"},
}


def get_collection_manager(collection: str) -> Optional[BaseCollectionManager]:
    """Повертає CollectionManager для колекції."""
    if collection == "unified_listings":
        return UnifiedListingsCollectionManager()
    if collection == "listing_analytics":
        return ListingAnalyticsCollectionManager()
    if collection == "real_estate_objects":
        return RealEstateObjectsCollectionManager()
    # Prozorro та Olx — поки що делегуємо на unified_listings якщо джерело зведено
    return None


def execute_pipeline(
    steps: List[Dict[str, Any]],
    collection: str,
    parameters: Optional[Dict[str, Any]] = None,
) -> Dict[str, Any]:
    """
    Виконує пайплайн через методи домен-шару.
    
    Returns:
        {"success": bool, "results": list, "count": int, "diagnostic_info": dict, "error": str?}
    """
    diagnostic_info = {"pipeline_stages": len(steps), "result_count": 0}
    
    mgr = get_collection_manager(collection)
    if not mgr:
        return {
            "success": False,
            "error": f"Колекція {collection} не підтримується через домен-шар",
            "results": [],
            "count": 0,
            "diagnostic_info": diagnostic_info,
        }
    
    # Підставляємо параметри
    _apply_parameters(steps, parameters or {})
    
    # Збираємо умови з кроків
    all_conditions = {}
    geo_filters_dict = {}
    sort_spec = []
    limit_val = None
    calculate_steps = []
    aggregate_step = None
    
    # city/region/exclude_city — геофільтр; $in/list → звичайні умови (addresses.$elemMatch)
    GEO_KEYS = frozenset(("city", "region", "exclude_city", "addresses.settlement", "addresses.region"))

    for step in steps:
        st = step.get("type")
        if st == "filter":
            conds = step.get("conditions") or step.get("filter_metrics") or step.get("filters") or {}
            for k, v in conds.items():
                if k == "exclude_city" and isinstance(v, str) and not v.startswith("$") and not (v.startswith("{{") and v.endswith("}}")):
                    geo_filters_dict["exclude_city"] = v
                    continue
                if k in GEO_KEYS:
                    # GeoFilterService.from_dict приймає лише string — city/region з $in/in або list йдуть в all_conditions
                    v_list = None
                    if isinstance(v, dict):
                        v_list = v.get("$in") or v.get("in")
                    elif isinstance(v, list):
                        v_list = v
                    if v_list is not None and isinstance(v_list, list):
                        logical_key = "city" if k in ("city", "addresses.settlement") else "region"
                        all_conditions[logical_key] = v_list
                    elif isinstance(v, str) and not v.startswith("$") and not (v.startswith("{{") and v.endswith("}}")):
                        geo_key = "city" if k in ("city", "addresses.settlement") else "region"
                        if geo_key not in geo_filters_dict or k in ("city", "region"):
                            geo_filters_dict[geo_key] = v
                    else:
                        all_conditions[k] = v
                else:
                    all_conditions[k] = v
        elif st == "sort":
            for s in step.get("fields") or step.get("sort") or []:
                if isinstance(s, dict) and (s.get("field") or s.get("field_path")):
                    sort_spec.append(s)
        elif st == "limit":
            limit_val = step.get("limit") or step.get("count")
        elif st == "calculate":
            calculate_steps.append(step)
        elif st == "aggregate":
            aggregate_step = step
    
    # За замовчуванням — лише активні аукціони (якщо користувач не вказав статус)
    if "status" not in all_conditions and collection == "unified_listings":
        all_conditions["status"] = DEFAULT_STATUS_ACTIVE

    # Будуємо FindQuery
    filter_group = _build_filter_group(all_conditions, collection)
    # Пропускаємо гео-параметри, що не підставлені
    geo_filters_clean = {
        k: v for k, v in geo_filters_dict.items()
        if isinstance(v, str) and not v.startswith("$") and not (v.startswith("{{") and v.endswith("}}"))
    }
    geo_filter = GeoFilterService().from_dict(geo_filters_clean) if geo_filters_clean else None

    # applied_filters_count для ResultValidator (filter consistency check)
    applied_filters_count = (len(filter_group.items) if filter_group else 0) + len(geo_filters_clean)
    diagnostic_info["applied_filters_count"] = applied_filters_count
    
    # Маппінг полів сортування; за замовчуванням — від нового до старого
    # Для unified_listings: сортування за датою = source_updated_at (дата в джерелі); при однаковій — system_updated_at (дата в БД)
    sort_mapped = []
    for s in sort_spec:
        field = s.get("field") or s.get("field_path")
        if field:
            phys = SORT_FIELD_MAP.get(collection, {}).get(field) or SourceFieldMapper.get_field_path(field, collection)
            sort_mapped.append({"field": phys, "order": s.get("order", s.get("direction", -1))})
    if not sort_mapped and collection in DEFAULT_SORT_FIELD:
        primary = DEFAULT_SORT_FIELD[collection]
        sort_mapped = [{"field": primary, "order": -1}]
    if sort_mapped and collection == "unified_listings" and sort_mapped[0].get("field") == "source_updated_at":
        if len(sort_mapped) == 1 or sort_mapped[1].get("field") != "system_updated_at":
            sort_mapped.append({"field": "system_updated_at", "order": sort_mapped[0].get("order", -1)})
    
    try:
        limit_int = int(limit_val) if limit_val is not None else None
    except (TypeError, ValueError):
        limit_int = None

    # Якщо є aggregate — не обмежуємо find(), інакше агрегація буде на обрізаній вибірці
    if aggregate_step:
        limit_int = None

    query = FindQuery(
        filters=filter_group,
        geo_filters=geo_filter,
        sort=sort_mapped if sort_mapped and not aggregate_step else None,
        limit=limit_int,
        collection=collection,
    )
    
    try:
        df = mgr.find(query)
        
        # Крок calculate — додаємо обчислені поля до DataFrame
        for calc in calculate_steps:
            metric = calc.get("metric")
            if metric == "bids_count" and collection == "prozorro_auctions":
                if "auction_data.bids" in str(df.columns):
                    # Якщо є вкладена структура
                    pass
                # Для unified_listings bids_count може бути вже в даних
            elif metric and metric in df.columns:
                pass  # Вже є
        
        # Крок aggregate — групування з метриками (avg, sum, min, max, count)
        if aggregate_step and not df.empty:
            df = _apply_aggregate_step(df, aggregate_step, collection)
            # Після агрегації — сортування та limit за замовчуванням
            if sort_mapped and not df.empty:
                sort_col = sort_mapped[0].get("field")
                order = sort_mapped[0].get("order", -1)
                if sort_col and sort_col in df.columns:
                    pass
                elif sort_col and "value" in df.columns and sort_col in ("price_per_m2_uah", "average_price_per_sqm"):
                    sort_col = "value"
                if sort_col and sort_col in df.columns:
                    df = df.sort_values(sort_col, ascending=(order == 1 or order == "asc"))
            if limit_val is not None and not df.empty:
                try:
                    n = int(limit_val)
                    df = df.head(n)
                except (TypeError, ValueError):
                    pass

        data = df.to_dict(orient="records") if not df.empty else []
        
        # Діагностика через CollectionManager (без прямого Mongo)
        try:
            total = mgr.get_total_count() if hasattr(mgr, "get_total_count") else None
            diagnostic_info["total_documents_in_collection"] = total
            diagnostic_info["addresses_available"] = _check_addresses_available(mgr, collection)
        except Exception as e:
            logger.debug("PipelineExecutor: diagnostic info: %s", e)
        
        diagnostic_info["result_count"] = len(data)
        diagnostic_info["execution_path"] = "domain_layer"
        
        return {
            "success": True,
            "results": data,
            "count": len(data),
            "diagnostic_info": diagnostic_info,
        }
    except Exception as e:
        logger.exception("PipelineExecutor: %s", e)
        return {
            "success": False,
            "error": str(e),
            "results": [],
            "count": 0,
            "diagnostic_info": diagnostic_info,
        }


def _apply_aggregate_step(df: pd.DataFrame, step: Dict[str, Any], collection: str) -> pd.DataFrame:
    """
    Застосовує крок агрегації: group_by + metrics (avg, sum, min, max, count).
    Логічні поля мапляться на фізичні колонки DataFrame.
    """
    group_by = step.get("group_by") or []
    metrics = step.get("metrics") or []
    if not group_by and not metrics:
        return df

    agg_map = AGGREGATE_FIELD_MAP.get(collection, {})
    group_cols = []
    for g in group_by:
        phys = agg_map.get(g, g)
        group_cols.append(phys)

    def _first_addr_field(addrs, key):
        if not isinstance(addrs, list) or not addrs:
            return None
        first = addrs[0]
        return first.get(key) if isinstance(first, dict) else None

    # Підготовка: витягуємо region/settlement з addresses для unified_listings
    if collection == "unified_listings" and "addresses" in df.columns:
        df = df.copy()
        if ("region" in group_cols or "region" in group_by) and "region" not in df.columns:
            df["region"] = df["addresses"].apply(lambda x: _first_addr_field(x, "region"))
        if ("settlement" in group_cols or "city" in group_by) and "settlement" not in df.columns:
            df["settlement"] = df["addresses"].apply(lambda x: _first_addr_field(x, "settlement"))

    # Перевіряємо наявність колонок для group_by
    available = set(df.columns)
    group_cols_ok = [c for c in group_cols if c in available]
    if not group_cols_ok and group_cols:
        logger.warning("PipelineExecutor: aggregate group_by columns %s not found in df", group_cols)
        return df
    # Виключаємо рядки з порожніми значеннями групування
    df = df.dropna(subset=group_cols_ok)

    agg_funcs = {"avg": "mean", "sum": "sum", "min": "min", "max": "max", "count": "count"}
    agg_dict = {}
    for m in metrics:
        if isinstance(m, dict):
            field = m.get("field")
            agg = (m.get("aggregation") or "avg").lower()
        else:
            field = str(m)
            agg = "avg"
        if not field:
            continue
        phys = agg_map.get(field, field)
        if phys in available:
            func = agg_funcs.get(agg, "mean")
            agg_dict[phys] = func

    if not group_cols_ok:
        return df

    try:
        if agg_dict:
            result = df.groupby(group_cols_ok, dropna=False).agg(agg_dict).reset_index()
            # Перейменуємо колонки метрик для зрозумілості (avg -> value для першої метрики)
            if len(agg_dict) == 1 and not result.empty:
                col = list(agg_dict.keys())[0]
                if col in result.columns:
                    first_val = result[col].iloc[0]
                    if isinstance(first_val, (int, float)):
                        result = result.rename(columns={col: "value"})
        else:
            result = df.groupby(group_cols_ok, dropna=False).agg("first").reset_index()
        return result
    except Exception as e:
        logger.warning("PipelineExecutor: aggregate step failed: %s", e)
        return df


def _apply_parameters(steps: List[Dict], params: Dict) -> None:
    """Рекурсивно підставляє параметри в кроки."""
    for step in steps:
        for key, value in list(step.items()):
            if isinstance(value, str) and value.startswith("$") and len(value) > 1:
                p = value[1:]
                if p in params:
                    step[key] = params[p]
            elif isinstance(value, dict):
                _apply_parameters([value], params)
            elif isinstance(value, list):
                for i, item in enumerate(value):
                    if isinstance(item, dict):
                        _apply_parameters([item], params)
                    elif isinstance(item, str) and item.startswith("$") and item[1:] in params:
                        value[i] = params[item[1:]]


def _contains_template(value: Any) -> bool:
    """Перевіряє, чи значення містить непідставлений шаблон {{...}}."""
    if isinstance(value, str):
        return "{{" in value and "}}" in value
    if isinstance(value, dict):
        return any(_contains_template(v) for v in value.values())
    return False


def _build_filter_group(conditions: Dict[str, Any], collection: str) -> Optional[FilterGroup]:
    """Будує FilterGroup з умов. Мапить логічні поля на фізичні шляхи."""
    elements = []
    for field, value in conditions.items():
        if value is None:
            continue
        if isinstance(value, str) and (value.startswith("$") or (value.startswith("{{") and value.endswith("}}"))):
            continue  # Параметр не підставлений
        if _contains_template(value):
            continue  # Шаблон не резолвлений
        phys_field = SourceFieldMapper.get_field_path(field, collection)
        if isinstance(value, list):
            if value:
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.IN, value=value))
        elif isinstance(value, dict):
            # Підтримка $in / in для city/region (список міст/областей)
            in_list = value.get("$in") or value.get("in")
            if isinstance(in_list, list) and in_list:
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.IN, value=in_list))
            elif "eq" in value:
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.EQ, value=value["eq"]))
            elif "gte" in value and "lte" in value:
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.GTE, value=value["gte"]))
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.LTE, value=value["lte"]))
            elif "gte" in value:
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.GTE, value=value["gte"]))
            elif "lte" in value:
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.LTE, value=value["lte"]))
            elif "gt" in value:
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.GT, value=value["gt"]))
            elif "lt" in value:
                elements.append(FilterElement(field=phys_field, operator=FilterOperator.LT, value=value["lt"]))
        elif value != "":
            elements.append(FilterElement(field=phys_field, operator=FilterOperator.EQ, value=value))
    if not elements:
        return None
    return FilterGroup(group_type=FilterGroupType.AND, items=elements)


def _check_addresses_available(mgr: BaseCollectionManager, collection: str) -> Optional[bool]:
    """Перевіряє наявність полів адрес через структурі (без Mongo)."""
    try:
        struct = mgr.get_field_structure()
        fields_str = str(struct.get("fields", {}))
        if collection == "unified_listings":
            return "addresses" in fields_str
        if collection == "prozorro_auctions":
            return "address_refs" in fields_str
        if collection == "olx_listings":
            return "address_refs" in fields_str
    except Exception:
        pass
    return None
