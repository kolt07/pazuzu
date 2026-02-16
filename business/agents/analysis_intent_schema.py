# -*- coding: utf-8 -*-
"""
Контракт Analysis Intent: структурований намір аналітичного запиту (не pipeline).
Використовується Interpreter (LLM Intent Extractor) та AnalysisPlannerAgent.
"""

from typing import Dict, Any, List, Optional, Tuple

# Дозволені сутності (колекції)
ENTITY_OLX = "olx_listings"
ENTITY_PROZORRO = "prozorro_auctions"
ALLOWED_ENTITIES = (ENTITY_OLX, ENTITY_PROZORRO)

# Дозволені періоди для time_range
TIME_RANGE_LAST_DAY = "last_1_day"
TIME_RANGE_LAST_WEEK = "last_7_days"
TIME_RANGE_LAST_MONTH = "last_30_days"
ALLOWED_TIME_RANGES = (TIME_RANGE_LAST_DAY, TIME_RANGE_LAST_WEEK, TIME_RANGE_LAST_MONTH)

# Дозволені dimensions (для групування)
DIMENSION_LOCATION = "location"
DIMENSION_REGION = "region"
DIMENSION_CITY = "city"
DIMENSION_DATE = "date"
DIMENSION_PROPERTY_TYPE = "property_type"
ALLOWED_DIMENSIONS = (DIMENSION_LOCATION, DIMENSION_REGION, DIMENSION_CITY, DIMENSION_DATE, DIMENSION_PROPERTY_TYPE)

# Дозволені типи агрегації метрик
AGG_TOP = "top"
AGG_COUNT = "count"
AGG_AVG = "avg"
AGG_SUM = "sum"
AGG_DISTRIBUTION = "distribution"
AGG_TREND = "trend"
ALLOWED_AGGREGATIONS = (AGG_TOP, AGG_COUNT, AGG_AVG, AGG_SUM, AGG_DISTRIBUTION, AGG_TREND)

# Презентація результату
PRESENTATION_LIST = "list"
PRESENTATION_TABLE = "table"
PRESENTATION_CHART = "chart"
ALLOWED_PRESENTATIONS = (PRESENTATION_LIST, PRESENTATION_TABLE, PRESENTATION_CHART)


def validate_analysis_intent(data: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Валідує словник analysis_intent. Повертає (True, None) або (False, повідомлення про помилку).
    """
    if not isinstance(data, dict):
        return False, "analysis_intent має бути словником"

    entity = data.get("entity")
    if entity not in ALLOWED_ENTITIES:
        return False, f"entity має бути одним з: {', '.join(ALLOWED_ENTITIES)}"

    time_range = data.get("time_range")
    if time_range is not None and time_range not in ALLOWED_TIME_RANGES:
        return False, f"time_range має бути одним з: {', '.join(ALLOWED_TIME_RANGES)} або null"

    dimensions = data.get("dimensions")
    if dimensions is not None:
        if not isinstance(dimensions, list):
            return False, "dimensions має бути списком"
        for d in dimensions:
            if d not in ALLOWED_DIMENSIONS:
                return False, f"dimension '{d}' не дозволений. Дозволені: {', '.join(ALLOWED_DIMENSIONS)}"

    filters = data.get("filters")
    if filters is not None and not isinstance(filters, dict):
        return False, "filters має бути словником"
    if filters:
        try:
            from domain.validators import validate_logical_filters
            validate_logical_filters(filters, context="analysis_intent.filters")
        except Exception as e:  # LogicalLayerViolation
            return False, str(e)

    metrics = data.get("metrics")
    if not metrics:
        return False, "metrics обов'язкове (непустий список)"
    if not isinstance(metrics, list):
        return False, "metrics має бути списком"
    for i, m in enumerate(metrics):
        if not isinstance(m, dict):
            return False, f"metrics[{i}] має бути словником"
        agg = m.get("aggregation")
        if agg and agg not in ALLOWED_AGGREGATIONS:
            return False, f"metrics[{i}].aggregation має бути одним з: {', '.join(ALLOWED_AGGREGATIONS)}"
        if m.get("limit") is not None and (not isinstance(m["limit"], int) or m["limit"] <= 0):
            return False, f"metrics[{i}].limit має бути додатнім цілим або відсутнім"

    presentation = data.get("presentation")
    if presentation is not None and presentation not in ALLOWED_PRESENTATIONS:
        return False, f"presentation має бути одним з: {', '.join(ALLOWED_PRESENTATIONS)} або null"

    return True, None


def analysis_intent_from_structured(structured: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    Витягує об'єкт analysis_intent з результату інтерпретатора (structured).
    Повертає None, якщо intent не analytical_query або analysis_intent відсутній/невалідний.
    """
    if structured.get("intent") != "analytical_query":
        return None
    data = structured.get("analysis_intent")
    if not data or not isinstance(data, dict):
        return None
    ok, _ = validate_analysis_intent(data)
    return data if ok else None
