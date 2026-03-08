# -*- coding: utf-8 -*-
"""
Analysis Planner Agent: приймає валідований Analysis Intent і будує гарантовано валідний
aggregation pipeline (без LLM). Не виконує сам — повертає spec для executor.
"""

import logging
from typing import Dict, Any, List, Optional, Tuple

from business.agents.analysis_intent_schema import (
    validate_analysis_intent,
    analysis_intent_from_structured,
    ENTITY_OLX,
    ENTITY_PROZORRO,
)
from utils.aggregation_patterns import AGGREGATION_PATTERNS
from utils.schema_filter_resolver import resolve_geo_filter

logger = logging.getLogger(__name__)


class AnalysisPlannerAgent:
    """
    Планувальник аналітичних запитів: Analysis Intent → нормалізація фільтрів →
    маппінг dimensions на схему → побудова pipeline → валідація.
    """

    def __init__(self, query_builder=None):
        """
        Args:
            query_builder: опційно екземпляр QueryBuilder для валідації pipeline.
        """
        self.query_builder = query_builder

    def plan(self, structured: Dict[str, Any]) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
        """
        Будує spec для виконання з результату інтерпретатора.

        Args:
            structured: результат interpret_user_query з intent=analytical_query та analysis_intent.

        Returns:
            (spec, error). spec = {"collection": str, "pipeline": list, "limit": int} або None.
        """
        intent_data = analysis_intent_from_structured(structured)
        if not intent_data:
            return None, "Відсутній або невалідний analysis_intent"

        ok, err = validate_analysis_intent(intent_data)
        if not ok:
            return None, err

        entity = intent_data.get("entity", ENTITY_OLX)
        if entity not in (ENTITY_OLX, ENTITY_PROZORRO):
            return None, f"Недозволена entity: {entity}"

        collection = entity
        time_range = intent_data.get("time_range")
        dimensions = intent_data.get("dimensions") or []
        filters = intent_data.get("filters") or {}
        metrics = intent_data.get("metrics") or []

        if not metrics:
            return None, "Потрібна хоча б одна метрика"

        primary = metrics[0]
        agg = (primary.get("aggregation") or "top").lower()
        if agg not in AGGREGATION_PATTERNS:
            return None, f"Недозволена агрегація: {agg}"

        build_fn = AGGREGATION_PATTERNS[agg]
        try:
            stages = build_fn(
                entity=entity,
                dimensions=dimensions,
                filters=filters,
                metric_spec=primary,
                time_range=time_range,
            )
        except Exception as e:
            logger.exception("AnalysisPlanner: помилка побудови патерну %s: %s", agg, e)
            return None, str(e)

        if not stages:
            return None, "Порожній pipeline після побудови патерну"

        limit = primary.get("limit") or 100
        if limit > 5000:
            limit = 5000

        spec = {
            "collection": collection,
            "pipeline": stages,
            "limit": limit,
        }

        if self.query_builder:
            is_valid, val_err = self.query_builder.validate_aggregation_pipeline(stages)
            if not is_valid:
                return None, val_err

        return spec, None
