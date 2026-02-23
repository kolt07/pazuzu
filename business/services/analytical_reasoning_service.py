# -*- coding: utf-8 -*-
"""
Analytical Reasoning Service: для складних запитів (наприклад порівняння OLX і ProZorro по регіонах).
LLM будує лише план кроків (JSON); виконання — детерміноване (executor без LLM).
Guardrails: max_steps, allowed_operations, dry-run валідація.
"""

import json
import logging
from typing import Dict, Any, List, Optional, Tuple

from config.settings import Settings
from business.services.llm_service import LLMService
from utils.query_builder import QueryBuilder
from utils.aggregation_patterns import build_avg
from utils.schema_filter_resolver import resolve_geo_filter

logger = logging.getLogger(__name__)

MAX_STEPS = 7
ALLOWED_STEP_TYPES = {"aggregate", "query", "join_by_region", "compute_difference", "sort"}


class AnalyticalReasoningService:
    """
    Складний аналітичний запит: LLM генерує план (steps), executor виконує кроки без LLM.
    """

    def __init__(self, settings: Optional[Settings] = None, llm_service: Optional[LLMService] = None):
        self.settings = settings or Settings()
        self._llm = llm_service
        self._qb: Optional[QueryBuilder] = None

    @property
    def llm_service(self) -> LLMService:
        if self._llm is None:
            self._llm = LLMService(self.settings)
        return self._llm

    @property
    def query_builder(self) -> QueryBuilder:
        if self._qb is None:
            self._qb = QueryBuilder()
        return self._qb

    def build_plan(self, user_query: str, analysis_intent: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Викликає LLM один раз для побудови плану кроків. Повертає JSON з полем steps або None.
        """
        prompt = self._create_plan_prompt(user_query, analysis_intent)
        try:
            raw = self.llm_service.generate_text(
                prompt,
                system_prompt="Ти планувальник аналітики. Поверни тільки валідний JSON з полем steps (масив кроків). Без пояснень.",
                temperature=0.0,
            )
        except Exception as e:
            logger.warning("AnalyticalReasoning build_plan: %s", e)
            return None
        if not raw:
            return None
        text = raw.strip()
        if text.startswith("```"):
            lines = text.split("\n")
            json_lines = []
            in_json = False
            for line in lines:
                if line.strip().startswith("```"):
                    in_json = not in_json if not in_json else False
                    continue
                if in_json:
                    json_lines.append(line)
            text = "\n".join(json_lines)
        if "{" in text:
            start, end = text.find("{"), text.rfind("}")
            if start != -1 and end > start:
                text = text[start : end + 1]
        try:
            return json.loads(text)
        except json.JSONDecodeError:
            return None

    def _create_plan_prompt(self, user_query: str, analysis_intent: Dict[str, Any]) -> str:
        return f"""Тобі необхідно сформувати план кроків для виконання аналітичного запиту у форматі JSON.

## Запит користувача: {user_query[:1500]}

## Поточний analysis_intent: {json.dumps(analysis_intent, ensure_ascii=False)[:500]}

## Закріплення завдання:
Поверни JSON з одним полем:
- steps: масив об'єктів. Дозволені типи кроків:
  - {{ "step": "aggregate", "source": "olx_listings"|"prozorro_auctions", "metric": "avg_price", "group_by": "region"|"city" }}
  - {{ "step": "join_by_region" }} — об'єднати два попередні результати по регіону
  - {{ "step": "compute_difference", "field": "avg" }} — різниця значень по регіону
  - {{ "step": "sort", "field": "difference", "order": "desc" }}

Максимум {MAX_STEPS} кроків. Поверни тільки JSON з полем steps, без коментарів."""

    def validate_plan(self, plan: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """Dry-run: перевірка плану на дозволені операції та max_steps."""
        if not plan or not isinstance(plan, dict):
            return False, "План має бути словником"
        steps = plan.get("steps")
        if not steps or not isinstance(steps, list):
            return False, "План має містити непустий масив steps"
        if len(steps) > MAX_STEPS:
            return False, f"Максимум {MAX_STEPS} кроків"
        for i, s in enumerate(steps):
            if not isinstance(s, dict):
                return False, f"Крок {i} має бути словником"
            step_type = s.get("step")
            if step_type not in ALLOWED_STEP_TYPES:
                return False, f"Крок {i}: недозволений тип {step_type}"
            if step_type == "aggregate" and s.get("source") not in ("olx_listings", "prozorro_auctions"):
                return False, f"Крок {i}: source має бути olx_listings або prozorro_auctions"
        return True, None

    def execute_plan(
        self,
        plan: Dict[str, Any],
        filters: Optional[Dict[str, Any]] = None,
        time_range: Optional[str] = None,
    ) -> Tuple[Optional[List[Dict[str, Any]]], Optional[str]]:
        """
        Виконує план детерміновано (без LLM). Повертає (результат, помилка).
        """
        ok, err = self.validate_plan(plan)
        if not ok:
            return None, err
        steps = plan.get("steps", [])
        filters = filters or {}
        results_by_source: Dict[str, List[Dict[str, Any]]] = {}
        try:
            for step in steps:
                st = step.get("step")
                if st == "aggregate":
                    source = step.get("source")
                    group_by = step.get("group_by", "region")
                    stages = build_avg(
                        entity=source,
                        dimensions=[group_by],
                        filters=filters,
                        metric_spec={"field": "price"},
                        time_range=time_range,
                    )
                    if not stages:
                        return None, f"Не вдалося побудувати агрегацію для {source}"
                    out = self.query_builder.execute_aggregation(source, stages, limit=500)
                    if not out.get("success"):
                        return None, out.get("error", "Помилка агрегації")
                    results_by_source[source] = out.get("results") or []
                elif st == "join_by_region":
                    olx = results_by_source.get("olx_listings") or []
                    proz = results_by_source.get("prozorro_auctions") or []
                    by_region: Dict[str, Dict[str, Any]] = {}
                    for r in olx:
                        rid = r.get("_id") or r.get("region")
                        if rid:
                            by_region[str(rid)] = {"region": rid, "olx_avg": r.get("avg")}
                    for r in proz:
                        rid = r.get("_id") or r.get("region")
                        if rid:
                            by_region.setdefault(str(rid), {"region": rid})
                            by_region[str(rid)]["prozorro_avg"] = r.get("avg")
                    merged = list(by_region.values())
                    results_by_source["_merged"] = merged
                elif st == "compute_difference":
                    merged = results_by_source.get("_merged") or []
                    field = step.get("field", "avg")
                    for r in merged:
                        o, p = r.get("olx_avg"), r.get("prozorro_avg")
                        if o is not None and p is not None:
                            r["difference"] = (o - p) if isinstance(o, (int, float)) and isinstance(p, (int, float)) else None
                        else:
                            r["difference"] = None
                    results_by_source["_merged"] = merged
                elif st == "sort":
                    merged = results_by_source.get("_merged") or []
                    order = -1 if (step.get("order") or "desc").lower() == "desc" else 1
                    key = step.get("field") or "difference"
                    merged.sort(key=lambda x: (x.get(key) is None, -(x.get(key) or 0) if order == -1 else (x.get(key) or 0)))
                    results_by_source["_merged"] = merged
            final = results_by_source.get("_merged") or results_by_source.get("olx_listings") or results_by_source.get("prozorro_auctions") or []
            return final, None
        except Exception as e:
            logger.exception("execute_plan: %s", e)
            return None, str(e)
