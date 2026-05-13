# -*- coding: utf-8 -*-
"""
Оцінка та ранжування стратегій FLX за узагальненими метриками (без LLM).

Метрики — евристики по нотатках сесії: coverage інструментів, обсяг evidence,
помилки, POI-proxy сигнали (якщо були виклики geocoding/places).
"""

from __future__ import annotations

import json
import logging
from typing import Any, Dict, List, Optional, Tuple

from business.domain.flx_strategy_models import StrategyRunResult

logger = logging.getLogger(__name__)

_POI_KEYS = (
    "restaurant",
    "meal_takeaway",
    "meal_delivery",
    "cafe",
    "bar",
    "food",
    "fast",
)


class StrategyEvaluationService:
    """Детермінований скоринг стратегій для крос-порівняння."""

    def __init__(
        self,
        *,
        weight_evidence: float = 1.0,
        weight_tools: float = 0.35,
        weight_failures: float = -0.45,
        weight_poi: float = 0.25,
        weight_expected: float = 0.4,
    ):
        self.weight_evidence = weight_evidence
        self.weight_tools = weight_tools
        self.weight_failures = weight_failures
        self.weight_poi = weight_poi
        self.weight_expected = weight_expected

    def build_run_result_from_notes(
        self,
        *,
        strategy_id: str,
        name: str,
        hypothesis: str,
        notes: List[Dict[str, Any]],
        expected_signals: Optional[List[str]] = None,
        planned_tools: Optional[List[str]] = None,
    ) -> StrategyRunResult:
        """Агрегує нотатки однієї стратегії в StrategyRunResult."""
        tool_calls: List[str] = []
        failures = 0
        evidence_items = 0
        text_blob_parts: List[str] = []

        for n in notes:
            kind = str(n.get("kind") or "")
            tcs = n.get("tool_call_summary") or {}
            if isinstance(tcs, dict) and tcs.get("name"):
                tn = str(tcs.get("name") or "")
                tool_calls.append(tn)
            if kind == "tool_error":
                failures += 1
            if kind == "observation":
                evidence_items += 1
                text_blob_parts.append(str(n.get("text") or ""))

        text_blob = "\n".join(text_blob_parts)
        poi_metrics = self._extract_poi_proxy_metrics(text_blob)

        partial_conclusion = ""
        if text_blob:
            partial_conclusion = text_blob[:1200] + ("…" if len(text_blob) > 1200 else "")

        unique_tools = list(dict.fromkeys(tool_calls))
        metrics: Dict[str, Any] = {
            "unique_tool_count": len(unique_tools),
            "total_tool_calls": len(tool_calls),
            "evidence_count": evidence_items,
            "failure_count": failures,
            **poi_metrics,
        }

        expected_hit = 0.5
        if expected_signals:
            low = text_blob.lower()
            hits = sum(1 for s in expected_signals if str(s).lower().strip() and str(s).lower() in low)
            expected_hit = min(1.0, hits / max(1, len(expected_signals)))
        metrics["expected_hit"] = expected_hit

        score, breakdown = self.score_run(
            metrics=metrics,
            planned_tools=planned_tools or [],
            tools_used=unique_tools,
        )

        return StrategyRunResult(
            strategy_id=strategy_id,
            name=name,
            hypothesis=hypothesis,
            steps_executed=max(1, evidence_items + failures),
            tool_calls=unique_tools,
            evidence_items=evidence_items,
            failures=failures,
            metrics=metrics,
            partial_conclusion=partial_conclusion,
            score=score,
            score_breakdown=breakdown,
        )

    def score_run(
        self,
        *,
        metrics: Dict[str, Any],
        planned_tools: List[str],
        tools_used: List[str],
    ) -> Tuple[float, Dict[str, float]]:
        """Повертає зведений score та розклад по компонентах."""
        evidence = float(metrics.get("evidence_count") or 0)
        failures = float(metrics.get("failure_count") or 0)
        poi_mix = float(metrics.get("poi_premium_mix") or 0.0)
        poi_div = float(metrics.get("poi_diversity") or 0.0)

        expected_hit = float(metrics.get("expected_hit") if metrics.get("expected_hit") is not None else 0.5)

        tool_overlap = 0.0
        if planned_tools:
            pu = set(tools_used or [])
            pt = set(planned_tools or [])
            if pt:
                tool_overlap = len(pu & pt) / max(1, len(pt))
        else:
            tool_overlap = 0.5 if tools_used else 0.0

        score = (
            self.weight_evidence * min(3.0, evidence / 3.0)
            + self.weight_tools * tool_overlap
            + self.weight_failures * min(3.0, failures)
            + self.weight_poi * (0.6 * poi_mix + 0.4 * min(1.0, poi_div))
            + self.weight_expected * expected_hit
        )

        breakdown = {
            "evidence": self.weight_evidence * min(3.0, evidence / 3.0),
            "tool_overlap": self.weight_tools * tool_overlap,
            "failures": self.weight_failures * min(3.0, failures),
            "poi_proxy": self.weight_poi * (0.6 * poi_mix + 0.4 * min(1.0, poi_div)),
            "expected_signals": self.weight_expected * expected_hit,
        }
        return score, breakdown

    def rank_runs(self, runs: List[StrategyRunResult]) -> List[StrategyRunResult]:
        """Сортує за score (desc), при рівності — за evidence_items."""
        return sorted(
            runs,
            key=lambda r: (r.score, r.evidence_items, -r.failures),
            reverse=True,
        )

    def _extract_poi_proxy_metrics(self, text: str) -> Dict[str, float]:
        """Легкі ознаки з серіалізованих observation (JSON або текст)."""
        if not text or len(text) < 10:
            return {"poi_premium_mix": 0.0, "fastfood_density": 0.0, "poi_diversity": 0.0}

        poi_diversity = 0.0
        premium_mix = 0.0
        fastfood_density = 0.0

        try:
            data = json.loads(text)
            places = self._places_from_payload(data)
        except Exception:
            places = []
            tl = text.lower()
            if "restaurant" in tl or "meal_takeaway" in tl:
                poi_diversity = 0.4
            for k in _POI_KEYS:
                if k in tl:
                    fastfood_density += 0.15
            fastfood_density = min(1.0, fastfood_density)
            premium_mix = 0.3 if "price_level" in tl or "rating" in tl else 0.1
            return {
                "poi_premium_mix": round(premium_mix, 4),
                "fastfood_density": round(fastfood_density, 4),
                "poi_diversity": round(poi_diversity, 4),
            }

        types_seen: set[str] = set()
        price_levels: List[int] = []
        for p in places:
            if not isinstance(p, dict):
                continue
            types = p.get("types") or p.get("place_types") or []
            if isinstance(types, list):
                for t in types:
                    types_seen.add(str(t).lower())
            pl = p.get("price_level")
            if pl is not None:
                try:
                    price_levels.append(int(pl))
                except (TypeError, ValueError):
                    pass

        if types_seen:
            poi_diversity = min(1.0, len(types_seen) / 8.0)
        fast_tokens = ("meal_takeaway", "fast_food", "restaurant")
        fast_count = sum(1 for t in types_seen if any(f in t for f in fast_tokens))
        fastfood_density = min(1.0, fast_count / 4.0) if fast_count else 0.0

        if price_levels:
            avg_pl = sum(price_levels) / len(price_levels)
            premium_mix = min(1.0, max(0.0, (avg_pl - 1) / 3.0))
        else:
            premium_mix = 0.35 if ("restaurant" in types_seen or "cafe" in types_seen) else 0.1

        return {
            "poi_premium_mix": round(float(premium_mix), 4),
            "fastfood_density": round(float(fastfood_density), 4),
            "poi_diversity": round(float(poi_diversity), 4),
        }

    def _places_from_payload(self, data: Any) -> List[Dict[str, Any]]:
        out: List[Dict[str, Any]] = []
        if isinstance(data, dict):
            if isinstance(data.get("places"), list):
                out.extend([p for p in data["places"] if isinstance(p, dict)])
            if isinstance(data.get("results"), list):
                out.extend([p for p in data["results"] if isinstance(p, dict)])
            nested = data.get("data") or data.get("response")
            if isinstance(nested, dict):
                out.extend(self._places_from_payload(nested))
        return out
