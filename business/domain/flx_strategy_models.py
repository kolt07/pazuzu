# -*- coding: utf-8 -*-
"""
Контракти FLX Strategy Engine (логічний рівень).

Використовуються Planner → InvestigationService → StrategyEvaluationService →
StrategyRegistryService. Поля Mongo/physical paths агентам не експонуються.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class StrategyCandidate:
    """Кандидатна стратегія з власним підпланом кроків."""

    strategy_id: str
    name: str
    hypothesis: str
    steps: List[Dict[str, Any]]
    planned_tools: List[str] = field(default_factory=list)
    expected_signals: List[str] = field(default_factory=list)
    risk_level: str = "medium"  # low | medium | high
    estimated_cost: str = "medium"  # low | medium | high
    stop_conditions: str = ""

    def to_plan_dict(self) -> Dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "hypothesis": self.hypothesis,
            "steps": self.steps,
            "planned_tools": list(self.planned_tools),
            "expected_signals": list(self.expected_signals),
            "risk_level": self.risk_level,
            "estimated_cost": self.estimated_cost,
            "stop_conditions": self.stop_conditions,
        }


@dataclass
class StrategyRunResult:
    """Результат виконання однієї стратегії в межах сесії."""

    strategy_id: str
    name: str
    hypothesis: str
    steps_executed: int
    tool_calls: List[str]
    evidence_items: int
    failures: int
    metrics: Dict[str, Any]
    partial_conclusion: str
    score: float = 0.0
    score_breakdown: Dict[str, float] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "strategy_id": self.strategy_id,
            "name": self.name,
            "hypothesis": self.hypothesis,
            "steps_executed": self.steps_executed,
            "tool_calls": list(self.tool_calls),
            "evidence_items": self.evidence_items,
            "failures": self.failures,
            "metrics": dict(self.metrics),
            "partial_conclusion": self.partial_conclusion,
            "score": self.score,
            "score_breakdown": dict(self.score_breakdown),
        }

    @classmethod
    def from_dict(cls, d: Dict[str, Any]) -> "StrategyRunResult":
        return cls(
            strategy_id=str(d.get("strategy_id") or ""),
            name=str(d.get("name") or ""),
            hypothesis=str(d.get("hypothesis") or ""),
            steps_executed=int(d.get("steps_executed") or 0),
            tool_calls=list(d.get("tool_calls") or []),
            evidence_items=int(d.get("evidence_items") or 0),
            failures=int(d.get("failures") or 0),
            metrics=dict(d.get("metrics") or {}),
            partial_conclusion=str(d.get("partial_conclusion") or ""),
            score=float(d.get("score") or 0.0),
            score_breakdown=dict(d.get("score_breakdown") or {}),
        )


@dataclass
class StrategyRegistryEntry:
    """Запис у довгостроковому реєстрі стратегій (персистентний)."""

    task_fingerprint: str
    strategy_signature: str
    strategy_id: str
    strategy_name: str
    strategy_template: Dict[str, Any]
    domain_tags: List[str]
    rolling_score: float
    success_rate: float
    cost_efficiency: float
    usage_count: int
    wins: int
    total_runs: int
    last_used_at: Optional[str] = None
    query_exemplar: str = ""
