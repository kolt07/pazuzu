# -*- coding: utf-8 -*-
"""Структурований когнітивний стан та типи для Long Graph (узгоджено з cognitive-agent roadmap)."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Dict, List, Optional


class ReasoningNodeType(str, Enum):
    """Типи вузлів reasoning graph."""

    PROBLEM = "problem"
    HYPOTHESIS = "hypothesis"
    EVIDENCE = "evidence"
    TOOL_RESULT = "tool_result"
    INSIGHT = "insight"
    CONTRADICTION = "contradiction"
    STRATEGY = "strategy"
    CONCLUSION = "conclusion"
    CLAIM = "claim"  # сумісність зі старими claims


class ReasoningEdgeType(str, Enum):
    """Типи ребер між вузлами."""

    SUPPORTS = "supports"
    CONTRADICTS = "contradicts"
    DERIVED_FROM = "derived_from"
    DEPENDS_ON = "depends_on"
    VALIDATED_BY = "validated_by"


# Ключі словника CognitiveState (JSON-serializable для LangGraph / Mongo)
CS_GOAL = "goal"
CS_CURRENT_TASK = "current_task"
CS_KNOWN_FACTS = "known_facts"
CS_UNKNOWNS = "unknowns"
CS_HYPOTHESES = "hypotheses"
CS_COMPLETED_STEPS = "completed_steps"
CS_PENDING_STEPS = "pending_steps"
CS_TOOL_CANDIDATES = "tool_candidates"
CS_CONFIDENCE = "confidence"
CS_LAST_TOOL_FAILED = "last_tool_failed"
CS_STRATEGIC_NOTES = "strategic_notes"


def new_cognitive_state(goal: str) -> Dict[str, Any]:
    """Початковий explicit state для циклу PLAN → EXECUTE → OBSERVE → UPDATE."""
    g = (goal or "").strip()[:2000]
    return {
        CS_GOAL: g,
        CS_CURRENT_TASK: "",
        CS_KNOWN_FACTS: [],
        CS_UNKNOWNS: [],
        CS_HYPOTHESES: [],
        CS_COMPLETED_STEPS: [],
        CS_PENDING_STEPS: [],
        CS_TOOL_CANDIDATES: [],
        CS_CONFIDENCE: 0.5,
        CS_LAST_TOOL_FAILED: False,
        CS_STRATEGIC_NOTES: "",
    }


@dataclass
class LongChainStepRecord:
    """Один крок Long Chain для збереження в Mongo."""

    step_id: str
    parent_step_id: Optional[str]
    goal: str
    action: str
    observation: str
    conclusion: str
    confidence: float
    derived_knowledge: str
    next_options: str
    iteration: int
    compressed: bool = False

    def to_mongo_doc(self, request_id: str, user_id: Optional[str]) -> Dict[str, Any]:
        from datetime import datetime, timezone

        return {
            "request_id": request_id,
            "user_id": user_id,
            "step_id": self.step_id,
            "parent_step_id": self.parent_step_id,
            "goal": self.goal[:1200],
            "action": self.action[:800],
            "observation": self.observation[:8000],
            "conclusion": self.conclusion[:1200],
            "confidence": float(self.confidence),
            "derived_knowledge": self.derived_knowledge[:2000],
            "next_options": self.next_options[:1200],
            "iteration": int(self.iteration),
            "compressed": bool(self.compressed),
            "created_at": datetime.now(timezone.utc),
        }


@dataclass
class SemanticMemoryRecord:
    """Дистильований запис після задачі (semantic memory)."""

    request_id: str
    user_id: Optional[str]
    user_query_excerpt: str
    summary_uk: str
    facts: List[str] = field(default_factory=list)
    tool_heuristics: List[str] = field(default_factory=list)
    failures: List[str] = field(default_factory=list)

    def to_mongo_doc(self) -> Dict[str, Any]:
        from datetime import datetime, timezone

        return {
            "request_id": self.request_id,
            "user_id": self.user_id,
            "user_query_excerpt": self.user_query_excerpt[:500],
            "summary_uk": self.summary_uk[:4000],
            "facts": [str(x)[:500] for x in self.facts[:40]],
            "tool_heuristics": [str(x)[:500] for x in self.tool_heuristics[:20]],
            "failures": [str(x)[:500] for x in self.failures[:20]],
            "created_at": datetime.now(timezone.utc),
        }
