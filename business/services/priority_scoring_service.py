# -*- coding: utf-8 -*-
"""Priority scoring for FLX research task queue."""

from __future__ import annotations

from typing import Dict

from business.domain.flx_research_models import ResearchTask


class PriorityScoringService:
    """Deterministic scoring for pending/reopened research tasks."""

    def score(
        self,
        task: ResearchTask,
        *,
        coverage_gap: float,
        dependency_unlock: float = 0.0,
        contradiction_penalty: float = 0.0,
        stale_bonus: float = 0.0,
    ) -> float:
        impact = 0.45
        uncertainty = max(0.0, 1.0 - float(task.confidence))
        base = (
            impact * max(0.0, min(1.0, coverage_gap))
            + 0.25 * uncertainty
            + 0.2 * max(0.0, min(1.0, dependency_unlock))
            + 0.1 * max(0.0, min(1.0, stale_bonus))
        )
        base -= 0.2 * max(0.0, min(1.0, contradiction_penalty))
        if task.reopened_count > 0:
            base += min(0.15, task.reopened_count * 0.03)
        return max(0.0, min(1.0, round(base, 4)))

    def score_map(self, task: ResearchTask, aspect_score: float, contradictions: int) -> Dict[str, float]:
        coverage_gap = max(0.0, 1.0 - aspect_score)
        penalty = 1.0 if contradictions > 0 else 0.0
        score = self.score(task, coverage_gap=coverage_gap, contradiction_penalty=penalty)
        return {
            "priority": score,
            "coverage_gap": round(coverage_gap, 4),
            "contradiction_penalty": float(penalty),
        }
