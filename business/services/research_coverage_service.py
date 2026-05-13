# -*- coding: utf-8 -*-
"""Coverage model for deep FLX investigations."""

from __future__ import annotations

from typing import Dict, List

from business.domain.flx_research_models import CoverageAspect


DEFAULT_ASPECTS = (
    "pricing",
    "infrastructure",
    "crime",
    "future_development",
    "demographics",
    "transport",
    "rental_yield",
)


class ResearchCoverageService:
    def empty_snapshot(self) -> Dict[str, Dict[str, float]]:
        return {
            name: CoverageAspect(
                name=name,
                score=0.0,
                confidence=0.0,
                evidence_count=0,
                source_diversity=0,
                contradiction_count=0,
            ).to_dict()
            for name in DEFAULT_ASPECTS
        }

    def update_aspect(
        self,
        coverage: Dict[str, Dict[str, float]],
        *,
        aspect: str,
        confidence: float,
        evidence_count: int,
        source_diversity: int,
        contradiction_delta: int = 0,
    ) -> Dict[str, Dict[str, float]]:
        current = dict((coverage or {}).get(aspect) or {})
        prev_score = float(current.get("score") or 0.0)
        signal = min(1.0, 0.5 * float(confidence) + 0.3 * min(1.0, evidence_count / 8.0) + 0.2 * min(1.0, source_diversity / 4.0))
        new_score = max(prev_score, round(signal, 4))
        current.update(
            {
                "name": aspect,
                "score": new_score,
                "confidence": round(max(float(current.get("confidence") or 0.0), float(confidence)), 4),
                "evidence_count": int(current.get("evidence_count") or 0) + int(max(0, evidence_count)),
                "source_diversity": max(int(current.get("source_diversity") or 0), int(max(0, source_diversity))),
                "contradiction_count": max(0, int(current.get("contradiction_count") or 0) + int(contradiction_delta)),
            }
        )
        out = dict(coverage or {})
        out[aspect] = current
        return out

    def aggregate_score(self, coverage: Dict[str, Dict[str, float]]) -> float:
        if not coverage:
            return 0.0
        scores: List[float] = [float(v.get("score") or 0.0) for v in coverage.values()]
        return round(sum(scores) / max(1, len(scores)), 4)

    def unresolved_contradictions(self, coverage: Dict[str, Dict[str, float]]) -> int:
        return sum(max(0, int(v.get("contradiction_count") or 0)) for v in (coverage or {}).values())
