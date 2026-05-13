# -*- coding: utf-8 -*-
"""Domain models for FLX deep research orchestration."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class ResearchTask:
    task_id: str
    goal: str
    branch: str
    priority: float = 0.5
    dependencies: List[str] = field(default_factory=list)
    status: str = "pending"  # pending | in_progress | done | blocked | cancelled
    confidence: float = 0.0
    evidence_refs: List[str] = field(default_factory=list)
    source_diversity: int = 0
    reopened_count: int = 0
    dedup_key: str = ""
    assigned_role: str = "Researcher"
    metadata: Dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "task_id": self.task_id,
            "goal": self.goal,
            "branch": self.branch,
            "priority": float(self.priority),
            "dependencies": list(self.dependencies),
            "status": self.status,
            "confidence": float(self.confidence),
            "evidence_refs": list(self.evidence_refs),
            "source_diversity": int(self.source_diversity),
            "reopened_count": int(self.reopened_count),
            "dedup_key": self.dedup_key,
            "assigned_role": self.assigned_role,
            "metadata": dict(self.metadata),
        }

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> "ResearchTask":
        return cls(
            task_id=str(data.get("task_id") or ""),
            goal=str(data.get("goal") or ""),
            branch=str(data.get("branch") or "general"),
            priority=float(data.get("priority") or 0.5),
            dependencies=[str(v) for v in (data.get("dependencies") or []) if str(v).strip()],
            status=str(data.get("status") or "pending"),
            confidence=float(data.get("confidence") or 0.0),
            evidence_refs=[str(v) for v in (data.get("evidence_refs") or []) if str(v).strip()],
            source_diversity=int(data.get("source_diversity") or 0),
            reopened_count=int(data.get("reopened_count") or 0),
            dedup_key=str(data.get("dedup_key") or ""),
            assigned_role=str(data.get("assigned_role") or "Researcher"),
            metadata=dict(data.get("metadata") or {}),
        )


@dataclass
class CoverageAspect:
    name: str
    score: float
    confidence: float
    evidence_count: int
    source_diversity: int
    contradiction_count: int = 0

    def to_dict(self) -> Dict[str, Any]:
        return {
            "name": self.name,
            "score": float(self.score),
            "confidence": float(self.confidence),
            "evidence_count": int(self.evidence_count),
            "source_diversity": int(self.source_diversity),
            "contradiction_count": int(self.contradiction_count),
        }


@dataclass
class ResearchBudget:
    web_searches: int = 40
    deep_analysis: int = 12
    api_calls: int = 500
    critic_iterations: int = 6

    def to_dict(self) -> Dict[str, Any]:
        return {
            "web_searches": int(self.web_searches),
            "deep_analysis": int(self.deep_analysis),
            "api_calls": int(self.api_calls),
            "critic_iterations": int(self.critic_iterations),
        }

    def consume(self, bucket: str, amount: int = 1) -> bool:
        if bucket not in self.to_dict():
            return False
        current = int(getattr(self, bucket, 0))
        if current < amount:
            return False
        setattr(self, bucket, current - amount)
        return True


@dataclass
class EvidenceClaim:
    claim_id: str
    statement: str
    confidence: float
    evidence_count: int
    source_diversity: int
    contradiction_status: str = "none"  # none | open | resolved
    evidence: List[str] = field(default_factory=list)
    sources: List[str] = field(default_factory=list)

    def to_dict(self) -> Dict[str, Any]:
        return {
            "claim_id": self.claim_id,
            "statement": self.statement,
            "confidence": float(self.confidence),
            "evidence_count": int(self.evidence_count),
            "source_diversity": int(self.source_diversity),
            "contradiction_status": self.contradiction_status,
            "evidence": list(self.evidence),
            "sources": list(self.sources),
        }
