# -*- coding: utf-8 -*-
"""Evidence / reasoning graph для трасованості тверджень (Flx) + типізовані вузли та ребра."""

from __future__ import annotations

import re
from typing import Any, Dict, List, Optional, Set

from business.domain.cognitive_agent_models import ReasoningEdgeType, ReasoningNodeType
from business.domain.flx_research_models import EvidenceClaim

_CONTRADICTION_HINTS = re.compile(
    r"протиріч|супереч|не\s+підтвердж|немає\s+даних|неможливо\s+підтверд|ризик\s+висок|невірн",
    re.IGNORECASE,
)


class EvidenceGraphService:
    def build_claim(self, *, statement: str, evidence: List[str], sources: List[str]) -> EvidenceClaim:
        evidence_count = len([x for x in (evidence or []) if str(x).strip()])
        source_diversity = len({str(x).strip() for x in (sources or []) if str(x).strip()})
        confidence = min(0.95, 0.35 + 0.08 * evidence_count + 0.07 * source_diversity)
        claim_id = f"claim_{abs(hash((statement or '')[:200]))}"
        return EvidenceClaim(
            claim_id=claim_id,
            statement=str(statement or "")[:1200],
            confidence=round(confidence, 4),
            evidence_count=evidence_count,
            source_diversity=source_diversity,
            contradiction_status="none",
            evidence=[str(x)[:1000] for x in (evidence or [])[:20]],
            sources=[str(x)[:400] for x in (sources or [])[:20]],
        )

    def from_notes(self, notes: List[Dict[str, Any]], *, max_nodes: int = 120) -> Dict[str, Any]:
        """Будує claims (сумісність) + reasoning nodes/edges та scores (Long Graph light)."""
        graph = self.build_reasoning_graph_from_notes(notes or [])
        pruned = self.prune_graph(graph, max_nodes=max_nodes)
        return pruned

    def build_reasoning_graph_from_notes(self, notes: List[Dict[str, Any]]) -> Dict[str, Any]:
        claims: List[Dict[str, Any]] = []
        nodes: List[Dict[str, Any]] = []
        edges: List[Dict[str, Any]] = []
        problem_id = "node_problem_root"
        nodes.append(
            {
                "id": problem_id,
                "type": ReasoningNodeType.PROBLEM.value,
                "label": "Research thread",
                "confidence": 0.5,
                "verification_status": "open",
            }
        )
        prev_claim_id: Optional[str] = None
        idx = 0
        for n in notes or []:
            text = str(n.get("text") or "").strip()
            if not text:
                continue
            tool = str(((n.get("tool_call_summary") or {}).get("name") or "")).strip()
            source = tool or str(n.get("kind") or "note")
            kind = str(n.get("kind") or "").lower()
            claim = self.build_claim(statement=text, evidence=[text], sources=[source])
            claims.append(claim.to_dict())
            claim_id = claim.claim_id
            if kind in ("observation", "user_answer"):
                ntype = ReasoningNodeType.EVIDENCE.value
            elif kind == "decision":
                ntype = ReasoningNodeType.STRATEGY.value
            else:
                ntype = ReasoningNodeType.INSIGHT.value
            nodes.append(
                {
                    "id": claim_id,
                    "type": ntype,
                    "label": text[:400],
                    "confidence": float(claim.confidence),
                    "source_tool": tool,
                    "kind": kind,
                    "verification_status": "unverified",
                }
            )
            edges.append(
                {
                    "source": problem_id,
                    "target": claim_id,
                    "type": ReasoningEdgeType.DEPENDS_ON.value,
                }
            )
            if prev_claim_id:
                edges.append(
                    {
                        "source": prev_claim_id,
                        "target": claim_id,
                        "type": ReasoningEdgeType.DERIVED_FROM.value,
                    }
                )
                if _CONTRADICTION_HINTS.search(text):
                    edges.append(
                        {
                            "source": claim_id,
                            "target": prev_claim_id,
                            "type": ReasoningEdgeType.CONTRADICTS.value,
                        }
                    )
            prev_claim_id = claim_id
            idx += 1
            if idx >= 200:
                break
        scores = self.score_nodes({"nodes": nodes, "edges": edges})
        return {
            "claims": claims[:120],
            "nodes": nodes,
            "edges": edges,
            "branch_scores": scores,
        }

    def score_nodes(self, graph: Dict[str, Any]) -> Dict[str, float]:
        """Простий score вузла: базова confidence + бонус за вхідні supports / штраф за contradicts."""
        nodes = graph.get("nodes") or []
        edges = graph.get("edges") or []
        incoming_support: Dict[str, int] = {}
        incoming_contra: Dict[str, int] = {}
        for e in edges:
            t = str(e.get("type") or "")
            src = str(e.get("source") or "")
            tgt = str(e.get("target") or "")
            if t == ReasoningEdgeType.SUPPORTS.value:
                incoming_support[tgt] = incoming_support.get(tgt, 0) + 1
            if t == ReasoningEdgeType.CONTRADICTS.value:
                incoming_contra[tgt] = incoming_contra.get(tgt, 0) + 1
        out: Dict[str, float] = {}
        for n in nodes:
            nid = str(n.get("id") or "")
            if not nid:
                continue
            base = float(n.get("confidence") or 0.4)
            bonus = 0.04 * min(4, incoming_support.get(nid, 0))
            pen = 0.07 * min(4, incoming_contra.get(nid, 0))
            out[nid] = round(max(0.05, min(0.98, base + bonus - pen)), 4)
        return out

    def prune_graph(self, graph: Dict[str, Any], *, max_nodes: int = 100) -> Dict[str, Any]:
        """Залишає найсильніші вузли + ребра між ними; root problem завжди лишається."""
        nodes: List[Dict[str, Any]] = list(graph.get("nodes") or [])
        edges: List[Dict[str, Any]] = list(graph.get("edges") or [])
        claims: List[Dict[str, Any]] = list(graph.get("claims") or [])
        if len(nodes) <= max_nodes:
            return {**graph, "pruned": False, "pruned_node_count": len(nodes)}
        scores = graph.get("branch_scores") or self.score_nodes(graph)
        root = next((n for n in nodes if n.get("type") == ReasoningNodeType.PROBLEM.value), None)
        ranked = sorted(
            [n for n in nodes if n.get("type") != ReasoningNodeType.PROBLEM.value],
            key=lambda x: scores.get(str(x.get("id")), 0.0),
            reverse=True,
        )
        cap = max(1, max_nodes - 1)
        keep_ids: Set[str] = set()
        if root:
            keep_ids.add(str(root.get("id")))
        for n in ranked[:cap]:
            keep_ids.add(str(n.get("id")))
        new_nodes = [n for n in nodes if str(n.get("id")) in keep_ids]
        new_edges = [
            e
            for e in edges
            if str(e.get("source")) in keep_ids and str(e.get("target")) in keep_ids
        ]
        kept_claim_ids = {str(n["id"]) for n in new_nodes if str(n.get("id", "")).startswith("claim_")}
        new_claims = [c for c in claims if str(c.get("claim_id")) in kept_claim_ids]
        if len(new_claims) < len(claims) * 0.3:
            new_claims = claims[: min(len(claims), len(new_nodes) + 20)]
        return {
            "claims": new_claims[:120],
            "nodes": new_nodes,
            "edges": new_edges,
            "branch_scores": scores,
            "pruned": True,
            "pruned_node_count": len(new_nodes),
        }
