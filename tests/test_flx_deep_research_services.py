# -*- coding: utf-8 -*-
"""Unit tests for FLX deep research services."""

import unittest

from business.domain.flx_research_models import ResearchBudget, ResearchTask
from business.services.evidence_graph_service import EvidenceGraphService
from business.services.priority_scoring_service import PriorityScoringService
from business.services.research_coverage_service import ResearchCoverageService


class TestFlxDeepResearchServices(unittest.TestCase):
    def test_priority_higher_for_lower_coverage(self):
        svc = PriorityScoringService()
        task = ResearchTask(task_id="t1", goal="Analyze rental demand", branch="investment", confidence=0.2)
        low_cov = svc.score(task, coverage_gap=0.9)
        high_cov = svc.score(task, coverage_gap=0.1)
        self.assertGreater(low_cov, high_cov)

    def test_coverage_updates_and_aggregate(self):
        svc = ResearchCoverageService()
        snapshot = svc.empty_snapshot()
        snapshot = svc.update_aspect(snapshot, aspect="pricing", confidence=0.8, evidence_count=3, source_diversity=2)
        self.assertGreater(float(snapshot["pricing"]["score"]), 0.0)
        agg = svc.aggregate_score(snapshot)
        self.assertGreaterEqual(agg, 0.0)

    def test_budget_consumption(self):
        budget = ResearchBudget(web_searches=2, deep_analysis=1, api_calls=1, critic_iterations=1)
        self.assertTrue(budget.consume("web_searches"))
        self.assertTrue(budget.consume("web_searches"))
        self.assertFalse(budget.consume("web_searches"))

    def test_evidence_graph_claim(self):
        svc = EvidenceGraphService()
        graph = svc.from_notes(
            [
                {"text": "Орендний попит високий у районі", "kind": "observation", "tool_call_summary": {"name": "flx.web_search"}},
                {"text": "Ризик ліквідності помірний", "kind": "decision", "tool_call_summary": {}},
            ]
        )
        self.assertIn("claims", graph)
        self.assertGreaterEqual(len(graph["claims"]), 1)
        self.assertIn("nodes", graph)
        self.assertIn("edges", graph)
        self.assertIn("branch_scores", graph)


if __name__ == "__main__":
    unittest.main()
