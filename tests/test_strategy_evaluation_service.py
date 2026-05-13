# -*- coding: utf-8 -*-
"""Unit tests for StrategyEvaluationService (stdlib unittest)."""

import unittest

from business.domain.flx_strategy_models import StrategyRunResult
from business.services.strategy_evaluation_service import StrategyEvaluationService
from business.services.strategy_registry_service import task_fingerprint_from_query


class TestStrategyEvaluation(unittest.TestCase):
    def test_rank_runs_prefers_more_evidence(self):
        svc = StrategyEvaluationService()
        a = StrategyRunResult(
            strategy_id="a",
            name="A",
            hypothesis="",
            steps_executed=3,
            tool_calls=["query_builder.execute_query"],
            evidence_items=5,
            failures=0,
            metrics={"evidence_count": 5, "failure_count": 0, "expected_hit": 0.8},
            partial_conclusion="x",
            score=0.5,
            score_breakdown={},
        )
        b = StrategyRunResult(
            strategy_id="b",
            name="B",
            hypothesis="",
            steps_executed=2,
            tool_calls=["geocoding.geocode_address"],
            evidence_items=1,
            failures=0,
            metrics={"evidence_count": 1, "failure_count": 0, "expected_hit": 0.5},
            partial_conclusion="y",
            score=0.4,
            score_breakdown={},
        )
        ranked = svc.rank_runs([b, a])
        self.assertEqual(ranked[0].strategy_id, "a")

    def test_task_fingerprint_stable(self):
        self.assertEqual(
            task_fingerprint_from_query("  Київ  ціни  "),
            task_fingerprint_from_query("київ ціни"),
        )


if __name__ == "__main__":
    unittest.main()
