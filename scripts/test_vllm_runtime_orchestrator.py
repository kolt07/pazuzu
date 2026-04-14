# -*- coding: utf-8 -*-
"""
Мінімальні unit-тести оркестратора vLLM runtime.

Запуск:
    py -m unittest scripts.test_vllm_runtime_orchestrator
"""

import unittest

from business.services.vllm_runtime_orchestrator import VllmRuntimeOrchestrator


class VllmRuntimeOrchestratorTest(unittest.TestCase):
    def test_build_query_contains_constraints(self):
        cfg = {
            "min_gpu_ram_gb": 24,
            "max_hourly_usd": 0.9,
            "min_reliability": 0.99,
            "gpu_name_like": "A5000",
            "region_like": "EU",
            "datacenter_like": "Equinix",
        }
        query = VllmRuntimeOrchestrator._build_query(cfg)
        self.assertIn("gpu_ram>=24", query)
        self.assertIn("dph_total<=0.9", query)
        self.assertIn("reliability>=0.99", query)
        self.assertIn("gpu_name~A5000", query)
        self.assertIn("geolocation~EU", query)
        self.assertIn("datacenter~Equinix", query)

    def test_estimated_cost(self):
        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        cost = obj._estimate_cost_usd(active_seconds=1800, cfg={"max_hourly_usd": 1.2})
        self.assertAlmostEqual(cost, 0.6, places=6)


if __name__ == "__main__":
    unittest.main()
