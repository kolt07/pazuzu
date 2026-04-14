# -*- coding: utf-8 -*-
"""
Мінімальні unit-тести оркестратора vLLM runtime.

Запуск:
    py -m unittest scripts.test_vllm_runtime_orchestrator
"""

import unittest
import requests

from business.services.vllm_runtime_orchestrator import VllmRuntimeOrchestrator


class VllmRuntimeOrchestratorTest(unittest.TestCase):
    def test_build_offer_filters_contains_constraints(self):
        cfg = {
            "min_gpu_ram_gb": 24,
            "max_hourly_usd": 0.9,
            "min_reliability": 0.99,
            "gpu_name_like": "A5000",
        }
        filters = VllmRuntimeOrchestrator._build_offer_filters(cfg)
        self.assertEqual(filters["gpu_ram"], {"gte": 24576})
        self.assertEqual(filters["inet_down"], {"gte": 700.0})
        self.assertEqual(filters["dph_total"], {"lte": 0.9})
        self.assertEqual(filters["reliability"], {"gte": 0.99})
        self.assertEqual(filters["gpu_name"], {"eq": "A5000"})
        self.assertEqual(filters["verified"], {"eq": True})
        self.assertEqual(filters["external"], {"eq": False})
        self.assertEqual(filters["compute_cap"], {"gte": 700})

    def test_select_offer_candidates_prioritizes_network_bandwidth_target(self):
        class _FakeClient:
            def search_offers(self, filters):
                return [
                    {"id": 21, "gpu_name": "RTX 3090", "inet_down": 1200, "dph_total": 0.09, "reliability": 0.95},
                    {"id": 22, "gpu_name": "RTX 3090", "inet_down": 980, "dph_total": 0.12, "reliability": 0.95},
                    {"id": 23, "gpu_name": "RTX 3090", "inet_down": 760, "dph_total": 0.08, "reliability": 0.99},
                ]

        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        candidates = obj._select_offer_candidates(
            _FakeClient(),
            {"min_gpu_ram_gb": 24, "min_inet_down_mbps": 700, "preferred_inet_down_mbps": 1000},
            limit=3,
        )
        self.assertEqual(candidates, ["22", "21", "23"])

    def test_rate_limit_error_detection(self):
        r = requests.Response()
        r.status_code = 429
        e429 = requests.HTTPError("429 Too Many Requests", response=r)
        self.assertTrue(VllmRuntimeOrchestrator._is_rate_limited_error(e429))
        r2 = requests.Response()
        r2.status_code = 400
        e400 = requests.HTTPError("400 Bad Request", response=r2)
        self.assertFalse(VllmRuntimeOrchestrator._is_rate_limited_error(e400))

    def test_estimated_cost(self):
        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        cost = obj._estimate_cost_usd(active_seconds=1800, cfg={"max_hourly_usd": 1.2})
        self.assertAlmostEqual(cost, 0.6, places=6)

    def test_select_offer_prefers_compatible_gpu_over_cheaper_incompatible(self):
        class _FakeClient:
            def search_offers(self, filters):
                return [
                    {"id": 1, "gpu_name": "Tesla P40", "dph_total": 0.09, "reliability": 0.99},
                    {"id": 2, "gpu_name": "RTX 3090", "dph_total": 0.12, "reliability": 0.99},
                ]

        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        offer_id = obj._select_offer_id(_FakeClient(), {"min_gpu_ram_gb": 24})
        self.assertEqual(offer_id, "2")

    def test_select_offer_candidates_returns_ordered_ids(self):
        class _FakeClient:
            def search_offers(self, filters):
                return [
                    {"id": 11, "gpu_name": "RTX 3090", "dph_total": 0.2, "reliability": 0.95},
                    {"id": 12, "gpu_name": "RTX 3090", "dph_total": 0.1, "reliability": 0.90},
                    {"id": 13, "gpu_name": "RTX 3090", "dph_total": 0.15, "reliability": 0.99},
                ]

        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        candidates = obj._select_offer_candidates(_FakeClient(), {"min_gpu_ram_gb": 24}, limit=2)
        self.assertEqual(candidates, ["12", "13"])

    def test_offer_filters_raise_gpu_ram_floor_for_gemma_2_9b(self):
        cfg = {
            "min_gpu_ram_gb": 20,
            "vllm_model": "google/gemma-2-9b-it",
        }
        filters = VllmRuntimeOrchestrator._build_offer_filters(cfg)
        self.assertEqual(filters["gpu_ram"], {"gte": 24576})

    def test_build_create_payload_includes_hf_token_in_env(self):
        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        cfg = {
            "image": "vastai/vllm:latest",
            "disk_gb": 40,
            "vllm_port": 8000,
            "vllm_model": "org/gated-model",
            "vllm_max_model_len": 4096,
            "vllm_gpu_memory_utilization": 0.9,
            "vllm_max_num_seqs": 8,
            "vllm_enforce_eager": False,
            "vllm_host": "0.0.0.0",
            "hf_token": "hf_test_secret",
        }
        payload = obj._build_create_payload(cfg)
        env = payload["env"]
        self.assertEqual(env["HF_TOKEN"], "hf_test_secret")
        self.assertNotIn("HUGGING_FACE_HUB_TOKEN", env)
        self.assertEqual(env["-p 8000:8000"], "1")
        self.assertEqual(env.get("PYTORCH_ALLOC_CONF"), "expandable_segments:True")
        onstart = payload["onstart"]
        self.assertTrue(onstart.startswith("if [ -n "))
        self.assertIn("hf auth login --token", onstart)
        self.assertIn("huggingface-cli login --token", onstart)
        self.assertIn("vllm serve org/gated-model", onstart)
        self.assertIn("--dtype float16", onstart)
        self.assertIn("--max-model-len 4096", onstart)
        self.assertIn("--gpu-memory-utilization 0.90", onstart)
        self.assertIn("--max-num-seqs 8", onstart)
        self.assertNotIn("--enforce-eager", onstart)
        self.assertIn('--hf-token "$HF_TOKEN"', onstart)

    def test_build_create_payload_omits_hf_when_empty(self):
        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        cfg = {
            "image": "vastai/vllm:latest",
            "disk_gb": 40,
            "vllm_port": 8000,
            "vllm_model": "google/gemma-2-9b-it",
            "vllm_max_model_len": 8192,
            "vllm_host": "0.0.0.0",
            "hf_token": "",
        }
        payload = obj._build_create_payload(cfg)
        self.assertNotIn("HF_TOKEN", payload["env"])
        self.assertEqual(payload["env"].get("PYTORCH_ALLOC_CONF"), "expandable_segments:True")
        self.assertFalse(payload["onstart"].startswith("if [ -n "))
        self.assertTrue(payload["onstart"].startswith("vllm serve "))
        self.assertNotIn("--hf-token", payload["onstart"])
        self.assertIn("--dtype float16", payload["onstart"])
        self.assertIn("--gpu-memory-utilization", payload["onstart"])
        self.assertIn("--max-num-seqs", payload["onstart"])
        self.assertIn("--enforce-eager", payload["onstart"])

    def test_build_create_payload_uses_ollama_for_gguf(self):
        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        cfg = {
            "image": "ollama/ollama:latest",
            "vllm_model": "ggml-org/gemma-4-E4B-it-GGUF",
            "vllm_port": 8000,
            "disk_gb": 40,
        }
        payload = obj._build_create_payload(cfg)
        onstart = payload["onstart"]
        self.assertIn("ollama serve", onstart)
        self.assertNotIn("ollama pull", onstart)
        self.assertNotIn("vllm serve", onstart)

    def test_normalize_ollama_model_ref(self):
        self.assertEqual(
            VllmRuntimeOrchestrator._normalize_ollama_model_ref("ggml-org/gemma-4-E4B-it-GGUF"),
            "hf.co/ggml-org/gemma-4-E4B-it-GGUF",
        )
        self.assertEqual(
            VllmRuntimeOrchestrator._normalize_ollama_model_ref("hf.co/ggml-org/gemma-4-E4B-it-GGUF"),
            "hf.co/ggml-org/gemma-4-E4B-it-GGUF",
        )
        self.assertEqual(
            VllmRuntimeOrchestrator._normalize_ollama_model_ref("gemma3:4b"),
            "gemma3:4b",
        )

    def test_extract_ssh_access_from_instance_payload(self):
        payload = {
            "ssh_host": "ssh6.vast.ai",
            "ssh_port": 10226,
        }
        access = VllmRuntimeOrchestrator._extract_ssh_access(payload)
        self.assertEqual(access, {"host": "ssh6.vast.ai", "port": 10226, "user": "root"})


if __name__ == "__main__":
    unittest.main()
