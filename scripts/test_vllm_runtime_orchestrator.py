# -*- coding: utf-8 -*-
"""
Мінімальні unit-тести оркестратора vLLM runtime.

Запуск:
    py -m unittest scripts.test_vllm_runtime_orchestrator
"""

import unittest
import threading
import time
import requests

from business.services.vllm_runtime_orchestrator import VllmRuntimeOrchestrator


class VllmRuntimeOrchestratorTest(unittest.TestCase):
    @staticmethod
    def _build_idle_lifecycle_subject():
        obj = VllmRuntimeOrchestrator.__new__(VllmRuntimeOrchestrator)
        obj._lock = threading.Lock()
        obj._instance_id = "test-instance"
        obj._instance_paused = False
        obj._empty_queue_since_ts = 0.0
        obj._paused_since_ts = 0.0
        obj._last_activity_ts = 0.0
        obj._log_gpu_usage = lambda *_args, **_kwargs: None
        obj._resume_instance_locked = lambda _cfg: None
        obj._pause_instance_locked = lambda _cfg, reason: None
        obj._teardown_locked = lambda reason: None

        class _Settings:
            @staticmethod
            def get_settings():
                return {
                    "pause_after_idle_sec": 600,
                    "destroy_after_pause_sec": 600,
                }

        obj._settings_svc = _Settings()
        return obj

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

    def test_choose_runtime_instance_to_keep_prefers_running_then_higher_id(self):
        rows = [
            {"id": 1, "cur_state": "stopped"},
            {"id": 2, "cur_state": "running"},
            {"id": 3, "cur_state": "running"},
        ]
        keep = VllmRuntimeOrchestrator._choose_runtime_instance_to_keep(rows)
        self.assertEqual(keep["id"], 3)

    def test_choose_runtime_instance_to_keep_single(self):
        row = {"id": 42, "cur_state": "running"}
        self.assertEqual(VllmRuntimeOrchestrator._choose_runtime_instance_to_keep([row]), row)

    def test_vast_instance_running_state_detection(self):
        self.assertTrue(VllmRuntimeOrchestrator._is_vast_instance_running({"cur_state": "running"}))
        self.assertTrue(VllmRuntimeOrchestrator._is_vast_instance_running({"actual_status": "ACTIVE"}))
        self.assertFalse(VllmRuntimeOrchestrator._is_vast_instance_running({"cur_state": "stopped"}))
        self.assertFalse(VllmRuntimeOrchestrator._is_vast_instance_running({"state": "paused"}))
        self.assertFalse(VllmRuntimeOrchestrator._is_vast_instance_running({}))

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

    def test_handle_pool_drain_pauses_after_idle_timeout(self):
        obj = self._build_idle_lifecycle_subject()
        obj._empty_queue_since_ts = time.time() - 700
        called = {"paused": False, "destroyed": False}

        def _pause(_cfg, reason=None):
            called["paused"] = True
            obj._instance_paused = True

        def _destroy(_reason):
            called["destroyed"] = True

        obj._pause_instance_locked = _pause
        obj._teardown_locked = _destroy
        obj.handle_pool_drain(lambda: False, lambda: True)
        self.assertTrue(called["paused"])
        self.assertFalse(called["destroyed"])

    def test_handle_pool_drain_destroys_paused_instance_when_safe(self):
        obj = self._build_idle_lifecycle_subject()
        obj._instance_paused = True
        obj._empty_queue_since_ts = time.time() - 1300
        obj._paused_since_ts = time.time() - 700
        called = {"destroy_reason": ""}
        obj._teardown_locked = lambda reason: called.update({"destroy_reason": reason})
        obj.handle_pool_drain(lambda: False, lambda: False)
        self.assertEqual(called["destroy_reason"], "idle_paused_timeout")

    def test_handle_pool_drain_resumes_when_new_tasks_appear(self):
        obj = self._build_idle_lifecycle_subject()
        obj._instance_paused = True
        obj._empty_queue_since_ts = time.time() - 50
        obj._paused_since_ts = time.time() - 50
        called = {"resumed": False}

        def _resume(_cfg):
            called["resumed"] = True
            obj._instance_paused = False

        obj._resume_instance_locked = _resume
        obj.handle_pool_drain(lambda: True, lambda: True)
        self.assertTrue(called["resumed"])
        self.assertEqual(obj._empty_queue_since_ts, 0.0)
        self.assertEqual(obj._paused_since_ts, 0.0)


if __name__ == "__main__":
    unittest.main()
