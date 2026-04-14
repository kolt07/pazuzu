# -*- coding: utf-8 -*-
"""
Мінімальні unit-тести для runtime налаштувань Vast.ai.

Запуск:
    py -m unittest scripts.test_vast_runtime_settings_service
"""

import unittest

from business.services.vast_ai_runtime_settings_service import VastRuntimeSettingsService


class _FakeRepo:
    def __init__(self):
        self._doc = {}

    def get_settings(self):
        return dict(self._doc)

    def save_settings(self, payload):
        self._doc = dict(payload)
        return "default"


class VastRuntimeSettingsServiceTest(unittest.TestCase):
    def setUp(self):
        self.svc = VastRuntimeSettingsService()
        self.svc.repository = _FakeRepo()

    def test_public_settings_masks_secrets(self):
        self.svc.repository.save_settings(
            {
                "vast_api_key": "abc123456789",
                "vllm_api_key": "zzz999888777",
                "hf_token": "hf_abcdefghijklmnopqrstuvwxyz",
                "is_enabled": True,
            }
        )
        public = self.svc.get_public_settings()
        self.assertEqual(public["vast_api_key"], "abc***789")
        self.assertEqual(public["vllm_api_key"], "zzz***777")
        self.assertEqual(public["hf_token"], "hf_***xyz")
        self.assertTrue(public["is_enabled"])

    def test_update_keeps_existing_secret_when_empty(self):
        self.svc.repository.save_settings({"vast_api_key": "old_secret", "hf_token": "hf_old_secret"})
        self.svc.update_settings({"vast_api_key": "", "hf_token": "", "max_hourly_usd": 1.25})
        saved = self.svc.get_settings()
        self.assertEqual(saved["vast_api_key"], "old_secret")
        self.assertEqual(saved["hf_token"], "hf_old_secret")
        self.assertEqual(saved["max_hourly_usd"], 1.25)

    def test_defaults_include_vram_tuning_for_vllm(self):
        saved = self.svc.get_settings()
        self.assertEqual(saved["target_cuda"], "11.8")
        self.assertEqual(saved["image"], "ollama/ollama:latest")
        self.assertEqual(saved["vllm_model"], "ggml-org/gemma-4-E4B-it-GGUF")
        self.assertTrue(saved["enable_ssh_tunnel"])
        self.assertEqual(saved["ssh_tunnel_local_port"], 8000)
        self.assertTrue(saved["ssh_instance_log_stream"])
        self.assertEqual(saved["endpoint_timeout_sec"], 240)
        self.assertEqual(saved["boot_timeout_sec"], 1200)
        self.assertEqual(saved["ready_timeout_sec"], 1200)
        self.assertEqual(saved["vllm_max_model_len"], 4096)
        self.assertAlmostEqual(saved["vllm_gpu_memory_utilization"], 0.9, places=4)
        self.assertTrue(saved["vllm_enforce_eager"])
        self.assertEqual(saved["vllm_max_num_seqs"], 4)


if __name__ == "__main__":
    unittest.main()
