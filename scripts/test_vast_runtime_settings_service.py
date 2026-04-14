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
                "is_enabled": True,
            }
        )
        public = self.svc.get_public_settings()
        self.assertEqual(public["vast_api_key"], "abc***789")
        self.assertEqual(public["vllm_api_key"], "zzz***777")
        self.assertTrue(public["is_enabled"])

    def test_update_keeps_existing_secret_when_empty(self):
        self.svc.repository.save_settings({"vast_api_key": "old_secret"})
        self.svc.update_settings({"vast_api_key": "", "max_hourly_usd": 1.25})
        saved = self.svc.get_settings()
        self.assertEqual(saved["vast_api_key"], "old_secret")
        self.assertEqual(saved["max_hourly_usd"], 1.25)


if __name__ == "__main__":
    unittest.main()
