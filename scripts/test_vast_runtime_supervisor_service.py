# -*- coding: utf-8 -*-
"""
Мінімальні unit-тести supervisor-а Vast runtime.

Запуск:
    py -m unittest scripts.test_vast_runtime_supervisor_service
"""

import unittest
from unittest.mock import patch

from business.services.vast_runtime_supervisor_service import VastRuntimeSupervisorService


class VastRuntimeSupervisorServiceTest(unittest.TestCase):
    @staticmethod
    def _build_subject(notify_admins_fn=None):
        obj = VastRuntimeSupervisorService.__new__(VastRuntimeSupervisorService)
        obj.notify_admins_fn = notify_admins_fn
        obj._last_balance_check_ts = 0.0
        obj._low_balance_alert_active = False
        obj.BALANCE_CHECK_INTERVAL_SEC = 0

        class _SettingsSvc:
            @staticmethod
            def get_settings():
                return {"vast_api_key": "test_key"}

        class _Orchestrator:
            _settings_svc = _SettingsSvc()

        obj._orchestrator = _Orchestrator()
        return obj

    def test_low_balance_notification_sent_only_once_while_low(self):
        sent_messages = []
        subject = self._build_subject(lambda message: sent_messages.append(message) or True)

        class _LowBalanceClient:
            def __init__(self, api_key: str, timeout_sec: int = 30) -> None:
                self.api_key = api_key
                self.timeout_sec = timeout_sec

            @staticmethod
            def get_current_user():
                return {"balance": 0.73}

        with patch("business.services.vast_runtime_supervisor_service.VastAiClient", _LowBalanceClient):
            subject._check_low_balance()
            subject._check_low_balance()

        self.assertEqual(len(sent_messages), 1)
        self.assertIn("$0.73", sent_messages[0])
        self.assertTrue(subject._low_balance_alert_active)

    def test_uses_credit_field_before_balance(self):
        sent_messages = []
        subject = self._build_subject(lambda message: sent_messages.append(message) or True)

        class _CreditAwareClient:
            def __init__(self, api_key: str, timeout_sec: int = 30) -> None:
                self.api_key = api_key
                self.timeout_sec = timeout_sec

            @staticmethod
            def get_current_user():
                return {"balance": 0.0, "credit": 1.25}

        with patch("business.services.vast_runtime_supervisor_service.VastAiClient", _CreditAwareClient):
            subject._check_low_balance()

        self.assertEqual(sent_messages, [])
        self.assertFalse(subject._low_balance_alert_active)

    def test_low_balance_alert_resets_after_recovery(self):
        sent_messages = []
        subject = self._build_subject(lambda message: sent_messages.append(message) or True)

        class _FakeClient:
            balances = [0.55, 1.40, 0.49]

            def __init__(self, api_key: str, timeout_sec: int = 30) -> None:
                self.api_key = api_key
                self.timeout_sec = timeout_sec

            @classmethod
            def get_current_user(cls):
                return {"balance": cls.balances.pop(0)}

        with patch("business.services.vast_runtime_supervisor_service.VastAiClient", _FakeClient):
            subject._check_low_balance()
            subject._check_low_balance()
            subject._check_low_balance()

        self.assertEqual(len(sent_messages), 2)
        self.assertIn("$0.55", sent_messages[0])
        self.assertIn("$0.49", sent_messages[1])
        self.assertTrue(subject._low_balance_alert_active)


if __name__ == "__main__":
    unittest.main()
