# -*- coding: utf-8 -*-
"""
Інтеграційний smoke-тест orchestration policy (mock): start -> process -> idle -> stop.

Запуск:
    py -m unittest scripts.test_vllm_runtime_integration_mock
"""

import unittest
import threading

from business.services.vllm_runtime_orchestrator import VllmRuntimeOrchestrator


class _MockOrchestrator(VllmRuntimeOrchestrator):
    def __init__(self):
        self.events = []
        self._lock = threading.Lock()
        self._instance_id = "mock-instance"
        self._session_id = None
        self._endpoint = "http://127.0.0.1:8000"
        self._started_at = None
        self._last_activity_ts = 0.0

    def _teardown_locked(self, reason: str) -> None:
        self.events.append(("teardown", reason))
        self._instance_id = None
        self._endpoint = None


class VllmRuntimeIntegrationMockTest(unittest.TestCase):
    def test_idle_shutdown_when_no_new_tasks(self):
        orch = _MockOrchestrator()
        checks = {"count": 0}

        def no_tasks():
            checks["count"] += 1
            return False

        # Не чекаємо реальну хвилину: перевизначаємо grace в контексті тесту.
        orch._settings_svc = type("S", (), {"get_settings": lambda self: {"idle_grace_sec": 1}})()
        orch.handle_pool_drain(no_tasks)
        self.assertIn(("teardown", "pool_drained"), orch.events)


if __name__ == "__main__":
    unittest.main()
