# -*- coding: utf-8 -*-
"""
Інтеграційні тести локального розгортання vLLM (OpenAI-compatible) на цій машині.

Не чіпають Vast.ai: лише HTTP до вже запущеного `vllm serve` / контейнера на localhost.

Увімкнення (інакше всі тести класу пропускаються):

    set PAZUZU_LOCAL_VLLM_SMOKE=1

Опційно:

    set PAZUZU_LOCAL_VLLM_URL=http://127.0.0.1:8000
    set PAZUZU_LOCAL_VLLM_API_KEY=...   (або LLM_API_KEY_VLLM_REMOTE з .env)

Запуск:

    py -m unittest scripts.test_vllm_local_deployment -v
"""

from __future__ import annotations

import os
import sys
import unittest
from typing import Any, Dict

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

import requests

from business.services.vllm_runtime_orchestrator import VllmRuntimeOrchestrator


def _local_smoke_enabled() -> bool:
    v = os.environ.get("PAZUZU_LOCAL_VLLM_SMOKE", "").strip().lower()
    return v in ("1", "true", "yes", "on")


def _local_vllm_base_url() -> str:
    return (os.environ.get("PAZUZU_LOCAL_VLLM_URL") or "http://127.0.0.1:8000").rstrip("/")


def _local_vllm_api_key() -> str:
    return (
        os.environ.get("PAZUZU_LOCAL_VLLM_API_KEY", "").strip()
        or os.environ.get("LLM_API_KEY_VLLM_REMOTE", "").strip()
    )


def _auth_headers(api_key: str) -> Dict[str, str]:
    if not api_key:
        return {}
    return {"Authorization": f"Bearer {api_key}"}


@unittest.skipUnless(
    _local_smoke_enabled(),
    "Set PAZUZU_LOCAL_VLLM_SMOKE=1 and start vLLM locally (e.g. vllm serve ...)",
)
class LocalVllmDeploymentSmokeTest(unittest.TestCase):
    """Перевірки живого endpoint на цій машині."""

    @classmethod
    def setUpClass(cls) -> None:
        cls.base = _local_vllm_base_url()
        cls.api_key = _local_vllm_api_key()
        cls.headers = _auth_headers(cls.api_key)

    def test_01_models_endpoint_returns_200(self) -> None:
        r = requests.get(f"{self.base}/v1/models", timeout=15, headers=self.headers)
        self.assertEqual(
            r.status_code,
            200,
            f"GET /v1/models failed: {r.status_code} {r.text[:400]}",
        )
        data = r.json()
        self.assertIsInstance(data, dict)
        self.assertIn("data", data)
        self.assertIsInstance(data["data"], list)

    def test_02_models_list_non_empty(self) -> None:
        r = requests.get(f"{self.base}/v1/models", timeout=15, headers=self.headers)
        self.assertEqual(r.status_code, 200)
        models = r.json().get("data") or []
        self.assertGreater(
            len(models),
            0,
            "vLLM /v1/models returned empty data[] — сервер ще не завантажив модель?",
        )

    def test_03_orchestrator_readiness_probe_matches(self) -> None:
        self.assertTrue(
            VllmRuntimeOrchestrator._is_vllm_ready(self.base, timeout_sec=15),
            "VllmRuntimeOrchestrator._is_vllm_ready має повертати True для цього endpoint",
        )

    def test_04_minimal_chat_completion(self) -> None:
        r = requests.get(f"{self.base}/v1/models", timeout=15, headers=self.headers)
        self.assertEqual(r.status_code, 200)
        models: Any = r.json().get("data") or []
        self.assertGreater(len(models), 0)
        model_id = models[0].get("id")
        self.assertIsInstance(model_id, str)
        self.assertTrue(model_id.strip())

        payload = {
            "model": model_id,
            "messages": [{"role": "user", "content": "Reply with exactly: OK"}],
            "max_tokens": 16,
            "temperature": 0,
        }
        r2 = requests.post(
            f"{self.base}/v1/chat/completions",
            json=payload,
            timeout=180,
            headers=self.headers,
        )
        self.assertEqual(
            r2.status_code,
            200,
            f"POST /v1/chat/completions failed: {r2.status_code} {r2.text[:600]}",
        )
        body = r2.json()
        choices = body.get("choices")
        self.assertIsInstance(choices, list)
        self.assertGreaterEqual(len(choices), 1)
        msg = (choices[0].get("message") or {}) if isinstance(choices[0], dict) else {}
        content = (msg.get("content") or "").strip()
        self.assertTrue(len(content) > 0, "порожня відповідь моделі")


class LocalVllmDeploymentEnvDocTest(unittest.TestCase):
    """Завжди виконується: перевірка, що модуль імпортується та прапорець smoke визначений."""

    def test_smoke_flag_is_documented_off_by_default(self) -> None:
        # Документуємо очікування: без явного PAZUZU_LOCAL_VLLM_SMOKE live-клас не біжить.
        if _local_smoke_enabled():
            self.assertTrue(_local_vllm_base_url().startswith("http"))
        else:
            self.assertFalse(_local_smoke_enabled())


if __name__ == "__main__":
    unittest.main()
