# -*- coding: utf-8 -*-
"""
Сервіс керування runtime-налаштуваннями Vast.ai / vLLM.
"""

import os
from copy import deepcopy
from typing import Any, Dict

from data.repositories.vast_runtime_settings_repository import VastRuntimeSettingsRepository


class VastRuntimeSettingsService:
    """Нормалізує, читає та зберігає параметри оренди GPU."""

    def __init__(self) -> None:
        self.repository = VastRuntimeSettingsRepository()

    @staticmethod
    def default_settings() -> Dict[str, Any]:
        return {
            "is_enabled": False,
            "provider": "vast.ai",
            "vast_api_key": "",
            "max_hourly_usd": 0.85,
            "min_gpu_ram_gb": 20,
            "min_reliability": 0.98,
            "target_cuda": "12.1",
            "gpu_name_like": "",
            "region_like": "",
            "datacenter_like": "",
            "disk_gb": 40,
            "image": "vllm/vllm-openai:latest",
            "vllm_model": "google/gemma-2-9b-it",
            "vllm_host": "0.0.0.0",
            "vllm_port": 8000,
            "vllm_max_model_len": 8192,
            "vllm_api_key": "",
            "boot_timeout_sec": 900,
            "ready_timeout_sec": 600,
            "idle_grace_sec": 60,
            "hard_budget_usd": 20.0,
            "fallback_provider": "ollama",
        }

    def get_settings(self) -> Dict[str, Any]:
        env_api_key = os.getenv("VAST_API_KEY", "")
        stored = self.repository.get_settings() or {}
        merged = self.default_settings()
        merged.update(stored)
        if env_api_key and not merged.get("vast_api_key"):
            merged["vast_api_key"] = env_api_key
        return self._normalize(merged)

    def get_public_settings(self) -> Dict[str, Any]:
        data = self.get_settings()
        out = deepcopy(data)
        out["vast_api_key"] = self._mask_secret(out.get("vast_api_key", ""))
        out["vllm_api_key"] = self._mask_secret(out.get("vllm_api_key", ""))
        return out

    def update_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_settings()
        updated = dict(current)
        for key, value in (payload or {}).items():
            if key in ("vast_api_key", "vllm_api_key"):
                if isinstance(value, str) and value.strip():
                    updated[key] = value.strip()
                continue
            updated[key] = value
        normalized = self._normalize(updated)
        self.repository.save_settings(normalized)
        return self.get_public_settings()

    @staticmethod
    def _mask_secret(secret: str) -> str:
        if not secret:
            return ""
        if len(secret) <= 6:
            return "*" * len(secret)
        return f"{secret[:3]}***{secret[-3:]}"

    @staticmethod
    def _normalize(data: Dict[str, Any]) -> Dict[str, Any]:
        out = dict(data)
        out["is_enabled"] = bool(out.get("is_enabled"))
        out["max_hourly_usd"] = float(out.get("max_hourly_usd") or 0.0)
        out["min_gpu_ram_gb"] = int(out.get("min_gpu_ram_gb") or 0)
        out["min_reliability"] = float(out.get("min_reliability") or 0.0)
        out["disk_gb"] = int(out.get("disk_gb") or 40)
        out["vllm_port"] = int(out.get("vllm_port") or 8000)
        out["vllm_max_model_len"] = int(out.get("vllm_max_model_len") or 8192)
        out["boot_timeout_sec"] = int(out.get("boot_timeout_sec") or 900)
        out["ready_timeout_sec"] = int(out.get("ready_timeout_sec") or 600)
        out["idle_grace_sec"] = int(out.get("idle_grace_sec") or 60)
        out["hard_budget_usd"] = float(out.get("hard_budget_usd") or 0.0)
        out["vast_api_key"] = str(out.get("vast_api_key") or "").strip()
        out["vllm_api_key"] = str(out.get("vllm_api_key") or "").strip()
        out["vllm_model"] = str(out.get("vllm_model") or "google/gemma-2-9b-it").strip()
        out["fallback_provider"] = str(out.get("fallback_provider") or "ollama").strip().lower()
        return out
