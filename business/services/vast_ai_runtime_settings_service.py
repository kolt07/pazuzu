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
            "min_gpu_ram_gb": 24,
            "min_reliability": 0.98,
            "target_cuda": "11.8",
            "gpu_name_like": "",
            "region_like": "",
            "datacenter_like": "",
            "disk_gb": 40,
            # Default runtime switches to Ollama template for GGUF models.
            "image": "ollama/ollama:latest",
            "vllm_model": "ggml-org/gemma-4-E4B-it-GGUF",
            "vllm_host": "0.0.0.0",
            "vllm_port": 8000,
            "vllm_max_model_len": 4096,
            "vllm_gpu_memory_utilization": 0.9,
            # Менше CUDA graphs / пік VRAM (див. conserving_memory у vLLM).
            "vllm_enforce_eager": True,
            "vllm_max_num_seqs": 4,
            "vllm_api_key": "",
            "hf_token": "",
            "enable_ssh_tunnel": True,
            "ssh_tunnel_local_port": 8000,
            "ssh_identity_file": "",
            "ssh_instance_log_stream": True,
            "endpoint_timeout_sec": 1200,
            "boot_timeout_sec": 1200,
            "ready_timeout_sec": 1200,
            "idle_grace_sec": 60,
            "pause_after_idle_sec": 60,
            "destroy_after_pause_sec": 1200,
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
        out["hf_token"] = self._mask_secret(out.get("hf_token", ""))
        return out

    def update_settings(self, payload: Dict[str, Any]) -> Dict[str, Any]:
        current = self.get_settings()
        updated = dict(current)
        for key, value in (payload or {}).items():
            if key in ("vast_api_key", "vllm_api_key", "hf_token"):
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
        out["vllm_max_model_len"] = int(out.get("vllm_max_model_len") or 4096)
        gpu_mu = float(out.get("vllm_gpu_memory_utilization") if out.get("vllm_gpu_memory_utilization") is not None else 0.9)
        out["vllm_gpu_memory_utilization"] = max(0.05, min(0.99, gpu_mu))
        _ee = out.get("vllm_enforce_eager")
        out["vllm_enforce_eager"] = True if _ee is None else bool(_ee)
        mns = int(out.get("vllm_max_num_seqs") or 4)
        out["vllm_max_num_seqs"] = max(1, min(256, mns))
        out["enable_ssh_tunnel"] = bool(out.get("enable_ssh_tunnel", True))
        out["ssh_tunnel_local_port"] = int(out.get("ssh_tunnel_local_port") or 8000)
        out["ssh_identity_file"] = str(out.get("ssh_identity_file") or "").strip()
        out["ssh_instance_log_stream"] = bool(out.get("ssh_instance_log_stream", True))
        out["endpoint_timeout_sec"] = int(out.get("endpoint_timeout_sec") or 1200)
        out["boot_timeout_sec"] = int(out.get("boot_timeout_sec") or 1200)
        out["ready_timeout_sec"] = int(out.get("ready_timeout_sec") or 1200)
        out["idle_grace_sec"] = int(out.get("idle_grace_sec") or 60)
        out["pause_after_idle_sec"] = int(out.get("pause_after_idle_sec") or 600)
        out["destroy_after_pause_sec"] = int(out.get("destroy_after_pause_sec") or 600)
        out["hard_budget_usd"] = float(out.get("hard_budget_usd") or 0.0)
        out["vast_api_key"] = str(out.get("vast_api_key") or "").strip()
        out["vllm_api_key"] = str(out.get("vllm_api_key") or "").strip()
        out["hf_token"] = str(out.get("hf_token") or "").strip()
        out["vllm_model"] = str(out.get("vllm_model") or "google/gemma-2-9b-it").strip()
        out["fallback_provider"] = str(out.get("fallback_provider") or "ollama").strip().lower()
        return out
