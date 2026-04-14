# -*- coding: utf-8 -*-
"""
Оркестратор оренди Vast.ai інстанса та готовності vLLM endpoint.
"""

import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Optional

import requests

from business.services.logging_service import LoggingService
from business.services.vast_ai_client import VastAiClient
from business.services.vast_ai_runtime_settings_service import VastRuntimeSettingsService
from data.repositories.gpu_runtime_sessions_repository import GpuRuntimeSessionsRepository


class VllmRuntimeOrchestrator:
    """Керує lifecycle сесії GPU для batch parsing."""

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._settings_svc = VastRuntimeSettingsService()
        self._logging = LoggingService()
        self._sessions = GpuRuntimeSessionsRepository()
        self._instance_id: Optional[str] = None
        self._session_id: Optional[str] = None
        self._endpoint: Optional[str] = None
        self._started_at: Optional[datetime] = None
        self._last_activity_ts: float = 0.0

    def is_enabled(self) -> bool:
        cfg = self._settings_svc.get_settings()
        return bool(cfg.get("is_enabled")) and bool(cfg.get("vast_api_key"))

    def ensure_runtime_ready(self) -> Optional[str]:
        with self._lock:
            cfg = self._settings_svc.get_settings()
            if not cfg.get("is_enabled"):
                return None
            if self._endpoint and self._is_vllm_ready(self._endpoint, timeout_sec=5):
                self._last_activity_ts = time.time()
                return self._endpoint

            if not cfg.get("vast_api_key"):
                raise RuntimeError("Vast.ai API key is not configured.")

            client = VastAiClient(api_key=cfg["vast_api_key"], timeout_sec=min(60, cfg["boot_timeout_sec"]))
            ask_id = self._select_offer_id(client, cfg)
            if not ask_id:
                raise RuntimeError("No Vast.ai offers matched the configured constraints.")

            payload = self._build_create_payload(cfg)
            self._session_id = self._sessions.start_session(
                {
                    "state": "starting",
                    "ask_id": ask_id,
                    "settings_snapshot": self._safe_settings_snapshot(cfg),
                }
            )
            self._log_gpu_usage("gpu_rent_started", {"ask_id": ask_id})
            created = client.create_instance(ask_id=ask_id, payload=payload)
            self._instance_id = str(
                created.get("new_contract")
                or created.get("instance_id")
                or created.get("id")
                or ""
            )
            if not self._instance_id:
                raise RuntimeError("Vast.ai did not return instance id.")

            endpoint = self._wait_for_network_endpoint(client, self._instance_id, cfg["boot_timeout_sec"])
            self._wait_for_vllm_readiness(endpoint, cfg["ready_timeout_sec"])
            self._endpoint = endpoint
            self._started_at = datetime.now(timezone.utc)
            self._last_activity_ts = time.time()
            self._sessions.update_session(
                self._session_id,
                {
                    "state": "running",
                    "instance_id": self._instance_id,
                    "endpoint": endpoint,
                    "ready_at": datetime.now(timezone.utc),
                },
            )
            self._log_gpu_usage(
                "gpu_ready",
                {"instance_id": self._instance_id, "endpoint": endpoint},
            )
            return self._endpoint

    def mark_processing_activity(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._last_activity_ts = time.time()
            self._log_gpu_usage("gpu_processing", metadata or {})

    def handle_pool_drain(self, has_new_tasks_fn) -> None:
        """Політика shutdown: empty -> wait idle_grace -> recheck -> stop."""
        with self._lock:
            if not self._instance_id:
                return
            cfg = self._settings_svc.get_settings()
            if has_new_tasks_fn():
                self._last_activity_ts = time.time()
                return
            idle_grace = int(cfg.get("idle_grace_sec") or 60)
            self._log_gpu_usage("gpu_idle_wait", {"idle_grace_sec": idle_grace})
        time.sleep(max(1, idle_grace))
        with self._lock:
            if has_new_tasks_fn():
                self._last_activity_ts = time.time()
                return
            self._teardown_locked(reason="pool_drained")

    def force_shutdown(self, reason: str = "manual") -> None:
        with self._lock:
            self._teardown_locked(reason=reason)

    def _teardown_locked(self, reason: str) -> None:
        if not self._instance_id:
            return
        cfg = self._settings_svc.get_settings()
        instance_id = self._instance_id
        endpoint = self._endpoint
        try:
            client = VastAiClient(api_key=cfg.get("vast_api_key", ""), timeout_sec=30)
            client.destroy_instance(instance_id)
        except Exception:
            try:
                client = VastAiClient(api_key=cfg.get("vast_api_key", ""), timeout_sec=30)
                client.stop_instance(instance_id)
            except Exception:
                pass
        elapsed_sec = 0.0
        if self._started_at:
            elapsed_sec = max(
                0.0,
                (datetime.now(timezone.utc) - self._started_at).total_seconds(),
            )
        estimated = self._estimate_cost_usd(elapsed_sec, cfg)
        self._log_gpu_usage(
            "gpu_teardown",
            {
                "reason": reason,
                "instance_id": instance_id,
                "endpoint": endpoint,
                "active_seconds": elapsed_sec,
                "estimated_cost_usd": estimated,
            },
        )
        if self._session_id:
            self._sessions.finish_session(self._session_id, "stopped", estimated)
        self._instance_id = None
        self._session_id = None
        self._endpoint = None
        self._started_at = None
        self._last_activity_ts = 0.0

    def _wait_for_network_endpoint(self, client: VastAiClient, instance_id: str, timeout_sec: int) -> str:
        deadline = time.time() + max(30, timeout_sec)
        last_err = "Endpoint was not published."
        while time.time() < deadline:
            try:
                data = client.show_instance(instance_id)
                endpoint = self._extract_http_endpoint(data)
                if endpoint:
                    return endpoint
                last_err = "Waiting for host/port mapping."
            except Exception as e:
                last_err = str(e)
            time.sleep(5)
        raise RuntimeError(f"Vast instance endpoint timeout: {last_err}")

    def _wait_for_vllm_readiness(self, endpoint: str, timeout_sec: int) -> None:
        deadline = time.time() + max(30, timeout_sec)
        last_err = "vLLM readiness check did not pass."
        while time.time() < deadline:
            if self._is_vllm_ready(endpoint, timeout_sec=8):
                return
            last_err = f"Waiting vLLM readiness on {endpoint}"
            time.sleep(4)
        raise RuntimeError(last_err)

    @staticmethod
    def _is_vllm_ready(endpoint: str, timeout_sec: int = 8) -> bool:
        base = endpoint.rstrip("/")
        try:
            r = requests.get(f"{base}/v1/models", timeout=timeout_sec)
            if r.status_code == 200:
                data = r.json()
                models = data.get("data") if isinstance(data, dict) else None
                return isinstance(models, list) and len(models) > 0
        except Exception:
            pass
        return False

    @staticmethod
    def _build_query(cfg: Dict[str, Any]) -> str:
        filters = [
            f"gpu_ram>={int(cfg.get('min_gpu_ram_gb', 0))}",
            f"dph_total<={float(cfg.get('max_hourly_usd', 0.0))}",
            f"reliability>={float(cfg.get('min_reliability', 0.0))}",
            "verified=true",
            "external=false",
        ]
        if cfg.get("gpu_name_like"):
            filters.append(f"gpu_name~{cfg['gpu_name_like']}")
        if cfg.get("region_like"):
            filters.append(f"geolocation~{cfg['region_like']}")
        if cfg.get("datacenter_like"):
            filters.append(f"datacenter~{cfg['datacenter_like']}")
        return " ".join(filters)

    def _select_offer_id(self, client: VastAiClient, cfg: Dict[str, Any]) -> str:
        query = self._build_query(cfg)
        offers = client.search_offers(query=query)
        if not offers:
            return ""
        offers = sorted(
            offers,
            key=lambda x: (
                float(x.get("dph_total") or x.get("dph") or 9999.0),
                -float(x.get("reliability2") or x.get("reliability") or 0.0),
            ),
        )
        best = offers[0]
        return str(best.get("id") or best.get("ask_id") or "")

    def _build_create_payload(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        port = int(cfg.get("vllm_port") or 8000)
        model = cfg.get("vllm_model") or "google/gemma-2-9b-it"
        model_len = int(cfg.get("vllm_max_model_len") or 8192)
        vllm_key = cfg.get("vllm_api_key") or ""
        onstart = (
            f"python -m vllm.entrypoints.openai.api_server "
            f"--host {cfg.get('vllm_host', '0.0.0.0')} "
            f"--port {port} "
            f"--model {model} "
            f"--max-model-len {model_len}"
        )
        if vllm_key:
            onstart += f" --api-key {vllm_key}"
        return {
            "image": cfg.get("image"),
            "disk": int(cfg.get("disk_gb") or 40),
            "runtype": "ssh",
            "onstart": onstart,
            "env": {},
        }

    @staticmethod
    def _extract_http_endpoint(instance_payload: Dict[str, Any]) -> Optional[str]:
        for key in ("public_ipaddr", "public_ip", "ssh_host"):
            ip = instance_payload.get(key)
            if ip:
                break
        else:
            ip = None
        if not ip:
            return None
        direct_port = (
            instance_payload.get("direct_port_start")
            or instance_payload.get("direct_port")
            or 8000
        )
        try:
            p = int(direct_port)
        except (TypeError, ValueError):
            p = 8000
        return f"http://{ip}:{p}"

    def _estimate_cost_usd(self, active_seconds: float, cfg: Dict[str, Any]) -> float:
        per_hour = float(cfg.get("max_hourly_usd") or 0.0)
        if per_hour <= 0 or active_seconds <= 0:
            return 0.0
        return round((active_seconds / 3600.0) * per_hour, 6)

    def _safe_settings_snapshot(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        snap = dict(cfg)
        if snap.get("vast_api_key"):
            snap["vast_api_key"] = "***"
        if snap.get("vllm_api_key"):
            snap["vllm_api_key"] = "***"
        return snap

    def _log_gpu_usage(self, event: str, metadata: Dict[str, Any]) -> None:
        try:
            self._logging.log_api_usage(
                service="gpu_runtime",
                source=event,
                from_cache=False,
                metadata=metadata,
            )
        except Exception:
            pass
