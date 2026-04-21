# -*- coding: utf-8 -*-
"""
Оркестратор оренди Vast.ai інстанса та готовності vLLM endpoint.
"""

import logging
import threading
import time
import json
import socket
import shutil
import subprocess
import os
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

import requests

from business.services.logging_service import LoggingService
from business.services.vast_ai_client import VastAiClient
from business.services.vast_billing_service import fetch_instance_contract_charges_usd
from business.services.vast_ai_runtime_settings_service import VastRuntimeSettingsService
from data.repositories.gpu_runtime_coordination_repository import GpuRuntimeCoordinationRepository
from data.repositories.gpu_runtime_sessions_repository import GpuRuntimeSessionsRepository

logger = logging.getLogger(__name__)

_SHARED_ORCHESTRATOR_LOCK = threading.Lock()
_SHARED_ORCHESTRATOR: Optional["VllmRuntimeOrchestrator"] = None


def get_shared_vllm_runtime_orchestrator() -> "VllmRuntimeOrchestrator":
    """Process-level singleton to avoid launching duplicate Vast instances."""
    global _SHARED_ORCHESTRATOR
    with _SHARED_ORCHESTRATOR_LOCK:
        if _SHARED_ORCHESTRATOR is None:
            _SHARED_ORCHESTRATOR = VllmRuntimeOrchestrator()
        return _SHARED_ORCHESTRATOR


class VllmRuntimeOrchestrator:
    """Керує lifecycle сесії GPU для batch parsing."""

    VAST_RUNTIME_LABEL_DEFAULT = "pazuzu-vllm-runtime"

    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._settings_svc = VastRuntimeSettingsService()
        self._logging = LoggingService()
        self._sessions = GpuRuntimeSessionsRepository()
        self._coord = GpuRuntimeCoordinationRepository()
        self._owner_id = f"host:{socket.gethostname()}:pid:{os.getpid()}"
        self._instance_id: Optional[str] = None
        self._session_id: Optional[str] = None
        self._endpoint: Optional[str] = None
        self._public_endpoint: Optional[str] = None
        self._ssh_local_endpoint: Optional[str] = None
        self._ssh_tunnel_proc: Optional[subprocess.Popen] = None
        self._ssh_log_proc: Optional[subprocess.Popen] = None
        self._obs_lock = threading.Lock()
        self._recent_observability_logs: List[str] = []
        self._started_at: Optional[datetime] = None
        self._last_activity_ts: float = 0.0
        self._instance_paused: bool = False
        self._empty_queue_since_ts: float = 0.0
        self._paused_since_ts: float = 0.0
        self._runtime_fail_streak: int = 0
        self._forced_check_event = threading.Event()
        self._forced_check_lock = threading.Lock()
        self._forced_check_reason: str = ""
        self._forced_check_running: bool = False
        self._last_forced_check_ts: float = 0.0
        self._forced_check_thread = threading.Thread(
            target=self._forced_check_loop,
            daemon=True,
            name="VllmRuntimeForcedHealthcheck",
        )
        self._forced_check_thread.start()

    def is_enabled(self) -> bool:
        cfg = self._settings_svc.get_settings()
        return bool(cfg.get("is_enabled")) and bool(cfg.get("vast_api_key"))

    def _runtime_instance_label(self, cfg: Dict[str, Any]) -> str:
        label = str(cfg.get("vast_instance_label") or "").strip()
        return label or self.VAST_RUNTIME_LABEL_DEFAULT

    @staticmethod
    def _normalize_vast_instance_id(raw: Any) -> str:
        if raw is None:
            return ""
        if isinstance(raw, int):
            return str(raw)
        return str(raw).strip()

    def _instances_matching_runtime_label(self, client: VastAiClient, cfg: Dict[str, Any]) -> List[Dict[str, Any]]:
        want = self._runtime_instance_label(cfg)
        out: List[Dict[str, Any]] = []
        try:
            for row in client.list_instances():
                if not isinstance(row, dict):
                    continue
                if str(row.get("label") or "").strip() != want:
                    continue
                out.append(row)
        except Exception as e:
            self._log_gpu_usage(
                "gpu_vast_list_failed",
                {"error": str(e)},
            )
        return out

    @staticmethod
    def _choose_runtime_instance_to_keep(instances: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not instances:
            return None
        if len(instances) == 1:
            return instances[0]

        def score(row: Dict[str, Any]) -> tuple:
            st = str(row.get("cur_state") or row.get("actual_status") or "").lower()
            running = 1 if st == "running" else 0
            try:
                iid = int(row.get("id") or 0)
            except (TypeError, ValueError):
                iid = 0
            return (running, iid)

        return max(instances, key=score)

    def _enforce_singleton_vast_instances(self, client: VastAiClient, cfg: Dict[str, Any]) -> None:
        """За API Vast залишає не більше одного інстанса з міткою runtime (джерело істини — Vast)."""
        rows = self._instances_matching_runtime_label(client, cfg)
        if len(rows) <= 1:
            return
        keep = self._choose_runtime_instance_to_keep(rows)
        if not keep:
            return
        keep_id = self._normalize_vast_instance_id(keep.get("id"))
        for row in rows:
            rid = self._normalize_vast_instance_id(row.get("id"))
            if not rid or rid == keep_id:
                continue
            try:
                client.destroy_instance(rid)
                logger.warning(
                    "Vast singleton: знищено дублікат інстанса %s (залишаємо %s)",
                    rid,
                    keep_id,
                )
                self._log_gpu_usage(
                    "gpu_vast_duplicate_destroyed",
                    {"keep_instance_id": keep_id, "destroyed_instance_id": rid},
                )
            except Exception as e:
                self._log_gpu_usage(
                    "gpu_vast_duplicate_destroy_failed",
                    {"instance_id": rid, "error": str(e)},
                )

    def _get_singleton_runtime_instance(self, client: VastAiClient, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        rows = self._instances_matching_runtime_label(client, cfg)
        if len(rows) != 1:
            return None
        row = rows[0]
        return row if isinstance(row, dict) else None

    @staticmethod
    def _extract_vast_instance_state(payload: Dict[str, Any]) -> str:
        if not isinstance(payload, dict):
            return ""
        for key in ("cur_state", "actual_status", "state", "status"):
            raw = payload.get(key)
            if raw is None:
                continue
            state = str(raw).strip().lower()
            if state:
                return state
        return ""

    @classmethod
    def _is_vast_instance_running(cls, payload: Dict[str, Any]) -> bool:
        state = cls._extract_vast_instance_state(payload)
        # Vast може повертати різні назви станів залежно від endpoint.
        return state in {"running", "active", "online"}

    def _execute_contract_boot_steps(
        self,
        client: VastAiClient,
        cfg: Dict[str, Any],
        instance_id: str,
        idx: int,
        session_id: str,
    ) -> str:
        """Мережа + observability + readiness після того, як контракт у Vast уже існує."""
        self._instance_id = str(instance_id)
        self._coord_update_state(state="starting", instance_id=self._instance_id, endpoint=None, public_endpoint=None)
        endpoint_timeout_sec = int(
            cfg.get("endpoint_timeout_sec") or min(1200, int(cfg.get("boot_timeout_sec") or 1200))
        )
        network_info = self._wait_for_network_endpoint_info(
            client,
            self._instance_id,
            endpoint_timeout_sec,
            int(cfg.get("vllm_port") or 8000),
            heartbeat_fn=lambda: self._coord_renew_lease(cfg, state="starting"),
        )
        public_endpoint = network_info["endpoint"]
        instance_payload = network_info["payload"]
        self._public_endpoint = public_endpoint
        self._coord_update_state(
            state="network_ready",
            instance_id=self._instance_id,
            public_endpoint=public_endpoint,
            endpoint=None,
        )
        self._log_gpu_usage(
            "gpu_instance_booted",
            {
                "instance_id": self._instance_id,
                "public_endpoint": public_endpoint,
                "attempt": idx,
            },
        )
        self._start_instance_observability(public_endpoint, instance_payload, cfg)
        if self._use_ollama_runtime(cfg):
            endpoint = self._wait_for_ollama_control_readiness(public_endpoint, cfg["boot_timeout_sec"], cfg)
            endpoint = self._ensure_ollama_model_ready(endpoint, public_endpoint, cfg["ready_timeout_sec"], cfg)
        else:
            endpoint = self._wait_for_runtime_readiness(public_endpoint, cfg["ready_timeout_sec"], cfg)
        self._endpoint = endpoint
        self._started_at = datetime.now(timezone.utc)
        self._last_activity_ts = time.time()
        self._coord_update_state(
            state="running",
            instance_id=self._instance_id,
            endpoint=endpoint,
            public_endpoint=public_endpoint,
            last_error="",
        )
        self._sessions.update_session(
            session_id,
            {
                "state": "running",
                "instance_id": self._instance_id,
                "endpoint": endpoint,
                "public_endpoint": public_endpoint,
                "ready_at": datetime.now(timezone.utc),
                "attempt": idx,
            },
        )
        self._log_gpu_usage(
            "gpu_ready",
            {
                "instance_id": self._instance_id,
                "endpoint": endpoint,
                "public_endpoint": public_endpoint,
                "attempt": idx,
                "ssh_tunnel_enabled": bool(self._ssh_local_endpoint),
            },
        )
        return self._endpoint or ""

    def ensure_runtime_ready(self) -> Optional[str]:
        with self._lock:
            cfg = self._settings_svc.get_settings()
            if not cfg.get("is_enabled"):
                return None
            logger.info(
                "[gpu-runtime] прогрів: перевірка готовності інстансу/Ollama (ensure_runtime_ready)…"
            )
            self._empty_queue_since_ts = 0.0
            self._paused_since_ts = 0.0
            self._coord_update_state(
                state="queue_active",
                queue_empty_since_at=None,
                queue_last_seen_at=self._utcnow(),
            )
            if self._instance_id and self._instance_paused:
                self._resume_instance_locked(cfg)
                if self._endpoint and self._is_runtime_ready(self._endpoint, cfg, timeout_sec=8):
                    self._last_activity_ts = time.time()
                    logger.info(
                        "[gpu-runtime] прогрів не потрібен (після resume): endpoint=%s",
                        self._endpoint,
                    )
                    return self._endpoint
            if self._endpoint and self._is_runtime_ready(self._endpoint, cfg, timeout_sec=5):
                self._coord_renew_lease(cfg, state="running")
                self._last_activity_ts = time.time()
                logger.info(
                    "[gpu-runtime] прогрів не потрібен (endpoint уже готовий): %s",
                    self._endpoint,
                )
                return self._endpoint

            if not cfg.get("vast_api_key"):
                raise RuntimeError("Vast.ai API key is not configured.")

            client = VastAiClient(api_key=cfg["vast_api_key"], timeout_sec=min(60, cfg["boot_timeout_sec"]))
            self._enforce_singleton_vast_instances(client, cfg)
            singleton = self._get_singleton_runtime_instance(client, cfg)
            singleton_id = self._normalize_vast_instance_id((singleton or {}).get("id")) if singleton else ""
            if singleton_id:
                singleton_state = self._extract_vast_instance_state(singleton or {})
                logger.info(
                    "[gpu-runtime] прогрів: використовуємо наявний контракт Vast instance_id=%s state=%s",
                    singleton_id,
                    singleton_state or "unknown",
                )
                self._session_id = self._sessions.start_session(
                    {
                        "state": "starting",
                        "ask_id": "",
                        "attempt": 0,
                        "settings_snapshot": self._safe_settings_snapshot(cfg),
                        "vast_adopt": True,
                    }
                )
                try:
                    # Узгодження state-машини між процесами:
                    # якщо singleton у Vast не running (paused/stopped), обов'язково
                    # виконуємо resume ПЕРЕД очікуванням runtime readiness.
                    self._instance_id = singleton_id
                    if not self._is_vast_instance_running(singleton or {}):
                        self._instance_paused = True
                        self._resume_instance_locked(cfg)
                        if self._endpoint and self._is_runtime_ready(self._endpoint, cfg, timeout_sec=8):
                            self._last_activity_ts = time.time()
                            logger.info(
                                "[gpu-runtime] singleton resumed і готовий: endpoint=%s",
                                self._endpoint,
                            )
                            return self._endpoint
                    return self._execute_contract_boot_steps(client, cfg, singleton_id, 0, self._session_id)
                except Exception as e:
                    last_error = str(e)
                    if self._session_id:
                        self._sessions.finish_session(self._session_id, "failed")
                    self._log_gpu_usage(
                        "gpu_startup_failed",
                        {
                            "instance_id": singleton_id,
                            "attempt": 0,
                            "stage": "vast_singleton_bootstrap",
                            "error": last_error,
                        },
                    )
                    self._coord_update_state(
                        state="failed",
                        instance_id=singleton_id,
                        endpoint=None,
                        public_endpoint=None,
                        last_error=last_error,
                    )
                    raise RuntimeError(
                        f"Існуючий Vast-інстанс із міткою {self._runtime_instance_label(cfg)} не вдалося довести до readiness: {last_error}"
                    ) from e

            if not self._coord_try_acquire(cfg, state="starting"):
                shared_endpoint = self._wait_for_shared_runtime_or_acquire(cfg, client)
                if shared_endpoint:
                    self._last_activity_ts = time.time()
                    logger.info(
                        "[gpu-runtime] використано спільний endpoint іншого процесу: %s",
                        shared_endpoint,
                    )
                    return shared_endpoint
            ask_ids = self._select_offer_candidates(client, cfg, limit=5)
            if not ask_ids:
                self._coord_release(state="idle", last_error="No Vast.ai offers matched the configured constraints.")
                raise RuntimeError("No Vast.ai offers matched the configured constraints.")

            payload = self._build_create_payload(cfg)
            last_error = ""
            for idx, ask_id in enumerate(ask_ids, start=1):
                self._session_id = self._sessions.start_session(
                    {
                        "state": "starting",
                        "ask_id": ask_id,
                        "attempt": idx,
                        "settings_snapshot": self._safe_settings_snapshot(cfg),
                    }
                )
                self._log_gpu_usage("gpu_rent_started", {"ask_id": ask_id, "attempt": idx, "candidates": len(ask_ids)})
                created = None
                create_error: Optional[Exception] = None
                for create_try in range(1, 4):
                    try:
                        created = client.create_instance(ask_id=ask_id, payload=payload)
                        create_error = None
                        break
                    except Exception as e:
                        create_error = e
                        if self._is_rate_limited_error(e) and create_try < 3:
                            wait_sec = min(12, create_try * 3)
                            self._log_gpu_usage(
                                "gpu_rent_rate_limited",
                                {
                                    "ask_id": ask_id,
                                    "attempt": idx,
                                    "create_try": create_try,
                                    "wait_sec": wait_sec,
                                },
                            )
                            time.sleep(wait_sec)
                            continue
                        break
                if create_error is not None or not isinstance(created, dict):
                    last_error = f"create_instance failed for ask_id={ask_id}: {create_error!s}"
                    if self._session_id:
                        self._sessions.finish_session(self._session_id, "failed")
                    self._log_gpu_usage(
                        "gpu_rent_failed",
                        {"ask_id": ask_id, "attempt": idx, "error": last_error},
                    )
                    continue
                self._instance_id = str(
                    created.get("new_contract")
                    or created.get("instance_id")
                    or created.get("id")
                    or ""
                )
                if not self._instance_id:
                    if self._session_id:
                        self._sessions.finish_session(self._session_id, "failed")
                    last_error = "Vast.ai did not return instance id."
                    self._coord_update_state(state="failed", last_error=last_error)
                    continue
                try:
                    return self._execute_contract_boot_steps(client, cfg, self._instance_id, idx, self._session_id)
                except Exception as e:
                    last_error = str(e)
                    startup_stage = "endpoint_publish"
                    lowered_error = last_error.lower()
                    if "ollama" in lowered_error and ("loadable" in lowered_error or "pull" in lowered_error or "model" in lowered_error):
                        startup_stage = "model_ready"
                    elif "readiness" in lowered_error or "control endpoint" in lowered_error or "/v1/models" in lowered_error:
                        startup_stage = "runtime_ready"
                    self._log_gpu_usage(
                        "gpu_startup_failed",
                        {
                            "instance_id": self._instance_id,
                            "attempt": idx,
                            "stage": startup_stage,
                            "error": last_error,
                        },
                    )
                    self._coord_update_state(
                        state="failed",
                        instance_id=self._instance_id,
                        endpoint=self._endpoint,
                        public_endpoint=self._public_endpoint,
                        last_error=last_error,
                    )
                    # Не залишаємо інстанс завислим при помилці старту/ready-check.
                    self._teardown_locked(reason=f"startup_failed_attempt_{idx}")
                    continue
            self._coord_release(state="failed", last_error=last_error)
            raise RuntimeError(f"All Vast startup attempts failed. Last error: {last_error}")

    def mark_processing_activity(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._last_activity_ts = time.time()
            self._empty_queue_since_ts = 0.0
            self._paused_since_ts = 0.0
            cfg = self._settings_svc.get_settings()
            self._coord_renew_lease(cfg, state="running")
            self._log_gpu_usage("gpu_processing", metadata or {})

    def get_cached_runtime_endpoint(self, wait_timeout_sec: int = 0) -> Optional[str]:
        """Повертає endpoint без синхронного health-check; джерело істини — локальний/coord state."""
        deadline = time.time() + max(0, int(wait_timeout_sec))
        while True:
            cfg = self._settings_svc.get_settings()
            if not cfg.get("is_enabled"):
                return None
            with self._lock:
                if self._endpoint and not self._instance_paused:
                    return self._endpoint
            try:
                state = self._coord.get_runtime_state() or {}
            except Exception:
                state = {}
            shared_state = str(state.get("state") or "").strip().lower()
            shared_endpoint = str(state.get("endpoint") or state.get("public_endpoint") or "").strip()
            if shared_endpoint and shared_state == "running":
                with self._lock:
                    self._instance_id = str(state.get("instance_id") or self._instance_id or "")
                    self._public_endpoint = str(state.get("public_endpoint") or self._public_endpoint or "")
                    self._endpoint = shared_endpoint
                    self._instance_paused = False
                    return self._endpoint
            if time.time() >= deadline:
                return None
            time.sleep(0.2)

    def report_inference_success(self, metadata: Optional[Dict[str, Any]] = None) -> None:
        with self._lock:
            self._runtime_fail_streak = 0
        self.mark_processing_activity(metadata)

    def report_inference_failure(self, error: str, metadata: Optional[Dict[str, Any]] = None) -> None:
        cfg = self._settings_svc.get_settings()
        threshold = max(1, int(cfg.get("forced_healthcheck_after_failures") or 3))
        with self._lock:
            self._runtime_fail_streak += 1
            streak = self._runtime_fail_streak
        fail_meta = dict(metadata or {})
        fail_meta.update({"error": str(error), "consecutive_failures": streak, "threshold": threshold})
        self._log_gpu_usage("gpu_inference_failed", fail_meta)
        if streak >= threshold:
            self.schedule_forced_healthcheck(reason=f"llm_consecutive_failures_{streak}")

    def schedule_forced_healthcheck(self, reason: str) -> None:
        cfg = self._settings_svc.get_settings()
        cooldown_sec = max(0, int(cfg.get("forced_healthcheck_cooldown_sec") or 30))
        now_ts = time.time()
        with self._forced_check_lock:
            if self._forced_check_running:
                self._forced_check_reason = reason or self._forced_check_reason
                return
            if cooldown_sec > 0 and (now_ts - self._last_forced_check_ts) < cooldown_sec:
                return
            self._forced_check_reason = reason or "manual"
            self._last_forced_check_ts = now_ts
            self._forced_check_event.set()

    def _forced_check_loop(self) -> None:
        while True:
            self._forced_check_event.wait()
            with self._forced_check_lock:
                self._forced_check_event.clear()
                reason = self._forced_check_reason or "unknown"
                self._forced_check_reason = ""
                self._forced_check_running = True
            try:
                self._log_gpu_usage("gpu_forced_healthcheck_started", {"reason": reason})
                self.ensure_runtime_ready()
                self._log_gpu_usage("gpu_forced_healthcheck_ready", {"reason": reason})
                with self._lock:
                    self._runtime_fail_streak = 0
            except Exception as e:
                self._log_gpu_usage(
                    "gpu_forced_healthcheck_failed",
                    {"reason": reason, "error": str(e)},
                )
            finally:
                with self._forced_check_lock:
                    self._forced_check_running = False

    def mark_source_load_activity(self) -> None:
        with self._lock:
            state = "paused" if self._instance_paused else ("running" if self._instance_id else "idle")
            self._coord_update_state(state=state, last_source_load_activity_at=self._utcnow())

    def handle_pool_drain(self, has_new_tasks_fn, has_active_initiators_fn=None) -> None:
        """Політика idle: active(10m)->pause(10m)->destroy(if no initiators)."""
        with self._lock:
            if not self._instance_id:
                self._empty_queue_since_ts = 0.0
                self._paused_since_ts = 0.0
                self._instance_paused = False
                return
            cfg = self._settings_svc.get_settings()
            now = time.time()
            has_active_initiators = bool(has_active_initiators_fn()) if callable(has_active_initiators_fn) else False
            if has_new_tasks_fn():
                if self._instance_paused:
                    self._resume_instance_locked(cfg)
                self._last_activity_ts = time.time()
                self._empty_queue_since_ts = 0.0
                self._paused_since_ts = 0.0
                self._coord_update_state(
                    state="running" if not self._instance_paused else "paused",
                    queue_empty_since_at=None,
                    queue_last_seen_at=self._utcnow(),
                )
                return

            pause_after_sec = int(cfg.get("pause_after_idle_sec") or 600)
            destroy_after_pause_sec = int(cfg.get("destroy_after_pause_sec") or 600)
            if self._empty_queue_since_ts <= 0:
                self._empty_queue_since_ts = now
                self._coord_update_state(queue_empty_since_at=self._utcnow(), queue_last_seen_at=self._utcnow(), state="idle_wait")
                self._log_gpu_usage(
                    "gpu_idle_wait",
                    {
                        "idle_grace_sec": pause_after_sec,
                        "stage": "active_before_pause",
                    },
                )
                return

            idle_for = max(0, int(now - self._empty_queue_since_ts))
            if not self._instance_paused:
                if idle_for < pause_after_sec:
                    return
                self._pause_instance_locked(cfg, reason="idle_queue_pause")
                self._paused_since_ts = time.time()
                return

            if self._paused_since_ts <= 0:
                self._paused_since_ts = now
                return

            paused_for = max(0, int(now - self._paused_since_ts))
            if paused_for < destroy_after_pause_sec:
                return
            if has_active_initiators or self._has_recent_shared_source_load_activity(within_sec=20 * 60):
                self._log_gpu_usage(
                    "gpu_destroy_delayed",
                    {
                        "reason": "active_source_load",
                        "paused_for_sec": paused_for,
                        "instance_id": self._instance_id,
                    },
                )
                return
            self._teardown_locked(reason="idle_paused_timeout")

    def force_shutdown(self, reason: str = "manual") -> None:
        with self._lock:
            self._teardown_locked(reason=reason)

    def get_observability_status(self) -> Dict[str, Any]:
        # Non-blocking snapshot: ensure_runtime_ready may hold _lock for long startup phases.
        # Reading primitive attrs without lock here is acceptable for observability purposes.
        tunnel_proc = self._ssh_tunnel_proc
        stream_proc = self._ssh_log_proc
        tunnel_alive = bool(tunnel_proc and tunnel_proc.poll() is None)
        stream_alive = bool(stream_proc and stream_proc.poll() is None)
        endpoint = self._endpoint
        public_endpoint = self._public_endpoint
        local_endpoint = self._ssh_local_endpoint
        effective_endpoint = endpoint or self._select_effective_endpoint(public_endpoint)
        endpoint_source = "runtime_ready" if endpoint else ("ssh_tunnel" if effective_endpoint == local_endpoint and effective_endpoint else ("public" if effective_endpoint == public_endpoint and effective_endpoint else "none"))
        instance_id = self._instance_id
        instance_paused = self._instance_paused
        with self._obs_lock:
            recent_logs = list(self._recent_observability_logs[-40:])
        return {
            "instance_id": instance_id,
            "instance_paused": instance_paused,
            "endpoint": endpoint,
            "effective_endpoint": effective_endpoint,
            "effective_endpoint_source": endpoint_source,
            "public_endpoint": public_endpoint,
            "ssh_local_endpoint": local_endpoint,
            "ssh_tunnel_enabled": bool(local_endpoint),
            "ssh_tunnel_alive": tunnel_alive,
            "ssh_log_stream_alive": stream_alive,
            "recent_logs": recent_logs,
        }

    def _select_effective_endpoint(self, public_endpoint: Optional[str]) -> Optional[str]:
        local_endpoint = self._ssh_local_endpoint
        if local_endpoint and self._is_ssh_tunnel_alive():
            return local_endpoint
        return public_endpoint

    def _is_ssh_tunnel_alive(self) -> bool:
        proc = self._ssh_tunnel_proc
        return bool(proc and proc.poll() is None)

    def _resolve_runtime_endpoint(self, public_endpoint: str, previous_endpoint: Optional[str], phase: str) -> str:
        endpoint = self._select_effective_endpoint(public_endpoint)
        if not endpoint:
            raise RuntimeError("Runtime endpoint is unavailable: no ssh tunnel and no public endpoint.")
        if previous_endpoint and previous_endpoint != endpoint:
            self._log_gpu_usage(
                "gpu_endpoint_switched",
                {
                    "phase": phase,
                    "from_endpoint": previous_endpoint,
                    "to_endpoint": endpoint,
                    "public_endpoint": public_endpoint,
                    "ssh_local_endpoint": self._ssh_local_endpoint,
                    "reason": "ssh_tunnel_unavailable" if endpoint == public_endpoint else "ssh_tunnel_restored",
                },
            )
        return endpoint

    def _teardown_locked(self, reason: str) -> None:
        if not self._instance_id:
            return
        cfg = self._settings_svc.get_settings()
        instance_id = self._instance_id
        endpoint = self._endpoint
        logger.warning(
            "Vast GPU teardown: reason=%s instance_id=%s endpoint=%s",
            reason,
            instance_id,
            endpoint or "",
        )
        self._stop_ssh_processes_locked()
        elapsed_sec = 0.0
        if self._started_at:
            elapsed_sec = max(
                0.0,
                (datetime.now(timezone.utc) - self._started_at).total_seconds(),
            )
        billed_usd, billed_err = (None, None)
        if self._started_at:
            gte = int(self._started_at.timestamp())
            lte = int(datetime.now(timezone.utc).timestamp())
            billed_usd, billed_err = fetch_instance_contract_charges_usd(
                cfg.get("vast_api_key", ""),
                str(instance_id),
                gte,
                lte,
            )
        effective_billed = float(billed_usd) if billed_usd is not None else 0.0
        try:
            client = VastAiClient(api_key=cfg.get("vast_api_key", ""), timeout_sec=30)
            client.destroy_instance(instance_id)
        except Exception:
            try:
                client = VastAiClient(api_key=cfg.get("vast_api_key", ""), timeout_sec=30)
                client.stop_instance(instance_id)
            except Exception:
                pass
        meta: Dict[str, Any] = {
            "reason": reason,
            "instance_id": instance_id,
            "endpoint": endpoint,
            "active_seconds": elapsed_sec,
            "billed_cost_usd": effective_billed,
            "cost_source": "vast_charges",
        }
        if billed_err:
            meta["vast_billing_error"] = billed_err
        self._log_gpu_usage("gpu_teardown", meta)
        if self._session_id:
            self._sessions.finish_session(self._session_id, "stopped", billed_cost_usd=effective_billed)
        self._instance_id = None
        self._session_id = None
        self._endpoint = None
        self._public_endpoint = None
        self._instance_paused = False
        self._empty_queue_since_ts = 0.0
        self._paused_since_ts = 0.0
        self._started_at = None
        self._last_activity_ts = 0.0
        self._coord_release(
            state="idle",
            instance_id=None,
            endpoint=None,
            public_endpoint=None,
            last_error="",
        )

    def _pause_instance_locked(self, cfg: Dict[str, Any], reason: str) -> None:
        if not self._instance_id or self._instance_paused:
            return
        instance_id = self._instance_id
        endpoint = self._endpoint or self._public_endpoint
        self._stop_ssh_processes_locked()
        try:
            client = VastAiClient(api_key=cfg.get("vast_api_key", ""), timeout_sec=30)
            client.stop_instance(instance_id)
        except Exception as e:
            self._log_gpu_usage(
                "gpu_pause_failed",
                {
                    "instance_id": instance_id,
                    "reason": reason,
                    "error": str(e),
                },
            )
            return
        self._instance_paused = True
        self._endpoint = None
        self._coord_update_state(
            state="paused",
            instance_id=instance_id,
            endpoint=None,
            public_endpoint=self._public_endpoint,
            paused_at=self._utcnow(),
        )
        self._log_gpu_usage(
            "gpu_paused",
            {
                "instance_id": instance_id,
                "reason": reason,
                "endpoint": endpoint,
            },
        )
        if self._session_id:
            self._sessions.update_session(
                self._session_id,
                {
                    "state": "paused",
                    "paused_at": datetime.now(timezone.utc),
                },
            )

    def _resume_instance_locked(self, cfg: Dict[str, Any]) -> None:
        if not self._instance_id:
            return
        instance_id = self._instance_id
        client = VastAiClient(api_key=cfg.get("vast_api_key", ""), timeout_sec=min(60, int(cfg.get("boot_timeout_sec") or 900)))
        resume_started_ts = time.time()
        try:
            client.start_instance(instance_id)
        except Exception:
            # Якщо інстанс already running — ігноруємо та переходимо до перевірки endpoint.
            pass
        endpoint_timeout_sec = int(cfg.get("endpoint_timeout_sec") or min(1200, int(cfg.get("boot_timeout_sec") or 1200)))
        try:
            network_info = self._wait_for_network_endpoint_info(
                client,
                instance_id,
                endpoint_timeout_sec,
                int(cfg.get("vllm_port") or 8000),
                heartbeat_fn=lambda: self._coord_renew_lease(cfg, state="resuming"),
            )
        except Exception as resume_error:
            wakeup_timeout_sec = int(cfg.get("sleep_wakeup_timeout_sec") or 300)
            waited_sec = max(0, int(time.time() - resume_started_ts))
            migration_enabled = bool(cfg.get("sleep_migration_enabled", True))
            should_migrate = migration_enabled and wakeup_timeout_sec > 0 and waited_sec >= wakeup_timeout_sec
            if not should_migrate:
                raise
            self._log_gpu_usage(
                "gpu_sleep_migration_triggered",
                {
                    "instance_id": instance_id,
                    "waited_sec": waited_sec,
                    "wakeup_timeout_sec": wakeup_timeout_sec,
                    "error": str(resume_error),
                },
            )
            network_info = self._migrate_sleeping_instance_to_new_contract_locked(
                client=client,
                cfg=cfg,
                source_instance_id=instance_id,
                wakeup_error=resume_error,
            )
            instance_id = str(self._instance_id or instance_id)
        public_endpoint = network_info["endpoint"]
        instance_payload = network_info["payload"]
        self._public_endpoint = public_endpoint
        self._coord_update_state(
            state="network_ready",
            instance_id=instance_id,
            public_endpoint=public_endpoint,
            endpoint=None,
        )
        self._start_instance_observability(public_endpoint, instance_payload, cfg)
        if self._use_ollama_runtime(cfg):
            endpoint = self._wait_for_ollama_control_readiness(public_endpoint, int(cfg.get("boot_timeout_sec") or 1200), cfg)
            endpoint = self._ensure_ollama_model_ready(endpoint, public_endpoint, int(cfg.get("ready_timeout_sec") or 1200), cfg)
        else:
            endpoint = self._wait_for_runtime_readiness(public_endpoint, int(cfg.get("ready_timeout_sec") or 1200), cfg)
        self._endpoint = endpoint
        self._instance_paused = False
        self._coord_update_state(
            state="running",
            instance_id=instance_id,
            endpoint=endpoint,
            public_endpoint=public_endpoint,
            last_error="",
            paused_at=None,
            queue_empty_since_at=None,
            queue_last_seen_at=self._utcnow(),
        )
        self._log_gpu_usage(
            "gpu_resumed",
            {
                "instance_id": instance_id,
                "endpoint": endpoint,
                "public_endpoint": public_endpoint,
            },
        )
        if self._session_id:
            self._sessions.update_session(
                self._session_id,
                {
                    "state": "running",
                    "endpoint": endpoint,
                    "public_endpoint": public_endpoint,
                    "resumed_at": datetime.now(timezone.utc),
                },
            )

    @staticmethod
    def _sleep_migration_copy_paths(cfg: Dict[str, Any]) -> List[str]:
        raw = cfg.get("sleep_migration_copy_paths") or ["/workspace/", "/root/.ollama/"]
        if isinstance(raw, str):
            values = [x.strip() for x in raw.split(",")]
        elif isinstance(raw, (list, tuple, set)):
            values = [str(x).strip() for x in raw]
        else:
            values = ["/workspace/"]
        out = [x for x in values if x]
        return out or ["/workspace/"]

    def _create_instance_with_retry_candidates(self, client: VastAiClient, cfg: Dict[str, Any]) -> str:
        ask_ids = self._select_offer_candidates(client, cfg, limit=5)
        if not ask_ids:
            raise RuntimeError("No Vast.ai offers matched the configured constraints for sleep migration.")
        payload = self._build_create_payload(cfg)
        last_error = ""
        for ask_id in ask_ids:
            try:
                created = client.create_instance(ask_id=ask_id, payload=payload)
                new_id = self._normalize_vast_instance_id(
                    created.get("new_contract") or created.get("instance_id") or created.get("id")
                )
                if new_id:
                    return new_id
                last_error = f"Vast did not return instance id for ask_id={ask_id}"
            except Exception as e:
                last_error = f"create_instance failed for ask_id={ask_id}: {e!s}"
        raise RuntimeError(last_error or "Failed to create destination instance for sleep migration.")

    def _migrate_sleeping_instance_to_new_contract_locked(
        self,
        client: VastAiClient,
        cfg: Dict[str, Any],
        source_instance_id: str,
        wakeup_error: Exception,
    ) -> Dict[str, Any]:
        copy_paths = self._sleep_migration_copy_paths(cfg)
        destination_instance_id = self._create_instance_with_retry_candidates(client, cfg)
        try:
            endpoint_timeout_sec = int(cfg.get("endpoint_timeout_sec") or min(1200, int(cfg.get("boot_timeout_sec") or 1200)))
            network_info = self._wait_for_network_endpoint_info(
                client,
                destination_instance_id,
                endpoint_timeout_sec,
                int(cfg.get("vllm_port") or 8000),
                heartbeat_fn=lambda: self._coord_renew_lease(cfg, state="sleep_migration_bootstrap"),
            )
            for path in copy_paths:
                copy_resp = client.copy_direct(
                    src_id=source_instance_id,
                    dst_id=destination_instance_id,
                    src_path=path,
                    dst_path=path,
                )
                self._log_gpu_usage(
                    "gpu_sleep_migration_copy_started",
                    {
                        "source_instance_id": source_instance_id,
                        "destination_instance_id": destination_instance_id,
                        "path": path,
                        "copy_response": copy_resp,
                    },
                )
            settle_sec = max(0, int(cfg.get("sleep_migration_settle_sec") or 45))
            if settle_sec > 0:
                time.sleep(settle_sec)
            try:
                client.destroy_instance(source_instance_id)
                self._log_gpu_usage(
                    "gpu_sleep_migration_source_destroyed",
                    {
                        "source_instance_id": source_instance_id,
                        "destination_instance_id": destination_instance_id,
                    },
                )
            except Exception as e:
                self._log_gpu_usage(
                    "gpu_sleep_migration_source_destroy_failed",
                    {
                        "source_instance_id": source_instance_id,
                        "destination_instance_id": destination_instance_id,
                        "error": str(e),
                    },
                )
            self._instance_id = destination_instance_id
            self._instance_paused = False
            self._coord_update_state(
                state="resuming",
                instance_id=destination_instance_id,
                endpoint=None,
                public_endpoint=None,
                last_error="",
            )
            self._log_gpu_usage(
                "gpu_sleep_migration_done",
                {
                    "source_instance_id": source_instance_id,
                    "destination_instance_id": destination_instance_id,
                    "copy_paths": copy_paths,
                    "wakeup_error": str(wakeup_error),
                },
            )
            return network_info
        except Exception:
            try:
                client.destroy_instance(destination_instance_id)
            except Exception:
                pass
            raise

    def _wait_for_network_endpoint_info(
        self,
        client: VastAiClient,
        instance_id: str,
        timeout_sec: int,
        service_port: int,
        heartbeat_fn=None,
    ) -> Dict[str, Any]:
        deadline = time.time() + max(30, timeout_sec)
        last_err = "Endpoint was not published."
        while time.time() < deadline:
            if callable(heartbeat_fn):
                heartbeat_fn()
            try:
                data = client.show_instance(instance_id)
                payload = data
                if isinstance(data, dict) and isinstance(data.get("instances"), dict):
                    payload = data.get("instances") or {}
                endpoint = self._extract_http_endpoint(payload, service_port=service_port)
                if endpoint:
                    return {"endpoint": endpoint, "payload": payload}
                last_err = f"Waiting host/port mapping for service port {service_port}."
            except Exception as e:
                last_err = str(e)
            time.sleep(5)
        raise RuntimeError(f"Vast instance endpoint timeout: {last_err}")

    def _start_instance_observability(self, public_endpoint: str, instance_payload: Dict[str, Any], cfg: Dict[str, Any]) -> None:
        self._ssh_local_endpoint = None
        self._stop_ssh_processes_locked()
        if bool(cfg.get("enable_ssh_tunnel", True)):
            self._start_ssh_tunnel(public_endpoint, instance_payload, cfg)
        if bool(cfg.get("ssh_instance_log_stream", True)):
            self._start_ssh_instance_log_stream(instance_payload, cfg)

    @staticmethod
    def _extract_port_mapping(instance_payload: Dict[str, Any], container_port: int) -> Optional[int]:
        if not isinstance(instance_payload, dict):
            return None
        ports = instance_payload.get("ports")
        if not isinstance(ports, dict):
            return None
        candidates = ports.get(f"{int(container_port)}/tcp")
        if not isinstance(candidates, list):
            return None
        for entry in candidates:
            if not isinstance(entry, dict):
                continue
            host_port = entry.get("HostPort")
            try:
                p = int(host_port)
                if 1 <= p <= 65535:
                    return p
            except (TypeError, ValueError):
                continue
        return None

    @staticmethod
    def _extract_ssh_access(instance_payload: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not isinstance(instance_payload, dict):
            return None
        host = (
            instance_payload.get("ssh_host")
            or instance_payload.get("public_ipaddr")
            or instance_payload.get("public_ip")
        )
        if not host:
            return None
        mapped_ssh_port = VllmRuntimeOrchestrator._extract_port_mapping(instance_payload, container_port=22)
        port = mapped_ssh_port or (
            instance_payload.get("external_ssh_port")
            or instance_payload.get("ssh_port")
            or instance_payload.get("ssh_port_start")
            or 22
        )
        try:
            ssh_port = int(port)
        except (TypeError, ValueError):
            ssh_port = 22
        if ssh_port <= 0:
            ssh_port = 22
        return {"host": str(host), "port": ssh_port, "user": "root"}

    @staticmethod
    def _pick_free_local_port(start_port: int, max_attempts: int = 30) -> int:
        base = max(1024, int(start_port))
        for idx in range(max_attempts):
            candidate = base + idx
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            try:
                sock.bind(("127.0.0.1", candidate))
                return candidate
            except OSError:
                continue
            finally:
                sock.close()
        raise RuntimeError(f"No free local port found near {start_port}")

    def _start_ssh_tunnel(self, public_endpoint: str, instance_payload: Dict[str, Any], cfg: Dict[str, Any]) -> None:
        ssh_bin = self._resolve_ssh_binary(cfg)
        if not ssh_bin:
            logger.warning(
                "[gpu-runtime] SSH-клієнт не знайдено (ssh). Для Docker: додайте пакет openssh-client у образ "
                "або задайте vast_runtime.ssh_binary / змінну PAZUZU_SSH_BINARY."
            )
            self._log_gpu_usage(
                "gpu_ssh_tunnel_unavailable",
                {"reason": "ssh_binary_not_found", "public_endpoint": public_endpoint},
            )
            return
        access = self._extract_ssh_access(instance_payload)
        if not access:
            self._log_gpu_usage(
                "gpu_ssh_tunnel_unavailable",
                {"reason": "ssh_access_not_found", "public_endpoint": public_endpoint},
            )
            return
        remote_port = int(cfg.get("vllm_port") or 8000)
        preferred_local_port = int(cfg.get("ssh_tunnel_local_port") or remote_port)
        local_port = self._pick_free_local_port(preferred_local_port)
        cmd = [
            ssh_bin,
            "-N",
            "-L",
            f"{local_port}:127.0.0.1:{remote_port}",
            "-p",
            str(access["port"]),
            f'{access["user"]}@{access["host"]}',
        ]
        cmd.extend(self._build_ssh_common_options(cfg))
        cmd.extend(
            [
                "-o",
                "ExitOnForwardFailure=yes",
                "-o",
                "ServerAliveInterval=20",
                "-o",
                "ServerAliveCountMax=3",
            ]
        )
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self._ssh_tunnel_proc = proc
        self._spawn_process_logger(proc, "vast_ssh_tunnel")
        time.sleep(1.0)
        if proc.poll() is not None:
            self._log_gpu_usage(
                "gpu_ssh_tunnel_failed",
                {
                    "public_endpoint": public_endpoint,
                    "host": access["host"],
                    "port": access["port"],
                },
            )
            self._ssh_tunnel_proc = None
            return
        self._ssh_local_endpoint = f"http://127.0.0.1:{local_port}"
        self._log_gpu_usage(
            "gpu_ssh_tunnel_ready",
            {
                "public_endpoint": public_endpoint,
                "local_endpoint": self._ssh_local_endpoint,
                "host": access["host"],
                "port": access["port"],
            },
        )

    def _start_ssh_instance_log_stream(self, instance_payload: Dict[str, Any], cfg: Dict[str, Any]) -> None:
        ssh_bin = self._resolve_ssh_binary(cfg)
        if not ssh_bin:
            return
        access = self._extract_ssh_access(instance_payload)
        if not access:
            return
        remote_cmd = (
            "bash -lc '"
            "if command -v docker >/dev/null 2>&1; then "
            "cid=$(docker ps -q | head -n 1); "
            "if [ -n \"$cid\" ]; then docker logs -f --tail 120 \"$cid\"; "
            "else echo \"pazuzu: no running docker container yet\"; fi; "
            "else echo \"pazuzu: docker is unavailable on host\"; fi'"
        )
        cmd = [
            ssh_bin,
            "-p",
            str(access["port"]),
            f'{access["user"]}@{access["host"]}',
            remote_cmd,
        ]
        cmd[4:4] = self._build_ssh_common_options(cfg)
        proc = subprocess.Popen(
            cmd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True,
            encoding="utf-8",
            errors="replace",
        )
        self._ssh_log_proc = proc
        self._spawn_process_logger(proc, "vast_instance_log")
        self._log_gpu_usage(
            "gpu_instance_log_stream_started",
            {"host": access["host"], "port": access["port"]},
        )

    def _spawn_process_logger(self, proc: subprocess.Popen, source: str) -> None:
        def _read_stream(stream, channel: str) -> None:
            if stream is None:
                return
            try:
                for raw_line in stream:
                    line = (raw_line or "").rstrip()
                    if not line:
                        continue
                    self._append_recent_observability_log(source, channel, line)
                    self._log_gpu_usage(
                        source,
                        {"channel": channel, "line": line[:1000]},
                    )
            except Exception:
                pass

        threading.Thread(target=_read_stream, args=(proc.stdout, "stdout"), daemon=True).start()
        threading.Thread(target=_read_stream, args=(proc.stderr, "stderr"), daemon=True).start()

    @staticmethod
    def _known_hosts_null_path() -> str:
        # OpenSSH on Windows expects NUL instead of /dev/null.
        return "NUL" if os.name == "nt" else os.devnull

    @staticmethod
    def _resolve_ssh_identity_file(cfg: Dict[str, Any]) -> str:
        value = str(cfg.get("ssh_identity_file") or os.getenv("PAZUZU_SSH_IDENTITY_FILE") or "").strip()
        if not value:
            return ""
        expanded = os.path.expanduser(os.path.expandvars(value))
        return expanded if os.path.isfile(expanded) else ""

    @staticmethod
    def _resolve_ssh_binary(cfg: Dict[str, Any]) -> Optional[str]:
        """Шлях до ssh: vast_runtime.ssh_binary, PAZUZU_SSH_BINARY, PATH, типові місця на Windows."""
        explicit = str(cfg.get("ssh_binary") or os.getenv("PAZUZU_SSH_BINARY") or "").strip()
        if explicit:
            expanded = os.path.expanduser(os.path.expandvars(explicit))
            if os.path.isfile(expanded):
                return expanded
            found = shutil.which(expanded)
            if found:
                return found
            base = os.path.basename(expanded)
            if base and base != expanded:
                found = shutil.which(base)
                if found:
                    return found
        found = shutil.which("ssh")
        if found:
            return found
        if os.name == "nt":
            for candidate in (
                r"C:\Windows\System32\OpenSSH\ssh.exe",
                r"C:\Program Files\Git\usr\bin\ssh.exe",
            ):
                if os.path.isfile(candidate):
                    return candidate
        return None

    def _build_ssh_common_options(self, cfg: Dict[str, Any]) -> List[str]:
        options: List[str] = [
            "-o",
            "BatchMode=yes",
            "-o",
            "StrictHostKeyChecking=no",
            "-o",
            f"UserKnownHostsFile={self._known_hosts_null_path()}",
        ]
        identity_file = self._resolve_ssh_identity_file(cfg)
        if identity_file:
            options.extend(
                [
                    "-i",
                    identity_file,
                    "-o",
                    "IdentitiesOnly=yes",
                ]
            )
        return options

    def _stop_ssh_processes_locked(self) -> None:
        for attr in ("_ssh_log_proc", "_ssh_tunnel_proc"):
            proc = getattr(self, attr, None)
            if not proc:
                continue
            try:
                if proc.poll() is None:
                    proc.terminate()
                    try:
                        proc.wait(timeout=3)
                    except Exception:
                        proc.kill()
            except Exception:
                pass
            setattr(self, attr, None)
        self._ssh_local_endpoint = None

    def _append_recent_observability_log(self, source: str, channel: str, line: str) -> None:
        with self._obs_lock:
            self._recent_observability_logs.append(f"[{source}:{channel}] {line}")
            if len(self._recent_observability_logs) > 400:
                self._recent_observability_logs = self._recent_observability_logs[-200:]

    def _wait_for_runtime_readiness(self, public_endpoint: str, timeout_sec: int, cfg: Dict[str, Any]) -> str:
        deadline = time.time() + max(30, timeout_sec)
        runtime_name = "Ollama" if self._use_ollama_runtime(cfg) else "vLLM"
        last_err = f"{runtime_name} readiness check did not pass."
        endpoint: Optional[str] = None
        while time.time() < deadline:
            self._coord_renew_lease(cfg, state="runtime_ready")
            endpoint = self._resolve_runtime_endpoint(public_endpoint, endpoint, phase="ready_check")
            if self._is_runtime_ready(endpoint, cfg, timeout_sec=8):
                self._log_gpu_usage(
                    "gpu_runtime_endpoint_ready",
                    {
                        "runtime": "vllm",
                        "endpoint": endpoint,
                    },
                )
                return endpoint
            last_err = f"Waiting {runtime_name} readiness on {endpoint}"
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
    def _is_ollama_ready(endpoint: str, timeout_sec: int = 8) -> bool:
        base = endpoint.rstrip("/")
        try:
            r = requests.get(f"{base}/api/tags", timeout=timeout_sec)
            if r.status_code == 200:
                return True
        except Exception:
            pass
        return False

    def _is_runtime_ready(self, endpoint: str, cfg: Dict[str, Any], timeout_sec: int = 8) -> bool:
        if self._use_ollama_runtime(cfg):
            model = self._normalize_ollama_model_ref(str(cfg.get("vllm_model") or ""))
            return self._is_ollama_model_available(endpoint, model, timeout_sec=timeout_sec)
        return self._is_vllm_ready(endpoint, timeout_sec=timeout_sec)

    @staticmethod
    def _is_ollama_model_available(endpoint: str, model: str, timeout_sec: int = 8) -> bool:
        if not model:
            return False
        base = endpoint.rstrip("/")
        try:
            r = requests.post(
                f"{base}/api/show",
                json={"model": model},
                timeout=timeout_sec,
            )
            return r.status_code == 200
        except Exception:
            return False

    def _wait_for_ollama_control_readiness(self, public_endpoint: str, timeout_sec: int, cfg: Dict[str, Any]) -> str:
        deadline = time.time() + max(30, timeout_sec)
        endpoint: Optional[str] = None
        last_err = "Waiting Ollama control endpoint."
        last_wait_log_ts = 0.0
        while time.time() < deadline:
            self._coord_renew_lease(cfg, state="ollama_control_ready")
            endpoint = self._resolve_runtime_endpoint(public_endpoint, endpoint, phase="ollama_control_ready")
            if self._is_ollama_ready(endpoint, timeout_sec=8):
                self._log_gpu_usage(
                    "gpu_runtime_endpoint_ready",
                    {
                        "runtime": "ollama",
                        "endpoint": endpoint,
                    },
                )
                return endpoint
            now_ts = time.time()
            if now_ts - last_wait_log_ts >= 30:
                self._log_gpu_usage(
                    "gpu_runtime_endpoint_wait",
                    {
                        "runtime": "ollama",
                        "endpoint": endpoint,
                    },
                )
                last_wait_log_ts = now_ts
            if not cfg.get("enable_ssh_tunnel", True) and public_endpoint:
                endpoint = public_endpoint
            time.sleep(3)
        raise RuntimeError(last_err)

    def _ensure_ollama_model_ready(self, endpoint: str, public_endpoint: str, timeout_sec: int, cfg: Dict[str, Any]) -> str:
        model = self._normalize_ollama_model_ref(str(cfg.get("vllm_model") or ""))
        if not model:
            raise RuntimeError("Ollama model is empty in runtime settings.")
        availability_timeout = min(60, max(20, timeout_sec // 3))
        repaired_once = False
        while True:
            if not self._is_ollama_model_available(endpoint, model, timeout_sec=8):
                self._log_gpu_usage(
                    "gpu_model_pull_started",
                    {
                        "runtime": "ollama",
                        "model": model,
                        "endpoint": endpoint,
                    },
                )
                try:
                    self._trigger_ollama_pull_with_feedback(endpoint, model, timeout_sec)
                except RuntimeError:
                    fallback_endpoint = self._resolve_runtime_endpoint(public_endpoint, endpoint, phase="ollama_pull_retry")
                    if fallback_endpoint == endpoint:
                        raise
                    self._log_gpu_usage(
                        "gpu_model_pull_retry",
                        {
                            "runtime": "ollama",
                            "model": model,
                            "from_endpoint": endpoint,
                            "to_endpoint": fallback_endpoint,
                        },
                    )
                    endpoint = fallback_endpoint
                    self._trigger_ollama_pull_with_feedback(endpoint, model, timeout_sec)
                if not self._wait_for_ollama_model_available(endpoint, model, timeout_sec=availability_timeout):
                    raise RuntimeError(f"Ollama pull completed but model is not available: {model}")

            loadable, load_error = self._wait_for_ollama_model_loadable(endpoint, model, timeout_sec=75)
            if loadable:
                break
            if repaired_once or not self._is_ollama_blob_load_error(load_error):
                raise RuntimeError(f"Ollama model is not loadable: {load_error or model}")
            self._log_gpu_usage(
                "gpu_model_repair_started",
                {
                    "runtime": "ollama",
                    "model": model,
                    "endpoint": endpoint,
                    "reason": "blob_load_error",
                },
            )
            self._delete_ollama_model(endpoint, model, timeout_sec=20)
            self._log_gpu_usage(
                "gpu_model_repair_pull",
                {
                    "runtime": "ollama",
                    "model": model,
                    "endpoint": endpoint,
                },
            )
            self._trigger_ollama_pull_with_feedback(endpoint, model, timeout_sec)
            if not self._wait_for_ollama_model_available(endpoint, model, timeout_sec=availability_timeout):
                raise RuntimeError(f"Ollama repair pull completed but model is not available: {model}")
            repaired_once = True
        self._log_gpu_usage(
            "gpu_model_pull_ready",
            {
                "runtime": "ollama",
                "model": model,
                "endpoint": endpoint,
            },
        )
        return endpoint

    def _wait_for_ollama_model_available(self, endpoint: str, model: str, timeout_sec: int = 30) -> bool:
        deadline = time.time() + max(10, int(timeout_sec))
        while time.time() < deadline:
            self._coord_renew_lease(self._settings_svc.get_settings(), state="model_available_wait")
            if self._is_ollama_model_available(endpoint, model, timeout_sec=8):
                return True
            time.sleep(2)
        return False

    @staticmethod
    def _is_ollama_blob_load_error(message: str) -> bool:
        txt = str(message or "").lower()
        return ("unable to load model" in txt) or ("models/blobs/sha256" in txt)

    def _check_ollama_model_loadability(self, endpoint: str, model: str, timeout_sec: int = 45) -> tuple[bool, str]:
        base = endpoint.rstrip("/")
        try:
            resp = requests.post(
                f"{base}/api/generate",
                json={
                    "model": model,
                    "prompt": "hello",
                    "stream": False,
                    "options": {"num_predict": 1},
                },
                timeout=timeout_sec,
            )
        except requests.RequestException as e:
            return False, str(e)
        if resp.status_code == 200:
            return True, ""
        body = ""
        try:
            parsed = resp.json()
            if isinstance(parsed, dict):
                body = str(parsed.get("error") or parsed)
            else:
                body = str(parsed)
        except Exception:
            body = (resp.text or "").strip()
        return False, f"http={resp.status_code} {body[:500]}".strip()

    def _wait_for_ollama_model_loadable(self, endpoint: str, model: str, timeout_sec: int = 75) -> tuple[bool, str]:
        deadline = time.time() + max(15, int(timeout_sec))
        last_error = ""
        while time.time() < deadline:
            self._coord_renew_lease(self._settings_svc.get_settings(), state="model_loadable_wait")
            ok, error = self._check_ollama_model_loadability(endpoint, model, timeout_sec=45)
            if ok:
                return True, ""
            last_error = error
            time.sleep(4)
        return False, last_error

    def _delete_ollama_model(self, endpoint: str, model: str, timeout_sec: int = 20) -> None:
        base = endpoint.rstrip("/")
        errors: List[str] = []
        for method in ("delete", "post"):
            try:
                req = getattr(requests, method)
                resp = req(
                    f"{base}/api/delete",
                    json={"model": model},
                    timeout=timeout_sec,
                )
                if resp.status_code in (200, 202, 204, 404):
                    return
                errors.append(f"{method.upper()} {resp.status_code}: {(resp.text or '')[:180]}")
            except requests.RequestException as e:
                errors.append(f"{method.upper()} {e}")
        raise RuntimeError(f"Ollama delete failed for {model}: {' | '.join(errors)}")

    def _trigger_ollama_pull_with_feedback(self, endpoint: str, model: str, timeout_sec: int) -> None:
        base = endpoint.rstrip("/")
        try:
            with requests.post(
                f"{base}/api/pull",
                json={"model": model, "stream": True},
                stream=True,
                timeout=(10, max(30, timeout_sec)),
            ) as resp:
                if resp.status_code != 200:
                    body = ""
                    try:
                        body = resp.text[:400]
                    except Exception:
                        body = ""
                    raise RuntimeError(f"Ollama pull failed ({resp.status_code}): {body}")
                last_status = ""
                last_progress_log = 0.0
                deadline = time.time() + max(30, timeout_sec)
                for line in resp.iter_lines(decode_unicode=True):
                    self._coord_renew_lease(self._settings_svc.get_settings(), state="model_pull")
                    if time.time() > deadline:
                        raise RuntimeError(f"Ollama pull timeout for {model}")
                    if not line:
                        continue
                    try:
                        payload = json.loads(line)
                    except Exception:
                        continue
                    status = str(payload.get("status") or "").strip()
                    completed = payload.get("completed")
                    total = payload.get("total")
                    now = time.time()
                    should_log = (
                        status != last_status
                        or (isinstance(completed, (int, float)) and isinstance(total, (int, float)) and now - last_progress_log >= 8)
                    )
                    if should_log:
                        meta: Dict[str, Any] = {
                            "runtime": "ollama",
                            "model": model,
                            "status": status or "unknown",
                        }
                        if isinstance(completed, (int, float)) and isinstance(total, (int, float)) and total > 0:
                            meta["progress_ratio"] = round(float(completed) / float(total), 4)
                        self._log_gpu_usage("gpu_model_pull_progress", meta)
                        last_status = status
                        last_progress_log = now
                    if payload.get("error"):
                        raise RuntimeError(f"Ollama pull error: {payload.get('error')}")
                    if status.lower() == "success":
                        return
                    if payload.get("done") is True:
                        return
                if self._is_ollama_model_available(endpoint, model, timeout_sec=8):
                    return
                raise RuntimeError(f"Ollama pull stream ended before done=true for {model}")
        except requests.RequestException as e:
            if self._is_ollama_model_available(endpoint, model, timeout_sec=8):
                return
            raise RuntimeError(f"Ollama pull request failed: {e}") from e

    @staticmethod
    def _to_float(value: Any, default: float = 0.0) -> float:
        try:
            return float(value)
        except (TypeError, ValueError):
            return default

    @staticmethod
    def _effective_min_gpu_ram_gb(cfg: Dict[str, Any]) -> int:
        """Raise RAM floor for models that are known to be tight on 22 GB cards."""
        configured = int(float(cfg.get("min_gpu_ram_gb", 0) or 0))
        model = str(cfg.get("vllm_model") or "").strip().lower()
        if "gemma-2-9b" in model:
            return max(configured, 24)
        return configured

    @staticmethod
    def _build_offer_filters(cfg: Dict[str, Any]) -> Dict[str, Any]:
        min_gpu_ram_mb = int(VllmRuntimeOrchestrator._effective_min_gpu_ram_gb(cfg) * 1024)
        min_inet_down_mbps = max(0.0, VllmRuntimeOrchestrator._to_float(cfg.get("min_inet_down_mbps", 700), 700.0))
        filters: Dict[str, Any] = {
            "limit": 100,
            "verified": {"eq": True},
            "rentable": {"eq": True},
            "rented": {"eq": False},
            "external": {"eq": False},
            # vLLM/PyTorch image requires at least Volta-level CUDA capability (sm_70+).
            "compute_cap": {"gte": 700},
            "order": [["dph_total", "asc"]],
        }
        if min_inet_down_mbps > 0:
            # Vast docs: inet_down / inet_up are in Mbps.
            filters["inet_down"] = {"gte": min_inet_down_mbps}
        if min_gpu_ram_mb > 0:
            filters["gpu_ram"] = {"gte": min_gpu_ram_mb}
        max_hourly = float(cfg.get("max_hourly_usd", 0.0) or 0.0)
        if max_hourly > 0:
            filters["dph_total"] = {"lte": max_hourly}
        min_reliability = float(cfg.get("min_reliability", 0.0) or 0.0)
        if min_reliability > 0:
            filters["reliability"] = {"gte": min_reliability}
        gpu_name_like = (cfg.get("gpu_name_like") or "").strip()
        if gpu_name_like:
            filters["gpu_name"] = {"eq": gpu_name_like}
        return filters

    @staticmethod
    def _preferred_tokens(raw_value: Any) -> List[str]:
        if raw_value is None:
            return []
        if isinstance(raw_value, (list, tuple, set)):
            values = raw_value
        else:
            values = str(raw_value).split(",")
        out: List[str] = []
        for item in values:
            token = str(item or "").strip().lower()
            if token:
                out.append(token)
        return out

    @classmethod
    def _offer_matches_terms(cls, offer: Dict[str, Any], field_names: List[str], preferred_terms: List[str]) -> bool:
        if not preferred_terms:
            return False
        haystacks = [str(offer.get(field_name) or "").strip().lower() for field_name in field_names]
        return any(term in hay for term in preferred_terms for hay in haystacks if hay)

    @staticmethod
    def _hf_hub_login_shell_prefix() -> str:
        """Bash prefix: persist Hub token before `vllm serve`.

        Per Hugging Face Hub docs, `hf auth login --token` stores the token under
        ~/.cache/huggingface/token and validates it; some stacks pick that up
        more reliably than env alone. Prefer ``hf``, fall back to ``huggingface-cli``.
        https://huggingface.co/docs/huggingface_hub/guides/cli#hf-auth-login
        """
        return (
            'if [ -n "${HF_TOKEN:-}" ]; then '
            "if command -v hf >/dev/null 2>&1; then hf auth login --token \"$HF_TOKEN\"; "
            "elif command -v huggingface-cli >/dev/null 2>&1; then "
            'huggingface-cli login --token "$HF_TOKEN"; '
            'else echo "pazuzu: HF_TOKEN is set but neither hf nor huggingface-cli is in PATH" >&2; '
            "exit 1; "
            "fi; fi && "
        )

    @staticmethod
    def _is_offer_gpu_compatible(offer: Dict[str, Any]) -> bool:
        """Skip legacy GPUs that are commonly incompatible with current PyTorch/vLLM builds."""
        name = str(offer.get("gpu_name") or "").strip().lower()
        if not name:
            return True
        incompatible_tokens = (
            "tesla p40",
            "tesla p100",
            "tesla k80",
            "tesla m40",
            "tesla m60",
            "quadro p",
        )
        return not any(token in name for token in incompatible_tokens)

    def _select_offer_candidates(self, client: VastAiClient, cfg: Dict[str, Any], limit: int = 5) -> List[str]:
        filters = self._build_offer_filters(cfg)
        offers = client.search_offers(filters=filters)
        if not offers:
            return []
        preferred_inet_down_mbps = max(0.0, self._to_float(cfg.get("preferred_inet_down_mbps", 1000), 1000.0))
        min_inet_down_mbps = max(0.0, self._to_float(cfg.get("min_inet_down_mbps", 700), 700.0))
        preferred_regions = self._preferred_tokens(cfg.get("region_like"))
        preferred_datacenters = self._preferred_tokens(cfg.get("datacenter_like"))
        ranked = sorted(
            offers,
            key=lambda x: (
                0 if self._offer_matches_terms(x, ["datacenter", "geolocation", "geoloc"], preferred_datacenters) else 1,
                0 if self._offer_matches_terms(x, ["geolocation", "geoloc", "region", "country"], preferred_regions) else 1,
                0 if self._to_float(x.get("inet_down"), 0.0) >= min_inet_down_mbps else 1,
                abs(self._to_float(x.get("inet_down"), 0.0) - preferred_inet_down_mbps),
                -self._to_float(x.get("inet_down"), 0.0),
                float(x.get("dph_total") or x.get("dph") or 9999.0),
                -float(x.get("reliability2") or x.get("reliability") or 0.0),
            ),
        )
        compatible = [o for o in ranked if self._is_offer_gpu_compatible(o)]
        pool = compatible if compatible else ranked
        out: List[str] = []
        for offer in pool:
            oid = str(offer.get("id") or offer.get("ask_id") or "").strip()
            if not oid:
                continue
            out.append(oid)
            if len(out) >= max(1, int(limit)):
                break
        return out

    def _select_offer_id(self, client: VastAiClient, cfg: Dict[str, Any]) -> str:
        candidates = self._select_offer_candidates(client, cfg, limit=1)
        return candidates[0] if candidates else ""

    @staticmethod
    def _is_rate_limited_error(error: Exception) -> bool:
        if isinstance(error, requests.HTTPError):
            resp = getattr(error, "response", None)
            if resp is not None and int(getattr(resp, "status_code", 0) or 0) == 429:
                return True
        txt = str(error).lower()
        return "429" in txt and "too many requests" in txt

    @staticmethod
    def _use_ollama_runtime(cfg: Dict[str, Any]) -> bool:
        image = str(cfg.get("image") or "").strip().lower()
        model = str(cfg.get("vllm_model") or "").strip().lower()
        return ("ollama" in image) or ("gguf" in model)

    @staticmethod
    def _normalize_ollama_model_ref(model: str) -> str:
        """Normalize model reference for `ollama pull`.

        Ollama library names like `gemma3:4b` stay unchanged.
        Hugging Face repos should be referenced as `hf.co/<repo>`.
        """
        ref = str(model or "").strip()
        if not ref:
            return ref
        lowered = ref.lower()
        if lowered.startswith("hf.co/"):
            return ref
        if "/" in ref:
            return f"hf.co/{ref}"
        return ref

    def _build_create_payload(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        port = int(cfg.get("vllm_port") or 8000)
        model = cfg.get("vllm_model") or "google/gemma-2-9b-it"
        model_len = int(cfg.get("vllm_max_model_len") or 4096)
        gpu_mu = float(cfg.get("vllm_gpu_memory_utilization") if cfg.get("vllm_gpu_memory_utilization") is not None else 0.9)
        gpu_mu = max(0.05, min(0.99, gpu_mu))
        max_num_seqs = int(cfg.get("vllm_max_num_seqs") or 4)
        max_num_seqs = max(1, min(256, max_num_seqs))
        _ee = cfg.get("vllm_enforce_eager")
        enforce_eager = True if _ee is None else bool(_ee)
        vllm_key = cfg.get("vllm_api_key") or ""
        hf_token = str(cfg.get("hf_token") or "").strip()
        hub_login = self._hf_hub_login_shell_prefix() if hf_token else ""
        if self._use_ollama_runtime(cfg):
            # 2-phase startup: first bring control endpoint up, then pull model via API with progress feedback.
            onstart = f"OLLAMA_HOST=0.0.0.0:{port} ollama serve"
        else:
            onstart = hub_login + (
                f"vllm serve {model} "
                f"--host {cfg.get('vllm_host', '0.0.0.0')} "
                f"--port {port} "
                f"--dtype float16 "
                f"--max-model-len {model_len} "
                f"--gpu-memory-utilization {gpu_mu:.2f} "
                f"--max-num-seqs {max_num_seqs}"
            )
            if enforce_eager:
                onstart += " --enforce-eager"
            if vllm_key:
                onstart += f" --api-key {vllm_key}"
            if hf_token:
                # vLLM CLI treats `--hf-token true` as the literal string "true" → 401 on Hub.
                # Передаємо той самий секрет, що вже в env для контейнера (розгортання $HF_TOKEN на хості).
                # Див. https://docs.vllm.ai/en/stable/cli/serve/#-hf-token
                onstart += ' --hf-token "$HF_TOKEN"'
        env: Dict[str, Any] = {
            f"-p {port}:{port}": "1",
            # Зменшує фрагментацію CUDA allocator (рекомендація з traceback PyTorch).
            "PYTORCH_ALLOC_CONF": "expandable_segments:True",
        }
        if hf_token:
            # Official variable; HUGGING_FACE_HUB_TOKEN is deprecated (huggingface_hub docs).
            env["HF_TOKEN"] = hf_token
        return {
            "image": cfg.get("image"),
            "disk": int(cfg.get("disk_gb") or 40),
            "runtype": "ssh_direct",
            "cancel_unavail": True,
            "label": self._runtime_instance_label(cfg),
            "onstart": onstart,
            "env": env,
        }

    @staticmethod
    def _extract_http_endpoint(instance_payload: Dict[str, Any], service_port: int = 8000) -> Optional[str]:
        if not isinstance(instance_payload, dict):
            return None
        for key in ("public_ipaddr", "public_ip", "ssh_host"):
            ip = instance_payload.get(key)
            if ip:
                break
        else:
            ip = None
        if not ip:
            return None
        # Prefer explicit Docker/NAT mapping for the requested service port.
        mapped_port = VllmRuntimeOrchestrator._extract_port_mapping(instance_payload, container_port=service_port)
        if mapped_port:
            return f"http://{ip}:{mapped_port}"
        direct_port = (
            instance_payload.get("direct_port_start")
            or instance_payload.get("direct_port")
        )
        try:
            p = int(direct_port)
        except (TypeError, ValueError):
            p = 0
        if p <= 0 or p >= 65535:
            return None
        return f"http://{ip}:{p}"

    def _safe_settings_snapshot(self, cfg: Dict[str, Any]) -> Dict[str, Any]:
        snap = dict(cfg)
        if snap.get("vast_api_key"):
            snap["vast_api_key"] = "***"
        if snap.get("vllm_api_key"):
            snap["vllm_api_key"] = "***"
        if snap.get("hf_token"):
            snap["hf_token"] = "***"
        return snap

    def _coord_lease_seconds(self, cfg: Dict[str, Any]) -> int:
        endpoint_timeout = int(cfg.get("endpoint_timeout_sec") or 1200)
        ready_timeout = int(cfg.get("ready_timeout_sec") or 1200)
        boot_timeout = int(cfg.get("boot_timeout_sec") or 1200)
        return max(90, min(1800, max(endpoint_timeout, ready_timeout, boot_timeout) + 120))

    def _coord_try_acquire(self, cfg: Dict[str, Any], state: str) -> bool:
        return self._coord.try_acquire(
            self._owner_id,
            self._coord_lease_seconds(cfg),
            {
                "state": state,
                "instance_id": self._instance_id,
                "endpoint": self._endpoint,
                "public_endpoint": self._public_endpoint,
                "last_error": "",
            },
        )

    def _coord_renew_lease(self, cfg: Dict[str, Any], state: str) -> None:
        try:
            self._coord.renew(
                self._owner_id,
                self._coord_lease_seconds(cfg),
                {
                    "state": state,
                    "instance_id": self._instance_id,
                    "endpoint": self._endpoint,
                    "public_endpoint": self._public_endpoint,
                },
            )
        except Exception:
            pass

    def _coord_update_state(self, state: str, **extra: Any) -> None:
        try:
            payload = {"state": state}
            payload.update(extra)
            self._coord.update_state(self._owner_id, payload)
        except Exception:
            pass

    def _coord_release(self, state: str, **extra: Any) -> None:
        try:
            payload = {"state": state}
            payload.update(extra)
            self._coord.release(self._owner_id, payload)
        except Exception:
            pass

    @staticmethod
    def _utcnow() -> datetime:
        return datetime.now(timezone.utc)

    @staticmethod
    def _mongo_datetime_as_utc_aware(dt: datetime) -> datetime:
        """PyMongo/Mongo часто дають naive UTC; порівняння з aware datetime падає."""
        if dt.tzinfo is None:
            return dt.replace(tzinfo=timezone.utc)
        return dt.astimezone(timezone.utc)

    def _has_recent_shared_source_load_activity(self, within_sec: int) -> bool:
        coord = getattr(self, "_coord", None)
        if coord is None:
            return False
        state = coord.get_runtime_state() or {}
        last_source_load_activity = state.get("last_source_load_activity_at")
        if not isinstance(last_source_load_activity, datetime):
            return False
        last_source_load_activity = self._mongo_datetime_as_utc_aware(last_source_load_activity)
        return (self._utcnow() - last_source_load_activity).total_seconds() <= max(1, int(within_sec))

    @staticmethod
    def _shared_state_has_active_owner(state: Optional[Dict[str, Any]]) -> bool:
        if not isinstance(state, dict):
            return False
        owner_id = state.get("owner_id")
        lease_until = state.get("lease_expires_at")
        if not owner_id or not isinstance(lease_until, datetime):
            return False
        lease_until = VllmRuntimeOrchestrator._mongo_datetime_as_utc_aware(lease_until)
        return lease_until > datetime.now(timezone.utc)

    def _wait_for_shared_runtime_or_acquire(self, cfg: Dict[str, Any], client: VastAiClient) -> Optional[str]:
        deadline = time.time() + max(
            30,
            int(cfg.get("endpoint_timeout_sec") or 1200),
            int(cfg.get("ready_timeout_sec") or 1200),
            int(cfg.get("boot_timeout_sec") or 1200),
        )
        while time.time() < deadline:
            try:
                self._enforce_singleton_vast_instances(client, cfg)
            except Exception:
                pass
            state = self._coord.get_runtime_state() or {}
            public_endpoint = str(state.get("public_endpoint") or "").strip()
            endpoint = public_endpoint or str(state.get("endpoint") or "").strip()
            runtime_state = str(state.get("state") or "").strip().lower()
            if endpoint and runtime_state == "running" and self._is_runtime_ready(endpoint, cfg, timeout_sec=8):
                self._instance_id = str(state.get("instance_id") or self._instance_id or "")
                self._public_endpoint = public_endpoint or endpoint
                self._endpoint = endpoint
                self._instance_paused = False
                self._log_gpu_usage(
                    "gpu_ready",
                    {
                        "instance_id": self._instance_id,
                        "endpoint": endpoint,
                        "public_endpoint": self._public_endpoint,
                        "attempt": 0,
                        "ssh_tunnel_enabled": False,
                    },
                )
                return endpoint
            if not self._shared_state_has_active_owner(state):
                if self._coord_try_acquire(cfg, state="starting"):
                    return None
            time.sleep(5)
        raise RuntimeError("Timed out waiting for another process to finish Vast runtime startup.")

    @staticmethod
    def _gpu_meta_tail(metadata: Dict[str, Any]) -> str:
        parts: List[str] = []
        for key in (
            "instance_id",
            "ask_id",
            "attempt",
            "model",
            "endpoint",
            "public_endpoint",
            "local_endpoint",
            "status",
            "stage",
            "reason",
            "runtime",
        ):
            val = metadata.get(key)
            if val is None or val == "":
                continue
            parts.append(f"{key}={val}")
        err = metadata.get("error")
        if err:
            parts.append(f"error={str(err)[:180]}")
        pr = metadata.get("progress_ratio")
        if pr is not None:
            try:
                pct = round(float(pr) * 100.0, 1)
                parts.append(f"progress_pct={pct}")
            except (TypeError, ValueError):
                parts.append(f"progress_ratio={pr}")
        return (" " + " ".join(parts)) if parts else ""

    def _format_gpu_worker_log_line(self, event: str, metadata: Dict[str, Any]) -> Optional[str]:
        """Короткий текст для Celery/docker logs (utf-8)."""
        pfx = "[gpu-runtime]"
        tail = self._gpu_meta_tail(metadata)
        if event == "gpu_rent_started":
            return (
                f"{pfx} оренда Vast: ask_id={metadata.get('ask_id')} "
                f"спроба {metadata.get('attempt')} з {metadata.get('candidates')}"
            )
        if event == "gpu_rent_failed":
            return f"{pfx} помилка оренди Vast:{tail}"
        if event == "gpu_instance_booted":
            pub = metadata.get("public_endpoint")
            return (
                f"{pfx} інстанс у мережі instance_id={metadata.get('instance_id')} "
                f"public_endpoint={pub}"
            )
        if event == "gpu_ready":
            return (
                f"{pfx} runtime готовий до LLM endpoint={metadata.get('endpoint')} "
                f"tunnel={'так' if metadata.get('ssh_tunnel_enabled') else 'ні'}"
            )
        if event == "gpu_startup_failed":
            return f"{pfx} збій старту GPU:{tail}"
        if event == "gpu_model_pull_started":
            return f"{pfx} модель Ollama: початок завантаження (pull){tail}"
        if event == "gpu_model_pull_progress":
            return f"{pfx} модель Ollama: завантаження:{tail}"
        if event == "gpu_model_pull_ready":
            return f"{pfx} модель Ollama: у пам'яті контейнера готова{tail}"
        if event == "gpu_model_pull_retry":
            return f"{pfx} модель Ollama: повтор pull{tail}"
        if event == "gpu_model_repair_started":
            return f"{pfx} модель Ollama: спроба ремонту (delete + pull){tail}"
        if event == "gpu_model_repair_pull":
            return f"{pfx} модель Ollama: повторний pull після ремонту{tail}"
        if event == "gpu_runtime_endpoint_ready":
            return f"{pfx} API рантайму доступний ({metadata.get('runtime')}){tail}"
        if event == "gpu_runtime_endpoint_wait":
            return f"{pfx} очікування API рантайму ({metadata.get('runtime')}){tail}"
        if event == "gpu_ssh_tunnel_ready":
            return f"{pfx} SSH-тунель до порту піднято local={metadata.get('local_endpoint')}{tail}"
        if event == "gpu_ssh_tunnel_failed":
            return f"{pfx} SSH-тунель не вдався:{tail}"
        if event == "gpu_ssh_tunnel_unavailable":
            return f"{pfx} SSH-тунель недоступний причина={metadata.get('reason')}{tail}"
        if event == "gpu_endpoint_switched":
            return (
                f"{pfx} змінено endpoint етап={metadata.get('phase')} "
                f"{metadata.get('from_endpoint')} → {metadata.get('to_endpoint')}"
            )
        if event == "gpu_instance_log_stream_started":
            return f"{pfx} стрім логів інстанса SSH host={metadata.get('host')}:{metadata.get('port')}"
        if event == "gpu_paused":
            return f"{pfx} інстанс призупинено (pause){tail}"
        if event == "gpu_pause_failed":
            return f"{pfx} pause інстанса не вдався:{tail}"
        if event == "gpu_resumed":
            return f"{pfx} інстанс відновлено (resume){tail}"
        if event == "gpu_destroy_delayed":
            return f"{pfx} destroy відкладено:{tail}"
        if event == "gpu_idle_wait":
            return f"{pfx} черга порожня, очікування перед pause:{tail}"
        if event == "gpu_vast_duplicate_destroyed":
            return (
                f"{pfx} видалено дублікат контракту destroyed={metadata.get('destroyed_instance_id')} "
                f"залишено={metadata.get('keep_instance_id')}"
            )
        if event == "gpu_vast_duplicate_destroy_failed":
            return f"{pfx} не вдалося знищити дублікат інстанса:{tail}"
        if event == "gpu_vast_list_failed":
            return f"{pfx} не вдалося отримати список контрактів Vast:{tail}"
        return f"{pfx} {event}{tail}" if tail else f"{pfx} {event}"

    def _emit_gpu_worker_log(self, event: str, metadata: Dict[str, Any]) -> None:
        """Дублює ключові gpu_* події в stderr (Docker logs), зокрема для LLM-воркера."""
        if event in {"gpu_processing", "gpu_teardown"}:
            return
        if not str(event).startswith("gpu_"):
            return
        line = self._format_gpu_worker_log_line(event, metadata)
        if not line:
            return
        warn_events = {
            "gpu_startup_failed",
            "gpu_rent_failed",
            "gpu_ssh_tunnel_failed",
            "gpu_pause_failed",
            "gpu_vast_duplicate_destroy_failed",
            "gpu_vast_list_failed",
        }
        if event in warn_events:
            logger.warning("%s", line)
        else:
            logger.info("%s", line)

    def _log_gpu_usage(self, event: str, metadata: Dict[str, Any]) -> None:
        if event in {
            "gpu_instance_booted",
            "gpu_model_pull_started",
            "gpu_model_pull_progress",
            "gpu_model_pull_ready",
            "gpu_model_pull_retry",
            "gpu_model_repair_started",
            "gpu_model_repair_pull",
            "gpu_startup_failed",
            "gpu_paused",
            "gpu_pause_failed",
            "gpu_resumed",
            "gpu_destroy_delayed",
            "gpu_runtime_endpoint_ready",
            "gpu_runtime_endpoint_wait",
            "gpu_ssh_tunnel_ready",
            "gpu_ssh_tunnel_failed",
            "gpu_ssh_tunnel_unavailable",
            "gpu_endpoint_switched",
            "gpu_instance_log_stream_started",
            "gpu_ready",
            "gpu_rent_failed",
        }:
            summary_parts = [f"event={event}"]
            status = metadata.get("status")
            model = metadata.get("model")
            stage = metadata.get("stage")
            error = metadata.get("error")
            endpoint = metadata.get("endpoint") or metadata.get("local_endpoint") or metadata.get("public_endpoint")
            if model:
                summary_parts.append(f"model={model}")
            if status:
                summary_parts.append(f"status={status}")
            if stage:
                summary_parts.append(f"stage={stage}")
            if "progress_ratio" in metadata:
                summary_parts.append(f"progress={metadata.get('progress_ratio')}")
            if endpoint:
                summary_parts.append(f"endpoint={endpoint}")
            if error:
                summary_parts.append(f"error={str(error)[:240]}")
            self._append_recent_observability_log("runtime_event", "meta", " ".join(summary_parts))
        try:
            self._logging.log_api_usage(
                service="gpu_runtime",
                source=event,
                from_cache=False,
                metadata=metadata,
            )
        except Exception:
            pass
        self._emit_gpu_worker_log(event, metadata)
