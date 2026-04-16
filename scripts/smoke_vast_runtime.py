# -*- coding: utf-8 -*-
"""
Димовий тест Vast.ai + vLLM: ensure_runtime_ready → GET /v1/models → force_shutdown.

Запуск з кореня репозиторію:
    py scripts/smoke_vast_runtime.py
"""

from __future__ import annotations

import argparse
import sys
import threading
import time
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
if hasattr(sys.stderr, "reconfigure"):
    sys.stderr.reconfigure(encoding="utf-8")


def main() -> int:
    p = argparse.ArgumentParser(description="Vast + vLLM smoke test")
    p.add_argument(
        "--log-file",
        default="",
        help="Додатково дописувати рядки у файл (UTF-8)",
    )
    args = p.parse_args()

    def log(msg: str) -> None:
        print(msg, flush=True)
        if args.log_file:
            Path(args.log_file).parent.mkdir(parents=True, exist_ok=True)
            with open(args.log_file, "a", encoding="utf-8") as f:
                f.write(msg + "\n")

    from config.settings import Settings
    from data.database.connection import MongoDBConnection
    from business.services.vast_ai_runtime_settings_service import VastRuntimeSettingsService
    from business.services.vllm_runtime_orchestrator import VllmRuntimeOrchestrator
    import requests

    Settings()
    MongoDBConnection.initialize(Settings())
    o = VllmRuntimeOrchestrator()
    if not o.is_enabled():
        log("SKIP: Vast runtime not enabled or no vast_api_key")
        return 0
    cfg = VastRuntimeSettingsService().get_settings()
    use_ollama = VllmRuntimeOrchestrator._use_ollama_runtime(cfg)
    log("ensure_runtime_ready() ... (може тривати до boot+ready таймаутів з Mongo)")
    result = {"endpoint": None, "error": None, "done": False}

    def _runner() -> None:
        try:
            result["endpoint"] = o.ensure_runtime_ready()
        except Exception as e:
            result["error"] = e
        finally:
            result["done"] = True

    t = threading.Thread(target=_runner, daemon=True, name="SmokeEnsureReady")
    t.start()
    last_lines: set = set()
    step_status = {
        1: False,  # Стартували інстанс
        2: False,  # Інстанс піднявся
        3: False,  # Ендпоінт піднявся
        4: False,  # Модель скачано
        5: False,  # Готово до роботи (hello-check)
    }
    step_details = {
        1: "",
        2: "",
        3: "",
        4: "",
        5: "",
    }
    if not use_ollama:
        step_status[4] = True
        step_details[4] = "vLLM mode: окремий pull моделі не потрібен на цьому етапі."

    last_progress_snapshot = ""

    def _emit_progress(force: bool = False) -> None:
        nonlocal last_progress_snapshot
        lines = [
            "АНАЛІТИКА СТАРТУ RUNTIME:",
            f"1) [{'x' if step_status[1] else ' '}] Стартували інстанс {step_details[1]}".rstrip(),
            f"2) [{'x' if step_status[2] else ' '}] Інстанс піднявся {step_details[2]}".rstrip(),
            f"3) [{'x' if step_status[3] else ' '}] Ендпоінт піднявся {step_details[3]}".rstrip(),
            f"4) [{'x' if step_status[4] else ' '}] Модель скачано {step_details[4]}".rstrip(),
            f"5) [{'x' if step_status[5] else ' '}] Готово до роботи {step_details[5]}".rstrip(),
        ]
        snapshot = "\n".join(lines)
        if force or snapshot != last_progress_snapshot:
            log(snapshot)
            last_progress_snapshot = snapshot

    _emit_progress(force=True)
    last_connection_snapshot = ""
    model_progress_ratio = -1.0
    model_progress_status = ""
    model_pull_started = False
    model_pull_ready = False
    runtime_control_ready = False
    last_model_wait_log_ts = 0.0
    current_instance_id = ""

    def _extract_kv_value(line: str, key: str) -> str:
        token = f"{key}="
        idx = line.find(token)
        if idx < 0:
            return ""
        start = idx + len(token)
        end = line.find(" ", start)
        if end < 0:
            end = len(line)
        return line[start:end].strip()

    def _emit_connection_state(status: dict, force: bool = False) -> None:
        nonlocal last_connection_snapshot
        line = (
            "CONNECTION "
            f"ssh_enabled={status.get('ssh_tunnel_enabled')} "
            f"ssh_alive={status.get('ssh_tunnel_alive')} "
            f"log_stream={status.get('ssh_log_stream_alive')} "
            f"effective={status.get('effective_endpoint') or '-'} "
            f"source={status.get('effective_endpoint_source') or '-'} "
            f"public={status.get('public_endpoint') or '-'} "
            f"ssh_local={status.get('ssh_local_endpoint') or '-'}"
        )
        if force or line != last_connection_snapshot:
            log(line)
            last_connection_snapshot = line

    while not result["done"]:
        status = o.get_observability_status()
        _emit_connection_state(status)
        recent = status.get("recent_logs") or []
        new_lines = []
        for line in recent[-12:]:
            if line in last_lines:
                continue
            new_lines.append(line)
            log(f"  {line}")
            last_lines.add(line)
        if len(last_lines) > 300:
            last_lines = set(list(last_lines)[-150:])

        for line in new_lines:
            if "event=gpu_model_pull_started" in line and not model_pull_started:
                model_pull_started = True
                model_name = _extract_kv_value(line, "model") or str(cfg.get("vllm_model") or "-")
                log(f"MODEL: download started (model={model_name})")
            if "event=gpu_startup_failed" in line:
                err = _extract_kv_value(line, "error") or "unknown startup error"
                log(f"RUNTIME FAIL: {err}")
            if "event=gpu_runtime_endpoint_wait" in line and not runtime_control_ready:
                endpoint = _extract_kv_value(line, "endpoint") or status.get("effective_endpoint") or "-"
                log(f"RUNTIME: control endpoint ще не ready ({endpoint})")
            if "event=gpu_runtime_endpoint_ready" in line and not runtime_control_ready:
                runtime_control_ready = True
                endpoint = _extract_kv_value(line, "endpoint") or status.get("effective_endpoint") or "-"
                log(f"RUNTIME: control endpoint ready ({endpoint})")
            if "event=gpu_model_pull_retry" in line:
                from_ep = _extract_kv_value(line, "from_endpoint") or "-"
                to_ep = _extract_kv_value(line, "to_endpoint") or "-"
                log(f"MODEL: retry pull через інший endpoint ({from_ep} -> {to_ep})")
            if "event=gpu_model_pull_progress" in line:
                status_label = _extract_kv_value(line, "status") or "working"
                progress_value = _extract_kv_value(line, "progress")
                try:
                    ratio = float(progress_value)
                except (TypeError, ValueError):
                    ratio = -1.0
                should_log_progress = False
                if ratio >= 0:
                    if model_progress_ratio < 0 or abs(ratio - model_progress_ratio) >= 0.01:
                        should_log_progress = True
                if status_label != model_progress_status:
                    should_log_progress = True
                if should_log_progress:
                    model_progress_ratio = ratio
                    model_progress_status = status_label
                    if ratio >= 0:
                        pct = max(0.0, min(100.0, ratio * 100.0))
                        step_details[4] = f"(~{pct:.1f}% | status={status_label})"
                        log(f"MODEL: download {pct:.1f}% (status={status_label})")
                    else:
                        step_details[4] = f"(status={status_label})"
                        log(f"MODEL: status={status_label}")
            if "event=gpu_model_pull_ready" in line and not model_pull_ready:
                model_pull_ready = True
                model_name = _extract_kv_value(line, "model") or str(cfg.get("vllm_model") or "-")
                log(f"MODEL: download completed (model={model_name})")

        has_event = lambda token: any(token in line for line in recent)
        instance_id = str(status.get("instance_id") or "").strip()
        if instance_id and instance_id != current_instance_id:
            if current_instance_id:
                log(f"RUNTIME: нова спроба оркестратора (instance_id={instance_id}, prev={current_instance_id})")
                step_status[2] = False
                step_status[3] = False
                step_status[4] = False
                step_status[5] = False
                step_details[2] = ""
                step_details[3] = ""
                step_details[4] = ""
                step_details[5] = ""
                model_progress_ratio = -1.0
                model_progress_status = ""
                model_pull_started = False
                model_pull_ready = False
                runtime_control_ready = False
                last_model_wait_log_ts = 0.0
            current_instance_id = instance_id
        if not step_status[1] and (has_event("event=gpu_rent_started") or instance_id):
            step_status[1] = True
        if instance_id:
            step_details[1] = f"(instance_id={instance_id})"
        if not step_status[2] and (has_event("event=gpu_instance_booted") or status.get("ssh_tunnel_alive")):
            step_status[2] = True
        if step_status[2]:
            step_details[2] = f"({status.get('public_endpoint') or '-'})"
        if not step_status[3] and (status.get("effective_endpoint") or status.get("public_endpoint")):
            step_status[3] = True
        if step_status[3]:
            src = status.get("effective_endpoint_source") or "unknown"
            step_details[3] = f"({status.get('effective_endpoint') or status.get('public_endpoint') or '-'} via {src})"
        if use_ollama and not step_status[4] and has_event("event=gpu_model_pull_ready"):
            step_status[4] = True
            step_details[4] = f"(model={cfg.get('vllm_model')})"
        if use_ollama and step_status[3] and not step_status[4] and runtime_control_ready:
            now_ts = time.time()
            if now_ts - last_model_wait_log_ts >= 30:
                log("MODEL: очікуємо прогрес завантаження (pull) ...")
                last_model_wait_log_ts = now_ts
        if not step_status[5] and has_event("event=gpu_ready"):
            # Фінальну готовність підтверджуємо тільки після Hello-check нижче.
            step_details[5] = f"(runtime ready @ {status.get('effective_endpoint') or status.get('endpoint') or '-'}, очікуємо hello-check)"
        _emit_progress()
        time.sleep(4)

    try:
        if result["error"] is not None:
            raise result["error"]
        ep = result["endpoint"]
    except Exception as e:
        log(f"FAIL: {e!r}")
        raise
    log(f"OK endpoint: {ep}")
    if use_ollama and not step_status[4]:
        recent_now = o.get_observability_status().get("recent_logs") or []
        if any("event=gpu_model_pull_ready" in line for line in recent_now):
            step_status[4] = True
            step_details[4] = f"(model={cfg.get('vllm_model')} ready)"
        else:
            # Модель могла бути вже в кеші до старту pull.
            step_status[4] = True
            step_details[4] = f"(model={cfg.get('vllm_model')} already present)"
    _emit_progress()
    status = o.get_observability_status()
    _emit_connection_state(status, force=True)
    log(
        "SSH status: "
        f"enabled={status.get('ssh_tunnel_enabled')} "
        f"alive={status.get('ssh_tunnel_alive')} "
        f"log_stream_alive={status.get('ssh_log_stream_alive')}"
    )
    log(
        "Endpoints: "
        f"effective={status.get('endpoint')} "
        f"public={status.get('public_endpoint')} "
        f"ssh_local={status.get('ssh_local_endpoint')}"
    )
    recent = status.get("recent_logs") or []
    if recent:
        log("Recent instance/tunnel logs:")
        for line in recent[-25:]:
            log(f"  {line}")
    else:
        log("Recent instance/tunnel logs: <empty>")
    ready_path = "/api/tags" if use_ollama else "/v1/models"
    r = requests.get(ep.rstrip("/") + ready_path, timeout=30)
    log(f"GET {ready_path}: {r.status_code}")
    if r.status_code != 200:
        log(r.text[:1200])
        return 1
    if use_ollama:
        model_ref = VllmRuntimeOrchestrator._normalize_ollama_model_ref(str(cfg.get("vllm_model") or ""))
        log(f"HELLO CHECK model ref: {model_ref}")
        log("HELLO CHECK: POST /api/generate ...")
        hello_resp = requests.post(
            ep.rstrip("/") + "/api/generate",
            json={
                "model": model_ref,
                "prompt": "Hello",
                "stream": False,
            },
            timeout=90,
        )
        log(f"HELLO CHECK /api/generate: {hello_resp.status_code}")
        if hello_resp.status_code != 200:
            original_model_ref = str(cfg.get("vllm_model") or "").strip()
            if original_model_ref and original_model_ref != model_ref and hello_resp.status_code == 404:
                log(f"HELLO CHECK original model ref: {original_model_ref}")
                log("HELLO CHECK: retry with original model id ...")
                hello_resp = requests.post(
                    ep.rstrip("/") + "/api/generate",
                    json={
                        "model": original_model_ref,
                        "prompt": "Hello",
                        "stream": False,
                    },
                    timeout=90,
                )
                log(f"HELLO CHECK /api/generate (original id): {hello_resp.status_code}")
            if hello_resp.status_code != 200:
                log(hello_resp.text[:1200])
                return 1
        try:
            payload = hello_resp.json()
        except Exception:
            payload = {}
        preview = str(payload.get("response") or "").strip()
        if preview:
            log(f"HELLO RESPONSE: {preview[:200]}")
        else:
            log("HELLO RESPONSE: <empty>")
    else:
        log("HELLO CHECK: POST /v1/chat/completions ...")
        model_id = ""
        try:
            models_payload = r.json() if r.content else {}
            models = models_payload.get("data") if isinstance(models_payload, dict) else None
            if isinstance(models, list) and models:
                model_id = str(models[0].get("id") or "").strip()
        except Exception:
            model_id = ""
        if not model_id:
            log("HELLO CHECK FAIL: model id not found in /v1/models")
            return 1
        hello_resp = requests.post(
            ep.rstrip("/") + "/v1/chat/completions",
            json={
                "model": model_id,
                "messages": [{"role": "user", "content": "Hello"}],
                "temperature": 0.0,
                "max_tokens": 32,
            },
            timeout=90,
        )
        log(f"HELLO CHECK /v1/chat/completions: {hello_resp.status_code}")
        if hello_resp.status_code != 200:
            log(hello_resp.text[:1200])
            return 1
        try:
            payload = hello_resp.json()
            choices = payload.get("choices") if isinstance(payload, dict) else None
            text = ""
            if isinstance(choices, list) and choices:
                msg = choices[0].get("message") if isinstance(choices[0], dict) else None
                text = str((msg or {}).get("content") or "").strip() if isinstance(msg, dict) else ""
            log(f"HELLO RESPONSE: {text[:200] if text else '<empty>'}")
        except Exception:
            log("HELLO RESPONSE: <unparsed>")

    step_status[5] = True
    step_details[5] = f"(hello-check ok, endpoint={ep})"
    _emit_progress(force=True)
    o.force_shutdown("smoke_vast_runtime_script")
    log("teardown done")
    log("SMOKE_OK")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception:
        import traceback

        traceback.print_exc()
        raise SystemExit(2)
