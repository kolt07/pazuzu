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
    while not result["done"]:
        status = o.get_observability_status()
        log(
            "WAIT "
            f"ssh_enabled={status.get('ssh_tunnel_enabled')} "
            f"ssh_alive={status.get('ssh_tunnel_alive')} "
            f"log_stream={status.get('ssh_log_stream_alive')} "
            f"effective={status.get('endpoint') or '-'} "
            f"public={status.get('public_endpoint') or '-'} "
            f"ssh_local={status.get('ssh_local_endpoint') or '-'}"
        )
        recent = status.get("recent_logs") or []
        for line in recent[-12:]:
            if line in last_lines:
                continue
            log(f"  {line}")
            last_lines.add(line)
        if len(last_lines) > 300:
            last_lines = set(list(last_lines)[-150:])
        time.sleep(4)

    try:
        if result["error"] is not None:
            raise result["error"]
        ep = result["endpoint"]
    except Exception as e:
        log(f"FAIL: {e!r}")
        raise
    log(f"OK endpoint: {ep}")
    status = o.get_observability_status()
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
    cfg = VastRuntimeSettingsService().get_settings()
    use_ollama = VllmRuntimeOrchestrator._use_ollama_runtime(cfg)
    ready_path = "/api/tags" if use_ollama else "/v1/models"
    r = requests.get(ep.rstrip("/") + ready_path, timeout=30)
    log(f"GET {ready_path}: {r.status_code}")
    if r.status_code != 200:
        log(r.text[:1200])
        return 1
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
