# -*- coding: utf-8 -*-
"""
Бенчмарк: скільки паралельних «потоків» парсингу через Ollama на Vast витримує інстанс.

Сценарій: одне й те саме оголошення (опис з БД), N потоків, загалом `total_requests` запитів;
старт потоків рознесено на `stagger_sec` секунд; між запитами в кожному потоці — випадкова пауза
`[sleep_min, sleep_max]` сек.

Приклад:
  py scripts/benchmark_ollama_vast_concurrency.py
  py scripts/benchmark_ollama_vast_concurrency.py --threads-min 1 --threads-max 10 --total-requests 200
  py scripts/benchmark_ollama_vast_concurrency.py --quick --total-requests 40
  # Уже прогрітий Ollama (напр. SSH-тунель vast → localhost:8000), без оренди/черги Vast:
  py scripts/benchmark_ollama_vast_concurrency.py --runtime-endpoint http://127.0.0.1:8000
  # Без Mongo: автоматично підставляється вбудований опис (або --description-file):
  py scripts/benchmark_ollama_vast_concurrency.py --runtime-endpoint http://127.0.0.1:8000 --skip-mongo

Налаштування Vast беруться з Mongo (`vast_runtime_settings`) + опційні CLI-оверрайди.
За замовчуванням не скидається singleton оркестратора і не перезаписується gpu_name з Mongo
(режим «підхопити вже піднятий інстанс»). Для примусового нового оркестратора: --reset-orchestrator.
"""

from __future__ import annotations

import argparse
import json
import logging
import random
import statistics
import sys
import threading
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

logging.basicConfig(
    level=logging.WARNING,
    format="%(message)s",
)
logger = logging.getLogger("benchmark_ollama_vast")

# Якщо Mongo недоступна (локальний бенчмарк лише на Ollama).
_DEFAULT_EMBEDDED_DESCRIPTION_UA = """\
Продається земельна ділянка під забудову. Площа 0,25 га, кадастровий номер 1234567890:12:345:678.
Комунікації: електрика по межі, газ у перспективі. До траси 2 км, асфальтований під’їзд.
Населений пункт: село біля обласного центру. Цільове призначення — під індивідуальне житлове будівництво.
Додатково: поруч ліс, ставок. Торг можливий. Документи перевірені.
"""


def _fetch_sample_description_prozorro() -> Tuple[str, str]:
    from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository

    repo = ProZorroAuctionsRepository()
    cursor = repo.collection.find(
        {"auction_data.description": {"$exists": True}},
        {"auction_data.description": 1, "auction_id": 1},
    ).limit(50)
    for doc in cursor:
        desc_obj = doc.get("auction_data", {}).get("description", {})
        if isinstance(desc_obj, dict):
            desc = desc_obj.get("uk_UA") or desc_obj.get("en_US") or ""
        else:
            desc = str(desc_obj) if desc_obj else ""
        if desc and len(desc.strip()) > 50:
            return str(doc.get("auction_id", "")), desc[:8000]
    raise RuntimeError("Не знайдено жодного опису ProZorro у БД (потрібен хоча б один документ).")


def _split_requests(num_threads: int, total: int) -> List[int]:
    base = total // num_threads
    rem = total % num_threads
    out = [base] * num_threads
    for i in range(rem):
        out[i] += 1
    return out


def _percentile(sorted_vals: List[float], p: float) -> float:
    if not sorted_vals:
        return 0.0
    k = (len(sorted_vals) - 1) * p
    f = int(k)
    c = min(f + 1, len(sorted_vals) - 1)
    if f == c:
        return sorted_vals[f]
    return sorted_vals[f] + (sorted_vals[c] - sorted_vals[f]) * (k - f)


def _run_scenario(
    *,
    num_threads: int,
    total_requests: int,
    description: str,
    stagger_sec: float,
    sleep_min: float,
    sleep_max: float,
    seed: int,
    settings: Any,
) -> Dict[str, Any]:
    from business.services.llm_service import LLMService

    rng = random.Random(seed + num_threads * 1000)
    counts = _split_requests(num_threads, total_requests)
    latencies_ms: List[float] = []
    errors = 0
    lock = threading.Lock()

    def worker(tid: int, n_req: int) -> None:
        nonlocal errors
        local = LLMService(settings)
        if num_threads <= 1:
            delay = 0.0
        else:
            delay = (tid / float(num_threads - 1)) * stagger_sec
        time.sleep(delay)
        for i in range(n_req):
            t0 = time.perf_counter()
            try:
                r = local.provider.parse_auction_description(description)
                ok = isinstance(r, dict) and any(
                    r.get(k) not in (None, "", [], {})
                    for k in ("cadastral_number", "addresses", "property_type", "tags", "building_area_sqm")
                )
                if not ok:
                    with lock:
                        errors += 1
            except Exception:
                with lock:
                    errors += 1
            dt_ms = (time.perf_counter() - t0) * 1000.0
            with lock:
                latencies_ms.append(dt_ms)
            if i < n_req - 1:
                time.sleep(rng.uniform(sleep_min, sleep_max))

    t_wall0 = time.perf_counter()
    threads: List[threading.Thread] = []
    for tid in range(num_threads):
        th = threading.Thread(target=worker, args=(tid, counts[tid]), daemon=True)
        threads.append(th)
        th.start()
    for th in threads:
        th.join()
    wall_sec = time.perf_counter() - t_wall0

    latencies_ms.sort()
    n = len(latencies_ms)
    mean_ms = statistics.mean(latencies_ms) if n else 0.0
    p50 = _percentile(latencies_ms, 0.50) if n else 0.0
    p95 = _percentile(latencies_ms, 0.95) if n else 0.0
    p99 = _percentile(latencies_ms, 0.99) if n else 0.0

    rps = total_requests / wall_sec if wall_sec > 0 else 0.0
    return {
        "threads": num_threads,
        "total_requests": total_requests,
        "wall_sec": wall_sec,
        "rps": rps,
        "errors": errors,
        "mean_latency_ms": mean_ms,
        "p50_latency_ms": p50,
        "p95_latency_ms": p95,
        "p99_latency_ms": p99,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Ollama/Vast concurrency benchmark")
    parser.add_argument("--threads-min", type=int, default=1)
    parser.add_argument("--threads-max", type=int, default=10)
    parser.add_argument("--total-requests", type=int, default=200)
    parser.add_argument("--stagger-sec", type=float, default=3.0, help="Рознести старт потоків на [0, stagger]")
    parser.add_argument("--sleep-min", type=float, default=0.1)
    parser.add_argument("--sleep-max", type=float, default=0.5)
    parser.add_argument("--seed", type=int, default=42)
    parser.add_argument(
        "--gpu-name",
        type=str,
        default="",
        help="Точний gpu_name у фільтрі Vast (порожньо = як у Mongo, зручно для вже орендованого інстанса)",
    )
    parser.add_argument("--max-hourly-usd", type=float, default=0.0, help="0 = не змінювати з Mongo")
    parser.add_argument("--pause-between-scenarios", type=float, default=15.0)
    parser.add_argument("--quick", action="store_true", help="total-requests=40, threads-max=4 (швидка перевірка)")
    parser.add_argument(
        "--runtime-endpoint",
        type=str,
        default="",
        help="HTTP base Ollama (напр. http://127.0.0.1:8000 після SSH). Пропускає ensure_runtime_ready/Vast — лише прогрітий інстанс.",
    )
    parser.add_argument(
        "--reset-orchestrator",
        action="store_true",
        help="Примусово скинути process-singleton VllmRuntimeOrchestrator (рідко потрібно).",
    )
    parser.add_argument(
        "--skip-mongo",
        action="store_true",
        help="Не підключатися до Mongo (опис — з --description-file або вбудований зразок).",
    )
    parser.add_argument(
        "--description-file",
        type=str,
        default="",
        help="UTF-8 файл з текстом оголошення для парсингу (пріоритет над БД).",
    )
    args = parser.parse_args()
    if args.quick:
        args.total_requests = min(args.total_requests, 40)
        args.threads_max = min(args.threads_max, 4)

    from config.settings import Settings
    from data.database.connection import MongoDBConnection
    from business.services.vast_ai_runtime_settings_service import VastRuntimeSettingsService
    from business.services.vllm_runtime_orchestrator import get_shared_vllm_runtime_orchestrator
    import business.services.vllm_runtime_orchestrator as vro
    from unittest.mock import patch

    import requests

    use_direct = bool(args.runtime_endpoint and args.runtime_endpoint.strip())
    stored: Dict[str, Any] = {}
    mongo_ok = False
    if args.skip_mongo:
        pass
    else:
        try:
            MongoDBConnection.initialize(Settings())
            stored = VastRuntimeSettingsService().get_settings()
            mongo_ok = True
        except Exception as e:
            if use_direct:
                print(
                    f"Попередження: MongoDB недоступна ({e!s}), використовуємо defaults+yaml і вбудований опис.",
                    flush=True,
                )
            else:
                raise

    if args.description_file.strip():
        path = Path(args.description_file.strip())
        description = path.read_text(encoding="utf-8")[:8000]
        auction_id = path.stem
    elif mongo_ok:
        auction_id, description = _fetch_sample_description_prozorro()
    else:
        auction_id = "embedded"
        description = _DEFAULT_EMBEDDED_DESCRIPTION_UA

    print(f"Зразок: id={auction_id}, len(description)={len(description)}", flush=True)

    app_settings = Settings()

    def merged_settings() -> Dict[str, Any]:
        merged = dict(VastRuntimeSettingsService.default_settings())
        yaml_overlay = VastRuntimeSettingsService._config_yaml_vast_runtime_overlay()
        if yaml_overlay:
            merged.update(yaml_overlay)
        merged.update(stored or {})
        merged["is_enabled"] = True
        if args.gpu_name.strip():
            merged["gpu_name_like"] = args.gpu_name.strip()
        if args.max_hourly_usd and args.max_hourly_usd > 0:
            merged["max_hourly_usd"] = float(args.max_hourly_usd)
        merged.setdefault("image", "ollama/ollama:latest")
        # Назва моделі для Ollama/OpenAI — як у pipeline парсингу (llm.parsing), не лише vast_runtime GGUF.
        pm = getattr(app_settings, "llm_parsing_model_name", "") or ""
        if pm.strip():
            merged["vllm_model"] = pm.strip()
        else:
            merged.setdefault("vllm_model", "gemma3:12b")
        return VastRuntimeSettingsService._normalize(merged)

    cfg = merged_settings()
    if not use_direct and not cfg.get("vast_api_key"):
        print("Помилка: vast_api_key відсутній (Mongo vast_runtime_settings або VAST_API_KEY).", flush=True)
        return 2

    print(
        json.dumps(
            {
                "mode": "direct_endpoint" if use_direct else "vast_ensure_ready",
                "gpu_name_like": cfg.get("gpu_name_like"),
                "vllm_model": cfg.get("vllm_model"),
                "max_hourly_usd": cfg.get("max_hourly_usd"),
                "image": cfg.get("image"),
            },
            ensure_ascii=False,
            indent=2,
        ),
        flush=True,
    )

    def _fake_get_settings(_self: Any) -> Dict[str, Any]:
        return cfg

    if args.reset_orchestrator:
        vro._SHARED_ORCHESTRATOR = None

    with patch.object(VastRuntimeSettingsService, "get_settings", _fake_get_settings):
        settings = app_settings
        settings.llm_rate_limit_calls_per_minute = 0
        settings.llm_parsing_provider = "vllm_remote"
        settings.llm_parsing_model_name = cfg.get("vllm_model") or "gemma3:12b"

        orch = get_shared_vllm_runtime_orchestrator()
        if use_direct:
            raw = args.runtime_endpoint.strip().rstrip("/")
            endpoint = raw if raw.startswith("http://") or raw.startswith("https://") else f"http://{raw}"
            try:
                r = requests.get(f"{endpoint}/v1/models", timeout=15)
                r.raise_for_status()
            except Exception as e:
                print(f"Помилка перевірки {endpoint}/v1/models: {e}", flush=True)
                return 4
            orch._endpoint = endpoint
            orch._public_endpoint = endpoint

            def _ensure_warmed() -> str:
                return endpoint

            orch.ensure_runtime_ready = _ensure_warmed  # type: ignore[method-assign]
            print(f"Режим прогрітого інстанса (без Vast): {endpoint}", flush=True)
        else:
            print("Підключення до наявного/прогрітого Vast-інстанса (ensure_runtime_ready)…", flush=True)
            t0 = time.perf_counter()
            endpoint = orch.ensure_runtime_ready()
            print(f"Endpoint готовий за {time.perf_counter() - t0:.1f}s: {endpoint}", flush=True)
            if not endpoint:
                print("ensure_runtime_ready повернув порожньо (is_enabled=false?).", flush=True)
                return 3

        results: List[Dict[str, Any]] = []
        for n in range(args.threads_min, args.threads_max + 1):
            print(f"\n=== Сценарій: {n} потоків, {args.total_requests} запитів ===", flush=True)
            r = _run_scenario(
                num_threads=n,
                total_requests=args.total_requests,
                description=description,
                stagger_sec=args.stagger_sec,
                sleep_min=args.sleep_min,
                sleep_max=args.sleep_max,
                seed=args.seed,
                settings=settings,
            )
            results.append(r)
            print(json.dumps(r, ensure_ascii=False, indent=2), flush=True)
            if n < args.threads_max:
                time.sleep(args.pause_between_scenarios)

        best = min(results, key=lambda x: x["wall_sec"])
        print("\n--- Підсумок ---", flush=True)
        print(
            "Найкоротший wall-clock (200 запитів загалом): "
            f"{best['threads']} потоків, wall={best['wall_sec']:.2f}s, "
            f"RPS={best['rps']:.3f}, помилок={best['errors']}, "
            f"p95 latency={best['p95_latency_ms']:.0f}ms",
            flush=True,
        )
        print("\nТаблиця (threads, wall_sec, rps, p95_ms, errors):", flush=True)
        for r in results:
            print(
                f"  {r['threads']:2d}  {r['wall_sec']:8.2f}  {r['rps']:6.3f}  {r['p95_latency_ms']:8.0f}  {r['errors']:3d}",
                flush=True,
            )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
