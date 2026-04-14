# -*- coding: utf-8 -*-
"""
Бенчмарк нового vLLM/Vast пайплайну на історично оброблених OLX описах.

Сценарій:
1) Беремо raw OLX записи.
2) Відбираємо ті, для яких є історичний результат у llm_cache.
3) Підіймаємо Vast/vLLM runtime через оркестратор.
4) Проганяємо описи через новий пайплайн (LLMService з provider=vllm_remote).
5) Порівнюємо нові результати з історичними (coverage + agreement) і друкуємо звіт.

Запуск:
    py scripts/vast_pipeline_benchmark.py --sample-size 5
"""

from __future__ import annotations

import argparse
import json
import statistics
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from business.services.llm_cache_service import LLMCacheService
from business.services.llm_service import LLMService
from business.services.olx_llm_extractor_service import OlxLLMExtractorService
from business.services.vllm_runtime_orchestrator import VllmRuntimeOrchestrator
from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def _normalize_scalar(v: Any) -> Any:
    if isinstance(v, str):
        return " ".join(v.strip().lower().split())
    return v


def _is_empty(v: Any) -> bool:
    if v is None:
        return True
    if isinstance(v, str):
        return not v.strip()
    if isinstance(v, (list, dict, tuple, set)):
        return len(v) == 0
    return False


def _flatten(value: Any, prefix: str = "") -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    if isinstance(value, dict):
        for k, v in value.items():
            key = f"{prefix}.{k}" if prefix else str(k)
            out.update(_flatten(v, key))
    elif isinstance(value, list):
        if not value:
            out[prefix] = []
        elif all(not isinstance(x, (dict, list)) for x in value):
            out[prefix] = sorted([_normalize_scalar(x) for x in value], key=lambda x: str(x))
        else:
            for idx, item in enumerate(value):
                key = f"{prefix}[{idx}]"
                out.update(_flatten(item, key))
    else:
        out[prefix] = _normalize_scalar(value)
    return out


def _field_coverage(obj: Dict[str, Any]) -> Tuple[int, int]:
    flat = _flatten(obj)
    total = len(flat)
    filled = sum(1 for v in flat.values() if not _is_empty(v))
    return filled, total


def _agreement_score(old_obj: Dict[str, Any], new_obj: Dict[str, Any]) -> Dict[str, float]:
    old_flat = _flatten(old_obj)
    new_flat = _flatten(new_obj)
    keys = sorted(set(old_flat.keys()) | set(new_flat.keys()))
    if not keys:
        return {"agreement": 1.0, "compared_fields": 0.0}
    matched = 0
    compared = 0
    for key in keys:
        old_v = old_flat.get(key)
        new_v = new_flat.get(key)
        if _is_empty(old_v) and _is_empty(new_v):
            continue
        compared += 1
        if old_v == new_v:
            matched += 1
            continue
        # М'яке порівняння чисел (щоб дрібні похибки не ламали метрику)
        try:
            old_f = float(old_v)
            new_f = float(new_v)
            if abs(old_f - new_f) < 1e-6:
                matched += 1
        except Exception:
            pass
    if compared == 0:
        return {"agreement": 1.0, "compared_fields": 0.0}
    return {"agreement": matched / compared, "compared_fields": float(compared)}


def _select_sample_with_history(sample_size: int) -> List[Dict[str, Any]]:
    raw_repo = RawOlxListingsRepository()
    cache = LLMCacheService()

    cursor = raw_repo.collection.find(
        {
            "detail.description": {"$exists": True, "$ne": ""},
            "search_data.title": {"$exists": True, "$ne": ""},
        },
        {"url": 1, "search_data": 1, "detail": 1, "updated_at": 1},
    ).sort("updated_at", -1)

    out: List[Dict[str, Any]] = []
    for doc in cursor:
        search_data = doc.get("search_data") or {}
        detail = doc.get("detail") or {}
        description_text = OlxLLMExtractorService._build_description_text(search_data, detail)
        if not description_text.strip():
            continue
        old_result = cache.get_cached_result(description_text)
        if not old_result:
            continue
        out.append(
            {
                "url": doc.get("url"),
                "description_text": description_text,
                "old_result": old_result,
            }
        )
        if len(out) >= sample_size:
            break
    return out


def run_benchmark(sample_size: int) -> Dict[str, Any]:
    settings = Settings()
    MongoDBConnection.initialize(settings)

    sample = _select_sample_with_history(sample_size)
    if not sample:
        raise RuntimeError(
            "Не знайдено історично оброблених записів у llm_cache для доступних raw OLX описів."
        )

    # Форсуємо новий пайплайн парсингу через vllm_remote
    settings.llm_parsing_provider = "vllm_remote"
    llm_service = LLMService(settings)
    runtime = VllmRuntimeOrchestrator()

    t_start_all = time.perf_counter()
    t0 = time.perf_counter()
    endpoint = runtime.ensure_runtime_ready()
    deploy_sec = time.perf_counter() - t0

    item_reports: List[Dict[str, Any]] = []
    item_durations: List[float] = []
    agreements: List[float] = []
    coverages_old: List[float] = []
    coverages_new: List[float] = []

    try:
        for item in sample:
            t_item = time.perf_counter()
            new_result = llm_service.parse_auction_description(item["description_text"])
            dur = time.perf_counter() - t_item
            item_durations.append(dur)

            old_filled, old_total = _field_coverage(item["old_result"] or {})
            new_filled, new_total = _field_coverage(new_result or {})
            old_cov = (old_filled / old_total) if old_total else 0.0
            new_cov = (new_filled / new_total) if new_total else 0.0
            coverages_old.append(old_cov)
            coverages_new.append(new_cov)

            aggr = _agreement_score(item["old_result"] or {}, new_result or {})
            agreements.append(aggr["agreement"])

            item_reports.append(
                {
                    "url": item["url"],
                    "duration_sec": round(dur, 3),
                    "old_coverage": round(old_cov, 3),
                    "new_coverage": round(new_cov, 3),
                    "agreement": round(aggr["agreement"], 3),
                    "compared_fields": int(aggr["compared_fields"]),
                }
            )
    finally:
        runtime.force_shutdown("benchmark_complete")

    total_sec = time.perf_counter() - t_start_all
    processing_sec = sum(item_durations)
    throughput = (len(item_durations) / processing_sec * 60.0) if processing_sec > 0 else 0.0

    return {
        "sample_size_requested": sample_size,
        "sample_size_used": len(sample),
        "runtime": {
            "endpoint": endpoint,
            "deploy_sec": round(deploy_sec, 3),
            "total_benchmark_sec": round(total_sec, 3),
            "processing_sec": round(processing_sec, 3),
        },
        "speed": {
            "avg_item_sec": round(statistics.mean(item_durations), 3) if item_durations else 0.0,
            "p95_item_sec": round(max(item_durations), 3) if item_durations else 0.0,
            "throughput_items_per_min": round(throughput, 2),
        },
        "quality": {
            "avg_agreement": round(statistics.mean(agreements), 3) if agreements else 0.0,
            "avg_old_coverage": round(statistics.mean(coverages_old), 3) if coverages_old else 0.0,
            "avg_new_coverage": round(statistics.mean(coverages_new), 3) if coverages_new else 0.0,
        },
        "items": item_reports,
    }


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark Vast/vLLM parsing vs historical cached outputs")
    parser.add_argument("--sample-size", type=int, default=5, help="Кількість історичних записів для порівняння")
    args = parser.parse_args()

    if args.sample_size <= 0:
        raise SystemExit("--sample-size має бути > 0")

    report = run_benchmark(sample_size=args.sample_size)
    print(json.dumps(report, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()
