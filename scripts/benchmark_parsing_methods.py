# -*- coding: utf-8 -*-
"""
Benchmark: порівняння методів парсингу оголошень (regex, Gemini, Ollama).

Запуск:
  py scripts/benchmark_parsing_methods.py
  py scripts/benchmark_parsing_methods.py --limit 5 --methods regex,ollama
  py scripts/benchmark_parsing_methods.py --limit 10 --use-cache

Методи:
  regex  — регулярні вирази (швидко, без мережі)
  gemini — Google Gemini (API, з кешем)
  ollama — локальна LLM gemma3:27b через Ollama
"""

import argparse
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional


if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from business.services.llm_service import LLMService
from business.services.llm_cache_service import LLMCacheService
from business.services.olx_llm_extractor_service import OlxLLMExtractorService
from utils.listing_regex_extractor import extract_from_description


def _get_prozorro_descriptions(limit: int) -> List[Dict[str, Any]]:
    """Отримує список описів з prozorro_auctions."""
    repo = ProZorroAuctionsRepository()
    cursor = repo.collection.find(
        {"auction_data.description": {"$exists": True}},
        {"auction_data.description": 1, "auction_id": 1},
    ).limit(limit)
    samples = []
    for doc in cursor:
        desc_obj = doc.get("auction_data", {}).get("description", {})
        if isinstance(desc_obj, dict):
            desc = desc_obj.get("uk_UA") or desc_obj.get("en_US") or ""
        else:
            desc = str(desc_obj) if desc_obj else ""
        if desc and len(desc.strip()) > 50:
            samples.append({
                "source": "prozorro",
                "id": doc.get("auction_id", ""),
                "description": desc[:3000],
            })
    return samples


def _get_olx_descriptions(limit: int) -> List[Dict[str, Any]]:
    """Отримує список описів з olx_listings (через OlxLLMExtractorService._build_description_text)."""
    repo = OlxListingsRepository()
    cursor = repo.collection.find(
        {"search_data": {"$exists": True}, "detail": {"$exists": True}},
        {"search_data": 1, "detail": 1, "url": 1},
    ).limit(limit * 2)
    samples = []
    for doc in cursor:
        search_data = doc.get("search_data") or {}
        detail = doc.get("detail") or {}
        desc = OlxLLMExtractorService._build_description_text(search_data, detail)
        if desc and len(desc.strip()) > 50:
            samples.append({
                "source": "olx",
                "id": doc.get("url", ""),
                "description": desc[:3000],
            })
            if len(samples) >= limit:
                break
    return samples


def _compare_results(ref: Dict[str, Any], other: Dict[str, Any]) -> Dict[str, Any]:
    """Порівнює два результати, повертає метрики узгодженості."""
    fields = [
        "cadastral_number", "building_area_sqm", "land_area_ha",
        "floor", "property_type", "utilities",
    ]
    matches = 0
    total = 0
    details = []

    for f in fields:
        rv = ref.get(f, "")
        ov = other.get(f, "")
        rv = str(rv).strip() if rv else ""
        ov = str(ov).strip() if ov else ""
        if rv or ov:
            total += 1
            eq = (rv == ov) or (not rv and not ov)
            if eq:
                matches += 1
            details.append({"field": f, "ref": rv[:30], "other": ov[:30], "match": eq})

    if total == 0:
        return {"match_ratio": 1.0, "matches": 0, "total": 0, "details": []}

    return {
        "match_ratio": matches / total,
        "matches": matches,
        "total": total,
        "details": details,
    }


def _run_regex(description: str) -> tuple[Dict[str, Any], float]:
    """Запускає regex-екстрактор, повертає (результат, час)."""
    start = time.perf_counter()
    result = extract_from_description(description)
    elapsed = time.perf_counter() - start
    return result, elapsed


def _run_llm(
    provider_name: str,
    description: str,
    cache_service: LLMCacheService,
    use_cache: bool,
    ollama_model: str = "gemma3:27b",
) -> tuple[Dict[str, Any], float]:
    """Запускає LLM (provider_name: gemini або ollama), повертає (результат, час)."""
    start = time.perf_counter()
    if use_cache and provider_name == "gemini":
        cached = cache_service.get_cached_result(description)
        if cached is not None:
            elapsed = time.perf_counter() - start
            return cached, elapsed

    try:
        settings = Settings()
        if provider_name == "ollama":
            settings.llm_parsing_provider = "ollama"
            settings.llm_parsing_model_name = ollama_model
        elif provider_name == "gemini":
            settings.llm_parsing_provider = "gemini"
            settings.llm_parsing_model_name = getattr(settings, "llm_assistant_model_name", "gemini-2.5-flash")
        llm = LLMService(settings)
        result = llm.parse_auction_description(description)
        if use_cache and provider_name == "gemini":
            cache_service.save_result(description, result)
        elapsed = time.perf_counter() - start
        return result, elapsed
    except Exception as e:
        elapsed = time.perf_counter() - start
        print(f"  [LLM {provider_name}] Помилка: {e}")
        return {}, elapsed


def _count_filled_fields(result: Dict[str, Any]) -> int:
    """Рахує кількість заповнених полів у результаті."""
    count = 0
    if result.get("cadastral_number"):
        count += 1
    if result.get("building_area_sqm"):
        count += 1
    if result.get("land_area_ha"):
        count += 1
    if result.get("addresses") and len(result["addresses"]) > 0:
        count += 1
    if result.get("floor"):
        count += 1
    if result.get("property_type"):
        count += 1
    if result.get("utilities"):
        count += 1
    if result.get("tags") and len(result["tags"]) > 0:
        count += 1
    return count


def run_benchmark(
    limit: int = 5,
    methods: Optional[List[str]] = None,
    use_cache: bool = True,
    source: str = "both",
    compare_with_existing: bool = True,
    ollama_model: str = "gemma3:27b",
) -> None:
    """
    Запускає benchmark.

    Args:
        limit: Кількість зразків з кожного джерела
        methods: Список методів (regex, gemini, ollama)
        use_cache: Використовувати кеш для Gemini
        source: Джерело — "prozorro", "olx", "both"
        compare_with_existing: Порівнювати з існуючим кешем (результати поточного потоку)
        ollama_model: Модель Ollama (напр. gemma3:12b, gemma3:27b)
    """
    methods = methods or ["regex", "gemini", "ollama"]
    Settings()
    MongoDBConnection.initialize(Settings())
    cache_service = LLMCacheService()

    samples = []
    if source in ("prozorro", "both"):
        samples.extend(_get_prozorro_descriptions(limit))
    if source in ("olx", "both"):
        samples.extend(_get_olx_descriptions(limit))

    if not samples:
        print("Немає зразків для benchmark. Перевірте наявність даних у prozorro_auctions та olx_listings.")
        return

    cache_stats = cache_service.get_cache_stats()
    print(f"Benchmark: {len(samples)} зразків, методи: {methods}")
    if "ollama" in methods:
        print(f"Ollama модель: {ollama_model}")
    print(f"Кеш LLM (існуючий потік): {cache_stats.get('entries_count', 0)} записів")
    print("=" * 60)

    times = {m: [] for m in methods}
    results_by_sample = []
    existing_count = 0

    for i, sample in enumerate(samples):
        desc = sample["description"]
        sid = sample["id"][:40] + "..." if len(sample.get("id", "")) > 40 else sample.get("id", "")
        print(f"\n[{i + 1}/{len(samples)}] {sample['source']} {sid}")

        sample_results = {"regex": None, "gemini": None, "ollama": None, "existing": None}

        if compare_with_existing:
            existing = cache_service.get_cached_result(desc)
            if existing:
                sample_results["existing"] = existing
                existing_count += 1

        if "regex" in methods:
            res, t = _run_regex(desc)
            sample_results["regex"] = res
            times["regex"].append(t)
            print(f"  regex: {t:.3f}s")

        if "gemini" in methods:
            try:
                res, t = _run_llm("gemini", desc, cache_service, use_cache, ollama_model)
                sample_results["gemini"] = res
                times["gemini"].append(t)
                print(f"  gemini: {t:.3f}s")
            except Exception as e:
                print(f"  gemini: помилка — {e}")

        if "ollama" in methods:
            try:
                res, t = _run_llm("ollama", desc, cache_service, False, ollama_model)
                sample_results["ollama"] = res
                times["ollama"].append(t)
                print(f"  ollama: {t:.3f}s")
            except Exception as e:
                print(f"  ollama: помилка — {e}")

        ref = sample_results.get("existing") or sample_results.get("gemini")
        if ref and sample_results["ollama"]:
            cmp = _compare_results(ref, sample_results["ollama"])
            print(f"  існуючий vs ollama: {cmp['matches']}/{cmp['total']} полів ({cmp['match_ratio']:.0%})")
        if ref and sample_results["regex"]:
            cmp = _compare_results(ref, sample_results["regex"])
            print(f"  існуючий vs regex: {cmp['matches']}/{cmp['total']} полів ({cmp['match_ratio']:.0%})")
        if sample_results["gemini"] and sample_results["ollama"] and not ref:
            cmp = _compare_results(sample_results["gemini"], sample_results["ollama"])
            print(f"  gemini vs ollama: {cmp['matches']}/{cmp['total']} полів ({cmp['match_ratio']:.0%})")

        if sample_results["regex"]:
            filled = _count_filled_fields(sample_results["regex"])
            print(f"  regex заповнено полів: {filled}/8")
        if ref:
            filled = _count_filled_fields(ref)
            print(f"  існуючий (LLM) заповнено полів: {filled}/8")

        results_by_sample.append(sample_results)

    print("\n" + "=" * 60)
    print("Підсумок (середній час на опис):")
    for m in methods:
        if times[m]:
            avg = sum(times[m]) / len(times[m])
            print(f"  {m}: {avg:.3f}s")
    print(f"\nЗразків з існуючим кешем (результат поточного потоку): {existing_count}/{len(samples)}")
    if results_by_sample:
        regex_results = [r.get("regex") for r in results_by_sample if r.get("regex")]
        ref_results = [r.get("existing") or r.get("gemini") for r in results_by_sample if r.get("existing") or r.get("gemini")]
        n = len(results_by_sample)
        if regex_results:
            regex_avg = sum(_count_filled_fields(r) for r in regex_results) / len(regex_results)
            print(f"Середнє заповнених полів (regex): {regex_avg:.1f}/8")
        if ref_results:
            llm_avg = sum(_count_filled_fields(ref) for ref in ref_results) / len(ref_results)
            print(f"Середнє заповнених полів (LLM/існуючий): {llm_avg:.1f}/8")
    print("\nКеш LLM використовуватиметься для повторних запусків з Gemini.")


def main() -> None:
    parser = argparse.ArgumentParser(description="Benchmark методів парсингу оголошень")
    parser.add_argument("--limit", type=int, default=5, help="Кількість зразків з кожного джерела")
    parser.add_argument(
        "--methods",
        type=str,
        default="regex,gemini,ollama",
        help="Методи через кому: regex, gemini, ollama",
    )
    parser.add_argument("--use-cache", action="store_true", default=True, help="Використовувати кеш для Gemini")
    parser.add_argument("--no-cache", action="store_true", help="Не використовувати кеш")
    parser.add_argument("--source", choices=["prozorro", "olx", "both"], default="both", help="Джерело зразків")
    parser.add_argument("--no-compare-existing", action="store_true", help="Не порівнювати з існуючим кешем")
    parser.add_argument("--ollama-model", type=str, default="gemma3:27b", help="Модель Ollama (напр. gemma3:12b)")
    args = parser.parse_args()

    methods = [m.strip() for m in args.methods.split(",") if m.strip()]
    use_cache = not args.no_cache

    run_benchmark(
        limit=args.limit,
        methods=methods,
        use_cache=use_cache,
        source=args.source,
        compare_with_existing=not args.no_compare_existing,
        ollama_model=args.ollama_model,
    )


if __name__ == "__main__":
    main()
