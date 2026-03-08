# -*- coding: utf-8 -*-
"""
Скрипт для порівняння флоу AI-помічника (LangChain Agent).

Запускає набір тестових запитів через LangChainAgentService і збирає метрики:
- кількість ітерацій
- тривалість
- наявність error recovery (при помилках tools)

Використання:
  py scripts/review_ai_assistant_flow.py
  py scripts/review_ai_assistant_flow.py --filter collections
  py scripts/review_ai_assistant_flow.py --limit 2

Потребує налаштованого LLM (config/config.yaml з api_keys).
"""

import sys
import time
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass


# Тестові кейси: (id, запит, опис)
REVIEW_TEST_CASES = [
    ("collections", "Які колекції доступні?", "Довідка — get_allowed_collections"),
    ("analytics_simple", "Скільки оголошень OLX за останній тиждень?", "Підрахунок — execute_analytics або execute_query"),
    ("schema", "Покажи схему unified_listings", "Schema MCP — get_collection_info"),
    ("error_recovery", "Порівняй ціни в Києві та Львові за м²", "Аналітика — можливий fallback ProZorro→OLX"),
]


def run_review(settings, filter_substring: str = "", limit: int = 0):
    """Запуск тестових запитів і збір метрик."""
    from data.database.connection import MongoDBConnection
    from business.services.langchain_agent_service import LangChainAgentService

    MongoDBConnection.initialize(settings)
    service = LangChainAgentService(settings)

    cases = REVIEW_TEST_CASES
    if filter_substring:
        cases = [c for c in cases if filter_substring in c[0] or filter_substring in c[1]]
    if limit > 0:
        cases = cases[:limit]

    print("=== Ревʼю AI-помічника: порівняння флоу ===\n")
    print("Покращення (після ревʼю 2026-02-24):")
    print("  - Error recovery hint при success=false")
    print("  - ReAct/антиципаторне міркування в системному промпті")
    print()

    results = []
    for case_id, query, desc in cases:
        print(f"[{case_id}] {desc}")
        print(f"  Запит: {query[:70]}{'...' if len(query) > 70 else ''}")
        try:
            t0 = time.perf_counter()
            response = service.process_query(
                user_query=query,
                user_id="review-test",
                request_id=f"review-{case_id}",
            )
            duration = time.perf_counter() - t0
            metrics = service._last_request_metrics

            iterations = metrics.get("iterations", 0)
            duration_sec = metrics.get("duration_seconds", duration)

            results.append({
                "case_id": case_id,
                "query": query,
                "iterations": iterations,
                "duration_sec": duration_sec,
                "response_len": len(response or ""),
                "success": True,
            })

            print(f"  Ітерацій: {iterations}, тривалість: {duration_sec:.2f} с")
            print(f"  Відповідь (перші 150 символів): {(response or '')[:150]}...")
            print()
        except Exception as e:
            print(f"  ERROR: {e}")
            results.append({
                "case_id": case_id,
                "query": query,
                "iterations": 0,
                "duration_sec": 0,
                "response_len": 0,
                "success": False,
                "error": str(e),
            })
            print()

    # Підсумок
    print("=" * 60)
    print("ПІДСУМОК")
    passed = sum(1 for r in results if r["success"])
    total = len(results)
    avg_iter = sum(r["iterations"] for r in results) / total if total else 0
    avg_dur = sum(r["duration_sec"] for r in results) / total if total else 0
    print(f"  Запитів: {passed}/{total} успіх")
    print(f"  Середня кількість ітерацій: {avg_iter:.1f}")
    print(f"  Середня тривалість: {avg_dur:.2f} с")
    print()
    print("Деталі ревʼю: docs/ai_assistant_review_2026.md")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Ревʼю флоу AI-помічника")
    parser.add_argument("--filter", type=str, default="", help="Фільтр по id або тексту запиту")
    parser.add_argument("--limit", type=int, default=0, help="Макс. кількість кейсів (0 = усі)")
    args = parser.parse_args()

    from config.settings import Settings
    settings = Settings()

    run_review(settings, args.filter.strip().lower(), args.limit)


if __name__ == "__main__":
    main()
