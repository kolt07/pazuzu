# -*- coding: utf-8 -*-
"""
Теоретичне опрацювання запиту: IntentDetector → QueryStructure → PipelineBuilder
без виконання пайплайну. Відображає маршрут обробки для дебагу.

Використання:
  py scripts/trace_query.py                                    # приклад: найдорожча нерухомість в Києві
  py scripts/trace_query.py "Скільки оголошень OLX у Львові?"  # власний запит

Для повного trace (включно з PipelineBuilder) потрібні запущені MCP сервери:
  py scripts/start_mcp_servers.py
"""

import json
import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

if sys.stdout.encoding and sys.stdout.encoding.lower() != "utf-8":
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

DEFAULT_QUERY = "Яка найдорожча нерухомість в Києві?"


def main():
    query = sys.argv[1].strip() if len(sys.argv) > 1 else DEFAULT_QUERY
    from config.settings import Settings
    from data.database.connection import MongoDBConnection
    from business.services.multi_agent_service import MultiAgentService

    settings = Settings()
    MongoDBConnection.initialize(settings)
    service = MultiAgentService(settings, user_service=None, notify_admins_fn=None)

    print("=== Теоретичне опрацювання запиту ===\n")
    print(f"Запит: {query}\n")

    trace = service.trace_query_processing(user_query=query, user_id=None)

    if trace.get("out_of_scope"):
        print("→ Запит поза межами системи (out_of_scope)")
        print(json.dumps(trace.get("intent_info", {}), ensure_ascii=False, indent=2))
        return

    print("1. IntentDetectorAgent:")
    intent_info = trace.get("intent_info", {})
    for k, v in intent_info.items():
        print(f"   {k}: {v}")
    print()

    print("2. QueryStructureAgent:")
    qs = trace.get("query_structure", {})
    print(f"   sources: {qs.get('sources')}")
    print(f"   filter_metrics: {json.dumps(qs.get('filter_metrics', {}), ensure_ascii=False)}")
    print(f"   sort_metrics: {qs.get('sort_metrics')}")
    print(f"   limit: {qs.get('limit')}")
    print(f"   response_metrics: {qs.get('response_metrics')}")
    print()

    print("3. PipelineBuilderAgent:")
    if trace.get("pipeline_error"):
        print(f"   [помилка] {trace['pipeline_error']}")
    else:
        print(f"   pipeline_id: {trace.get('pipeline_id')}")
        print(f"   from_cache: {trace.get('pipeline_from_cache')}")
        print(f"   description: {trace.get('pipeline_description')}")
        pipeline = trace.get("pipeline", {})
        steps = pipeline.get("steps", [])
        print(f"   steps: {len(steps)}")
        for i, step in enumerate(steps):
            print(f"      [{i}] {step.get('action', step)}")
    print("\nГотово.")


if __name__ == "__main__":
    main()
