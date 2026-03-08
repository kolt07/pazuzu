# -*- coding: utf-8 -*-
"""
Replay запиту за request_id: виконує збережений план без виклику інтерпретатора/LLM.
Для дебагу та оцінки. Використання: py scripts/replay_request.py <request_id>
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.agent_activity_log_repository import AgentActivityLogRepository
from business.services.langchain_agent_service import LangChainAgentService
from business.agents.planner_agent import PlannerAgent
from business.agents.analyst_agent import AnalystAgent


def main():
    if len(sys.argv) < 2:
        print("Використання: py scripts/replay_request.py <request_id>", file=sys.stderr)
        sys.exit(1)
    request_id = sys.argv[1].strip()
    settings = Settings()
    MongoDBConnection.initialize(settings)
    repo = AgentActivityLogRepository()
    entries = repo.get_by_request_id(request_id)
    if not entries:
        print("Записів не знайдено для request_id:", request_id, file=sys.stderr)
        sys.exit(1)
    structured = None
    steps = None
    for e in entries:
        if e.get("step") == "intent":
            structured = e.get("payload") or {}
        if e.get("agent_name") == "planner" and e.get("step") == "action":
            steps = (e.get("payload") or {}).get("steps")
    if not structured:
        print("Intent не знайдено.", file=sys.stderr)
        sys.exit(1)
    if not steps:
        print("План (steps) не знайдено.", file=sys.stderr)
        sys.exit(1)
    langchain_service = LangChainAgentService(settings)
    langchain_service._current_request_id = request_id
    analyst = AnalystAgent(run_tool_fn=langchain_service.run_tool)
    previous_results = []
    for i, step in enumerate(steps):
        result = analyst.run_step(step, i, previous_results)
        previous_results.append(result)
        print(f"Step {i}: {step.get('action')} -> success={result.get('success')}, rows={result.get('rows_count')}")
    print("Replay завершено.")
    langchain_service._current_request_id = None


if __name__ == "__main__":
    main()
