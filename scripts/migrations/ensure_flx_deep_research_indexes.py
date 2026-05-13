# -*- coding: utf-8 -*-
"""Ensure indexes for FLX deep research fields."""

from __future__ import annotations

from config.settings import Settings
from data.database.connection import MongoDBConnection


def main() -> None:
    settings = Settings()
    db = MongoDBConnection.get_database(settings)

    sessions = db["investigation_sessions"]
    notes = db["investigation_notes"]

    sessions.create_index([("session_id", 1)], unique=True, name="idx_session_id_unique")
    sessions.create_index([("state", 1), ("updated_at", -1)], name="idx_state_updated")
    sessions.create_index([("research_tasks.status", 1)], name="idx_research_task_status")
    sessions.create_index([("research_tasks.priority", -1)], name="idx_research_task_priority")
    sessions.create_index([("research_branches", 1)], name="idx_research_branches")
    sessions.create_index([("contrarian_done", 1)], name="idx_contrarian_done")

    notes.create_index([("session_id", 1), ("kind", 1), ("created_at", -1)], name="idx_session_kind_created")
    notes.create_index([("strategy_id", 1), ("created_at", -1)], name="idx_strategy_created")

    print("FLX deep research indexes ensured.")


if __name__ == "__main__":
    main()
