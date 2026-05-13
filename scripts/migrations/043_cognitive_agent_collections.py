# -*- coding: utf-8 -*-
"""
Міграція 043: колекції agent_reasoning_chain та agent_semantic_memory (когнітивний агент).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.agent_reasoning_chain_repository import AgentReasoningChainRepository
from data.repositories.agent_semantic_memory_repository import AgentSemanticMemoryRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        cr = AgentReasoningChainRepository()
        sm = AgentSemanticMemoryRepository()
        cr.collection.create_index("request_id")
        cr.collection.create_index([("created_at", 1)])
        cr.collection.create_index([("request_id", 1), ("created_at", 1)])
        sm.collection.create_index("request_id")
        sm.collection.create_index("user_id")
        sm.collection.create_index([("created_at", -1)])
        print("Міграція 043: agent_reasoning_chain, agent_semantic_memory та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 043:", e)
        return False


if __name__ == "__main__":
    import sys

    sys.exit(0 if run_migration() else 1)
