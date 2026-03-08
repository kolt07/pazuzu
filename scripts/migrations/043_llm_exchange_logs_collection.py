# -*- coding: utf-8 -*-
"""
Міграція 043: Колекція llm_exchange_logs для повних запитів/відповідей до LLM (Gemini, Ollama).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        coll = db["llm_exchange_logs"]

        coll.create_index("timestamp")
        coll.create_index("source")
        coll.create_index("provider")
        coll.create_index([("timestamp", -1)])
        coll.create_index([("provider", 1), ("timestamp", -1)])

        print("Міграція 043: колекція llm_exchange_logs та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 043:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
