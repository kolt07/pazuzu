# -*- coding: utf-8 -*-
"""
Міграція 018: Колекція llm_feedback для зберігання фідбеку користувачів про відповіді LLM.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        coll = db["llm_feedback"]
        
        # Індекси для швидкого пошуку
        coll.create_index("request_id")
        coll.create_index([("user_id", 1), ("created_at", -1)])
        coll.create_index([("feedback_type", 1), ("created_at", -1)])
        coll.create_index("created_at")
        
        print("Міграція 018: колекція llm_feedback та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 018:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
