# -*- coding: utf-8 -*-
"""
Міграція 017: Колекція pipeline_templates для збереження пайплайнів обробки даних
для повторного використання.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        coll = db["pipeline_templates"]
        
        # Індекси для швидкого пошуку
        coll.create_index("query_structure.sources")
        coll.create_index([("query_structure.sources", 1), ("usage_count", -1)])
        coll.create_index("created_at")
        coll.create_index("last_used_at")
        
        print("Міграція 017: колекція pipeline_templates та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 017:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
