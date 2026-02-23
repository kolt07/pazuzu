# -*- coding: utf-8 -*-
"""
Міграція 037: Колекція listing_analytics для збереження LLM-згенерованої
детальної аналітики оголошення (ціна за одиницю площі, місцезнаходження, оточення).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()
        coll = db["listing_analytics"]

        coll.create_index([("source", 1), ("source_id", 1)], unique=True)
        coll.create_index("analysis_at")

        print("Міграція 037: колекція listing_analytics та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 037:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
