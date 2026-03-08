# -*- coding: utf-8 -*-
"""
Міграція 023: Колекція report_templates для збереження шаблонів звітів користувачів.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        coll = db["report_templates"]

        coll.create_index("user_id")
        coll.create_index([("user_id", 1), ("order", 1)])
        coll.create_index("is_default")
        coll.create_index("created_at")

        print("Міграція 023: колекція report_templates та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 023:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
