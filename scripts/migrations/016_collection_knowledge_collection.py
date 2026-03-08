# -*- coding: utf-8 -*-
"""
Міграція 016: Колекція collection_knowledge для збереження результатів
автоматичного дослідження даних (профілювання): статистика по полях, топ значень, середні.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        coll = db["collection_knowledge"]
        coll.create_index("collection_name")
        coll.create_index([("collection_name", 1), ("generated_at", -1)])
        print("Міграція 016: колекція collection_knowledge та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 016:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
