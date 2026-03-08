# -*- coding: utf-8 -*-
"""
Міграція 028: Колекція property_usage_analysis для збереження попереднього аналізу
використання об'єкта (існуюче використання, геоаналіз, можливі використання зі скорингом).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()
        coll = db["property_usage_analysis"]

        coll.create_index([("source", 1), ("source_id", 1)], unique=True)
        coll.create_index("analysis_at")

        print("Міграція 028: колекція property_usage_analysis та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 028:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
