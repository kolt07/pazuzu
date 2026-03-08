# -*- coding: utf-8 -*-
"""
Міграція 011: Колекція session_state для короткого стану сесії (last_region, active_collection тощо).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.session_state_repository import SessionStateRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = SessionStateRepository()
        repo.collection.create_index("user_id", unique=True)
        print("Міграція 011: колекція session_state та індекс створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 011:", e)
        return False
