# -*- coding: utf-8 -*-
"""
Міграція 027: Колекція chat_sessions для контексту діалогів (історія повідомлень, службові дані).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.chat_session_repository import ChatSessionRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = ChatSessionRepository()
        repo.collection.create_index(
            [("user_id", 1), ("chat_id", 1)],
            unique=True,
        )
        repo.collection.create_index(
            [("updated_at", 1)],
            expireAfterSeconds=60 * 60 * 24 * 30,  # TTL 30 днів
        )
        print("Міграція 027: колекція chat_sessions та індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 027:", e)
        return False
