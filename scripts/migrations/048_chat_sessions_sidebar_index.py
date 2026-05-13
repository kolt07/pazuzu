# -*- coding: utf-8 -*-
"""
Міграція 048: індекс chat_sessions для списку чатів Mini App (user_id + updated_at).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.chat_session_repository import ChatSessionRepository


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        ChatSessionRepository().ensure_sidebar_indexes()
        print("Міграція 048: індекс user_id_updated_desc для chat_sessions перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 048:", e)
        import traceback

        traceback.print_exc()
        return False


if __name__ == "__main__":
    raise SystemExit(0 if run_migration() else 1)
