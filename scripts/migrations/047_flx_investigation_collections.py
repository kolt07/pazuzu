# -*- coding: utf-8 -*-
"""
Міграція 047: Колекції агента-інвестігейтора Flx.

Створює:
- investigation_sessions: стан дослідження (план, крок, артефакти).
- investigation_notes: записник агента (думки, спостереження, гіпотези).
- flx_lessons_learned: підсумки попередніх досліджувань для самовдосконалення.
- investigation_events: capped-колекція подій для SSE-каналу прогресу.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection


def _ensure_capped_collection(db, name: str, size_bytes: int, max_docs: int) -> None:
    """Створює capped-колекцію, якщо її немає; інакше залишає як є.

    Capped-колекції мають фіксовану структуру і не змінюються через collMod без drop.
    """
    if name in db.list_collection_names():
        return
    db.create_collection(name, capped=True, size=size_bytes, max=max_docs)


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()

        # 1) investigation_sessions — основна сесія дослідження
        sessions = db["investigation_sessions"]
        sessions.create_index([("session_id", 1)], unique=True)
        sessions.create_index([("user_id", 1), ("updated_at", -1)])
        sessions.create_index([("state", 1), ("updated_at", -1)])
        # TTL 30 днів від updated_at
        sessions.create_index(
            [("updated_at", 1)],
            expireAfterSeconds=60 * 60 * 24 * 30,
            name="ttl_updated_at_30d",
        )

        # 2) investigation_notes — нотатки агента під час дослідження
        notes = db["investigation_notes"]
        notes.create_index([("session_id", 1), ("seq", 1)])
        notes.create_index([("created_at", 1)], expireAfterSeconds=60 * 60 * 24 * 30, name="ttl_created_at_30d")

        # 3) flx_lessons_learned — самовдосконалення
        lessons = db["flx_lessons_learned"]
        lessons.create_index([("user_id", 1), ("created_at", -1)])
        lessons.create_index([("topic_tags", 1)])
        # Текстовий індекс для пошуку схожих кейсів
        try:
            lessons.create_index(
                [
                    ("query_pattern", "text"),
                    ("what_worked", "text"),
                    ("recommendations", "text"),
                ],
                name="lessons_text_search",
                default_language="none",
            )
        except Exception as e:
            print(f"  попередження: текстовий індекс flx_lessons_learned не створено: {e}")

        # 4) investigation_events — capped colection для SSE-стрімів (32 МБ, ~50k подій)
        _ensure_capped_collection(
            db,
            name="investigation_events",
            size_bytes=32 * 1024 * 1024,
            max_docs=50000,
        )
        events = db["investigation_events"]
        events.create_index([("session_id", 1), ("seq", 1)])

        print("Міграція 047: колекції Flx (sessions/notes/lessons/events) і індекси створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 047:", e)
        import traceback
        traceback.print_exc()
        return False
