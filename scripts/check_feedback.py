# -*- coding: utf-8 -*-
"""
Скрипт для перегляду фідбеку за user_id та датою.
Запуск: py scripts/check_feedback.py 171554829 2026-02-16
"""

import sys
from pathlib import Path
from datetime import datetime, timezone, timedelta

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.feedback_repository import FeedbackRepository


def main():
    user_id = sys.argv[1] if len(sys.argv) > 1 else "171554829"
    date_str = sys.argv[2] if len(sys.argv) > 2 else "2026-02-16"

    Settings()
    MongoDBConnection.initialize(Settings())

    repo = FeedbackRepository()
    coll = repo.collection

    try:
        if " " in date_str:
            dt = datetime.strptime(date_str, "%Y-%m-%d %H:%M")
            end = dt.replace(tzinfo=timezone.utc) + timedelta(hours=24)
        else:
            dt = datetime.strptime(date_str + " 20:00", "%Y-%m-%d %H:%M")
            end = dt.replace(tzinfo=timezone.utc) + timedelta(hours=2)
        start = dt.replace(tzinfo=timezone.utc)
    except ValueError:
        start = datetime(2026, 2, 16, 19, 0, 0, tzinfo=timezone.utc)
        end = datetime(2026, 2, 16, 22, 0, 0, tzinfo=timezone.utc)

    cursor = coll.find({
        "user_id": str(user_id),
        "created_at": {"$gte": start, "$lte": end},
    }).sort("created_at", 1)

    items = list(cursor)
    print(f"Знайдено фідбеків: {len(items)}")
    print("=" * 80)

    for i, doc in enumerate(items):
        print(f"\n--- Фідбек {i + 1} ---")
        print(f"request_id: {doc.get('request_id')}")
        print(f"feedback_type: {doc.get('feedback_type')}")
        print(f"created_at: {doc.get('created_at')}")
        print(f"\nЗапит:\n{doc.get('user_query', '')[:500]}")
        print(f"\nВідповідь:\n{doc.get('response_text', '')[:2000]}")
        conv = doc.get("conversation") or []
        print(f"\nБесіда: {len(conv)} повідомлень")
        if conv:
            for j, m in enumerate(conv[:5]):
                role = m.get("role", "")
                content = (m.get("content", "") or "")[:200]
                print(f"  [{j+1}] {role}: {content}...")
            if len(conv) > 5:
                print(f"  ... ще {len(conv) - 5} повідомлень")
        diag = doc.get("diagnostic_result") or {}
        if diag.get("issues"):
            print(f"\nДіагностика issues: {diag['issues']}")
        print()


if __name__ == "__main__":
    main()
