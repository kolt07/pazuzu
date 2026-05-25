# -*- coding: utf-8 -*-
"""
Міграція 058: прибрати UNIQUE з mista_id у raw_mista_settlements; прибрати явні null.

Запуск: py scripts/migrations/058_fix_raw_mista_id_index.py
"""

from __future__ import annotations

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    print("=" * 60)
    print("Міграція 058: fix raw_mista_settlements mista_id index")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        coll = MongoDBConnection.get_database()["raw_mista_settlements"]

        null_count = coll.count_documents({"mista_id": None})
        if null_count:
            r = coll.update_many({"mista_id": None}, {"$unset": {"mista_id": ""}})
            print(f"✓ Прибрано поле mista_id=null у {r.modified_count} документів")

        dup1 = coll.count_documents({"mista_id": 1})
        if dup1 > 1:
            r = coll.update_many({"mista_id": 1}, {"$unset": {"mista_id": ""}})
            print(f"✓ Очищено mista_id=1 у {r.modified_count} документів (було {dup1})")

        for idx in coll.list_indexes():
            name = idx.get("name") or ""
            key = idx.get("key") or {}
            if name == "_id_" or "mista_id" not in key:
                continue
            if idx.get("unique"):
                coll.drop_index(name)
                print(f"✓ Видалено унікальний індекс {name}")

        coll.create_index("mista_id", sparse=True)
        print("✓ Індекс mista_id (sparse, не unique)")
        print("Міграція 058 завершена")
        return True
    except Exception as e:
        print(f"Помилка: {e}")
        return False


if __name__ == "__main__":
    sys.exit(0 if run_migration() else 1)
