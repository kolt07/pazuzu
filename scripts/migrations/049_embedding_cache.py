# -*- coding: utf-8 -*-
"""
Міграція 049: колекція embedding_cache.

Кеш ембедингів за SHA-256 хешем тексту + ім'ям моделі. Дозволяє повторно
використовувати вже згенеровані вектори при перепарсингу або backfill,
скорочуючи навантаження на pazuzu-embeddings (TEI bge-m3) і зовнішні провайдери.

Структура документа:
    {
      _id: <ObjectId>,
      text_hash: "<sha256 hex>",
      model_name: "BAAI/bge-m3",
      vector: [float, ...],
      dim: 1024,
      created_at: ISODate,
      last_used_at: ISODate,
    }

Індекси:
    - {text_hash: 1, model_name: 1} unique  - первинний lookup
    - {created_at: 1} TTL                    - автоматичне очищення старих кешів
"""
from __future__ import annotations

from config.settings import Settings
from data.database.connection import MongoDBConnection


COLLECTION_NAME = "embedding_cache"


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()

        if COLLECTION_NAME not in db.list_collection_names():
            db.create_collection(COLLECTION_NAME)

        coll = db.get_collection(COLLECTION_NAME)
        coll.create_index(
            [("text_hash", 1), ("model_name", 1)],
            unique=True,
            name="text_hash_model_unique",
        )

        ttl_days = max(1, int(getattr(settings, "embeddings_cache_ttl_days", 30)))
        ttl_seconds = ttl_days * 24 * 3600
        try:
            coll.create_index(
                "created_at",
                expireAfterSeconds=ttl_seconds,
                name="created_at_ttl",
            )
        except Exception as e:
            existing = None
            try:
                for ix in coll.list_indexes():
                    if ix.get("name") == "created_at_ttl":
                        existing = ix
                        break
            except Exception:
                existing = None
            if existing and int(existing.get("expireAfterSeconds", -1)) != ttl_seconds:
                try:
                    db.command(
                        "collMod",
                        COLLECTION_NAME,
                        index={"name": "created_at_ttl", "expireAfterSeconds": ttl_seconds},
                    )
                except Exception as inner:
                    print(f"Попередження: не вдалося оновити TTL embedding_cache: {inner}")
            elif not existing:
                raise e

        print(
            f"Міграція 049: embedding_cache ok (TTL={ttl_days}d, unique[text_hash+model_name])."
        )
        return True
    except Exception as e:
        print("Помилка міграції 049:", e)
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    raise SystemExit(0 if run_migration() else 1)
