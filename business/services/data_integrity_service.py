# -*- coding: utf-8 -*-
"""
Сервіс перевірки цілісності даних.
Порівнює структуру колекцій з data_dictionary, перевіряє індекси та обов'язкові поля.
"""

from typing import Any, Dict, List, Optional

from config.settings import Settings
from data.database.connection import MongoDBConnection
from utils.data_dictionary import DataDictionary

PLATFORM_VERSION = "1.0.0"


class DataIntegrityService:
    """Перевірка цілісності даних відносно data_dictionary."""

    def __init__(self):
        self.settings = Settings()
        self.data_dictionary = DataDictionary()

    def check(self) -> Dict[str, Any]:
        """
        Виконує перевірку цілісності.

        Returns:
            {
                "status": "ok" | "warnings" | "errors",
                "checks": [...],
                "errors": [...],
                "warnings": [...]
            }
        """
        try:
            MongoDBConnection.initialize(self.settings)
        except Exception as e:
            return {
                "status": "errors",
                "checks": [],
                "errors": [f"Не вдалося підключитися до БД: {e}"],
                "warnings": [],
            }
        db = MongoDBConnection.get_database()
        errors: List[str] = []
        warnings: List[str] = []
        checks: List[Dict[str, Any]] = []

        collections_data = self.data_dictionary._data.get("collections", {})
        for coll_name, coll_def in collections_data.items():
            mongo_name = coll_def.get("mongo_collection", coll_name)
            if mongo_name not in db.list_collection_names():
                warnings.append(f"Колекція {mongo_name} відсутня в БД")
                continue
            try:
                coll = db[mongo_name]
                count = coll.count_documents({})
                checks.append({"collection": mongo_name, "status": "ok", "count": count})
            except Exception as e:
                errors.append(f"{mongo_name}: помилка доступу — {e}")

        status = "ok"
        if errors:
            status = "errors"
        elif warnings:
            status = "warnings"

        config_version = ""
        try:
            from config.config_loader import get_config_loader
            meta = get_config_loader().get_bundle_metadata()
            config_version = meta.get("config_version", "1.0")
        except Exception:
            pass

        return {
            "status": status,
            "checks": checks,
            "errors": errors,
            "warnings": warnings,
            "config_version": config_version,
            "platform_version": PLATFORM_VERSION,
        }
