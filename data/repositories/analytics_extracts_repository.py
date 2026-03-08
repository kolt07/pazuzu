# -*- coding: utf-8 -*-
"""
Репозиторій для колекції analytics_extracts.

Зберігає виокремлені дані з оголошень для швидких агрегацій.
Один документ = одне оголошення з плоскими полями метрик та адреси.
"""

from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "analytics_extracts"


class AnalyticsExtractsRepository(BaseRepository):
    """Репозиторій для роботи з виокремленими даними аналітики."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        """Створює індекси при першому зверненні."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("source", 1), ("source_id", 1)], unique=True)
            self.collection.create_index("source")
            self.collection.create_index("region")
            self.collection.create_index("city")
            self.collection.create_index("city_district")
            self.collection.create_index("source_date")
            self._indexes_created = True
        except Exception:
            pass

    def upsert_extract(self, doc: Dict[str, Any]) -> bool:
        """
        Створює або оновлює документ виокремлених даних.

        Args:
            doc: Документ з полями source, source_id та метриками

        Returns:
            True якщо успішно
        """
        source = doc.get("source")
        source_id = doc.get("source_id")
        if not source or not source_id:
            return False

        self._ensure_indexes()
        self.collection.replace_one(
            {"source": source, "source_id": source_id},
            doc,
            upsert=True,
        )
        return True

    def upsert_many(self, documents: List[Dict[str, Any]]) -> int:
        """
        Масовий upsert документів.

        Returns:
            Кількість оброблених документів
        """
        if not documents:
            return 0

        self._ensure_indexes()
        count = 0
        for doc in documents:
            if self.upsert_extract(doc):
                count += 1
        return count

    def clear_all(self) -> int:
        """Видаляє всі документи. Повертає кількість видалених."""
        result = self.collection.delete_many({})
        return result.deleted_count
