# -*- coding: utf-8 -*-
"""
Репозиторій для тимчасових вибірок агента (результати запиту зберігаються пакетом для подальшого експорту в файл).
Колекція agent_temp_exports: документи { batch_id, source_collection, created_at, doc }.
"""

from typing import List, Dict, Any, Optional, Tuple
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class AgentTempExportsRepository(BaseRepository):
    """Зберігання тимчасових вибірок для експорту: один документ на ряд результату з спільним batch_id."""

    def __init__(self):
        super().__init__("agent_temp_exports")

    def insert_batch(
        self,
        batch_id: str,
        source_collection: str,
        docs: List[Dict[str, Any]],
    ) -> int:
        """
        Зберігає вибірку документів під одним batch_id.
        При порожньому docs вставляє один мета-документ, щоб export_from_temp_collection міг повернути (source_collection, []).
        Повертає кількість вставлених документів (0 або більше).
        """
        now = datetime.now(timezone.utc)
        if not docs:
            self.collection.insert_one({
                "batch_id": batch_id,
                "source_collection": source_collection,
                "created_at": now,
                "doc": {},
            })
            return 0
        records = [
            {
                "batch_id": batch_id,
                "source_collection": source_collection,
                "created_at": now,
                "doc": d,
            }
            for d in docs
        ]
        self.collection.insert_many(records)
        return len(records)

    def get_batch(self, batch_id: str) -> Tuple[Optional[str], List[Dict[str, Any]]]:
        """
        Повертає (source_collection, список doc) для batch_id.
        Якщо batch не знайдено — (None, []).
        """
        cursor = self.collection.find({"batch_id": batch_id}).sort("_id", 1)
        docs = []
        source_collection = None
        for row in cursor:
            if source_collection is None:
                source_collection = row.get("source_collection") or ""
            doc = row.get("doc")
            # Порожній doc — мета-запис для batch з 0 результатів; не додаємо в список
            if isinstance(doc, dict) and doc:
                docs.append(doc)
        return (source_collection, docs)

    def delete_batch(self, batch_id: str) -> int:
        """Видаляє всі документи з даним batch_id. Повертає кількість видалених."""
        result = self.collection.delete_many({"batch_id": batch_id})
        return result.deleted_count
