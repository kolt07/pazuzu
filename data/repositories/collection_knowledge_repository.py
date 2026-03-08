# -*- coding: utf-8 -*-
"""
Репозиторій для збережених результатів дослідження даних (профілювання колекцій).
Один документ на колекцію: collection_name, generated_at, total_documents, sample_size, field_stats.
"""

from typing import Dict, Any, List, Optional
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class CollectionKnowledgeRepository(BaseRepository):
    """Зберігання та читання профілів колекцій (знання про дані)."""

    def __init__(self):
        super().__init__("collection_knowledge")

    def save(
        self,
        collection_name: str,
        total_documents: int,
        sample_size: int,
        field_stats: Dict[str, Any],
        generated_at: Optional[datetime] = None,
    ) -> str:
        """
        Зберігає профіль колекції (перезаписує останній за collection_name).

        Args:
            collection_name: назва колекції (prozorro_auctions, olx_listings, llm_cache)
            total_documents: загальна кількість документів
            sample_size: розмір вибірки, по якій побудовано профіль
            field_stats: словник зі статистикою по полях (шлях -> {numeric_stats|value_distribution})
            generated_at: час генерації (за замовчуванням — зараз)

        Returns:
            ID збереженого документа
        """
        now = generated_at or datetime.now(timezone.utc)
        doc = {
            "collection_name": collection_name,
            "generated_at": now,
            "total_documents": total_documents,
            "sample_size": sample_size,
            "field_stats": field_stats,
        }
        # Зберігаємо як новий документ (історія); для читання беремо останній
        return self.create(doc)

    def get_latest(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """Повертає останній збережений профіль для колекції."""
        doc = self.collection.find_one(
            {"collection_name": collection_name},
            sort=[("generated_at", -1)],
        )
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def get_all_latest(self, collection_names: Optional[List[str]] = None) -> Dict[str, Dict[str, Any]]:
        """
        Повертає останній профіль для кожної з колекцій.
        Якщо collection_names не вказано — для всіх, що є в БД.
        """
        pipeline = [
            {"$sort": {"generated_at": -1}},
            {"$group": {"_id": "$collection_name", "doc": {"$first": "$$ROOT"}}},
        ]
        if collection_names:
            pipeline.insert(0, {"$match": {"collection_name": {"$in": collection_names}}})
        cursor = self.collection.aggregate(pipeline)
        result = {}
        for item in cursor:
            doc = item.get("doc") or {}
            name = doc.get("collection_name") or item["_id"]
            if doc.get("_id"):
                doc["_id"] = str(doc["_id"])
            result[name] = doc
        return result
