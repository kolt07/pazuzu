# -*- coding: utf-8 -*-
"""
Репозиторій прогресу скрапера кадастрової карти.
Зберігає стан обробки комірок сітки (cadastral_scraper_cells).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from pymongo import ReturnDocument, UpdateOne
from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "cadastral_scraper_cells"
STATUS_PENDING = "pending"
STATUS_PROCESSING = "processing"
STATUS_DONE = "done"
STATUS_ERROR = "error"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Повертає документ з _id у вигляді рядка."""
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class CadastralScraperProgressRepository(BaseRepository):
    """Робота з колекцією прогресу скрапера (комірки сітки)."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def get_next_pending_cell(self) -> Optional[Dict[str, Any]]:
        """
        Повертає наступну комірку зі статусом pending або error для повторної спроби.
        Center-first: спочатку обробляються тайли ближче до центру України (Київ).
        Атомарно змінює статус на processing.
        """
        result = self.collection.find_one_and_update(
            {"status": {"$in": [STATUS_PENDING, STATUS_ERROR]}},
            {
                "$set": {
                    "status": STATUS_PROCESSING,
                    "processing_started_at": datetime.now(timezone.utc),
                }
            },
            sort=[("sort_priority", 1), ("cell_id", 1)],
            return_document=ReturnDocument.AFTER,
        )
        return _normalize_doc(result)

    def mark_cell_done(
        self,
        cell_id: str,
        parcels_count: int,
    ) -> bool:
        """Позначає комірку як успішно оброблену."""
        result = self.collection.update_one(
            {"cell_id": cell_id},
            {
                "$set": {
                    "status": STATUS_DONE,
                    "processed_at": datetime.now(timezone.utc),
                    "parcels_count": parcels_count,
                    "error_message": None,
                },
                "$unset": {"processing_started_at": ""},
            },
        )
        return result.modified_count > 0

    def mark_cell_error(
        self,
        cell_id: str,
        error_message: str,
    ) -> bool:
        """Позначає комірку як помилку (для повторної спроби пізніше)."""
        result = self.collection.update_one(
            {"cell_id": cell_id},
            {
                "$set": {
                    "status": STATUS_ERROR,
                    "processed_at": datetime.now(timezone.utc),
                    "error_message": error_message[:2000] if error_message else None,
                },
                "$unset": {"processing_started_at": ""},
            },
        )
        return result.modified_count > 0

    def reset_empty_done_cells(self) -> int:
        """
        Скидає комірки зі статусом done, що були опрацьовані як порожні (parcels_count=0).
        Повертає їх у pending для повторної спроби.
        """
        result = self.collection.update_many(
            {
                "status": STATUS_DONE,
                "$or": [
                    {"parcels_count": 0},
                    {"parcels_count": {"$exists": False}},
                ],
            },
            {
                "$set": {"status": STATUS_PENDING},
                "$unset": {"processed_at": "", "parcels_count": "", "error_message": ""},
            },
        )
        return result.modified_count

    def reset_stale_processing(self, max_age_minutes: int = 60) -> int:
        """
        Скидає комірки зі статусом processing, що зависли довше max_age_minutes.
        Повертає кількість скинутих.
        """
        from datetime import timedelta
        cutoff = datetime.now(timezone.utc) - timedelta(minutes=max_age_minutes)
        result = self.collection.update_many(
            {
                "status": STATUS_PROCESSING,
                "processing_started_at": {"$lt": cutoff},
            },
            {"$set": {"status": STATUS_PENDING}, "$unset": {"processing_started_at": ""}},
        )
        return result.modified_count

    def ensure_cell_exists(
        self,
        cell_id: str,
        zoom: int,
        bbox: Dict[str, float],
    ) -> bool:
        """Створює комірку зі статусом pending, якщо її ще немає."""
        result = self.collection.update_one(
            {"cell_id": cell_id},
            {
                "$setOnInsert": {
                    "cell_id": cell_id,
                    "zoom": zoom,
                    "bbox": bbox,
                    "status": STATUS_PENDING,
                    "parcels_count": 0,
                }
            },
            upsert=True,
        )
        return result.upserted_id is not None

    def ensure_cells_exist(
        self,
        cells: List[Dict[str, Any]],
        batch_size: int = 5000,
    ) -> int:
        """
        Створює комірки зі статусом pending, якщо їх ще немає.
        cells: список {"cell_id", "zoom", "bbox"}.
        batch_size: розмір batch для bulk_write.
        Повертає кількість створених.
        """
        if not cells:
            return 0
        created = 0
        for i in range(0, len(cells), batch_size):
            batch = cells[i : i + batch_size]
            ops = []
            for c in batch:
                doc = {
                    "cell_id": c["cell_id"],
                    "zoom": c["zoom"],
                    "bbox": c.get("bbox", {}),
                    "status": STATUS_PENDING,
                    "parcels_count": 0,
                }
                if "tile_x" in c:
                    doc["tile_x"] = c["tile_x"]
                if "tile_y" in c:
                    doc["tile_y"] = c["tile_y"]
                if "sort_priority" in c:
                    doc["sort_priority"] = c["sort_priority"]
                ops.append(UpdateOne({"cell_id": c["cell_id"]}, {"$setOnInsert": doc}, upsert=True))
            result = self.collection.bulk_write(ops, ordered=False)
            created += result.upserted_count
        return created

    def get_stats(self) -> Dict[str, int]:
        """Повертає статистику: total, pending, done, error, processing."""
        pipeline = [
            {"$group": {"_id": "$status", "count": {"$sum": 1}}},
        ]
        result = list(self.collection.aggregate(pipeline))
        stats = {
            "total": 0,
            STATUS_PENDING: 0,
            STATUS_DONE: 0,
            STATUS_ERROR: 0,
            STATUS_PROCESSING: 0,
        }
        for r in result:
            s = r.get("_id", "")
            c = r.get("count", 0)
            stats["total"] += c
            if s in stats:
                stats[s] = c
        return stats

    def ensure_index(self) -> None:
        """Створює індекси."""
        self.collection.create_index("cell_id", unique=True)
        self.collection.create_index("status")
        self.collection.create_index([("status", 1), ("processed_at", 1)])
        self.collection.create_index([("status", 1), ("sort_priority", 1), ("cell_id", 1)])
