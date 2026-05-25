# -*- coding: utf-8 -*-
"""Репозиторій сирих даних НП з mista.ua."""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository


class RawMistaSettlementsRepository(BaseRepository):
    """Колекція raw_mista_settlements."""

    def __init__(self):
        super().__init__("raw_mista_settlements")
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        if self._indexes_created:
            return
        try:
            self.collection.create_index("mista_url", unique=True)
            self._ensure_mista_id_index_non_unique()
            self.collection.create_index("scrape_status")
            self._indexes_created = True
        except Exception:
            pass

    def _ensure_mista_id_index_non_unique(self) -> None:
        """Прибирає legacy unique індекс mista_id (null/дублі ламали insert)."""
        try:
            for idx in self.collection.list_indexes():
                name = idx.get("name") or ""
                key = idx.get("key") or {}
                if "mista_id" not in key:
                    continue
                if idx.get("unique"):
                    self.collection.drop_index(name)
        except Exception:
            pass
        try:
            self.collection.create_index("mista_id", sparse=True)
        except Exception:
            pass

    def _safe_mista_id_for_write(
        self,
        mista_id: Optional[int],
        mista_url: str,
        *,
        allow_overwrite_existing: bool = False,
    ) -> Optional[int]:
        """Не записує mista_id, якщо вже зайнятий іншим URL (legacy-помилки парсера)."""
        if mista_id is None:
            return None
        other = self.find_one({"mista_id": mista_id, "mista_url": {"$ne": mista_url}})
        if other and not allow_overwrite_existing:
            return None
        return mista_id

    def upsert_list_row(self, row: Dict[str, Any]) -> str:
        self._ensure_indexes()
        url = row.get("mista_url")
        if not url:
            raise ValueError("mista_url required")
        now = datetime.now(timezone.utc)
        mista_id = self._safe_mista_id_for_write(row.get("mista_id"), url)
        fields: Dict[str, Any] = {
            "mista_url": url,
            "name": row.get("name"),
            "region_name": row.get("region_name"),
            "oblast_rayon_name": row.get("oblast_rayon_name"),
            "settlement_status": row.get("settlement_status"),
            "population": row.get("population"),
            "area_sq_km": row.get("area_sq_km"),
            "list_page": row.get("list_page"),
            "scrape_status": "list_only",
            "updated_at": now,
        }
        if mista_id is not None:
            fields["mista_id"] = mista_id
        existing = self.find_one({"mista_url": url})
        if existing:
            update_op: Dict[str, Any] = {"$set": {**fields, "created_at": existing.get("created_at", now)}}
            if mista_id is None:
                update_op["$unset"] = {"mista_id": ""}
            self.collection.update_one({"mista_url": url}, update_op)
            return str(existing["_id"])
        fields["created_at"] = now
        return self.create(fields)

    def upsert_parsed(self, mista_url: str, parsed: Dict[str, Any]) -> None:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        mista_id = self._safe_mista_id_for_write(parsed.get("mista_id"), mista_url)
        update_fields: Dict[str, Any] = {
            "parsed": parsed,
            "scrape_status": "parsed",
            "name": parsed.get("name"),
            "region_name": parsed.get("region_name"),
            "updated_at": now,
        }
        if mista_id is not None:
            update_fields["mista_id"] = mista_id
        self.collection.update_one(
            {"mista_url": mista_url},
            {"$set": update_fields},
            upsert=True,
        )

    def find_pending_details(self, limit: int = 0) -> List[Dict[str, Any]]:
        self._ensure_indexes()
        filt = {"scrape_status": {"$in": ["list_only", None]}}
        cursor = self.collection.find(filt, sort=[("list_page", 1), ("name", 1)])
        if limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)

    def find_parsed(self, limit: int = 0) -> List[Dict[str, Any]]:
        filt = {"scrape_status": "parsed", "parsed": {"$exists": True}}
        cursor = self.collection.find(filt)
        if limit > 0:
            cursor = cursor.limit(limit)
        return list(cursor)

    def count_by_status(self) -> Dict[str, int]:
        pipeline = [{"$group": {"_id": "$scrape_status", "n": {"$sum": 1}}}]
        out: Dict[str, int] = {}
        for row in self.collection.aggregate(pipeline):
            out[str(row.get("_id") or "unknown")] = row.get("n", 0)
        return out

    def count_total(self) -> int:
        self._ensure_indexes()
        return self.collection.count_documents({})

    def count_pending_details(self) -> int:
        self._ensure_indexes()
        return self.collection.count_documents({"scrape_status": {"$in": ["list_only", None]}})
