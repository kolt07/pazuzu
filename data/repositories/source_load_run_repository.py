# -*- coding: utf-8 -*-
"""
Durable прогрес завантаження з джерел (Phase 1/2) для resume після перезапуску.
Колекція source_load_runs.
"""

from __future__ import annotations

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional
from uuid import uuid4

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "source_load_runs"

STATUS_PENDING = "pending"
STATUS_RUNNING = "running"
STATUS_PHASE1_DONE = "phase1_done"
STATUS_PHASE2_WAITING = "phase2_waiting"
STATUS_DONE = "done"
STATUS_FAILED = "failed"

UNIT_PENDING = "pending"
UNIT_RUNNING = "running"
UNIT_DONE = "done"


def make_olx_unit_key(region: str, category_label: str) -> str:
    return f"olx|{region}|{category_label}"


def make_prozorro_day_unit_key(day_iso: str) -> str:
    return f"prozorro|day|{day_iso}"


class SourceLoadRunRepository(BaseRepository):
    """Один документ на source_load run_id."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def _ensure_indexes(self) -> None:
        try:
            self.collection.create_index("run_id", unique=True, name="run_id_unique")
            self.collection.create_index("status", name="status_idx")
            self.collection.create_index("updated_at", name="updated_at_idx")
            self.collection.create_index("celery_task_id", name="celery_task_id_idx")
        except Exception:
            pass

    def create_run(
        self,
        *,
        sources: Optional[List[str]] = None,
        days: Optional[int] = None,
        regions: Optional[List[str]] = None,
        listing_types: Optional[List[str]] = None,
        olx_phase1_max_threads: Optional[int] = None,
        work_units: Optional[List[Dict[str, Any]]] = None,
        celery_task_id: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> str:
        self._ensure_indexes()
        rid = (run_id or "").strip() or uuid4().hex
        now = datetime.now(timezone.utc)
        doc = {
            "run_id": rid,
            "status": STATUS_PENDING,
            "sources": list(sources or []),
            "days": days,
            "regions": list(regions or []),
            "listing_types": list(listing_types or []),
            "olx_phase1_max_threads": olx_phase1_max_threads,
            "work_units": list(work_units or []),
            "phase2": {},
            "celery_task_id": celery_task_id,
            "message": "created",
            "created_at": now,
            "updated_at": now,
        }
        self.collection.update_one(
            {"run_id": rid},
            {"$setOnInsert": doc},
            upsert=True,
        )
        return rid

    def get_run(self, run_id: str) -> Optional[Dict[str, Any]]:
        self._ensure_indexes()
        doc = self.collection.find_one({"run_id": str(run_id or "").strip()})
        if doc and "_id" in doc and hasattr(doc["_id"], "binary"):
            doc["_id"] = str(doc["_id"])
        return doc

    def patch_run(self, run_id: str, patch: Dict[str, Any]) -> None:
        self._ensure_indexes()
        rid = str(run_id or "").strip()
        if not rid or not patch:
            return
        patch = {**patch, "updated_at": datetime.now(timezone.utc)}
        self.collection.update_one({"run_id": rid}, {"$set": patch}, upsert=False)

    def set_work_units(self, run_id: str, work_units: List[Dict[str, Any]]) -> None:
        self.patch_run(run_id, {"work_units": list(work_units or [])})

    def get_unit(self, run_id: str, unit_key: str) -> Optional[Dict[str, Any]]:
        run = self.get_run(run_id)
        if not run:
            return None
        for u in run.get("work_units") or []:
            if u.get("key") == unit_key:
                return u
        return None

    def patch_work_unit(self, run_id: str, unit_key: str, patch: Dict[str, Any]) -> None:
        """Оновлює один work unit через positional $ (key match)."""
        self._ensure_indexes()
        rid = str(run_id or "").strip()
        uk = str(unit_key or "").strip()
        if not rid or not uk or not patch:
            return
        set_fields = {f"work_units.$.{k}": v for k, v in patch.items()}
        set_fields["updated_at"] = datetime.now(timezone.utc)
        set_fields["work_units.$.updated_at"] = set_fields["updated_at"]
        self.collection.update_one(
            {"run_id": rid, "work_units.key": uk},
            {"$set": set_fields},
        )

    def mark_unit_running(self, run_id: str, unit_key: str) -> None:
        self.patch_work_unit(run_id, unit_key, {"status": UNIT_RUNNING})

    def mark_unit_done(self, run_id: str, unit_key: str, last_page: Optional[int] = None) -> None:
        patch: Dict[str, Any] = {"status": UNIT_DONE}
        if last_page is not None:
            patch["last_page"] = int(last_page)
        self.patch_work_unit(run_id, unit_key, patch)

    def set_unit_last_page(self, run_id: str, unit_key: str, last_page: int) -> None:
        self.patch_work_unit(run_id, unit_key, {"last_page": int(last_page), "status": UNIT_RUNNING})

    def all_units_done(self, run_id: str, source: Optional[str] = None) -> bool:
        run = self.get_run(run_id)
        if not run:
            return False
        units = run.get("work_units") or []
        if not units:
            return True
        for u in units:
            if source and u.get("source") != source:
                continue
            if u.get("status") != UNIT_DONE:
                return False
        return True

    def patch_phase2(self, run_id: str, phase2_patch: Dict[str, Any]) -> None:
        rid = str(run_id or "").strip()
        if not rid or not phase2_patch:
            return
        set_fields = {f"phase2.{k}": v for k, v in phase2_patch.items()}
        set_fields["updated_at"] = datetime.now(timezone.utc)
        self.collection.update_one({"run_id": rid}, {"$set": set_fields})
