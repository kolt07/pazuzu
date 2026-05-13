# -*- coding: utf-8 -*-
"""
Статус довгої національної кластеризації кадастру (Celery / фоновий потік).
"""

from datetime import datetime, timezone
from typing import Any, Dict, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "cadastral_cluster_build_jobs"


class CadastralClusterBuildJobRepository(BaseRepository):
    """Один документ на job_id."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def _ensure(self) -> None:
        try:
            self.collection.create_index("job_id", unique=True, name="job_id_unique")
            self.collection.create_index("updated_at", name="updated_at_idx")
        except Exception:
            pass

    def upsert_job(self, job_id: str, **fields: Any) -> None:
        self._ensure()
        jid = str(job_id or "").strip()
        if not jid:
            return
        now = datetime.now(timezone.utc)
        doc = {"job_id": jid, "updated_at": now, **fields}
        self.collection.replace_one({"job_id": jid}, doc, upsert=True)

    def patch_job(self, job_id: str, patch: Dict[str, Any]) -> None:
        self._ensure()
        jid = str(job_id or "").strip()
        if not jid or not patch:
            return
        patch = {**patch, "updated_at": datetime.now(timezone.utc)}
        self.collection.update_one({"job_id": jid}, {"$set": patch}, upsert=False)

    def get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        self._ensure()
        doc = self.collection.find_one({"job_id": str(job_id or "").strip()})
        if doc and "_id" in doc and hasattr(doc["_id"], "binary"):
            doc["_id"] = str(doc["_id"])
        return doc
