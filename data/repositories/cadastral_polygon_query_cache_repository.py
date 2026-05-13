# -*- coding: utf-8 -*-
"""
Кеш результатів полігон-пошуку кадастрових ділянок (топонім + фільтри + fingerprint полігона).
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "cadastral_polygon_query_cache"


class CadastralPolygonQueryCacheRepository(BaseRepository):
    """Зберігає cadastral_numbers та метадані для пагінації LLM-тулзів."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def _ensure_indexes(self) -> None:
        try:
            self.collection.create_index("query_fingerprint", unique=True, name="query_fingerprint_unique")
            self.collection.create_index("query_id", unique=True, name="query_id_unique")
            self.collection.create_index("created_at", name="created_at_idx")
        except Exception:
            pass

    def find_by_fingerprint(self, query_fingerprint: str) -> Optional[Dict[str, Any]]:
        self._ensure_indexes()
        doc = self.collection.find_one({"query_fingerprint": query_fingerprint})
        return self._norm(doc)

    def find_by_query_id(self, query_id: str) -> Optional[Dict[str, Any]]:
        self._ensure_indexes()
        doc = self.collection.find_one({"query_id": str(query_id).strip()})
        return self._norm(doc)

    @staticmethod
    def _norm(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        if not doc:
            return None
        if "_id" in doc and hasattr(doc["_id"], "binary"):
            doc["_id"] = str(doc["_id"])
        return doc

    def save_query_result(
        self,
        *,
        query_fingerprint: str,
        query_id: str,
        toponym: str,
        boundary_kind: str,
        formatted_address: Optional[str],
        place_id: Optional[str],
        polygon: Dict[str, Any],
        purpose_codes: List[str],
        business_groups: List[str],
        cadastral_numbers: List[str],
        total_count: int,
        truncated: bool,
    ) -> bool:
        self._ensure_indexes()
        now = datetime.now(timezone.utc)
        doc: Dict[str, Any] = {
            "query_fingerprint": query_fingerprint,
            "query_id": query_id,
            "toponym": toponym,
            "boundary_kind": boundary_kind,
            "formatted_address": formatted_address,
            "place_id": place_id,
            "polygon": polygon,
            "purpose_codes": purpose_codes,
            "business_groups": business_groups,
            "cadastral_numbers": cadastral_numbers,
            "total_count": int(total_count),
            "truncated": bool(truncated),
            "created_at": now,
        }
        try:
            self.collection.replace_one(
                {"query_fingerprint": query_fingerprint},
                doc,
                upsert=True,
            )
            return True
        except Exception:
            return False
