# -*- coding: utf-8 -*-
"""
Семантичний індекс MCP tools у Qdrant для retrieval перед bind_tools.

Якщо Qdrant/TEI недоступні — повертаємо повний набір імен (fallback на рівні викликача).
"""

from __future__ import annotations

import hashlib
import logging
from typing import Any, Dict, List, Optional, Set

from business.services.embedding_service import EmbeddingService

logger = logging.getLogger(__name__)


def _point_id(tool_name: str) -> str:
    raw = f"mcp_tool::{(tool_name or '').strip()}".encode("utf-8", errors="ignore")
    return hashlib.md5(raw).hexdigest()


def _qdrant_ok() -> bool:
    try:
        from qdrant_client import QdrantClient  # noqa: F401

        return True
    except Exception:
        return False


class ToolSemanticIndexService:
    """Upsert описів tools і пошук top-k за текстом запиту."""

    def __init__(self, settings: Any):
        self._settings = settings
        self._host = str(getattr(settings, "qdrant_host", "localhost") or "localhost")
        self._port = int(getattr(settings, "qdrant_port", 6333) or 6333)
        self._grpc_port = int(getattr(settings, "qdrant_grpc_port", 6334) or 6334)
        self._use_grpc = bool(getattr(settings, "qdrant_use_grpc", False))
        self._api_key = str(getattr(settings, "qdrant_api_key", "") or "") or None
        self._coll = str(getattr(settings, "vector_collection_mcp_tools", "mcp_tools_vec") or "mcp_tools_vec")
        self._vector_size = int(getattr(settings, "vector_size", 1024) or 1024)
        self._distance = str(getattr(settings, "vector_distance", "cosine") or "cosine").lower()
        self._embedder = EmbeddingService.get_instance(settings)
        self._client = None

    @property
    def is_configured(self) -> bool:
        return bool(self._host) and self._embedder.is_configured and _qdrant_ok()

    def _get_client(self):
        if self._client is not None:
            return self._client
        if not _qdrant_ok():
            return None
        try:
            from qdrant_client import QdrantClient

            kwargs: Dict[str, Any] = {"host": self._host}
            if self._use_grpc:
                kwargs.update({"grpc_port": self._grpc_port, "prefer_grpc": True, "port": self._port})
            else:
                kwargs["port"] = self._port
            if self._api_key:
                kwargs["api_key"] = self._api_key
            self._client = QdrantClient(**kwargs)
        except Exception as e:
            logger.warning("ToolSemanticIndexService: Qdrant init failed: %s", e)
            self._client = None
        return self._client

    def _distance_enum(self):
        from qdrant_client.http import models as qmodels

        mapping = {
            "cosine": qmodels.Distance.COSINE,
            "dot": qmodels.Distance.DOT,
            "euclid": qmodels.Distance.EUCLID,
            "euclidean": qmodels.Distance.EUCLID,
        }
        return mapping.get(self._distance, qmodels.Distance.COSINE)

    def ensure_collection(self) -> bool:
        client = self._get_client()
        if client is None or not self._embedder.is_configured:
            return False
        try:
            from qdrant_client.http import models as qmodels

            existing = {c.name for c in client.get_collections().collections}
            if self._coll not in existing:
                client.create_collection(
                    collection_name=self._coll,
                    vectors_config=qmodels.VectorParams(size=self._vector_size, distance=self._distance_enum()),
                )
                logger.info("ToolSemanticIndexService: created Qdrant collection %s", self._coll)
            try:
                client.create_payload_index(
                    collection_name=self._coll,
                    field_name="tool_name",
                    field_schema=qmodels.PayloadSchemaType.KEYWORD,
                )
            except Exception:
                pass
            return True
        except Exception as e:
            logger.warning("ToolSemanticIndexService: ensure_collection failed: %s", e)
            return False

    def sync_tools(self, tools: List[Any]) -> int:
        """Індексує LangChain StructuredTool (name + description)."""
        if not tools or not self.is_configured:
            return 0
        if not self.ensure_collection():
            return 0
        client = self._get_client()
        if client is None:
            return 0
        texts: List[str] = []
        payloads: List[Dict[str, Any]] = []
        ids: List[str] = []
        for t in tools:
            name = getattr(t, "name", "") or ""
            desc = getattr(t, "description", "") or ""
            if not name:
                continue
            txt = f"Tool: {name}\n{desc}".strip()
            if not txt:
                continue
            texts.append(txt[:4000])
            payloads.append({"tool_name": name, "text_preview": txt[:512]})
            ids.append(_point_id(name))
        if not texts:
            return 0
        vectors = self._embedder.embed_texts(texts)
        try:
            from qdrant_client.http import models as qmodels

            points = []
            for pid, vec, payload in zip(ids, vectors, payloads):
                if vec is None:
                    continue
                points.append(qmodels.PointStruct(id=pid, vector=vec, payload=payload))
            if not points:
                return 0
            client.upsert(collection_name=self._coll, points=points, wait=False)
            return len(points)
        except Exception as e:
            logger.warning("ToolSemanticIndexService: upsert failed: %s", e)
            return 0

    def retrieve_tool_names(
        self,
        query_text: str,
        allowed_names: Optional[Set[str]] = None,
        top_k: int = 12,
    ) -> List[str]:
        """Повертає ranked імена tools (тільки ті, що в allowed_names якщо задано)."""
        if not query_text or not self.is_configured:
            return []
        client = self._get_client()
        if client is None or not self.ensure_collection():
            return []
        vecs = self._embedder.embed_texts([query_text[:2000]])
        if not vecs or vecs[0] is None:
            return []
        vec = vecs[0]
        try:
            from qdrant_client.http import models as qmodels

            hits = client.search(
                collection_name=self._coll,
                query_vector=vec,
                limit=max(30, top_k * 3),
                with_payload=True,
            )
        except Exception as e:
            logger.debug("ToolSemanticIndexService search: %s", e)
            return []
        out: List[str] = []
        for h in hits:
            pl = h.payload or {}
            name = str(pl.get("tool_name") or "").strip()
            if not name:
                continue
            if allowed_names is not None and name not in allowed_names:
                continue
            out.append(name)
            if len(out) >= top_k:
                break
        return out

    def collection_point_count(self) -> int:
        client = self._get_client()
        if client is None:
            return 0
        try:
            info = client.get_collection(self._coll)
            return int(getattr(info, "points_count", 0) or 0)
        except Exception:
            return 0
