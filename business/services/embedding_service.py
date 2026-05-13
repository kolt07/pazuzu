# -*- coding: utf-8 -*-
"""
EmbeddingService — обгортка над HuggingFace Text Embeddings Inference (TEI).

Призначення:
  - єдиний вхід у систему генерації векторів для unified_listings / cadastral_parcels;
  - батчинг + ретраї + кеш у Mongo embedding_cache (за SHA-256 хешем тексту);
  - тонкий wrapper, без бізнес-логіки. Composition промпт/тексту лежить
    у VectorIndexService (`_compose_listing_text` / `_compose_parcel_text`).

Цей сервіс не виконує жодної бізнес-логіки і не приймає бізнес-рішень — він тільки
відповідає за роботу з ембедингами (вимога llm-agent-architecture: LLM/ML утиліти
ізольовані за чіткими інтерфейсами).
"""
from __future__ import annotations

import hashlib
import logging
import threading
import time
from datetime import datetime, timezone
from typing import Any, Dict, Iterable, List, Optional, Sequence

import httpx

from data.database.connection import MongoDBConnection


logger = logging.getLogger(__name__)


_EMBEDDING_CACHE_COLLECTION = "embedding_cache"
_DEFAULT_BATCH_SIZE = 16
_DEFAULT_TIMEOUT_SEC = 30
_DEFAULT_MAX_RETRIES = 3


class EmbeddingService:
    """
    Тонкий клієнт TEI / OpenAI-compatible embedding API з Mongo-кешем.

    Використання::

        svc = EmbeddingService(settings)
        vectors = svc.embed_texts(["щось", "ще"])

    Якщо `endpoint` не сконфігуровано або сервіс недоступний — `embed_*` повертають
    `None` для кожного непрорахованого елемента. Це дозволяє виклику нагору (pipeline
    парсингу) виконувати best-effort upsert у Qdrant без падіння основного потоку.
    """

    _instance: Optional["EmbeddingService"] = None
    _instance_lock = threading.Lock()

    def __init__(self, settings: Any):
        self._settings = settings
        self._endpoint: str = str(getattr(settings, "embeddings_endpoint", "") or "").rstrip("/")
        self._model: str = str(getattr(settings, "embeddings_model", "BAAI/bge-m3") or "BAAI/bge-m3")
        self._batch_size: int = int(getattr(settings, "embeddings_batch_size", _DEFAULT_BATCH_SIZE) or _DEFAULT_BATCH_SIZE)
        self._timeout: int = int(getattr(settings, "embeddings_timeout_sec", _DEFAULT_TIMEOUT_SEC) or _DEFAULT_TIMEOUT_SEC)
        self._max_retries: int = int(getattr(settings, "embeddings_max_retries", _DEFAULT_MAX_RETRIES) or _DEFAULT_MAX_RETRIES)
        self._cache_enabled: bool = bool(getattr(settings, "embeddings_cache_enabled", True))
        self._dimension: int = int(getattr(settings, "vector_size", 1024) or 1024)

        self._client: Optional[httpx.Client] = None
        self._cache_coll = None
        if self._cache_enabled:
            try:
                self._cache_coll = MongoDBConnection.get_database().get_collection(_EMBEDDING_CACHE_COLLECTION)
            except Exception as e:
                logger.warning("EmbeddingService: cache disabled (mongo unavailable): %s", e)
                self._cache_coll = None

    @classmethod
    def get_instance(cls, settings: Any) -> "EmbeddingService":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(settings)
        return cls._instance

    @property
    def model_name(self) -> str:
        return self._model

    @property
    def dimension(self) -> int:
        return self._dimension

    @property
    def is_configured(self) -> bool:
        return bool(self._endpoint)

    def _get_client(self) -> httpx.Client:
        if self._client is None:
            self._client = httpx.Client(
                base_url=self._endpoint,
                timeout=httpx.Timeout(self._timeout, connect=5.0),
                headers={"Content-Type": "application/json"},
            )
        return self._client

    @staticmethod
    def _hash_text(text: str, model_name: str) -> str:
        h = hashlib.sha256()
        h.update(model_name.encode("utf-8", errors="ignore"))
        h.update(b"\x00")
        h.update(text.encode("utf-8", errors="ignore"))
        return h.hexdigest()

    def _cache_get_many(self, hashes: Sequence[str]) -> Dict[str, List[float]]:
        if not self._cache_coll or not hashes:
            return {}
        try:
            cursor = self._cache_coll.find(
                {"text_hash": {"$in": list(hashes)}, "model_name": self._model},
                {"text_hash": 1, "vector": 1, "_id": 0},
            )
            result: Dict[str, List[float]] = {}
            now = datetime.now(timezone.utc)
            touched: List[str] = []
            for doc in cursor:
                th = doc.get("text_hash")
                vec = doc.get("vector")
                if th and isinstance(vec, list):
                    result[th] = vec
                    touched.append(th)
            if touched:
                try:
                    self._cache_coll.update_many(
                        {"text_hash": {"$in": touched}, "model_name": self._model},
                        {"$set": {"last_used_at": now}},
                    )
                except Exception:
                    pass
            return result
        except Exception as e:
            logger.warning("embedding_cache get failed: %s", e)
            return {}

    def _cache_set_many(self, items: Sequence[Dict[str, Any]]) -> None:
        if not self._cache_coll or not items:
            return
        try:
            now = datetime.now(timezone.utc)
            from pymongo import UpdateOne

            ops: List[UpdateOne] = []
            for it in items:
                ops.append(
                    UpdateOne(
                        {"text_hash": it["text_hash"], "model_name": self._model},
                        {
                            "$setOnInsert": {
                                "text_hash": it["text_hash"],
                                "model_name": self._model,
                                "created_at": now,
                                "dim": len(it["vector"]),
                            },
                            "$set": {
                                "vector": it["vector"],
                                "last_used_at": now,
                            },
                        },
                        upsert=True,
                    )
                )
            if ops:
                self._cache_coll.bulk_write(ops, ordered=False)
        except Exception as e:
            logger.warning("embedding_cache set failed: %s", e)

    def _post_embed(self, texts: Sequence[str]) -> List[List[float]]:
        """Виклик TEI /embed з простим експоненційним бекофом на 5xx/мережу."""
        if not texts:
            return []
        client = self._get_client()
        backoff = 1.0
        last_err: Optional[Exception] = None
        for attempt in range(1, self._max_retries + 1):
            try:
                resp = client.post("/embed", json={"inputs": list(texts)})
                if resp.status_code == 200:
                    data = resp.json()
                    if isinstance(data, list):
                        return [list(map(float, v)) for v in data]
                    if isinstance(data, dict) and isinstance(data.get("embeddings"), list):
                        return [list(map(float, v)) for v in data["embeddings"]]
                    raise RuntimeError(f"unexpected TEI response shape: {type(data).__name__}")
                # 4xx — не ретраїмо, окрім 429
                if 400 <= resp.status_code < 500 and resp.status_code != 429:
                    raise RuntimeError(f"TEI {resp.status_code}: {resp.text[:300]}")
                last_err = RuntimeError(f"TEI {resp.status_code}: {resp.text[:200]}")
            except (httpx.TimeoutException, httpx.NetworkError, httpx.HTTPError) as e:
                last_err = e
            if attempt < self._max_retries:
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)
        raise RuntimeError(f"embed failed after {self._max_retries} attempts: {last_err}")

    def embed_texts(self, texts: Iterable[str]) -> List[Optional[List[float]]]:
        """
        Повертає список векторів у тому ж порядку. None — якщо для конкретного
        тексту вектор не вдалося отримати (наприклад, ендпоінт недоступний).
        """
        items: List[str] = [t if isinstance(t, str) else str(t or "") for t in texts]
        if not items:
            return []
        if not self._endpoint:
            logger.debug("EmbeddingService: endpoint not configured, returning None for %d texts", len(items))
            return [None] * len(items)

        hashes = [self._hash_text(t, self._model) for t in items]
        cache_hits = self._cache_get_many(list({h for h in hashes}))

        out: List[Optional[List[float]]] = [None] * len(items)
        to_compute_idx: List[int] = []
        to_compute_texts: List[str] = []
        for i, (txt, h) in enumerate(zip(items, hashes)):
            cached = cache_hits.get(h)
            if cached is not None:
                out[i] = cached
            else:
                to_compute_idx.append(i)
                to_compute_texts.append(txt)

        if not to_compute_idx:
            return out

        produced: List[Dict[str, Any]] = []
        for start in range(0, len(to_compute_texts), self._batch_size):
            batch_texts = to_compute_texts[start : start + self._batch_size]
            batch_idx = to_compute_idx[start : start + self._batch_size]
            try:
                vectors = self._post_embed(batch_texts)
            except Exception as e:
                logger.warning("EmbeddingService: batch %d-%d failed: %s", start, start + len(batch_texts), e)
                for idx in batch_idx:
                    out[idx] = None
                continue
            if len(vectors) != len(batch_texts):
                logger.warning(
                    "EmbeddingService: TEI returned %d vectors for %d texts",
                    len(vectors), len(batch_texts),
                )
            for offset, vec in enumerate(vectors):
                if offset >= len(batch_idx):
                    break
                idx = batch_idx[offset]
                out[idx] = vec
                produced.append({"text_hash": hashes[idx], "vector": vec})

        if produced:
            self._cache_set_many(produced)

        return out

    def embed_text(self, text: str) -> Optional[List[float]]:
        result = self.embed_texts([text])
        return result[0] if result else None

    def close(self) -> None:
        try:
            if self._client is not None:
                self._client.close()
        except Exception:
            pass
        self._client = None
