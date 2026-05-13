# -*- coding: utf-8 -*-
"""
VectorIndexService — обгортка над Qdrant для семантичного пошуку по
unified_listings та cadastral_parcels (+ кластерах).

Сервіс не приймає бізнес-рішень: він тільки веде векторні індекси та
повертає top-K результати з payload-фільтрами. LLM-агенти викликають
його винятково через зареєстровані FLX/MCP tools (див.
investigation_service._build_tools_registry).
"""
from __future__ import annotations

import hashlib
import logging
import threading
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

from business.services.embedding_service import EmbeddingService


logger = logging.getLogger(__name__)


def _qdrant_models():
    """Лениве імпортування qdrant_client — не блокуємо старт, якщо лібу ще не встановлено."""
    try:
        from qdrant_client import QdrantClient  # noqa: F401
        from qdrant_client.http import models as qmodels  # noqa: F401
        from qdrant_client.http.exceptions import UnexpectedResponse  # noqa: F401
        return True
    except Exception as e:  # pragma: no cover
        logger.warning("qdrant-client not installed: %s", e)
        return False


def _make_point_id(source: str, source_id: str) -> str:
    raw = f"{source}::{source_id}".encode("utf-8", errors="ignore")
    return hashlib.md5(raw).hexdigest()


def _compose_listing_text(doc: Mapping[str, Any]) -> str:
    """Композитний текст для семантичного індексу unified_listings."""
    title = (doc.get("title") or "").strip()
    description = (doc.get("description") or "").strip()
    property_type = (doc.get("property_type") or "").strip()
    region = (doc.get("region") or "").strip()
    oblast = (doc.get("oblast_raion") or "").strip()
    city = (doc.get("city") or "").strip()
    district = (doc.get("city_district") or "").strip()
    tags = doc.get("tags") or []
    if not isinstance(tags, list):
        tags = []
    tags_str = ", ".join(str(t) for t in tags if t)
    price_uah = doc.get("price_uah")
    price_usd = doc.get("price_usd")
    building_area = doc.get("building_area_sqm")
    land_area = doc.get("land_area_sqm")
    cadastral = doc.get("cadastral_numbers") or []
    cad_str = ", ".join(str(c) for c in cadastral if c)

    geo_parts = [p for p in (region, oblast, city, district) if p]
    geo_str = " / ".join(geo_parts)

    lines = [
        title or "",
        description or "",
        f"[type] {property_type}" if property_type else "",
        f"[geo] {geo_str}" if geo_str else "",
        f"[tags] {tags_str}" if tags_str else "",
    ]
    if price_uah is not None or price_usd is not None:
        price_bits = []
        if price_uah is not None:
            price_bits.append(f"{price_uah} UAH")
        if price_usd is not None:
            price_bits.append(f"{price_usd} USD")
        lines.append(f"[price] {' / '.join(price_bits)}")
    if building_area or land_area:
        lines.append(f"[area] building={building_area or 0} land={land_area or 0}")
    if cad_str:
        lines.append(f"[cadastre] {cad_str}")
    return "\n".join([ln for ln in lines if ln])


def _compose_parcel_text(doc: Mapping[str, Any]) -> str:
    """Композитний текст для семантичного індексу cadastral_parcels."""
    cadastral_number = (doc.get("cadastral_number") or "").strip()
    purpose = (doc.get("purpose") or "").strip()
    purpose_label = (doc.get("purpose_label") or "").strip()
    category = (doc.get("category") or "").strip()
    ownership = (doc.get("ownership_form") or "").strip()
    oblast = (doc.get("oblast_name") or doc.get("oblast") or "").strip()
    raion = (doc.get("raion_name") or doc.get("raion") or "").strip()
    settlement = (doc.get("settlement_name") or doc.get("settlement") or "").strip()
    area = doc.get("area_sqm")

    geo_parts = [p for p in (oblast, raion, settlement) if p]
    geo_str = " / ".join(geo_parts)

    lines = [
        f"[cadastre] {cadastral_number}" if cadastral_number else "",
        f"[purpose] {purpose_label or purpose}",
        f"[category] {category}" if category else "",
        f"[ownership] {ownership}" if ownership else "",
        f"[geo] {geo_str}" if geo_str else "",
    ]
    if area is not None:
        lines.append(f"[area_sqm] {area}")
    return "\n".join([ln for ln in lines if ln])


def _compose_cluster_text(doc: Mapping[str, Any]) -> str:
    purpose = (doc.get("purpose") or "").strip()
    purpose_label = (doc.get("purpose_label") or "").strip()
    ownership = (doc.get("ownership_form") or "").strip()
    parcel_count = doc.get("parcel_count")
    total_area_sqm = doc.get("total_area_sqm")
    cadnums = doc.get("cadastral_numbers") or []
    sample = ", ".join(str(c) for c in cadnums[:5] if c)
    lines = [
        f"[cluster] purpose={purpose_label or purpose} ownership={ownership}",
        f"[size] parcels={parcel_count or 0} total_area_sqm={total_area_sqm or 0}",
    ]
    if sample:
        lines.append(f"[sample_cadnums] {sample}")
    return "\n".join([ln for ln in lines if ln])


class VectorIndexService:
    """Тонкий клієнт Qdrant з payload-фільтрацією, embedding всередині."""

    _instance: Optional["VectorIndexService"] = None
    _instance_lock = threading.Lock()

    def __init__(self, settings: Any):
        self._settings = settings
        self._host = str(getattr(settings, "qdrant_host", "localhost") or "localhost")
        self._port = int(getattr(settings, "qdrant_port", 6333) or 6333)
        self._grpc_port = int(getattr(settings, "qdrant_grpc_port", 6334) or 6334)
        self._use_grpc = bool(getattr(settings, "qdrant_use_grpc", False))
        self._api_key = str(getattr(settings, "qdrant_api_key", "") or "") or None
        self._coll_listings = str(getattr(settings, "vector_collection_listings", "unified_listings_vec"))
        self._coll_cadastral = str(getattr(settings, "vector_collection_cadastral", "cadastral_parcels_vec"))
        self._vector_size = int(getattr(settings, "vector_size", 1024) or 1024)
        self._distance = str(getattr(settings, "vector_distance", "cosine") or "cosine").lower()

        self._embedder = EmbeddingService.get_instance(settings)
        self._client = None
        self._ensured = False

    @classmethod
    def get_instance(cls, settings: Any) -> "VectorIndexService":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = cls(settings)
        return cls._instance

    @property
    def is_configured(self) -> bool:
        return bool(self._host) and self._embedder.is_configured

    def _get_client(self):
        if self._client is not None:
            return self._client
        if not _qdrant_models():
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
            logger.warning("Qdrant client init failed: %s", e)
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

    def ensure_collections(self) -> bool:
        """Створює `unified_listings_vec` і `cadastral_parcels_vec`, якщо відсутні."""
        if self._ensured:
            return True
        client = self._get_client()
        if client is None:
            return False
        try:
            from qdrant_client.http import models as qmodels

            existing = {c.name for c in client.get_collections().collections}
            for coll in (self._coll_listings, self._coll_cadastral):
                if coll not in existing:
                    client.create_collection(
                        collection_name=coll,
                        vectors_config=qmodels.VectorParams(
                            size=self._vector_size,
                            distance=self._distance_enum(),
                        ),
                    )
                    logger.info("Qdrant: created collection %s (size=%d, distance=%s)", coll, self._vector_size, self._distance)
                self._ensure_payload_indexes(client, coll)
            self._ensured = True
            return True
        except Exception as e:
            logger.warning("Qdrant ensure_collections failed: %s", e)
            return False

    def _ensure_payload_indexes(self, client, coll: str) -> None:
        """Створює keyword/numeric індекси по часто-використовуваних payload-полях."""
        from qdrant_client.http import models as qmodels

        common_keyword = []
        common_float = []
        if coll == self._coll_listings:
            common_keyword = ["source", "region", "city", "property_type", "status"]
            common_float = ["price_uah", "price_usd", "building_area_sqm", "land_area_sqm"]
        elif coll == self._coll_cadastral:
            common_keyword = ["doc_type", "purpose", "purpose_label", "ownership_form", "oblast_name", "category"]
            common_float = ["area_sqm", "parcel_count", "total_area_sqm"]
        for field in common_keyword:
            try:
                client.create_payload_index(collection_name=coll, field_name=field, field_schema=qmodels.PayloadSchemaType.KEYWORD)
            except Exception:
                pass
        for field in common_float:
            try:
                client.create_payload_index(collection_name=coll, field_name=field, field_schema=qmodels.PayloadSchemaType.FLOAT)
            except Exception:
                pass

    @staticmethod
    def _safe_float(value: Any) -> Optional[float]:
        try:
            if value is None:
                return None
            return float(value)
        except (TypeError, ValueError):
            return None

    def _listing_payload(self, doc: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "doc_type": "listing",
            "source": doc.get("source"),
            "source_id": doc.get("source_id"),
            "page_url": doc.get("page_url"),
            "title": (doc.get("title") or "")[:512],
            "property_type": doc.get("property_type"),
            "status": doc.get("status"),
            "region": doc.get("region"),
            "oblast_raion": doc.get("oblast_raion"),
            "city": doc.get("city"),
            "city_district": doc.get("city_district"),
            "tags": doc.get("tags") or [],
            "price_uah": self._safe_float(doc.get("price_uah")),
            "price_usd": self._safe_float(doc.get("price_usd")),
            "building_area_sqm": self._safe_float(doc.get("building_area_sqm")),
            "land_area_sqm": self._safe_float(doc.get("land_area_sqm")),
            "cadastral_numbers": doc.get("cadastral_numbers") or [],
        }

    def _parcel_payload(self, doc: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "doc_type": "parcel",
            "cadastral_number": doc.get("cadastral_number"),
            "purpose": doc.get("purpose"),
            "purpose_label": doc.get("purpose_label"),
            "category": doc.get("category"),
            "ownership_form": doc.get("ownership_form"),
            "area_sqm": self._safe_float(doc.get("area_sqm")),
            "oblast_name": doc.get("oblast_name") or doc.get("oblast"),
            "raion_name": doc.get("raion_name") or doc.get("raion"),
            "settlement_name": doc.get("settlement_name") or doc.get("settlement"),
            "koatuu": doc.get("koatuu") or doc.get("koatuu_code"),
        }

    def _cluster_payload(self, doc: Mapping[str, Any]) -> Dict[str, Any]:
        return {
            "doc_type": "cluster",
            "cluster_id": doc.get("cluster_id"),
            "purpose": doc.get("purpose"),
            "purpose_label": doc.get("purpose_label"),
            "ownership_form": doc.get("ownership_form"),
            "parcel_count": self._safe_float(doc.get("parcel_count")),
            "total_area_sqm": self._safe_float(doc.get("total_area_sqm")),
            "sample_cadnums": (doc.get("cadastral_numbers") or [])[:10],
        }

    def upsert_listings(self, docs: Sequence[Mapping[str, Any]]) -> int:
        if not docs:
            return 0
        if not self.is_configured:
            return 0
        client = self._get_client()
        if client is None:
            return 0
        if not self.ensure_collections():
            return 0
        try:
            from qdrant_client.http import models as qmodels

            valid: List[Tuple[str, str, str, Dict[str, Any]]] = []
            for d in docs:
                source = (d.get("source") or "").strip()
                source_id = (d.get("source_id") or "").strip()
                if not source or not source_id:
                    continue
                text = _compose_listing_text(d)
                if not text:
                    continue
                pid = _make_point_id(source, source_id)
                valid.append((pid, source, source_id, dict(self._listing_payload(d))))

            if not valid:
                return 0
            texts = [_compose_listing_text(d) for d in docs if (d.get("source") and d.get("source_id"))]
            vectors = self._embedder.embed_texts(texts)
            points = []
            for (pid, _, _, payload), vec in zip(valid, vectors):
                if vec is None:
                    continue
                points.append(qmodels.PointStruct(id=pid, vector=vec, payload=payload))
            if not points:
                return 0
            client.upsert(collection_name=self._coll_listings, points=points, wait=False)
            return len(points)
        except Exception as e:
            logger.warning("Qdrant upsert_listings failed: %s", e)
            return 0

    def upsert_parcels(self, docs: Sequence[Mapping[str, Any]]) -> int:
        if not docs:
            return 0
        if not self.is_configured:
            return 0
        client = self._get_client()
        if client is None:
            return 0
        if not self.ensure_collections():
            return 0
        try:
            from qdrant_client.http import models as qmodels

            valid_docs: List[Mapping[str, Any]] = []
            ids: List[str] = []
            payloads: List[Dict[str, Any]] = []
            texts: List[str] = []
            for d in docs:
                cad = (d.get("cadastral_number") or "").strip()
                if not cad:
                    continue
                txt = _compose_parcel_text(d)
                if not txt:
                    continue
                valid_docs.append(d)
                ids.append(_make_point_id("parcel", cad))
                payloads.append(self._parcel_payload(d))
                texts.append(txt)
            if not texts:
                return 0
            vectors = self._embedder.embed_texts(texts)
            points = []
            for pid, vec, payload in zip(ids, vectors, payloads):
                if vec is None:
                    continue
                points.append(qmodels.PointStruct(id=pid, vector=vec, payload=payload))
            if not points:
                return 0
            client.upsert(collection_name=self._coll_cadastral, points=points, wait=False)
            return len(points)
        except Exception as e:
            logger.warning("Qdrant upsert_parcels failed: %s", e)
            return 0

    def upsert_clusters(self, docs: Sequence[Mapping[str, Any]]) -> int:
        if not docs:
            return 0
        if not self.is_configured:
            return 0
        client = self._get_client()
        if client is None:
            return 0
        if not self.ensure_collections():
            return 0
        try:
            from qdrant_client.http import models as qmodels

            ids: List[str] = []
            payloads: List[Dict[str, Any]] = []
            texts: List[str] = []
            for d in docs:
                cid = (d.get("cluster_id") or "").strip()
                if not cid:
                    continue
                txt = _compose_cluster_text(d)
                if not txt:
                    continue
                ids.append(_make_point_id("cluster", cid))
                payloads.append(self._cluster_payload(d))
                texts.append(txt)
            if not texts:
                return 0
            vectors = self._embedder.embed_texts(texts)
            points = []
            for pid, vec, payload in zip(ids, vectors, payloads):
                if vec is None:
                    continue
                points.append(qmodels.PointStruct(id=pid, vector=vec, payload=payload))
            if not points:
                return 0
            client.upsert(collection_name=self._coll_cadastral, points=points, wait=False)
            return len(points)
        except Exception as e:
            logger.warning("Qdrant upsert_clusters failed: %s", e)
            return 0

    def _build_filter(self, filters: Optional[Mapping[str, Any]], allowed_fields: Mapping[str, str]):
        """
        filters — словник з логічними ключами; повертає qmodels.Filter або None.
        allowed_fields = {"region": "keyword", "price_uah_min": "range:price_uah_gte", ...}
        """
        if not filters:
            return None
        try:
            from qdrant_client.http import models as qmodels
        except Exception:
            return None

        must: List[Any] = []
        ranges: Dict[str, Dict[str, float]] = {}
        for key, val in filters.items():
            if val is None or val == "":
                continue
            spec = allowed_fields.get(key)
            if not spec:
                continue
            if spec.startswith("keyword:"):
                field = spec.split(":", 1)[1]
                if isinstance(val, (list, tuple, set)):
                    must.append(qmodels.FieldCondition(key=field, match=qmodels.MatchAny(any=list(val))))
                else:
                    must.append(qmodels.FieldCondition(key=field, match=qmodels.MatchValue(value=val)))
            elif spec.startswith("range:"):
                # 'range:price_uah_gte' / 'range:price_uah_lte'
                rest = spec.split(":", 1)[1]
                if rest.endswith("_gte"):
                    field = rest[:-4]
                    ranges.setdefault(field, {})["gte"] = float(val)
                elif rest.endswith("_lte"):
                    field = rest[:-4]
                    ranges.setdefault(field, {})["lte"] = float(val)

        for field, bounds in ranges.items():
            must.append(qmodels.FieldCondition(key=field, range=qmodels.Range(**bounds)))

        if not must:
            return None
        return qmodels.Filter(must=must)

    _LISTING_FILTER_SCHEMA: Dict[str, str] = {
        "source": "keyword:source",
        "region": "keyword:region",
        "city": "keyword:city",
        "property_type": "keyword:property_type",
        "status": "keyword:status",
        "price_uah_min": "range:price_uah_gte",
        "price_uah_max": "range:price_uah_lte",
        "price_usd_min": "range:price_usd_gte",
        "price_usd_max": "range:price_usd_lte",
        "building_area_min": "range:building_area_sqm_gte",
        "building_area_max": "range:building_area_sqm_lte",
        "land_area_min": "range:land_area_sqm_gte",
        "land_area_max": "range:land_area_sqm_lte",
    }

    _CADASTRAL_FILTER_SCHEMA: Dict[str, str] = {
        "doc_type": "keyword:doc_type",
        "purpose": "keyword:purpose",
        "purpose_label": "keyword:purpose_label",
        "category": "keyword:category",
        "ownership_form": "keyword:ownership_form",
        "oblast": "keyword:oblast_name",
        "oblast_name": "keyword:oblast_name",
        "area_sqm_min": "range:area_sqm_gte",
        "area_sqm_max": "range:area_sqm_lte",
    }

    def search_listings(
        self,
        query_text: str,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not query_text or not query_text.strip():
            return []
        if not self.is_configured:
            return []
        client = self._get_client()
        if client is None:
            return []
        if not self.ensure_collections():
            return []
        try:
            vec = self._embedder.embed_text(query_text.strip())
            if vec is None:
                return []
            qfilter = self._build_filter(filters, self._LISTING_FILTER_SCHEMA)
            hits = client.search(
                collection_name=self._coll_listings,
                query_vector=vec,
                query_filter=qfilter,
                limit=max(1, min(int(top_k or 10), 100)),
                with_payload=True,
            )
            return [self._render_listing_hit(h) for h in hits]
        except Exception as e:
            logger.warning("Qdrant search_listings failed: %s", e)
            return []

    def search_parcels(
        self,
        query_text: str,
        top_k: int = 10,
        filters: Optional[Mapping[str, Any]] = None,
    ) -> List[Dict[str, Any]]:
        if not query_text or not query_text.strip():
            return []
        if not self.is_configured:
            return []
        client = self._get_client()
        if client is None:
            return []
        if not self.ensure_collections():
            return []
        try:
            vec = self._embedder.embed_text(query_text.strip())
            if vec is None:
                return []
            qfilter = self._build_filter(filters, self._CADASTRAL_FILTER_SCHEMA)
            hits = client.search(
                collection_name=self._coll_cadastral,
                query_vector=vec,
                query_filter=qfilter,
                limit=max(1, min(int(top_k or 10), 100)),
                with_payload=True,
            )
            return [self._render_cadastral_hit(h) for h in hits]
        except Exception as e:
            logger.warning("Qdrant search_parcels failed: %s", e)
            return []

    def delete_listing(self, source: str, source_id: str) -> bool:
        if not source or not source_id:
            return False
        client = self._get_client()
        if client is None:
            return False
        try:
            from qdrant_client.http import models as qmodels

            pid = _make_point_id(source, source_id)
            client.delete(
                collection_name=self._coll_listings,
                points_selector=qmodels.PointIdsList(points=[pid]),
                wait=False,
            )
            return True
        except Exception as e:
            logger.warning("Qdrant delete_listing failed: %s", e)
            return False

    @staticmethod
    def _render_listing_hit(hit: Any) -> Dict[str, Any]:
        payload = dict(getattr(hit, "payload", None) or {})
        return {
            "score": float(getattr(hit, "score", 0.0) or 0.0),
            "source": payload.get("source"),
            "source_id": payload.get("source_id"),
            "page_url": payload.get("page_url"),
            "title": payload.get("title"),
            "property_type": payload.get("property_type"),
            "status": payload.get("status"),
            "region": payload.get("region"),
            "city": payload.get("city"),
            "price_uah": payload.get("price_uah"),
            "price_usd": payload.get("price_usd"),
            "building_area_sqm": payload.get("building_area_sqm"),
            "land_area_sqm": payload.get("land_area_sqm"),
            "tags": payload.get("tags") or [],
        }

    @staticmethod
    def _render_cadastral_hit(hit: Any) -> Dict[str, Any]:
        payload = dict(getattr(hit, "payload", None) or {})
        return {
            "score": float(getattr(hit, "score", 0.0) or 0.0),
            "doc_type": payload.get("doc_type"),
            "cadastral_number": payload.get("cadastral_number"),
            "cluster_id": payload.get("cluster_id"),
            "purpose": payload.get("purpose"),
            "purpose_label": payload.get("purpose_label"),
            "category": payload.get("category"),
            "ownership_form": payload.get("ownership_form"),
            "area_sqm": payload.get("area_sqm"),
            "oblast_name": payload.get("oblast_name"),
            "raion_name": payload.get("raion_name"),
            "settlement_name": payload.get("settlement_name"),
            "parcel_count": payload.get("parcel_count"),
            "total_area_sqm": payload.get("total_area_sqm"),
            "sample_cadnums": payload.get("sample_cadnums") or [],
        }
