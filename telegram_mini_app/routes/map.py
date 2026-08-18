# -*- coding: utf-8 -*-
"""
API вкладки «Мапа»: конфіг Google Maps та оголошення.

Режими:
- filter — усі маркери за фільтром (з геокодуванням), bounds для fitBounds, кластеризація на сервері;
- viewport — швидко: координати/кадастр у bbox, без Geocoding, кластеризація на сервері.
"""

import logging
import time
from typing import Any, Dict, List, Literal, Optional, Set

from fastapi import APIRouter, HTTPException, Request
from pydantic import BaseModel, Field

from business.services.listing_map_placement_service import ListingMapPlacementService
from config.settings import Settings
from telegram_mini_app.routes.search import (
    _get_validated_user,
    _normalize_unified_doc,
    _sanitize_json_floats,
)
from utils.map_grid_cluster import cluster_markers

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/api/map", tags=["map"])

MAP_QUERY_MAX_LIMIT = 800
MAP_FILTER_DEFAULT_LIMIT = 500
MAP_VIEWPORT_DEFAULT_LIMIT = 200
MAP_MAX_MAP_POINTS = 120
MAP_SINGLE_MARKER_ZOOM = 14


class MapBbox(BaseModel):
    sw_lat: float
    sw_lng: float
    ne_lat: float
    ne_lng: float


class MapQueryRequest(BaseModel):
    filter: Optional[Dict[str, Any]] = None
    filter_string: str = ""  # legacy
    mode: Literal["filter", "viewport"] = "viewport"
    bbox: Optional[MapBbox] = None
    zoom: int = Field(default=8, ge=1, le=21)
    sort_field: str = "source_updated_at"
    sort_order: str = "desc"
    limit: Optional[int] = None


def _doc_listing_id(doc: Dict[str, Any]) -> str:
    lid = doc.get("_id")
    if lid is not None:
        return str(lid)
    return f"{doc.get('source', '')}:{doc.get('source_id', '')}"


def _bbox_dict(bbox: MapBbox) -> Dict[str, float]:
    return {
        "sw_lat": bbox.sw_lat,
        "sw_lng": bbox.sw_lng,
        "ne_lat": bbox.ne_lat,
        "ne_lng": bbox.ne_lng,
    }


def _bounds_from_markers(markers: List[Dict[str, Any]]) -> Optional[Dict[str, float]]:
    if not markers:
        return None
    lats = [float(m["lat"]) for m in markers]
    lngs = [float(m["lng"]) for m in markers]
    pad_lat = 0.02 if len(lats) == 1 else 0.0
    pad_lng = 0.02 if len(lngs) == 1 else 0.0
    return {
        "sw_lat": min(lats) - pad_lat,
        "ne_lat": max(lats) + pad_lat,
        "sw_lng": min(lngs) - pad_lng,
        "ne_lng": max(lngs) + pad_lng,
    }


def _collect_markers(
    data: List[Dict[str, Any]],
    placement_svc: ListingMapPlacementService,
    *,
    bbox: Optional[Dict[str, float]] = None,
    allow_geocode: bool = True,
) -> tuple[List[Dict[str, Any]], Set[str], int]:
    """Повертає (raw_markers, listing_ids з координатами, skipped_no_coords)."""
    markers_out: List[Dict[str, Any]] = []
    listing_ids: Set[str] = set()
    skipped = 0

    for doc in data or []:
        listing_id = _doc_listing_id(doc)
        if bbox is not None:
            markers = placement_svc.resolve_markers_in_bbox(
                doc, bbox, allow_geocode=allow_geocode
            )
        else:
            markers = placement_svc.resolve_markers(doc, allow_geocode=allow_geocode)

        if not markers:
            skipped += 1
            continue

        listing_ids.add(listing_id)
        source = (doc.get("source") or "").strip().lower()
        source_id = doc.get("source_id")
        for m in markers:
            markers_out.append({
                "listing_id": listing_id,
                "source": source,
                "source_id": source_id,
                "lat": m["lat"],
                "lng": m["lng"],
                "label": m["label"],
                "placement": m["placement"],
                "cadastral_number": m.get("cadastral_number"),
            })

    return markers_out, listing_ids, skipped


def _build_map_points(
    raw_markers: List[Dict[str, Any]],
    zoom: int,
    bbox: Optional[Dict[str, float]],
) -> List[Dict[str, Any]]:
    if zoom >= MAP_SINGLE_MARKER_ZOOM:
        return [
            {
                "type": "marker",
                "lat": m["lat"],
                "lng": m["lng"],
                "listing_id": m.get("listing_id"),
                "source": m.get("source"),
                "source_id": m.get("source_id"),
                "label": m.get("label"),
                "placement": m.get("placement"),
                "cadastral_number": m.get("cadastral_number"),
            }
            for m in raw_markers[:MAP_MAX_MAP_POINTS * 3]
        ]
    return cluster_markers(
        raw_markers,
        zoom,
        bbox=bbox,
        max_points=MAP_MAX_MAP_POINTS,
    )


def _attach_price_indicators(items: List[Dict[str, Any]]) -> None:
    try:
        from business.services.price_analytics_service import PriceAnalyticsService

        analytics = PriceAnalyticsService()
        indicators = analytics.get_price_indicators_for_items(items)
        for item in items:
            cid = f"{item.get('source', '')}:{item.get('source_id', '')}"
            if cid in indicators:
                ind = indicators[cid]
                item["price_indicator"] = ind.get("indicator")
                item["price_indicator_source"] = ind.get("source_level")
    except Exception:
        pass


@router.get("/config")
def map_config(request: Request):
    """Повертає ключ Maps JavaScript API для клієнта (якщо налаштовано)."""
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    settings: Settings = getattr(request.app.state, "settings", None) or Settings()
    key = (settings.google_maps_api_key or "").strip()
    return {"maps_api_key": key if key else None}


@router.post("/query")
def map_query(request: Request, body: MapQueryRequest):
    """
    filter — кластери/маркери за фільтром + bounds для fitBounds.
    viewport — кластери/маркери у bbox (без геокодування).
    """
    t0 = time.perf_counter()
    user_id, user_service = _get_validated_user(request)
    if not user_service.is_user_authorized(user_id):
        raise HTTPException(status_code=403, detail="User not authorized")

    from domain.services.unified_search_service import find_by_filter_spec, find_by_filter_string
    from domain.services.filter_spec_service import normalize_filter_spec

    mode = (body.mode or "viewport").strip().lower()
    is_filter_mode = mode == "filter"
    zoom = int(body.zoom or 8)

    if is_filter_mode:
        limit = min(body.limit or MAP_FILTER_DEFAULT_LIMIT, MAP_QUERY_MAX_LIMIT)
        bbox = None
        bbox_dict = None
        allow_geocode = True
    else:
        if body.bbox is None:
            raise HTTPException(status_code=400, detail="bbox is required for viewport mode")
        limit = min(body.limit or MAP_VIEWPORT_DEFAULT_LIMIT, MAP_QUERY_MAX_LIMIT)
        bbox_dict = _bbox_dict(body.bbox)
        bbox = bbox_dict
        allow_geocode = False

    sort = [{"field": body.sort_field, "order": -1 if body.sort_order == "desc" else 1}]

    if body.filter is not None:
        data, total_matched, err = find_by_filter_spec(
            filter_spec=normalize_filter_spec(body.filter),
            sort=sort,
            limit=limit,
            skip=0,
        )
    else:
        data, total_matched, err = find_by_filter_string(
            filter_string=body.filter_string or "",
            sort=sort,
            limit=limit,
            skip=0,
        )
    if err is not None:
        raise HTTPException(status_code=400, detail=err)

    settings: Settings = getattr(request.app.state, "settings", None) or Settings()
    placement_svc = ListingMapPlacementService(settings)

    raw_markers, listing_ids, skipped_no_coords = _collect_markers(
        data or [],
        placement_svc,
        bbox=bbox,
        allow_geocode=allow_geocode,
    )

    map_points = _build_map_points(raw_markers, zoom, bbox_dict)

    docs_with_markers = [
        d for d in (data or [])
        if _doc_listing_id(d) in listing_ids
    ]
    items = [_normalize_unified_doc(doc) for doc in docs_with_markers]

    if is_filter_mode:
        _attach_price_indicators(items)

    elapsed_ms = int((time.perf_counter() - t0) * 1000)
    logger.info(
        "map/query mode=%s zoom=%s docs=%s raw_markers=%s map_points=%s ms=%s",
        mode,
        zoom,
        len(data or []),
        len(raw_markers),
        len(map_points),
        elapsed_ms,
    )

    result: Dict[str, Any] = {
        "mode": mode,
        "zoom": zoom,
        "items": items,
        "map_points": map_points,
        "markers": raw_markers,
        "total_matched": total_matched if total_matched is not None else len(data or []),
        "total_in_viewport": len(listing_ids),
        "total_raw_markers": len(raw_markers),
        "skipped_no_coords": skipped_no_coords,
    }
    if is_filter_mode:
        result["bounds"] = _bounds_from_markers(raw_markers)

    return _sanitize_json_floats(result)
