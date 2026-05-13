# -*- coding: utf-8 -*-
"""
Межі топоніма для кадастрового полігон-пошуку: Geocoding → Place Details (New) → GeoJSON Polygon.

Google зазвичай повертає viewport (прямокутник), не точний полігон адмінодиниць.
boundary_kind завжди вказує на viewport_rectangle, коли полігон зібраний з viewport/bounds.
"""

from __future__ import annotations

import logging
from typing import Any, Dict, List, Optional

from shapely.geometry import Polygon, mapping
from shapely.geometry.polygon import orient

from business.services.geocoding_service import GeocodingService, DEFAULT_REGION
from business.services.places_service import PlacesService

logger = logging.getLogger(__name__)

# Пріоритет типів Geocoding для вибору «найкращого» кандидата під адмінодиницю
_PREFERRED_GEOCODE_TYPES = frozenset({
    "locality",
    "sublocality",
    "sublocality_level_1",
    "administrative_area_level_3",
    "administrative_area_level_2",
    "administrative_area_level_1",
    "neighborhood",
})


def viewport_latlng_to_geojson_polygon(viewport_ll: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """
    viewport_ll: { southwest: {latitude, longitude}, northeast: {latitude, longitude} }
    Повертає GeoJSON Polygon з коректною орієнтацією (CCW) для 2dsphere.
    """
    if not isinstance(viewport_ll, dict):
        return None
    sw = viewport_ll.get("southwest") or {}
    ne = viewport_ll.get("northeast") or {}
    try:
        sw_lng = float(sw.get("longitude"))
        sw_lat = float(sw.get("latitude"))
        ne_lng = float(ne.get("longitude"))
        ne_lat = float(ne.get("latitude"))
    except (TypeError, ValueError):
        return None
    if sw_lng == ne_lng or sw_lat == ne_lat:
        return None
    # Прямокутник: SW → SE → NE → NW → SW (lon, lat)
    ring = [
        (sw_lng, sw_lat),
        (ne_lng, sw_lat),
        (ne_lng, ne_lat),
        (sw_lng, ne_lat),
        (sw_lng, sw_lat),
    ]
    try:
        poly = Polygon(ring)
        if not poly.is_valid:
            poly = poly.buffer(0)
        poly = orient(poly, sign=1.0)
        geom = mapping(poly)
        if geom.get("type") != "Polygon":
            return None
        return geom  # type: ignore[return-value]
    except Exception as e:
        logger.debug("viewport_latlng_to_geojson_polygon: %s", e)
        return None


def _score_geocode_candidate(res: Dict[str, Any]) -> int:
    types = set(res.get("types") or [])
    score = 0
    for t in types:
        if t in _PREFERRED_GEOCODE_TYPES:
            score += 10
    if res.get("place_id"):
        score += 5
    lt = (res.get("location_type") or "").upper()
    if lt in ("ROOFTOP", "RANGE_INTERPOLATED"):
        score += 2
    return score


def _pick_geocode_result(results: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    if not results:
        return None
    best = max(results, key=_score_geocode_candidate)
    return best


class CadastralToponymBoundaryService:
    """Геометрія меж для запиту «ділянки в межах топоніма»."""

    def __init__(
        self,
        geocoding: Optional[GeocodingService] = None,
        places: Optional[PlacesService] = None,
    ) -> None:
        self._geocoding = geocoding or GeocodingService()
        self._places = places or PlacesService()

    def resolve_boundary(
        self,
        toponym: str,
        *,
        region: str = DEFAULT_REGION,
        caller: str = "cadastral_toponym_boundary",
    ) -> Dict[str, Any]:
        """
        Повертає:
            ok, boundary_kind, polygon (GeoJSON), formatted_address, place_id,
            geocode_preview, error
        """
        text = (toponym or "").strip()
        if not text:
            return {"ok": False, "error": "toponym порожній"}

        geo = self._geocoding.geocode(text, region=region, caller=caller)
        results = geo.get("results") or []
        cand = _pick_geocode_result(results)
        if not cand:
            return {
                "ok": False,
                "error": "geocode_zero_results",
                "query_text": text,
            }

        viewport_ll: Optional[Dict[str, Any]] = None
        formatted_address = cand.get("formatted_address") or ""
        place_id = (cand.get("place_id") or "").strip()
        boundary_kind = "viewport_rectangle"

        if place_id:
            det = self._places.get_place_details(place_id)
            if det.get("success") and det.get("place"):
                pl = det["place"]
                viewport_ll = pl.get("viewport_latlng")
                if pl.get("formatted_address"):
                    formatted_address = pl["formatted_address"]

        if not viewport_ll:
            viewport_ll = cand.get("viewport_latlng")
        if not viewport_ll:
            viewport_ll = cand.get("bounds_latlng")

        polygon = viewport_latlng_to_geojson_polygon(viewport_ll) if viewport_ll else None
        if not polygon:
            return {
                "ok": False,
                "error": "no_viewport_geometry",
                "query_text": text,
                "formatted_address": formatted_address,
                "place_id": place_id or None,
            }

        return {
            "ok": True,
            "boundary_kind": boundary_kind,
            "polygon": polygon,
            "formatted_address": formatted_address,
            "place_id": place_id or None,
            "geocode_types": cand.get("types") or [],
            "location_type": cand.get("location_type"),
        }
