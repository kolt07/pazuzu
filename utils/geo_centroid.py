# -*- coding: utf-8 -*-
"""Обчислення центру полігону GeoJSON (WGS84, кільця [lon, lat])."""

from typing import Any, Dict, Mapping, Optional, Tuple


def centroid_from_bounds(bounds: Any) -> Optional[Tuple[float, float]]:
    """
    Повертає (latitude, longitude) центру полігона з GeoJSON bounds.

    bounds: GeoJSON Polygon з coordinates[0] — кільце [lon, lat].
    """
    if not isinstance(bounds, Mapping):
        return None
    coords = bounds.get("coordinates")
    if not isinstance(coords, list) or not coords:
        return None
    ring = coords[0] if coords else None
    if not isinstance(ring, list) or not ring:
        return None
    try:
        xs = [float(p[0]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
        ys = [float(p[1]) for p in ring if isinstance(p, (list, tuple)) and len(p) >= 2]
        if not xs or not ys:
            return None
        return (sum(ys) / len(ys), sum(xs) / len(xs))
    except (TypeError, ValueError):
        return None


def parse_lat_lng(
    coords: Any,
) -> Optional[Tuple[float, float]]:
    """Парсить {latitude, longitude} або {lat, lon} у (lat, lng)."""
    if not isinstance(coords, Mapping):
        return None
    lat = coords.get("latitude")
    if lat is None:
        lat = coords.get("lat")
    lng = coords.get("longitude")
    if lng is None:
        lng = coords.get("lon")
    if lng is None:
        lng = coords.get("lng")
    if lat is None or lng is None:
        return None
    try:
        return (float(lat), float(lng))
    except (TypeError, ValueError):
        return None
