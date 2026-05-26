# -*- coding: utf-8 -*-
"""
Grid-кластеризація маркерів для mini-app «Мапа».

Повертає обмежену кількість точек (кластери + одиночні маркери) замість сотень raw markers.
"""

from __future__ import annotations

import math
from collections import defaultdict
from typing import Any, Dict, List, Optional, Tuple

MapPoint = Dict[str, Any]


def _cell_degrees_for_zoom(zoom: int) -> float:
    """Розмір комірки сітки в градусах (приблизно як у Google Maps clusterer)."""
    z = max(1, min(21, int(zoom)))
    # zoom 6 ~ 0.5°, zoom 10 ~ 0.05°, zoom 14 ~ 0.003°
    return 360.0 / (256 * (2 ** z)) * 64


def cluster_markers(
    markers: List[Dict[str, Any]],
    zoom: int,
    *,
    bbox: Optional[Dict[str, float]] = None,
    max_points: int = 120,
) -> List[MapPoint]:
    """
    Групує маркери в кластери за сіткою. На виході — не більше max_points елементів.

    Вхідний marker: lat, lng, listing_id, source, source_id, label, placement, ...
    """
    if not markers:
        return []

    cell = _cell_degrees_for_zoom(zoom)
    if cell <= 0:
        cell = 0.05

    sw_lat = bbox.get("sw_lat") if bbox else None
    sw_lng = bbox.get("sw_lng") if bbox else None

    buckets: Dict[Tuple[int, int], List[Dict[str, Any]]] = defaultdict(list)

    for m in markers:
        lat = float(m["lat"])
        lng = float(m["lng"])
        if sw_lat is not None and sw_lng is not None:
            ci = int(math.floor((lat - sw_lat) / cell))
            cj = int(math.floor((lng - sw_lng) / cell))
        else:
            ci = int(math.floor(lat / cell))
            cj = int(math.floor(lng / cell))
        buckets[(ci, cj)].append(m)

    points: List[MapPoint] = []

    for _key, group in buckets.items():
        if len(group) == 1:
            m = group[0]
            points.append({
                "type": "marker",
                "lat": m["lat"],
                "lng": m["lng"],
                "listing_id": m.get("listing_id"),
                "source": m.get("source"),
                "source_id": m.get("source_id"),
                "label": m.get("label"),
                "placement": m.get("placement"),
                "cadastral_number": m.get("cadastral_number"),
            })
        else:
            lats = [float(x["lat"]) for x in group]
            lngs = [float(x["lng"]) for x in group]
            listing_ids = []
            seen = set()
            olx_n = 0
            prz_n = 0
            for x in group:
                lid = x.get("listing_id")
                if lid and lid not in seen:
                    seen.add(lid)
                    listing_ids.append(lid)
                src = (x.get("source") or "").lower()
                if src == "olx":
                    olx_n += 1
                else:
                    prz_n += 1
            dominant = "olx" if olx_n >= prz_n else "prozorro"
            points.append({
                "type": "cluster",
                "lat": sum(lats) / len(lats),
                "lng": sum(lngs) / len(lngs),
                "count": len(group),
                "listing_ids": listing_ids,
                "listing_count": len(listing_ids),
                "label": "OLX" if dominant == "olx" else "Przr",
                "source": dominant,
                "olx_count": olx_n,
                "prozorro_count": prz_n,
            })

    if len(points) <= max_points:
        return points

    # Занадто багато — зливаємо найближчі кластери в мета-кластери (грубо: збільшуємо cell)
    return cluster_markers(markers, max(1, zoom - 2), bbox=bbox, max_points=max_points)
