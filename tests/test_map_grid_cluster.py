# -*- coding: utf-8 -*-
"""Тести grid-кластеризації маркерів мапи."""

from utils.map_grid_cluster import cluster_markers


def _mk(lat, lng, lid):
    return {
        "lat": lat,
        "lng": lng,
        "listing_id": lid,
        "source": "olx",
        "source_id": lid,
        "label": "OLX",
    }


def test_cluster_reduces_many_points():
    markers = [_mk(50.0 + i * 0.001, 25.0 + i * 0.001, str(i)) for i in range(200)]
    points = cluster_markers(markers, zoom=8, max_points=80)
    assert len(points) <= 80
    assert any(p["type"] == "cluster" for p in points)


def test_high_zoom_keeps_singletons():
    markers = [_mk(50.0, 25.0, "a"), _mk(50.01, 25.01, "b")]
    points = cluster_markers(markers, zoom=16, max_points=80)
    assert len(points) == 2
    assert all(p["type"] == "marker" for p in points)
