# -*- coding: utf-8 -*-
"""Тести ListingMapPlacementService — пріоритет розміщення маркерів."""

from unittest.mock import MagicMock

import pytest

from business.services.listing_map_placement_service import ListingMapPlacementService


@pytest.fixture
def placement_svc():
    svc = ListingMapPlacementService(settings=MagicMock(google_maps_api_key=""))
    svc._cadastral_repo = MagicMock()
    svc._cities_repo = MagicMock()
    svc.geocoding = MagicMock()
    return svc


def test_complete_address_takes_priority_over_cadastral(placement_svc):
    doc = {
        "source": "olx",
        "cadastral_numbers": ["6320685503:03:000:0202"],
        "addresses": [{
            "is_complete": True,
            "coordinates": {"latitude": 50.45, "longitude": 30.52},
        }],
    }
    markers = placement_svc.resolve_markers(doc)
    assert len(markers) == 1
    assert markers[0]["placement"] == "address"
    assert markers[0]["label"] == "OLX"
    assert markers[0]["lat"] == pytest.approx(50.45)
    placement_svc._cadastral_repo.find_many_by_cadastral_numbers.assert_not_called()


def test_cadastral_markers_one_per_parcel(placement_svc):
    doc = {
        "source": "prozorro",
        "cadastral_numbers": ["111:01:001", "111:01:002"],
        "addresses": [],
    }
    placement_svc._cadastral_repo.find_many_by_cadastral_numbers.return_value = [
        {
            "cadastral_number": "111:01:001",
            "bounds": {
                "type": "Polygon",
                "coordinates": [[[30.0, 50.0], [30.1, 50.0], [30.1, 50.1], [30.0, 50.1], [30.0, 50.0]]],
            },
        },
        {
            "cadastral_number": "111:01:002",
            "bounds": {
                "type": "Polygon",
                "coordinates": [[[31.0, 49.0], [31.2, 49.0], [31.2, 49.2], [31.0, 49.2], [31.0, 49.0]]],
            },
        },
    ]
    markers = placement_svc.resolve_markers(doc)
    assert len(markers) == 2
    assert all(m["placement"] == "cadastral" for m in markers)
    assert markers[0]["label"] == "Przr"


def test_settlement_from_cities_repository(placement_svc):
    doc = {
        "source": "olx",
        "region": "Київська",
        "city": "Бориспіль",
        "addresses": [{"settlement": "Бориспіль", "region": "Київська"}],
    }
    placement_svc._cities_repo.find_coordinates.return_value = {"lat": 50.35, "lon": 30.95}
    markers = placement_svc.resolve_markers(doc)
    assert len(markers) == 1
    assert markers[0]["placement"] == "settlement"
    placement_svc.geocoding.geocode.assert_not_called()


def test_viewport_mode_skips_geocode(placement_svc):
    doc = {
        "source": "olx",
        "region": "Київська",
        "city": "Бориспіль",
        "addresses": [{"settlement": "Бориспіль", "region": "Київська", "street": "Центральна"}],
    }
    placement_svc._cities_repo.find_coordinates.return_value = None
    assert placement_svc.resolve_markers(doc, allow_geocode=False) == []
    placement_svc.geocoding.geocode.assert_not_called()


def test_street_geocode_when_street_without_building(placement_svc):
    doc = {
        "source": "olx",
        "region": "Київська",
        "city": "Київ",
        "addresses": [{
            "region": "Київська",
            "settlement": "Київ",
            "street": "Хрещатик",
            "building": None,
        }],
    }
    placement_svc._cities_repo.find_coordinates.return_value = None
    placement_svc.geocoding.geocode.return_value = {
        "results": [{"latitude": 50.45, "longitude": 30.52}],
    }
    markers = placement_svc.resolve_markers(doc)
    assert len(markers) == 1
    assert markers[0]["placement"] == "street"
    placement_svc.geocoding.geocode.assert_called_once()


def test_resolve_markers_in_bbox(placement_svc):
    doc = {
        "source": "olx",
        "addresses": [{
            "is_complete": True,
            "coordinates": {"latitude": 50.0, "longitude": 30.0},
        }],
    }
    bbox = {"sw_lat": 49.0, "sw_lng": 29.0, "ne_lat": 51.0, "ne_lng": 31.0}
    assert len(placement_svc.resolve_markers_in_bbox(doc, bbox)) == 1
    bbox_out = {"sw_lat": 55.0, "sw_lng": 35.0, "ne_lat": 56.0, "ne_lng": 36.0}
    assert len(placement_svc.resolve_markers_in_bbox(doc, bbox_out)) == 0
