# -*- coding: utf-8 -*-
"""Тести FilterSpec / фасетів."""

from domain.services.filter_spec_service import (
    empty_filter_spec,
    facets_to_filter_spec,
    filter_spec_to_facet_state,
    filter_spec_to_models,
    filter_string_to_filter_spec,
    normalize_filter_spec,
)


def test_facets_to_filter_spec_basic():
    facets = {
        "status": ["активне"],
        "source": ["olx", "prozorro"],
        "property_type": [],
        "price_uah": {"min": 100000, "max": 500000},
        "building_area_sqm": {"min": None, "max": None},
        "land_area_sotky": {"min": None, "max": None},
        "source_updated_at": {"from": None, "to": None},
        "title_contains": "",
        "description_contains": "",
        "geo": {
            "regions": [{"id": "rid1", "name": "Київська область"}],
            "settlements": [],
            "city_districts": [],
            "settlement_population": {"min": None, "max": None},
            "settlement_area_sq_km": {"min": None, "max": None},
        },
    }
    spec = facets_to_filter_spec(facets)
    assert spec["version"] == 1
    assert spec["group_type"] == "and"
    fields = [it.get("field") for it in spec["items"] if it.get("type") == "element"]
    assert "status" in fields
    assert "source" in fields
    assert "price_uah" in fields
    geo_items = [it for it in spec["items"] if it.get("type") == "geo"]
    assert any(g.get("geo_type") == "region" and g.get("region_id") == "rid1" for g in geo_items)


def test_filter_spec_roundtrip_facets():
    facets = {
        "status": ["активне"],
        "source": ["olx"],
        "property_type": ["Земельна ділянка"],
        "price_uah": {"min": 10, "max": 20},
        "building_area_sqm": {"min": None, "max": None},
        "land_area_sotky": {"min": None, "max": None},
        "source_updated_at": {"from": None, "to": None},
        "title_contains": "склад",
        "description_contains": "",
        "geo": {
            "regions": [],
            "settlements": [
                {
                    "city_id": "cid1",
                    "name": "Вінниця",
                    "region": "Вінницька область",
                    "region_id": "rid2",
                }
            ],
            "city_districts": [],
            "settlement_population": {"min": None, "max": None},
            "settlement_area_sq_km": {"min": None, "max": None},
        },
    }
    spec = facets_to_filter_spec(facets)
    back, simple = filter_spec_to_facet_state(spec)
    assert simple is True
    assert back is not None
    assert "olx" in back["source"]
    assert back["title_contains"] == "склад"
    assert back["geo"]["settlements"][0]["city_id"] == "cid1"


def test_facets_multi_settlements_and_price_usd():
    facets = {
        "status": [],
        "source": [],
        "property_type": [],
        "price_currency": "usd",
        "price_uah": {"min": None, "max": None},
        "price_usd": {"min": 50000, "max": 120000},
        "building_area_sqm": {"min": None, "max": None},
        "land_area_sotky": {"min": None, "max": None},
        "source_updated_at": {"from": None, "to": None},
        "title_contains": "",
        "description_contains": "",
        "geo": {
            "regions": [],
            "settlements": [
                {"city_id": "c1", "name": "Київ", "region": "м. Київ"},
                {"city_id": "c2", "name": "Бровари", "region": "Київська область"},
            ],
            "city_districts": [],
            "settlement_population": {"min": None, "max": None},
            "settlement_area_sq_km": {"min": None, "max": None},
        },
    }
    spec = facets_to_filter_spec(facets)
    fields = [it.get("field") for it in spec["items"] if it.get("type") == "element"]
    assert "price_usd" in fields
    assert "price_uah" not in fields
    or_groups = [
        it for it in spec["items"]
        if it.get("type") == "group" and it.get("group_type") == "or"
    ]
    assert len(or_groups) == 1
    assert len(or_groups[0]["items"]) == 2
    back, simple = filter_spec_to_facet_state(spec)
    assert simple is True
    assert back["price_currency"] == "usd"
    assert back["price_usd"]["min"] == 50000
    assert len(back["geo"]["settlements"]) == 2


def test_filter_string_to_filter_spec_migration():
    s = '"Джерело" = olx AND "Активність" = \'активне\''
    spec, err = filter_string_to_filter_spec(s)
    assert err is None
    assert spec is not None

    def walk(items):
        for it in items or []:
            yield it
            if it.get("type") == "group":
                yield from walk(it.get("items"))

    fields = [it.get("field") for it in walk(spec["items"]) if it.get("type") == "element"]
    assert "source" in fields
    assert "status" in fields
    # після flatten — елементи на корені
    assert any(it.get("type") == "element" and it.get("field") == "source" for it in spec["items"])


def test_filter_spec_to_models():
    spec = normalize_filter_spec({
        "version": 1,
        "group_type": "and",
        "items": [
            {"type": "element", "field": "source", "operator": "eq", "value": "olx"},
            {
                "type": "geo",
                "geo_type": "settlement",
                "operator": "inside",
                "value": "Одеса",
                "city_id": "abc",
                "region_id": "def",
                "geoRegion": "Одеська область",
            },
        ],
    })
    group, geo, err = filter_spec_to_models(spec)
    assert err is None
    assert group is not None
    assert geo is not None
    assert getattr(geo.root, "city_id", None) == "abc"
    assert getattr(geo.root, "region_id", None) == "def"


def test_empty_spec():
    spec = empty_filter_spec()
    group, geo, err = filter_spec_to_models(spec)
    assert err is None
    assert group is None
    assert geo is None


def test_complex_or_not_simple_facets():
    spec = {
        "version": 1,
        "group_type": "or",
        "items": [
            {"type": "element", "field": "source", "operator": "eq", "value": "olx"},
            {"type": "element", "field": "source", "operator": "eq", "value": "prozorro"},
        ],
    }
    facets, simple = filter_spec_to_facet_state(spec)
    assert simple is False
    assert facets is None
