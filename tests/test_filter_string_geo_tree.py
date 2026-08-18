# -*- coding: utf-8 -*-
"""Тести дерева геофільтрів конструктора."""

from domain.services.filter_string_service import (
    filter_string_from_tree,
    filter_string_to_models,
    filter_string_to_tree,
    tree_to_filter_models,
)
from domain.models.filter_models import GeoFilterGroup, FilterGroupType
from business.services.settlement_criteria_resolver import (
    SETTLEMENT_POPULATION_FIELD,
    SETTLEMENT_REGION_CONTEXT_FIELD,
    strip_and_apply_settlement_criteria,
)
from domain.models.filter_models import FilterElement


def test_tree_settlement_population_geo_node():
    root = {
        "group_type": "and",
        "items": [
            {
                "type": "geo",
                "geo_type": "settlement_population",
                "operator": "inside",
                "geoRegion": "Полтавська область",
                "population_min": 50000,
                "population_max": 200000,
            }
        ],
    }
    group, geo = tree_to_filter_models(root)
    assert geo is None
    assert group is not None
    fields = [el.field for el in group.items if isinstance(el, FilterElement)]
    assert SETTLEMENT_REGION_CONTEXT_FIELD not in fields
    assert SETTLEMENT_POPULATION_FIELD in fields

    s = filter_string_from_tree(root)
    assert "контекст" not in s
    assert "Населення НП" in s or "населення" in s.lower()


def test_tree_or_settlements_same_region_string():
    """OR-група з кількома НП: geo OR geo, без плоского OR контекстних полів."""
    root = {
        "group_type": "or",
        "items": [
            {
                "type": "geo",
                "geo_type": "settlement",
                "operator": "inside",
                "geoRegion": "Волинська область",
                "value": "Іваничі",
            },
            {
                "type": "geo",
                "geo_type": "settlement",
                "operator": "inside",
                "geoRegion": "Волинська область",
                "value": "Горохів",
            },
            {
                "type": "geo",
                "geo_type": "settlement",
                "operator": "inside",
                "geoRegion": "Волинська область",
                "value": "Нововолинськ",
            },
        ],
    }
    s = filter_string_from_tree(root)
    assert "контекст" not in s
    assert " OR " in s
    assert " AND geo(" not in s.replace(" ", "")
    assert "Іваничі" in s and "Горохів" in s and "Нововолинськ" in s
    assert "REGION 'Волинська область'" in s

    parsed = filter_string_to_models(s)
    assert parsed.success
    assert isinstance(parsed.geo_filter.root, GeoFilterGroup)
    assert parsed.geo_filter.root.group_type == FilterGroupType.OR
    assert len(parsed.geo_filter.root.items) == 3


def test_tree_and_with_or_regions_and_population():
    """І (AND): АБО дві області + населення — у рядку OR між geo, не AND."""
    root = {
        "group_type": "and",
        "items": [
            {
                "type": "group",
                "group_type": "or",
                "items": [
                    {
                        "type": "geo",
                        "geo_type": "region",
                        "operator": "inside",
                        "value": "Одеська область",
                    },
                    {
                        "type": "geo",
                        "geo_type": "region",
                        "operator": "inside",
                        "value": "Волинська область",
                    },
                ],
            },
            {
                "type": "geo",
                "geo_type": "settlement_population",
                "operator": "inside",
                "population_min": 10000,
            },
        ],
    }
    s = filter_string_from_tree(root)
    assert " OR " in s
    assert "Одеська область" in s and "Волинська область" in s
    assert "Населення НП" in s or "населення" in s.lower()
    assert " AND geo('Область' INSIDE 'Одеська область') AND geo('Область' INSIDE 'Волинська" not in s

    group, geo = tree_to_filter_models(root)
    assert group is not None
    assert geo is not None
    assert isinstance(geo.root, GeoFilterGroup)
    assert geo.root.group_type == FilterGroupType.OR
    assert len(geo.root.items) == 2

    parsed = filter_string_to_models(s)
    assert parsed.success


def test_filter_string_to_tree_roundtrip_or_regions_and_population():
    original = (
        '"Населення НП, осіб" >= 10000 AND '
        "(geo('Область' INSIDE 'Одеська область') OR geo('Область' INSIDE 'Волинська область'))"
    )
    root, err = filter_string_to_tree(original)
    assert err is None
    assert root is not None
    assert " OR " in filter_string_from_tree(root)
    assert " AND geo('Область' INSIDE 'Одеська область') AND geo('Область' INSIDE 'Волинська" not in filter_string_from_tree(
        root
    )

    root2, err2 = filter_string_to_tree(
        '"Населення НП, осіб" >= 10000 AND geo(\'Область\' INSIDE \'Одеська область\')'
    )
    assert err2 is None
    items = root2.get("items") or []
    geo_types = [it.get("geo_type") for it in items if it.get("type") == "geo"]
    assert "settlement_population" in geo_types
    assert "region" in geo_types


def test_tree_settlement_name_without_population_fields():
    root = {
        "group_type": "and",
        "items": [
            {
                "type": "geo",
                "geo_type": "settlement",
                "operator": "inside",
                "geoRegion": "Вінницька область",
                "value": "Вінниця",
            }
        ],
    }
    group, geo = tree_to_filter_models(root)
    assert geo is not None
    pop_fields = [
        el
        for el in (group.items if group else [])
        if isinstance(el, FilterElement) and el.field == SETTLEMENT_POPULATION_FIELD
    ]
    assert not pop_fields
