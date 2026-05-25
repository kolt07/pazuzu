# -*- coding: utf-8 -*-
"""Тести дерева геофільтрів конструктора."""

from domain.services.filter_string_service import (
    filter_string_from_tree,
    filter_string_to_models,
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
    assert SETTLEMENT_REGION_CONTEXT_FIELD in fields
    assert SETTLEMENT_POPULATION_FIELD in fields


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
