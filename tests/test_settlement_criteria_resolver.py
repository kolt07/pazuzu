# -*- coding: utf-8 -*-
"""Тести SettlementCriteriaResolver."""

from unittest.mock import MagicMock, patch

from business.services.settlement_criteria_resolver import (
    SettlementCriteria,
    SettlementCriteriaResolver,
    extract_settlement_criteria_from_group,
    strip_and_apply_settlement_criteria,
)
from domain.models.filter_models import (
    FilterElement,
    FilterGroup,
    FilterGroupType,
    FilterOperator,
    GeoFilter,
    GeoFilterElement,
    GeoFilterOperator,
)


def test_extract_population_criteria():
    group = FilterGroup(
        group_type=FilterGroupType.AND,
        items=[
            FilterElement(
                field="settlement_region_context",
                operator=FilterOperator.EQ,
                value="Кіровоградська область",
            ),
            FilterElement(
                field="settlement_population",
                operator=FilterOperator.GTE,
                value=100000,
            ),
        ],
    )
    new_group, criteria = extract_settlement_criteria_from_group(group)
    assert new_group is None or not new_group.items
    assert len(criteria) == 1
    assert criteria[0].population_gte == 100000
    assert criteria[0].region == "Кіровоградська область"


@patch.object(SettlementCriteriaResolver, "resolve_city_names", return_value=["Кропивницький", "Кременчук"])
def test_criteria_to_geo_filter_or(mock_resolve):
    resolver = SettlementCriteriaResolver()
    criteria = SettlementCriteria(region="Полтавська область", population_gte=200000)
    gf = resolver.criteria_to_geo_filter([criteria])
    assert gf is not None
    root = gf.root
    assert hasattr(root, "items")
    assert len(root.items) == 2
    assert all(getattr(it, "region", None) == "Полтавська область" for it in root.items)


def test_no_match_empty_cities():
    resolver = SettlementCriteriaResolver()
    with patch.object(resolver, "resolve_city_names", return_value=[]):
        gf = resolver.criteria_to_geo_filter([SettlementCriteria(population_gte=999999999)])
    assert gf.root.value == "__NO_MATCH__"


def test_strip_and_apply_with_geo_region():
    group = FilterGroup(
        group_type=FilterGroupType.AND,
        items=[
            FilterElement(field="settlement_population", operator=FilterOperator.GTE, value=50000),
        ],
    )
    geo = GeoFilter(
        root=GeoFilterElement(
            operator=GeoFilterOperator.INSIDE,
            geo_type="region",
            value="Полтавська область",
        )
    )
    with patch.object(SettlementCriteriaResolver, "resolve_city_names", return_value=["Полтава"]):
        new_group, new_geo = strip_and_apply_settlement_criteria(group, geo)
    assert new_group is None or not new_group.items
