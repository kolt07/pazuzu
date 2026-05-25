# -*- coding: utf-8 -*-
"""Гомоніми НП: різні області, одна назва."""

from unittest.mock import MagicMock, patch

from business.services.settlement_criteria_resolver import (
    SettlementCriteria,
    SettlementCriteriaResolver,
)
from domain.models.filter_models import GeoFilter, GeoFilterElement, GeoFilterOperator
from domain.managers.collection_manager import UnifiedListingsCollectionManager
from utils.settlement_normalizer import (
    build_city_filter_options,
    parse_settlement_filter_label,
)
from utils.ukraine_regions import (
    normalize_region_for_repository_lookup,
    normalize_region_to_canonical,
)


def test_normalize_region_misto_kyiv():
    assert normalize_region_to_canonical("місто Київ") == "м. Київ"
    assert normalize_region_for_repository_lookup("місто Київ") == "Київ"


def test_ivanichi_typo_key_differs_from_canonical():
    from utils.settlement_normalizer import normalize_settlement_key

    assert normalize_settlement_key("Іваничі") == "іваничі"
    assert normalize_settlement_key("Іваниничі") == "іваниничі"
    assert normalize_settlement_key("Іваничі") != normalize_settlement_key("Іваниничі")


def test_build_city_filter_options_disambiguate():
    cities = [
        {"_id": "1", "name": "Новосілки", "region_id": "r1", "region_name": "Полтавська область"},
        {"_id": "2", "name": "Новосілки", "region_id": "r2", "region_name": "Харківська область"},
    ]
    opts = build_city_filter_options(cities, disambiguate_region=True)
    assert len(opts) == 2
    labels = {o["label"] for o in opts}
    assert any("Полтавська" in lb for lb in labels)
    assert any("Харківська" in lb for lb in labels)


def test_parse_settlement_filter_label_with_region():
    name, reg = parse_settlement_filter_label("Новосілки · Полтавська")
    assert name == "Новосілки"
    assert reg == "Полтавська"


def test_geo_settlement_mongo_includes_region():
    mgr = UnifiedListingsCollectionManager.__new__(UnifiedListingsCollectionManager)
    elem = GeoFilterElement(
        operator=GeoFilterOperator.INSIDE,
        geo_type="settlement",
        value="Новосілки",
        region="Полтавська область",
    )
    mongo = mgr._geo_filter_to_mongo(GeoFilter(root=elem))
    import json

    s = json.dumps(mongo, ensure_ascii=False)
    assert "region" in s




@patch.object(SettlementCriteriaResolver, "resolve_city_names", return_value=["Новосілки"])
def test_criteria_to_geo_filter_carries_region(mock_resolve):
    resolver = SettlementCriteriaResolver()
    criteria = SettlementCriteria(region="Полтавська область", population_gte=1000)
    gf = resolver.criteria_to_geo_filter([criteria])
    assert isinstance(gf.root, GeoFilterElement)
    assert gf.root.region == "Полтавська область"
