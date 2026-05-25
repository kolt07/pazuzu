# -*- coding: utf-8 -*-
"""Пошук НП за колишніми назвами (search_aliases)."""

from unittest.mock import MagicMock, patch

from business.services.settlement_matching_service import SettlementMatchingService
from data.repositories.geography_repository import CitiesRepository


def test_find_by_alias_in_repository():
    repo = CitiesRepository()
    repo.find_one = MagicMock(
        return_value={
            "_id": "abc",
            "name": "Кропивницький",
            "name_normalized": "кропивницький",
            "region_id": "reg1",
            "search_aliases": ["кіровоград", "єлизаветград"],
        }
    )
    repo.find_fuzzy_by_name_and_region = MagicMock(return_value=None)

    with patch.object(repo, "_active_city_filter", return_value={}):
        with patch.object(repo, "_normalize_name", return_value="кіровоград"):
            found = repo.find_by_name_and_region("Кіровоград", "reg1")

    assert found is not None
    assert found["name"] == "Кропивницький"


def test_matching_service_alias_match():
    svc = SettlementMatchingService()
    candidates = [
        {
            "name": "Кропивницький",
            "name_normalized": "кропивницький",
            "region_id": "r1",
            "search_aliases": ["кіровоград"],
        }
    ]
    m = svc.find_best_match("Кіровоград", "r1", candidates)
    assert m is not None
    assert m.match_type == "alias"
    assert m.city["name"] == "Кропивницький"
