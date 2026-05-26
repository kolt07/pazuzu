# -*- coding: utf-8 -*-
"""Префіксний пошук НП: и/і, typo-н (іваниничі)."""

import re
from unittest.mock import MagicMock, patch

from utils.settlement_normalizer import (
    normalize_settlement_key,
    settlement_name_from_mista_url,
    settlement_prefix_regex_pattern,
    settlement_search_or_clauses,
    settlement_typo_n_variant,
)


def test_normalize_key_keeps_ukrainian_yi_in_kyiv():
    key = normalize_settlement_key("\u041a\u0438\u0457\u0432")
    assert "\u0438" in key
    assert key == "\u043a\u0438\u0457\u0432"


def test_settlement_name_from_mista_url():
    url = "https://mista.ua/%D0%A3%D0%BA%D1%80%D0%B0%D1%97%D0%BD%D0%B0/%D0%9D%D0%BE%D0%B2%D0%BE%D0%B2%D0%BE%D0%BB%D0%B8%D0%BD%D1%81%D1%8C%D0%BA_%D0%BE%D0%B1%D0%BB%D0%B0%D1%81%D1%82%D1%8C/%D0%9D%D0%BE%D0%B2%D0%BE%D0%B2%D0%BE%D0%BB%D0%B8%D0%BD%D1%81%D1%8C%D0%BA"
    assert settlement_name_from_mista_url(url) == "Нововолинськ"


def test_ivanichi_typo_n_variant():
    assert settlement_typo_n_variant("іваничі") == "іваниничі"
    assert settlement_typo_n_variant("любешів") is None


def test_ivanichi_yi_variants_share_regex_prefix():
    pattern = settlement_prefix_regex_pattern(normalize_settlement_key("\u0406\u0432\u0430\u043d\u0438\u0447\u0456"))
    assert re.match(pattern, "\u0456\u0432\u0430\u043d\u0438\u0447\u0438")
    assert re.match(pattern, "\u0456\u0432\u0430\u043d\u0438\u0447\u0456")


def test_search_or_clauses_includes_typo_exact_match():
    clauses = settlement_search_or_clauses("іваничі")
    assert {"name_normalized": "іваниничі"} in clauses
    assert {"search_aliases": "іваничі"} in clauses


@patch("data.repositories.geography_repository.CitiesRepository.find_many")
def test_search_by_name_prefix_finds_typo_record(mock_find_many):
    from data.repositories.geography_repository import CitiesRepository

    mock_find_many.return_value = [
        {
            "_id": "r1",
            "name": "Іваниничі",
            "region_id": "rivne",
            "population": 232,
        }
    ]
    repo = CitiesRepository()
    repo._indexes_created = True
    repo.search_by_name_prefix("\u0406\u0432\u0430\u043d\u0438\u0447\u0456", limit=10)
    assert mock_find_many.called
    filt = mock_find_many.call_args.kwargs.get("filter") or mock_find_many.call_args[1].get("filter")
    or_part = filt.get("$and", [{}])[0]
    if "$or" in str(filt):
        or_clauses = filt.get("$or") or next(
            (c.get("$or") for c in filt.get("$and", []) if isinstance(c, dict) and "$or" in c),
            [],
        )
    else:
        or_clauses = filt.get("$or", [])
    assert {"name_normalized": "іваниничі"} in or_clauses
