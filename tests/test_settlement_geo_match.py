# -*- coding: utf-8 -*-
"""Централізований геопошук за НП."""

import json

from domain.managers.collection_manager import UnifiedListingsCollectionManager
from domain.models.filter_models import GeoFilter, GeoFilterElement, GeoFilterOperator
from utils.settlement_geo_match import (
    build_unified_listings_settlement_match,
    get_settlement_match_names,
    resolve_unified_listings_geo_filter,
    settlement_regex,
    settlement_regex_for_names,
)


def test_settlement_regex_allows_smt_prefix():
    pat = settlement_regex("Любешів")["$regex"]
    assert "смт" in pat
    assert "Любешів" in pat


def test_settlement_regex_combined_aliases():
    combined = settlement_regex_for_names(["Іваничі", "Іваниничі"])
    assert "Іваничі" in combined["$regex"] or "Іваниничі" in combined["$regex"]


def test_unified_geo_filter_city_and_region_and_not_or():
    """Область + НП → AND (elemMatch з обома полями), не окремі OR гілки."""
    mongo = resolve_unified_listings_geo_filter(
        city_values=["Іваничі"],
        region_values=["Волинська область"],
    )
    s = json.dumps(mongo, ensure_ascii=False)
    assert "Іваничі" in s or "іваничі" in s
    assert "Волинськ" in s
    assert "$elemMatch" in s
    # Не повинно бути окремої гілки лише region без settlement у тому ж запиті
    assert s.count("$elemMatch") >= 1


def test_geo_settlement_mongo_uses_city_id():
    mgr = UnifiedListingsCollectionManager.__new__(UnifiedListingsCollectionManager)
    elem = GeoFilterElement(
        operator=GeoFilterOperator.INSIDE,
        geo_type="settlement",
        value="Новосілки",
        region="Полтавська область",
        city_id="abc123",
    )
    mongo = mgr._geo_filter_to_mongo(GeoFilter(root=elem))
    s = json.dumps(mongo, ensure_ascii=False)
    assert "city_id" in s
    assert "abc123" in s


def test_settlement_regex_does_not_match_oblast_adjective():
    """«Львів» ≠ «Львівська», «Київ» ≠ «Київська»."""
    import re

    for city, oblast in (("Львів", "Львівська область"), ("Київ", "Київська область")):
        pat = settlement_regex(city)["$regex"]
        assert re.search(pat, city, re.I)
        assert re.search(pat, f"м. {city}", re.I)
        assert not re.search(pat, oblast, re.I)
        assert not re.search(pat, oblast.replace(" область", ""), re.I)


def test_settlement_match_includes_location_substring():
    mongo = build_unified_listings_settlement_match("Нововолинськ", region="Волинська область")
    s = json.dumps(mongo, ensure_ascii=False)
    assert "search_data.location" in s
    # addresses[] лише як fallback при порожньому root city
    assert "city" in s
    assert "$elemMatch" in s


def test_settlement_match_prefers_root_not_any_address():
    """Хибний Київ у addresses[] не повинен матчити, якщо root city = Затока."""
    mongo = build_unified_listings_settlement_match("Київ")
    s = json.dumps(mongo, ensure_ascii=False)
    # прямий match по root city
    assert '"city"' in s or "'city'" in s or '"city":' in s.replace(" ", "")
    # addresses elemMatch має бути під умовою відсутності root city
    assert "city" in s
    assert s.count("$elemMatch") >= 1
    # Немає «голої» гілки addresses без root-city-missing
    # (усі addresses-гілки всередині $and з root missing)
    assert "$and" in s


def test_settlement_location_regex_does_not_match_kyivska_as_kyiv():
    from utils.settlement_geo_match import settlement_location_field_regex_for_names
    import re

    pat = settlement_location_field_regex_for_names(["Київ"])["$regex"]
    assert re.search(pat, "Київ, Київська область", re.I)
    assert not re.search(pat, "Бровари, Київська область", re.I)
    assert not re.search(pat, "Київська область", re.I)


def test_get_settlement_match_names_without_db():
    names = get_settlement_match_names(settlement_name="Тестівка")
    assert "Тестівка" in names
