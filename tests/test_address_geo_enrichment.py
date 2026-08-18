# -*- coding: utf-8 -*-

from utils.address_geo_enrichment import (
    analyze_geo_reprocess_candidate,
    build_listing_context,
    enrich_llm_geo_result,
    format_address_line_from_llm,
    normalize_region_name,
)


def test_normalize_region_name_strips_oblast_suffix():
    assert normalize_region_name("Полтавська область") == "Полтавська"
    assert normalize_region_name("Одеська обл.") == "Одеська"


def test_enrich_street_only_address_with_location_context():
    llm_result = {
        "location_context": {
            "region": "Полтавська",
            "settlement": "Полтава",
        },
        "addresses": [
            {
                "street_type": "вул.",
                "street": "Садова",
                "geo_present": {"region": False, "settlement": False, "street": True},
            }
        ],
    }
    enriched = enrich_llm_geo_result(llm_result)
    addr = enriched["addresses"][0]
    assert addr["settlement"] == "Полтава"
    assert addr["region"] == "Полтавська"
    assert addr["geo_present"]["settlement"] is True
    assert addr["geo_present"]["region"] is True


def test_enrich_uses_listing_context_when_llm_missed_settlement():
    llm_result = {
        "addresses": [
            {"street_type": "вул.", "street": "Центральна"},
        ],
    }
    listing_context = {"settlement": "Бровари", "region": "Київська"}
    enriched = enrich_llm_geo_result(llm_result, listing_context=listing_context)
    line = format_address_line_from_llm(enriched["addresses"][0])
    assert "Бровари" in line
    assert "Київська" in line
    assert "Центральна" in line


def test_enrich_propagates_settlement_from_other_address():
    llm_result = {
        "addresses": [
            {"settlement": "Лубни", "region": "Полтавська"},
            {"street": "Шевченка", "geo_present": {"region": False, "settlement": False, "street": True}},
        ],
    }
    enriched = enrich_llm_geo_result(llm_result)
    second = enriched["addresses"][1]
    assert second["settlement"] == "Лубни"
    assert second["region"] == "Полтавська"


def test_build_listing_context_from_detail_and_search_location():
    ctx = build_listing_context(
        search_data={"location": "Бровари, Київська область"},
        detail_data={"location": {"city": "Бровари", "region": "Київська область"}},
    )
    assert ctx["settlement"] == "Бровари"
    assert ctx["region"] == "Київська"


def test_build_listing_context_splits_kyiv_city_and_district():
    ctx = build_listing_context(
        search_data={"location": "Київ, Голосіївський"},
        detail_data={
            "location": {
                "city": "Київ, Голосіївський",
                "region": "Київська область",
                "raw": "Київ, Голосіївський, Київська область",
            }
        },
    )
    assert ctx["settlement"] == "Київ"
    assert ctx["district"] == "Голосіївський"
    # Не «Київська» — інакше геокод обирає область замість міста
    assert ctx["region"] in ("м. Київ", "місто Київ")


def test_normalize_olx_detail_location_unglues_kyiv_district():
    from utils.address_geo_enrichment import normalize_olx_detail_location

    out = normalize_olx_detail_location(
        {"city": "Київ, Печерський", "region": "Київська область"}
    )
    assert out["city"] == "Київ"
    assert out["city_district"] == "Печерський"
    assert out["region"] == "місто Київ"


def test_kyiv_geocode_not_contradicted_by_glued_card_city():
    from utils.address_geo_enrichment import (
        address_contradicts_location_context,
        filter_geocode_results_by_location_context,
        resolve_listing_location_context,
    )

    ctx = resolve_listing_location_context(
        search_data={"location": "Київ, Печерський"},
        detail_data={
            "location": {"city": "Київ, Печерський", "region": "Київська область"},
            "llm": {
                "addresses": [
                    {
                        "region": "Київська область",
                        "settlement": "Київ",
                        "street": "Предславинська",
                    }
                ]
            },
        },
    )
    assert ctx["settlement"] == "Київ"
    assert not address_contradicts_location_context(
        {"settlement": "Київ", "region": "місто Київ"},
        ctx,
    )
    # Область без міста — суперечить спецмісту
    assert address_contradicts_location_context(
        {"settlement": None, "region": "Київська область"},
        ctx,
    )

    results = [
        {
            "formatted_address": "вулиця Предславинська, 53, Київ, Україна",
            "address_structured": {
                "city": "Київ",
                "region": "місто Київ",
                "street": "вулиця Предславинська",
                "street_number": "53",
            },
            "location_type": "ROOFTOP",
            "types": ["street_address"],
        },
        {
            "formatted_address": "Київська область, Україна",
            "address_structured": {"region": "Київська область"},
            "location_type": "APPROXIMATE",
            "types": ["administrative_area_level_1"],
        },
    ]
    kept = filter_geocode_results_by_location_context(results, ctx)
    assert len(kept) == 1
    assert kept[0]["address_structured"]["city"] == "Київ"


def test_pick_geocode_prefers_street_over_oblast():
    from business.services.unified_listings_service import UnifiedListingsService

    svc = UnifiedListingsService.__new__(UnifiedListingsService)
    results = [
        {
            "formatted_address": "Київська область, Україна",
            "address_structured": {"region": "Київська область"},
            "location_type": "APPROXIMATE",
            "types": ["administrative_area_level_1"],
        },
        {
            "formatted_address": "вулиця Предславинська, 53, Київ",
            "address_structured": {
                "city": "Київ",
                "region": "місто Київ",
                "street": "вулиця Предславинська",
                "street_number": "53",
            },
            "location_type": "ROOFTOP",
            "types": ["street_address"],
        },
    ]
    # Старий баг: query з «Київська область» обирав перший (область)
    picked = svc._pick_geocode_result(
        results,
        "Київська область, Печерський, м. Київ, вулиця Предславинська, 53-а",
    )
    assert picked["address_structured"]["city"] == "Київ"


def test_analyze_flags_glued_kyiv_city_district():
    analysis = analyze_geo_reprocess_candidate(
        {"location": "Київ, Голосіївський"},
        {
            "location": {"city": "Київ, Голосіївський", "region": "Київська область"},
            "llm": {"addresses": [{"settlement": "Київ", "street": "Панорамна"}]},
            "resolved_locations": [
                {
                    "query_text": "x",
                    "results": [
                        {
                            "address_structured": {"city": "Київ", "region": "місто Київ"},
                            "types": ["locality"],
                        }
                    ],
                }
            ],
        },
    )
    assert analysis["needs_reprocess"] is True
    assert "glued_city_district" in analysis["reasons"]


def test_analyze_flags_glued_odesa_city_district():
    analysis = analyze_geo_reprocess_candidate(
        {"location": "Одеса, Приморський"},
        {
            "location": {"city": "Одеса, Приморський", "region": "Одеська область"},
            "llm": {"addresses": [{"settlement": "Одеса", "region": "Одеська область"}]},
        },
    )
    assert analysis["needs_reprocess"] is True
    assert "glued_city_district" in analysis["reasons"]


def test_build_address_fallback_from_llm_without_resolved():
    from utils.address_geo_enrichment import build_address_from_llm_or_context

    addrs = build_address_from_llm_or_context(
        search_data={"location": "Одеса, Приморський"},
        detail_data={
            "location": {"city": "Одеса, Приморський", "region": "Одеська область"},
            "llm": {
                "addresses": [
                    {
                        "region": "Одеська область",
                        "district": "Приморський",
                        "settlement": "Одеса",
                        "street": "Б. Арнаутська",
                    }
                ]
            },
        },
    )
    assert len(addrs) == 1
    assert addrs[0]["settlement"] == "Одеса"
    assert addrs[0]["city_district"] == "Приморський"
    assert addrs[0].get("district") in (None, "")


def test_build_olx_geo_query_includes_any_glued_city():
    from utils.address_geo_enrichment import build_olx_geo_reprocess_mongo_query

    q = build_olx_geo_reprocess_mongo_query()
    blob = str(q)
    assert "detail.location.city" in blob
    assert ".+," in blob
    assert "search_data.location" in blob



def test_listing_needs_geo_reprocess_for_street_only():
    search_data = {"location": "Полтава, Полтавська область"}
    detail_data = {
        "location": {"city": "Полтава", "region": "Полтавська"},
        "llm": {
            "addresses": [{"street_type": "вул.", "street": "Садова"}],
        },
    }
    analysis = analyze_geo_reprocess_candidate(search_data, detail_data)
    assert analysis["needs_reprocess"] is True
    assert "street_without_settlement" in analysis["reasons"]
    assert "Полтава" in analysis["enriched_query"]


def test_address_contradicts_location_context_kyiv_vs_zatoka():
    from utils.address_geo_enrichment import (
        address_contradicts_location_context,
        filter_geocode_results_by_location_context,
    )

    ctx = {"settlement": "Затока", "region": "Одеська"}
    assert address_contradicts_location_context(
        {"settlement": "Київ", "region": "місто Київ"},
        ctx,
    )
    assert not address_contradicts_location_context(
        {"settlement": "Затока", "region": "Одеська область"},
        ctx,
    )

    results = [
        {
            "formatted_address": "Вокзальна, Київ",
            "address_structured": {"city": "Київ", "region": "місто Київ"},
        },
        {
            "formatted_address": "Затока, Одеська область",
            "address_structured": {"city": "Затока", "region": "Одеська область"},
        },
    ]
    kept = filter_geocode_results_by_location_context(results, ctx)
    assert len(kept) == 1
    assert kept[0]["address_structured"]["city"] == "Затока"


def test_filter_geocode_drops_all_when_only_conflicts():
    from utils.address_geo_enrichment import filter_geocode_results_by_location_context

    ctx = {"settlement": "Ромни", "region": "Сумська"}
    results = [
        {
            "address_structured": {"city": "Львів", "region": "Львівська область", "street": "Залізнична"},
        }
    ]
    assert filter_geocode_results_by_location_context(results, ctx) == []
