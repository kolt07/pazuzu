# -*- coding: utf-8 -*-
"""
Порівняння охоплення оголошень для фільтра НП (strict vs broad).

  py scripts/check_settlement_listing_coverage.py --settlement "Нововолинськ" --region "Волинська область"
  docker exec -w /app pazuzu-app python3 scripts/check_settlement_listing_coverage.py --settlement "Нововолинськ" --region "Волинська область"
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.geography_service import GeographyService
from utils.settlement_geo_match import (
    build_unified_listings_settlement_match,
    get_settlement_match_names,
    region_regex_mongo,
    settlement_regex_for_names,
)
from domain.services.filter_string_service import filter_string_from_tree
from domain.services.unified_search_service import find_by_filter_string


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--settlement", default="Нововолинськ")
    ap.add_argument("--region", default="Волинська область")
    args = ap.parse_args()

    Settings()
    MongoDBConnection.initialize(Settings())
    db = MongoDBConnection.get_database()
    coll = db["unified_listings"]
    gs = GeographyService()

    name = args.settlement
    region = args.region
    reg = gs.regions_repo.find_by_name(region.replace(" область", "").strip() + " область")
    if not reg:
        reg = gs.regions_repo.find_by_name(region)
    region_id = str(reg["_id"]) if reg else None
    city_doc = gs.cities_repo.find_by_name_and_region(name, region_id) if region_id else None
    city_id = str(city_doc["_id"]) if city_doc else None

    print(f"НП: {name} / {region}")
    print(f"city_id: {city_id}\n")

    strict = build_unified_listings_settlement_match(name, region=region, city_id=city_id)
    strict_no_region = build_unified_listings_settlement_match(name, region=None, city_id=city_id)
    strict_no_id = build_unified_listings_settlement_match(name, region=region, city_id=None)

    names = get_settlement_match_names(
        city_id=city_id, settlement_name=name, region=region
    )
    settlement_re = settlement_regex_for_names(names)
    broad_or = {
        "$or": [
            {"city": settlement_re},
            {"addresses.settlement": settlement_re},
            {"search_data.location": {"$regex": "нововол", "$options": "i"}},
            {"detail.address_refs.city.name": settlement_re},
        ]
    }

    active = {"status": {"$eq": "активне"}}
    counts = {
        "strict (region+city_id)": coll.count_documents(strict),
        "strict + активне": coll.count_documents({"$and": [active, strict]}),
        "strict без region": coll.count_documents(strict_no_region),
        "strict без city_id": coll.count_documents(strict_no_id),
        "value only (no region/id)": coll.count_documents(
            build_unified_listings_settlement_match(name)
        ),
        "broad (location substring)": coll.count_documents(broad_or),
        "broad + активне": coll.count_documents({"$and": [active, broad_or]}),
    }
    for label, n in counts.items():
        print(f"  {label:28} {n}")

    # Приклади, що є в broad, але не в strict
    strict_ids = {d["_id"] for d in coll.find(strict, {"_id": 1})}
    missed = list(
        coll.find(
            {"$and": [broad_or, {"_id": {"$nin": list(strict_ids)}}]},
            {"city": 1, "region": 1, "search_data.location": 1, "source": 1},
        ).limit(15)
    )
    print(f"\nПропущено strict-фільтром (з broad): {len(missed)} (показано до 15)")
    for d in missed[:15]:
        loc = (d.get("search_data") or {}).get("location") or ""
        print(f"  - city={d.get('city')!r} region={d.get('region')!r} loc={loc[:60]!r} src={d.get('source')}")

    fs = filter_string_from_tree({
        "group_type": "and",
        "items": [{
            "type": "geo",
            "geo_type": "settlement",
            "operator": "inside",
            "geoRegion": region,
            "value": name,
            "city_id": city_id or "",
        }],
    })
    print(f"\nfilter_string: {fs}")
    _, total, err = find_by_filter_string(fs, limit=1)
    print(f"find_by_filter_string (full meta): total={total} err={err}")

    fs_no_id = filter_string_from_tree({
        "group_type": "and",
        "items": [{
            "type": "geo",
            "geo_type": "settlement",
            "operator": "inside",
            "value": name,
        }],
    })
    _, total2, err2 = find_by_filter_string(fs_no_id, limit=1)
    print(f"find_by_filter_string (лише назва): {fs_no_id}")
    print(f"  total={total2} err={err2}")

    any_re = {
        "$or": [
            {"city": {"$regex": "нововол", "$options": "i"}},
            {"search_data.location": {"$regex": "нововол", "$options": "i"}},
            {"addresses.settlement": {"$regex": "нововол", "$options": "i"}},
            {"detail.address_refs": {"$elemMatch": {"city.name": {"$regex": "нововол", "$options": "i"}}}},
        ]
    }
    key = name[:5].lower() if len(name) >= 5 else name.lower()
    if key != "нововол":
        any_re["$or"].append({"city": {"$regex": key, "$options": "i"}})
    n_any = coll.count_documents(any_re)
    print(f"\nБудь-яке поле містить «{key}»: {n_any}")
    missed = list(
        coll.find(
            {"$and": [any_re, {"$nor": [strict]}]},
            {"city": 1, "region": 1, "search_data": 1, "source": 1, "addresses": 1},
        ).limit(20)
    )
    print(f"Є в broad-any, але не в strict: {len(missed)}")
    for d in missed:
        loc = (d.get("search_data") or {}).get("location") or ""
        print(
            f"  {d.get('source')} city={d.get('city')!r} region={d.get('region')!r} "
            f"loc={loc[:55]!r}"
        )

    # Профіль полів у strict-вибірці
    sample = list(coll.find(strict, {"city": 1, "region": 1, "search_data": 1, "source": 1, "addresses": 1}).limit(25))
    empty_city = sum(1 for d in sample if not (d.get("city") or "").strip())
    loc_only = sum(
        1 for d in sample
        if (d.get("search_data") or {}).get("location") and not (d.get("city") or "").strip()
    )
    print(f"\nПрофіль strict ({len(sample)} docs): empty city={empty_city}, location-only={loc_only}")

    settlement_re = settlement_regex_for_names(names)
    city_only = coll.count_documents({"city": settlement_re})
    city_and_region = coll.count_documents(
        {"$and": [{"city": settlement_re}, {"region": region_regex_mongo(region) or {}}]}
    )
    city_no_region = coll.count_documents(
        {
            "city": settlement_re,
            "$or": [{"region": None}, {"region": ""}, {"region": {"$exists": False}}],
        }
    )
    print(f"city match (any region): {city_only}")
    print(f"city AND region match: {city_and_region}")
    print(f"city but region empty: {city_no_region}")
    if city_only > city_and_region:
        print("  ⚠ частина оголошень має НП у city, але region порожній/інший — strict з REGION відсікає")

    reg_short = (region or "").replace(" область", "").strip()
    if reg_short and reg_short != region:
        strict_short_reg = build_unified_listings_settlement_match(
            name, region=reg_short, city_id=city_id
        )
        print(f"strict з region={reg_short!r}: {coll.count_documents(strict_short_reg)}")

    by_id = coll.count_documents({"city_id": city_id}) if city_id else 0
    print(f"лише city_id={city_id}: {by_id}")

    # Симуляція UI без вибору зі списку (немає city_id, є region з picker)
    sim_ui = build_unified_listings_settlement_match(name, region=reg_short or region, city_id=None)
    print(f"симуляція: value+region без city_id: {coll.count_documents(sim_ui)}")

    loc_prefix = coll.count_documents({"search_data.location": settlement_re})
    loc_substr_n = coll.count_documents(
        {"search_data.location": {"$regex": re.escape(name), "$options": "i"}}
    )
    print(f"location prefix-match: {loc_prefix}, location substring: {loc_substr_n}")
    miss_loc = list(
        coll.find(
            {
                "search_data.location": {"$regex": re.escape(name), "$options": "i"},
                "$nor": [{"city": settlement_re}],
            },
            {"city": 1, "search_data": 1, "source": 1},
        ).limit(10)
    )
    print(f"location substring but city NOT prefix-match: {len(miss_loc)}")
    for d in miss_loc[:5]:
        print(f"  {d.get('source')} city={d.get('city')!r} loc={(d.get('search_data') or {}).get('location')!r}")

    MongoDBConnection.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
