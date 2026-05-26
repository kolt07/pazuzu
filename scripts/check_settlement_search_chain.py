# -*- coding: utf-8 -*-
"""
Діагностика ланцюжка НП: cities → /filters/cities → пошук.

Запуск:
  py scripts/check_settlement_search_chain.py --settlement "Іваничі" --region "Волинська область"
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.geography_service import GeographyService
from utils.settlement_normalizer import normalize_settlement_key, build_city_filter_options
from utils.settlement_geo_match import build_unified_listings_settlement_match
from domain.services.unified_search_service import find_by_filter_string
from domain.services.filter_string_service import filter_string_from_tree


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--settlement", default="Іваничі")
    ap.add_argument("--region", default="Волинська область")
    args = ap.parse_args()

    Settings()
    MongoDBConnection.initialize(Settings())
    db = MongoDBConnection.get_database()
    gs = GeographyService()

    name = args.settlement
    region = args.region
    print(f"Перевірка: {name} / {region}\n")

    # 1. cities
    from utils.ukraine_regions import normalize_region_for_repository_lookup

    reg = gs.regions_repo.find_by_name(normalize_region_for_repository_lookup(region) or region)
    in_catalog = False
    city_id = None
    if reg:
        found = gs.cities_repo.find_by_name_and_region(name, str(reg["_id"]))
        in_catalog = bool(found)
        if found:
            city_id = str(found["_id"])
        catalog = gs.get_cities_by_region(str(reg["_id"]))
        opts = build_city_filter_options(catalog, region_name=region)
        in_opts = any(normalize_settlement_key(o.get("name") or "") == normalize_settlement_key(name) for o in opts)
    else:
        in_opts = False

    print(f"[1] cities DB:        {'OK' if in_catalog else 'НЕМАЄ'} (id={city_id})")
    print(f"[2] endpoint payload: {'OK' if in_opts else 'НЕМАЄ'}")

    # 2. listings
    mongo = build_unified_listings_settlement_match(name, region=region, city_id=city_id)
    n_listings = db.unified_listings.count_documents(mongo)
    print(f"[3] unified_listings: {n_listings} оголошень")

    # 3. API search
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
    _, total, err = find_by_filter_string(fs, limit=5)
    print(f"[4] search API:       total={total} err={err}")

    print()
    if in_catalog and in_opts and n_listings == 0:
        print("ВИСНОВОК: довідник і UI-endpoint OK; збій на етапі ДАНИХ (немає оголошень).")
    elif not in_catalog:
        print("ВИСНОВОК: збій на етапі cities (довідник).")
    elif not in_opts:
        print("ВИСНОВОК: збій на етапі endpoint /filters/cities.")
    else:
        print("ВИСНОВОК: ланцюжок OK.")

    MongoDBConnection.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
