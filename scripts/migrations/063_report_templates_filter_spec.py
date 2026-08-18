# -*- coding: utf-8 -*-
"""
Міграція 063: report_templates.params.filter_string → params.filter (FilterSpec).

Конвертує збережені DSL-рядки та legacy flat-параметри у структурований FilterSpec
з ключами полів та geo id (де можливо). filter_string залишається як backup.

Запуск: py scripts/migrations/063_report_templates_filter_spec.py
"""

from __future__ import annotations

import sys
from typing import Any, Dict, Optional

from config.settings import Settings
from data.database.connection import MongoDBConnection


def _flat_params_to_filter_spec(params: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Збирає FilterSpec з legacy flat params (без filter_string)."""
    from domain.services.filter_spec_service import empty_filter_spec, facets_to_filter_spec

    has_any = any(
        params.get(k)
        for k in (
            "region",
            "city",
            "property_type",
            "title_contains",
            "description_contains",
            "price",
            "building_area_sqm",
            "land_area_sotky",
            "land_area_ha",
        )
    )
    # source / date_filter лишаються окремими полями шаблону — не дублюємо в filter
    if not has_any:
        return None

    facets: Dict[str, Any] = {
        "status": [],
        "source": [],
        "property_type": [],
        "price_uah": {"min": None, "max": None},
        "building_area_sqm": {"min": None, "max": None},
        "land_area_sotky": {"min": None, "max": None},
        "source_updated_at": {"from": None, "to": None},
        "title_contains": params.get("title_contains") or "",
        "description_contains": params.get("description_contains") or "",
        "geo": {
            "regions": [],
            "settlements": [],
            "city_districts": [],
            "settlement_population": {"min": None, "max": None},
            "settlement_area_sq_km": {"min": None, "max": None},
        },
    }
    if params.get("property_type"):
        facets["property_type"] = [params["property_type"]]
    region = (params.get("region") or "").strip()
    if region:
        facets["geo"]["regions"] = [{"name": region}]
    city = (params.get("city") or "").strip()
    if city:
        facets["geo"]["settlements"] = [{"name": city, "region": region or ""}]

    price = params.get("price") or {}
    if isinstance(price, dict):
        if price.get("min") is not None:
            facets["price_uah"]["min"] = price["min"]
        if price.get("max") is not None:
            facets["price_uah"]["max"] = price["max"]
        if price.get("op") == "gte" and price.get("value") is not None:
            facets["price_uah"]["min"] = price["value"]
        if price.get("op") == "lte" and price.get("value") is not None:
            facets["price_uah"]["max"] = price["value"]
        if price.get("op") == "eq" and price.get("value") is not None:
            facets["price_uah"]["min"] = price["value"]
            facets["price_uah"]["max"] = price["value"]

    building = params.get("building_area_sqm") or {}
    if isinstance(building, dict) and building.get("value") is not None:
        op = building.get("op") or "gte"
        if op in ("gte", "gt"):
            facets["building_area_sqm"]["min"] = building["value"]
        elif op in ("lte", "lt"):
            facets["building_area_sqm"]["max"] = building["value"]
        elif op == "eq":
            facets["building_area_sqm"]["min"] = building["value"]
            facets["building_area_sqm"]["max"] = building["value"]

    land = params.get("land_area_sotky") or params.get("land_area_ha") or {}
    if isinstance(land, dict) and land.get("value") is not None:
        op = land.get("op") or "gte"
        if op in ("gte", "gt"):
            facets["land_area_sotky"]["min"] = land["value"]
        elif op in ("lte", "lt"):
            facets["land_area_sotky"]["max"] = land["value"]
        elif op == "eq":
            facets["land_area_sotky"]["min"] = land["value"]
            facets["land_area_sotky"]["max"] = land["value"]

    spec = facets_to_filter_spec(facets)
    if not (spec.get("items") or []):
        return empty_filter_spec() if has_any else None
    return spec


def _convert_params(params: Dict[str, Any]) -> Dict[str, Any]:
    from domain.services.filter_spec_service import (
        enrich_filter_spec_geo_ids,
        filter_string_to_filter_spec,
        normalize_filter_spec,
    )

    out = dict(params or {})
    existing = out.get("filter")
    if isinstance(existing, dict) and (existing.get("items") is not None or existing.get("version")):
        out["filter"] = enrich_filter_spec_geo_ids(normalize_filter_spec(existing))
        return out

    fs = (out.get("filter_string") or "").strip()
    if fs:
        spec, err = filter_string_to_filter_spec(fs)
        if err:
            print(f"  WARN: не вдалося розпарсити filter_string: {err}")
            return out
        out["filter"] = enrich_filter_spec_geo_ids(spec or {})
        return out

    flat_spec = _flat_params_to_filter_spec(out)
    if flat_spec is not None:
        out["filter"] = enrich_filter_spec_geo_ids(flat_spec)
    return out


def run_migration() -> bool:
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        col = db["report_templates"]
        updated = 0
        skipped = 0
        for doc in col.find({}):
            params = doc.get("params") or {}
            if not isinstance(params, dict):
                skipped += 1
                continue
            new_params = _convert_params(params)
            if new_params.get("filter") == params.get("filter") and "filter" in params:
                skipped += 1
                continue
            if "filter" not in new_params:
                skipped += 1
                continue
            col.update_one({"_id": doc["_id"]}, {"$set": {"params": new_params}})
            updated += 1
            name = doc.get("name") or str(doc.get("_id"))
            print(f"  OK: {name}")
        print(f"Міграція 063: оновлено {updated}, пропущено {skipped}.")
        return True
    except Exception as e:
        print("Помилка міграції 063:", e)
        return False


if __name__ == "__main__":
    sys.exit(0 if run_migration() else 1)
