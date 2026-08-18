# -*- coding: utf-8 -*-
"""
FilterSpec: канонічний структурований контракт відборів оголошень.

UI і API працюють з JSON (ключі полів + city_id/region_id), не з DSL-рядком.
Рядок фільтрів використовується лише для міграції старих шаблонів.
"""

from __future__ import annotations

from copy import deepcopy
from typing import Any, Dict, List, Optional, Tuple

from domain.services.filter_string_service import (
    filter_string_to_tree,
    tree_to_filter_models,
)
from domain.models.filter_models import FilterGroup, GeoFilter

FILTER_SPEC_VERSION = 1

# Поля, які фасетний UI вміє відображати (решта → advanced-only)
_FACET_ELEMENT_FIELDS = frozenset({
    "status",
    "source",
    "property_type",
    "price_uah",
    "price_usd",
    "building_area_sqm",
    "land_area_sotky",
    "land_area_ha",
    "price_per_m2_uah",
    "price_per_m2_usd",
    "price_per_ha_uah",
    "price_per_ha_usd",
    "source_updated_at",
    "title",
    "description",
})

_RANGE_FIELDS = frozenset({
    "price_uah",
    "price_usd",
    "building_area_sqm",
    "land_area_sotky",
    "land_area_ha",
    "price_per_m2_uah",
    "price_per_m2_usd",
    "price_per_ha_uah",
    "price_per_ha_usd",
})

_MULTI_EQ_FIELDS = frozenset({"status", "source", "property_type"})


def empty_filter_spec() -> Dict[str, Any]:
    return {"version": FILTER_SPEC_VERSION, "group_type": "and", "items": []}


def normalize_filter_spec(spec: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Нормалізує FilterSpec: version, group_type, items; camelCase → snake_case для geo."""
    if not spec or not isinstance(spec, dict):
        return empty_filter_spec()
    out = deepcopy(spec)
    out["version"] = int(out.get("version") or FILTER_SPEC_VERSION)
    gt = str(out.get("group_type") or out.get("groupType") or "and").strip().lower()
    out["group_type"] = gt if gt in ("and", "or", "not") else "and"
    items = out.get("items")
    if not isinstance(items, list):
        items = []
    out["items"] = [_normalize_tree_item(it) for it in items if isinstance(it, dict)]
    out["items"] = _flatten_trivial_and_groups(out["items"])
    return out


def _flatten_trivial_and_groups(items: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Розгортає зайві AND-групи з одним/кількома елементами (після парсингу DSL)."""
    flat: List[Dict[str, Any]] = []
    for it in items:
        if not isinstance(it, dict):
            continue
        if str(it.get("type") or "").lower() == "group" and str(it.get("group_type") or "and").lower() == "and":
            children = it.get("items") or []
            # flatten nested AND into parent list
            flat.extend(_flatten_trivial_and_groups([c for c in children if isinstance(c, dict)]))
            continue
        if str(it.get("type") or "").lower() == "group":
            it = dict(it)
            it["items"] = _flatten_trivial_and_groups(it.get("items") or [])
            flat.append(it)
            continue
        flat.append(it)
    return flat

def _normalize_tree_item(it: Dict[str, Any]) -> Dict[str, Any]:
    item = dict(it)
    t = str(item.get("type") or "").strip().lower()
    item["type"] = t
    if t == "group":
        gt = str(item.get("group_type") or item.get("groupType") or "and").strip().lower()
        item["group_type"] = gt if gt in ("and", "or", "not") else "and"
        children = item.get("items")
        item["items"] = [
            _normalize_tree_item(c) for c in (children or []) if isinstance(c, dict)
        ]
        return item
    if t == "geo":
        if "geoType" in item and "geo_type" not in item:
            item["geo_type"] = item.pop("geoType")
        if "geoRegion" in item and not item.get("geoRegion") is False:
            # keep geoRegion for UI; also mirror to region for models path
            pass
        if "cityId" in item and "city_id" not in item:
            item["city_id"] = item.pop("cityId")
        if "regionId" in item and "region_id" not in item:
            item["region_id"] = item.pop("regionId")
        # Accept region as alias of geoRegion
        if item.get("region") and not item.get("geoRegion"):
            item["geoRegion"] = item["region"]
        return item
    if t == "element":
        if "operator" in item and item["operator"] is not None:
            item["operator"] = str(item["operator"]).strip().lower()
        return item
    return item


def validate_filter_spec(spec: Optional[Dict[str, Any]]) -> Optional[str]:
    """Повертає повідомлення про помилку або None."""
    if spec is None:
        return None
    if not isinstance(spec, dict):
        return "FilterSpec має бути об'єктом"
    items = spec.get("items")
    if items is not None and not isinstance(items, list):
        return "FilterSpec.items має бути масивом"

    def walk(node: Dict[str, Any], path: str) -> Optional[str]:
        t = str(node.get("type") or "").strip().lower()
        if t == "group":
            for i, child in enumerate(node.get("items") or []):
                if not isinstance(child, dict):
                    return f"{path}.items[{i}]: очікується об'єкт"
                err = walk(child, f"{path}.items[{i}]")
                if err:
                    return err
            return None
        if t == "element":
            field = node.get("field")
            if not field or not isinstance(field, str):
                return f"{path}: element без field"
            return None
        if t == "geo":
            geo_type = str(node.get("geo_type") or node.get("geoType") or "").strip()
            if not geo_type:
                return f"{path}: geo без geo_type"
            return None
        if not t:
            return f"{path}: відсутній type"
        return f"{path}: невідомий type={t!r}"

    for i, it in enumerate(items or []):
        if not isinstance(it, dict):
            return f"items[{i}]: очікується об'єкт"
        err = walk(it, f"items[{i}]")
        if err:
            return err
    return None


def filter_spec_to_models(
    spec: Optional[Dict[str, Any]],
) -> Tuple[Optional[FilterGroup], Optional[GeoFilter], Optional[str]]:
    """
    FilterSpec → FilterGroup + GeoFilter.
    Повертає (filter_group, geo_filter, error).
    """
    if not spec or not isinstance(spec, dict):
        return None, None, None
    err = validate_filter_spec(spec)
    if err:
        return None, None, err
    normalized = normalize_filter_spec(spec)
    # tree_to_filter_models очікує {group_type, items} без version
    root = {"group_type": normalized["group_type"], "items": normalized["items"]}
    if not root["items"]:
        return None, None, None
    group, geo = tree_to_filter_models(root)
    return group, geo, None


def models_to_filter_spec(
    filter_group: Optional[FilterGroup],
    geo_filter: Optional[GeoFilter],
) -> Dict[str, Any]:
    """FilterGroup + GeoFilter → FilterSpec (через існуючу серіалізацію в tree)."""
    from domain.services.filter_string_service import (
        _filter_group_to_tree_items,
        _geo_filter_root_to_tree_items,
    )

    items: List[Dict[str, Any]] = []
    group_type = "and"
    if filter_group and filter_group.items:
        group_type = filter_group.group_type.value
        items.extend(_filter_group_to_tree_items(filter_group))
    if geo_filter and geo_filter.root:
        items.extend(_geo_filter_root_to_tree_items(geo_filter.root))
    return normalize_filter_spec({
        "version": FILTER_SPEC_VERSION,
        "group_type": group_type,
        "items": items,
    })


def filter_string_to_filter_spec(
    filter_string: str,
    collection: str = "unified_listings",
) -> Tuple[Optional[Dict[str, Any]], Optional[str]]:
    """Міграція: DSL-рядок → FilterSpec."""
    root, err = filter_string_to_tree(filter_string, collection=collection)
    if err:
        return None, err
    if not root:
        return empty_filter_spec(), None
    return normalize_filter_spec({
        "version": FILTER_SPEC_VERSION,
        "group_type": root.get("group_type") or "and",
        "items": root.get("items") or [],
    }), None


def enrich_filter_spec_geo_ids(spec: Dict[str, Any]) -> Dict[str, Any]:
    """
    Резолвить region_id / city_id за назвами, де можливо.
    Не падає при відсутності БД — повертає spec як є.
    """
    out = normalize_filter_spec(spec)

    def enrich_item(it: Dict[str, Any]) -> Dict[str, Any]:
        if it.get("type") == "group":
            it["items"] = [enrich_item(c) for c in (it.get("items") or [])]
            return it
        if it.get("type") != "geo":
            return it
        geo_type = str(it.get("geo_type") or "").strip()
        try:
            from data.repositories.geography_repository import CitiesRepository, RegionsRepository
            from utils.ukraine_regions import normalize_region_for_repository_lookup

            regions_repo = RegionsRepository()
            cities_repo = CitiesRepository()

            if geo_type == "region":
                if not it.get("region_id"):
                    name = str(it.get("value") or "").strip()
                    if name:
                        lookup = normalize_region_for_repository_lookup(name) or name
                        reg = regions_repo.find_by_name(lookup)
                        if reg and reg.get("_id") is not None:
                            it["region_id"] = str(reg["_id"])
                            if not it.get("value"):
                                it["value"] = reg.get("name") or name
                elif not it.get("value"):
                    reg = regions_repo.find_by_id(str(it["region_id"]))
                    if reg:
                        it["value"] = reg.get("name") or it.get("value")

            if geo_type == "settlement":
                region_name = (
                    str(it.get("geoRegion") or it.get("region") or "").strip() or None
                )
                if not it.get("region_id") and region_name:
                    lookup = normalize_region_for_repository_lookup(region_name) or region_name
                    reg = regions_repo.find_by_name(lookup)
                    if reg and reg.get("_id") is not None:
                        it["region_id"] = str(reg["_id"])
                        it["geoRegion"] = reg.get("name") or region_name

                if not it.get("city_id"):
                    name = str(it.get("value") or "").strip()
                    rid = it.get("region_id")
                    if name and rid:
                        city = cities_repo.find_by_name_and_region(name, str(rid))
                        if city and city.get("_id") is not None:
                            it["city_id"] = str(city["_id"])
                            it["value"] = city.get("name") or name
                elif it.get("city_id") and not it.get("value"):
                    city = cities_repo.find_by_id(str(it["city_id"]))
                    if city:
                        it["value"] = city.get("name") or it.get("value")
                        if not it.get("region_id") and city.get("region_id") is not None:
                            it["region_id"] = str(city["region_id"])
        except Exception:
            pass
        return it

    out["items"] = [enrich_item(it) for it in out.get("items") or []]
    return out


def empty_facet_state() -> Dict[str, Any]:
    return {
        "status": [],
        "source": [],
        "property_type": [],
        "price_currency": "uah",
        "price_uah": {"min": None, "max": None},
        "price_usd": {"min": None, "max": None},
        "building_area_sqm": {"min": None, "max": None},
        "land_area_sotky": {"min": None, "max": None},
        "source_updated_at": {"from": None, "to": None},
        "title_contains": "",
        "description_contains": "",
        "geo": {
            "regions": [],
            "settlements": [],
            "city_districts": [],
            "settlement_population": {"min": None, "max": None},
            "settlement_area_sq_km": {"min": None, "max": None},
        },
    }


def facets_to_filter_spec(facets: Optional[Dict[str, Any]]) -> Dict[str, Any]:
    """Фасетний стан UI → FilterSpec (AND між вимірами, OR/in всередині)."""
    f = facets or {}
    items: List[Dict[str, Any]] = []

    def _add_multi(field: str, values: Any) -> None:
        vals = values if isinstance(values, list) else ([values] if values else [])
        cleaned = [v for v in vals if v is not None and str(v).strip() != ""]
        if not cleaned:
            return
        if len(cleaned) == 1:
            items.append({
                "type": "element",
                "field": field,
                "operator": "eq",
                "value": cleaned[0] if field != "source" else str(cleaned[0]).strip().lower(),
            })
        else:
            norm = [
                str(v).strip().lower() if field == "source" else v
                for v in cleaned
            ]
            items.append({
                "type": "element",
                "field": field,
                "operator": "in",
                "value": norm,
            })

    _add_multi("status", f.get("status"))
    _add_multi("source", f.get("source"))
    _add_multi("property_type", f.get("property_type"))

    currency = str(f.get("price_currency") or "uah").strip().lower()
    price_field = "price_usd" if currency == "usd" else "price_uah"
    # Якщо явно заповнено price_usd без currency — теж врахувати
    if currency != "usd":
        usd = f.get("price_usd") or {}
        if isinstance(usd, dict) and (usd.get("min") is not None or usd.get("max") is not None):
            if not (f.get("price_uah") or {}).get("min") and not (f.get("price_uah") or {}).get("max"):
                price_field = "price_usd"

    for field in (price_field, "building_area_sqm", "land_area_sotky"):
        rng = f.get(field) or {}
        if not isinstance(rng, dict):
            continue
        if rng.get("min") is not None and rng.get("min") != "":
            try:
                items.append({
                    "type": "element",
                    "field": field,
                    "operator": "gte",
                    "value": float(rng["min"]),
                })
            except (TypeError, ValueError):
                pass
        if rng.get("max") is not None and rng.get("max") != "":
            try:
                items.append({
                    "type": "element",
                    "field": field,
                    "operator": "lte",
                    "value": float(rng["max"]),
                })
            except (TypeError, ValueError):
                pass

    dates = f.get("source_updated_at") or {}
    if isinstance(dates, dict):
        if dates.get("from"):
            items.append({
                "type": "element",
                "field": "source_updated_at",
                "operator": "gte",
                "value": dates["from"],
            })
        if dates.get("to"):
            items.append({
                "type": "element",
                "field": "source_updated_at",
                "operator": "lte",
                "value": dates["to"],
            })

    title = (f.get("title_contains") or "").strip()
    if title:
        items.append({
            "type": "element",
            "field": "title",
            "operator": "contains",
            "value": title,
        })
    desc = (f.get("description_contains") or "").strip()
    if desc:
        items.append({
            "type": "element",
            "field": "description",
            "operator": "contains",
            "value": desc,
        })

    geo = f.get("geo") or {}
    regions = geo.get("regions") or []
    if len(regions) == 1:
        r = regions[0] if isinstance(regions[0], dict) else {"name": regions[0]}
        geo_item: Dict[str, Any] = {
            "type": "geo",
            "geo_type": "region",
            "operator": "inside",
            "value": r.get("name") or r.get("value") or "",
        }
        if r.get("id") or r.get("region_id"):
            geo_item["region_id"] = str(r.get("id") or r.get("region_id"))
        items.append(geo_item)
    elif len(regions) > 1:
        or_items = []
        for r in regions:
            rd = r if isinstance(r, dict) else {"name": r}
            gi: Dict[str, Any] = {
                "type": "geo",
                "geo_type": "region",
                "operator": "inside",
                "value": rd.get("name") or rd.get("value") or "",
            }
            if rd.get("id") or rd.get("region_id"):
                gi["region_id"] = str(rd.get("id") or rd.get("region_id"))
            or_items.append(gi)
        items.append({"type": "group", "group_type": "or", "items": or_items})

    settlements = geo.get("settlements") or []
    if len(settlements) == 1:
        s = settlements[0] if isinstance(settlements[0], dict) else {"name": settlements[0]}
        gi = {
            "type": "geo",
            "geo_type": "settlement",
            "operator": "inside",
            "value": s.get("name") or s.get("value") or "",
        }
        if s.get("city_id") or s.get("cityId"):
            gi["city_id"] = str(s.get("city_id") or s.get("cityId"))
        if s.get("region_id") or s.get("regionId"):
            gi["region_id"] = str(s.get("region_id") or s.get("regionId"))
        region_name = s.get("region") or s.get("geoRegion")
        if region_name:
            gi["geoRegion"] = region_name
        items.append(gi)
    elif len(settlements) > 1:
        or_items = []
        for s in settlements:
            sd = s if isinstance(s, dict) else {"name": s}
            gi = {
                "type": "geo",
                "geo_type": "settlement",
                "operator": "inside",
                "value": sd.get("name") or sd.get("value") or "",
            }
            if sd.get("city_id") or sd.get("cityId"):
                gi["city_id"] = str(sd.get("city_id") or sd.get("cityId"))
            if sd.get("region_id") or sd.get("regionId"):
                gi["region_id"] = str(sd.get("region_id") or sd.get("regionId"))
            region_name = sd.get("region") or sd.get("geoRegion")
            if region_name:
                gi["geoRegion"] = region_name
            or_items.append(gi)
        items.append({"type": "group", "group_type": "or", "items": or_items})

    for dist in geo.get("city_districts") or []:
        name = dist.get("name") if isinstance(dist, dict) else dist
        if name and str(name).strip():
            items.append({
                "type": "geo",
                "geo_type": "city_district",
                "operator": "inside",
                "value": str(name).strip(),
            })

    pop = geo.get("settlement_population") or {}
    if isinstance(pop, dict) and (pop.get("min") is not None or pop.get("max") is not None):
        gi = {
            "type": "geo",
            "geo_type": "settlement_population",
            "operator": "inside",
            "value": "",
        }
        if pop.get("min") is not None and pop.get("min") != "":
            gi["population_min"] = int(pop["min"])
        if pop.get("max") is not None and pop.get("max") != "":
            gi["population_max"] = int(pop["max"])
        items.append(gi)

    area = geo.get("settlement_area_sq_km") or {}
    if isinstance(area, dict) and (area.get("min") is not None or area.get("max") is not None):
        gi = {
            "type": "geo",
            "geo_type": "settlement_area",
            "operator": "inside",
            "value": "",
        }
        if area.get("min") is not None and area.get("min") != "":
            gi["area_min"] = float(area["min"])
        if area.get("max") is not None and area.get("max") != "":
            gi["area_max"] = float(area["max"])
        items.append(gi)

    return normalize_filter_spec({
        "version": FILTER_SPEC_VERSION,
        "group_type": "and",
        "items": items,
    })


def filter_spec_to_facet_state(
    spec: Optional[Dict[str, Any]],
) -> Tuple[Optional[Dict[str, Any]], bool]:
    """
    FilterSpec → FacetState.
    Повертає (facets, is_simple).
    is_simple=False якщо дерево не вкладається в фасетну модель (потрібен Advanced).
    """
    normalized = normalize_filter_spec(spec)
    facets = empty_facet_state()
    items = normalized.get("items") or []
    if not items:
        return facets, True

    # Корінь має бути AND для фасетів
    if str(normalized.get("group_type") or "and").lower() != "and":
        return None, False

    def _absorb_range(field: str, op: str, value: Any) -> bool:
        if field not in _RANGE_FIELDS and field != "source_updated_at":
            return False
        if field == "source_updated_at":
            if op in ("gte", "gt"):
                if facets["source_updated_at"]["from"] is not None:
                    return False
                facets["source_updated_at"]["from"] = value
                return True
            if op in ("lte", "lt"):
                if facets["source_updated_at"]["to"] is not None:
                    return False
                facets["source_updated_at"]["to"] = value
                return True
            return False
        key = field if field in facets else None
        if key is None:
            # optional extra range fields not in default facet UI
            facets.setdefault(field, {"min": None, "max": None})
            key = field
        if op in ("gte", "gt"):
            if facets[key]["min"] is not None:
                return False
            facets[key]["min"] = value
            if field == "price_usd":
                facets["price_currency"] = "usd"
            elif field == "price_uah":
                facets["price_currency"] = "uah"
            return True
        if op in ("lte", "lt"):
            if facets[key]["max"] is not None:
                return False
            facets[key]["max"] = value
            if field == "price_usd":
                facets["price_currency"] = "usd"
            elif field == "price_uah":
                facets["price_currency"] = "uah"
            return True
        if op == "eq":
            facets[key]["min"] = value
            facets[key]["max"] = value
            if field == "price_usd":
                facets["price_currency"] = "usd"
            elif field == "price_uah":
                facets["price_currency"] = "uah"
            return True
        return False

    def absorb_geo_region(it: Dict[str, Any]) -> bool:
        if str(it.get("geo_type") or "") != "region":
            return False
        if str(it.get("operator") or "inside").lower() not in ("inside", "eq"):
            return False
        entry = {
            "id": it.get("region_id") or None,
            "name": it.get("value") or "",
        }
        if entry["id"]:
            entry["id"] = str(entry["id"])
        facets["geo"]["regions"].append(entry)
        return True

    def absorb_geo_settlement(it: Dict[str, Any]) -> bool:
        if str(it.get("geo_type") or "") != "settlement":
            return False
        if str(it.get("operator") or "inside").lower() not in ("inside", "eq"):
            return False
        # demographic on settlement → not simple facet combo with name in same node unless only demo
        entry = {
            "city_id": it.get("city_id") or it.get("cityId"),
            "region_id": it.get("region_id") or it.get("regionId"),
            "name": it.get("value") or "",
            "region": it.get("geoRegion") or it.get("region") or "",
        }
        if entry["city_id"]:
            entry["city_id"] = str(entry["city_id"])
        if entry["region_id"]:
            entry["region_id"] = str(entry["region_id"])
        facets["geo"]["settlements"].append(entry)
        return True

    def absorb_item(it: Dict[str, Any]) -> bool:
        t = str(it.get("type") or "").lower()
        if t == "element":
            field = str(it.get("field") or "")
            op = str(it.get("operator") or "eq").lower()
            value = it.get("value")
            if field not in _FACET_ELEMENT_FIELDS:
                return False
            if field in _MULTI_EQ_FIELDS:
                if op == "eq":
                    facets[field].append(value)
                    return True
                if op == "in" and isinstance(value, list):
                    facets[field].extend(value)
                    return True
                return False
            if field == "title" and op == "contains":
                if facets["title_contains"]:
                    return False
                facets["title_contains"] = str(value or "")
                return True
            if field == "description" and op == "contains":
                if facets["description_contains"]:
                    return False
                facets["description_contains"] = str(value or "")
                return True
            if field in _RANGE_FIELDS or field == "source_updated_at":
                return _absorb_range(field, op, value)
            return False

        if t == "geo":
            gt = str(it.get("geo_type") or "")
            if gt == "region":
                return absorb_geo_region(it)
            if gt == "settlement":
                # population/area on same node → still ok for facets if we also set demo
                ok = absorb_geo_settlement(it)
                pop_min, pop_max = it.get("population_min"), it.get("population_max")
                area_min, area_max = it.get("area_min"), it.get("area_max")
                if pop_min is not None or pop_max is not None:
                    if facets["geo"]["settlement_population"]["min"] is not None or \
                       facets["geo"]["settlement_population"]["max"] is not None:
                        return False
                    facets["geo"]["settlement_population"]["min"] = pop_min
                    facets["geo"]["settlement_population"]["max"] = pop_max
                if area_min is not None or area_max is not None:
                    if facets["geo"]["settlement_area_sq_km"]["min"] is not None or \
                       facets["geo"]["settlement_area_sq_km"]["max"] is not None:
                        return False
                    facets["geo"]["settlement_area_sq_km"]["min"] = area_min
                    facets["geo"]["settlement_area_sq_km"]["max"] = area_max
                return ok
            if gt == "city_district":
                if str(it.get("operator") or "inside").lower() not in ("inside", "eq"):
                    return False
                facets["geo"]["city_districts"].append(it.get("value") or "")
                return True
            if gt == "settlement_population":
                if facets["geo"]["settlement_population"]["min"] is not None or \
                   facets["geo"]["settlement_population"]["max"] is not None:
                    return False
                facets["geo"]["settlement_population"]["min"] = it.get("population_min")
                facets["geo"]["settlement_population"]["max"] = it.get("population_max")
                return True
            if gt == "settlement_area":
                if facets["geo"]["settlement_area_sq_km"]["min"] is not None or \
                   facets["geo"]["settlement_area_sq_km"]["max"] is not None:
                    return False
                facets["geo"]["settlement_area_sq_km"]["min"] = it.get("area_min")
                facets["geo"]["settlement_area_sq_km"]["max"] = it.get("area_max")
                return True
            return False

        if t == "group":
            gt = str(it.get("group_type") or "and").lower()
            children = it.get("items") or []
            if gt != "or" or not children:
                return False
            # OR-група лише однотипних geo region або settlement
            kinds = set()
            for ch in children:
                if not isinstance(ch, dict) or str(ch.get("type") or "") != "geo":
                    return False
                kinds.add(str(ch.get("geo_type") or ""))
            if kinds == {"region"}:
                return all(absorb_geo_region(ch) for ch in children)
            if kinds == {"settlement"}:
                return all(absorb_geo_settlement(ch) for ch in children)
            return False

        return False

    for it in items:
        if not isinstance(it, dict) or not absorb_item(it):
            return None, False
    return facets, True


def filter_spec_summary(spec: Optional[Dict[str, Any]]) -> str:
    """Коткий текст для чіпів/summary (без DSL)."""
    facets, simple = filter_spec_to_facet_state(spec)
    if not simple or not facets:
        n = len((normalize_filter_spec(spec).get("items") or []))
        return f"Розширені відбори ({n})" if n else ""
    parts: List[str] = []
    for s in facets.get("source") or []:
        parts.append(str(s).upper() if str(s).lower() in ("olx", "prozorro") else str(s))
    for p in facets.get("property_type") or []:
        parts.append(str(p))
    for st in facets.get("status") or []:
        parts.append(str(st))
    currency = str(facets.get("price_currency") or "uah").lower()
    price = facets.get("price_usd") if currency == "usd" else facets.get("price_uah")
    price = price or {}
    if price.get("min") is not None or price.get("max") is not None:
        unit = "$" if currency == "usd" else "грн"
        lo = price.get("min")
        hi = price.get("max")
        if lo is not None and hi is not None:
            parts.append(f"ціна {lo}–{hi} {unit}")
        elif lo is not None:
            parts.append(f"ціна ≥ {lo} {unit}")
        else:
            parts.append(f"ціна ≤ {hi} {unit}")
    for r in (facets.get("geo") or {}).get("regions") or []:
        parts.append(r.get("name") if isinstance(r, dict) else str(r))
    for s in (facets.get("geo") or {}).get("settlements") or []:
        parts.append(s.get("name") if isinstance(s, dict) else str(s))
    for d in (facets.get("geo") or {}).get("city_districts") or []:
        parts.append(str(d))
    if facets.get("title_contains"):
        parts.append(f"«{facets['title_contains']}»")
    return ", ".join(p for p in parts if p)
