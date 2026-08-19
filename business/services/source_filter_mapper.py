# -*- coding: utf-8 -*-
"""
Перетворення FilterSpec + deal_types + глибина на плани запитів до джерел (OLX / ProZorro).
Фільтри, яких немає в джерелі, лишаються для постфільтрації після pipeline.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import datetime, timedelta, timezone
from typing import Any, Dict, List, Optional, Tuple

from utils.deal_type import DEAL_RENT, DEAL_SALE, normalize_deal_types

LAND_PROPERTY_TYPES = frozenset({
    "Земельна ділянка",
    "Земельна ділянка з нерухомістю",
})
COMMERCIAL_PROPERTY_TYPES = frozenset({
    "Комерційна нерухомість",
    "Будівля",
    "Приміщення",
    "Квартира",
    "Будинок",
    "інше",
    "Інше",
})


@dataclass
class SourceQueryPlan:
    sources: List[str]
    deal_types: List[str]
    depth_days: Optional[int]
    cutoff_utc: Optional[datetime]
    regions: List[str]
    settlements: List[str]
    listing_type_patterns: List[str]
    price_uah_min: Optional[float] = None
    price_uah_max: Optional[float] = None
    building_area_min: Optional[float] = None
    building_area_max: Optional[float] = None
    land_area_sotky_min: Optional[float] = None
    land_area_sotky_max: Optional[float] = None
    query_text: Optional[str] = None
    has_geo: bool = False
    extra_olx_query_pairs: List[Tuple[str, str]] = field(default_factory=list)


def _walk_items(items: List[Dict[str, Any]]):
    for it in items or []:
        if not isinstance(it, dict):
            continue
        yield it
        if str(it.get("type") or "").lower() == "group":
            yield from _walk_items(it.get("items") or [])


def _num(value: Any) -> Optional[float]:
    if value is None or value == "":
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _collect_eq_in(spec: Dict[str, Any], field: str) -> List[Any]:
    out: List[Any] = []
    for it in _walk_items(spec.get("items") or []):
        if str(it.get("type") or "").lower() != "element":
            continue
        if str(it.get("field") or "") != field:
            continue
        op = str(it.get("operator") or "eq").lower()
        val = it.get("value")
        if op in ("eq",) and val is not None and val != "":
            out.append(val)
        elif op == "in" and isinstance(val, list):
            out.extend([v for v in val if v is not None and v != ""])
    return out


def _range_bounds(spec: Dict[str, Any], field: str) -> Tuple[Optional[float], Optional[float]]:
    lo = hi = None
    for it in _walk_items(spec.get("items") or []):
        if str(it.get("type") or "").lower() != "element":
            continue
        if str(it.get("field") or "") != field:
            continue
        op = str(it.get("operator") or "").lower()
        n = _num(it.get("value"))
        if n is None:
            continue
        if op in ("gte", "gt"):
            lo = n if lo is None else max(lo, n)
        elif op in ("lte", "lt"):
            hi = n if hi is None else min(hi, n)
        elif op == "eq":
            lo = hi = n
    return lo, hi


def _text_contains(spec: Dict[str, Any], field: str) -> Optional[str]:
    parts = []
    for it in _walk_items(spec.get("items") or []):
        if str(it.get("type") or "").lower() != "element":
            continue
        if str(it.get("field") or "") != field:
            continue
        if str(it.get("operator") or "").lower() != "contains":
            continue
        val = str(it.get("value") or "").strip()
        if val:
            parts.append(val)
    if not parts:
        return None
    return " ".join(parts)


def _geo_names(spec: Dict[str, Any], geo_type: str) -> List[str]:
    names: List[str] = []
    for it in _walk_items(spec.get("items") or []):
        if str(it.get("type") or "").lower() != "geo":
            continue
        if str(it.get("geo_type") or "") != geo_type:
            continue
        val = str(it.get("value") or it.get("geoRegion") or "").strip()
        if val:
            names.append(val)
    return names


def _listing_type_patterns(property_types: List[str]) -> List[str]:
    if not property_types:
        return []
    want_land = any(p in LAND_PROPERTY_TYPES for p in property_types)
    want_comm = any(p in COMMERCIAL_PROPERTY_TYPES for p in property_types)
    if want_land and want_comm:
        return []
    if want_land:
        return ["Земл"]
    if want_comm:
        return ["Нежитлов"]
    return []


def _olx_extra_pairs(plan: SourceQueryPlan) -> List[Tuple[str, str]]:
    pairs: List[Tuple[str, str]] = []
    if plan.price_uah_min is not None:
        pairs.append(("search[filter_float_price:from]", str(int(plan.price_uah_min))))
    if plan.price_uah_max is not None:
        pairs.append(("search[filter_float_price:to]", str(int(plan.price_uah_max))))
    if plan.query_text:
        pairs.append(("search[query]", plan.query_text))
        pairs.append(("q", plan.query_text))
    return pairs


def build_source_query_plan(
    filter_spec: Optional[Dict[str, Any]],
    deal_types: Optional[List[str]] = None,
    depth_days: Optional[int] = None,
) -> SourceQueryPlan:
    spec = filter_spec if isinstance(filter_spec, dict) else {}
    sources_raw = [str(s).strip().lower() for s in _collect_eq_in(spec, "source")]
    sources = [s for s in sources_raw if s in ("olx", "prozorro")]
    if not sources:
        sources = ["olx", "prozorro"]

    dts = normalize_deal_types(deal_types)
    days = None
    if depth_days is not None:
        try:
            d = int(depth_days)
            if d > 0:
                days = d
        except (TypeError, ValueError):
            days = None
    cutoff = None
    if days:
        now = datetime.now(timezone.utc)
        start = now.replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff = start - timedelta(days=days)

    property_types = [str(v) for v in _collect_eq_in(spec, "property_type")]
    regions = _geo_names(spec, "region")
    settlements = _geo_names(spec, "settlement")
    if not regions:
        extra_regions = []
        for it in _walk_items(spec.get("items") or []):
            if str(it.get("type") or "").lower() != "geo":
                continue
            if str(it.get("geo_type") or "") != "settlement":
                continue
            r = str(it.get("geoRegion") or it.get("region") or "").strip()
            if r:
                extra_regions.append(r)
        regions = extra_regions
    has_geo = bool(regions or settlements or _geo_names(spec, "city_district"))

    pmin, pmax = _range_bounds(spec, "price_uah")
    if pmin is None and pmax is None:
        pmin, pmax = _range_bounds(spec, "price_usd")
    bmin, bmax = _range_bounds(spec, "building_area_sqm")
    lmin, lmax = _range_bounds(spec, "land_area_sotky")
    query_text = _text_contains(spec, "title") or _text_contains(spec, "description")

    plan = SourceQueryPlan(
        sources=sources,
        deal_types=dts,
        depth_days=days,
        cutoff_utc=cutoff,
        regions=regions,
        settlements=settlements,
        listing_type_patterns=_listing_type_patterns(property_types),
        price_uah_min=pmin,
        price_uah_max=pmax,
        building_area_min=bmin,
        building_area_max=bmax,
        land_area_sotky_min=lmin,
        land_area_sotky_max=lmax,
        query_text=query_text,
        has_geo=has_geo,
    )
    plan.extra_olx_query_pairs = _olx_extra_pairs(plan)
    return plan


def is_broad_plan(plan: SourceQueryPlan) -> bool:
    """Nationwide unlimited без області/НП — потребує підтвердження."""
    return (not plan.regions and not plan.settlements) and plan.depth_days is None
