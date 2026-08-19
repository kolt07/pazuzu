# -*- coding: utf-8 -*-
"""
Детермінована аналітична довідка дослідження ринку (без LLM).
Квартилі / IQR як у PriceAnalyticsService; sale і rent ніколи не змішуються.
"""

from __future__ import annotations

from collections import Counter, defaultdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from business.services.price_analytics_service import (
    MIN_SETTLEMENT_COUNT,
    OUTLIER_IQR_MULTIPLIER,
    PriceAnalyticsService,
    _quartiles,
    _std_dev,
)
from data.repositories.price_analytics_repository import (
    LISTING_TYPE_LAND,
    LISTING_TYPE_MIXED,
    LISTING_TYPE_REAL_ESTATE,
)
from utils.deal_type import DEAL_RENT, DEAL_SALE

AREA_BUCKETS = (
    (0, 100, "<100"),
    (100, 300, "100–300"),
    (300, 1000, "300–1000"),
    (1000, None, ">1000"),
)
MIN_DISTRIBUTION_N = 5


def _pos_float(v: Any) -> Optional[float]:
    try:
        n = float(v)
    except (TypeError, ValueError):
        return None
    return n if n > 0 else None


def _dt_iso(v: Any) -> Optional[str]:
    if isinstance(v, datetime):
        return v.strftime("%Y-%m-%d")
    if isinstance(v, str) and len(v) >= 10:
        return v[:10]
    return None


def _numeric_stats(values: List[float]) -> Dict[str, Any]:
    clean = [v for v in values if v is not None and v > 0]
    n = len(clean)
    if n == 0:
        return {"n": 0}
    mn, mx = min(clean), max(clean)
    if n < MIN_DISTRIBUTION_N:
        return {"n": n, "min": round(mn, 2), "max": round(mx, 2)}
    q1, q2, q3, q4 = _quartiles(clean)
    mean = sum(clean) / n
    std = _std_dev(clean)
    iqr = q3 - q1
    outliers = 0
    if iqr > 0:
        outliers = sum(1 for v in clean if v < q1 - OUTLIER_IQR_MULTIPLIER * iqr or v > q3 + OUTLIER_IQR_MULTIPLIER * iqr)
    return {
        "n": n,
        "min": round(mn, 2),
        "p25": round(q1, 2),
        "median": round(q2, 2),
        "p75": round(q3, 2),
        "max": round(mx, 2),
        "mean": round(mean, 2),
        "std": round(std, 2),
        "iqr_outliers": outliers,
    }


def _listing_kind(doc: Dict[str, Any]) -> str:
    land = _pos_float(doc.get("land_area_sqm")) or 0
    bld = _pos_float(doc.get("building_area_sqm")) or 0
    if land > 0 and bld > 0:
        return "mixed"
    if land > 0:
        return "land"
    if bld > 0:
        return "real_estate"
    return "other"


def _bucket_label(value: Optional[float], land: bool) -> Optional[str]:
    if value is None or value <= 0:
        return None
    # земля: сотки = м²/100
    v = value / 100.0 if land else value
    for lo, hi, label in AREA_BUCKETS:
        if v < lo:
            continue
        if hi is None or v < hi:
            unit = "с" if land else "м²"
            return f"{label} {unit}"
    return None


def _price_per_sotka(doc: Dict[str, Any]) -> Optional[float]:
    ha = _pos_float(doc.get("price_per_ha_uah"))
    if ha is None:
        return None
    return ha / 100.0


class MarketResearchAnalyticsService:
    """Будує JSON-довідку по вибірці unified_listings."""

    def build(
        self,
        docs: List[Dict[str, Any]],
        *,
        filter_summary: str = "",
        deal_types: Optional[List[str]] = None,
        depth_days: Optional[int] = None,
        limitations: Optional[List[str]] = None,
    ) -> Dict[str, Any]:
        deal_types = list(deal_types or [DEAL_SALE])
        by_deal: Dict[str, List[Dict[str, Any]]] = {DEAL_SALE: [], DEAL_RENT: []}
        sources = Counter()
        property_types = Counter()
        regions = Counter()
        cities = Counter()
        statuses = Counter()
        dates: List[str] = []
        for d in docs:
            dt = (d.get("deal_type") or DEAL_SALE)
            if dt not in by_deal:
                dt = DEAL_SALE
            by_deal[dt].append(d)
            sources[str(d.get("source") or "")] += 1
            property_types[str(d.get("property_type") or "")] += 1
            if d.get("region"):
                regions[str(d.get("region"))] += 1
            if d.get("city"):
                cities[str(d.get("city"))] += 1
            statuses[str(d.get("status") or "")] += 1
            iso = _dt_iso(d.get("source_updated_at"))
            if iso:
                dates.append(iso)

        date_min = min(dates) if dates else None
        date_max = max(dates) if dates else None

        price_blocks = []
        for deal in deal_types:
            subset = by_deal.get(deal) or []
            if not subset:
                continue
            kinds: Dict[str, List[Dict[str, Any]]] = defaultdict(list)
            for d in subset:
                kinds[_listing_kind(d)].append(d)
            kind_stats = {}
            for kind, group in kinds.items():
                kind_stats[kind] = {
                    "price_uah": _numeric_stats([_pos_float(x.get("price_uah")) or 0 for x in group]),
                    "price_per_m2_uah": _numeric_stats([
                        _pos_float(x.get("price_per_m2_uah")) or 0
                        for x in group
                        if (_pos_float(x.get("building_area_sqm")) or 0) > 0
                    ]),
                    "price_per_sotka_uah": _numeric_stats([
                        _price_per_sotka(x) or 0
                        for x in group
                        if (_pos_float(x.get("land_area_sqm")) or 0) > 0
                    ]),
                }
            price_blocks.append({
                "deal_type": deal,
                "n": len(subset),
                "by_kind": kind_stats,
            })

        region_medians = self._group_median_price(docs, "region")
        area_buckets = self._area_bucket_medians(docs)
        tag_medians = self._tag_medians(docs)
        comparisons = self._indicator_comparisons(docs, deal_types)

        lims = list(limitations or [])
        if any((b.get("n") or 0) < MIN_DISTRIBUTION_N for b in price_blocks for _k, st in (b.get("by_kind") or {}).items()):
            pass
        small = sum(1 for d in docs if True) < MIN_DISTRIBUTION_N
        if small:
            lims.append("Вибірка менша за 5 оголошень — повний розподіл не рахується.")
        if DEAL_RENT in deal_types:
            lims.append("Оренда й продаж у розподілі цін розділені; індикатор ринку (price_analytics) — для продажу.")
        lims.append("OLX обмежує видачу 25 сторінками на пару категорія×область.")

        return {
            "passport": {
                "filter_summary": filter_summary,
                "deal_types": deal_types,
                "depth_days": depth_days,
                "n": len(docs),
                "sources": dict(sources),
                "date_from": date_min,
                "date_to": date_max,
            },
            "composition": {
                "property_type": dict(property_types.most_common()),
                "region": dict(regions.most_common(15)),
                "city_top10": dict(cities.most_common(10)),
                "status": dict(statuses),
            },
            "price_distribution": price_blocks,
            "slices": {
                "region_median_price_uah": region_medians,
                "area_buckets": area_buckets,
                "tags_top": tag_medians,
            },
            "market_comparison": comparisons,
            "limitations": lims,
        }

    def _group_median_price(self, docs: List[Dict[str, Any]], field: str) -> List[Dict[str, Any]]:
        groups: Dict[str, List[float]] = defaultdict(list)
        for d in docs:
            key = str(d.get(field) or "").strip()
            price = _pos_float(d.get("price_uah"))
            if key and price:
                groups[key].append(price)
        rows = []
        for key, vals in groups.items():
            st = _numeric_stats(vals)
            rows.append({"name": key, **st})
        rows.sort(key=lambda r: r.get("n") or 0, reverse=True)
        return rows[:15]

    def _area_bucket_medians(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        buckets: Dict[str, List[float]] = defaultdict(list)
        for d in docs:
            kind = _listing_kind(d)
            price = _pos_float(d.get("price_uah"))
            if not price:
                continue
            if kind == "land":
                label = _bucket_label(_pos_float(d.get("land_area_sqm")), land=True)
            else:
                label = _bucket_label(_pos_float(d.get("building_area_sqm")), land=False)
            if label:
                buckets[label].append(price)
        rows = []
        for label, vals in buckets.items():
            st = _numeric_stats(vals)
            rows.append({"bucket": label, **st})
        rows.sort(key=lambda r: r.get("bucket") or "")
        return rows

    def _tag_medians(self, docs: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        tag_prices: Dict[str, List[float]] = defaultdict(list)
        for d in docs:
            tags = d.get("tags") or []
            price = _pos_float(d.get("price_uah"))
            if not price or not isinstance(tags, list):
                continue
            for t in tags:
                name = str(t or "").strip().lower()
                if name:
                    tag_prices[name].append(price)
        rows = []
        for tag, vals in tag_prices.items():
            if len(vals) < 2:
                continue
            st = _numeric_stats(vals)
            rows.append({"tag": tag, **st})
        rows.sort(key=lambda r: r.get("n") or 0, reverse=True)
        return rows[:15]

    def _indicator_comparisons(
        self,
        docs: List[Dict[str, Any]],
        deal_types: List[str],
    ) -> List[Dict[str, Any]]:
        if DEAL_SALE not in deal_types:
            return []
        sale_docs = [d for d in docs if (d.get("deal_type") or DEAL_SALE) == DEAL_SALE]
        if not sale_docs:
            return []
        svc = PriceAnalyticsService()
        by_city: Dict[Tuple[str, str], List[float]] = defaultdict(list)
        for d in sale_docs:
            city = str(d.get("city") or "").strip()
            region = str(d.get("region") or "").strip()
            m2 = _pos_float(d.get("price_per_m2_uah"))
            sot = _price_per_sotka(d)
            kind = _listing_kind(d)
            metric_val = sot if kind == "land" else m2
            if city and metric_val:
                by_city[(city, region)].append(metric_val)
        rows = []
        for (city, region), vals in by_city.items():
            if len(vals) < 2:
                continue
            sample_median = _numeric_stats(vals).get("median")
            if sample_median is None:
                continue
            kind_counts = Counter(_listing_kind(d) for d in sale_docs if d.get("city") == city)
            listing_type = LISTING_TYPE_REAL_ESTATE
            if kind_counts.get("land", 0) >= kind_counts.get("real_estate", 0) and kind_counts.get("land", 0) >= kind_counts.get("mixed", 0):
                listing_type = LISTING_TYPE_LAND
            elif kind_counts.get("mixed", 0) > kind_counts.get("real_estate", 0):
                listing_type = LISTING_TYPE_MIXED
            metric = "price_per_ha_uah" if listing_type == LISTING_TYPE_LAND else "price_per_m2_uah"
            ind = svc.repo.get_indicator(city, metric, region, listing_type)
            if not ind or (ind.get("count") or 0) < MIN_SETTLEMENT_COUNT:
                ind = svc.repo.get_region_indicator(region, metric, listing_type) if region else None
                scope = "region"
            else:
                scope = "city"
            q2 = (ind or {}).get("q2")
            if not q2:
                continue
            market = float(q2)
            if listing_type == LISTING_TYPE_LAND:
                market = market / 100.0
            delta_pct = ((sample_median - market) / market * 100.0) if market else None
            rows.append({
                "city": city,
                "region": region,
                "scope": scope,
                "sample_n": len(vals),
                "sample_median": sample_median,
                "market_median": round(market, 2),
                "delta_pct": round(delta_pct, 1) if delta_pct is not None else None,
            })
        rows.sort(key=lambda r: abs(r.get("delta_pct") or 0), reverse=True)
        return rows[:12]
