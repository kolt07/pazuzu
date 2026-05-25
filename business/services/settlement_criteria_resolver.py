# -*- coding: utf-8 -*-
"""Резолвер критеріїв населення/площі НП для геопошуку оголошень."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from data.repositories.geography_repository import CitiesRepository, RegionsRepository
from domain.models.filter_models import (
    FilterElement,
    FilterGroup,
    FilterGroupType,
    FilterOperator,
    GeoFilter,
    GeoFilterElement,
    GeoFilterGroup,
    GeoFilterOperator,
)
from utils.settlement_geo_match import get_settlement_match_names
from utils.ukraine_regions import normalize_region_for_repository_lookup


SETTLEMENT_POPULATION_FIELD = "settlement_population"
SETTLEMENT_AREA_FIELD = "settlement_area_sq_km"
SETTLEMENT_REGION_CONTEXT_FIELD = "settlement_region_context"
SETTLEMENT_NAME_CONTEXT_FIELD = "settlement_name_context"


@dataclass
class SettlementCriteria:
    region: Optional[str] = None
    settlement: Optional[str] = None
    population_gte: Optional[int] = None
    population_lte: Optional[int] = None
    population_gt: Optional[int] = None
    population_lt: Optional[int] = None
    area_gte: Optional[float] = None
    area_lte: Optional[float] = None
    area_gt: Optional[float] = None
    area_lt: Optional[float] = None


def _num_value(val: Any) -> Optional[float]:
    if val is None:
        return None
    try:
        return float(val)
    except (TypeError, ValueError):
        return None


def _apply_pop_op(criteria: SettlementCriteria, op: FilterOperator, value: Any) -> None:
    n = _num_value(value)
    if n is None:
        return
    iv = int(n)
    if op == FilterOperator.GTE:
        criteria.population_gte = iv
    elif op == FilterOperator.LTE:
        criteria.population_lte = iv
    elif op == FilterOperator.GT:
        criteria.population_gt = iv
    elif op == FilterOperator.LT:
        criteria.population_lt = iv
    elif op == FilterOperator.EQ:
        criteria.population_gte = iv
        criteria.population_lte = iv


def _apply_area_op(criteria: SettlementCriteria, op: FilterOperator, value: Any) -> None:
    n = _num_value(value)
    if n is None:
        return
    if op == FilterOperator.GTE:
        criteria.area_gte = n
    elif op == FilterOperator.LTE:
        criteria.area_lte = n
    elif op == FilterOperator.GT:
        criteria.area_gt = n
    elif op == FilterOperator.LT:
        criteria.area_lt = n
    elif op == FilterOperator.EQ:
        criteria.area_gte = n
        criteria.area_lte = n


def extract_settlement_criteria_from_group(
    group: Optional[FilterGroup],
) -> tuple[Optional[FilterGroup], List[SettlementCriteria]]:
    """Витягує settlement_population / settlement_area_sq_km з FilterGroup."""
    if not group or not group.items:
        return group, []

    kept: List[Any] = []
    criteria_list: List[SettlementCriteria] = []
    current = SettlementCriteria()

    for item in group.items:
        if isinstance(item, FilterElement):
            if item.field == SETTLEMENT_REGION_CONTEXT_FIELD:
                if isinstance(item.value, str) and item.value.strip():
                    current.region = item.value.strip()
                continue
            if item.field == SETTLEMENT_NAME_CONTEXT_FIELD:
                if isinstance(item.value, str) and item.value.strip():
                    current.settlement = item.value.strip()
                continue
            if item.field == SETTLEMENT_POPULATION_FIELD:
                _apply_pop_op(current, item.operator, item.value)
                continue
            if item.field == SETTLEMENT_AREA_FIELD:
                _apply_area_op(current, item.operator, item.value)
                continue
        kept.append(item)

    if any(
        [
            current.population_gte,
            current.population_lte,
            current.population_gt,
            current.population_lt,
            current.area_gte,
            current.area_lte,
            current.area_gt,
            current.area_lt,
        ]
    ):
        criteria_list.append(current)

    new_group = FilterGroup(group_type=group.group_type, items=kept) if kept else None
    return new_group, criteria_list


def extract_settlement_criteria_from_geo_tree(geo_items: List[Dict[str, Any]]) -> List[SettlementCriteria]:
    """Критерії з geo-вузлів дерева (population_min, geoRegion, …)."""
    out: List[SettlementCriteria] = []
    for it in geo_items or []:
        if str(it.get("geo_type") or "") != "settlement":
            continue
        c = SettlementCriteria(
            region=(it.get("geoRegion") or "").strip() or None,
            settlement=(it.get("value") or "").strip() or None,
        )
        if it.get("population_min") is not None:
            c.population_gte = int(it["population_min"])
        if it.get("population_max") is not None:
            c.population_lte = int(it["population_max"])
        if it.get("area_min") is not None:
            c.area_gte = float(it["area_min"])
        if it.get("area_max") is not None:
            c.area_lte = float(it["area_max"])
        if any(
            [
                c.population_gte,
                c.population_lte,
                c.population_gt,
                c.population_lt,
                c.area_gte,
                c.area_lte,
                c.area_gt,
                c.area_lt,
                c.settlement,
            ]
        ):
            out.append(c)
    return out


def extract_region_from_geo_filter(geo_filter: Optional[GeoFilter]) -> Optional[str]:
    if not geo_filter or not geo_filter.root:
        return None

    def walk(node) -> Optional[str]:
        if isinstance(node, GeoFilterElement):
            if node.geo_type == "region" and node.value:
                return str(node.value)
            if node.geo_type == "settlement" and getattr(node, "region", None):
                return str(node.region)
            return None
        if isinstance(node, GeoFilterGroup):
            for it in node.items:
                r = walk(it)
                if r:
                    return r
        return None

    return walk(geo_filter.root)


class SettlementCriteriaResolver:
    def __init__(self):
        self.cities_repo = CitiesRepository()
        self.regions_repo = RegionsRepository()

    def _region_id(self, region_name: Optional[str]) -> Optional[str]:
        if not region_name:
            return None
        lookup = normalize_region_for_repository_lookup(region_name) or region_name
        reg = self.regions_repo.find_by_name(lookup)
        return str(reg["_id"]) if reg else None

    def resolve_city_names(self, criteria: SettlementCriteria) -> List[str]:
        region_id = self._region_id(criteria.region)
        cities = self.cities_repo.find_matching_criteria(
            region_id,
            population_gte=criteria.population_gte,
            population_lte=criteria.population_lte,
            population_gt=criteria.population_gt,
            population_lt=criteria.population_lt,
            area_gte=criteria.area_gte,
            area_lte=criteria.area_lte,
            area_gt=criteria.area_gt,
            area_lt=criteria.area_lt,
            settlement_name=criteria.settlement,
        )
        names: List[str] = []
        seen: set[tuple[str, str]] = set()
        for c in cities:
            n = (c.get("name") or "").strip()
            rid = str(c.get("region_id") or region_id or "")
            if not n:
                continue
            key = (rid, n.casefold())
            if key in seen:
                continue
            seen.add(key)
            names.append(n)
        return names

    def criteria_to_geo_filter(
        self,
        criteria_list: List[SettlementCriteria],
        *,
        base_geo: Optional[GeoFilter] = None,
    ) -> Optional[GeoFilter]:
        if not criteria_list:
            return base_geo

        elems: List[GeoFilterElement] = []
        for c in criteria_list:
            names = self.resolve_city_names(c)
            if not names:
                continue
            for name in names:
                elems.append(
                    GeoFilterElement(
                        operator=GeoFilterOperator.INSIDE,
                        geo_type="settlement",
                        value=name,
                        region=c.region,
                    )
                )

        if not elems:
            return GeoFilter(
                root=GeoFilterElement(
                    operator=GeoFilterOperator.INSIDE,
                    geo_type="settlement",
                    value="__NO_MATCH__",
                )
            )
        pop_geo = (
            GeoFilter(root=elems[0])
            if len(elems) == 1
            else GeoFilter(root=GeoFilterGroup(group_type=FilterGroupType.OR, items=elems))
        )

        if not base_geo:
            return pop_geo

        return GeoFilter(
            root=GeoFilterGroup(
                group_type=FilterGroupType.AND,
                items=[base_geo.root, pop_geo.root],  # type: ignore[list-item]
            )
        )

    def resolve_settlement_display_name(
        self,
        name: str,
        region: Optional[str] = None,
    ) -> str:
        region_id = self._region_id(region)
        if not region_id:
            return name
        found = self.cities_repo.find_by_name_and_region(name, region_id)
        return (found.get("name") or name) if found else name


def resolve_geo_filter_settlement_aliases(geo_filter: Optional[GeoFilter]) -> Optional[GeoFilter]:
    """Підставляє канонічні назви НП (колишні назви → поточна)."""
    if not geo_filter or not geo_filter.root:
        return geo_filter

    resolver = SettlementCriteriaResolver()
    region_hint = extract_region_from_geo_filter(geo_filter)

    def walk(node):
        if isinstance(node, GeoFilterElement):
            if node.geo_type == "settlement" and node.value and str(node.value) != "__NO_MATCH__":
                region_ctx = getattr(node, "region", None) or region_hint
                node.value = resolver.resolve_settlement_display_name(
                    str(node.value),
                    region_ctx,
                )
            return node
        if isinstance(node, GeoFilterGroup):
            for i, it in enumerate(node.items):
                node.items[i] = walk(it)
            return node
        return node

    walk(geo_filter.root)
    return geo_filter


def strip_and_apply_settlement_criteria(
    filter_group: Optional[FilterGroup],
    geo_filter: Optional[GeoFilter],
) -> tuple[Optional[FilterGroup], Optional[GeoFilter]]:
    """Видаляє логічні поля НП з групи та доповнює geo_filter."""
    geo_filter = resolve_geo_filter_settlement_aliases(geo_filter)
    new_group, criteria_list = extract_settlement_criteria_from_group(filter_group)
    if not criteria_list:
        return new_group, geo_filter

    region_from_geo = extract_region_from_geo_filter(geo_filter)
    for c in criteria_list:
        if not c.region and region_from_geo:
            c.region = region_from_geo

    resolver = SettlementCriteriaResolver()
    merged_geo = resolver.criteria_to_geo_filter(criteria_list, base_geo=geo_filter)
    return new_group, merged_geo
