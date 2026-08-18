# -*- coding: utf-8 -*-
"""Резолвер критеріїв населення/площі НП для геопошуку оголошень."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Optional

from bson import ObjectId
from bson.errors import InvalidId

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


def _criteria_has_values(criteria: SettlementCriteria) -> bool:
    return any(
        [
            criteria.region,
            criteria.settlement,
            criteria.population_gte,
            criteria.population_lte,
            criteria.population_gt,
            criteria.population_lt,
            criteria.area_gte,
            criteria.area_lte,
            criteria.area_gt,
            criteria.area_lt,
        ]
    )


def _strip_settlement_fields_from_group(
    group: FilterGroup,
    current: SettlementCriteria,
) -> Optional[FilterGroup]:
    """Рекурсивно прибирає логічні поля НП; накопичує критерії в current."""
    kept: List[Any] = []
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
        elif isinstance(item, FilterGroup):
            sub = _strip_settlement_fields_from_group(item, current)
            if sub is not None and sub.items:
                kept.append(sub)
    if not kept:
        return None
    return FilterGroup(group_type=group.group_type, items=kept)


def extract_settlement_criteria_from_group(
    group: Optional[FilterGroup],
) -> tuple[Optional[FilterGroup], List[SettlementCriteria]]:
    """Витягує settlement_population / settlement_area_sq_km з FilterGroup (включно з вкладеними групами)."""
    if not group or not group.items:
        return group, []

    current = SettlementCriteria()
    new_group = _strip_settlement_fields_from_group(group, current)
    criteria_list: List[SettlementCriteria] = []
    if _criteria_has_values(current):
        criteria_list.append(current)
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
    regions = extract_regions_from_geo_filter(geo_filter)
    return regions[0] if regions else None


def extract_regions_from_geo_filter(geo_filter: Optional[GeoFilter]) -> List[str]:
    """Усі області з geo (включно з OR-групи)."""
    if not geo_filter or not geo_filter.root:
        return []

    found: List[str] = []

    def walk(node) -> None:
        if isinstance(node, GeoFilterElement):
            if node.geo_type == "region" and node.value:
                val = str(node.value).strip()
                if val and val not in found:
                    found.append(val)
            elif node.geo_type == "settlement" and getattr(node, "region", None):
                val = str(node.region).strip()
                if val and val not in found:
                    found.append(val)
        elif isinstance(node, GeoFilterGroup):
            for it in node.items:
                walk(it)

    walk(geo_filter.root)
    return found


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

    def _find_city_for_geo_element(self, elem: GeoFilterElement) -> Optional[Dict[str, Any]]:
        """Документ НП з каталогу для явного geo-вузла (city_id або назва+область)."""
        city_id = (getattr(elem, "city_id", None) or "").strip() if getattr(elem, "city_id", None) else None
        if city_id:
            try:
                doc = self.cities_repo.find_by_id(city_id)
                if doc:
                    return doc
            except Exception:
                pass
            try:
                doc = self.cities_repo.find_by_id(ObjectId(city_id))
                if doc:
                    return doc
            except (InvalidId, Exception):
                pass
        name = (elem.value or "").strip() if isinstance(elem.value, str) else ""
        if not name:
            return None
        region_id = self._region_id(getattr(elem, "region", None))
        if region_id:
            return self.cities_repo.find_by_name_and_region(name, region_id)
        return self.cities_repo.find_one({"name_normalized": self.cities_repo._normalize_name(name)})

    @staticmethod
    def _city_satisfies_criteria(city: Optional[Dict[str, Any]], criteria: SettlementCriteria) -> bool:
        """
        Перевіряє демографічні критерії для конкретного НП.
        Якщо population/area у каталозі відсутні — не відсікаємо (дані ще не заповнені).
        """
        if not city:
            return False
        if criteria.settlement:
            key = (city.get("name") or "").strip().casefold()
            want = criteria.settlement.strip().casefold()
            if key != want:
                return False
        pop = city.get("population")
        if pop is not None:
            try:
                pv = int(pop)
            except (TypeError, ValueError):
                pv = None
            if pv is not None:
                if criteria.population_gte is not None and pv < criteria.population_gte:
                    return False
                if criteria.population_lte is not None and pv > criteria.population_lte:
                    return False
                if criteria.population_gt is not None and pv <= criteria.population_gt:
                    return False
                if criteria.population_lt is not None and pv >= criteria.population_lt:
                    return False
        area = city.get("area_sq_km")
        if area is not None:
            try:
                av = float(area)
            except (TypeError, ValueError):
                av = None
            if av is not None:
                if criteria.area_gte is not None and av < criteria.area_gte:
                    return False
                if criteria.area_lte is not None and av > criteria.area_lte:
                    return False
                if criteria.area_gt is not None and av <= criteria.area_gt:
                    return False
                if criteria.area_lt is not None and av >= criteria.area_lt:
                    return False
        return True

    @staticmethod
    def _geo_root_is_region_only(geo_filter: GeoFilter) -> bool:
        root = geo_filter.root
        return isinstance(root, GeoFilterElement) and root.geo_type == "region"

    def _iter_settlement_geo_elements(self, node: Any) -> List[GeoFilterElement]:
        out: List[GeoFilterElement] = []
        if isinstance(node, GeoFilterElement):
            if node.geo_type == "settlement" and node.value and str(node.value) != "__NO_MATCH__":
                out.append(node)
        elif isinstance(node, GeoFilterGroup):
            for it in node.items:
                out.extend(self._iter_settlement_geo_elements(it))
        return out

    def _base_geo_satisfies_criteria_list(
        self,
        base_geo: GeoFilter,
        criteria_list: List[SettlementCriteria],
    ) -> bool:
        """Явний geo('НП' …) + критерії населення: перевірка по каталогу, не розгортання по області."""
        settlements = self._iter_settlement_geo_elements(base_geo.root)
        if not settlements or not criteria_list:
            return False
        for elem in settlements:
            city = self._find_city_for_geo_element(elem)
            for raw in criteria_list:
                crit = SettlementCriteria(
                    region=raw.region or getattr(elem, "region", None),
                    settlement=raw.settlement,
                    population_gte=raw.population_gte,
                    population_lte=raw.population_lte,
                    population_gt=raw.population_gt,
                    population_lt=raw.population_lt,
                    area_gte=raw.area_gte,
                    area_lte=raw.area_lte,
                    area_gt=raw.area_gt,
                    area_lt=raw.area_lt,
                )
                if not self._city_satisfies_criteria(city, crit):
                    return False
        return True

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
            if base_geo and self._base_geo_satisfies_criteria_list(base_geo, criteria_list):
                return base_geo
            no_match = GeoFilterElement(
                operator=GeoFilterOperator.INSIDE,
                geo_type="settlement",
                value="__NO_MATCH__",
            )
            if base_geo and self._geo_root_is_region_only(base_geo):
                return GeoFilter(
                    root=GeoFilterGroup(
                        group_type=FilterGroupType.AND,
                        items=[base_geo.root, no_match],  # type: ignore[list-item]
                    )
                )
            return GeoFilter(root=no_match)
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

    regions_from_geo = extract_regions_from_geo_filter(geo_filter)
    if regions_from_geo:
        expanded: List[SettlementCriteria] = []
        for c in criteria_list:
            if c.region:
                expanded.append(c)
                continue
            for region_name in regions_from_geo:
                expanded.append(
                    SettlementCriteria(
                        region=region_name,
                        settlement=c.settlement,
                        population_gte=c.population_gte,
                        population_lte=c.population_lte,
                        population_gt=c.population_gt,
                        population_lt=c.population_lt,
                        area_gte=c.area_gte,
                        area_lte=c.area_lte,
                        area_gt=c.area_gt,
                        area_lt=c.area_lt,
                    )
                )
        criteria_list = expanded

    resolver = SettlementCriteriaResolver()
    merged_geo = resolver.criteria_to_geo_filter(criteria_list, base_geo=geo_filter)
    return new_group, merged_geo
