# -*- coding: utf-8 -*-
"""
UnifiedSearchService: централізований пошук по unified_listings.
Будує FindQuery з дерева фільтрів (FilterGroup + GeoFilter) або з рядка фільтрів,
виконує пошук через CollectionManager, повертає список документів та total.
Використовується на сторінці пошуку та при формуванні звітів.
"""

from typing import Any, Dict, List, Optional, Tuple, Union

from domain.models.filter_models import FilterGroup, FindQuery, GeoFilter
from domain.models.filter_models import FilterElement, FilterGroupType, FilterOperator
from domain.services.filter_spec_service import (
    enrich_filter_spec_geo_ids,
    filter_spec_to_models,
    normalize_filter_spec,
)
from utils.source_field_mapper import SourceFieldMapper

COLLECTION = "unified_listings"
DEFAULT_SORT_FIELD = "source_updated_at"
DEFAULT_SORT_ORDER = -1
DEFAULT_STATUS_ACTIVE = "активне"
DEFAULT_DEAL_TYPE_SALE = "sale"


def find(
    filter_group: Optional[FilterGroup] = None,
    geo_filter: Optional[GeoFilter] = None,
    sort: Optional[List[Dict[str, Any]]] = None,
    limit: int = 50,
    skip: int = 0,
    fields: Optional[List[str]] = None,
    default_status_active: bool = True,
    default_deal_type_sale: bool = True,
) -> Tuple[List[Dict[str, Any]], int]:
    """
    Виконує пошук по unified_listings за деревом фільтрів та геофільтром.
    
    Returns:
        (list of documents, total count)
    """
    from domain.managers.collection_manager import UnifiedListingsCollectionManager

    mgr = UnifiedListingsCollectionManager()
    if sort is None:
        sort = [{"field": DEFAULT_SORT_FIELD, "order": DEFAULT_SORT_ORDER}]
    if sort and sort[0].get("field") == "source_updated_at" and len(sort) == 1:
        sort = sort + [{"field": "system_updated_at", "order": sort[0].get("order", DEFAULT_SORT_ORDER)}]
    from business.services.settlement_criteria_resolver import strip_and_apply_settlement_criteria

    filter_group, geo_filter = strip_and_apply_settlement_criteria(filter_group, geo_filter)

    if default_status_active:
        filter_group = _with_default_status(filter_group, COLLECTION)
    if default_deal_type_sale:
        filter_group = _with_default_deal_type(filter_group, COLLECTION)
    query = FindQuery(
        filters=filter_group,
        geo_filters=geo_filter,
        sort=sort,
        limit=limit,
        skip=skip,
        fields=fields,
        collection=COLLECTION,
    )
    df = mgr.find(query)
    total = mgr.get_count(query) if (filter_group or geo_filter) else mgr.get_total_count()
    if total is None:
        total = len(df)
    data = df.to_dict(orient="records") if not df.empty else []
    return data, total


def _contradictory_source_eq_message(filter_group: Optional[FilterGroup]) -> Optional[str]:
    """Повідомлення, якщо в рядку одночасно source=olx AND source=prozorro (неможливо)."""
    if not filter_group:
        return None
    values: List[str] = []

    def walk(group: FilterGroup) -> None:
        for item in group.items:
            if isinstance(item, FilterElement):
                if item.field == "source" and item.operator == FilterOperator.EQ:
                    v = item.value
                    if v is not None and str(v).strip():
                        values.append(str(v).strip().lower())
            elif isinstance(item, FilterGroup):
                walk(item)

    walk(filter_group)
    unique = set(values)
    if len(unique) > 1:
        return (
            "Суперечливі умови по полю «Джерело» в рядку відборів "
            f"({', '.join(sorted(unique))}): одне оголошення не може бути в двох джерелах одночасно. "
            "Приберіть «Джерело» з рядка і оберіть OLX + ProZorro у полі «Джерело даних» шаблону."
        )
    return None


def _merge_date_and_source(
    filter_group: Optional[FilterGroup],
    date_filter_days: Optional[int] = None,
    source: Optional[str] = None,
) -> Optional[FilterGroup]:
    from datetime import datetime, timedelta, timezone

    phys = lambda f: SourceFieldMapper.get_field_path(f, COLLECTION)
    extra_elems: List[FilterElement] = []
    if date_filter_days is not None and int(date_filter_days) > 0:
        since = (datetime.now(timezone.utc) - timedelta(days=int(date_filter_days))).isoformat()
        extra_elems.append(
            FilterElement(field=phys("source_updated_at"), operator=FilterOperator.GTE, value=since)
        )
    src = (source or "").strip() if isinstance(source, str) else ""
    if src:
        extra_elems.append(FilterElement(field=phys("source"), operator=FilterOperator.EQ, value=src))

    if not extra_elems:
        return filter_group
    merged: List[Union[FilterElement, FilterGroup]] = []
    if filter_group is not None:
        merged.append(filter_group)
    merged.extend(extra_elems)
    return FilterGroup(group_type=FilterGroupType.AND, items=merged)


def find_by_filter_spec(
    filter_spec: Optional[Dict[str, Any]] = None,
    sort: Optional[List[Dict[str, Any]]] = None,
    limit: int = 50,
    skip: int = 0,
    date_filter_days: Optional[int] = None,
    source: Optional[str] = None,
    default_status_active: bool = True,
    default_deal_type_sale: bool = True,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[int], Optional[str]]:
    """
    Пошук за канонічним FilterSpec (JSON з ключами полів та geo id).
    Returns: (documents, total, error).
    """
    spec = normalize_filter_spec(filter_spec)
    if spec.get("items"):
        spec = enrich_filter_spec_geo_ids(spec)

    group, geo, err = filter_spec_to_models(spec)
    if err:
        return None, None, err

    src_err = _contradictory_source_eq_message(group)
    if src_err:
        return None, None, src_err

    final_group = _merge_date_and_source(group, date_filter_days, source)
    data, total = find(
        filter_group=final_group,
        geo_filter=geo,
        sort=sort,
        limit=limit,
        skip=skip,
        default_status_active=default_status_active,
        default_deal_type_sale=default_deal_type_sale,
    )
    return data, total, None


def find_by_filter_string(
    filter_string: str,
    sort: Optional[List[Dict[str, Any]]] = None,
    limit: int = 50,
    skip: int = 0,
    date_filter_days: Optional[int] = None,
    source: Optional[str] = None,
    default_status_active: bool = True,
    default_deal_type_sale: bool = True,
) -> Tuple[Optional[List[Dict[str, Any]]], Optional[int], Optional[str]]:
    """
    Legacy: парсить рядок фільтрів і виконує пошук.
    Для нових шляхів використовуйте find_by_filter_spec.
    """
    from domain.services.filter_spec_service import filter_string_to_filter_spec

    text = (filter_string or "").strip()
    if not text:
        return find_by_filter_spec(
            None,
            sort=sort,
            limit=limit,
            skip=skip,
            date_filter_days=date_filter_days,
            source=source,
            default_status_active=default_status_active,
            default_deal_type_sale=default_deal_type_sale,
        )
    spec, err = filter_string_to_filter_spec(text, collection=COLLECTION)
    if err:
        return None, None, err
    return find_by_filter_spec(
        spec,
        sort=sort,
        limit=limit,
        skip=skip,
        date_filter_days=date_filter_days,
        source=source,
        default_status_active=default_status_active,
        default_deal_type_sale=default_deal_type_sale,
    )


def build_query_from_flat_params(
    region: Optional[str] = None,
    city: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    price_eq: Optional[float] = None,
    source: Optional[str] = None,
    property_type: Optional[str] = None,
    building_area_sqm_op: Optional[str] = None,
    building_area_sqm_value: Optional[float] = None,
    land_area_sotky_op: Optional[str] = None,
    land_area_sotky_value: Optional[float] = None,
    land_area_ha_op: Optional[str] = None,
    land_area_ha_value: Optional[float] = None,
    land_area_hectares_op: Optional[str] = None,
    land_area_hectares_value: Optional[float] = None,
    date_filter_days: Optional[int] = None,
    status: Optional[str] = None,
    title_contains: Optional[str] = None,
    description_contains: Optional[str] = None,
    price_per_m2_min: Optional[float] = None,
    price_per_m2_max: Optional[float] = None,
    price_per_ha_min: Optional[float] = None,
    price_per_ha_max: Optional[float] = None,
    **kwargs: Any,
) -> FindQuery:
    """
    Будує FindQuery з «плоских» параметрів (як у поточному API пошуку).
    """
    from datetime import datetime, timezone, timedelta
    from domain.services.geo_filter_service import GeoFilterService

    elements: List[FilterElement] = []
    phys = lambda f: SourceFieldMapper.get_field_path(f, COLLECTION)

    if status is not None and status != "":
        elements.append(FilterElement(field=phys("status"), operator=FilterOperator.EQ, value=status))
    elif status is None or status == "":
        elements.append(FilterElement(field=phys("status"), operator=FilterOperator.EQ, value=DEFAULT_STATUS_ACTIVE))

    if source:
        elements.append(FilterElement(field=phys("source"), operator=FilterOperator.EQ, value=source))
    if property_type:
        elements.append(FilterElement(field=phys("property_type"), operator=FilterOperator.EQ, value=property_type))

    if date_filter_days is not None and date_filter_days > 0:
        since = (datetime.now(timezone.utc) - timedelta(days=date_filter_days)).isoformat()
        elements.append(FilterElement(field=phys("source_updated_at"), operator=FilterOperator.GTE, value=since))

    if price_eq is not None:
        elements.append(FilterElement(field=phys("price_uah"), operator=FilterOperator.EQ, value=price_eq))
    else:
        if price_min is not None:
            elements.append(FilterElement(field=phys("price_uah"), operator=FilterOperator.GTE, value=price_min))
        if price_max is not None:
            elements.append(FilterElement(field=phys("price_uah"), operator=FilterOperator.LTE, value=price_max))

    if building_area_sqm_op and building_area_sqm_value is not None:
        op = {"eq": FilterOperator.EQ, "gte": FilterOperator.GTE, "lte": FilterOperator.LTE}.get(
            building_area_sqm_op, FilterOperator.EQ
        )
        elements.append(FilterElement(field=phys("building_area_sqm"), operator=op, value=building_area_sqm_value))
    if land_area_sotky_op and land_area_sotky_value is not None:
        op = {"eq": FilterOperator.EQ, "gte": FilterOperator.GTE, "lte": FilterOperator.LTE}.get(
            land_area_sotky_op, FilterOperator.EQ
        )
        elements.append(FilterElement(field=phys("land_area_sqm"), operator=op, value=float(land_area_sotky_value) * 100.0))
    # Legacy: historical API використовував land_area_ha_* для значень у сотках.
    if land_area_ha_op and land_area_ha_value is not None:
        op = {"eq": FilterOperator.EQ, "gte": FilterOperator.GTE, "lte": FilterOperator.LTE}.get(
            land_area_ha_op, FilterOperator.EQ
        )
        elements.append(FilterElement(field=phys("land_area_sqm"), operator=op, value=float(land_area_ha_value) * 100.0))
    if land_area_hectares_op and land_area_hectares_value is not None:
        op = {"eq": FilterOperator.EQ, "gte": FilterOperator.GTE, "lte": FilterOperator.LTE}.get(
            land_area_hectares_op, FilterOperator.EQ
        )
        elements.append(
            FilterElement(
                field=phys("land_area_sqm"),
                operator=op,
                value=float(land_area_hectares_value) * 10000.0,
            )
        )

    if title_contains:
        elements.append(FilterElement(field="title", operator=FilterOperator.CONTAINS, value=title_contains))
    if description_contains:
        elements.append(FilterElement(field="description", operator=FilterOperator.CONTAINS, value=description_contains))

    if price_per_m2_min is not None:
        elements.append(FilterElement(field=phys("price_per_m2_uah"), operator=FilterOperator.GTE, value=price_per_m2_min))
    if price_per_m2_max is not None:
        elements.append(FilterElement(field=phys("price_per_m2_uah"), operator=FilterOperator.LTE, value=price_per_m2_max))
    if price_per_ha_min is not None:
        elements.append(FilterElement(field=phys("price_per_ha_uah"), operator=FilterOperator.GTE, value=price_per_ha_min))
    if price_per_ha_max is not None:
        elements.append(FilterElement(field=phys("price_per_ha_uah"), operator=FilterOperator.LTE, value=price_per_ha_max))

    filter_group = FilterGroup(group_type=FilterGroupType.AND, items=elements) if elements else None
    geo_filter = None
    if region or city:
        geo_filter = GeoFilterService().from_dict({"region": region or "", "city": city or ""})
    sort_spec = [{"field": DEFAULT_SORT_FIELD, "order": DEFAULT_SORT_ORDER}]
    return FindQuery(
        filters=filter_group,
        geo_filters=geo_filter,
        sort=sort_spec,
        collection=COLLECTION,
    )


def _with_default_status(
    filter_group: Optional[FilterGroup],
    collection: str,
) -> Optional[FilterGroup]:
    """Додає умову status=активне на верхній рівень, якщо ще немає фільтра status."""
    from domain.models.filter_models import FilterElement, FilterGroupType, FilterOperator

    def has_status(gr: Optional[FilterGroup]) -> bool:
        if not gr:
            return False
        for item in gr.items:
            if isinstance(item, FilterElement) and item.field == "status":
                return True
            if isinstance(item, FilterGroup) and has_status(item):
                return True
        return False

    if has_status(filter_group):
        return filter_group
    phys_status = SourceFieldMapper.get_field_path("status", collection)
    status_elem = FilterElement(field=phys_status, operator=FilterOperator.EQ, value=DEFAULT_STATUS_ACTIVE)
    if not filter_group or not filter_group.items:
        return FilterGroup(group_type=FilterGroupType.AND, items=[status_elem])
    return FilterGroup(group_type=FilterGroupType.AND, items=[status_elem, filter_group])


def _with_default_deal_type(
    filter_group: Optional[FilterGroup],
    collection: str,
) -> Optional[FilterGroup]:
    """Додає deal_type=sale, якщо у фільтрі ще немає deal_type (щоб оренда не змішувалась із продажем)."""
    from domain.models.filter_models import FilterElement, FilterGroupType, FilterOperator

    phys = SourceFieldMapper.get_field_path("deal_type", collection)

    def has_deal_type(gr: Optional[FilterGroup]) -> bool:
        if not gr:
            return False
        for item in gr.items:
            if isinstance(item, FilterElement) and item.field in ("deal_type", phys):
                return True
            if isinstance(item, FilterGroup) and has_deal_type(item):
                return True
        return False

    if has_deal_type(filter_group):
        return filter_group
    deal_elem = FilterElement(field=phys, operator=FilterOperator.EQ, value=DEFAULT_DEAL_TYPE_SALE)
    if not filter_group or not filter_group.items:
        return FilterGroup(group_type=FilterGroupType.AND, items=[deal_elem])
    return FilterGroup(group_type=FilterGroupType.AND, items=[deal_elem, filter_group])


def get_search_fields_config(collection: str = "unified_listings") -> Dict[str, Any]:
    """Повертає конфіг полів пошуку для UI (поля, типи, оператори)."""
    from domain.services.filter_string_service import _load_search_fields_config
    return _load_search_fields_config(collection)


def filter_string_from_flat_params(
    region: Optional[str] = None,
    city: Optional[str] = None,
    price_min: Optional[float] = None,
    price_max: Optional[float] = None,
    price_eq: Optional[float] = None,
    source: Optional[str] = None,
    property_type: Optional[str] = None,
    building_area_sqm_op: Optional[str] = None,
    building_area_sqm_value: Optional[float] = None,
    land_area_sotky_op: Optional[str] = None,
    land_area_sotky_value: Optional[float] = None,
    land_area_ha_op: Optional[str] = None,
    land_area_ha_value: Optional[float] = None,
    land_area_hectares_op: Optional[str] = None,
    land_area_hectares_value: Optional[float] = None,
    date_filter_days: Optional[int] = None,
    status: Optional[str] = None,
    title_contains: Optional[str] = None,
    description_contains: Optional[str] = None,
    price_per_m2_min: Optional[float] = None,
    price_per_m2_max: Optional[float] = None,
    price_per_ha_min: Optional[float] = None,
    price_per_ha_max: Optional[float] = None,
    **kwargs: Any,
) -> str:
    """
    Генерує рядок фільтрів з плоских параметрів (для LLM та звітів).
    Повертає порожній рядок, якщо немає жодної умови.
    """
    from domain.services.filter_string_service import filter_group_to_string
    query = build_query_from_flat_params(
        region=region,
        city=city,
        price_min=price_min,
        price_max=price_max,
        price_eq=price_eq,
        source=source,
        property_type=property_type,
        building_area_sqm_op=building_area_sqm_op,
        building_area_sqm_value=building_area_sqm_value,
        land_area_sotky_op=land_area_sotky_op,
        land_area_sotky_value=land_area_sotky_value,
        land_area_ha_op=land_area_ha_op,
        land_area_ha_value=land_area_ha_value,
        land_area_hectares_op=land_area_hectares_op,
        land_area_hectares_value=land_area_hectares_value,
        date_filter_days=date_filter_days,
        status=status,
        title_contains=title_contains,
        description_contains=description_contains,
        price_per_m2_min=price_per_m2_min,
        price_per_m2_max=price_per_m2_max,
        price_per_ha_min=price_per_ha_min,
        price_per_ha_max=price_per_ha_max,
        **kwargs,
    )
    return filter_group_to_string(query.filters, query.geo_filters, collection=COLLECTION)
