# -*- coding: utf-8 -*-
"""
FilterStringService: серіалізація та парсинг рядка фільтрів.
Формат: "Активність" = True AND "Дата в джерелі" >= '01.01.2026' AND (geo('Область' INSIDE 'Київська')).
Корінь — група AND; групи та елементи з логікою І/АБО.
"""

import re
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union

import yaml

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


class ParseResult:
    """Результат парсингу рядка фільтрів."""

    def __init__(
        self,
        success: bool,
        filter_group: Optional[FilterGroup] = None,
        geo_filter: Optional[GeoFilter] = None,
        error: Optional[str] = None,
        error_position: Optional[int] = None,
    ):
        self.success = success
        self.filter_group = filter_group
        self.geo_filter = geo_filter
        self.error = error
        self.error_position = error_position


# Маппінг рядкових операторів у FilterOperator
_OP_STR_TO_ENUM = {
    "=": FilterOperator.EQ,
    "==": FilterOperator.EQ,
    "!=": FilterOperator.NE,
    ">": FilterOperator.GT,
    ">=": FilterOperator.GTE,
    "<": FilterOperator.LT,
    "<=": FilterOperator.LTE,
    "in": FilterOperator.IN,
    "nin": FilterOperator.NIN,
    "contains": FilterOperator.CONTAINS,
    "not_contains": FilterOperator.NOT_CONTAINS,
    "filled": FilterOperator.FILLED,
    "empty": FilterOperator.EMPTY,
}

# Гео-оператори в рядку
_GEO_OP_STR = {
    "inside": GeoFilterOperator.INSIDE,
    "not_inside": GeoFilterOperator.NOT_INSIDE,
    "in_radius": GeoFilterOperator.IN_RADIUS,
}

# Назви топонімів для виводу
_GEO_TYPE_LABELS = {
    "region": "Область",
    "settlement": "Населений пункт",
    "settlement_population": "Населення НП",
    "settlement_area": "Площа НП",
    "city_district": "Район міста",
}

_BUILDER_HIDDEN_FIELD_KEYS = frozenset({
    "settlement_population",
    "settlement_area_sq_km",
    "settlement_region_context",
    "settlement_name_context",
})


def _load_search_fields_config(collection: str = "unified_listings") -> Dict[str, Any]:
    """Завантажує config/search_fields.yaml і повертає конфіг полів колекції."""
    try:
        config_path = Path(__file__).resolve().parents[2] / "config" / "search_fields.yaml"
        with open(config_path, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        return (data.get("collections") or {}).get(collection) or {}
    except Exception:
        return {}


def get_field_label_to_key(collection: str = "unified_listings") -> Dict[str, str]:
    """Повертає маппінг label_uk → field key для парсингу рядка."""
    conf = _load_search_fields_config(collection)
    fields = conf.get("fields") or {}
    mapping = {str(v.get("label_uk", k)): k for k, v in fields.items() if v.get("label_uk")}
    # Backward compatibility for historic labels in saved filter strings.
    legacy_aliases = {
        "Площа землі, га": "land_area_ha",
        "Ціна за га, грн": "price_per_ha_uah",
        "Ціна за га, USD": "price_per_ha_usd",
    }
    for legacy_label, field_key in legacy_aliases.items():
        if field_key in fields and legacy_label not in mapping:
            mapping[legacy_label] = field_key
    return mapping


def get_field_key_to_label(collection: str = "unified_listings") -> Dict[str, str]:
    """Повертає маппінг field key → label_uk для серіалізації."""
    conf = _load_search_fields_config(collection)
    fields = conf.get("fields") or {}
    return {k: str(v.get("label_uk", k)) for k, v in fields.items()}


def filter_group_to_string(
    group: Optional[FilterGroup],
    geo_filter: Optional[GeoFilter] = None,
    collection: str = "unified_listings",
) -> str:
    """
    Перетворює FilterGroup та опційно GeoFilter на рядок фільтрів.
    Використовує лейбли з config/search_fields.yaml.
    """
    labels = get_field_key_to_label(collection)
    parts: List[str] = []

    def value_to_str(v: Any) -> str:
        if v is None:
            return "null"
        if isinstance(v, bool):
            return "True" if v else "False"
        if isinstance(v, (int, float)):
            return str(v)
        if isinstance(v, list):
            return "[" + ", ".join(value_to_str(x) for x in v) + "]"
        s = str(v).replace("\\", "\\\\").replace("'", "\\'")
        return "'" + s + "'"

    def op_to_str(op: FilterOperator) -> str:
        if op == FilterOperator.EQ:
            return "="
        if op == FilterOperator.NE:
            return "!="
        if op == FilterOperator.GT:
            return ">"
        if op == FilterOperator.GTE:
            return ">="
        if op == FilterOperator.LT:
            return "<"
        if op == FilterOperator.LTE:
            return "<="
        if op in (FilterOperator.CONTAINS, FilterOperator.NOT_CONTAINS, FilterOperator.IN, FilterOperator.NIN,
                  FilterOperator.FILLED, FilterOperator.EMPTY):
            return op.value.upper()
        return op.value

    def serialize_element(elem: FilterElement) -> str:
        label = labels.get(elem.field, elem.field)
        op_str = op_to_str(elem.operator)
        if elem.operator in (FilterOperator.FILLED, FilterOperator.EMPTY):
            return '"%s" %s' % (label, op_str)
        return '"%s" %s %s' % (label, op_str, value_to_str(elem.value))

    def serialize_group(gr: FilterGroup) -> str:
        inner = []
        for item in gr.items:
            if isinstance(item, FilterElement):
                inner.append(serialize_element(item))
            elif isinstance(item, FilterGroup):
                inner.append("(" + serialize_group(item) + ")")
        joiner = " AND " if gr.group_type == FilterGroupType.AND else " OR "
        return joiner.join(inner)

    if group and group.items:
        parts.append(serialize_group(group))

    if geo_filter:
        geo_str = _geo_filter_to_string(geo_filter.root)
        if geo_str:
            parts.append(geo_str)

    return " AND ".join(parts) if parts else ""


def _geo_filter_to_string(node: Union[GeoFilterElement, GeoFilterGroup]) -> str:
    """Серіалізує GeoFilter у (geo(...) OR geo(...)) — OR/AND між викликами geo, не всередині."""
    if isinstance(node, GeoFilterElement):
        return "geo(" + _geo_element_to_str(node) + ")"
    if isinstance(node, GeoFilterGroup):
        inner: List[str] = []
        for it in node.items:
            if isinstance(it, (GeoFilterElement, GeoFilterGroup)):
                part = _geo_filter_to_string(it)
                if part:
                    inner.append(part)
        if not inner:
            return ""
        joiner = " OR " if node.group_type == FilterGroupType.OR else " AND "
        if len(inner) == 1:
            return inner[0]
        return "(" + joiner.join(inner) + ")"
    return ""


def _geo_element_to_str(elem: GeoFilterElement) -> str:
    """Один гео-елемент у рядок: 'Область' INSIDE 'Київська' [REGION 'Волинська']."""
    label = _GEO_TYPE_LABELS.get(elem.geo_type, elem.geo_type)
    op = elem.operator
    if op in (GeoFilterOperator.EQ, GeoFilterOperator.INSIDE):
        op_str = "INSIDE"
    elif op in (GeoFilterOperator.NE, GeoFilterOperator.NOT_INSIDE):
        op_str = "NOT INSIDE"
    elif op == GeoFilterOperator.IN_RADIUS:
        val = "'%s'" % str(elem.value) if isinstance(elem.value, str) else str(elem.value)
        r = elem.radius_km or 0
        return "'%s' IN_RADIUS %s %s km" % (label, val, r)
    else:
        op_str = str(op.value).upper()
    val_str = "'%s'" % str(elem.value).replace("'", "\\'") if elem.value is not None else "''"
    base = "'%s' %s %s" % (label, op_str, val_str)
    region_ctx = getattr(elem, "region", None)
    if elem.geo_type == "settlement" and region_ctx:
        reg = str(region_ctx).replace("'", "\\'")
        base += " REGION '%s'" % reg
    city_id = getattr(elem, "city_id", None)
    if elem.geo_type == "settlement" and city_id:
        base += " CITY_ID '%s'" % str(city_id).replace("'", "\\'")
    return base


def filter_string_to_models(
    s: str,
    collection: str = "unified_listings",
) -> ParseResult:
    """
    Парсить рядок фільтрів і повертає FilterGroup та опційно GeoFilter.
    При синтаксичній помилці повертає success=False та error.
    """
    if not s or not str(s).strip():
        return ParseResult(success=True, filter_group=None, geo_filter=None)

    label_to_key = get_field_label_to_key(collection)
    s = str(s).strip()
    s = re.sub(r"\s+", " ", s)

    try:
        filter_group, geo_filter = _parse_expression(s, label_to_key)
        return ParseResult(
            success=True,
            filter_group=filter_group,
            geo_filter=geo_filter,
        )
    except ValueError as e:
        return ParseResult(success=False, error=str(e))
    except Exception as e:
        return ParseResult(success=False, error=str(e))


def _parse_expression(s: str, label_to_key: Dict[str, str]) -> Tuple[Optional[FilterGroup], Optional[GeoFilter]]:
    """Рекурсивно парсить вираз. AND має нижчий пріоритет ніж OR."""
    s = s.strip()
    if not s:
        return None, None

    s, geo_filter = _extract_geo_filter_from_expression(s)

    if not s:
        return None, geo_filter

    and_parts = _split_top_level(s, " AND ")
    if len(and_parts) > 1:
        items = []
        for part in and_parts:
            gr, _ = _parse_expression(part, label_to_key)
            if gr:
                items.append(gr)
        if not items:
            return None, geo_filter
        if len(items) == 1:
            return items[0], geo_filter
        return FilterGroup(group_type=FilterGroupType.AND, items=items), geo_filter

    or_parts = _split_top_level(s, " OR ")
    if len(or_parts) > 1:
        items = []
        for part in or_parts:
            gr, _ = _parse_expression(part, label_to_key)
            if gr:
                items.append(gr)
        if not items:
            return None, geo_filter
        if len(items) == 1:
            return items[0], geo_filter
        return FilterGroup(group_type=FilterGroupType.OR, items=items), geo_filter

    if s.startswith("(") and s.endswith(")"):
        return _parse_expression(s[1:-1].strip(), label_to_key)[0], geo_filter

    elem = _parse_term(s, label_to_key)
    if elem:
        return FilterGroup(group_type=FilterGroupType.AND, items=[elem]), geo_filter
    return None, geo_filter


def _split_top_level(s: str, sep: str) -> List[str]:
    """Розбиває рядок по sep тільки на верхньому рівні (не всередині дужок)."""
    parts = []
    depth = 0
    start = 0
    sep_upper = sep.upper()
    i = 0
    while i <= len(s) - len(sep):
        if s[i] == "(":
            depth += 1
            i += 1
            continue
        if s[i] == ")":
            depth -= 1
            i += 1
            continue
        if depth == 0 and s[i:i + len(sep)].upper() == sep_upper:
            parts.append(s[start:i].strip())
            start = i + len(sep)
            i = start
            continue
        i += 1
    parts.append(s[start:].strip())
    return [p for p in parts if p]


def _parse_term(s: str, label_to_key: Dict[str, str]) -> Optional[FilterElement]:
    """Парсить один терм: "Label" op value. Оператор може бути =, !=, >=, <=, >, < або слово (eq, in, contains тощо)."""
    # Оператор: символьний (=, !=, >=, <=, >, <) або слово (\w+)
    op_pattern = r"(?:==|!=|>=|<=|=|>|<|\w+)"
    quoted = re.match(r'"([^"]*)"\s+(' + op_pattern + r')\s*(.*)$', s, re.DOTALL)
    if not quoted:
        return None
    label, op_str, rest = quoted.group(1), quoted.group(2).strip().lower(), quoted.group(3).strip()
    field_key = label_to_key.get(label, label)
    op_enum = _OP_STR_TO_ENUM.get(op_str)
    if op_enum is None:
        try:
            op_enum = FilterOperator(op_str)
        except ValueError:
            return None
    if op_enum in (FilterOperator.FILLED, FilterOperator.EMPTY):
        value = True
    else:
        value = _parse_value(rest)
        if value is None and rest:
            value = rest
    if field_key == "source" and isinstance(value, str) and value.strip():
        value = value.strip().lower()
    return FilterElement(field=field_key, operator=op_enum, value=value)


def _parse_value(s: str) -> Any:
    """Парсить значення: число, 'рядок', True, False, [a,b]."""
    s = s.strip()
    if not s:
        return None
    if s.lower() == "true":
        return True
    if s.lower() == "false":
        return False
    if s.lower() == "null":
        return None
    if s.startswith("'") and s.endswith("'"):
        return s[1:-1].replace("\\'", "'")
    if s.startswith("[") and s.endswith("]"):
        inner = s[1:-1].strip()
        if not inner:
            return []
        return [_parse_value(x.strip()) for x in re.split(r",", inner)]
    try:
        if "." in s:
            return float(s)
        return int(s)
    except ValueError:
        return s


def _parse_geo_element(s: str) -> Optional[GeoFilterElement]:
    """Парсить один гео-терм: 'Область' INSIDE 'Київська' [REGION 'Волинська область'].
    У лапках підтримується екранування: \\' та \\\\."""
    s = s.strip()
    label_to_geo = {v: k for k, v in _GEO_TYPE_LABELS.items()}
    quoted = r"'((?:[^'\\]|\\.)*)'"
    pattern = (
        r"^"
        + quoted
        + r"\s+(INSIDE|NOT\s+INSIDE|IN_RADIUS)\s+"
        + quoted
        + r"(?:\s+REGION\s+"
        + quoted
        + r")?"
        + r"(?:\s+CITY_ID\s+"
        + quoted
        + r")?"
        + r"\s*(?:(\d+(?:\.\d+)?)\s*km)?$"
    )
    alt = re.match(pattern, s, re.IGNORECASE)
    if alt:
        label = _unescape_quoted(alt.group(1))
        op_str = alt.group(2).replace(" ", "_").lower()
        value = _unescape_quoted(alt.group(3))
        region_val = _unescape_quoted(alt.group(4)) if alt.group(4) else None
        city_id_val = _unescape_quoted(alt.group(5)) if alt.group(5) else None
        radius = alt.group(6)
        geo_type = label_to_geo.get(label, "settlement")
        if "not_inside" in op_str:
            op_enum = GeoFilterOperator.NOT_INSIDE
        elif "in_radius" in op_str:
            op_enum = GeoFilterOperator.IN_RADIUS
        else:
            op_enum = GeoFilterOperator.INSIDE
        radius_km = float(radius) if radius else None
        if op_enum == GeoFilterOperator.IN_RADIUS and radius_km:
            return GeoFilterElement(operator=op_enum, geo_type="coordinates", value=value, radius_km=radius_km)
        return GeoFilterElement(
            operator=op_enum,
            geo_type=geo_type,
            value=value,
            radius_km=radius_km,
            region=region_val if geo_type == "settlement" and region_val else None,
            city_id=city_id_val if geo_type == "settlement" and city_id_val else None,
        )
    return None


def _unescape_quoted(s: str) -> str:
    """Розекрановує рядок з гео-значення: \\' -> ', \\\\ -> \\."""
    if not s:
        return s
    return s.replace("\\\\", "\x00").replace("\\'", "'").replace("\x00", "\\")


def _geo_parts_to_filter(
    parts: List[GeoFilterElement],
    *,
    joiners: Optional[List[FilterGroupType]] = None,
) -> GeoFilter:
    """Збирає список гео-елементів в GeoFilter з OR/AND між викликами geo()."""
    if not parts:
        raise ValueError("Порожній гео-фільтр")
    if len(parts) == 1:
        return GeoFilter(root=parts[0])
    group_type = FilterGroupType.AND
    if joiners:
        if all(j == FilterGroupType.OR for j in joiners):
            group_type = FilterGroupType.OR
        elif all(j == FilterGroupType.AND for j in joiners):
            group_type = FilterGroupType.AND
    return GeoFilter(root=GeoFilterGroup(group_type=group_type, items=parts))


def _extract_geo_filter_from_expression(s: str) -> Tuple[str, Optional[GeoFilter]]:
    """
    Витягує geo(...) з виразу, зберігаючи OR/AND між сусідніми викликами.
    Повертає рядок без geo-частини та GeoFilter.
    """
    geo_start = re.compile(r"\bgeo\s*\(\s*", re.IGNORECASE)
    spans: List[Tuple[int, int, str]] = []
    pos = 0
    while True:
        m = geo_start.search(s, pos)
        if not m:
            break
        start = m.start()
        i = m.end()
        depth = 1
        while i < len(s) and depth > 0:
            ch = s[i]
            if ch == "(":
                depth += 1
            elif ch == ")":
                depth -= 1
            i += 1
        if depth != 0:
            break
        inner = s[m.end() : i - 1].strip()
        spans.append((start, i, inner))
        pos = i

    if not spans:
        return s, None

    elems: List[GeoFilterElement] = []
    for _, _, inner in spans:
        elem = _parse_geo_element(inner)
        if elem:
            elems.append(elem)
    if not elems:
        return s, None

    joiners: List[FilterGroupType] = []
    for idx in range(len(spans) - 1):
        between = s[spans[idx][1] : spans[idx + 1][0]].strip()
        between = re.sub(r"^\(+", "", between)
        between = re.sub(r"\)+$", "", between)
        between = between.strip()
        if between.upper() == "OR":
            joiners.append(FilterGroupType.OR)
        else:
            joiners.append(FilterGroupType.AND)

    geo_filter = _geo_parts_to_filter(elems, joiners=joiners or None)

    new_s = s
    for start, end, _ in reversed(spans):
        new_s = new_s[:start] + " " + new_s[end:]
    new_s = re.sub(r"\s+", " ", new_s).strip()
    new_s = re.sub(r"^\s*AND\s+|\s+AND\s*$", "", new_s).strip()
    new_s = re.sub(r"^\(\s*\)\s*$", "", new_s).strip()
    return new_s, geo_filter


def get_builder_config(collection: str = "unified_listings") -> Dict[str, Any]:
    """Повертає конфіг для UI конструктора фільтрів: поля + гео (з лейблами та операторами)."""
    try:
        with open(
            Path(__file__).resolve().parents[2] / "config" / "search_fields.yaml",
            "r",
            encoding="utf-8",
        ) as f:
            data = yaml.safe_load(f)
    except Exception:
        return {"fields": {}, "geo": {}}
    coll = (data.get("collections") or {}).get(collection) or {}
    raw_fields = coll.get("fields") or {}
    fields = {
        k: v
        for k, v in raw_fields.items()
        if k not in _BUILDER_HIDDEN_FIELD_KEYS and not v.get("builder_hidden")
    }
    geo_cfg = data.get("geo") or {}
    labels = dict(_GEO_TYPE_LABELS)
    labels.update(geo_cfg.get("type_labels_uk") or {})
    return {
        "fields": fields,
        "geo": {**geo_cfg, "type_labels_uk": labels},
    }


def structure_to_filter_models(
    filters: List[Dict[str, Any]],
    geo: Optional[List[Dict[str, Any]]] = None,
) -> Tuple[Optional[FilterGroup], Optional[GeoFilter]]:
    """
    Будує FilterGroup та GeoFilter з простого JSON (для UI конструктора).
    filters: [{"field": "source", "operator": "eq", "value": "olx"}, ...]
    geo: [{"geo_type": "region", "operator": "inside", "value": "Київська"}, ...]
    """
    elements: List[FilterElement] = []
    for f in filters or []:
        field = f.get("field")
        op_raw = f.get("operator")
        value = f.get("value")
        if not field:
            continue
        try:
            op_enum = FilterOperator(op_raw) if op_raw else FilterOperator.EQ
        except ValueError:
            op_enum = _OP_STR_TO_ENUM.get(str(op_raw).strip(), FilterOperator.EQ)
        if op_enum in (FilterOperator.FILLED, FilterOperator.EMPTY):
            value = True
        elements.append(FilterElement(field=field, operator=op_enum, value=value))
    group = FilterGroup(group_type=FilterGroupType.AND, items=elements) if elements else None

    geo_filter: Optional[GeoFilter] = None
    if geo:
        geo_elems: List[GeoFilterElement] = []
        for g in geo:
            val = g.get("value")
            if val is None or (isinstance(val, str) and not val.strip()):
                continue
            geo_type = str(g.get("geo_type") or "region").strip()
            op_raw = str(g.get("operator") or "inside").strip().lower()
            op_enum = _GEO_OP_STR.get(op_raw, GeoFilterOperator.INSIDE)
            geo_elems.append(
                GeoFilterElement(operator=op_enum, geo_type=geo_type, value=val.strip() if isinstance(val, str) else val)
            )
        if geo_elems:
            geo_filter = (
                GeoFilter(root=geo_elems[0])
                if len(geo_elems) == 1
                else GeoFilter(root=GeoFilterGroup(group_type=FilterGroupType.AND, items=geo_elems))
            )
    return group, geo_filter


def filter_string_from_structure(
    filters: List[Dict[str, Any]],
    geo: Optional[List[Dict[str, Any]]] = None,
    collection: str = "unified_listings",
) -> str:
    """Генерує рядок фільтрів з структури (filters + geo) для UI конструктора."""
    group, geo_filter = structure_to_filter_models(filters, geo)
    return filter_group_to_string(group, geo_filter=geo_filter, collection=collection)


def _append_settlement_criteria_items(
    items: List[FilterElement],
    it: Dict[str, Any],
    *,
    include_settlement_name: bool = True,
    include_population: bool = True,
    include_area: bool = True,
) -> None:
    """Додає логічні критерії НП (резолвляться в GeoFilter при пошуку)."""
    from domain.models.filter_models import FilterOperator as FO
    from business.services.settlement_criteria_resolver import (
        SETTLEMENT_AREA_FIELD,
        SETTLEMENT_NAME_CONTEXT_FIELD,
        SETTLEMENT_POPULATION_FIELD,
        SETTLEMENT_REGION_CONTEXT_FIELD,
    )

    geo_reg = (it.get("geoRegion") or "").strip()
    if geo_reg:
        items.append(
            FilterElement(
                field=SETTLEMENT_REGION_CONTEXT_FIELD,
                operator=FO.EQ,
                value=geo_reg,
            )
        )
    if include_settlement_name:
        val = it.get("value")
        if val is not None and isinstance(val, str) and val.strip():
            items.append(
                FilterElement(
                    field=SETTLEMENT_NAME_CONTEXT_FIELD,
                    operator=FO.EQ,
                    value=val.strip(),
                )
            )
    if include_population:
        if it.get("population_min") is not None:
            items.append(
                FilterElement(
                    field=SETTLEMENT_POPULATION_FIELD,
                    operator=FO.GTE,
                    value=int(it["population_min"]),
                )
            )
        if it.get("population_max") is not None:
            items.append(
                FilterElement(
                    field=SETTLEMENT_POPULATION_FIELD,
                    operator=FO.LTE,
                    value=int(it["population_max"]),
                )
            )
    if include_area:
        if it.get("area_min") is not None:
            items.append(
                FilterElement(
                    field=SETTLEMENT_AREA_FIELD,
                    operator=FO.GTE,
                    value=float(it["area_min"]),
                )
            )
        if it.get("area_max") is not None:
            items.append(
                FilterElement(
                    field=SETTLEMENT_AREA_FIELD,
                    operator=FO.LTE,
                    value=float(it["area_max"]),
                )
            )


def _geo_node_has_criteria(it: Dict[str, Any], geo_type: str) -> bool:
    if geo_type == "settlement_population":
        return any(
            it.get(k) is not None and it.get(k) != ""
            for k in ("population_min", "population_max", "geoRegion")
        )
    if geo_type == "settlement_area":
        return any(
            it.get(k) is not None and it.get(k) != ""
            for k in ("area_min", "area_max", "geoRegion")
        )
    if geo_type == "settlement":
        return any(
            it.get(k) is not None and it.get(k) != ""
            for k in (
                "population_min",
                "population_max",
                "area_min",
                "area_max",
                "geoRegion",
            )
        )
    return False


def _geo_node_has_demographic_criteria(it: Dict[str, Any]) -> bool:
    """Населення/площа НП (без урахування geoRegion — область іде в geo REGION)."""
    return any(
        it.get(k) is not None and it.get(k) != ""
        for k in ("population_min", "population_max", "area_min", "area_max")
    )


def tree_to_filter_models(root: Dict[str, Any]) -> Tuple[Optional[FilterGroup], Optional[GeoFilter]]:
    """
    Будує FilterGroup та GeoFilter з дерева (root).
    root: {"group_type": "and"|"or", "items": [ {"type": "element", "field", "operator", "value"}, {"type": "group", "group_type", "items": [...]}, {"type": "geo", "geo_type", "operator", "value"} ]}
    """
    if not root or not isinstance(root.get("items"), list):
        return None, None

    geo_elems: List[GeoFilterElement] = []
    try:
        root_group_type = FilterGroupType((root.get("group_type") or "and").strip().lower())
    except ValueError:
        root_group_type = FilterGroupType.AND

    def build_group(node: Dict[str, Any]) -> Optional[FilterGroup]:
        gt = (node.get("group_type") or "and").strip().lower()
        try:
            group_type = FilterGroupType(gt)
        except ValueError:
            group_type = FilterGroupType.AND
        items: List[Union[FilterElement, FilterGroup]] = []
        for it in node.get("items") or []:
            if not isinstance(it, dict):
                continue
            t = (it.get("type") or "").strip().lower()
            if t == "geo":
                val = it.get("value")
                geo_type = str(it.get("geo_type") or "region").strip()
                has_criteria = _geo_node_has_criteria(it, geo_type)
                val_empty = val is None or (isinstance(val, str) and not str(val).strip())
                if val_empty and not has_criteria:
                    continue
                op_raw = str(it.get("operator") or "inside").strip().lower()
                op_enum = _GEO_OP_STR.get(op_raw, GeoFilterOperator.INSIDE)
                if geo_type not in ("settlement_population", "settlement_area") and not val_empty:
                    geo_reg = (it.get("geoRegion") or "").strip() or None
                    city_id = (it.get("city_id") or it.get("cityId") or "").strip() or None
                    geo_elems.append(
                        GeoFilterElement(
                            operator=op_enum,
                            geo_type=geo_type,
                            value=val.strip() if isinstance(val, str) else val,
                            region=geo_reg if geo_type == "settlement" else None,
                            city_id=city_id if geo_type == "settlement" else None,
                        )
                    )
                if geo_type == "settlement":
                    # Назву/область у FilterGroup лише якщо немає value (фільтр за демографією без НП)
                    if val_empty:
                        _append_settlement_criteria_items(
                            items,
                            it,
                            include_settlement_name=True,
                            include_population=False,
                            include_area=False,
                        )
                    elif _geo_node_has_demographic_criteria(it):
                        _append_settlement_criteria_items(
                            items,
                            it,
                            include_settlement_name=False,
                            include_population=True,
                            include_area=True,
                        )
                elif geo_type == "settlement_population":
                    _append_settlement_criteria_items(
                        items,
                        it,
                        include_settlement_name=False,
                        include_population=True,
                        include_area=False,
                    )
                elif geo_type == "settlement_area":
                    _append_settlement_criteria_items(
                        items,
                        it,
                        include_settlement_name=False,
                        include_population=False,
                        include_area=True,
                    )
                continue
            if t == "group":
                child = build_group(it)
                if child and child.items:
                    items.append(child)
                continue
            if t == "element":
                field = it.get("field")
                if not field:
                    continue
                op_raw = it.get("operator") or "eq"
                try:
                    op_enum = FilterOperator(op_raw)
                except ValueError:
                    op_enum = _OP_STR_TO_ENUM.get(str(op_raw).strip(), FilterOperator.EQ)
                value = it.get("value")
                if op_enum in (FilterOperator.FILLED, FilterOperator.EMPTY):
                    value = True
                items.append(FilterElement(field=field, operator=op_enum, value=value))
        if not items:
            return None
        return FilterGroup(group_type=group_type, items=items)

    group = build_group(root)
    geo_filter: Optional[GeoFilter] = None
    if geo_elems:
        if len(geo_elems) == 1:
            geo_filter = GeoFilter(root=geo_elems[0])
        elif root_group_type == FilterGroupType.OR:
            geo_filter = GeoFilter(
                root=GeoFilterGroup(group_type=FilterGroupType.OR, items=geo_elems)
            )
        else:
            geo_filter = GeoFilter(
                root=GeoFilterGroup(group_type=FilterGroupType.AND, items=geo_elems)
            )
    return group, geo_filter


def filter_string_from_tree(root: Dict[str, Any], collection: str = "unified_listings") -> str:
    """Генерує рядок фільтрів з дерева (root) для UI конструктора."""
    group, geo_filter = tree_to_filter_models(root)
    return filter_group_to_string(group, geo_filter=geo_filter, collection=collection)
