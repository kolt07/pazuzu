# -*- coding: utf-8 -*-
"""Тип угоди оголошення: продаж (sale) або оренда (rent)."""

from typing import Any, Dict, Optional

DEAL_SALE = "sale"
DEAL_RENT = "rent"
DEAL_TYPES = (DEAL_SALE, DEAL_RENT)

_RENT_URL_MARKERS = ("arenda", "orenda", "/rent")
_RENT_METHOD_MARKERS = (
    "lease",
    "rental",
    "оренд",
    "аренд",
)


def normalize_deal_type(value: Any, default: str = DEAL_SALE) -> str:
    raw = str(value or "").strip().lower()
    if raw in DEAL_TYPES:
        return raw
    if raw in ("оренда", "оренду", "rent"):
        return DEAL_RENT
    if raw in ("продаж", "продажі", "sale"):
        return DEAL_SALE
    return default


def deal_type_from_olx_url(url: str) -> str:
    u = (url or "").lower()
    if any(m in u for m in _RENT_URL_MARKERS):
        return DEAL_RENT
    return DEAL_SALE


def deal_type_from_prozorro_data(auction_data: Optional[Dict[str, Any]]) -> str:
    """Визначає sale/rent з auction_data (leaseType, sellingMethod, procedureType)."""
    data = auction_data or {}
    if data.get("leaseType") or data.get("lease_type"):
        return DEAL_RENT
    sale_type = data.get("saleType") or data.get("sale_type")
    lease_type = data.get("leaseType") or data.get("lease_type")
    if sale_type and not lease_type:
        return DEAL_SALE
    for field in ("sellingMethod", "procedureType", "procedure_type", "custom_type"):
        val = str(data.get(field) or "").lower()
        if any(m in val for m in _RENT_METHOD_MARKERS):
            return DEAL_RENT
    return DEAL_SALE


def parse_deal_types(values: Any) -> list:
    """Унікальний список sale/rent. None → [sale]; порожній список → []."""
    if values is None or values is False:
        return [DEAL_SALE]
    if isinstance(values, str):
        values = [values]
    if not isinstance(values, (list, tuple)):
        return [DEAL_SALE]
    out = []
    seen = set()
    for v in values:
        dt = normalize_deal_type(v, default="")
        if dt in DEAL_TYPES and dt not in seen:
            seen.add(dt)
            out.append(dt)
    return out


def normalize_deal_types(values: Any) -> list:
    """Повертає унікальний список sale/rent зі входу (list/str). Порожній → [sale]."""
    return parse_deal_types(values) or [DEAL_SALE]
