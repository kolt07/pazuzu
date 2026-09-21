# -*- coding: utf-8 -*-
"""Конвертація ціни оголошення (OLX UAH/USD/EUR) у гривню для метрик."""

from __future__ import annotations

from typing import Any, Optional


def _to_float(value: Any) -> Optional[float]:
    if value is None:
        return None
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, str):
        s = value.strip().replace("\u00a0", " ").replace(" ", "").replace(",", ".")
        if not s:
            return None
        try:
            return float(s)
        except ValueError:
            return None
    return None


def normalize_listing_currency(currency: Any, price_text: Any = None) -> str:
    """Валюта оголошення: поле currency, інакше знак у price_text ($ / грн / €)."""
    cur = str(currency or "").strip().upper()
    text = str(price_text or "")
    text_l = text.lower()
    if cur == "UAH" and "$" in text and "грн" not in text_l:
        return "USD"
    if cur in ("UAH", "USD", "EUR"):
        return cur
    if "$" in text and "грн" not in text_l:
        return "USD"
    if "€" in text or "eur" in text_l:
        return "EUR"
    return "UAH"


def listing_price_to_uah(
    amount: Any,
    currency: Any,
    uah_per_usd: Any,
    price_text: Any = None,
) -> Optional[float]:
    """Переводить ціну оголошення в гривню. USD/EUR — через курс; UAH лишається як є."""
    val = _to_float(amount)
    if val is None or val <= 0:
        return None
    cur = normalize_listing_currency(currency, price_text)
    rate = _to_float(uah_per_usd)
    if cur == "USD":
        return val * rate if rate and rate > 0 else None
    if cur == "EUR":
        return val * rate * 1.1 if rate and rate > 0 else None
    return val
