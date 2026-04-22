# -*- coding: utf-8 -*-
"""
Утиліти для нормалізації площі земельних ділянок із тексту.
"""

from __future__ import annotations

import re
from typing import Any, List, Optional

_SOTOK_PATTERN = re.compile(
    r"(\d[\d\s]*[.,]?\d*)\s*сот(?:к(?:а|и|у|ою|ами|ах)?|ок|их|і|и)?\b",
    re.IGNORECASE,
)


def _to_float(value: Any) -> Optional[float]:
    if value in (None, ""):
        return None
    try:
        return float(str(value).replace(" ", "").replace(",", "."))
    except (TypeError, ValueError):
        return None


def coerce_land_area_sqm(land_area_sqm: Any, land_area_ha: Any) -> Optional[float]:
    """Повертає площу землі у м² з полів sqm/ha."""
    sqm = _to_float(land_area_sqm)
    if sqm is not None and sqm > 0:
        return sqm
    ha = _to_float(land_area_ha)
    if ha is not None and ha > 0:
        return ha * 10000.0
    return None


def extract_sotok_values(text: str) -> List[float]:
    """
    Витягує значення площі в сотках із тексту.
    Дедуплікує повтори однакових значень (часто дублюються в заголовку й описі).
    """
    if not text:
        return []
    values: List[float] = []
    seen = set()
    for match in _SOTOK_PATTERN.finditer(text):
        value = _to_float(match.group(1))
        if value is None or value <= 0 or value > 100000:
            continue
        key = round(value, 4)
        if key in seen:
            continue
        seen.add(key)
        values.append(value)
    return values


def extract_sotok_area_sqm(text: str) -> float:
    """Повертає сумарну площу у м² за згадками у сотках."""
    return sum(extract_sotok_values(text)) * 100.0


def should_fix_land_area_sqm_by_sotok(current_sqm: Any, expected_sqm: float) -> bool:
    """
    Визначає, чи потрібно підмінити площу на значення, витягнуте з "соток".
    """
    if expected_sqm <= 0:
        return False
    current = _to_float(current_sqm)
    if current is None or current <= 0:
        return True

    ratio = current / expected_sqm
    if 0.97 <= ratio <= 1.03:
        return False

    # Типові помилки масштабу при парсингу соток.
    for wrong_ratio in (0.01, 0.1, 10.0, 100.0):
        tolerance = wrong_ratio * 0.15
        if wrong_ratio - tolerance <= ratio <= wrong_ratio + tolerance:
            return True

    return ratio < 0.2 or ratio > 5.0
