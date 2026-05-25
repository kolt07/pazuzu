# -*- coding: utf-8 -*-
"""Конвертація координат DMS (градуси-хвилини-секунди) у десяткові градуси WGS84."""

from __future__ import annotations

import re
from typing import Optional, Tuple

_DMS_PAIR = re.compile(
    r"(\d{1,3})[°\s]+(\d{1,2})['′\s]+(\d{1,2}(?:[.,]\d+)?)[\"″\s]*"
    r"(?:пн\.?\s*ш\.?|N)?\s*"
    r"(\d{1,3})[°\s]+(\d{1,2})['′\s]+(\d{1,2}(?:[.,]\d+)?)[\"″\s]*"
    r"(?:сх\.?\s*д\.?|E)?",
    re.IGNORECASE,
)


def _dms_to_decimal(degrees: int, minutes: int, seconds: float) -> float:
    sign = -1 if degrees < 0 else 1
    deg = abs(degrees)
    return sign * (deg + minutes / 60.0 + seconds / 3600.0)


def parse_dms_coordinates(text: str) -> Optional[Tuple[float, float]]:
    """
    Парсить рядок типу «48°30'34″ пн. ш. 32°16'01″ сх. д.» → (lat, lon).
    """
    if not text or not str(text).strip():
        return None
    m = _DMS_PAIR.search(str(text))
    if not m:
        return None
    lat_d, lat_m, lat_s, lon_d, lon_m, lon_s = m.groups()
    lat = _dms_to_decimal(int(lat_d), int(lat_m), float(lat_s.replace(",", ".")))
    lon = _dms_to_decimal(int(lon_d), int(lon_m), float(lon_s.replace(",", ".")))
    return (round(lat, 6), round(lon, 6))
