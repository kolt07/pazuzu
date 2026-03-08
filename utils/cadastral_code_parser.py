# -*- coding: utf-8 -*-
"""
Парсер кадастрового номера України для визначення топографічної прив'язки.

Структура кадастрового номера: НКЗ:НКК:НЗД
- НКЗ (12 цифр): номер кадастрової зони. Перші 10 цифр = код КОАТУУ.
- НКК (3 цифри): номер кадастрового кварталу.
- НЗД (4 цифри): номер земельної ділянки.

КОАТУУ (10 цифр):
- 1-2: область (або місто зі спеціальним статусом)
- 3-5: місто обласного підпорядкування / район
- 6-8: місто районного підпорядкування / сільрада
- 9-10: село / селище

Приклад: 6310138500:10:012:0045
- 63 — Харківська область
- 101 — м. Харків
- 38500 — Немишлянський район
- 10 — кадастрова зона
- 012 — квартал
- 0045 — ділянка
"""

from pathlib import Path
from typing import Any, Dict, Optional

import yaml

# Шлях до конфігу з кодами областей
_KOATUU_CONFIG_PATH = Path(__file__).resolve().parent.parent / "config" / "koatuu_oblast_codes.yaml"
_OBLAST_CODES: Optional[Dict[str, str]] = None


def _load_oblast_codes() -> Dict[str, str]:
    """Завантажує маппінг кодів областей з YAML."""
    global _OBLAST_CODES
    if _OBLAST_CODES is not None:
        return _OBLAST_CODES
    if _KOATUU_CONFIG_PATH.exists():
        with open(_KOATUU_CONFIG_PATH, encoding="utf-8") as f:
            data = yaml.safe_load(f)
            _OBLAST_CODES = data.get("oblast_codes") or {}
    else:
        _OBLAST_CODES = {}
    return _OBLAST_CODES


def parse_cadastral_number(cadastral_number: str) -> Optional[Dict[str, Any]]:
    """
    Парсить кадастровий номер та повертає структуровані дані для індексації.

    Args:
        cadastral_number: рядок у форматі НКЗ:НКК:НЗД або варіанти (з пробілами, без двокрапок).

    Returns:
        Словник з полями: oblast_code, oblast_name, koatuu_prefix, zone, quarter, parcel,
        district_code, city_code; або None якщо номер невалідний.
    """
    if not cadastral_number or not isinstance(cadastral_number, str):
        return None
    raw = str(cadastral_number).strip()
    if not raw:
        return None

    # Нормалізація: прибираємо зайві пробіли
    parts = [p.strip() for p in raw.replace(" ", "").split(":") if p.strip()]
    if len(parts) < 3:
        # Спробуємо інтерпретувати як один блок (напр. 6310138500100120045)
        digits = "".join(c for c in raw if c.isdigit())
        if len(digits) >= 19:
            nkz = digits[:12]
            nkk = digits[12:15].zfill(3)
            nzd = digits[15:19].zfill(4)
        else:
            return None
    elif len(parts) == 4:
        # Формат 10:2:3:4 — 6310138500:10:012:0045
        nkz_raw = "".join(c for c in parts[0] if c.isdigit())
        zone_raw = "".join(c for c in parts[1] if c.isdigit())
        nkk = "".join(c for c in parts[2] if c.isdigit()).zfill(3)[-3:]
        nzd = "".join(c for c in parts[3] if c.isdigit()).zfill(4)[-4:]
        if len(nkz_raw) < 10:
            return None
        nkz = nkz_raw[:10] + (zone_raw.zfill(2)[-2:] if zone_raw else "00")
    else:
        # Формат 3 частини: НКЗ(12):НКК(3):НЗД(4)
        nkz_raw = "".join(c for c in parts[0] if c.isdigit())
        nkk = "".join(c for c in parts[1] if c.isdigit()).zfill(3)[-3:]
        nzd = "".join(c for c in parts[2] if c.isdigit()).zfill(4)[-4:]
        if len(nkz_raw) < 10:
            return None
        nkz = nkz_raw[:12].zfill(12) if len(nkz_raw) >= 10 else None
        if not nkz:
            return None

    # Перші 10 цифр НКЗ = КОАТУУ
    koatuu_prefix = nkz[:10] if len(nkz) >= 10 else nkz.zfill(10)[:10]
    oblast_code = koatuu_prefix[:2]
    district_code = koatuu_prefix[2:5] if len(koatuu_prefix) >= 5 else None
    city_code = koatuu_prefix[5:8] if len(koatuu_prefix) >= 8 else None

    zone = nkz[10:12] if len(nkz) >= 12 else None

    oblast_codes = _load_oblast_codes()
    oblast_name = oblast_codes.get(oblast_code)

    return {
        "oblast_code": oblast_code,
        "oblast_name": oblast_name,
        "koatuu_prefix": koatuu_prefix,
        "district_code": district_code,
        "city_code": city_code,
        "zone": zone,
        "quarter": nkk,
        "parcel": nzd,
    }


def get_location_for_search(cadastral_number: str) -> Optional[Dict[str, Any]]:
    """
    Повертає мінімальний набір полів для індексної колекції пошуку за місцезнаходженням.

    Використовується при побудові cadastral_parcel_location_index.
    """
    parsed = parse_cadastral_number(cadastral_number)
    if not parsed:
        return None
    return {
        "cadastral_number": str(cadastral_number).strip(),
        "oblast_code": parsed["oblast_code"],
        "oblast_name": parsed.get("oblast_name"),
        "koatuu_prefix": parsed["koatuu_prefix"],
        "district_code": parsed.get("district_code"),
        "city_code": parsed.get("city_code"),
        "zone": parsed.get("zone"),
        "quarter": parsed["quarter"],
        "parcel": parsed["parcel"],
    }
