# -*- coding: utf-8 -*-
"""
Утиліти для розрахунку цінових метрик:
- загальна ціна в USD
- ціна за м² (UAH та USD)
- ціна за гектар (UAH та USD)

Всі розрахунки виконуються в коді сервісів/роутів, які передають:
- базову ціну в гривні (total_price_uah)
- площу в м² (area_m2) та/або площу в гектарах (land_area_ha)
- курс продажу USD (uah_per_usd)
"""

from __future__ import annotations

from typing import Any, Dict, Optional


def _to_float(value: Any) -> Optional[float]:
    """Акуратне приведення до float з підтримкою рядків."""
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


def compute_price_metrics(
    total_price_uah: Any,
    building_area_sqm: Any = None,
    land_area_ha: Any = None,
    uah_per_usd: Any = None,
) -> Dict[str, Optional[float]]:
    """
    Розраховує набір цінових метрик.

    Правила заповнення price_per_m2 та price_per_ha:
    - Земельна ділянка без нерухомості (land_area_ha > 0, building_area_sqm відсутня):
      price_per_m2 = NULL, price_per_ha = заповнена
    - Земельна ділянка з нерухомістю (обидві площі > 0):
      за наявності даних — заповнюємо обидва поля
    - Чисто нерухомість (building_area_sqm > 0, land_area_ha відсутня):
      price_per_m2 = заповнена, price_per_ha = NULL

    Args:
        total_price_uah: загальна ціна в гривні
        building_area_sqm: площа в м² (будівля/приміщення)
        land_area_ha: площа земельної ділянки в гектарах
        uah_per_usd: курс продажу USD (скільки гривень за 1 USD)

    Returns:
        dict з ключами:
            - total_price_uah
            - total_price_usd
            - price_per_m2_uah
            - price_per_m2_usd
            - price_per_ha_uah
            - price_per_ha_usd
    """
    price_uah = _to_float(total_price_uah)
    area_sqm = _to_float(building_area_sqm)
    area_ha = _to_float(land_area_ha)
    rate = _to_float(uah_per_usd)

    metrics: Dict[str, Optional[float]] = {
        "total_price_uah": None,
        "total_price_usd": None,
        "price_per_m2_uah": None,
        "price_per_m2_usd": None,
        "price_per_ha_uah": None,
        "price_per_ha_usd": None,
    }

    if price_uah is None or price_uah <= 0:
        return metrics

    metrics["total_price_uah"] = price_uah

    if rate and rate > 0:
        metrics["total_price_usd"] = price_uah / rate

    # Ціна за м² — лише за умови наявності площі в м² (будівля/приміщення)
    if area_sqm and area_sqm > 0:
        price_per_m2_uah = price_uah / area_sqm
        metrics["price_per_m2_uah"] = price_per_m2_uah
        if rate and rate > 0:
            metrics["price_per_m2_usd"] = price_per_m2_uah / rate

    # Ціна за га — лише за умови наявності площі землі в гектарах
    if area_ha and area_ha > 0:
        price_per_ha_uah = price_uah / area_ha
        metrics["price_per_ha_uah"] = price_per_ha_uah
        if rate and rate > 0:
            metrics["price_per_ha_usd"] = price_per_ha_uah / rate

    return metrics

