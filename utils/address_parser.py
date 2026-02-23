# -*- coding: utf-8 -*-
"""
Утиліти для парсингу адрес з різних джерел (ProZorro streetAddress, OLX тощо).
Витягує вулицю та номер будинку з рядка типу "вул. Соборності, 7" або "вул. Хрещатик 15".
"""

from typing import Any, Dict, Optional, Tuple


def parse_street_address(street_address: Any) -> Tuple[str, Optional[str]]:
    """
    Парсить рядок streetAddress (вулиця + опційно номер будинку) на окремі частини.

    Підтримує формати:
    - "вул. Соборності, 7"
    - "вул. Соборності 7"
    - "вул. Соборності, 7 корпус А"
    - "вулиця Івана Богуна" (без номера)
    - "Хрещатик, 15"

    Args:
        street_address: Рядок або словник з uk_UA/en_US (multilingual)

    Returns:
        (street_name, building_number): назва вулиці (без типу) та номер будинку або None
    """
    if not street_address:
        return "", None
    if isinstance(street_address, dict):
        street_ua = (
            street_address.get("uk_UA")
            or street_address.get("en_US")
            or ""
        ).strip()
    else:
        street_ua = str(street_address).strip()
    if not street_ua:
        return "", None

    # Розділяємо за комою: "вул. Соборності, 7" -> street_part, building_part
    street_parts = street_ua.split(",", 1)
    street_part = street_parts[0].strip()
    building_part = street_parts[1].strip() if len(street_parts) > 1 else ""

    # Прибираємо тип вулиці з початку
    street_type_patterns = [
        "вулиця ",
        "вул. ",
        "вул ",
        "просп. ",
        "просп ",
        "бул. ",
        "бул ",
        "пров. ",
        "пров ",
        "пл. ",
        "пл ",
    ]
    street_name = street_part
    for pattern in street_type_patterns:
        if street_part.lower().startswith(pattern.lower()):
            street_name = street_part[len(pattern) :].strip()
            break

    building_number: Optional[str] = None
    if building_part:
        # "7", "7 корпус А", "7, корпус А" -> беремо перше слово як номер
        building_part = building_part.replace(",", " ").strip()
        building_parts = building_part.split(None, 1)
        first = building_parts[0] if building_parts else ""
        if first and (first.isdigit() or (len(first) > 1 and first[0].isdigit())):
            building_number = first
    else:
        # Номер в кінці назви: "Соборності 7"
        words = street_name.split()
        if words and words[-1].isdigit():
            building_number = words[-1]
            street_name = " ".join(words[:-1])

    return street_name, building_number


def parse_prozorro_item_address(address: Dict[str, Any]) -> Dict[str, Any]:
    """
    Витягує структуровану адресу з item.address ProZorro (region, locality, streetAddress).

    Returns:
        Dict з region, settlement, street, building, formatted_address, formatted_address
        (building може бути None якщо не вдалося витягти)
    """
    result: Dict[str, Any] = {
        "region": None,
        "settlement": None,
        "street": None,
        "building": None,
        "formatted_address": None,
    }
    if not address or not isinstance(address, dict):
        return result

    def _get_ua(obj: Any) -> str:
        if isinstance(obj, dict):
            return (obj.get("uk_UA") or obj.get("en_US") or "").strip()
        return str(obj).strip() if obj else ""

    region = _get_ua(address.get("region"))
    if region:
        region = region.replace(" область", "").replace(" обл.", "").strip()
    locality = _get_ua(address.get("locality"))
    street_address_raw = address.get("streetAddress")
    street_name, building_number = parse_street_address(street_address_raw)

    result["region"] = region or None
    result["settlement"] = locality or None
    result["street"] = street_name or None
    result["building"] = building_number

    parts = [p for p in [region, locality] if p]
    street_display = _get_ua(street_address_raw) if street_address_raw else street_name
    if street_display:
        parts.append(street_display)
    result["formatted_address"] = ", ".join(parts) if parts else None

    return result
