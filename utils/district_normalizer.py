# -*- coding: utf-8 -*-
"""
Нормалізація назв районів міст для збігу з даними в БД.

Google Maps та різні джерела можуть повертати варіанти: "Солом'янський", "Соломянський",
"Солом'янський район". Користувач може писати "Соломянському" (давальний відмінок).
"""

import re
from typing import List, Optional, Tuple

# Київ: варіанти написання → канонічна назва для фільтра (без "район")
KYIV_DISTRICTS_MAP = {
    "деснянський": "Деснянський",
    "святошинський": "Святошинський",
    "дніпровський": "Дніпровський",
    "печерський": "Печерський",
    "голосіївський": "Голосіївський",
    "дарницький": "Дарницький",
    "солом'янський": "Солом'янський",
    "соломянський": "Солом'янський",
    "солом'янському": "Солом'янський",
    "соломянському": "Солом'янський",
    "оболонський": "Оболонський",
    "шевченківський": "Шевченківський",
    "подільський": "Подільський",
}


def normalize_district_for_kyiv(user_input: str) -> Optional[str]:
    """
    Нормалізує назву району Києва з тексту користувача.

    Args:
        user_input: Текст типу "Соломянському", "Солом'янський", "в Соломянському районі"

    Returns:
        Канонічна назва ("Солом'янський") або None
    """
    if not user_input or not isinstance(user_input, str):
        return None
    text = user_input.strip().lower()
    # Прибираємо "район", "районі", "в", "у"
    text = re.sub(r"\b(район[аиуі]?|в|у)\b", "", text, flags=re.IGNORECASE).strip()
    return KYIV_DISTRICTS_MAP.get(text)


def extract_district_from_query(query: str, city: str = "Київ") -> Optional[str]:
    """
    Витягує назву району з запиту користувача для заданого міста.

    Патерни: "в Соломянському районі", "Солом'янський район", "район Соломянський".

    Returns:
        Канонічна назва району або None
    """
    if city != "Київ":
        return None
    q = (query or "").strip().lower()
    # Шукаємо згадку району
    for variant, canonical in KYIV_DISTRICTS_MAP.items():
        if variant in q:
            return canonical
    return None


def split_city_and_district(city_value: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Розділяє рядок типу "Київ, Солом'янський" на city та district.
    LLM іноді повертає їх об'єднаними в одному полі city.

    Returns:
        (city, district) — окремі значення; якщо район не виявлено — (city, None)
    """
    if not city_value or not isinstance(city_value, str):
        return (city_value, None)
    s = city_value.strip()
    if ", " not in s:
        return (s or None, None)
    parts = [p.strip() for p in s.split(",", 1)]
    if len(parts) != 2:
        return (s or None, None)
    city_part, district_part = parts[0], parts[1]
    if not district_part:
        return (city_part or None, None)
    canonical = normalize_district_for_kyiv(district_part)
    if canonical and "київ" in (city_part or "").lower():
        return (city_part, canonical)
    return (city_part or None, district_part or None)


def get_district_filter_value(normalized: str) -> dict:
    """
    Повертає MongoDB-фільтр для city_district з урахуванням варіантів написання.

    Якщо в БД може бути "Солом'янський" або "Соломянський" — regex з опціональним апострофом.
    """
    if not normalized:
        return {}
    # Для "Солом'янський" — regex Солом'?янський (апостроф опціональний)
    for apostrophe in ("'", "'", "`"):
        if apostrophe in normalized:
            parts = normalized.split(apostrophe, 1)
            # В regex '?' після символу = 0 або 1 раз
            pattern = re.escape(parts[0]) + re.escape(apostrophe) + "?" + re.escape(parts[1])
            return {"$regex": f"^{pattern}", "$options": "i"}
    return {"$regex": f"^{re.escape(normalized)}", "$options": "i"}
