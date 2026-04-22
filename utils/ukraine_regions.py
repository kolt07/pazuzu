# -*- coding: utf-8 -*-
"""
Канонічні назви областей України та утиліти нормалізації топонімів регіону.
Використовується для:
- стабільного списку опцій у UI-фільтрах;
- уніфікованого пошуку за областю незалежно від варіантів запису.
"""

from __future__ import annotations

import re
from typing import List, Optional

# Вичерпний перелік регіонів України у міжнародно визнаних межах.
# Формат для UI: "X область" + окремі адміністративні одиниці зі спецстатусом.
UA_REGION_OPTIONS: List[str] = [
    "Вінницька область",
    "Волинська область",
    "Дніпропетровська область",
    "Донецька область",
    "Житомирська область",
    "Закарпатська область",
    "Запорізька область",
    "Івано-Франківська область",
    "Київська область",
    "Кіровоградська область",
    "Луганська область",
    "Львівська область",
    "Миколаївська область",
    "Одеська область",
    "Полтавська область",
    "Рівненська область",
    "Сумська область",
    "Тернопільська область",
    "Харківська область",
    "Херсонська область",
    "Хмельницька область",
    "Черкаська область",
    "Чернівецька область",
    "Чернігівська область",
    "АР Крим",
    "м. Київ",
    "м. Севастополь",
]

_SPECIAL_CITY_CANONICAL = {
    "київ": "м. Київ",
    "м київ": "м. Київ",
    "м. київ": "м. Київ",
    "київ місто": "м. Київ",
    "севастополь": "м. Севастополь",
    "м севастополь": "м. Севастополь",
    "м. севастополь": "м. Севастополь",
}

_CRIMEA_KEYS = {"аркрим", "ар крим", "автономнареспублікакрим", "автономна республіка крим", "крим"}

_OBLAST_SHORT_TO_CANONICAL = {
    "вінницька": "Вінницька область",
    "волинська": "Волинська область",
    "дніпропетровська": "Дніпропетровська область",
    "донецька": "Донецька область",
    "житомирська": "Житомирська область",
    "закарпатська": "Закарпатська область",
    "запорізька": "Запорізька область",
    "іванофранківська": "Івано-Франківська область",
    "київська": "Київська область",
    "кіровоградська": "Кіровоградська область",
    "луганська": "Луганська область",
    "львівська": "Львівська область",
    "миколаївська": "Миколаївська область",
    "одеська": "Одеська область",
    "полтавська": "Полтавська область",
    "рівненська": "Рівненська область",
    "сумська": "Сумська область",
    "тернопільська": "Тернопільська область",
    "харківська": "Харківська область",
    "херсонська": "Херсонська область",
    "хмельницька": "Хмельницька область",
    "черкаська": "Черкаська область",
    "чернівецька": "Чернівецька область",
    "чернігівська": "Чернігівська область",
}

_SPECIAL_CITY_TO_CITY = {
    "м. Київ": "Київ",
    "м. Севастополь": "Севастополь",
}


def get_ua_region_options() -> List[str]:
    """Повертає стабільний список регіонів для UI-фільтра."""
    return list(UA_REGION_OPTIONS)


def normalize_region_to_canonical(value: Optional[str]) -> Optional[str]:
    """
    Нормалізує довільне значення регіону до канонічного формату для UI.
    Наприклад: "Чернігівська обл." -> "Чернігівська область".
    """
    if not value or not isinstance(value, str):
        return None

    raw = value.strip()
    if not raw:
        return None

    key = _normalize_key(raw)
    if key in _SPECIAL_CITY_CANONICAL:
        return _SPECIAL_CITY_CANONICAL[key]
    if key in _CRIMEA_KEYS:
        return "АР Крим"

    normalized = raw.lower().strip()
    normalized = re.sub(r"^\s*(?:о\.|обл\.?|область)\s+", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+(?:обл\.?|область|області)\s*$", "", normalized, flags=re.IGNORECASE)
    normalized = re.sub(r"\s+", " ", normalized).strip()
    normalized = _normalize_oblast_inflection(normalized)
    normalized_no_punct = normalized.replace("-", "").replace(" ", "")

    return _OBLAST_SHORT_TO_CANONICAL.get(normalized_no_punct)


def normalize_region_for_repository_lookup(value: Optional[str]) -> Optional[str]:
    """
    Повертає назву регіону у форматі, що очікують regions/cities репозиторії.
    Для областей — коротка форма без слова "область".
    """
    canonical = normalize_region_to_canonical(value)
    if not canonical:
        return value.strip() if isinstance(value, str) and value.strip() else None
    if canonical in _SPECIAL_CITY_TO_CITY:
        return _SPECIAL_CITY_TO_CITY[canonical]
    if canonical == "АР Крим":
        return "АР Крим"
    return canonical.replace(" область", "")


def is_special_city_region(value: Optional[str]) -> bool:
    canonical = normalize_region_to_canonical(value)
    return canonical in _SPECIAL_CITY_TO_CITY


def special_city_from_region(value: Optional[str]) -> Optional[str]:
    canonical = normalize_region_to_canonical(value)
    return _SPECIAL_CITY_TO_CITY.get(canonical) if canonical else None


def build_region_search_regex(value: Optional[str]) -> Optional[str]:
    """
    Будує regex для пошуку за регіоном з підтримкою синонімів:
    "Чернігівська область" == "Чернігівська обл." == "о. Чернігівська".
    """
    canonical = normalize_region_to_canonical(value)
    if not canonical:
        return None

    if canonical == "АР Крим":
        return r"(?:^|[\s,;:()-])(?:ар\s*крим|автономна\s+республіка\s+крим|крим)(?:$|[\s,;:()-])"
    if canonical == "м. Київ":
        return r"(?:^|[\s,;:()-])(?:м\.\s*)?київ(?:$|[\s,;:()-])"
    if canonical == "м. Севастополь":
        return r"(?:^|[\s,;:()-])(?:м\.\s*)?севастополь(?:$|[\s,;:()-])"

    short = canonical.replace(" область", "")
    stem = short[:-1] if short.endswith("а") else short
    escaped_stem = re.escape(stem)
    return (
        rf"(?:^|[\s,;:()-])"
        rf"(?:о\.\s*|обл\.?\s*)?"
        rf"{escaped_stem}(?:а|ої|у|ою|ій)?"
        rf"(?:\s*(?:область|обл\.?|області))?"
        rf"(?:$|[\s,;:()-])"
    )


def _normalize_key(value: str) -> str:
    v = value.lower().replace("-", " ").strip()
    v = re.sub(r"[^\w\s.]", "", v, flags=re.UNICODE)
    v = re.sub(r"\s+", " ", v).strip()
    return v


def _normalize_oblast_inflection(value: str) -> str:
    # Чернігівської / Чернігівську / Чернігівською / Чернігівській -> Чернігівська
    return re.sub(r"(.+ськ)(?:ої|у|ою|ій)$", r"\1а", value, flags=re.IGNORECASE)
