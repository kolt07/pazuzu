# -*- coding: utf-8 -*-
"""
Regex-екстрактор структурованої інформації з опису оголошення нерухомості.

Витягує без LLM: cadastral_number, building_area_sqm, land_area_ha, базову адресу.
Використовується як перший крок у каскадному підході (regex → LLM).
"""

import re
from typing import Any, Dict, List, Optional


# Кадастровий номер: НКЗ:НКК:НЗД (напр. 6320685503:03:000:0202)
_CADASTRAL_PATTERN = re.compile(
    r'\b(\d{10,12}(?::\d{1,4}){2,3})\b',
    re.IGNORECASE
)

# Площа нерухомості (м²): "65 м²", "120 кв.м", "площа 65", "65м2"
_BUILDING_AREA_PATTERNS = [
    re.compile(r'(?:площа|площею|загальна\s+площа|житлова|корисна)[:\s]*(\d[\d\s]*[.,]?\d*)\s*(?:м²|м2|кв\.?\s*м)', re.IGNORECASE),
    re.compile(r'(\d[\d\s]*[.,]?\d*)\s*(?:м²|м2|кв\.?\s*м)', re.IGNORECASE),
    re.compile(r'(?:площа|площею)[:\s]*(\d[\d\s]*[.,]?\d*)', re.IGNORECASE),
]

# Площа землі (га): "0.5 га", "5 гектар", "10 соток"
_LAND_AREA_PATTERNS = [
    re.compile(r'(\d[\d\s]*[.,]?\d*)\s*(?:га|гектар|гектарів)', re.IGNORECASE),
    re.compile(r'(?:площа\s+ділянки|площа\s+землі|земельна\s+ділянка)[:\s]*(\d[\d\s]*[.,]?\d*)\s*(?:га|м²|м2)?', re.IGNORECASE),
    re.compile(r'(\d[\d\s]*[.,]?\d*)\s*соток(?:\s+землі)?', re.IGNORECASE),
]

# Область: "Харківська область", "Київська обл."
_REGION_PATTERN = re.compile(
    r'(?:^|[,.\s])([А-Яа-яІіЇїЄєҐґ]+(?:ська|цька|цька|ська|ська)\s+область|'
    r'[А-Яа-яІіЇїЄєҐґ]+(?:ська|цька)\s+обл\.?)',
    re.IGNORECASE
)

# Населений пункт: "м. Київ", "с. Верхньоводяне", "смт. Бориспіль"
_SETTLEMENT_PATTERN = re.compile(
    r'(?:м\.|с\.|смт\.|с-ще\.?)\s*([А-Яа-яІіЇїЄєҐґ][А-Яа-яІіЇїЄєҐґ\s\-]+?)(?:\s|,|$)',
    re.IGNORECASE
)

# Вулиця: "вул. Незалежності", "вулиця Хрещатик"
_STREET_PATTERN = re.compile(
    r'(?:вул\.|вулиця|просп\.|бул\.|пров\.|пл\.)\s*([А-Яа-яІіЇїЄєҐґ][А-Яа-яІіЇїЄєҐґ\s\-]+?)(?:\s*,|\s+(\d+[а-яіїєґ]*)?|$)',
    re.IGNORECASE
)

# Номер будинку після вулиці або в кінці рядка
_BUILDING_PATTERN = re.compile(
    r'(?:будинок|буд\.|№)\s*(\d+[а-яіїєґА-ЯІЇЄҐ]*)|'
    r',\s*(\d+[а-яіїєґА-ЯІЇЄҐ]*)(?:\s|,|$)|'
    r'(?:вул\.|вулиця)[^,]+,\s*(\d+)',
    re.IGNORECASE
)


def _parse_float(value: str) -> Optional[float]:
    """Парсить число з українського формату (пробіли, кома)."""
    if not value:
        return None
    cleaned = str(value).replace(' ', '').replace(',', '.')
    try:
        return float(cleaned)
    except ValueError:
        return None


def _normalize_region(region: str) -> str:
    """Нормалізує назву області (без 'область', 'обл.')."""
    if not region:
        return ''
    r = region.strip()
    for suffix in [' область', ' обл.', ' обл']:
        if r.lower().endswith(suffix.lower()):
            return r[:-len(suffix)].strip()
    return r


def extract_cadastral_number(text: str) -> str:
    """Витягує кадастровий номер з тексту."""
    if not text:
        return ''
    match = _CADASTRAL_PATTERN.search(text)
    if match:
        return match.group(1).strip()
    return ''


def extract_building_area_sqm(text: str) -> str:
    """
    Витягує площу нерухомості (будівель/приміщень) в м².
    Повертає порожній рядок, якщо не знайдено або це площа землі.
    """
    if not text:
        return ''
    # Пропускаємо контекст "площа землі", "площа ділянки"
    if re.search(r'площа\s+(землі|ділянки|земельної)', text, re.IGNORECASE):
        pass  # Можливо є ще площа будівлі в іншому місці
    values = []
    for pat in _BUILDING_AREA_PATTERNS:
        for m in pat.finditer(text):
            val = _parse_float(m.group(1))
            if val is not None and 0.1 < val < 100000000:  # Реалістичний діапазон
                values.append(val)
    if not values:
        return ''
    return str(sum(values))


def extract_land_area_ha(text: str) -> str:
    """Витягує площу земельної ділянки в гектарах."""
    if not text:
        return ''
    values = []
    for pat in _LAND_AREA_PATTERNS:
        for m in pat.finditer(text):
            val = _parse_float(m.group(1))
            if val is not None and 0.001 < val < 100000:
                # Сотки -> га (100 соток = 1 га)
                if 'соток' in (m.group(0) or '').lower():
                    val = val * 0.01
                values.append(val)
    if not values:
        return ''
    return str(sum(values))


def extract_addresses(text: str) -> List[Dict[str, Any]]:
    """
    Витягує базову адресу (region, settlement, street, building).
    Формат: [{region, district, settlement_type, settlement, street_type, street, building, ...}]
    """
    if not text:
        return []
    addr = {}
    # Область
    m_region = _REGION_PATTERN.search(text)
    if m_region:
        addr['region'] = _normalize_region(m_region.group(1).strip())
    # Населений пункт
    m_settlement = _SETTLEMENT_PATTERN.search(text)
    if m_settlement:
        addr['settlement'] = m_settlement.group(1).strip()
    # Вулиця
    m_street = _STREET_PATTERN.search(text)
    if m_street:
        addr['street'] = m_street.group(1).strip() if m_street.group(1) else ''
        if m_street.group(2):
            addr['building'] = m_street.group(2).strip()
    # Номер будинку (fallback)
    if 'building' not in addr:
        m_building = _BUILDING_PATTERN.search(text)
        if m_building:
            addr['building'] = (m_building.group(1) or m_building.group(2) or m_building.group(3) or '').strip()
    if not addr:
        return []
    # Доповнюємо порожні поля
    for key in ['region', 'district', 'settlement_type', 'settlement', 'street_type', 'street', 'building', 'building_part', 'room']:
        if key not in addr:
            addr[key] = ''
    return [addr]


def extract_floor(text: str) -> str:
    """Витягує поверх (якщо є)."""
    if not text:
        return ''
    m = re.search(r'(?:поверх|на\s+(\d+)\s*поверсі|(\d+)\s*поверх)', text, re.IGNORECASE)
    if m:
        return (m.group(1) or m.group(2) or '').strip()
    return ''


def extract_property_type(text: str) -> str:
    """Визначає тип нерухомості за ключовими словами."""
    if not text:
        return ''
    t = text.lower()
    if 'земельна ділянка' in t or 'землі житлової забудови' in t or 'земля під будівництво' in t:
        return 'Земля під будівництво'
    if 'с/г' in t or 'сільськогосподарськ' in t:
        return 'Землі с/г призначення'
    if 'нерухомість' in t or 'земельна ділянка з нерухомістю' in t:
        return 'Нерухомість'
    return ''


def extract_utilities(text: str) -> str:
    """Витягує комунікації (через кому)."""
    if not text:
        return ''
    utils = []
    if re.search(r'електропостачання|електрика|світло', text, re.IGNORECASE):
        utils.append('електрика')
    if re.search(r'водопостачання|вода|водопровід', text, re.IGNORECASE):
        utils.append('вода')
    if re.search(r'газопостачання|газ', text, re.IGNORECASE):
        utils.append('газ')
    if re.search(r'каналізація|каналізація', text, re.IGNORECASE):
        utils.append('каналізація')
    if re.search(r'опалення|тепло', text, re.IGNORECASE):
        utils.append('опалення')
    if re.search(r'відсутні|не підведені', text, re.IGNORECASE):
        return 'відсутні'
    return ', '.join(utils) if utils else ''


def extract_tags(text: str) -> List[str]:
    """Витягує теги за ключовими словами."""
    if not text:
        return []
    t = text.lower()
    tags = []
    tag_map = {
        'крамниця': ['крамниця', 'магазин'],
        'аптека': ['аптека'],
        'офіс': ['офіс'],
        'склад': ['склад', 'складське'],
        'кафе': ['кафе'],
        'ресторан': ['ресторан'],
        'газ': ['газ'],
        'вода': ['вода'],
        'електрика': ['електрика', 'світло'],
        'каналізація': ['каналізація'],
        'опалення': ['опалення'],
    }
    for tag, keywords in tag_map.items():
        if any(kw in t for kw in keywords) and tag not in tags:
            tags.append(tag)
    return tags


def extract_from_description(text: str) -> Dict[str, Any]:
    """
    Повний regex-екстрактор. Повертає структуру, сумісну з parse_auction_description.
    """
    if not text or not str(text).strip():
        return _empty_result()
    return {
        'cadastral_number': extract_cadastral_number(text),
        'building_area_sqm': extract_building_area_sqm(text),
        'land_area_ha': extract_land_area_ha(text),
        'addresses': extract_addresses(text),
        'floor': extract_floor(text),
        'property_type': extract_property_type(text),
        'utilities': extract_utilities(text),
        'tags': extract_tags(text),
        'arrests_info': '',  # Regex не покриває
    }


def _empty_result() -> Dict[str, Any]:
    """Порожній результат (сумісний з LLM)."""
    return {
        'cadastral_number': '',
        'building_area_sqm': '',
        'land_area_ha': '',
        'addresses': [],
        'floor': '',
        'property_type': '',
        'utilities': '',
        'tags': [],
        'arrests_info': '',
    }


def can_skip_llm(regex_result: Dict[str, Any], min_fields: Optional[List[str]] = None) -> bool:
    """
    Визначає, чи достатньо результатів regex для пропуску LLM.
    min_fields: список полів, які мають бути заповнені (за замовчуванням: площа або адреса).
    """
    if min_fields is None:
        min_fields = ['building_area_sqm', 'land_area_ha', 'addresses']
    for field in min_fields:
        val = regex_result.get(field)
        if field == 'addresses':
            if isinstance(val, list) and len(val) > 0:
                return True
        elif val and str(val).strip():
            return True
    return False
