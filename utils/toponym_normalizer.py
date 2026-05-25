# -*- coding: utf-8 -*-
"""
Нормалізація топонімів для геофільтрів.
Перетворює значення з запиту користувача у формат, як у БД при завантаженні з джерел.
Правила: ті самі, що в UnifiedListingsService та interpreter_agent_handbook.
"""

import re
from typing import Optional

from utils.schema_filter_resolver import REGION_SYNONYMS


def normalize_region(value: Optional[str]) -> Optional[str]:
    """
    Нормалізує назву області до формату в БД.
    """
    if not value or not isinstance(value, str):
        return None
    v = value.strip()
    if not v:
        return None

    v = re.sub(r'\s+області\s*$', '', v, flags=re.I)
    v = re.sub(r'\s+область\s*$', '', v, flags=re.I)
    v = re.sub(r'\s+обл\.?\s*$', '', v, flags=re.I)
    v = v.strip()

    v = re.sub(r'(.+ськ)(ій|ої|у|ою)$', r'\1а', v, flags=re.I)

    short_form = REGION_SYNONYMS.get(v)
    if short_form and " область" not in short_form and " обл." not in short_form:
        return short_form
    if v.endswith(" область") or v.endswith(" обл."):
        v = v.replace(" область", "").replace(" обл.", "").strip()

    return v if v else None


_SETTLEMENT_INFLECTION = {
    'києві': 'Київ',
    'львові': 'Львів',
    'вінниці': 'Вінниця',
    'харкові': 'Харків',
    'одесі': 'Одеса',
    'дніпрі': 'Дніпро',
    'запоріжжі': 'Запоріжжя',
    'полтаві': 'Полтава',
    'сумах': 'Суми',
    'чернігові': 'Чернігів',
    'житомирі': 'Житомир',
    'черкасах': 'Черкаси',
    'тернополі': 'Тернопіль',
    'івано-франківську': 'Івано-Франківськ',
    'луцьку': 'Луцьк',
    'рівному': 'Рівне',
    'ужгороді': 'Ужгород',
    'миколаєві': 'Миколаїв',
    'херсоні': 'Херсон',
    'кропивницькому': 'Кропивницький',
}


def normalize_settlement(value: Optional[str]) -> Optional[str]:
    """
    Нормалізує назву населеного пункту до формату в БД.
    """
    if not value or not isinstance(value, str):
        return None

    from utils.settlement_normalizer import normalize_settlement_name

    v = value.strip()
    if not v:
        return None
    v_lower = re.sub(r'^(?:у|в|з|на|до)\s+', '', v, flags=re.I).strip().lower()
    if v_lower in _SETTLEMENT_INFLECTION:
        return _SETTLEMENT_INFLECTION[v_lower]

    return normalize_settlement_name(value)


def normalize_geo_filter_values(
    city: Optional[str] = None,
    region: Optional[str] = None,
    exclude_city: Optional[str] = None,
    exclude_region: Optional[str] = None,
) -> dict:
    """Нормалізує city та region для геофільтра до формату в БД."""
    result = {}
    if city:
        n = normalize_settlement(city)
        if n:
            result["city"] = n
    if region:
        n = normalize_region(region)
        if n:
            result["region"] = n
    if exclude_city:
        n = normalize_settlement(exclude_city)
        if n:
            result["exclude_city"] = n
    if exclude_region:
        n = normalize_region(exclude_region)
        if n:
            result["exclude_region"] = n
    return result
