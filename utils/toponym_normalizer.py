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
    
    Правила (як при завантаженні з ProZorro):
    - Прибрати " область", " обл.", " області" (різні відмінки)
    - Волинській → Волинська (привести прикметник до називного відмінку)
    
    Формат у БД: "Київська", "Волинська" (без "область").
    У БД також є "Київська область" — регекс ^Київська їх обох охоплює.
    
    Args:
        value: "Волинській області", "Волинська область", "Київська" тощо
        
    Returns:
        "Волинська", "Київська" — форма як у БД
    """
    if not value or not isinstance(value, str):
        return None
    v = value.strip()
    if not v:
        return None
    
    # Прибираємо "область", "обл.", "області" (з пробілом перед)
    v = re.sub(r'\s+області\s*$', '', v, flags=re.I)
    v = re.sub(r'\s+область\s*$', '', v, flags=re.I)
    v = re.sub(r'\s+обл\.?\s*$', '', v, flags=re.I)
    v = v.strip()
    
    # Прикметник: -ій, -ої, -у, -ою → -а (жіночий рід)
    # Волинській, Київської, Вінницьку → Волинська, Київська, Вінницька
    v = re.sub(r'(.+ськ)(ій|ої|у|ою)$', r'\1а', v, flags=re.I)
    
    # Синонім: якщо є в REGION_SYNONYMS — повертаємо канонічну коротку форму
    # У БД є і "Волинська", і "Волинська область" — коротка форма краще для regex
    short_form = REGION_SYNONYMS.get(v)
    if short_form and " область" not in short_form and " обл." not in short_form:
        return short_form
    # Якщо поточне значення — "X область", повертаємо X
    if v.endswith(" область") or v.endswith(" обл."):
        v = v.replace(" область", "").replace(" обл.", "").strip()
    
    return v if v else None


def normalize_settlement(value: Optional[str]) -> Optional[str]:
    """
    Нормалізує назву населеного пункту до формату в БД.
    
    Правила (interpreter_agent_handbook):
    - settlement без префікса (м., с. тощо), з великої літери
    - У Києві → Київ, в Львові → Львів
    
    Args:
        value: "у Києві", "в Львові", "м. Київ", "Київ" тощо
        
    Returns:
        "Київ", "Львів" — форма як у БД
    """
    if not value or not isinstance(value, str):
        return None
    v = value.strip()
    if not v:
        return None
    
    # Прибираємо зайві префікси попереду
    v = re.sub(r'^(у|в|з|на|до)\s+', '', v, flags=re.I)
    v = re.sub(r'^(м\.|місто|с\.|село|смт\.)\s*', '', v, flags=re.I)
    v = v.strip()
    
    # Локальний відмінок: -і → -ь (Києві → Київ), -ові → -ів (Львові → Львів)
    # Прості підстановки для типових міст
    SETTLEMENT_INFLECTION = {
        "києві": "Київ",
        "львові": "Львів",
        "вінниці": "Вінниця",
        "харкові": "Харків",
        "одесі": "Одеса",
        "дніпрі": "Дніпро",
        "запоріжжі": "Запоріжжя",
        "полтаві": "Полтава",
        "сумах": "Суми",
        "чернігові": "Чернігів",
        "житомирі": "Житомир",
        "черкасах": "Черкаси",
        "тернополі": "Тернопіль",
        "івано-франківську": "Івано-Франківськ",
        "луцьку": "Луцьк",
        "рівному": "Рівне",
        "ужгороді": "Ужгород",
        "миколаєві": "Миколаїв",
        "херсоні": "Херсон",
        "кропивницькому": "Кропивницький",
    }
    v_lower = v.lower()
    if v_lower in SETTLEMENT_INFLECTION:
        return SETTLEMENT_INFLECTION[v_lower]
    
    # Загальний патерн: -і, -у → -а, -о для деяких
    # Якщо значення вже в називному (Київ, Львів) — залишаємо
    if len(v) > 2 and v[-1] in "ійу" and v[-2:] not in ("ей", "ий"):
        # Спрощення: якщо закінчується на "і" — можливо це локатив
        pass
    return v if v else None


def normalize_geo_filter_values(
    city: Optional[str] = None,
    region: Optional[str] = None,
        exclude_city: Optional[str] = None,
    exclude_region: Optional[str] = None,
) -> dict:
    """
    Нормалізує city та region для геофільтра до формату в БД.
    
    Returns:
        dict з city, region, exclude_city, exclude_region (нормалізовані)
    """
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
