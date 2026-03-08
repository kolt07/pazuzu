# -*- coding: utf-8 -*-
"""
Налаштування скрапера OLX.
Заходи антибот: один запит за раз, затримка перед запитом, реалістичні заголовки.
"""

import os
import random
from pathlib import Path
from urllib.parse import urlencode
from typing import Dict, List, Optional, Tuple

# Шлях до конфігу областей OLX (відносно кореня проекту)
_OLX_REGION_SLUGS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "olx_region_slugs.yaml"
_OLX_LAND_TYPE_SLUGS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "olx_land_type_slugs.yaml"
_OLX_COMM_OBJECT_TYPE_SLUGS_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "olx_commercial_object_type_slugs.yaml"

# Базовий URL сайту (без trailing slash)
BASE_URL = os.getenv("OLX_SCRAPER_BASE_URL", "https://www.olx.ua")
# Перша сторінка розділу "Нерухомість"
REAL_ESTATE_FIRST_PAGE_PATH = "/uk/nedvizhimost/"
# Нежитлова (комерційна) нерухомість (всі оголошення)
COMMERCIAL_REAL_ESTATE_PATH = "/uk/nedvizhimost/kommercheskaya-nedvizhimost/"
# Тільки продаж комерційної нерухомості (без оренди)
# Алгоритм: сторінка нерухомості → фільтр «продаж нежитлової» → сортування від найновіших
COMMERCIAL_REAL_ESTATE_SALE_PATH = "/uk/nedvizhimost/kommercheskaya-nedvizhimost/prodazha-kommercheskoy-nedvizhimosti/"
# Земельні ділянки (включно з ділянками під забудову)
LAND_PATH = "/uk/nedvizhimost/zemlya/"
# Продаж землі (підкатегорія з фільтром за типом: житлова, оздоровча, рекреаційна тощо)
LAND_SALE_PATH = "/uk/nedvizhimost/zemlya/prodazha-zemli/"

# Сортування: найновіші спочатку (параметр у URL)
OLX_SORT_NEWEST = "search[order]=created_at:desc"

# Додаткові фільтри на OLX (параметри search[...] у URL)
# Нерухомість: площа від N м², поверх від-до
FILTER_REAL_ESTATE_TOTAL_AREA_FROM_M2 = int(os.getenv("OLX_FILTER_TOTAL_AREA_FROM", "200"))
FILTER_REAL_ESTATE_FLOOR_FROM = int(os.getenv("OLX_FILTER_FLOOR_FROM", "1"))
FILTER_REAL_ESTATE_FLOOR_TO = int(os.getenv("OLX_FILTER_FLOOR_TO", "2"))
# Земля: площа від N соток
FILTER_LAND_AREA_FROM_SOTOK = float(os.getenv("OLX_FILTER_LAND_AREA_FROM_SOTOK", "15"))

# Затримка перед єдиним запитом (секунди) — імітація людини
DELAY_BEFORE_REQUEST_MIN = float(os.getenv("OLX_SCRAPER_DELAY_MIN", "2"))
DELAY_BEFORE_REQUEST_MAX = float(os.getenv("OLX_SCRAPER_DELAY_MAX", "5"))
# Затримка між запитами сторінки оголошення (деталі) — обережніше, 2–10 с
DELAY_DETAIL_MIN = float(os.getenv("OLX_SCRAPER_DELAY_DETAIL_MIN", "2"))
DELAY_DETAIL_MAX = float(os.getenv("OLX_SCRAPER_DELAY_DETAIL_MAX", "10"))

# Використовувати браузер (Playwright) замість HTTP-запитів для сторінок пошуку та деталей.
# 1/true/yes — клікер; інакше — стара логіка (requests). При увімкненні області обробляються послідовно (один браузер).
USE_BROWSER = (os.getenv("OLX_SCRAPER_USE_BROWSER", "").strip().lower() in ("1", "true", "yes"))

# Кількість сторінок пошуку на один пошуковий запит. OLX перенаправляє на 25-ту сторінку при спробі
# перейти далі — обмеження платформи. Для більшого обсягу використовуємо пошук по областях.
MAX_SEARCH_PAGES = int(os.getenv("OLX_SCRAPER_MAX_PAGES", "25"))

# Максимальна кількість областей, що обробляються паралельно (потоки). По одному потоку на область.
MAX_PARALLEL_REGIONS = int(os.getenv("OLX_SCRAPER_MAX_PARALLEL_REGIONS", "25"))

# Таймаут одного запиту (секунди)
REQUEST_TIMEOUT = int(os.getenv("OLX_SCRAPER_TIMEOUT", "25"))
# Таймаут для сторінки оголошення (деталі), с — браузер (клікер) та requests; OLX іноді повільно віддає
REQUEST_DETAIL_TIMEOUT = int(os.getenv("OLX_SCRAPER_DETAIL_TIMEOUT", "90"))

# Затримка після отримання сторінки (секунди) — OLX може підвантажувати контент з затримкою
DELAY_AFTER_PAGE_LOAD = float(os.getenv("OLX_SCRAPER_DELAY_AFTER_LOAD", "3"))
# Кількість повторних спроб при 0 оголошень на сторінці
RETRY_EMPTY_PAGE_COUNT = int(os.getenv("OLX_SCRAPER_RETRY_EMPTY", "2"))

# User-Agent — звичайний браузер, не бот
USER_AGENT = os.getenv(
    "OLX_SCRAPER_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Куки для запитів (обхід антиботу: підставити куки з браузера після ручної перевірки).
# OLX_SCRAPER_COOKIES — JSON-рядок списку об'єктів {"name": "..." , "value": "..."}.
# Або OLX_SCRAPER_COOKIES_FILE — шлях до файлу з таким же JSON (один рядок або форматування).
COOKIES_JSON = os.getenv("OLX_SCRAPER_COOKIES", "")
COOKIES_FILE = os.getenv("OLX_SCRAPER_COOKIES_FILE", "")


def get_cookies_for_session() -> List[Dict[str, str]]:
    """
    Повертає список куків для requests: [{"name": "...", "value": "..."}, ...].
    Джерело: змінна OLX_SCRAPER_COOKIES (JSON) або файл OLX_SCRAPER_COOKIES_FILE.
    """
    import json
    raw = COOKIES_JSON.strip()
    if not raw and COOKIES_FILE.strip():
        path = Path(COOKIES_FILE.strip())
        if path.exists():
            try:
                with open(path, "r", encoding="utf-8") as f:
                    raw = f.read().strip()
            except Exception:
                return []
    if not raw:
        return []
    try:
        data = json.loads(raw)
        if isinstance(data, list):
            out = []
            for item in data:
                if isinstance(item, dict) and item.get("name") and "value" in item:
                    out.append({"name": str(item["name"]), "value": str(item["value"])})
            return out
        return []
    except Exception:
        return []

# Каталог для збереження результатів (відносно кореня проекту або скрипта)
OUTPUT_DIR = os.getenv("OLX_SCRAPER_OUTPUT_DIR", "output")
OUTPUT_FILENAME = os.getenv("OLX_SCRAPER_OUTPUT_FILE", "olx_nedvizhimost_page1.json")


def get_olx_region_slugs() -> Dict[str, str]:
    """Повертає словник {назва області: OLX slug}. Завантажує з config/olx_region_slugs.yaml."""
    try:
        import yaml
        if _OLX_REGION_SLUGS_PATH.exists():
            with open(_OLX_REGION_SLUGS_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                return dict(data.get("olx_region_slugs") or {})
    except Exception:
        pass
    return {}


def get_olx_land_type_slugs() -> Dict[str, str]:
    """Повертає словник {назва типу землі: OLX slug}. Завантажує з config/olx_land_type_slugs.yaml.
    Використовується для фільтрації — за замовчуванням без с/г призначення."""
    try:
        import yaml
        if _OLX_LAND_TYPE_SLUGS_PATH.exists():
            with open(_OLX_LAND_TYPE_SLUGS_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                return dict(data.get("olx_land_type_slugs") or {})
    except Exception:
        pass
    return {}


def get_olx_comm_re_object_type_slugs_include() -> List[str]:
    """Повертає список OLX slug типів об'єкта комерційної нерухомості для фільтра «усі крім бізнес-центрів».
    Завантажує з config/olx_commercial_object_type_slugs.yaml (olx_comm_re_object_type_slugs_include).
    Якщо файлу немає — повертає порожній список (фільтр за типом об'єкта не застосовується)."""
    try:
        import yaml
        if _OLX_COMM_OBJECT_TYPE_SLUGS_PATH.exists():
            with open(_OLX_COMM_OBJECT_TYPE_SLUGS_PATH, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
                slugs = data.get("olx_comm_re_object_type_slugs_include") or []
                return list(slugs) if isinstance(slugs, list) else []
    except Exception:
        pass
    return []


def get_delay_seconds() -> float:
    """Повертає випадкову затримку в межах [MIN, MAX]."""
    return random.uniform(DELAY_BEFORE_REQUEST_MIN, DELAY_BEFORE_REQUEST_MAX)


def get_delay_detail_seconds() -> float:
    """Повертає випадкову затримку перед запитом сторінки оголошення (2–10 с)."""
    return random.uniform(DELAY_DETAIL_MIN, DELAY_DETAIL_MAX)


def get_real_estate_list_url(page: int = 1) -> str:
    """Повертає URL першої (або вказаної) сторінки списку нерухомості."""
    if page <= 1:
        return f"{BASE_URL.rstrip('/')}{REAL_ESTATE_FIRST_PAGE_PATH.rstrip('/')}/"
    return f"{BASE_URL.rstrip('/')}{REAL_ESTATE_FIRST_PAGE_PATH.rstrip('/')}/?page={page}"


def _build_category_url(
    base_path: str,
    page: int,
    sort_newest: bool,
    region_slug: Optional[str] = None,
    land_type_slug: Optional[str] = None,
    extra_query_pairs: Optional[List[Tuple[str, str]]] = None,
) -> str:
    """Збирає URL категорії з опціональними фільтрами.
    OLX: path-суфікс /{region_slug}/ для областей; для землі — /{land_type_slug}/{region_slug}/.
    extra_query_pairs: додаткові параметри пошуку (напр. search[filter_float_total_area:from]=200)."""
    path = base_path.rstrip("/")
    if land_type_slug:
        path = f"{path}/{land_type_slug}"
    if region_slug:
        path = f"{path}/{region_slug}"
    base = f"{BASE_URL.rstrip('/')}{path}/"
    pairs: List[Tuple[str, str]] = []
    if page > 1:
        pairs.append(("page", str(page)))
    if sort_newest:
        pairs.append(("search[order]", "created_at:desc"))
    for k, v in extra_query_pairs or []:
        pairs.append((k, v))
    if pairs:
        return base + "?" + urlencode(pairs)
    return base


def get_commercial_real_estate_list_url(
    page: int = 1,
    sale_only: bool = True,
    sort_newest: bool = True,
    region_slug: Optional[str] = None,
) -> str:
    """
    Повертає URL сторінки списку нежитлової (комерційної) нерухомості.
    sale_only: тільки оголошення про продаж (без оренди).
    sort_newest: сортування «Найновіші спочатку».
    region_slug: OLX slug області для фільтрації (напр. kyivskaya, lvivska).
    Додаткові фільтри на OLX: площа від FILTER_REAL_ESTATE_TOTAL_AREA_FROM_M2 м², поверх 1–2,
    тип об'єкта — усі крім бізнес-центрів (з olx_commercial_object_type_slugs.yaml).
    """
    path = COMMERCIAL_REAL_ESTATE_SALE_PATH if sale_only else COMMERCIAL_REAL_ESTATE_PATH
    extra: List[Tuple[str, str]] = [
        ("search[filter_float_total_area:from]", str(FILTER_REAL_ESTATE_TOTAL_AREA_FROM_M2)),
        ("search[filter_float_floor:from]", str(FILTER_REAL_ESTATE_FLOOR_FROM)),
        ("search[filter_float_floor:to]", str(FILTER_REAL_ESTATE_FLOOR_TO)),
    ]
    for slug in get_olx_comm_re_object_type_slugs_include():
        extra.append(("search[filter_enum_comm_re_object_type]", slug))
    return _build_category_url(path, page, sort_newest, region_slug, extra_query_pairs=extra)


def get_land_list_url(
    page: int = 1,
    sort_newest: bool = True,
    region_slug: Optional[str] = None,
    land_type_slug: Optional[str] = None,
) -> str:
    """Повертає URL сторінки списку земельних ділянок.
    land_type_slug: фільтр за типом землі (з olx_land_type_slugs) — виключає с/г при використанні.
    Без land_type_slug — загальна сторінка /zemlya/ (legacy).
    Додатковий фільтр на OLX: площа від FILTER_LAND_AREA_FROM_SOTOK соток."""
    base = LAND_SALE_PATH if land_type_slug else LAND_PATH
    extra = [
        ("search[filter_float_land_area:from]", str(int(FILTER_LAND_AREA_FROM_SOTOK))),
    ]
    return _build_category_url(base, page, sort_newest, region_slug, land_type_slug, extra_query_pairs=extra)
