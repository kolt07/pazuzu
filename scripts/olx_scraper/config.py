# -*- coding: utf-8 -*-
"""
Налаштування скрапера OLX.
Заходи антибот: один запит за раз, затримка перед запитом, реалістичні заголовки.
"""

import os
import random
from typing import Optional

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

# Сортування: найновіші спочатку (параметр у URL)
OLX_SORT_NEWEST = "search[order]=created_at:desc"

# Затримка перед єдиним запитом (секунди) — імітація людини
DELAY_BEFORE_REQUEST_MIN = float(os.getenv("OLX_SCRAPER_DELAY_MIN", "2"))
DELAY_BEFORE_REQUEST_MAX = float(os.getenv("OLX_SCRAPER_DELAY_MAX", "5"))
# Затримка між запитами сторінки оголошення (деталі) — обережніше, 2–10 с
DELAY_DETAIL_MIN = float(os.getenv("OLX_SCRAPER_DELAY_DETAIL_MIN", "2"))
DELAY_DETAIL_MAX = float(os.getenv("OLX_SCRAPER_DELAY_DETAIL_MAX", "10"))

# Кількість сторінок пошуку, якщо не задано days (зупинка по даті). При заданому days обмеження не використовується.
MAX_SEARCH_PAGES = int(os.getenv("OLX_SCRAPER_MAX_PAGES", "100"))

# Таймаут одного запиту (секунди)
REQUEST_TIMEOUT = int(os.getenv("OLX_SCRAPER_TIMEOUT", "25"))

# User-Agent — звичайний браузер, не бот
USER_AGENT = os.getenv(
    "OLX_SCRAPER_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)

# Каталог для збереження результатів (відносно кореня проекту або скрипта)
OUTPUT_DIR = os.getenv("OLX_SCRAPER_OUTPUT_DIR", "output")
OUTPUT_FILENAME = os.getenv("OLX_SCRAPER_OUTPUT_FILE", "olx_nedvizhimost_page1.json")


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


def get_commercial_real_estate_list_url(page: int = 1, sale_only: bool = True, sort_newest: bool = True) -> str:
    """
    Повертає URL сторінки списку нежитлової (комерційної) нерухомості.
    sale_only: тільки оголошення про продаж (без оренди).
    sort_newest: сортування «Найновіші спочатку».
    """
    path = (COMMERCIAL_REAL_ESTATE_SALE_PATH if sale_only else COMMERCIAL_REAL_ESTATE_PATH).rstrip("/")
    base = f"{BASE_URL.rstrip('/')}{path}/"
    params = []
    if page > 1:
        params.append(f"page={page}")
    if sort_newest:
        params.append(OLX_SORT_NEWEST)
    if params:
        return base + "?" + "&".join(params)
    return base


def get_land_list_url(page: int = 1) -> str:
    """Повертає URL сторінки списку земельних ділянок (земля, включно з ділянками під забудову)."""
    path = LAND_PATH.rstrip("/")
    if page <= 1:
        return f"{BASE_URL.rstrip('/')}{path}/"
    return f"{BASE_URL.rstrip('/')}{path}/?page={page}"
