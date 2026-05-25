# -*- coding: utf-8 -*-
"""Налаштування скрапера mista.ua."""

import os
import random

BASE_URL = os.getenv("MISTA_SCRAPER_BASE_URL", "https://mista.ua").rstrip("/")
# Шлях українською (GET ?obl=ID працює лише з цим шляхом, не з percent-encoded)
SETTLEMENTS_SEARCH_PATH = "Пошук_населених_пунктів"
SETTLEMENTS_LIST_PATH = "/%D0%9F%D0%BE%D1%88%D1%83%D0%BA_%D0%BD%D0%B0%D1%81%D0%B5%D0%BB%D0%B5%D0%BD%D0%B8%D1%85_%D0%BF%D1%83%D0%BD%D0%BA%D1%82%D1%96%D0%B2"

DELAY_BEFORE_REQUEST_MIN = float(os.getenv("MISTA_SCRAPER_DELAY_MIN", "2"))
DELAY_BEFORE_REQUEST_MAX = float(os.getenv("MISTA_SCRAPER_DELAY_MAX", "5"))
REQUEST_TIMEOUT = int(os.getenv("MISTA_SCRAPER_TIMEOUT", "45"))
MAX_RETRIES = int(os.getenv("MISTA_SCRAPER_MAX_RETRIES", "3"))

USER_AGENT = os.getenv(
    "MISTA_SCRAPER_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
    "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
)


def get_delay_seconds() -> float:
    return random.uniform(DELAY_BEFORE_REQUEST_MIN, DELAY_BEFORE_REQUEST_MAX)


# Антибот-cookie з JS-сторінки (fallback, якщо не вдалося витягти з HTML)
MISTA_ZB_COOKIE = os.getenv(
    "MISTA_SCRAPER_ZB_COOKIE",
    "M9cRO9MN18JwMVD51LZ21yZxM9ouM9o2",
)

# Параметри AJAX-пагінації списку (див. citySearchEl.pagination на mista.ua)
LIST_AJAX_RELOAD = "ajax"
LIST_AJAX_CSCONTENT = "1"
LIST_ROWS_PER_PAGE = 40


def settlements_list_url(obl_id: int | None = None) -> str:
    """
    URL сторінки пошуку НП.

    З фільтром області: GET/POST на /Пошук_населених_пунктів/?obl=<id>
    (див. dropdown «Область» на mista.ua).
    """
    base = f"{BASE_URL}/{SETTLEMENTS_SEARCH_PATH}"
    if obl_id is not None:
        return f"{base}/?obl={int(obl_id)}"
    return base


def settlements_list_post_url() -> str:
    """URL для AJAX POST пагінації (encoded path, як у citySearchEl.pagination)."""
    return f"{BASE_URL}{SETTLEMENTS_LIST_PATH}"


def settlements_list_ajax_params(page: int = 1, *, obl_id: int | None = None) -> dict[str, str]:
    """
    POST-параметри для сторінки списку (1-based page → citySPG 0-based).
    Сайт не підтримує ?page=N (404); використовує ?citySPG=N лише в AJAX.
    """
    params = {
        "reload": LIST_AJAX_RELOAD,
        "cscontent": LIST_AJAX_CSCONTENT,
        "citySPG": str(max(0, page - 1)),
    }
    if obl_id is not None:
        params["obl"] = str(int(obl_id))
    return params
