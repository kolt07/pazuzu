# -*- coding: utf-8 -*-
"""
Завантаження сторінок OLX через HTTP (requests).

List-сторінки пошуку — лише через Playwright (browser_fetcher.get_list_page).
Цей модуль лишається для detail-запитів у допоміжних скриптах і reprocess-сервісах.
"""

import time
import sys
from pathlib import Path
from typing import Optional

import requests

# Додаємо корінь проекту в path для імпорту config
_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.olx_scraper import config as scraper_config


def get_session() -> requests.Session:
    """Повертає сесію з заголовками, схожими на звичайний браузер (Chrome)."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": scraper_config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "uk,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    })
    cookies_list = getattr(scraper_config, "get_cookies_for_session", lambda: [])()
    if cookies_list:
        for c in cookies_list:
            session.cookies.set(c.get("name", ""), c.get("value", ""), domain=".olx.ua")
    return session


def fetch_page(
    url: str,
    delay_before: bool = True,
    delay_after: bool = False,
    session: Optional["requests.Session"] = None,
    is_detail: bool = False,
) -> requests.Response:
    """
    Завантажує одну сторінку через HTTP. Для list-пошуку OLX не використовується — тільки браузер.
    is_detail=True — сторінка оголошення (більший таймаут).
    """
    if not is_detail:
        raise RuntimeError(
            "HTTP list-fetch для OLX вимкнено. Використовуйте browser_fetcher.get_list_page()."
        )

    if delay_before:
        sec = scraper_config.get_delay_detail_seconds()
        print(f"[OLX scraper] Затримка {sec:.1f} с перед detail-запитом...", flush=True)
        time.sleep(sec)

    sess = session if session is not None else get_session()
    timeout = getattr(scraper_config, "REQUEST_DETAIL_TIMEOUT", scraper_config.REQUEST_TIMEOUT)
    response = sess.get(url, timeout=timeout, allow_redirects=True)
    response.raise_for_status()
    if response.encoding == "ISO-8859-1" or not response.apparent_encoding:
        response.encoding = response.apparent_encoding or "utf-8"
    _ = delay_after
    return response
