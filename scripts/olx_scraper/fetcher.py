# -*- coding: utf-8 -*-
"""
Завантаження сторінок OLX з обмеженнями для зменшення ризику блокування:
- один запит за раз (без конкурентних запитів);
- затримка перед запитом;
- реалістичні заголовки (User-Agent, Accept-Language, Referer).
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
    """Повертає сесію з заголовками, схожими на звичайний браузер."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": scraper_config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/avif,image/webp,*/*;q=0.8",
        "Accept-Language": "uk,en;q=0.9",
        # Тільки gzip, deflate — requests декодує їх. br (brotli) без пакета brotli дає бінарний мусор.
        "Accept-Encoding": "gzip, deflate",
        "DNT": "1",
        "Connection": "keep-alive",
        "Upgrade-Insecure-Requests": "1",
        "Sec-Fetch-Dest": "document",
        "Sec-Fetch-Mode": "navigate",
        "Sec-Fetch-Site": "none",
    })
    return session


def fetch_page(
    url: str,
    delay_before: bool = True,
    delay_after: bool = False,
    session: Optional["requests.Session"] = None,
) -> requests.Response:
    """
    Завантажує одну сторінку. Перед запитом робить паузу (антибот).
    Опційно — затримка після отримання (OLX може підвантажувати контент з затримкою).
    session: якщо передано — використовується для повторного використання з'єднання (keep-alive).
    """
    if delay_before:
        sec = scraper_config.get_delay_seconds()
        print(f"[OLX scraper] Затримка {sec:.1f} с перед запитом...", flush=True)
        time.sleep(sec)

    sess = session if session is not None else get_session()
    # Referer на головну сторінку категорії при першому запиті не обов'язковий
    response = sess.get(
        url,
        timeout=scraper_config.REQUEST_TIMEOUT,
        allow_redirects=True,
    )
    response.raise_for_status()
    # Кодування з заголовків або контенту
    if response.encoding == "ISO-8859-1" or not response.apparent_encoding:
        response.encoding = response.apparent_encoding or "utf-8"

    # Затримка після отримання — OLX може показувати 0 оголошень і підвантажувати їх згодом
    if delay_after and hasattr(scraper_config, "DELAY_AFTER_PAGE_LOAD"):
        sec = scraper_config.DELAY_AFTER_PAGE_LOAD
        if sec > 0:
            print(f"[OLX scraper] Затримка {sec:.1f} с після завантаження сторінки...", flush=True)
            time.sleep(sec)

    return response
