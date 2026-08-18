# -*- coding: utf-8 -*-
"""
Завантаження сторінок OLX з обмеженнями для зменшення ризику блокування:
- обмежена паралельність list-HTTP між Phase1-потоками;
- затримка перед запитом;
- реалістичні заголовки (User-Agent, Accept-Language, Referer).
"""

import threading
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

# Глобальний ліміт паралельних HTTP list-запитів (не detail).
_list_http_sema: Optional[threading.Semaphore] = None
_list_http_sema_lock = threading.Lock()


def _get_list_http_semaphore() -> threading.Semaphore:
    global _list_http_sema
    with _list_http_sema_lock:
        if _list_http_sema is None:
            n = max(1, int(getattr(scraper_config, "LIST_HTTP_CONCURRENCY", 1) or 1))
            _list_http_sema = threading.Semaphore(n)
        return _list_http_sema


def get_session() -> requests.Session:
    """Повертає сесію з заголовками, схожими на звичайний браузер (Chrome)."""
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
        # Client hints (Chrome) — допомагають виглядати як звичайний браузер
        "Sec-Ch-Ua": '"Not_A Brand";v="8", "Chromium";v="120", "Google Chrome";v="120"',
        "Sec-Ch-Ua-Mobile": "?0",
        "Sec-Ch-Ua-Platform": '"Windows"',
    })
    # Підміна куків з конфігу/файлу (обхід антиботу)
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
    Завантажує одну сторінку. Перед запитом робить паузу (антибот).
    Опційно — затримка після отримання (OLX може підвантажувати контент з затримкою).
    session: якщо передано — використовується для повторного використання з'єднання (keep-alive).
    is_detail: True для сторінки оголошення — використовується більший таймаут (REQUEST_DETAIL_TIMEOUT).
    List-запити (is_detail=False) проходять через глобальний semaphore LIST_HTTP_CONCURRENCY.
    """
    if delay_before:
        sec = scraper_config.get_delay_seconds()
        print(f"[OLX scraper] Затримка {sec:.1f} с перед запитом...", flush=True)
        time.sleep(sec)

    sess = session if session is not None else get_session()
    timeout = getattr(scraper_config, "REQUEST_DETAIL_TIMEOUT", scraper_config.REQUEST_TIMEOUT) if is_detail else scraper_config.REQUEST_TIMEOUT

    def _do_get() -> requests.Response:
        return sess.get(
            url,
            timeout=timeout,
            allow_redirects=True,
        )

    if is_detail:
        response = _do_get()
    else:
        sema = _get_list_http_semaphore()
        with sema:
            response = _do_get()
    response.raise_for_status()
    # Кодування з заголовків або контенту
    if response.encoding == "ISO-8859-1" or not response.apparent_encoding:
        response.encoding = response.apparent_encoding or "utf-8"

    # HTTP (requests) уже має повний HTML — післязавантажувальна пауза не потрібна.
    # delay_after залишено в сигнатурі для сумісності; для browser list див. BrowserPageFetcher.
    _ = delay_after

    return response
