# -*- coding: utf-8 -*-
"""HTTP-завантаження сторінок mista.ua."""

from __future__ import annotations

import re
import sys
import time
from pathlib import Path
from typing import Optional

import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.mista_scraper import config as scraper_config

_ZB_COOKIE_RE = re.compile(
    r"""cookie\s*\(\s*["']zb["']\s*,\s*["']([^"']+)["']""",
    re.IGNORECASE,
)


def get_session() -> requests.Session:
    session = requests.Session()
    session.headers.update({
        "User-Agent": scraper_config.USER_AGENT,
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "uk,en;q=0.9",
        "Accept-Encoding": "gzip, deflate",
        "Connection": "keep-alive",
    })
    return session


def _mista_cookie_domain() -> str:
    base = scraper_config.BASE_URL.replace("https://", "").replace("http://", "")
    return base.split("/")[0] or "mista.ua"


def ensure_mista_session(session: requests.Session) -> None:
    """Встановлює zb/zbSet — без них сайт повертає заглушку замість таблиці."""
    domain = _mista_cookie_domain()
    if session.cookies.get("zbSet", domain=domain):
        return

    url = scraper_config.settlements_list_url()
    resp = session.get(url, timeout=scraper_config.REQUEST_TIMEOUT, allow_redirects=True)
    zb = scraper_config.MISTA_ZB_COOKIE
    if len(resp.text) < 8000:
        m = _ZB_COOKIE_RE.search(resp.text)
        if m:
            zb = m.group(1)

    session.cookies.set("zb", zb, path="/", domain=domain)
    session.cookies.set("zbSet", "true", path="/", domain=domain)


def fetch_settlements_list_page(
    page: int = 1,
    *,
    obl_id: Optional[int] = None,
    delay_before: bool = True,
    session: Optional[requests.Session] = None,
) -> str:
    """
    Завантажує HTML таблиці списку НП (сторінка 1-based).

    Без obl: POST на encoded path (reload=ajax, citySPG=N).
    З obl (фільтр області): стор. 1 — GET /Пошук_населених_пунктів/?obl=ID;
    стор. 2+ — POST на той самий URL з ajax-параметрами.
    """
    if delay_before:
        sec = scraper_config.get_delay_seconds()
        print(f"[mista] Затримка {sec:.1f} с...", flush=True)
        time.sleep(sec)

    sess = session or get_session()
    ensure_mista_session(sess)

    if obl_id is not None:
        url = scraper_config.settlements_list_url(obl_id)
        if page <= 1:
            request_fn = lambda: sess.get(url, timeout=scraper_config.REQUEST_TIMEOUT, allow_redirects=True)
        else:
            data = scraper_config.settlements_list_ajax_params(page, obl_id=obl_id)

            def request_fn():
                return sess.post(url, data=data, timeout=scraper_config.REQUEST_TIMEOUT)
    else:
        url = scraper_config.settlements_list_post_url()
        data = scraper_config.settlements_list_ajax_params(page)

        def request_fn():
            return sess.post(url, data=data, timeout=scraper_config.REQUEST_TIMEOUT)

    last_err: Optional[Exception] = None
    for attempt in range(1, scraper_config.MAX_RETRIES + 1):
        try:
            resp = request_fn()
            resp.raise_for_status()
            if resp.encoding in (None, "ISO-8859-1"):
                resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except Exception as e:
            last_err = e
            if attempt < scraper_config.MAX_RETRIES:
                time.sleep(2 * attempt)
    raise last_err  # type: ignore[misc]


def fetch_settlements_search_page(
    *,
    delay_before: bool = False,
    session: Optional[requests.Session] = None,
) -> str:
    """Повна HTML-сторінка пошуку (для парсингу списку областей у фільтрі)."""
    if delay_before:
        time.sleep(scraper_config.get_delay_seconds())

    sess = session or get_session()
    ensure_mista_session(sess)
    url = scraper_config.settlements_list_url()
    resp = sess.get(url, timeout=scraper_config.REQUEST_TIMEOUT, allow_redirects=True)
    resp.raise_for_status()
    if resp.encoding in (None, "ISO-8859-1"):
        resp.encoding = resp.apparent_encoding or "utf-8"
    return resp.text


def fetch_page(
    url: str,
    *,
    delay_before: bool = True,
    session: Optional[requests.Session] = None,
) -> str:
    """Завантажує HTML сторінки."""
    if delay_before:
        sec = scraper_config.get_delay_seconds()
        print(f"[mista] Затримка {sec:.1f} с...", flush=True)
        time.sleep(sec)

    sess = session or get_session()
    ensure_mista_session(sess)
    last_err: Optional[Exception] = None
    for attempt in range(1, scraper_config.MAX_RETRIES + 1):
        try:
            resp = sess.get(url, timeout=scraper_config.REQUEST_TIMEOUT, allow_redirects=True)
            resp.raise_for_status()
            if resp.encoding in (None, "ISO-8859-1"):
                resp.encoding = resp.apparent_encoding or "utf-8"
            return resp.text
        except Exception as e:
            last_err = e
            if attempt < scraper_config.MAX_RETRIES:
                time.sleep(2 * attempt)
    raise last_err  # type: ignore[misc]
