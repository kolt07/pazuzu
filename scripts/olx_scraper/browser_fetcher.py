# -*- coding: utf-8 -*-
"""
Отримання HTML сторінок OLX через браузер (Playwright) замість HTTP-запитів.

Використовується в основному скрапері при увімкненні OLX_SCRAPER_USE_BROWSER:
замість fetcher.fetch_page / _fetch_detail_page викликаються методи цього класу.
Повертає об'єкти з .text та .status_code, сумісні з поточним використанням у run_update.
Стара логіка (requests) залишається за замовчуванням.
"""

from __future__ import annotations

import random
import time
from typing import Any, Callable, Optional

# Імпорт playwright лише при використанні класу


class PageResult:
    """Результат завантаження сторінки: .text (HTML) та .status_code, як у requests.Response."""

    __slots__ = ("text", "status_code")

    def __init__(self, text: str, status_code: int = 200):
        self.text = text or ""
        self.status_code = status_code


class BrowserPageFetcher:
    """
    Контекстний менеджер: відкриває один браузер (Chromium), надає методи
    get_list_page та get_detail_page для отримання HTML. Після виходу — закриває браузер.
    """

    def __init__(
        self,
        headless: bool = True,
        log_fn: Optional[Callable[[str], None]] = None,
    ):
        self._headless = headless
        self._log_fn = log_fn or (lambda s: None)
        self._playwright: Any = None
        self._browser: Any = None
        self._context: Any = None
        self._page: Any = None

    def __enter__(self) -> "BrowserPageFetcher":
        try:
            from playwright.sync_api import sync_playwright
        except ImportError:
            raise RuntimeError(
                "Playwright не встановлено. Виконайте: pip install playwright && playwright install chromium"
            )
        import sys
        from pathlib import Path
        _root = Path(__file__).resolve().parent.parent.parent
        if str(_root) not in sys.path:
            sys.path.insert(0, str(_root))
        from scripts.olx_scraper import config as scraper_config

        self._playwright = sync_playwright().start()
        self._browser = self._playwright.chromium.launch(headless=self._headless)
        self._context = self._browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=scraper_config.USER_AGENT,
            locale="uk-UA",
        )
        self._context.set_extra_http_headers({"Accept-Language": "uk,en;q=0.9"})
        self._page = self._context.new_page()
        detail_timeout_ms = max(45000, getattr(scraper_config, "REQUEST_DETAIL_TIMEOUT", 90) * 1000)
        self._page.set_default_timeout(min(60000, detail_timeout_ms))
        self._page.set_default_navigation_timeout(detail_timeout_ms)
        return self

    def __exit__(self, *args: Any) -> None:
        try:
            if self._browser:
                self._browser.close()
        finally:
            if self._playwright:
                self._playwright.stop()

    def get_list_page(
        self,
        url: str,
        delay_before: bool = True,
        delay_after: bool = False,
    ) -> PageResult:
        """
        Відкриває URL сторінки пошуку, повертає PageResult(.text=html, .status_code).
        Затримки як у конфігу (get_delay_seconds, DELAY_AFTER_PAGE_LOAD).
        """
        from scripts.olx_scraper import config as scraper_config

        if delay_before:
            sec = scraper_config.get_delay_seconds()
            self._log_fn(f"[OLX browser] Затримка {sec:.1f} с перед запитом...")
            time.sleep(sec)
        try:
            response = self._page.goto(url, wait_until="domcontentloaded")
            status = response.status if response else 0
            if delay_after:
                sec = getattr(scraper_config, "DELAY_AFTER_PAGE_LOAD", 3) or 0
                if sec > 0:
                    time.sleep(sec)
            html = self._page.content()
            return PageResult(html, status)
        except Exception as e:
            self._log_fn(f"[OLX browser] Помилка завантаження {url[:50]}...: {e}")
            err_str = str(e).lower()
            if "404" in err_str or "net::err_aborted" in err_str:
                return PageResult("", 404)
            if "502" in err_str or "503" in err_str or "504" in err_str:
                return PageResult("", 502)
            raise

    def get_detail_page(self, url: str) -> PageResult:
        """
        Відкриває сторінку оголошення, повертає PageResult(.text=html, .status_code).
        Затримка перед запитом — get_delay_detail_seconds. При ознаках антиботу — одна повторна спроба.
        """
        from scripts.olx_scraper import config as scraper_config
        from scripts.olx_scraper.parser import detect_antibot_page

        sec = scraper_config.get_delay_detail_seconds()
        time.sleep(sec)
        timeout_ms = max(60000, getattr(scraper_config, "REQUEST_DETAIL_TIMEOUT", 90) * 1000)
        # Селектори опису OLX — чекаємо їх появу після JS-рендеру перед зняттям HTML
        _DESCRIPTION_SELECTORS = [
            '[data-cy="ad_description"]',
            '[data-cy="ad_description_content"]',
            '[data-testid="ad-description"]',
            '[data-testid="ad_description"]',
        ]
        try:
            response = self._page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
            status = response.status if response else 0
            time.sleep(1 + random.uniform(0.5, 2))
            for sel in _DESCRIPTION_SELECTORS:
                try:
                    self._page.wait_for_selector(sel, timeout=15000)
                    break
                except Exception:
                    continue
            else:
                time.sleep(3)
            html = self._page.content()
            antibot = detect_antibot_page(html)
            if antibot.get("is_antibot") and antibot.get("hints"):
                self._log_fn(f"[OLX browser] Ознаки антиботу: {', '.join(antibot['hints'])}. Повтор через 8 с...")
                time.sleep(8)
                response = self._page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                status = response.status if response else 0
                for sel in _DESCRIPTION_SELECTORS:
                    try:
                        self._page.wait_for_selector(sel, timeout=15000)
                        break
                    except Exception:
                        continue
                html = self._page.content()
            return PageResult(html, status)
        except Exception as e:
            self._log_fn(f"[OLX browser] Помилка деталей {url[:50]}...: {e}")
            err_str = str(e).lower()
            if "404" in err_str:
                return PageResult("", 404)
            raise
