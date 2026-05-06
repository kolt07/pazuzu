# -*- coding: utf-8 -*-
"""
Отримання HTML сторінок OLX через браузер (Playwright).

Використовується основним скрапером для detail-сторінок оголошень.
Повертає об'єкти з .text та .status_code, сумісні з використанням у run_update.
"""

from __future__ import annotations

import asyncio
import queue
import random
import threading
import time
from dataclasses import dataclass
from typing import Any, Callable, Optional

# Імпорт playwright лише при використанні класу


# Базовий URL OLX для підстановки куків
_OLX_BASE_URL = "https://www.olx.ua"
_DESCRIPTION_SELECTORS = [
    '[data-cy="ad_description"]',
    '[data-cy="ad_description_content"]',
    '[data-testid="ad-description"]',
    '[data-testid="ad_description"]',
]


def _add_olx_cookies_to_context(context: Any, scraper_config: Any) -> None:
    """Додає куки з конфігу (OLX_SCRAPER_COOKIES / COOKIES_FILE) до контексту браузера."""
    try:
        cookies = scraper_config.get_cookies_for_session()
    except Exception:
        return
    if not cookies:
        return
    pw_cookies = []
    for c in cookies:
        if isinstance(c, dict) and c.get("name") and "value" in c:
            pw_cookies.append({
                "name": str(c["name"]),
                "value": str(c["value"]),
                "url": _OLX_BASE_URL,
            })
    if pw_cookies:
        try:
            context.add_cookies(pw_cookies)
        except Exception:
            pass


async def _add_olx_cookies_to_context_async(context: Any, scraper_config: Any) -> None:
    """Async-варіант під async_playwright."""
    try:
        cookies = scraper_config.get_cookies_for_session()
    except Exception:
        return
    if not cookies:
        return
    pw_cookies = []
    for c in cookies:
        if isinstance(c, dict) and c.get("name") and "value" in c:
            pw_cookies.append({
                "name": str(c["name"]),
                "value": str(c["value"]),
                "url": _OLX_BASE_URL,
            })
    if pw_cookies:
        try:
            await context.add_cookies(pw_cookies)
        except Exception:
            pass


def _is_crash_error(exc: BaseException) -> bool:
    """Чи є виняток пов'язаний з крашем сторінки/браузера (Target crashed, Page crashed)."""
    msg = (getattr(exc, "message", "") or str(exc)).lower()
    return "crashed" in msg or "target closed" in msg


def _is_navigation_timeout_error(exc: BaseException) -> bool:
    """Чи схожий виняток на timeout під час Page.goto."""
    msg = (getattr(exc, "message", "") or str(exc)).lower()
    return "timeout" in msg and "goto" in msg


def _build_launch_options(headless: bool, scraper_config: Any) -> dict:
    """Формує launch options для Chromium/Chrome з docker-safe флагами за конфігом."""
    args = ["--disable-blink-features=AutomationControlled"]
    if getattr(scraper_config, "BROWSER_DOCKER_SAFE_ARGS", False):
        args.extend([
            "--disable-dev-shm-usage",
            "--no-sandbox",
            "--disable-setuid-sandbox",
        ])
    launch_options: dict = {
        "headless": headless,
        "args": args,
        "ignore_default_args": ["--enable-automation"],
    }
    if getattr(scraper_config, "BROWSER_USE_CHROME", False):
        launch_options["channel"] = "chrome"
    return launch_options


def _set_page_timeouts(page: Any, scraper_config: Any) -> None:
    detail_timeout_ms = max(45000, getattr(scraper_config, "REQUEST_DETAIL_TIMEOUT", 90) * 1000)
    page.set_default_timeout(min(60000, detail_timeout_ms))
    page.set_default_navigation_timeout(detail_timeout_ms)


def _create_browser_context(browser: Any, scraper_config: Any) -> Any:
    context = browser.new_context(
        viewport={"width": 1280, "height": 720},
        user_agent=scraper_config.USER_AGENT,
        locale="uk-UA",
        java_script_enabled=True,
    )
    context.set_extra_http_headers({"Accept-Language": "uk,en;q=0.9"})
    _add_olx_cookies_to_context(context, scraper_config)
    return context


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
        # Мінімізуємо ознаки автоматизації + додаємо docker-safe флаги при роботі в контейнері.
        launch_options = _build_launch_options(self._headless, scraper_config)
        self._browser = self._playwright.chromium.launch(**launch_options)
        self._context = _create_browser_context(self._browser, scraper_config)
        self._page = self._context.new_page()
        _set_page_timeouts(self._page, scraper_config)
        return self

    def _recreate_page(self) -> None:
        """Закриває поточну сторінку та створює нову (після крашу)."""
        try:
            if self._page:
                self._page.close()
        except Exception:
            pass
        from scripts.olx_scraper import config as scraper_config
        self._page = self._context.new_page()
        _set_page_timeouts(self._page, scraper_config)

    def _recreate_context(self) -> None:
        """Закриває контекст і сторінку, створює новий контекст і сторінку (після повторного крашу)."""
        try:
            if self._context:
                self._context.close()
        except Exception:
            pass
        from scripts.olx_scraper import config as scraper_config
        self._context = _create_browser_context(self._browser, scraper_config)
        self._page = self._context.new_page()
        _set_page_timeouts(self._page, scraper_config)

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
        При краші сторінки (Target/Page crashed) — одна повторна спроба з новою сторінкою.
        """
        from scripts.olx_scraper import config as scraper_config

        if delay_before:
            sec = scraper_config.get_delay_seconds()
            self._log_fn(f"[OLX browser] Затримка {sec:.1f} с перед запитом...")
            time.sleep(sec)
        last_exc: Optional[Exception] = None
        for attempt in range(2):
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
                last_exc = e
                self._log_fn(f"[OLX browser] Помилка завантаження {url[:50]}...: {e}")
                err_str = str(e).lower()
                if "404" in err_str or "net::err_aborted" in err_str:
                    return PageResult("", 404)
                if "502" in err_str or "503" in err_str or "504" in err_str:
                    return PageResult("", 502)
                if _is_crash_error(e) and attempt == 0:
                    self._log_fn("[OLX browser] Краш сторінки — перестворюємо сторінку та повторюємо...")
                    self._recreate_page()
                    time.sleep(2)
                    continue
                raise
        if last_exc:
            raise last_exc
        return PageResult("", 0)

    def get_detail_page(self, url: str) -> PageResult:
        """
        Відкриває сторінку оголошення, повертає PageResult(.text=html, .status_code).
        Затримка перед запитом — get_delay_detail_seconds. При ознаках антиботу — одна повторна спроба.
        При краші: спочатку нова сторінка, при повторному краші — новий контекст, далі fallback на domcontentloaded.
        """
        from scripts.olx_scraper import config as scraper_config
        from scripts.olx_scraper.parser import detect_antibot_page

        sec = scraper_config.get_delay_detail_seconds()
        time.sleep(sec)
        timeout_ms = max(60000, getattr(scraper_config, "REQUEST_DETAIL_TIMEOUT", 90) * 1000)
        wait_until = getattr(scraper_config, "BROWSER_DETAIL_WAIT_UNTIL", "load")
        use_fast_wait = False
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            current_wait = "domcontentloaded" if (attempt == 2 or use_fast_wait) else wait_until
            try:
                response = self._page.goto(url, wait_until=current_wait, timeout=timeout_ms)
                status = response.status if response else 0
                time.sleep(1.5 + random.uniform(0.5, 2.5))
                for sel in _DESCRIPTION_SELECTORS:
                    try:
                        self._page.wait_for_selector(sel, timeout=20000)
                        break
                    except Exception:
                        continue
                else:
                    time.sleep(2)
                html = self._page.content()
                antibot = detect_antibot_page(html)
                if antibot.get("is_antibot") and antibot.get("hints"):
                    self._log_fn(f"[OLX browser] Ознаки антиботу: {', '.join(antibot.get('hints', []))}. Повтор через 8 с...")
                    time.sleep(8)
                    response = self._page.goto(url, wait_until=current_wait, timeout=timeout_ms)
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
                last_exc = e
                self._log_fn(f"[OLX browser] Помилка деталей {url[:50]}...: {e}")
                err_str = str(e).lower()
                if "404" in err_str:
                    return PageResult("", 404)
                if _is_navigation_timeout_error(e) and current_wait == "load":
                    self._log_fn("[OLX browser] Timeout на wait_until=load, повторюємо з domcontentloaded...")
                    use_fast_wait = True
                    continue
                if _is_crash_error(e):
                    if attempt == 0:
                        self._log_fn("[OLX browser] Краш сторінки — перестворюємо сторінку та повторюємо...")
                        self._recreate_page()
                        time.sleep(2)
                        continue
                    if attempt == 1:
                        self._log_fn("[OLX browser] Повторний краш — перестворюємо контекст, повтор з domcontentloaded...")
                        self._recreate_context()
                        time.sleep(3)
                        continue
                raise
        if last_exc:
            raise last_exc
        return PageResult("", 0)


class BrowserPagePool:
    """
    Singleton-подібний оркестратор браузера:
    - один процес браузера;
    - внутрішня asyncio-черга запитів;
    - N worker-сторінок (по одній на logical worker) для одночасного завантаження detail.

    Зовнішні потоки викликають sync `get_detail_page(url)` і чекають результат.
    """

    def __init__(
        self,
        headless: bool = True,
        pool_size: int = 1,
        log_fn: Optional[Callable[[str], None]] = None,
    ):
        self._headless = headless
        self._pool_size = max(1, int(pool_size or 1))
        self._log_fn = log_fn or (lambda s: None)
        self._owner_thread: Optional[threading.Thread] = None
        self._ready_event = threading.Event()
        self._closed_event = threading.Event()
        self._startup_error: Optional[BaseException] = None
        self._loop: Optional[asyncio.AbstractEventLoop] = None
        self._request_queue: Optional[asyncio.Queue] = None
        self._stop_requested = False
        self._slots: list[dict[str, Any]] = []
        self._playwright: Any = None
        self._browser: Any = None

    @dataclass
    class _PoolRequest:
        url: str
        done: threading.Event
        result: Optional["PageResult"] = None
        error: Optional[BaseException] = None

    def __enter__(self) -> "BrowserPagePool":
        self._owner_thread = threading.Thread(target=self._run_owner_loop, daemon=True, name="OLXBrowserPoolOwner")
        self._owner_thread.start()
        if not self._ready_event.wait(timeout=120):
            raise RuntimeError("BrowserPool startup timeout.")
        if self._startup_error:
            raise RuntimeError(f"BrowserPool startup failed: {self._startup_error}")
        self._log_fn(f"[OLX browser] BrowserPool ініціалізовано: {self._pool_size} сторінок, черга запитів увімкнена.")
        return self

    def __exit__(self, *args: Any) -> None:
        self._stop_requested = True
        loop = self._loop
        req_q = self._request_queue
        if loop and req_q:
            for _ in range(self._pool_size):
                asyncio.run_coroutine_threadsafe(req_q.put(None), loop)
        if self._owner_thread and self._owner_thread.is_alive():
            self._owner_thread.join(timeout=60)
        self._closed_event.set()

    def _run_owner_loop(self) -> None:
        try:
            loop = asyncio.new_event_loop()
            self._loop = loop
            asyncio.set_event_loop(loop)
            loop.run_until_complete(self._async_owner_main())
        except BaseException as e:
            self._startup_error = e
            self._ready_event.set()
        finally:
            if self._loop and not self._loop.is_closed():
                self._loop.close()

    async def _async_owner_main(self) -> None:
        try:
            from playwright.async_api import async_playwright
        except ImportError as e:
            raise RuntimeError(
                "Playwright не встановлено. Виконайте: pip install playwright && playwright install chromium"
            ) from e
        import sys
        from pathlib import Path
        _root = Path(__file__).resolve().parent.parent.parent
        if str(_root) not in sys.path:
            sys.path.insert(0, str(_root))
        from scripts.olx_scraper import config as scraper_config

        self._request_queue = asyncio.Queue()
        self._playwright = await async_playwright().start()
        launch_options = _build_launch_options(self._headless, scraper_config)
        self._browser = await self._playwright.chromium.launch(**launch_options)

        for idx in range(self._pool_size):
            context = await self._browser.new_context(
                viewport={"width": 1280, "height": 720},
                user_agent=scraper_config.USER_AGENT,
                locale="uk-UA",
                java_script_enabled=True,
            )
            await context.set_extra_http_headers({"Accept-Language": "uk,en;q=0.9"})
            await _add_olx_cookies_to_context_async(context, scraper_config)
            page = await context.new_page()
            _set_page_timeouts(page, scraper_config)
            self._slots.append({"context": context, "page": page, "slot_idx": idx})

        workers = [asyncio.create_task(self._slot_worker(slot)) for slot in self._slots]
        self._ready_event.set()
        try:
            await asyncio.gather(*workers)
        finally:
            await self._close_all_slots()
            if self._browser:
                await self._browser.close()
            if self._playwright:
                await self._playwright.stop()

    async def _close_all_slots(self) -> None:
        for slot in self._slots:
            try:
                if slot.get("context"):
                    await slot["context"].close()
            except Exception:
                pass
        self._slots.clear()

    async def _recreate_slot_page(self, slot: dict) -> None:
        from scripts.olx_scraper import config as scraper_config
        try:
            if slot.get("page"):
                await slot["page"].close()
        except Exception:
            pass
        page = await slot["context"].new_page()
        _set_page_timeouts(page, scraper_config)
        slot["page"] = page

    async def _recreate_slot_context(self, slot: dict) -> None:
        from scripts.olx_scraper import config as scraper_config
        try:
            if slot.get("context"):
                await slot["context"].close()
        except Exception:
            pass
        context = await self._browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=scraper_config.USER_AGENT,
            locale="uk-UA",
            java_script_enabled=True,
        )
        await context.set_extra_http_headers({"Accept-Language": "uk,en;q=0.9"})
        await _add_olx_cookies_to_context_async(context, scraper_config)
        page = await context.new_page()
        _set_page_timeouts(page, scraper_config)
        slot["context"] = context
        slot["page"] = page

    async def _slot_worker(self, slot: dict) -> None:
        while True:
            req = await self._request_queue.get()  # type: ignore[arg-type]
            if req is None:
                return
            try:
                req.result = await self._fetch_detail_with_slot(slot, req.url)
            except BaseException as e:
                req.error = e
            finally:
                req.done.set()

    def get_detail_page(self, url: str) -> PageResult:
        if not self._loop or not self._request_queue:
            raise RuntimeError("BrowserPool is not initialized.")
        done = threading.Event()
        req = BrowserPagePool._PoolRequest(url=url, done=done)
        fut = asyncio.run_coroutine_threadsafe(self._request_queue.put(req), self._loop)
        fut.result(timeout=10)
        if not done.wait(timeout=240):
            raise RuntimeError(f"BrowserPool timeout while fetching detail: {url[:80]}")
        if req.error:
            raise req.error
        return req.result or PageResult("", 0)

    async def _fetch_detail_with_slot(self, slot: dict, url: str) -> PageResult:
        from scripts.olx_scraper import config as scraper_config
        from scripts.olx_scraper.parser import detect_antibot_page

        await asyncio.sleep(scraper_config.get_delay_detail_seconds())
        timeout_ms = max(60000, getattr(scraper_config, "REQUEST_DETAIL_TIMEOUT", 90) * 1000)
        wait_until = getattr(scraper_config, "BROWSER_DETAIL_WAIT_UNTIL", "load")
        use_fast_wait = False
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            current_wait = "domcontentloaded" if (attempt == 2 or use_fast_wait) else wait_until
            page = slot["page"]
            try:
                response = await page.goto(url, wait_until=current_wait, timeout=timeout_ms)
                status = response.status if response else 0
                await asyncio.sleep(1.5 + random.uniform(0.5, 2.5))
                for sel in _DESCRIPTION_SELECTORS:
                    try:
                        await page.wait_for_selector(sel, timeout=20000)
                        break
                    except Exception:
                        continue
                else:
                    await asyncio.sleep(2)
                html = await page.content()
                antibot = detect_antibot_page(html)
                if antibot.get("is_antibot") and antibot.get("hints"):
                    self._log_fn(f"[OLX browser] Ознаки антиботу: {', '.join(antibot.get('hints', []))}. Повтор через 8 с...")
                    await asyncio.sleep(8)
                    response = await page.goto(url, wait_until=current_wait, timeout=timeout_ms)
                    status = response.status if response else 0
                    for sel in _DESCRIPTION_SELECTORS:
                        try:
                            await page.wait_for_selector(sel, timeout=15000)
                            break
                        except Exception:
                            continue
                    html = await page.content()
                return PageResult(html, status)
            except Exception as e:
                last_exc = e
                self._log_fn(f"[OLX browser] Помилка деталей {url[:50]}...: {e}")
                err_str = str(e).lower()
                if "404" in err_str:
                    return PageResult("", 404)
                if _is_navigation_timeout_error(e) and current_wait == "load":
                    self._log_fn("[OLX browser] Timeout на wait_until=load (pool), повторюємо з domcontentloaded...")
                    use_fast_wait = True
                    continue
                if _is_crash_error(e):
                    if attempt == 0:
                        self._log_fn("[OLX browser] Краш сторінки (pool) — перестворюємо сторінку та повторюємо...")
                        await self._recreate_slot_page(slot)
                        await asyncio.sleep(2)
                        continue
                    if attempt == 1:
                        self._log_fn("[OLX browser] Повторний краш (pool) — перестворюємо контекст, повтор з domcontentloaded...")
                        await self._recreate_slot_context(slot)
                        await asyncio.sleep(3)
                        continue
                raise
        if last_exc:
            raise last_exc
        return PageResult("", 0)
