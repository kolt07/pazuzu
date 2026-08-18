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


def _description_selector_combined() -> str:
    """Один CSS-селектор для будь-якого відомого блоку опису (замість N послідовних wait)."""
    return ", ".join(_DESCRIPTION_SELECTORS)


def _wait_for_description_sync(page: Any, timeout_ms: int = 15000) -> None:
    try:
        page.wait_for_selector(_description_selector_combined(), timeout=timeout_ms)
    except Exception:
        time.sleep(2)


async def _wait_for_description_async(page: Any, timeout_ms: int = 15000) -> None:
    try:
        await page.wait_for_selector(_description_selector_combined(), timeout=timeout_ms)
    except Exception:
        await asyncio.sleep(2)


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
                settle = scraper_config.get_detail_post_goto_settle_seconds()
                if settle > 0:
                    time.sleep(settle)
                _wait_for_description_sync(self._page, timeout_ms=15000)
                html = self._page.content()
                antibot = detect_antibot_page(html)
                if antibot.get("is_antibot") and antibot.get("hints"):
                    self._log_fn(f"[OLX browser] Ознаки антиботу: {', '.join(antibot.get('hints', []))}. Повтор через 8 с...")
                    time.sleep(8)
                    response = self._page.goto(url, wait_until=current_wait, timeout=timeout_ms)
                    status = response.status if response else 0
                    _wait_for_description_sync(self._page, timeout_ms=12000)
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
    Оркестратор браузера:
    - один процес Chromium;
    - внутрішня asyncio-черга запитів;
    - N worker-сторінок для паралельного detail/list-fetch.

    Зовнішні потоки викликають sync `get_detail_page(url)` / `get_list_page(url)`.
    Завислий Playwright (goto без відповіді) розблоковується жорстким бюджетом:
    закриття page → recreate slot/context → за потреби повний restart браузера.
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
        self._restart_lock: Optional[asyncio.Lock] = None
        self._consecutive_hangs = 0
        self._recovery_generation = 0

    @dataclass
    class _PoolRequest:
        url: str
        done: threading.Event
        kind: str = "detail"  # "detail" | "list"
        result: Optional["PageResult"] = None
        error: Optional[BaseException] = None
        abandoned: bool = False

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
        self._restart_lock = asyncio.Lock()
        self._playwright = await async_playwright().start()
        await self._launch_browser_and_slots(scraper_config)

        workers = [asyncio.create_task(self._slot_worker(slot)) for slot in self._slots]
        self._ready_event.set()
        try:
            await asyncio.gather(*workers)
        finally:
            await self._close_all_slot_contexts()
            if self._browser:
                try:
                    await self._browser.close()
                except Exception:
                    pass
            if self._playwright:
                try:
                    await self._playwright.stop()
                except Exception:
                    pass

    async def _launch_browser_and_slots(self, scraper_config: Any) -> None:
        launch_options = _build_launch_options(self._headless, scraper_config)
        self._browser = await self._playwright.chromium.launch(**launch_options)
        if not self._slots:
            for idx in range(self._pool_size):
                self._slots.append({"context": None, "page": None, "slot_idx": idx})
        for slot in self._slots:
            await self._assign_fresh_context(slot, scraper_config)

    async def _assign_fresh_context(self, slot: dict, scraper_config: Any) -> None:
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

    async def _close_all_slot_contexts(self) -> None:
        for slot in self._slots:
            try:
                if slot.get("context"):
                    await slot["context"].close()
            except Exception:
                pass
            slot["context"] = None
            slot["page"] = None

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
        await self._assign_fresh_context(slot, scraper_config)

    async def _force_close_page(self, slot: dict) -> None:
        """Примусово закриває page, щоб розблокувати завислий page.goto (Playwright CancelledError часто ігнорується)."""
        try:
            page = slot.get("page")
            if page:
                await page.close()
        except Exception:
            pass
        slot["page"] = None

    async def _restart_browser(self) -> None:
        """Повний restart Chromium; slot dicts мутуються in-place (workers тримають ті самі refs)."""
        if self._restart_lock is None:
            self._restart_lock = asyncio.Lock()
        async with self._restart_lock:
            from scripts.olx_scraper import config as scraper_config
            self._recovery_generation += 1
            gen = self._recovery_generation
            self._log_fn(f"[OLX browser] BrowserPool: повний restart Chromium (generation={gen})...")
            await self._close_all_slot_contexts()
            try:
                if self._browser:
                    await self._browser.close()
            except Exception:
                pass
            self._browser = None
            try:
                await self._launch_browser_and_slots(scraper_config)
                self._consecutive_hangs = 0
                self._log_fn(f"[OLX browser] BrowserPool: Chromium перезапущено (generation={gen}).")
            except Exception as e:
                self._log_fn(f"[OLX browser] BrowserPool: помилка restart Chromium: {e}")
                raise

    def _schedule_recovery(self, reason: str) -> None:
        """З клієнтського потоку: поставити recovery на owner-loop (неблокуюче)."""
        loop = self._loop
        if not loop or loop.is_closed() or self._stop_requested:
            return
        self._log_fn(f"[OLX browser] BrowserPool: schedule recovery ({reason})")

        async def _recover() -> None:
            try:
                await self._restart_browser()
            except Exception as e:
                self._log_fn(f"[OLX browser] BrowserPool recovery failed: {e}")

        try:
            asyncio.run_coroutine_threadsafe(_recover(), loop)
        except Exception as e:
            self._log_fn(f"[OLX browser] BrowserPool: не вдалося schedule recovery: {e}")

    async def _recover_after_hang(self, slot: dict) -> None:
        from scripts.olx_scraper import config as scraper_config
        self._consecutive_hangs += 1
        restart_after = max(1, int(getattr(scraper_config, "BROWSER_POOL_RESTART_AFTER_HANGS", 2) or 2))
        if self._consecutive_hangs >= restart_after:
            await self._restart_browser()
            return
        self._log_fn(
            f"[OLX browser] BrowserPool: recreate context після hang "
            f"(slot={slot.get('slot_idx')}, hangs={self._consecutive_hangs}/{restart_after})"
        )
        try:
            await self._recreate_slot_context(slot)
        except Exception as e:
            self._log_fn(f"[OLX browser] BrowserPool: recreate context failed ({e}), full restart...")
            await self._restart_browser()

    async def _slot_worker(self, slot: dict) -> None:
        from scripts.olx_scraper import config as scraper_config

        fetch_budget = float(getattr(scraper_config, "BROWSER_POOL_FETCH_BUDGET_SEC", 160) or 160)
        fetch_budget = max(60.0, fetch_budget)

        while True:
            req = await self._request_queue.get()  # type: ignore[arg-type]
            if req is None:
                return
            if req.abandoned:
                req.done.set()
                continue

            kind = getattr(req, "kind", "detail") or "detail"
            if kind == "list":
                fetch_task = asyncio.create_task(self._fetch_list_with_slot(slot, req.url))
            else:
                fetch_task = asyncio.create_task(self._fetch_detail_with_slot(slot, req.url))
            try:
                done_set, _ = await asyncio.wait({fetch_task}, timeout=fetch_budget)
                if fetch_task not in done_set:
                    self._log_fn(
                        f"[OLX browser] BrowserPool: fetch budget {fetch_budget:.0f}s вичерпано "
                        f"(slot={slot.get('slot_idx')}): {req.url[:60]}..."
                    )
                    await self._force_close_page(slot)
                    fetch_task.cancel()
                    try:
                        await asyncio.wait_for(fetch_task, timeout=8)
                    except Exception:
                        pass
                    full_restart_done = False
                    if not fetch_task.done():
                        self._log_fn("[OLX browser] BrowserPool: fetch-task все ще живий після close — full restart")
                        try:
                            await self._restart_browser()
                            full_restart_done = True
                        except Exception as e:
                            self._log_fn(f"[OLX browser] BrowserPool: restart після zombie-task: {e}")
                        try:
                            await asyncio.wait_for(fetch_task, timeout=5)
                        except Exception:
                            pass
                    if not req.abandoned:
                        req.error = RuntimeError(
                            f"BrowserPool fetch budget exceeded while fetching {kind}: {req.url[:80]}"
                        )
                    if not full_restart_done:
                        await self._recover_after_hang(slot)
                else:
                    try:
                        req.result = fetch_task.result()
                        self._consecutive_hangs = 0
                    except BaseException as e:
                        if not req.abandoned:
                            req.error = e
                        if _is_crash_error(e) or "target closed" in str(e).lower():
                            try:
                                await self._recreate_slot_context(slot)
                            except Exception:
                                await self._restart_browser()
            except BaseException as e:
                if not req.abandoned:
                    req.error = e
            finally:
                req.done.set()

    def _enqueue_and_wait(self, url: str, kind: str, *, _retries_left: int = 1) -> PageResult:
        if not self._loop or not self._request_queue:
            raise RuntimeError("BrowserPool is not initialized.")
        from scripts.olx_scraper import config as scraper_config

        client_timeout = float(getattr(scraper_config, "BROWSER_POOL_CLIENT_TIMEOUT_SEC", 200) or 200)
        client_timeout = max(90.0, client_timeout)

        done = threading.Event()
        req = BrowserPagePool._PoolRequest(url=url, done=done, kind=kind)
        fut = asyncio.run_coroutine_threadsafe(self._request_queue.put(req), self._loop)
        fut.result(timeout=10)
        if not done.wait(timeout=client_timeout):
            req.abandoned = True
            self._schedule_recovery(f"client timeout {client_timeout:.0f}s: {url[:50]}")
            if _retries_left > 0:
                self._log_fn(
                    f"[OLX browser] BrowserPool timeout — recovery + retry ({_retries_left} left): {url[:50]}..."
                )
                time.sleep(5)
                return self._enqueue_and_wait(url, kind, _retries_left=_retries_left - 1)
            raise RuntimeError(f"BrowserPool timeout while fetching {kind}: {url[:80]}")
        if req.error:
            err_msg = str(req.error).lower()
            is_recoverable = (
                "fetch budget exceeded" in err_msg
                or "crashed" in err_msg
                or "target closed" in err_msg
                or "browser has been closed" in err_msg
            )
            if is_recoverable and _retries_left > 0:
                self._log_fn(
                    f"[OLX browser] BrowserPool recoverable error — retry ({_retries_left} left): {url[:50]}... ({req.error})"
                )
                time.sleep(2)
                return self._enqueue_and_wait(url, kind, _retries_left=_retries_left - 1)
            raise req.error
        return req.result or PageResult("", 0)

    def get_detail_page(self, url: str, *, _retries_left: int = 1) -> PageResult:
        return self._enqueue_and_wait(url, "detail", _retries_left=_retries_left)

    def get_list_page(
        self,
        url: str,
        delay_before: bool = True,
        delay_after: bool = False,
        *,
        _retries_left: int = 1,
    ) -> PageResult:
        """
        List-сторінка через той самий BrowserPool (fallback при HTTP 403).
        delay_before/delay_after — для сумісності з BrowserPageFetcher; пауза перед goto
        уже є в _fetch_list_with_slot (get_delay_seconds).
        """
        _ = delay_before
        _ = delay_after
        return self._enqueue_and_wait(url, "list", _retries_left=_retries_left)

    async def _fetch_list_with_slot(self, slot: dict, url: str) -> PageResult:
        """Швидкий list-fetch (domcontentloaded), без очікування блоку опису detail."""
        from scripts.olx_scraper import config as scraper_config
        from scripts.olx_scraper.parser import detect_antibot_page

        await asyncio.sleep(scraper_config.get_delay_seconds())
        timeout_ms = max(25000, getattr(scraper_config, "REQUEST_TIMEOUT", 25) * 1000)
        last_exc: Optional[Exception] = None
        for attempt in range(2):
            page = slot.get("page")
            if page is None:
                await self._recreate_slot_context(slot)
                page = slot["page"]
            try:
                response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                status = response.status if response else 0
                delay_after = getattr(scraper_config, "DELAY_AFTER_PAGE_LOAD", 0) or 0
                if delay_after > 0:
                    await asyncio.sleep(delay_after)
                html = await page.content()
                antibot = detect_antibot_page(html)
                if antibot.get("is_antibot") and antibot.get("hints"):
                    self._log_fn(
                        f"[OLX browser] List: ознаки антиботу: {', '.join(antibot.get('hints', []))}. Повтор через 8 с..."
                    )
                    await asyncio.sleep(8)
                    response = await page.goto(url, wait_until="domcontentloaded", timeout=timeout_ms)
                    status = response.status if response else 0
                    html = await page.content()
                return PageResult(html, status)
            except Exception as e:
                last_exc = e
                self._log_fn(f"[OLX browser] Помилка list {url[:50]}...: {e}")
                err_str = str(e).lower()
                if "404" in err_str or "net::err_aborted" in err_str:
                    return PageResult("", 404)
                if "502" in err_str or "503" in err_str or "504" in err_str:
                    return PageResult("", 502)
                if _is_crash_error(e) and attempt == 0:
                    self._log_fn("[OLX browser] Краш list-сторінки (pool) — перестворюємо сторінку...")
                    await self._recreate_slot_page(slot)
                    await asyncio.sleep(2)
                    continue
                raise
        if last_exc:
            raise last_exc
        return PageResult("", 0)

    async def _fetch_detail_with_slot(self, slot: dict, url: str) -> PageResult:
        from scripts.olx_scraper import config as scraper_config
        from scripts.olx_scraper.parser import detect_antibot_page

        await asyncio.sleep(scraper_config.get_delay_detail_seconds())
        # Обмежуємо один goto, щоб 2–3 спроби вміщались у fetch budget.
        configured_ms = max(30000, getattr(scraper_config, "REQUEST_DETAIL_TIMEOUT", 90) * 1000)
        timeout_ms = min(60000, configured_ms)
        wait_until = getattr(scraper_config, "BROWSER_DETAIL_WAIT_UNTIL", "load")
        use_fast_wait = False
        last_exc: Optional[Exception] = None
        for attempt in range(3):
            current_wait = "domcontentloaded" if (attempt == 2 or use_fast_wait) else wait_until
            page = slot.get("page")
            if page is None:
                await self._recreate_slot_context(slot)
                page = slot["page"]
            try:
                response = await page.goto(url, wait_until=current_wait, timeout=timeout_ms)
                status = response.status if response else 0
                settle = scraper_config.get_detail_post_goto_settle_seconds()
                if settle > 0:
                    await asyncio.sleep(settle)
                await _wait_for_description_async(page, timeout_ms=12000)
                html = await page.content()
                antibot = detect_antibot_page(html)
                if antibot.get("is_antibot") and antibot.get("hints"):
                    self._log_fn(f"[OLX browser] Ознаки антиботу: {', '.join(antibot.get('hints', []))}. Повтор через 8 с...")
                    await asyncio.sleep(8)
                    response = await page.goto(url, wait_until=current_wait, timeout=timeout_ms)
                    status = response.status if response else 0
                    await _wait_for_description_async(page, timeout_ms=10000)
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
