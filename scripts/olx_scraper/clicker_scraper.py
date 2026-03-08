# -*- coding: utf-8 -*-
"""
Експериментальний OLX-скрапер на основі браузерної автоматизації (клікер).

Замість HTTP-запитів використовує Playwright: відкриває OLX у реальному браузері,
імітує поведінку користувача (навігація, скрол, кліки), збирає HTML і передає
його в існуючі парсери (parse_listings_page, parse_detail_page). Результати
зберігаються в raw_olx_listings — далі обробляються тим самим pipeline (Phase 2).

Не змінює основні флоу оновлення даних (run_update, source_data_load_service).
"""

from __future__ import annotations

import random
import time
from typing import Any, Callable, Dict, List, Optional

# Імпорти всередині run() щоб не тягнути playwright при старті додатку
# та уникнути помилок якщо playwright не встановлено


def run_olx_clicker_scraper(
    settings: Any,
    *,
    max_pages: int = 3,
    max_listings: int = 30,
    headless: bool = True,
    log_fn: Optional[Callable[[str], None]] = None,
    progress_callback: Optional[Callable[[Dict[str, Any]], None]] = None,
) -> Dict[str, Any]:
    """
    Запускає експериментальний скрапер OLX через браузер (Playwright).

    max_pages: скільки сторінок пошуку обробити (перехід «наступна»).
    max_listings: максимум оголошень, для яких завантажити деталі та зберегти в raw.
    headless: чи запускати браузер без GUI.
    log_fn: функція для логів (напр. запис у файл або статус задачі).
    progress_callback: викликається з dict {pages_done, listings_seen, details_fetched, saved, errors}.

    Повертає: {
        "success": bool,
        "message": str,
        "pages_done": int,
        "listings_seen": int,
        "details_fetched": int,
        "saved": int,
        "errors": int,
    }
    """
    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)

    try:
        from playwright.sync_api import sync_playwright
    except ImportError:
        log("Playwright не встановлено. Виконайте: pip install playwright && playwright install chromium")
        return {
            "success": False,
            "message": "Playwright не встановлено",
            "pages_done": 0,
            "listings_seen": 0,
            "details_fetched": 0,
            "saved": 0,
            "errors": 1,
        }

    from pathlib import Path
    import sys
    _root = Path(__file__).resolve().parent.parent.parent
    if str(_root) not in sys.path:
        sys.path.insert(0, str(_root))

    from data.database.connection import MongoDBConnection
    from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
    from scripts.olx_scraper import config as scraper_config
    from scripts.olx_scraper.parser import parse_listings_page, parse_detail_page, detect_antibot_page
    from scripts.olx_scraper.helpers import search_data_from_listing
    from utils.hash_utils import calculate_search_data_hash

    MongoDBConnection.initialize(settings)
    raw_repo = RawOlxListingsRepository()

    pages_done = 0
    listings_seen = 0
    details_fetched = 0
    saved = 0
    errors = 0
    seen_urls: set[str] = set()

    def report() -> None:
        if progress_callback:
            progress_callback({
                "pages_done": pages_done,
                "listings_seen": listings_seen,
                "details_fetched": details_fetched,
                "saved": saved,
                "errors": errors,
            })

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=headless)
        context = browser.new_context(
            viewport={"width": 1280, "height": 720},
            user_agent=scraper_config.USER_AGENT,
            locale="uk-UA",
        )
        context.set_extra_http_headers({
            "Accept-Language": "uk,en;q=0.9",
        })
        page = context.new_page()
        page.set_default_timeout(30000)
        page.set_default_navigation_timeout(45000)

        try:
            # Стартова сторінка — нежитлова нерухомість (продаж), сортування нові
            start_url = scraper_config.get_commercial_real_estate_list_url(
                page=1, sale_only=True, sort_newest=True, region_slug=None
            )
            log(f"[OLX clicker] Відкриваю {start_url}")
            page.goto(start_url, wait_until="domcontentloaded")
            time.sleep(2 + random.uniform(1, 3))  # імітація перегляду

            for page_num in range(1, max_pages + 1):
                if page_num > 1:
                    next_url = scraper_config.get_commercial_real_estate_list_url(
                        page=page_num, sale_only=True, sort_newest=True, region_slug=None
                    )
                    log(f"[OLX clicker] Сторінка {page_num}: {next_url}")
                    page.goto(next_url, wait_until="domcontentloaded")
                    time.sleep(1.5 + random.uniform(0.5, 2))

                html = page.content()
                antibot = detect_antibot_page(html)
                if antibot.get("is_antibot"):
                    log(f"[OLX clicker] Ознаки антиботу: {antibot.get('hints', [])}")
                    errors += 1
                    report()
                    break

                listings = parse_listings_page(html)
                if not listings:
                    log(f"[OLX clicker] На сторінці {page_num} не знайдено оголошень")
                    pages_done += 1
                    report()
                    continue

                pages_done += 1
                for item in listings:
                    url = (item.get("url") or "").strip()
                    if not url or url in seen_urls:
                        continue
                    seen_urls.add(url)
                    listings_seen += 1
                    if details_fetched + saved >= max_listings:
                        log(f"[OLX clicker] Досягнуто ліміт {max_listings} оголошень")
                        break

                    # Перевірка: чи потрібно оновлювати (новий або інший search_data_hash)
                    search_data = search_data_from_listing(item)
                    new_hash = calculate_search_data_hash(search_data)
                    existing = raw_repo.find_by_url(url)
                    if existing and (existing.get("search_data_hash") == new_hash):
                        continue

                    time.sleep(random.uniform(2, 6))  # затримка між відкриттям оголошень
                    try:
                        page.goto(url, wait_until="domcontentloaded")
                        time.sleep(1 + random.uniform(0.5, 2))
                        detail_html = page.content()
                        detail_data = parse_detail_page(detail_html)
                        if detail_data.get("_inactive"):
                            detail_data.pop("_inactive", None)
                        raw_repo.upsert_raw(
                            url=url,
                            search_data=search_data,
                            detail=detail_data or None,
                            fetch_filters={"source": "clicker", "category": "commercial_real_estate"},
                            approximate_region=None,
                        )
                        details_fetched += 1
                        saved += 1
                        log(f"[OLX clicker] Збережено: {url[:60]}...")
                    except Exception as e:
                        errors += 1
                        log(f"[OLX clicker] Помилка {url[:50]}...: {e}")
                    report()

                if details_fetched + saved >= max_listings:
                    break

        finally:
            browser.close()

    message = (
        f"Сторінок: {pages_done}, оголошень переглянуто: {listings_seen}, "
        f"завантажено та збережено: {saved}, помилок: {errors}"
    )
    return {
        "success": errors == 0 or saved > 0,
        "message": message,
        "pages_done": pages_done,
        "listings_seen": listings_seen,
        "details_fetched": details_fetched,
        "saved": saved,
        "errors": errors,
    }
