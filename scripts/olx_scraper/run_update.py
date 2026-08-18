# -*- coding: utf-8 -*-
"""
Оновлення оголошень OLX: нежитлова нерухомість + земельні ділянки.
Використовується в процедурах оновлення даних (main, Telegram) разом із ProZorro.

Запуск з кореня проекту:
  py scripts/olx_scraper/run_update.py
"""

import logging
import queue
import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Set, Tuple, Union

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
from business.services.olx_llm_extractor_service import OlxLLMExtractorService
from business.services.unified_listings_service import UnifiedListingsService
from business.services.geocoding_service import GeocodingService
from business.services.currency_rate_service import CurrencyRateService
from utils.price_metrics import compute_price_metrics
from scripts.olx_scraper import config as scraper_config
from scripts.olx_scraper.fetcher import fetch_page, get_session
from scripts.olx_scraper.parser import parse_listings_page, parse_detail_page

from scripts.olx_scraper.helpers import (
    search_data_from_listing,
    search_data_changed,
    _collect_and_geocode_locations,
    _address_line_from_llm_address,
)
from utils.hash_utils import calculate_search_data_hash
from business.services.llm_processing_regions_service import is_region_enabled_for_llm

logger = logging.getLogger(__name__)


try:
    from tqdm import tqdm
except ImportError:
    tqdm = None


def _parse_listed_at_iso(iso_str: Optional[str]) -> Optional[datetime]:
    """Парсить listed_at_iso (UTC) для порівняння з порогом. Повертає datetime або None."""
    if not iso_str or not isinstance(iso_str, str):
        return None
    try:
        s = iso_str.strip().replace("Z", "+00:00")
        return datetime.fromisoformat(s)
    except (ValueError, TypeError):
        return None


def _process_category(
    get_list_url: Callable[[int], str],
    max_pages: Optional[int],
    category_label: str,
    repo: OlxListingsRepository,
    llm_extractor: OlxLLMExtractorService,
    geocoding_service: GeocodingService,
    unified_service: Optional[UnifiedListingsService] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    cutoff_utc: Optional[datetime] = None,
    usd_rate: Optional[float] = None,
    session: Optional[Any] = None,
    region_name: Optional[str] = None,
    browser_fetcher: Optional[Any] = None,
) -> Tuple[int, int, set, List[str]]:
    """
    Обробляє одну категорію OLX у два етапи:
    1) Прохід по сторінках пошуку — збір повного списку оголошень.
    2) Завантаження деталей (сирі дані) — без LLM. Оголошення, що потребують LLM, додаються в pending.
    LLM-обробка виконується окремо в Phase 2 (після збору всіх сирих даних).
    region_name: область пошуку OLX (фільтр сторінки); якщо не в переліку увімкнених — LLM не викликається.
    Повертає (total_listings, total_detail_fetches, search_urls, pending_llm_urls).
    """
    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            print(msg, flush=True)

    # ——— Етап 1: сторінки пошуку — збираємо список оголошень ———
    all_listings: List[Dict[str, Any]] = []
    stop_pages = False
    page = 1

    while True:
        if stop_pages:
            break
        if max_pages is not None and page > max_pages:
            break
        url = get_list_url(page)
        if page == 1:
            log(f"[OLX] {category_label}: URL пошуку (перша сторінка): {url}")
        page_label = f"{page}" if max_pages is None else f"{page}/{max_pages}"
        log(f"[OLX] {category_label}: сторінка пошуку {page_label}")
        listings = []
        retry_count = getattr(scraper_config, "RETRY_EMPTY_PAGE_COUNT", 2)
        fetch_failed = False
        for attempt in range(max(1, retry_count + 1)):
            try:
                response = fetch_page(
                    url,
                    delay_before=(attempt == 0),
                    delay_after=True,
                    session=session,
                )
            except Exception as e:
                err_str = str(e)
                is_404 = "404" in err_str
                is_server_error = "502" in err_str or "503" in err_str or "504" in err_str
                log(f"[OLX] Помилка запиту {url}: {e}")
                if is_404:
                    # 404 = сторінка не існує (менше результатів ніж сторінок) — кінець пагінації
                    log(f"[OLX] {category_label}: 404 на сторінці {page} — кінець результатів")
                    stop_pages = True
                    break
                if attempt < retry_count:
                    delay_sec = scraper_config.get_delay_seconds()
                    if is_server_error:
                        delay_sec = max(delay_sec, 10)  # 502/503 — довша пауза перед повтором
                    time.sleep(delay_sec)
                    continue
                log(f"[OLX] {category_label}: пропускаємо сторінку {page} після {retry_count + 1} спроб")
                fetch_failed = True
                page += 1
                break
            # OLX при 0 результатів показує ліві/рекомендовані оголошення. Зупинка лише при явному 0:
            # - «знайшли 0 оголошень» / «ми знайшли 0 оголошень»
            # - «ми нічого не знайшли, тому підібрали рекламні» + regex знаходить «знайшли 0 оголошень»
            # (фраза «рекламні» з’являється й на сторінках з результатами, напр. Донецька — тому перевіряємо обидва).
            import re
            html_lower = response.text.lower()
            html_norm = html_lower.replace("\u00a0", " ").replace("\u202f", " ")
            has_explicit_zero = (
                "знайшли 0 оголошень" in html_lower
                or "ми знайшли 0 оголошень" in html_lower
            )
            m = re.search(r"знайшли\s+(?:понад\s+)?([\d\s]+)оголошень", html_norm)
            found_n = int(m.group(1).replace(" ", "")) if m and m.group(1).replace(" ", "").isdigit() else None
            has_reklamni = "ми нічого не знайшли, тому підібрали рекламні" in html_lower
            should_stop = has_explicit_zero or (has_reklamni and found_n == 0)
            if should_stop:
                log(f"[OLX] {category_label}: сторінка показує 0 результатів — зупинка (OLX підставляє ліві оголошення)")
                stop_pages = True
                break
            listings = parse_listings_page(response.text)
            log(f"[OLX] Оголошень на сторінці: {len(listings)}")
            if listings:
                break
        min_full = getattr(scraper_config, "MIN_LISTINGS_PER_FULL_PAGE", 15)
        if listings and len(listings) < min_full:
            log(f"[OLX] {category_label}: на сторінці менше ніж повна вибірка ({len(listings)} < {min_full}) — остання сторінка, зупинка пагінації")
            stop_pages = True
        if fetch_failed:
            continue
        if not listings:
            break

        # Топові (платні) оголошення OLX вставляються на сторінку без урахування сортування.
        # Зупиняємось лише якщо більше половини оголошень на сторінці старші за граничну дату (cutoff).
        # Шукаємо допоки не отримаємо потрібну дату або не закінчаться сторінки пошуку.
        count_older_than_cutoff = 0
        for item in listings:
            listing_url = item.get("url")
            if cutoff_utc is not None:
                listed_at = _parse_listed_at_iso(item.get("listed_at_iso"))
                if listed_at is not None and listed_at < cutoff_utc:
                    count_older_than_cutoff += 1
                    continue  # Не додаємо до all_listings оголошення старші за cutoff
            if listing_url:
                all_listings.append(item)

        if cutoff_utc is not None and count_older_than_cutoff > len(listings) / 2:
            log(f"[OLX] {category_label}: більше половини оголошень на сторінці старші за граничну дату ({count_older_than_cutoff}/{len(listings)}) — зупинка")
            stop_pages = True

        # Діагностика: при cutoff — скільки оголошень мають listed_at_iso (для перевірки парсингу дати)
        if cutoff_utc is not None and page == 1 and listings:
            with_iso = sum(1 for it in listings if it.get("listed_at_iso"))
            log(f"[OLX] {category_label}: на сторінці {with_iso}/{len(listings)} оголошень з розпарсеною датою (listed_at_iso)")

        # Дата останнього оголошення на сторінці (діагностика)
        last_on_page = listings[-1] if listings else None
        if last_on_page:
            last_date = last_on_page.get("date_text") or last_on_page.get("listed_at_iso") or "—"
            log(f"[OLX] {category_label}: дата останнього оголошення на сторінці: {last_date}")

        if stop_pages:
            break
        page += 1

    total_count = len(all_listings)
    search_urls = {item.get("url") for item in all_listings if item.get("url")}
    log(f"[OLX] {category_label}: всього оголошень до опрацювання: {total_count}")

    if total_count == 0:
        return 0, 0, search_urls, []

    if browser_fetcher is None:
        raise RuntimeError("Browser detail fetcher is required for OLX detail pages.")

    # ——— Етап 2: завантаження деталей (сирі дані, без LLM). LLM — у Phase 2. ———
    total_detail_fetches = 0
    pending_llm_urls: List[str] = []
    for idx, item in enumerate(all_listings, start=1):
        listing_url = item.get("url")
        if not listing_url:
            continue
        log(f"[OLX] {category_label}: опрацювання {idx}/{total_count}")
        search_data = search_data_from_listing(item)
        if region_name:
            search_data["region_filter"] = region_name
        existing = repo.find_by_url(listing_url)

        need_detail = False
        if existing is None:
            need_detail = True
        elif not existing.get("detail"):
            need_detail = True
        elif search_data_changed(existing.get("search_data") or {}, search_data):
            need_detail = True

        if need_detail:
            try:
                detail_result = browser_fetcher.get_detail_page(listing_url)
                detail_data = parse_detail_page(detail_result.text)
                if detail_data.get("_inactive"):
                    detail_data.pop("_inactive", None)

                # --- Хеш ключових полів оголошення для контролю повторних викликів LLM ---
                old_detail = (existing or {}).get("detail") or {}
                old_llm = old_detail.get("llm")
                old_hash = old_detail.get("llm_content_hash")

                # Якщо раніше хеш не зберігали, але є старий LLM — обчислюємо його на льоту
                if old_llm and not old_hash:
                    try:
                        old_hash = llm_extractor.calculate_listing_hash(
                            (existing or {}).get("search_data") or {},
                            old_detail,
                        )
                    except Exception:
                        old_hash = None

                new_hash = None
                try:
                    new_hash = llm_extractor.calculate_listing_hash(search_data, detail_data)
                except Exception:
                    new_hash = None

                if old_llm and old_hash and new_hash and old_hash == new_hash:
                    # Ключова інформація не змінилась — перевикористовуємо попередній результат LLM (Phase 1)
                    detail_data["llm"] = old_llm
                    detail_data["llm_content_hash"] = old_hash
                else:
                    # Потребує LLM — зберігаємо сирі дані, LLM у Phase 2 (лише якщо область увімкнена для LLM)
                    if new_hash:
                        detail_data["llm_content_hash"] = new_hash
                    detail_data["llm_pending"] = True
                    if region_name is None or is_region_enabled_for_llm(region_name):
                        pending_llm_urls.append(listing_url)

                # Геокодування, метрики, formatted_address — лише коли є LLM (reuse). Інакше — у Phase 2.
                if "llm" in detail_data:
                    geography_service = None
                    try:
                        from business.services.geography_service import GeographyService
                        geography_service = GeographyService()
                    except ImportError:
                        pass

                    result = _collect_and_geocode_locations(
                        search_data, detail_data, geocoding_service, geography_service
                    )
                    if len(result) == 3:
                        geocode_hashes, resolved_locations, address_refs_list = result
                    else:
                        geocode_hashes, resolved_locations = result
                        address_refs_list = []

                    detail_data["geocode_query_hashes"] = geocode_hashes
                    detail_data["resolved_locations"] = resolved_locations
                    if address_refs_list:
                        detail_data["address_refs"] = address_refs_list

                    price_value = search_data.get("price_value")
                    llm_struct = (detail_data.get("llm") or {}) if isinstance(detail_data, dict) else {}
                    total_area_m2 = llm_struct.get("total_area_m2")
                    if total_area_m2 is None:
                        total_area_m2 = search_data.get("area_m2")
                    land_area_sqm = llm_struct.get("land_area_sqm")
                    if land_area_sqm is None and llm_struct.get("land_area_ha") is not None:
                        try:
                            land_area_sqm = float(llm_struct["land_area_ha"]) * 10000.0
                        except (TypeError, ValueError):
                            land_area_sqm = None
                    metrics = compute_price_metrics(
                        total_price_uah=price_value,
                        building_area_sqm=total_area_m2,
                        land_area_sqm=land_area_sqm,
                        uah_per_usd=usd_rate,
                    )
                    detail_data["price_metrics"] = metrics

                    if price_value is not None:
                        currency = (search_data.get("currency") or "UAH").strip().upper() or "UAH"
                        if currency not in ("UAH", "USD", "EUR"):
                            currency = "UAH"
                        detail_data["price"] = {"value": float(price_value) if not isinstance(price_value, float) else price_value, "currency": currency}

                    formatted_address = ""
                    if address_refs_list and geography_service:
                        parts = []
                        for refs in address_refs_list:
                            if isinstance(refs, dict):
                                addr = geography_service.format_address(refs)
                                if addr and addr not in parts:
                                    parts.append(addr)
                        formatted_address = "; ".join(parts) if parts else ""
                    if not formatted_address:
                        for addr in (detail_data.get("llm") or {}).get("addresses") or []:
                            if isinstance(addr, dict):
                                line = _address_line_from_llm_address(addr)
                                if line:
                                    formatted_address = line
                                    break
                    if not formatted_address:
                        formatted_address = (search_data.get("location") or "").strip()
                    if formatted_address:
                        if "llm" not in detail_data:
                            detail_data["llm"] = {}
                        detail_data["llm"]["parsed_address"] = {"formatted_address": formatted_address}
                else:
                    # llm_pending — базові поля з search_data
                    price_value = search_data.get("price_value")
                    if price_value is not None:
                        currency = (search_data.get("currency") or "UAH").strip().upper() or "UAH"
                        if currency not in ("UAH", "USD", "EUR"):
                            currency = "UAH"
                        detail_data["price"] = {"value": float(price_value) if not isinstance(price_value, float) else price_value, "currency": currency}

                repo.upsert_listing(listing_url, search_data, detail=detail_data, is_active=True)
                total_detail_fetches += 1
                if unified_service:
                    try:
                        unified_service.sync_olx_listing(listing_url)
                    except Exception as sync_err:
                        log(f"[OLX] Помилка синхронізації в unified: {sync_err}")
            except Exception as e:
                log(f"[OLX] Помилка деталей {listing_url[:50]}...: {e}")
                # При помилці зберігаємо хоча б оновлені search_data / старі detail
                repo.upsert_listing(
                    listing_url,
                    search_data,
                    detail=existing.get("detail") if existing else None,
                    is_active=True,
                )
                if unified_service:
                    try:
                        unified_service.sync_olx_listing(listing_url)
                    except Exception as sync_err:
                        log(f"[OLX] Помилка синхронізації в unified: {sync_err}")
        else:
            # Дані з пошуку не змінились — оновлюємо лише olx_listings (search_data, updated_at).
            repo.upsert_listing(listing_url, search_data, detail=existing.get("detail"), is_active=True)

    return total_count, total_detail_fetches, search_urls, pending_llm_urls


def _process_category_raw_only(
    get_list_url: Callable[[int], str],
    max_pages: Optional[int],
    category_label: str,
    raw_repo: RawOlxListingsRepository,
    log_fn: Optional[Callable[[str], None]] = None,
    cutoff_utc: Optional[datetime] = None,
    session: Optional[Any] = None,
    region_name: Optional[str] = None,
    browser_fetcher: Optional[Any] = None,
    start_page: int = 1,
    source_load_run_id: Optional[str] = None,
    on_page_done: Optional[Callable[[int], None]] = None,
) -> Tuple[int, List[str]]:
    """
    Phase 1 (raw pipeline): list HTTP → одразу detail+upsert по сторінці (streaming, low-RAM).
    start_page: продовження пагінації після resume (last_page+1).
    on_page_done(page): checkpoint після успішної обробки сторінки.
    Повертає (total_count, loaded_urls) — loaded_urls лише змінені/нові в цьому проході.
    """
    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            print(msg, flush=True)

    if browser_fetcher is None:
        raise RuntimeError("Browser detail fetcher is required for OLX detail pages.")

    stop_pages = False
    page = max(1, int(start_page or 1))
    total_count = 0
    loaded_urls: List[str] = []
    fetch_filters: Dict[str, Any] = {"category_label": category_label}
    if region_name:
        fetch_filters["region_filter"] = region_name
    approximate_region = region_name

    while True:
        if stop_pages:
            break
        if max_pages is not None and page > max_pages:
            break
        url = get_list_url(page)
        if page == 1 or page == start_page:
            log(f"[OLX raw] {category_label}: URL пошуку (сторінка {page}): {url}")
        page_label = f"{page}" if max_pages is None else f"{page}/{max_pages}"
        log(f"[OLX raw] {category_label}: сторінка пошуку {page_label}")
        listings = []
        retry_count = getattr(scraper_config, "RETRY_EMPTY_PAGE_COUNT", 2)
        fetch_failed = False
        last_was_403 = False
        for attempt in range(max(1, retry_count + 1)):
            response_text = ""
            status_code = 200
            try:
                response = fetch_page(
                    url,
                    delay_before=(attempt == 0),
                    delay_after=False,
                    session=session,
                )
                response_text = response.text
                status_code = getattr(response, "status_code", 200)
                if status_code == 404:
                    log(f"[OLX raw] {category_label}: 404 на сторінці {page} — кінець результатів")
                    stop_pages = True
                    break
                if status_code == 403:
                    raise RuntimeError("HTTP 403 Forbidden")
                if status_code >= 500:
                    raise RuntimeError(f"HTTP {status_code}")
            except Exception as e:
                err_str = str(e)
                is_404 = "404" in err_str
                is_403 = "403" in err_str
                is_server_error = "502" in err_str or "503" in err_str or "504" in err_str
                last_was_403 = is_403
                log(f"[OLX raw] Помилка запиту {url}: {e}")
                if is_404:
                    log(f"[OLX raw] {category_label}: 404 на сторінці {page} — кінець результатів")
                    stop_pages = True
                    break
                if attempt < retry_count:
                    if is_403:
                        delay_sec = scraper_config.get_403_backoff_seconds()
                        log(f"[OLX raw] HTTP 403 — backoff {delay_sec:.1f} с перед повтором...")
                    else:
                        delay_sec = scraper_config.get_delay_seconds()
                        if is_server_error:
                            delay_sec = max(delay_sec, 10)
                    time.sleep(delay_sec)
                    continue
                # Після HTTP-спроб: браузерний fallback на 403 (антибот часто пропускає Playwright)
                use_browser_list = (
                    last_was_403
                    and getattr(scraper_config, "LIST_FALLBACK_BROWSER_ON_403", True)
                    and browser_fetcher is not None
                    and hasattr(browser_fetcher, "get_list_page")
                )
                if use_browser_list:
                    log(f"[OLX raw] {category_label}: HTTP 403 після {retry_count + 1} спроб — fallback list через браузер")
                    try:
                        br = browser_fetcher.get_list_page(url, delay_before=True, delay_after=False)
                        status_code = getattr(br, "status_code", 0) or 0
                        response_text = getattr(br, "text", "") or ""
                        if status_code == 404:
                            log(f"[OLX raw] {category_label}: 404 на сторінці {page} — кінець результатів")
                            stop_pages = True
                            break
                        if status_code == 403 or status_code >= 500:
                            raise RuntimeError(f"Browser list HTTP {status_code}")
                        last_was_403 = False
                        # успіх — йдемо парсити response_text нижче (не break з except)
                    except Exception as be:
                        log(f"[OLX raw] Browser list fallback не вдався: {be}")
                        log(
                            f"[OLX raw] {category_label}: зупинка категорії на сторінці {page} "
                            f"(антибот 403, не скіпаємо пагінацію)"
                        )
                        stop_pages = True
                        fetch_failed = True
                        break
                else:
                    if last_was_403:
                        log(
                            f"[OLX raw] {category_label}: зупинка категорії на сторінці {page} "
                            f"після {retry_count + 1} спроб (403)"
                        )
                        stop_pages = True
                    else:
                        log(f"[OLX raw] {category_label}: пропускаємо сторінку {page} після {retry_count + 1} спроб")
                        page += 1
                    fetch_failed = True
                    break
            import re
            html_lower = response_text.lower()
            html_norm = html_lower.replace("\u00a0", " ").replace("\u202f", " ")
            has_explicit_zero = (
                "знайшли 0 оголошень" in html_lower
                or "ми знайшли 0 оголошень" in html_lower
            )
            m = re.search(r"знайшли\s+(?:понад\s+)?([\d\s]+)оголошень", html_norm)
            found_n = int(m.group(1).replace(" ", "")) if m and m.group(1).replace(" ", "").isdigit() else None
            has_reklamni = "ми нічого не знайшли, тому підібрали рекламні" in html_lower
            should_stop = has_explicit_zero or (has_reklamni and found_n == 0)
            if should_stop:
                log(f"[OLX raw] {category_label}: сторінка показує 0 результатів — зупинка (OLX підставляє ліві оголошення)")
                stop_pages = True
                break
            listings = parse_listings_page(response_text)
            log(f"[OLX raw] Оголошень на сторінці: {len(listings)}")
            if listings:
                break
            if attempt < retry_count:
                delay_sec = scraper_config.get_delay_seconds()
                log(f"[OLX raw] 0 оголошень — повторна спроба через {delay_sec:.1f} с...")
                time.sleep(delay_sec)
        min_full = getattr(scraper_config, "MIN_LISTINGS_PER_FULL_PAGE", 15)
        if listings and len(listings) < min_full:
            log(f"[OLX raw] {category_label}: на сторінці менше ніж повна вибірка ({len(listings)} < {min_full}) — остання сторінка, зупинка пагінації")
            stop_pages = True
        if fetch_failed:
            continue
        if not listings:
            break

        count_older_than_cutoff = 0
        page_items: List[Dict[str, Any]] = []
        for item in listings:
            listing_url = item.get("url")
            if cutoff_utc is not None:
                listed_at = _parse_listed_at_iso(item.get("listed_at_iso"))
                if listed_at is not None and listed_at < cutoff_utc:
                    count_older_than_cutoff += 1
                    continue
            if listing_url:
                page_items.append(item)

        if cutoff_utc is not None and count_older_than_cutoff > len(listings) / 2:
            log(f"[OLX raw] {category_label}: більше половини оголошень на сторінці старші за граничну дату ({count_older_than_cutoff}/{len(listings)}) — зупинка")
            stop_pages = True

        if cutoff_utc is not None and page == 1 and listings:
            with_iso = sum(1 for it in listings if it.get("listed_at_iso"))
            log(f"[OLX raw] {category_label}: на сторінці {with_iso}/{len(listings)} оголошень з розпарсеною датою (listed_at_iso)")

        last_on_page = listings[-1] if listings else None
        if last_on_page:
            last_date = last_on_page.get("date_text") or last_on_page.get("listed_at_iso") or "—"
            log(f"[OLX raw] {category_label}: дата останнього оголошення на сторінці: {last_date}")

        # Streaming: detail + upsert одразу після list-сторінки (без накопичення all_listings)
        for idx, item in enumerate(page_items, start=1):
            listing_url = item.get("url")
            if not listing_url:
                continue
            total_count += 1
            log(f"[OLX raw] {category_label}: опрацювання стор.{page} {idx}/{len(page_items)}")
            search_data = search_data_from_listing(item)
            new_hash = calculate_search_data_hash(search_data)
            existing_raw = raw_repo.find_by_url(listing_url)
            if existing_raw and existing_raw.get("search_data_hash") == new_hash:
                continue
            try:
                detail_result = browser_fetcher.get_detail_page(listing_url)
                detail_html = detail_result.text
                detail_data = parse_detail_page(detail_html)
                if detail_data.get("_inactive"):
                    detail_data.pop("_inactive", None)
                raw_repo.upsert_raw(
                    url=listing_url,
                    search_data=search_data,
                    detail=detail_data or None,
                    fetch_filters=fetch_filters,
                    approximate_region=approximate_region,
                    source_load_run_id=source_load_run_id,
                )
                loaded_urls.append(listing_url)
            except Exception as e:
                log(f"[OLX raw] Помилка деталей {listing_url[:50]}...: {e}")

        if on_page_done is not None:
            try:
                on_page_done(page)
            except Exception as e:
                log(f"[OLX raw] checkpoint page {page}: {e}")

        if stop_pages:
            break
        page += 1

    log(f"[OLX raw] {category_label}: всього оголошень (у вікні cutoff): {total_count}, оновлено/завантажено: {len(loaded_urls)}")
    return total_count, loaded_urls


def _process_region_raw_only(
    region_name: str,
    categories: List[Dict[str, Any]],
    raw_repo: RawOlxListingsRepository,
    log_fn: Optional[Callable[[str], None]],
    cutoff_utc: Optional[datetime],
    max_pages_override: Optional[int] = None,
    browser_fetcher: Optional[Any] = None,
) -> Tuple[int, List[str]]:
    """Phase 1: одна область — усі категорії, запис лише в raw. Список — requests, деталі — browser_fetcher. Повертає (total_count, loaded_urls)."""
    session = get_session()
    total_count = 0
    loaded_urls: List[str] = []
    for cat in categories:
        get_list_url = cat.get("get_list_url")
        if not callable(get_list_url):
            continue
        max_pages = (
            max_pages_override
            if max_pages_override is not None
            else min(int(cat.get("max_pages", scraper_config.MAX_SEARCH_PAGES)), scraper_config.MAX_SEARCH_PAGES)
        )
        n_listings, urls = _process_category_raw_only(
            get_list_url,
            max_pages,
            cat.get("label", "?"),
            raw_repo,
            log_fn=log_fn,
            cutoff_utc=cutoff_utc,
            session=session,
            region_name=region_name,
            browser_fetcher=browser_fetcher,
        )
        total_count += n_listings
        loaded_urls.extend(urls)
    return total_count, loaded_urls


def _normalize_region_filter(
    regions_filter: Optional[List[str]],
    available_region_names: List[str],
) -> Tuple[List[str], List[str]]:
    """Нормалізує фільтр регіонів до канонічних назв із ``olx_region_slugs``.

    Підтримуються варіанти:
    - точна канонічна назва ("Київська");
    - довільний регістр ("київська", "КИЇВСЬКА");
    - суфікс "область"/"обл." ("Київська область", "Київська обл.");
    - slug ("ko", "lv", "kiev");
    - latin-подібний slug-варіант, який тоді ігнорується з попередженням.

    Повертає кортеж (matched, unknown) — обидва списки канонічних рядків з вводу.
    Якщо ``regions_filter`` порожній/None — повертає (порожній, порожній); викликач сам
    вирішує, чи це означає «без фільтра» (трактує як None).
    """
    if not regions_filter:
        return [], []
    try:
        slugs = scraper_config.get_olx_region_slugs() or {}
    except Exception:
        slugs = {}
    name_by_lower = {name.lower(): name for name in available_region_names}
    name_by_slug = {str(slug).lower(): name for name, slug in slugs.items() if slug}

    matched_set: List[str] = []
    seen: Set[str] = set()
    unknown: List[str] = []
    for raw in regions_filter:
        if not raw:
            continue
        candidate = str(raw).strip()
        if not candidate:
            continue
        lowered = candidate.lower()
        for suffix in (" область", " обл.", " обл"):
            if lowered.endswith(suffix):
                lowered = lowered[: -len(suffix)].rstrip()
                break
        canonical = name_by_lower.get(lowered) or name_by_slug.get(lowered)
        if canonical and canonical not in seen:
            seen.add(canonical)
            matched_set.append(canonical)
        elif not canonical:
            unknown.append(candidate)
    return matched_set, unknown


def _normalize_listing_types_filter(
    listing_types_filter: Optional[List[str]],
) -> Tuple[List[str], List[str]]:
    """Нормалізує фільтр типів оголошень до значень з ``_get_base_categories()``.

    Допускає:
    - повне ім'я ("Нежитлова нерухомість", "Земельні ділянки", "Земля — рекреаційні");
    - короткі узагальнення ("нежитлова", "земля", "комерційна");
    - довільний регістр.

    Повертає (substring_patterns, unknown). ``substring_patterns`` — це канонічні підрядки,
    які зіставляються з мітками категорій (case-insensitive).
    """
    if not listing_types_filter:
        return [], []
    try:
        base_labels = [str(c.get("label") or "").strip() for c in _get_base_categories()]
        base_labels = [b for b in base_labels if b]
    except Exception:
        base_labels = []

    short_aliases = {
        # Підрядки беремо узагальнено, щоб патерн зіставлявся з усіма варіантами:
        # "Нежитлова нерухомість", "Земля — ...", "Земельні ділянки".
        "нежитлов": "Нежитлов",
        "комерц": "Нежитлов",
        "commercial": "Нежитлов",
        "земл": "Земл",
        "ділянк": "Земл",
        "land": "Земл",
    }

    patterns: List[str] = []
    seen: Set[str] = set()
    unknown: List[str] = []
    base_lower = [b.lower() for b in base_labels]
    for raw in listing_types_filter:
        if not raw:
            continue
        candidate = str(raw).strip()
        if not candidate:
            continue
        lowered = candidate.lower()
        pattern: Optional[str] = None
        # 1) точна канонічна назва бази → беремо як патерн
        for label, label_lower in zip(base_labels, base_lower):
            if lowered == label_lower:
                pattern = label
                break
        # 2) короткий алias (нежитлов/земл/...) → стійка коротка підстрока
        if not pattern:
            for alias_key, alias_value in short_aliases.items():
                if alias_key in lowered:
                    pattern = alias_value
                    break
        # 3) користувацький рядок є підрядком якоїсь канонічної мітки — беремо його як патерн,
        # щоб він зіставився з усіма мітками, що містять його (мульти-збіг).
        if not pattern:
            for label_lower in base_lower:
                if lowered in label_lower:
                    pattern = candidate
                    break
        if pattern and pattern not in seen:
            seen.add(pattern)
            patterns.append(pattern)
        elif not pattern:
            unknown.append(candidate)
    return patterns, unknown


def _filter_regions_with_categories(
    regions_with_cats: List[Tuple[str, List[Dict[str, Any]]]],
    regions_filter: Optional[List[str]] = None,
    listing_types_filter: Optional[List[str]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> List[Tuple[str, List[Dict[str, Any]]]]:
    """Фільтрує список (region_name, categories) за областями та/або типами оголошень.

    Перед фільтрацією значення з фільтрів нормалізуються до канонічних
    (див. ``_normalize_region_filter`` та ``_normalize_listing_types_filter``).
    Невпізнані значення логуються — після цього порожній набір категорій означає
    реально невалідний фільтр, а не дрібну розбіжність регістру/суфіксу.
    """

    def _log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            logger.warning("%s", msg)

    if not regions_filter and not listing_types_filter:
        return regions_with_cats

    available_region_names = [r for r, _ in regions_with_cats]
    normalized_regions, unknown_regions = _normalize_region_filter(
        regions_filter, available_region_names
    )
    normalized_types, unknown_types = _normalize_listing_types_filter(listing_types_filter)

    if regions_filter and unknown_regions:
        _log(
            f"[OLX raw] фільтр regions: невпізнано {unknown_regions} (доступні приклади: "
            f"{available_region_names[:5]}…)"
        )
    if listing_types_filter and unknown_types:
        try:
            base_labels = [c.get("label") for c in _get_base_categories()]
        except Exception:
            base_labels = []
        _log(
            f"[OLX raw] фільтр listing_types: невпізнано {unknown_types} (доступні: {base_labels})"
        )

    if regions_filter and not normalized_regions:
        _log(
            "[OLX raw] фільтр regions після нормалізації порожній — повертаємо 0 категорій, "
            "щоб не завантажити випадково всі регіони."
        )
        return []
    if listing_types_filter and not normalized_types:
        _log(
            "[OLX raw] фільтр listing_types після нормалізації порожній — повертаємо 0 категорій, "
            "щоб не завантажити випадково всі типи."
        )
        return []

    result = []
    for region_name, cats in regions_with_cats:
        if normalized_regions and region_name not in normalized_regions:
            continue
        if normalized_types:
            filtered_cats = [
                c for c in cats
                if any(p.lower() in (c.get("label") or "").lower() for p in normalized_types)
            ]
            if not filtered_cats:
                continue
            cats = filtered_cats
        result.append((region_name, cats))
    return result


def _phase1_worker(
    job_queue: "queue.Queue[Tuple[str, Dict[str, Any]]]",
    raw_repo: RawOlxListingsRepository,
    log_fn: Callable[[str], None],
    cutoff_utc: Optional[datetime],
    max_pages_override: Optional[int],
    results_lock: threading.Lock,
    total_listings_ref: List[int],
    all_loaded_urls_ref: List[str],
    source_pending_ref: List[int],
    source_inflight_ref: List[int],
    source_state_lock: threading.Lock,
    llm_process_url_fn: Optional[Callable[[str], bool]],
    llm_enqueue_region_filter_fn: Optional[Callable[[str], bool]],
    llm_queue: Optional["queue.Queue[str]"],
    llm_seen_urls: Set[str],
    llm_seen_lock: threading.Lock,
    llm_processed_urls_ref: List[str],
    browser_fetcher: Optional[Any] = None,
    source_load_run_id: Optional[str] = None,
    unit_state: Optional[Dict[str, Dict[str, Any]]] = None,
) -> None:
    """
    Воркер Phase 1: бере завдання з черги (region_name, category_dict), виконує streaming
    _process_category_raw_only; detail — через спільний BrowserPagePool.
    """
    from data.repositories.source_load_run_repository import (
        SourceLoadRunRepository,
        make_olx_unit_key,
        UNIT_DONE,
    )

    session = get_session()
    run_repo = SourceLoadRunRepository() if source_load_run_id else None

    def run_job(region_name: str, cat: Dict[str, Any]) -> Tuple[int, List[str]]:
        get_list_url = cat.get("get_list_url")
        if not callable(get_list_url):
            return 0, []
        label = cat.get("label", "?")
        unit_key = make_olx_unit_key(region_name, label)
        start_page = 1
        if unit_state is not None:
            st = unit_state.get(unit_key) or {}
            if st.get("status") == UNIT_DONE:
                log_fn(f"[OLX raw] skip done work unit: {unit_key}")
                return 0, []
            last_page = st.get("last_page")
            if isinstance(last_page, int) and last_page >= 1:
                start_page = last_page + 1
                log_fn(f"[OLX raw] resume {unit_key} з сторінки {start_page}")
        if run_repo and source_load_run_id:
            run_repo.mark_unit_running(source_load_run_id, unit_key)

        max_pages = (
            max_pages_override
            if max_pages_override is not None
            else min(
                int(cat.get("max_pages", scraper_config.MAX_SEARCH_PAGES)),
                scraper_config.MAX_SEARCH_PAGES,
            )
        )

        def on_page_done(page_num: int) -> None:
            if run_repo and source_load_run_id:
                run_repo.set_unit_last_page(source_load_run_id, unit_key, page_num)
            if unit_state is not None:
                unit_state[unit_key] = {
                    **(unit_state.get(unit_key) or {}),
                    "last_page": page_num,
                    "status": "running",
                }

        n_list, urls = _process_category_raw_only(
            get_list_url,
            max_pages,
            label,
            raw_repo,
            log_fn=log_fn,
            cutoff_utc=cutoff_utc,
            session=session,
            region_name=region_name,
            browser_fetcher=browser_fetcher,
            start_page=start_page,
            source_load_run_id=source_load_run_id,
            on_page_done=on_page_done,
        )
        if run_repo and source_load_run_id:
            run_repo.mark_unit_done(source_load_run_id, unit_key)
        if unit_state is not None:
            unit_state[unit_key] = {**(unit_state.get(unit_key) or {}), "status": UNIT_DONE}
        return n_list, urls

    def _drain_llm_once() -> bool:
        if not llm_process_url_fn or not llm_queue:
            return False
        try:
            listing_url = llm_queue.get(timeout=0.2)
        except queue.Empty:
            return False
        if llm_process_url_fn(listing_url):
            with results_lock:
                llm_processed_urls_ref.append(listing_url)
        return True

    def _can_stop() -> bool:
        with source_state_lock:
            no_more_source = source_pending_ref[0] == 0 and source_inflight_ref[0] == 0
        if not no_more_source:
            return False
        if not llm_process_url_fn or not llm_queue:
            return True
        return llm_queue.empty()

    def _run_loop() -> None:
        while True:
            try:
                region_name, cat = job_queue.get_nowait()
                with source_state_lock:
                    source_pending_ref[0] = max(0, source_pending_ref[0] - 1)
                    source_inflight_ref[0] += 1
            except queue.Empty:
                if _drain_llm_once():
                    continue
                if _can_stop():
                    break
                continue

            try:
                n_list, urls = run_job(region_name, cat)
                with results_lock:
                    total_listings_ref[0] += n_list
                    # Не тримаємо гігантський список у RAM: для Phase 2 — Mongo по run_id.
                    # all_loaded_urls_ref лише для зворотної сумісності (короткий шлях без run_id).
                    if not source_load_run_id:
                        all_loaded_urls_ref.extend(urls)
                if llm_process_url_fn and llm_queue:
                    allow_region = True
                    if llm_enqueue_region_filter_fn:
                        allow_region = llm_enqueue_region_filter_fn(region_name)
                    if allow_region:
                        with llm_seen_lock:
                            for u in urls:
                                if u and u not in llm_seen_urls:
                                    llm_seen_urls.add(u)
                                    llm_queue.put(u)
            finally:
                with source_state_lock:
                    source_inflight_ref[0] = max(0, source_inflight_ref[0] - 1)

    if browser_fetcher is not None:
        _run_loop()
        return

    from scripts.olx_scraper.browser_fetcher import BrowserPageFetcher

    with BrowserPageFetcher(headless=True, log_fn=log_fn) as bf:
        browser_fetcher = bf
        _run_loop()


def run_olx_update_raw_only(
    settings: Optional[Settings] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    days: Optional[int] = None,
    regions: Optional[List[str]] = None,
    listing_types: Optional[List[str]] = None,
    max_workers: Optional[int] = None,
    llm_process_url_fn: Optional[Callable[[str], bool]] = None,
    llm_enqueue_region_filter_fn: Optional[Callable[[str], bool]] = None,
    source_load_run_id: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Phase 1 pipeline: завантаження сирих даних OLX лише в raw_olx_listings (без LLM, без olx_listings).

    Пул завдань: (область, категорія); detail — спільний BrowserPagePool (low-RAM).
    source_load_run_id: durable resume (skip done units, last_page).
    """
    from data.repositories.source_load_run_repository import (
        SourceLoadRunRepository,
        UNIT_DONE,
        UNIT_PENDING,
        make_olx_unit_key,
    )

    if llm_process_url_fn is not None:
        raise ValueError(
            "Inline LLM processing during OLX Phase 1 is disabled. "
            "Use Phase 2 batch processing only."
        )
    if llm_enqueue_region_filter_fn is not None:
        raise ValueError(
            "Inline LLM queue filters are disabled. "
            "Use Phase 2 batch processing only."
        )

    settings = settings or Settings()
    MongoDBConnection.initialize(settings)
    raw_repo = RawOlxListingsRepository()
    raw_repo.ensure_index()

    cutoff_utc: Optional[datetime] = None
    if days is not None and days >= 1:
        now_utc = datetime.now(timezone.utc)
        start_of_today_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_utc = start_of_today_utc - timedelta(days=days)
    max_pages_override = scraper_config.MAX_SEARCH_PAGES

    log_lock = threading.Lock()

    def log(msg: str) -> None:
        with log_lock:
            if log_fn:
                log_fn(msg)
            else:
                print(msg, flush=True)

    regions_with_cats = _build_regions_with_categories()
    regions_with_cats = _filter_regions_with_categories(
        regions_with_cats,
        regions_filter=regions,
        listing_types_filter=listing_types,
        log_fn=log,
    )
    if not regions_with_cats:
        log(
            "[OLX raw] Phase 1: після фільтрації областей/типів немає категорій для обробки. "
            f"regions={regions!r} listing_types={listing_types!r}"
        )
        return {
            "success": True,
            "total_listings": 0,
            "loaded_urls": [],
            "llm_processed_urls": [],
            "skipped_reason": "empty_filter",
            "regions_filter": list(regions or []),
            "listing_types_filter": list(listing_types or []),
        }

    num_workers = max_workers if max_workers is not None else getattr(scraper_config, "OLX_PHASE1_MAX_THREADS", 5)
    use_pool = num_workers > 0

    unit_state: Dict[str, Dict[str, Any]] = {}
    run_repo: Optional[SourceLoadRunRepository] = None
    if source_load_run_id:
        run_repo = SourceLoadRunRepository()
        existing = run_repo.get_run(source_load_run_id)
        existing_units = {u.get("key"): u for u in (existing or {}).get("work_units") or [] if u.get("key")}
        built_units: List[Dict[str, Any]] = []
        for region_name, cats in regions_with_cats:
            for cat in cats:
                if not callable(cat.get("get_list_url")):
                    continue
                key = make_olx_unit_key(region_name, cat.get("label", "?"))
                prev = existing_units.get(key) or {}
                unit = {
                    "key": key,
                    "source": "olx",
                    "region": region_name,
                    "category": cat.get("label", "?"),
                    "status": prev.get("status") or UNIT_PENDING,
                    "last_page": prev.get("last_page"),
                }
                built_units.append(unit)
                unit_state[key] = unit
        merged = [u for u in (existing or {}).get("work_units") or [] if u.get("source") == "prozorro"]
        merged.extend(built_units)
        run_repo.set_work_units(source_load_run_id, merged)

    if use_pool:
        job_list: List[Tuple[str, Dict[str, Any]]] = []
        for region_name, cats in regions_with_cats:
            for cat in cats:
                if not callable(cat.get("get_list_url")):
                    continue
                key = make_olx_unit_key(region_name, cat.get("label", "?"))
                if unit_state.get(key, {}).get("status") == UNIT_DONE:
                    continue
                job_list.append((region_name, cat))
        if not job_list:
            loaded = (
                raw_repo.list_urls_by_source_load_run_id(source_load_run_id)
                if source_load_run_id
                else []
            )
            return {"success": True, "total_listings": 0, "loaded_urls": loaded, "llm_processed_urls": []}
        job_queue: "queue.Queue[Tuple[str, Dict[str, Any]]]" = queue.Queue()
        for j in job_list:
            job_queue.put(j)

        total_listings_ref: List[int] = [0]
        all_loaded_urls: List[str] = []
        llm_processed_urls: List[str] = []
        results_lock = threading.Lock()
        source_state_lock = threading.Lock()
        source_pending_ref: List[int] = [len(job_list)]
        source_inflight_ref: List[int] = [0]
        llm_queue: Optional["queue.Queue[str]"] = queue.Queue() if llm_process_url_fn else None
        llm_seen_urls: Set[str] = set()
        llm_seen_lock = threading.Lock()
        browser_pool_size = scraper_config.resolve_browser_pool_size(num_workers)
        log(
            "[OLX raw] Phase 1: пул завдань, %s потоків, pool_size=%s, %s завдань"
            % (num_workers, browser_pool_size, len(job_list))
        )
        worker_kwargs = {
            "source_load_run_id": source_load_run_id,
            "unit_state": unit_state if source_load_run_id else None,
        }
        try:
            from scripts.olx_scraper.browser_fetcher import BrowserPagePool
            with BrowserPagePool(headless=True, pool_size=browser_pool_size, log_fn=log) as browser_fetcher:
                if num_workers == 1:
                    _phase1_worker(
                        job_queue,
                        raw_repo,
                        log,
                        cutoff_utc,
                        max_pages_override,
                        results_lock,
                        total_listings_ref,
                        all_loaded_urls,
                        source_pending_ref,
                        source_inflight_ref,
                        source_state_lock,
                        llm_process_url_fn,
                        llm_enqueue_region_filter_fn,
                        llm_queue,
                        llm_seen_urls,
                        llm_seen_lock,
                        llm_processed_urls,
                        browser_fetcher,
                        **worker_kwargs,
                    )
                else:
                    workers = [
                        threading.Thread(
                            target=_phase1_worker,
                            args=(
                                job_queue,
                                raw_repo,
                                log,
                                cutoff_utc,
                                max_pages_override,
                                results_lock,
                                total_listings_ref,
                                all_loaded_urls,
                                source_pending_ref,
                                source_inflight_ref,
                                source_state_lock,
                                llm_process_url_fn,
                                llm_enqueue_region_filter_fn,
                                llm_queue,
                                llm_seen_urls,
                                llm_seen_lock,
                                llm_processed_urls,
                                browser_fetcher,
                            ),
                            kwargs=worker_kwargs,
                            name="OLXPhase1-%d" % (i + 1),
                        )
                        for i in range(num_workers)
                    ]
                    for t in workers:
                        t.start()
                    for t in workers:
                        t.join()
        except Exception as e:
            log(f"[OLX raw] Помилка BrowserPool: {e}")
            return {"success": False, "total_listings": 0, "loaded_urls": [], "llm_processed_urls": []}
        total_listings = total_listings_ref[0]
    else:
        # Legacy max_workers=0: sequential, але через BrowserPagePool (не N Chromium / не per-region browser).
        all_loaded_urls = []
        total_listings = 0
        llm_processed_urls = []
        log("[OLX raw] Phase 1: legacy sequential через BrowserPagePool (deprecated path; prefer max_workers>0).")
        from scripts.olx_scraper.browser_fetcher import BrowserPagePool
        pool_size = scraper_config.resolve_browser_pool_size(1)
        try:
            with BrowserPagePool(headless=True, pool_size=pool_size, log_fn=log) as browser_fetcher:
                for region_name, cats in regions_with_cats:
                    for cat in cats:
                        if not callable(cat.get("get_list_url")):
                            continue
                        key = make_olx_unit_key(region_name, cat.get("label", "?"))
                        if unit_state.get(key, {}).get("status") == UNIT_DONE:
                            continue
                        start_page = 1
                        st = unit_state.get(key) or {}
                        if isinstance(st.get("last_page"), int) and st["last_page"] >= 1:
                            start_page = st["last_page"] + 1
                        if run_repo and source_load_run_id:
                            run_repo.mark_unit_running(source_load_run_id, key)

                        def on_page_done(page_num: int, _key=key) -> None:
                            if run_repo and source_load_run_id:
                                run_repo.set_unit_last_page(source_load_run_id, _key, page_num)

                        max_pages = (
                            max_pages_override
                            if max_pages_override is not None
                            else min(
                                int(cat.get("max_pages", scraper_config.MAX_SEARCH_PAGES)),
                                scraper_config.MAX_SEARCH_PAGES,
                            )
                        )
                        try:
                            n_list, urls = _process_category_raw_only(
                                cat["get_list_url"],
                                max_pages,
                                cat.get("label", "?"),
                                raw_repo,
                                log_fn=log,
                                cutoff_utc=cutoff_utc,
                                session=get_session(),
                                region_name=region_name,
                                browser_fetcher=browser_fetcher,
                                start_page=start_page,
                                source_load_run_id=source_load_run_id,
                                on_page_done=on_page_done,
                            )
                            total_listings += n_list
                            if not source_load_run_id:
                                all_loaded_urls.extend(urls)
                            if run_repo and source_load_run_id:
                                run_repo.mark_unit_done(source_load_run_id, key)
                        except Exception as e:
                            log(f"[OLX raw] Помилка {region_name}/{cat.get('label')}: {e}")
        except Exception as e:
            log(f"[OLX raw] Помилка браузера: {e}")

    if source_load_run_id:
        loaded_urls = raw_repo.list_urls_by_source_load_run_id(source_load_run_id)
    else:
        loaded_urls = list(dict.fromkeys(all_loaded_urls))
    log(f"[OLX raw] Phase 1 готово. Оголошень: {total_listings}, завантажено/оновлено URL: {len(loaded_urls)}")
    return {
        "success": True,
        "total_listings": total_listings,
        "loaded_urls": loaded_urls,
        "llm_processed_urls": list(dict.fromkeys(llm_processed_urls)),
    }


def _process_region(
    region_name: str,
    categories: List[Dict[str, Any]],
    repo: OlxListingsRepository,
    llm_extractor: OlxLLMExtractorService,
    geocoding_service: GeocodingService,
    unified_service: Optional[UnifiedListingsService],
    log_fn: Optional[Callable[[str], None]],
    cutoff_utc: Optional[datetime],
    usd_rate: Optional[float],
    max_pages_override: Optional[int] = None,
) -> Tuple[int, int, List[Dict[str, Any]], set, List[str]]:
    """
    Обробляє одну область: 2 категорії (нежитлова + земля) з одним HTTP-сеансом.
    Деталі оголошень завантажуються лише через браузерний fetcher.
    Повертає (total_listings, total_detail_fetches, by_category, search_urls, pending_llm_urls).
    """
    session = get_session()
    total_listings = 0
    total_detail_fetches = 0
    by_category: List[Dict[str, Any]] = []
    all_search_urls: set = set()
    pending_llm_urls: List[str] = []

    from scripts.olx_scraper.browser_fetcher import BrowserPageFetcher

    with BrowserPageFetcher(headless=True, log_fn=log_fn) as browser_fetcher:
        for cat in categories:
            label = cat.get("label", "?")
            get_list_url = cat.get("get_list_url")
            if not callable(get_list_url):
                continue
            max_pages = (
                max_pages_override
                if max_pages_override is not None
                else min(
                    int(cat.get("max_pages", scraper_config.MAX_SEARCH_PAGES)),
                    scraper_config.MAX_SEARCH_PAGES,
                )
            )
            n_listings, n_details, cat_urls, cat_pending = _process_category(
                get_list_url,
                max_pages,
                label,
                repo,
                llm_extractor,
                geocoding_service,
                unified_service=unified_service,
                log_fn=log_fn,
                cutoff_utc=cutoff_utc,
                usd_rate=usd_rate,
                session=session,
                region_name=region_name,
                browser_fetcher=browser_fetcher,
            )
            total_listings += n_listings
            total_detail_fetches += n_details
            all_search_urls.update(cat_urls or set())
            pending_llm_urls.extend(cat_pending or [])
            by_category.append({"label": label, "listings": n_listings, "detail_fetches": n_details})

    return total_listings, total_detail_fetches, by_category, all_search_urls, pending_llm_urls


def _process_single_llm_pending_url(
    listing_url: str,
    raw_repo: Union[RawOlxListingsRepository, OlxListingsRepository],
    main_repo: OlxListingsRepository,
    llm_extractor: OlxLLMExtractorService,
    geocoding_service: GeocodingService,
    unified_service: Optional[UnifiedListingsService],
    usd_rate: Optional[float],
    log_fn: Optional[Callable[[str], None]],
    geography_service: Optional[Any] = None,
) -> bool:
    """
    Обробляє один URL у Phase 2 (LLM + гео + upsert у main + sync unified).
    Повертає True, якщо обробка успішна.
    """
    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            print(msg, flush=True)

    t_item_start = time.perf_counter()
    try:
        doc = raw_repo.find_by_url(listing_url)
        if not doc:
            return False
        search_data = doc.get("search_data") or {}
        detail_data = (doc.get("detail") or {}).copy()
        if detail_data.get("_inactive"):
            detail_data.pop("_inactive", None)

        t0 = time.perf_counter()
        llm_data = llm_extractor.extract_structured_data(search_data, detail_data)
        t_llm = time.perf_counter() - t0
        if llm_data:
            detail_data["llm"] = llm_data
        if llm_extractor.has_cadastral_in_price_text(search_data):
            recovered_price = llm_extractor.recover_price_ignoring_cadastral(search_data, detail_data)
            llm_meta = detail_data.get("llm")
            if not isinstance(llm_meta, dict):
                llm_meta = {}
                detail_data["llm"] = llm_meta
            recovery_meta = {
                "status": "failed",
                "reason": "price_text_contains_cadastral_number",
            }
            recovered_value = recovered_price.get("price_value")
            if recovered_value is not None:
                old_price = search_data.get("price_value")
                search_data["price_value"] = recovered_value
                recovered_currency = (recovered_price.get("currency") or "").strip().upper()
                if recovered_currency in ("UAH", "USD", "EUR"):
                    search_data["currency"] = recovered_currency
                recovered_price_text = (recovered_price.get("price_text") or "").strip()
                if recovered_price_text:
                    search_data["price_text"] = recovered_price_text
                recovery_meta["status"] = "recovered"
                recovery_meta["old_price_value"] = old_price
                recovery_meta["new_price_value"] = recovered_value
                logger.info(
                    "[OLX] Виправлено ціну після кадастрового конфлікту: old=%s new=%s url=%s",
                    old_price,
                    recovered_value,
                    listing_url[:60] + "...",
                )
            else:
                logger.warning(
                    "[OLX] Не вдалося відновити ціну після кадастрового конфлікту: url=%s",
                    listing_url[:60] + "...",
                )
            llm_meta["price_recovery"] = recovery_meta
        new_hash = detail_data.get("llm_content_hash")
        if new_hash:
            detail_data["llm_content_hash"] = new_hash

        t0 = time.perf_counter()
        result = _collect_and_geocode_locations(
            search_data, detail_data, geocoding_service, geography_service
        )
        t_geocode = time.perf_counter() - t0
        if len(result) == 3:
            geocode_hashes, resolved_locations, address_refs_list = result
        else:
            geocode_hashes, resolved_locations = result
            address_refs_list = []

        detail_data["geocode_query_hashes"] = geocode_hashes
        detail_data["resolved_locations"] = resolved_locations
        if address_refs_list:
            detail_data["address_refs"] = address_refs_list

        price_value = search_data.get("price_value")
        llm_struct = (detail_data.get("llm") or {}) if isinstance(detail_data, dict) else {}
        total_area_m2 = llm_struct.get("total_area_m2")
        if total_area_m2 is None:
            total_area_m2 = search_data.get("area_m2")
        land_area_sqm = llm_struct.get("land_area_sqm")
        if land_area_sqm is None and llm_struct.get("land_area_ha") is not None:
            try:
                land_area_sqm = float(llm_struct["land_area_ha"]) * 10000.0
            except (TypeError, ValueError):
                land_area_sqm = None
        metrics = compute_price_metrics(
            total_price_uah=price_value,
            building_area_sqm=total_area_m2,
            land_area_sqm=land_area_sqm,
            uah_per_usd=usd_rate,
        )
        detail_data["price_metrics"] = metrics

        if price_value is not None:
            currency = (search_data.get("currency") or "UAH").strip().upper() or "UAH"
            if currency not in ("UAH", "USD", "EUR"):
                currency = "UAH"
            detail_data["price"] = {"value": float(price_value) if not isinstance(price_value, float) else price_value, "currency": currency}

        formatted_address = ""
        if address_refs_list and geography_service:
            parts = []
            for refs in address_refs_list:
                if isinstance(refs, dict):
                    addr = geography_service.format_address(refs)
                    if addr and addr not in parts:
                        parts.append(addr)
            formatted_address = "; ".join(parts) if parts else ""
        if not formatted_address:
            for addr in (detail_data.get("llm") or {}).get("addresses") or []:
                if isinstance(addr, dict):
                    line = _address_line_from_llm_address(addr)
                    if line:
                        formatted_address = line
                        break
        if not formatted_address:
            formatted_address = (search_data.get("location") or "").strip()
        if formatted_address:
            if "llm" not in detail_data:
                detail_data["llm"] = {}
            detail_data["llm"]["parsed_address"] = {"formatted_address": formatted_address}

        t0 = time.perf_counter()
        main_repo.upsert_listing(listing_url, search_data, detail=detail_data, is_active=True)
        t_upsert = time.perf_counter() - t0

        t_sync = 0.0
        if unified_service:
            try:
                t0 = time.perf_counter()
                unified_service.sync_olx_listing(listing_url)
                t_sync = time.perf_counter() - t0
            except Exception as sync_err:
                log(f"[OLX] Помилка синхронізації в unified: {sync_err}")

        t_total = time.perf_counter() - t_item_start
        if t_total >= 10.0:
            n_geocode = len(geocode_hashes) if geocode_hashes else 0
            logger.info(
                "[OLX] item timing: total=%.1fs llm=%.1fs geocode=%.1fs(%d) upsert=%.1fs sync=%.1fs url=%s",
                t_total, t_llm, t_geocode, n_geocode, t_upsert, t_sync, listing_url[:60] + "..."
            )
        return True
    except Exception as e:
        log(f"[OLX] Помилка LLM для {listing_url[:50]}...: {e}")
        return False


def _process_llm_pending(
    pending_urls: List[str],
    raw_repo: Union[RawOlxListingsRepository, OlxListingsRepository],
    main_repo: OlxListingsRepository,
    llm_extractor: OlxLLMExtractorService,
    geocoding_service: GeocodingService,
    unified_service: Optional[UnifiedListingsService],
    usd_rate: Optional[float],
    log_fn: Optional[Callable[[str], None]],
    skip_activity_check: bool = False,
) -> int:
    """
    Phase 2: обробка оголошень через LLM; дані беруться з raw/main, після LLM — запис у olx_listings та sync у unified.
    Актуальність оголошення не перевіряється — усі зберігаються як активні (is_active=True).
    skip_activity_check збережено для сумісності викликів, не використовується.
    Повертає кількість успішно оброблених.
    """
    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            print(msg, flush=True)

    if not pending_urls:
        return 0

    geography_service = None
    try:
        from business.services.geography_service import GeographyService
        geography_service = GeographyService()
    except ImportError:
        pass

    processed = 0
    iterator = pending_urls
    if tqdm:
        iterator = tqdm(
            pending_urls,
            desc="[OLX] LLM-обробка",
            unit="оголош.",
            ncols=100,
            disable=False,
        )

    for listing_url in iterator:
        if _process_single_llm_pending_url(
            listing_url=listing_url,
            raw_repo=raw_repo,
            main_repo=main_repo,
            llm_extractor=llm_extractor,
            geocoding_service=geocoding_service,
            unified_service=unified_service,
            usd_rate=usd_rate,
            log_fn=log_fn,
            geography_service=geography_service,
        ):
            processed += 1

    return processed


# Базові категорії: нежитлова нерухомість + земля по типах (без с/г).
# Земля: окрема категорія на тип з olx_land_type_slugs — фільтрація на рівні URL OLX.
def _get_base_categories() -> List[Dict[str, Any]]:
    """Повертає базові категорії: комерційна + земля по типах (без с/г)."""
    cats: List[Dict[str, Any]] = [
        {"label": "Нежитлова нерухомість", "get_list_url": scraper_config.get_commercial_real_estate_list_url},
    ]
    land_slugs = scraper_config.get_olx_land_type_slugs()
    for label, slug in land_slugs.items():
        get_fn = lambda p, fn=scraper_config.get_land_list_url, lt=slug, **kw: fn(p, land_type_slug=lt, **kw)
        cats.append({"label": f"Земля — {label}", "get_list_url": get_fn})
    if not land_slugs:
        cats.append({"label": "Земельні ділянки", "get_list_url": scraper_config.get_land_list_url})
    return cats




def _build_region_categories() -> List[Dict[str, Any]]:
    """
    Будує список категорій для пошуку по областях.
    Кожна пара (категорія × область) — окремий пошук, max 25 сторінок, зупинка по cutoff_utc.
    """
    slugs = scraper_config.get_olx_region_slugs()
    if not slugs:
        # Fallback: без фільтра по областях (якщо конфіг відсутній)
        return [
            {
                "label": "Нежитлова нерухомість",
                "get_list_url": scraper_config.get_commercial_real_estate_list_url,
                "max_pages": scraper_config.MAX_SEARCH_PAGES,
            },
            {
                "label": "Земельні ділянки",
                "get_list_url": scraper_config.get_land_list_url,
                "max_pages": scraper_config.MAX_SEARCH_PAGES,
            },
        ]
    categories = []
    for base in _get_base_categories():
        for region_name, region_slug in slugs.items():
            get_fn = base["get_list_url"]
            # Замикання: rs=region_slug у default-аргументі, щоб не захопити змінну циклу
            get_list_url = lambda p, fn=get_fn, rs=region_slug: fn(p, region_slug=rs)
            categories.append({
                "label": f"{base['label']} — {region_name}",
                "get_list_url": get_list_url,
                "max_pages": scraper_config.MAX_SEARCH_PAGES,
            })
    return categories


def _build_regions_with_categories() -> List[Tuple[str, List[Dict[str, Any]]]]:
    """
    Групує категорії по областях. Кожна область має 2 категорії: нежитлова + земля.
    Повертає [(region_name, [cat1, cat2]), ...] для паралельної обробки.
    """
    slugs = scraper_config.get_olx_region_slugs()
    if not slugs:
        # Fallback: один «регіон» з двома категоріями без фільтра
        return [
            ("Україна", [
                {"label": "Нежитлова нерухомість", "get_list_url": scraper_config.get_commercial_real_estate_list_url, "max_pages": scraper_config.MAX_SEARCH_PAGES},
                {"label": "Земельні ділянки", "get_list_url": scraper_config.get_land_list_url, "max_pages": scraper_config.MAX_SEARCH_PAGES},
            ]),
        ]
    result = []
    for region_name, region_slug in slugs.items():
        cats = []
        for base in _get_base_categories():
            get_fn = base["get_list_url"]
            get_list_url = lambda p, fn=get_fn, rs=region_slug: fn(p, region_slug=rs)
            cats.append({
                "label": f"{base['label']} — {region_name}",
                "get_list_url": get_list_url,
                "max_pages": scraper_config.MAX_SEARCH_PAGES,
            })
        result.append((region_name, cats))
    return result


def run_olx_update(
    settings: Optional[Settings] = None,
    categories: Optional[List[Dict[str, Any]]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    days: Optional[int] = None,
    full: bool = False,
) -> Dict[str, Any]:
    """
    Запускає оновлення оголошень OLX по заданих категоріях.
    Викликається з main.py та Telegram після/разом із оновленням ProZorro.

    Args:
        settings: налаштування (якщо None — створюються нові).
        categories: список словників { "label", "get_list_url", "max_pages" };
                    якщо None — паралельна обробка по областях (по одному потоку на область).
        log_fn: опціональна функція для логів (наприклад, у Telegram).
        days: якщо 1, 7 або 30 — поріг за календарними днями: зупиняємося, коли зустрічаємо
              оголошення з попередньої доби. Якщо None — без обмеження.
        full: якщо True — завантажує всі оголошення зі сторінок пошуку без обмеження сторінок.

    Returns:
        Словник: success, total_listings, total_detail_fetches, by_category.

    Deprecated: для production використовуйте run_olx_update_raw_only + BrowserPagePool
    (low-RAM). Цей шлях може відкривати окремий Chromium на регіон при паралелі.
    """
    import warnings
    warnings.warn(
        "run_olx_update is deprecated; use run_olx_update_raw_only + BrowserPagePool",
        DeprecationWarning,
        stacklevel=2,
    )
    settings = settings or Settings()
    MongoDBConnection.initialize(settings)
    repo = OlxListingsRepository()
    repo.ensure_index()
    llm_extractor = OlxLLMExtractorService(settings)
    geocoding_service = GeocodingService(settings)
    unified_service = UnifiedListingsService(settings)

    # Курс продажу USD з Ощадбанку (може бути None, тоді просто не рахуємо USD-метрики)
    try:
        currency_service = CurrencyRateService(settings)
        usd_rate = currency_service.get_today_usd_rate(allow_fetch=True)
    except Exception:
        usd_rate = None

    # Поріг за календарем: зупинка при оголошенні з «попередньої доби» (строго раніше вікна)
    # За добу (days=1): вікно 04.02–05.02, зупинка при зустрічі 03.02 → cutoff = початок 04.02.
    # За тиждень (days=7): вікно 30.01–05.02, зупинка при 29.01 → cutoff = початок 30.01.
    cutoff_utc: Optional[datetime] = None
    if days is not None and days >= 1 and not full:
        now_utc = datetime.now(timezone.utc)
        start_of_today_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_utc = start_of_today_utc - timedelta(days=days)

    max_pages_override = scraper_config.MAX_SEARCH_PAGES if (full or cutoff_utc is not None) else None

    log_lock = threading.Lock()

    def log(msg: str) -> None:
        with log_lock:
            if log_fn:
                log_fn(msg)
            else:
                print(msg, flush=True)

    regions_with_cats = _build_regions_with_categories()
    if categories is not None:
        # Якщо передано явний список категорій — обробляємо послідовно (legacy, без паралелізму)
        total_listings = 0
        total_detail_fetches = 0
        by_category = []
        all_search_urls: set = set()
        pending_llm_urls: List[str] = []
        from scripts.olx_scraper.browser_fetcher import BrowserPageFetcher
        with BrowserPageFetcher(headless=True, log_fn=log) as browser_fetcher:
            for cat in categories:
                label = cat.get("label", "?")
                get_list_url = cat.get("get_list_url")
                max_pages = max_pages_override or min(
                    int(cat.get("max_pages", scraper_config.MAX_SEARCH_PAGES)),
                    scraper_config.MAX_SEARCH_PAGES,
                )
                if not callable(get_list_url):
                    log(f"[OLX] Пропуск категорії {label}: немає get_list_url")
                    continue
                n_listings, n_details, cat_urls, cat_pending = _process_category(
                    get_list_url, max_pages, label, repo, llm_extractor, geocoding_service,
                    unified_service=unified_service, log_fn=log, cutoff_utc=cutoff_utc,
                    usd_rate=usd_rate, session=None, region_name=None, browser_fetcher=browser_fetcher,
                )
                total_listings += n_listings
                total_detail_fetches += n_details
                all_search_urls.update(cat_urls or set())
                pending_llm_urls.extend(cat_pending or [])
                by_category.append({"label": label, "listings": n_listings, "detail_fetches": n_details})

        # Phase 2 для sequential режиму
        pending_llm_urls = list(dict.fromkeys(pending_llm_urls))
        if pending_llm_urls:
            log(f"[OLX] Phase 2: LLM-обробка {len(pending_llm_urls)} оголошень")
        llm_processed = _process_llm_pending(
            pending_llm_urls,
            repo,
            repo,
            llm_extractor,
            geocoding_service,
            unified_service,
            usd_rate,
            log,
        ) if pending_llm_urls else 0
    else:
        # Phase 1: паралельна обробка по областях — завантаження сирих даних
        total_listings = 0
        total_detail_fetches = 0
        by_category = []
        all_search_urls = set()
        pending_llm_urls: List[str] = []
        max_workers = min(
            len(regions_with_cats),
            getattr(scraper_config, "MAX_PARALLEL_REGIONS", 25),
        )
        log(f"[OLX] Phase 1: паралельна обробка {len(regions_with_cats)} областей, {max_workers} потоків")

        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = {
                executor.submit(
                    _process_region,
                    region_name,
                    cats,
                    repo,
                    llm_extractor,
                    geocoding_service,
                    unified_service,
                    log,
                    cutoff_utc,
                    usd_rate,
                    max_pages_override,
                ): region_name
                for region_name, cats in regions_with_cats
            }
            for future in as_completed(futures):
                region_name = futures[future]
                try:
                    n_list, n_detail, by_cat, cat_urls, cat_pending = future.result()
                    total_listings += n_list
                    total_detail_fetches += n_detail
                    by_category.extend(by_cat)
                    all_search_urls.update(cat_urls or set())
                    pending_llm_urls.extend(cat_pending or [])
                except Exception as e:
                    log(f"[OLX] Помилка області {region_name}: {e}")

        # Phase 2: LLM-обробка одним потоком з прогрес-баром
        pending_llm_urls = list(dict.fromkeys(pending_llm_urls))
        llm_processed = 0
        if pending_llm_urls:
            log(f"[OLX] Phase 2: LLM-обробка {len(pending_llm_urls)} оголошень (один потік)")
            llm_processed = _process_llm_pending(
                pending_llm_urls,
                repo,
                repo,
                llm_extractor,
                geocoding_service,
                unified_service,
                usd_rate,
                log,
            )

    log(f"[OLX] Готово. Оголошень: {total_listings}, деталей: {total_detail_fetches}, LLM: {llm_processed}")
    return {
        "success": True,
        "total_listings": total_listings,
        "total_detail_fetches": total_detail_fetches,
        "llm_processed": llm_processed,
        "by_category": by_category,
    }


def main() -> None:
    run_olx_update()


if __name__ == "__main__":
    main()
