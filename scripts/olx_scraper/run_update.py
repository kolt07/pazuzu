# -*- coding: utf-8 -*-
"""
Оновлення оголошень OLX: нежитлова нерухомість + земельні ділянки.
Використовується в процедурах оновлення даних (main, Telegram) разом із ProZorro.

Запуск з кореня проекту:
  py scripts/olx_scraper/run_update.py
"""

import sys
import threading
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")
elif getattr(sys.stdout, "encoding", "").lower() != "utf-8":
    import io
    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding="utf-8", errors="replace")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.olx_listings_repository import OlxListingsRepository
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
) -> Tuple[int, int, set]:
    """
    Обробляє одну категорію OLX у два етапи:
    1) Прохід по сторінках пошуку — збір повного списку оголошень, визначення обсягу.
    2) Опрацювання списку — завантаження деталей, LLM, геокодування, збереження в БД.
    Зупинка по даті (cutoff_utc) або по max_pages лише на етапі 1.
    Повертає (total_listings, total_detail_fetches, search_urls).
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
            if attempt < retry_count:
                delay_sec = getattr(scraper_config, "DELAY_AFTER_PAGE_LOAD", 3)
                log(f"[OLX] 0 оголошень — повторна спроба через {delay_sec:.0f} с...")
                time.sleep(delay_sec)
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
        return 0, 0, search_urls

    # ——— Етап 2: опрацювання кожного оголошення зі списку ———
    total_detail_fetches = 0
    for idx, item in enumerate(all_listings, start=1):
        listing_url = item.get("url")
        if not listing_url:
            continue
        log(f"[OLX] {category_label}: опрацювання {idx}/{total_count}")
        search_data = search_data_from_listing(item)
        existing = repo.find_by_url(listing_url)

        need_detail = False
        if existing is None:
            need_detail = True
        elif not existing.get("detail"):
            need_detail = True
        elif search_data_changed(existing.get("search_data") or {}, search_data):
            need_detail = True

        if need_detail:
            time.sleep(scraper_config.get_delay_detail_seconds())
            try:
                detail_response = fetch_page(listing_url, delay_before=False, session=session)
                detail_data = parse_detail_page(detail_response.text)

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

                reused_llm = False
                if old_llm and old_hash and new_hash and old_hash == new_hash:
                    # Ключова інформація не змінилась — перевикористовуємо попередній результат LLM
                    detail_data["llm"] = old_llm
                    detail_data["llm_content_hash"] = old_hash
                    reused_llm = True
                else:
                    llm_data = llm_extractor.extract_structured_data(search_data, detail_data)
                    if llm_data:
                        detail_data["llm"] = llm_data
                    if new_hash:
                        detail_data["llm_content_hash"] = new_hash

                # Імпортуємо GeographyService якщо доступний
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

                # --- Цінові метрики (UAH / USD, за м² та за гектар) ---
                price_value = search_data.get("price_value")
                llm_struct = (detail_data.get("llm") or {}) if isinstance(detail_data, dict) else {}
                total_area_m2 = llm_struct.get("total_area_m2")
                if total_area_m2 is None:
                    total_area_m2 = search_data.get("area_m2")
                land_area_ha = llm_struct.get("land_area_ha")

                metrics = compute_price_metrics(
                    total_price_uah=price_value,
                    building_area_sqm=total_area_m2,
                    land_area_ha=land_area_ha,
                    uah_per_usd=usd_rate,
                )
                detail_data["price_metrics"] = metrics

                # detail.price (value, currency) для відображення та агента
                if price_value is not None:
                    currency = (search_data.get("currency") or "UAH").strip().upper() or "UAH"
                    if currency not in ("UAH", "USD", "EUR"):
                        currency = "UAH"
                    detail_data["price"] = {"value": float(price_value) if not isinstance(price_value, float) else price_value, "currency": currency}

                # detail.llm.parsed_address.formatted_address — повна адреса
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
            # sync_olx_listing і process_listing (ОНМ) не потрібні — контент той самий.
            repo.upsert_listing(listing_url, search_data, detail=existing.get("detail"), is_active=True)

    return total_count, total_detail_fetches, search_urls


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
) -> Tuple[int, int, List[Dict[str, Any]], set]:
    """
    Обробляє одну область: 2 категорії (нежитлова + земля) з одним HTTP-сеансом.
    Повертає (total_listings, total_detail_fetches, by_category, search_urls).
    """
    session = get_session()
    total_listings = 0
    total_detail_fetches = 0
    by_category: List[Dict[str, Any]] = []
    all_search_urls: set = set()

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
        n_listings, n_details, cat_urls = _process_category(
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
        )
        total_listings += n_listings
        total_detail_fetches += n_details
        all_search_urls.update(cat_urls or set())
        by_category.append({"label": label, "listings": n_listings, "detail_fetches": n_details})

    return total_listings, total_detail_fetches, by_category, all_search_urls


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
    """
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
            n_listings, n_details, cat_urls = _process_category(
                get_list_url, max_pages, label, repo, llm_extractor, geocoding_service,
                unified_service=unified_service, log_fn=log, cutoff_utc=cutoff_utc,
                usd_rate=usd_rate, session=None,
            )
            total_listings += n_listings
            total_detail_fetches += n_details
            all_search_urls.update(cat_urls or set())
            by_category.append({"label": label, "listings": n_listings, "detail_fetches": n_details})
    else:
        # Паралельна обробка по областях: один потік на область, session повторно в межах потоку
        total_listings = 0
        total_detail_fetches = 0
        by_category = []
        all_search_urls = set()
        max_workers = min(
            len(regions_with_cats),
            getattr(scraper_config, "MAX_PARALLEL_REGIONS", 25),
        )
        log(f"[OLX] Паралельна обробка: {len(regions_with_cats)} областей, {max_workers} потоків")

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
                    n_list, n_detail, by_cat, cat_urls = future.result()
                    total_listings += n_list
                    total_detail_fetches += n_detail
                    by_category.extend(by_cat)
                    all_search_urls.update(cat_urls or set())
                except Exception as e:
                    log(f"[OLX] Помилка області {region_name}: {e}")

    log(f"[OLX] Готово. Всього оголошень: {total_listings}, запитів деталей: {total_detail_fetches}")
    return {
        "success": True,
        "total_listings": total_listings,
        "total_detail_fetches": total_detail_fetches,
        "by_category": by_category,
    }


def main() -> None:
    run_olx_update()


if __name__ == "__main__":
    main()
