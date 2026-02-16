# -*- coding: utf-8 -*-
"""
Оновлення оголошень OLX: нежитлова нерухомість + земельні ділянки.
Використовується в процедурах оновлення даних (main, Telegram) разом із ProZorro.

Запуск з кореня проекту:
  py scripts/olx_scraper/run_update.py
"""

import sys
import time
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
from scripts.olx_scraper.fetcher import fetch_page
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
) -> Tuple[int, int]:
    """
    Обробляє одну категорію OLX у два етапи:
    1) Прохід по сторінках пошуку — збір повного списку оголошень, визначення обсягу.
    2) Опрацювання списку — завантаження деталей, LLM, геокодування, збереження в БД.
    Зупинка по даті (cutoff_utc) або по max_pages лише на етапі 1.
    Повертає (total_listings, total_detail_fetches).
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
        page_label = f"{page}" if max_pages is None else f"{page}/{max_pages}"
        log(f"[OLX] {category_label}: сторінка пошуку {page_label}")
        try:
            response = fetch_page(url, delay_before=True)
        except Exception as e:
            log(f"[OLX] Помилка запиту {url}: {e}")
            page += 1
            continue
        listings = parse_listings_page(response.text)
        log(f"[OLX] Оголошень на сторінці: {len(listings)}")
        if not listings:
            break

        for item in listings:
            if cutoff_utc is not None:
                listed_at = _parse_listed_at_iso(item.get("listed_at_iso"))
                if listed_at is not None and listed_at < cutoff_utc:
                    log(f"[OLX] {category_label}: досягнуто порогу дати (оголошення від {item.get('date_text', '')}), далі не збираємо")
                    stop_pages = True
                    continue  # Пропускаємо це оголошення, але продовжуємо збирати нові на сторінці (порядок у DOM може відрізнятися від сортування)
            listing_url = item.get("url")
            if listing_url:
                all_listings.append(item)

        # Дата останнього оголошення на сторінці (останнє в списку при сортуванні «від найновіших»)
        last_on_page = listings[-1] if listings else None
        if last_on_page:
            last_date = last_on_page.get("date_text") or last_on_page.get("listed_at_iso") or "—"
            log(f"[OLX] {category_label}: дата останнього оголошення на сторінці: {last_date}")

        if stop_pages:
            break
        page += 1

    total_count = len(all_listings)
    log(f"[OLX] {category_label}: всього оголошень до опрацювання: {total_count}")
    if total_count == 0:
        return 0, 0

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
                detail_response = fetch_page(listing_url, delay_before=False)
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

                repo.upsert_listing(listing_url, search_data, detail=detail_data)
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
                )
                if unified_service:
                    try:
                        unified_service.sync_olx_listing(listing_url)
                    except Exception as sync_err:
                        log(f"[OLX] Помилка синхронізації в unified: {sync_err}")
        else:
            repo.upsert_listing(listing_url, search_data, detail=existing.get("detail"))
            if unified_service:
                try:
                    unified_service.sync_olx_listing(listing_url)
                except Exception as sync_err:
                    log(f"[OLX] Помилка синхронізації в unified: {sync_err}")

    return total_count, total_detail_fetches


# Категорії за замовчуванням: нежитлова нерухомість + земельні ділянки
DEFAULT_CATEGORIES: List[Dict[str, Any]] = [
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


def run_olx_update(
    settings: Optional[Settings] = None,
    categories: Optional[List[Dict[str, Any]]] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    days: Optional[int] = None,
) -> Dict[str, Any]:
    """
    Запускає оновлення оголошень OLX по заданих категоріях.
    Викликається з main.py та Telegram після/разом із оновленням ProZorro.

    Args:
        settings: налаштування (якщо None — створюються нові).
        categories: список словників { "label", "get_list_url", "max_pages" };
                    якщо None — використовуються DEFAULT_CATEGORIES.
        log_fn: опціональна функція для логів (наприклад, у Telegram).
        days: якщо 1 або 7 — поріг за календарними днями: зупиняємося, коли зустрічаємо
              оголошення з попередньої доби (наприклад, при 05.02 і «за добу» — беремо до
              зустрічі 03.02; за тиждень — до зустрічі дня перед 7-денним вікном). Якщо None — без обмеження.

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

    categories = categories or DEFAULT_CATEGORIES
    total_listings = 0
    total_detail_fetches = 0
    by_category: List[Dict[str, Any]] = []

    # Поріг за календарем: зупинка при оголошенні з «попередньої доби» (строго раніше вікна)
    # За добу (days=1): вікно 04.02–05.02, зупинка при зустрічі 03.02 → cutoff = початок 04.02.
    # За тиждень (days=7): вікно 30.01–05.02, зупинка при 29.01 → cutoff = початок 30.01.
    cutoff_utc: Optional[datetime] = None
    if days is not None and days >= 1:
        now_utc = datetime.now(timezone.utc)
        start_of_today_utc = now_utc.replace(hour=0, minute=0, second=0, microsecond=0)
        cutoff_utc = start_of_today_utc - timedelta(days=days)

    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            print(msg, flush=True)

    for cat in categories:
        label = cat.get("label", "?")
        get_list_url = cat.get("get_list_url")
        # Коли задано days (cutoff_utc) — обмеження на сторінки не ставимо, зупинка лише по даті
        max_pages = None if cutoff_utc is not None else int(cat.get("max_pages", scraper_config.MAX_SEARCH_PAGES))
        if not callable(get_list_url):
            log(f"[OLX] Пропуск категорії {label}: немає get_list_url")
            continue
        n_listings, n_details = _process_category(
            get_list_url,
            max_pages,
            label,
            repo,
            llm_extractor,
            geocoding_service,
            unified_service=unified_service,
            log_fn=log,
            cutoff_utc=cutoff_utc,
            usd_rate=usd_rate,
        )
        total_listings += n_listings
        total_detail_fetches += n_details
        by_category.append({
            "label": label,
            "listings": n_listings,
            "detail_fetches": n_details,
        })

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
