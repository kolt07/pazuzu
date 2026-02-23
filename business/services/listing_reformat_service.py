# -*- coding: utf-8 -*-
"""
Сервіс переформатування оголошень: повторна обробка сирих даних через LLM та геосервіси.
Використовується для виправлення помилок парсингу (наприклад, ціна за 2$ замість 2 000 000$).
"""

import logging
import re
from typing import Any, Dict, Optional

from config.settings import Settings
from data.repositories.olx_listings_repository import OlxListingsRepository
from business.services.olx_llm_extractor_service import OlxLLMExtractorService
from business.services.geocoding_service import GeocodingService
from business.services.unified_listings_service import UnifiedListingsService
from utils.price_metrics import compute_price_metrics

logger = logging.getLogger(__name__)


def _collect_and_geocode_locations(search_data: dict, detail_data: dict, geocoding_service, geography_service=None):
    """Імпорт з helpers для уникнення циклічних залежностей."""
    from scripts.olx_scraper.helpers import _collect_and_geocode_locations as _impl
    return _impl(search_data, detail_data, geocoding_service, geography_service)


def _extract_price_from_olx_url(url: str) -> Optional[float]:
    """
    Витягує ціну з URL оголошення OLX.
    Приклад: .../prodazh-promislovo-bazi-4-68-ga-volin-2-000-000-IDZUDDD.html -> 2000000
    Патерн: X-XXX-XXX або X-XXX-XXX-XXX перед -ID (ціна в slug).
    """
    if not url:
        return None
    match = re.search(r"-(\d{1,3}(?:-\d{3}){2,})-ID[A-Z0-9]+", url, re.I)
    if match:
        num_str = match.group(1).replace("-", "")
        try:
            val = int(num_str)
            if val >= 100:
                return float(val)
        except ValueError:
            pass
    return None


def _reparse_price_from_text(search_data: dict, detail_data: dict, olx_url: Optional[str] = None) -> Optional[float]:
    """
    Повторно парсить ціну з price_text (виправлення помилок типу 2$ замість 2 000 000$).
    Використовує виправлену логіку парсера (роздільники тисяч ·, nbsp тощо).
    Fallback: опис, заголовок, URL slug.
    """
    from scripts.olx_scraper.parser import _extract_price_value

    price_text = search_data.get("price_text")
    if price_text:
        val = _extract_price_value(price_text)
        if val is not None and val > 0:
            return val

    # Fallback: опис або заголовок (наприклад "Продаж бази $2 000 000")
    for text in [
        detail_data.get("description") or "",
        search_data.get("title") or "",
        search_data.get("raw_snippet") or "",
    ]:
        if not text or len(text) < 5:
            continue
        # Патерни: $2 000 000, $2,000,000, $2·000·000, 2 000 000 $
        for pattern in [
            r"\$\s*([\d\s.,·\u00a0\u202f]+)",
            r"([\d\s.,·\u00a0\u202f]+)\s*\$",
        ]:
            for m in re.finditer(pattern, text):
                raw = m.group(1).strip()
                if raw:
                    val = _extract_price_value(raw)
                    if val is not None and val >= 100:
                        return val
    # Fallback: URL slug (наприклад .../volin-2-000-000-IDZUDDD.html)
    if olx_url:
        val = _extract_price_from_olx_url(olx_url)
        if val is not None:
            return val
    return None


class ListingReformatService:
    """
    Переформатування оголошення: повторний LLM-парсинг, геокодування, перерахунок метрик.
    Як ніби оголошення щойно завантажили з джерела.
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.olx_repo = OlxListingsRepository()
        self.llm_extractor = OlxLLMExtractorService(self.settings)
        self.geocoding_service = GeocodingService(self.settings)
        self.unified_service = UnifiedListingsService(self.settings)

    def reformat_olx_listing(self, olx_url: str) -> Dict[str, Any]:
        """
        Переформатує оголошення OLX: повторний LLM-парсинг, геокодування, перерахунок метрик, синхронізація.
        
        Returns:
            {"success": bool, "message": str, "updated": bool}
        """
        olx_doc = self.olx_repo.find_by_url(olx_url)
        if not olx_doc:
            return {"success": False, "message": "Оголошення OLX не знайдено", "updated": False}

        search_data = olx_doc.get("search_data") or {}
        detail_data = dict(olx_doc.get("detail") or {})

        # Видаляємо кешовані результати для повторної обробки
        for key in ("llm", "llm_content_hash", "resolved_locations", "geocode_query_hashes", 
                    "address_refs", "price_metrics", "price"):
            detail_data.pop(key, None)

        if not search_data.get("title") and not search_data.get("price_text"):
            return {"success": False, "message": "Немає search_data для обробки", "updated": False}

        try:
            # 0. Re-fetch сторінки OLX для отримання актуальної ціни (обхід помилок парсингу в БД)
            try:
                from scripts.olx_scraper.fetcher import fetch_page
                from scripts.olx_scraper.parser import parse_detail_page
                import time
                from scripts.olx_scraper import config as scraper_config
                time.sleep(scraper_config.get_delay_detail_seconds())
                response = fetch_page(olx_url, delay_before=False)
                fresh_detail = parse_detail_page(response.text)
                if fresh_detail.get("price_text") and fresh_detail.get("price_value") is not None:
                    search_data["price_text"] = fresh_detail["price_text"]
                    search_data["price_value"] = fresh_detail["price_value"]
                    if fresh_detail.get("currency"):
                        search_data["currency"] = fresh_detail["currency"]
                    logger.info("Оновлено ціну з OLX: %s -> %s", olx_url[:50], fresh_detail.get("price_value"))
                if fresh_detail.get("description"):
                    detail_data["description"] = fresh_detail["description"]
            except Exception as fetch_err:
                logger.warning("Не вдалося перезавантажити сторінку OLX %s: %s", olx_url[:50], fetch_err)

            # 1. LLM-екстракція
            llm_data = self.llm_extractor.extract_structured_data(search_data, detail_data)
            if llm_data:
                detail_data["llm"] = llm_data
            new_hash = self.llm_extractor.calculate_listing_hash(search_data, detail_data)
            if new_hash:
                detail_data["llm_content_hash"] = new_hash

            # 2. Геокодування
            geography_service = None
            try:
                from business.services.geography_service import GeographyService
                geography_service = GeographyService()
            except ImportError:
                pass

            result = _collect_and_geocode_locations(
                search_data, detail_data, self.geocoding_service, geography_service
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

            # 3. Цінові метрики — повторно парсимо ціну з price_text/опису/URL (виправлення 2$ замість 2 000 000$)
            price_value = _reparse_price_from_text(search_data, detail_data, olx_url)
            old_price = search_data.get("price_value")
            if price_value is None:
                price_value = old_price
            elif price_value != old_price:
                search_data["price_value"] = price_value
                logger.info("Виправлено ціну з %s на %s (price_text: %r)", old_price, price_value, search_data.get("price_text", "")[:60])
            llm_struct = detail_data.get("llm") or {}
            total_area_m2 = llm_struct.get("total_area_m2") or search_data.get("area_m2")
            land_area_ha = llm_struct.get("land_area_ha")

            usd_rate = None
            try:
                from business.services.currency_rate_service import CurrencyRateService
                usd_rate = CurrencyRateService(self.settings).get_today_usd_rate(allow_fetch=True)
            except Exception:
                pass

            currency = (search_data.get("currency") or "UAH").strip().upper() or "UAH"
            if currency not in ("UAH", "USD", "EUR"):
                currency = "UAH"
            total_price_uah = price_value
            if price_value is not None and currency == "USD" and usd_rate:
                total_price_uah = price_value * usd_rate
            elif price_value is not None and currency == "EUR" and usd_rate:
                total_price_uah = price_value * usd_rate * 1.1

            metrics = compute_price_metrics(
                total_price_uah=total_price_uah,
                building_area_sqm=total_area_m2,
                land_area_ha=land_area_ha,
                uah_per_usd=usd_rate,
            )
            detail_data["price_metrics"] = metrics
            if price_value is not None:
                detail_data["price"] = {"value": float(price_value), "currency": currency}

            # 4. formatted_address
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
                from scripts.olx_scraper.helpers import _address_line_from_llm_address
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

            # 5. Збереження в olx_listings
            self.olx_repo.upsert_listing(olx_url, search_data, detail=detail_data, is_active=True)

            # 6. Синхронізація в unified_listings (передаємо курс для коректної конвертації USD→UAH)
            self.unified_service.sync_olx_listing(olx_url, usd_rate_override=usd_rate)

            # 7. Контрольна перевірка аномальних цін — встановлюємо або очищаємо price_notes
            try:
                from business.services.price_anomaly_service import PriceAnomalyService
                anomaly_svc = PriceAnomalyService()
                anomalous = anomaly_svc.find_anomalous_listings(limit=500)
                found_anomaly = False
                for item in anomalous:
                    sid = item.get("source_id")
                    if sid == olx_url or (olx_url and sid and (olx_url in sid or sid in olx_url)):
                        logger.info("Оголошення %s має аномальну ціну: %s", olx_url[:50], item.get("anomaly_type"))
                        anomaly_svc.set_price_notes("olx", sid or olx_url, "Ціна потребує перевірки")
                        found_anomaly = True
                        break
                if not found_anomaly:
                    from data.repositories.olx_listings_repository import _olx_url_variants
                    for variant in [olx_url] + [v for v in _olx_url_variants(olx_url) if v != olx_url]:
                        unified_doc = self.unified_service.unified_repo.find_by_source_id("olx", variant)
                        if unified_doc:
                            anomaly_svc.set_price_notes("olx", unified_doc.get("source_id") or variant, "")
                            break
            except Exception as e:
                logger.debug("Пропуск перевірки аномалій: %s", e)

            logger.info("Переформатування OLX %s завершено", olx_url[:60])
            return {"success": True, "message": "Оголошення переформатовано", "updated": True}

        except Exception as e:
            logger.exception("Помилка переформатування OLX %s: %s", olx_url[:60], e)
            return {"success": False, "message": str(e), "updated": False}

    def reformat_prozorro_auction(self, auction_id: str) -> Dict[str, Any]:
        """
        Переформатує аукціон ProZorro: повторна синхронізація в unified_listings.
        ProZorro має структуровані дані, LLM не використовується.
        """
        try:
            success = self.unified_service.sync_prozorro_auction(auction_id)
            return {"success": success, "message": "Аукціон переформатовано" if success else "Аукціон не знайдено", "updated": success}
        except Exception as e:
            logger.exception("Помилка переформатування ProZorro %s: %s", auction_id, e)
            return {"success": False, "message": str(e), "updated": False}

    def reformat_listing(self, source: str, source_id: str) -> Dict[str, Any]:
        """
        Переформатує оголошення або аукціон.
        
        Args:
            source: "olx" або "prozorro"
            source_id: URL для OLX, auction_id для ProZorro
        """
        source = (source or "").strip().lower()
        if source == "olx":
            return self.reformat_olx_listing(source_id)
        if source == "prozorro":
            return self.reformat_prozorro_auction(source_id)
        return {"success": False, "message": "Невідоме джерело", "updated": False}
