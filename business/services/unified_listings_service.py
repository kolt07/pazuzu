# -*- coding: utf-8 -*-
"""
Сервіс для синхронізації даних з джерел (OLX, ProZorro) в зведену таблицю unified_listings.
"""

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional, Set
import logging

from data.repositories.unified_listings_repository import UnifiedListingsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from business.services.geocoding_service import GeocodingService
from business.services.currency_rate_service import CurrencyRateService
from utils.price_metrics import compute_price_metrics
from utils.address_parser import parse_prozorro_item_address

logger = logging.getLogger(__name__)


class UnifiedListingsService:
    """Сервіс для синхронізації даних в зведену таблицю."""

    def __init__(self, settings=None):
        """Ініціалізація сервісу."""
        self.settings = settings
        self.unified_repo = UnifiedListingsRepository()
        self.olx_repo = OlxListingsRepository()
        self.prozorro_repo = ProZorroAuctionsRepository()
        self.geocoding_service = GeocodingService(settings)
        self.currency_service = CurrencyRateService(settings)
        self._usd_rate: Optional[float] = None
        self._ensure_usd_rate()

    def _ensure_usd_rate(self) -> None:
        """Оновлює курс USD, якщо ще не встановлено."""
        if self._usd_rate is not None and self._usd_rate > 0:
            return
        try:
            self._usd_rate = self.currency_service.get_today_usd_rate(allow_fetch=True)
        except Exception:
            self._usd_rate = None

    def _extract_region_from_query(self, query_text: str) -> Optional[str]:
        """Витягує назву області з query_text (напр. 'Житомирська обл.' або 'Волинська область')."""
        if not query_text or not isinstance(query_text, str):
            return None
        m = re.search(
            r"([А-Яа-яІіЇїЄєҐґ\-\s]+?)\s*(?:область|обл\.?)\b",
            query_text,
            re.I,
        )
        if m:
            return m.group(1).strip() or None
        return None

    def _normalize_address_from_geocode(
        self, geocode_result: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """
        Нормалізує адресу з результату геокодування в уніфікований формат.
        Якщо query_text містить область і є кілька результатів — обираємо той, чий регіон збігається.
        
        Args:
            geocode_result: Результат з geocoding_service.geocode()
            
        Returns:
            Словник з нормалізованою адресою або None
        """
        results = geocode_result.get("results", [])
        if not results:
            return None
        
        query_text = (geocode_result.get("query_text") or "").strip()
        query_region = self._extract_region_from_query(query_text)
        
        # Якщо в запиті є область — шукаємо результат, що їй відповідає
        if query_region and len(results) > 1:
            qr_lower = query_region.lower()
            for r in results:
                addr_struct = r.get("address_structured", {})
                region = addr_struct.get("region") or ""
                if region and qr_lower in region.lower():
                    result = r
                    break
            else:
                result = results[0]
        else:
            result = results[0]
        address_structured = result.get("address_structured", {})
        
        # Визначаємо повноту адреси
        # Повна адреса має street та street_number
        is_complete = bool(
            address_structured.get("street") and address_structured.get("street_number")
        )
        
        # Перевіряємо, чи координати не є просто центром населеного пункту
        # Якщо location_type == "APPROXIMATE" - це приблизна локація
        location_type = result.get("location_type", "")
        if location_type == "APPROXIMATE" and not is_complete:
            is_complete = False
        
        address = {
            "region": address_structured.get("region"),
            "settlement": address_structured.get("city") or address_structured.get("sublocality"),
            "district": address_structured.get("administrative_area_level_2"),
            "street": address_structured.get("street"),
            "building": address_structured.get("street_number"),
            "apartment": None,  # Не витягується з геокодування
            "coordinates": {
                "latitude": result.get("latitude"),
                "longitude": result.get("longitude"),
            },
            "is_complete": is_complete,
            "formatted_address": result.get("formatted_address", ""),
        }
        
        return address

    def _extract_addresses_from_olx(self, olx_doc: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Витягує та нормалізує адреси з документа OLX.
        Пріоритет: адреси з resolved_locations (порядок — LLM спочатку, потім raw, city, location).
        При кількох результатах геокодування обираємо той, чий регіон збігається з query_text.
        """
        addresses = []
        
        # Перевіряємо resolved_locations (порядок уже виставлений в helpers: LLM → raw → city → location)
        detail = olx_doc.get("detail", {})
        resolved_locations = detail.get("resolved_locations", [])
        
        for loc in resolved_locations:
            results = loc.get("results", [])
            query_text = (loc.get("query_text") or "").strip()
            if results:
                geocode_result = {
                    "results": results,
                    "query_text": query_text,
                }
                addr = self._normalize_address_from_geocode(geocode_result)
                if addr:
                    addresses.append(addr)
        
        # Якщо немає resolved_locations, пробуємо геокодувати з location
        if not addresses:
            search_data = olx_doc.get("search_data", {})
            location = search_data.get("location")
            if location:
                try:
                    geocode_result = self.geocoding_service.geocode(
                        query=location, region="ua", caller="unified_listings_olx"
                    )
                    addr = self._normalize_address_from_geocode(geocode_result)
                    if addr:
                        addresses.append(addr)
                except Exception as e:
                    logger.warning(f"Помилка геокодування OLX location {location}: {e}")
        
        return addresses

    def _extract_addresses_from_prozorro(
        self, prozorro_doc: Dict[str, Any]
    ) -> List[Dict[str, Any]]:
        """Витягує та нормалізує адреси з документа ProZorro."""
        addresses = []
        seen_address_strs: Set[str] = set()
        auction_data = prozorro_doc.get("auction_data", {})
        
        # 1. Спробуємо address_refs (якщо є після міграції 013)
        address_refs_list = auction_data.get("address_refs", [])
        if address_refs_list:
            for refs in address_refs_list:
                if not isinstance(refs, dict):
                    continue
                
                region = refs.get("region")
                city = refs.get("city")
                street = refs.get("street")
                building = refs.get("building")
                
                address_parts = []
                if isinstance(region, dict):
                    region_name = region.get("name")
                    if region_name:
                        address_parts.append(region_name)
                elif isinstance(region, str):
                    address_parts.append(region)
                
                if isinstance(city, dict):
                    city_name = city.get("name")
                    if city_name:
                        address_parts.append(city_name)
                elif isinstance(city, str):
                    address_parts.append(city)
                
                if isinstance(street, dict):
                    street_name = street.get("name")
                    if street_name:
                        street_type = street.get("street_type", "")
                        if street_type:
                            address_parts.append(f"{street_type} {street_name}")
                        else:
                            address_parts.append(street_name)
                elif isinstance(street, str):
                    address_parts.append(street)
                
                if isinstance(building, dict):
                    building_number = building.get("number")
                    if building_number:
                        building_str = str(building_number)
                        building_part = building.get("building_part")
                        if building_part:
                            building_str += f"/{building_part}"
                        address_parts.append(building_str)
                elif isinstance(building, str):
                    address_parts.append(building)
                
                if address_parts:
                    address_str = ", ".join(address_parts)
                    if address_str in seen_address_strs:
                        continue
                    seen_address_strs.add(address_str)
                    try:
                        geocode_result = self.geocoding_service.geocode(
                            query=address_str, region="ua", caller="unified_listings_prozorro"
                        )
                        addr = self._normalize_address_from_geocode(geocode_result)
                        if addr:
                            if isinstance(region, dict):
                                addr["region"] = region.get("name")
                            elif isinstance(region, str):
                                addr["region"] = region
                            if isinstance(city, dict):
                                addr["settlement"] = city.get("name")
                            elif isinstance(city, str):
                                addr["settlement"] = city
                            if isinstance(street, dict):
                                addr["street"] = street.get("name")
                            elif isinstance(street, str):
                                addr["street"] = street
                            if isinstance(building, dict):
                                addr["building"] = building.get("number")
                            elif isinstance(building, str):
                                addr["building"] = building
                            addr["apartment"] = None
                            addresses.append(addr)
                    except Exception as e:
                        logger.warning(
                            f"Помилка геокодування ProZorro address {address_str}: {e}"
                        )
        
        # 2. Якщо адрес немає — беремо з auction_data.items[].address (основний fallback)
        if not addresses:
            items = auction_data.get("items", [])
            for item in items:
                if not isinstance(item, dict):
                    continue
                item_address = item.get("address")
                if not item_address or not isinstance(item_address, dict):
                    continue
                parsed = parse_prozorro_item_address(item_address)
                if not parsed.get("region") and not parsed.get("settlement") and not parsed.get("street"):
                    continue
                address_str = parsed.get("formatted_address") or ""
                if not address_str or address_str in seen_address_strs:
                    continue
                seen_address_strs.add(address_str)
                try:
                    geocode_result = self.geocoding_service.geocode(
                        query=address_str, region="ua", caller="unified_listings_prozorro_items"
                    )
                    addr = self._normalize_address_from_geocode(geocode_result)
                    if addr:
                        addr["region"] = parsed.get("region")
                        addr["settlement"] = parsed.get("settlement")
                        addr["street"] = parsed.get("street")
                        if parsed.get("building"):
                            addr["building"] = parsed["building"]
                        addr["apartment"] = None
                        addresses.append(addr)
                except Exception as e:
                    logger.warning(
                        "Помилка геокодування ProZorro item address %s: %s",
                        address_str[:80], e,
                    )
        
        return addresses

    def _extract_cadastral_numbers(self, doc: Dict[str, Any], source: str) -> List[str]:
        """Витягує кадастрові номери з документа."""
        cadastral_numbers = []
        
        if source == "olx":
            detail = doc.get("detail", {})
            llm = detail.get("llm", {})
            cadastral = llm.get("cadastral_number")
            if cadastral:
                if isinstance(cadastral, str):
                    cadastral_numbers.append(cadastral)
                elif isinstance(cadastral, list):
                    cadastral_numbers.extend([str(c) for c in cadastral if c])
        
        elif source == "prozorro":
            auction_data = doc.get("auction_data", {})
            items = auction_data.get("items", [])
            for item in items:
                if not isinstance(item, dict):
                    continue
                item_props = item.get("itemProps")
                if isinstance(item_props, dict):
                    cadastral = item_props.get("cadastralNumber")
                    if cadastral:
                        if isinstance(cadastral, str):
                            cadastral_numbers.append(cadastral)
                        elif isinstance(cadastral, list):
                            cadastral_numbers.extend([str(c) for c in cadastral if c])
        
        # Видаляємо дублікати та порожні значення
        return list(set([c for c in cadastral_numbers if c and c.strip()]))

    def _determine_property_type(self, doc: Dict[str, Any], source: str) -> str:
        """Визначає тип оголошення."""
        if source == "olx":
            detail = doc.get("detail", {})
            llm = detail.get("llm", {})
            prop_type = (llm.get("property_type") or "").strip()
            if prop_type:
                prop_type_lower = prop_type.lower()
                # Земля с/г призначення — перевіряємо першим, щоб не звести до "Земельна ділянка"
                if "с/г" in prop_type_lower or "сільськогосподарськ" in prop_type_lower:
                    return "Землі с/г призначення"
                if "земля" in prop_type_lower or "ділянка" in prop_type_lower:
                    if "нерухомість" in prop_type_lower or "будівл" in prop_type_lower:
                        return "Земельна ділянка з нерухомістю"
                    return "Земельна ділянка"
                elif "нерухомість" in prop_type_lower or "будівл" in prop_type_lower:
                    return "Комерційна нерухомість"

            # Fallback: якщо LLM не повернув тип — інферуємо з заголовка/опису
            search_data = doc.get("search_data", {})
            title = ((search_data.get("title") or "") + " " + (detail.get("description") or "")).lower()
            if "с/г" in title or "сільськогосподарськ" in title:
                return "Землі с/г призначення"
            if any(kw in title for kw in ("земельн", "земля", "ділянк", "соток", "га ", " гектар")):
                if any(kw in title for kw in ("будинк", "будівл", "приміщен", "офіс", "склад", "магазин")):
                    return "Земельна ділянка з нерухомістю"
                return "Земельна ділянка"
            if any(kw in title for kw in ("нерухомість", "приміщен", "офіс", "склад", "магазин", "комерц", "нежитлов")):
                return "Комерційна нерухомість"
            return "інше"
        
        elif source == "prozorro":
            auction_data = doc.get("auction_data", {})
            items = auction_data.get("items", [])
            
            has_land = False
            has_building = False
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                # 1. CPV-коди (classification.id): 06 = земля, 04 = нерухомість, 05 = комплекс
                classification = item.get("classification", {})
                class_id = ""
                if isinstance(classification, dict):
                    class_id = (classification.get("id") or "") or ""
                if isinstance(class_id, str):
                    if class_id.startswith("06"):
                        has_land = True
                    elif class_id.startswith("04"):
                        has_building = True
                    elif class_id.startswith("05"):
                        has_land = True
                        has_building = True
                
                # 2. itemProps.itemPropsType (ProZorro.Sale)
                item_props = item.get("itemProps") or {}
                if isinstance(item_props, dict):
                    item_props_type = (item_props.get("itemPropsType") or "").lower()
                    if item_props_type == "land":
                        has_land = True
                    elif item_props_type in ("building", "asset"):
                        has_building = True
                    # Наявність площ у itemProps також вказує на тип
                    if item_props.get("landArea") and item_props.get("landArea", 0) > 0:
                        has_land = True
                    if any(
                        item_props.get(k)
                        for k in ("totalObjectArea", "totalBuildingArea", "usableArea")
                    ):
                        has_building = True
                
                # 3. Fallback: одиниці виміру (quantity.unit)
                unit = item.get("unit", {})
                unit_name = unit.get("name", {}) if isinstance(unit, dict) else {}
                unit_ua = ""
                if isinstance(unit_name, dict):
                    unit_ua = (unit_name.get("uk_UA") or "").lower()
                elif isinstance(unit_name, str):
                    unit_ua = unit_name.lower()
                if unit_ua:
                    if "га" in unit_ua or "гектар" in unit_ua:
                        has_land = True
                    if "м²" in unit_ua or "кв.м" in unit_ua or "квадратний" in unit_ua:
                        has_building = True
            
            if has_land and has_building:
                return "Земельна ділянка з нерухомістю"
            elif has_land:
                return "Земельна ділянка"
            elif has_building:
                return "Комерційна нерухомість"
            
            return "інше"
        
        return "інше"

    def _extract_price_info(self, doc: Dict[str, Any], source: str) -> Dict[str, Any]:
        """Витягує інформацію про ціну."""
        price_info = {
            "price_uah": None,
            "price_usd": None,
            "currency_rate": None,
        }
        
        if source == "olx":
            detail = doc.get("detail", {})
            search_data = doc.get("search_data", {})
            
            # Спочатку пробуємо з detail.price
            price_value = None
            currency = "UAH"
            
            price = detail.get("price")
            if isinstance(price, dict):
                price_value = price.get("value")
                currency = price.get("currency", "UAH")
            elif price is not None:
                # Якщо price не словник, але не None - можливо це прямий number
                try:
                    price_value = float(price)
                except (ValueError, TypeError):
                    pass
            
            # Якщо не знайшли в detail.price, пробуємо search_data
            if price_value is None:
                price_value = search_data.get("price_value")
                currency = search_data.get("currency", "UAH")
            
            # Нормалізуємо валюту
            if isinstance(currency, str):
                currency = currency.strip().upper() or "UAH"
            else:
                currency = "UAH"
            
            if currency not in ("UAH", "USD", "EUR"):
                currency = "UAH"
            
            # Обчислюємо ціни
            if price_value is not None:
                try:
                    price_value_float = float(price_value)
                    if price_value_float > 0:
                        if currency == "USD" and self._usd_rate:
                            # Якщо ціна в USD, конвертуємо в UAH
                            price_info["price_uah"] = price_value_float * self._usd_rate
                            price_info["price_usd"] = price_value_float
                            price_info["currency_rate"] = self._usd_rate
                        elif currency == "EUR" and self._usd_rate:
                            # EUR -> USD -> UAH (приблизно, якщо немає курсу EUR)
                            # Поки що просто конвертуємо через USD
                            price_info["price_uah"] = price_value_float * self._usd_rate * 1.1  # Приблизно
                            price_info["price_usd"] = price_value_float * 1.1
                            price_info["currency_rate"] = self._usd_rate
                        elif currency == "UAH":
                            price_info["price_uah"] = price_value_float
                            if self._usd_rate:
                                price_info["price_usd"] = price_value_float / self._usd_rate
                                price_info["currency_rate"] = self._usd_rate
                except (ValueError, TypeError):
                    pass
        
        elif source == "prozorro":
            auction_data = doc.get("auction_data", {})
            value = auction_data.get("value", {})
            
            # value може бути словником з amount або прямим числом
            if isinstance(value, dict):
                amount = value.get("amount")
            elif isinstance(value, (int, float)):
                amount = value
            else:
                amount = None
            
            if amount:
                price_info["price_uah"] = float(amount)
                if self._usd_rate:
                    price_info["price_usd"] = float(amount) / self._usd_rate
                    price_info["currency_rate"] = self._usd_rate
        
        return price_info

    def _convert_to_sqm(self, value: float, unit_ua: str) -> float:
        """Конвертує площу в м². unit_ua — одиниця з item.unit.name.uk_UA."""
        if not value:
            return 0.0
        u = (unit_ua or "").lower()
        if any(x in u for x in ["м²", "м2", "кв.м", "кв м", "квадратний метр"]):
            return float(value)
        if any(x in u for x in ["гектар", "hectare", "га"]):
            return float(value) * 10000.0
        if any(x in u for x in ["сотка", "соток", "ar"]):
            return float(value) * 100.0
        return float(value)  # припускаємо м²

    def _convert_to_hectares(self, value: float, unit_ua: str) -> float:
        """Конвертує площу в га. unit_ua — одиниця з item.unit.name.uk_UA."""
        if not value:
            return 0.0
        u = (unit_ua or "").lower()
        if any(x in u for x in ["гектар", "hectare", "га"]):
            return float(value)
        if any(x in u for x in ["м²", "м2", "кв.м", "кв м", "квадратний метр"]):
            return float(value) * 0.0001
        if any(x in u for x in ["сотка", "соток", "ar"]):
            return float(value) * 0.01
        return float(value) * 0.0001  # припускаємо м²

    def _extract_area_info(self, doc: Dict[str, Any], source: str) -> Dict[str, float]:
        """Витягує інформацію про площі."""
        area_info = {
            "building_area_sqm": None,
            "land_area_ha": None,
        }
        
        if source == "olx":
            detail = doc.get("detail", {})
            search_data = doc.get("search_data", {})
            llm = detail.get("llm", {})
            
            building_area = llm.get("building_area_sqm")
            land_area = llm.get("land_area_ha")
            
            if building_area:
                try:
                    area_info["building_area_sqm"] = float(building_area)
                except (ValueError, TypeError):
                    pass
            
            if land_area:
                try:
                    area_info["land_area_ha"] = float(land_area)
                except (ValueError, TypeError):
                    pass

            # Fallback: area_m2 як building_area — тільки якщо немає land_area_ha.
            # Для земельних ділянок area_m2 часто є площею землі в м², тому не
            # використовуємо її як building_area — це дало б хибну price_per_m2.
            if (
                not area_info["building_area_sqm"]
                and not area_info["land_area_ha"]
                and search_data.get("area_m2") is not None
            ):
                try:
                    area_info["building_area_sqm"] = float(search_data["area_m2"])
                except (ValueError, TypeError):
                    pass

        elif source == "prozorro":
            auction_data = doc.get("auction_data", {})
            items = auction_data.get("items", [])
            
            total_building_sqm = 0.0
            total_land_ha = 0.0
            
            for item in items:
                if not isinstance(item, dict):
                    continue
                
                # 1. Пріоритет: itemProps (ProZorro.Sale зберігає площу тут)
                item_props = item.get("itemProps") or {}
                if isinstance(item_props, dict):
                    item_props_type = item_props.get("itemPropsType", "")
                    classification = item.get("classification", {}) or {}
                    class_id = (classification.get("id") or "") if isinstance(classification, dict) else ""
                    
                    unit_ua = ""
                    unit = item.get("unit")
                    if isinstance(unit, dict):
                        unit_name = unit.get("name")
                        if isinstance(unit_name, dict):
                            unit_ua = (unit_name.get("uk_UA") or "").lower()
                        elif isinstance(unit_name, str):
                            unit_ua = unit_name.lower()
                    elif isinstance(unit, str):
                        unit_ua = unit.lower()
                    
                    # Земля: landArea
                    if item_props_type == "land" or (class_id and class_id.startswith("06")):
                        land_area = item_props.get("landArea")
                        if land_area is not None:
                            try:
                                val = float(land_area)
                                if val > 0:
                                    total_land_ha += self._convert_to_hectares(val, unit_ua)
                            except (ValueError, TypeError):
                                pass
                        continue
                    
                    # Нерухомість: totalObjectArea, totalBuildingArea, usableArea
                    building_area = (
                        item_props.get("totalObjectArea")
                        or item_props.get("totalBuildingArea")
                        or item_props.get("usableArea")
                    )
                    if building_area is not None:
                        try:
                            val = float(building_area)
                            if val > 0:
                                total_building_sqm += self._convert_to_sqm(val, unit_ua)
                        except (ValueError, TypeError):
                            pass
                        continue
                
                # 2. Fallback: quantity.value (стандартний ProZorro)
                quantity = item.get("quantity")
                qty_value = None
                if isinstance(quantity, dict):
                    qty_value = quantity.get("value")
                elif isinstance(quantity, (int, float)):
                    qty_value = quantity
                
                unit_ua = ""
                unit = item.get("unit")
                if isinstance(unit, dict):
                    unit_name = unit.get("name")
                    if isinstance(unit_name, dict):
                        unit_ua = (unit_name.get("uk_UA") or "").lower()
                    elif isinstance(unit_name, str):
                        unit_ua = unit_name.lower()
                elif isinstance(unit, str):
                    unit_ua = unit.lower()
                
                if qty_value is not None:
                    try:
                        qty_float = float(qty_value)
                        if qty_float > 0:
                            if unit_ua:
                                if "м²" in unit_ua or "кв.м" in unit_ua:
                                    total_building_sqm += qty_float
                                elif "га" in unit_ua or "гектар" in unit_ua:
                                    total_land_ha += qty_float
                            elif qty_float > 1000:
                                total_land_ha += qty_float / 10000.0
                            else:
                                total_building_sqm += qty_float
                    except (ValueError, TypeError):
                        pass
            
            if total_building_sqm > 0:
                area_info["building_area_sqm"] = total_building_sqm
            if total_land_ha > 0:
                area_info["land_area_ha"] = total_land_ha
        
        return area_info

    def sync_olx_listing(self, olx_url: str, usd_rate_override: Optional[float] = None) -> bool:
        """
        Синхронізує одне оголошення OLX в зведену таблицю.
        
        Args:
            olx_url: URL оголошення OLX
            usd_rate_override: опційний курс USD для конвертації (напр. з reformat)
            
        Returns:
            True якщо успішно
        """
        if usd_rate_override is not None and usd_rate_override > 0:
            self._usd_rate = usd_rate_override
        else:
            self._ensure_usd_rate()
        olx_doc = self.olx_repo.find_by_url(olx_url)
        if not olx_doc:
            logger.warning(f"OLX оголошення {olx_url} не знайдено")
            return False
        
        try:
            unified_doc = self._convert_olx_to_unified(olx_doc)
            property_type = unified_doc.get("property_type", "")
            # Не синхронізуємо сміттєві оголошення (не нерухомість, не земля)
            if property_type == "інше":
                canonical_url = olx_doc.get("url", olx_url)
                self.unified_repo.delete_by_source_id("olx", canonical_url)
                return False
            ok = self.unified_repo.upsert_listing(unified_doc)
            if ok:
                try:
                    from business.services.real_estate_objects_service import RealEstateObjectsService
                    reo_service = RealEstateObjectsService()
                    reo_service.process_listing("olx", olx_url, olx_doc=olx_doc)
                except Exception as reo_err:
                    logger.warning("Обробка ОНМ для OLX %s: %s", olx_url[:50], reo_err)
            return ok
        except Exception as e:
            logger.error(f"Помилка синхронізації OLX оголошення {olx_url}: {e}", exc_info=True)
            raise  # Піднімаємо помилку далі для детального логування в міграції

    def sync_prozorro_auction(self, auction_id: str) -> bool:
        """
        Синхронізує один аукціон ProZorro в зведену таблицю.
        
        Args:
            auction_id: ID аукціону ProZorro
            
        Returns:
            True якщо успішно
        """
        prozorro_doc = self.prozorro_repo.find_by_auction_id(auction_id)
        if not prozorro_doc:
            logger.warning(f"ProZorro аукціон {auction_id} не знайдено")
            return False
        
        try:
            unified_doc = self._convert_prozorro_to_unified(prozorro_doc)
            ok = self.unified_repo.upsert_listing(unified_doc)
            if ok:
                try:
                    from business.services.real_estate_objects_service import RealEstateObjectsService
                    reo_service = RealEstateObjectsService()
                    reo_service.process_listing("prozorro", auction_id, prozorro_doc=prozorro_doc)
                except Exception as reo_err:
                    logger.warning("Обробка ОНМ для ProZorro %s: %s", auction_id, reo_err)
            return ok
        except Exception as e:
            logger.error(f"Помилка синхронізації ProZorro аукціону {auction_id}: {e}", exc_info=True)
            raise  # Піднімаємо помилку далі для детального логування в міграції

    def _convert_olx_to_unified(self, olx_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертує документ OLX в уніфікований формат."""
        url = olx_doc.get("url", "")
        search_data = olx_doc.get("search_data", {})
        detail = olx_doc.get("detail", {})
        
        # Дата оновлення в джерелі
        source_updated_at = olx_doc.get("updated_at")
        if isinstance(source_updated_at, str):
            try:
                source_updated_at = datetime.fromisoformat(source_updated_at.replace("Z", "+00:00"))
            except Exception:
                source_updated_at = datetime.now(timezone.utc)
        elif not isinstance(source_updated_at, datetime):
            source_updated_at = datetime.now(timezone.utc)
        
        # Статус: всі оголошення OLX вважаються активними
        status = "активне"
        
        # Заголовок та опис
        title = search_data.get("title", "")
        description = detail.get("description", "")
        
        # Посилання
        page_url = url
        
        # Ідентифікатор
        identifier = url
        
        # Тип оголошення
        property_type = self._determine_property_type(olx_doc, "olx")
        
        # Адреси
        addresses = self._extract_addresses_from_olx(olx_doc)
        
        # Кадастрові номери
        cadastral_numbers = self._extract_cadastral_numbers(olx_doc, "olx")
        
        # Ціна
        price_info = self._extract_price_info(olx_doc, "olx")
        
        # Площі
        area_info = self._extract_area_info(olx_doc, "olx")
        
        # Поверх та теги з LLM
        llm = detail.get("llm", {}) if isinstance(detail, dict) else {}
        floor = llm.get("floor") or ""
        tags = llm.get("tags")
        if not isinstance(tags, list):
            tags = []
        
        # Розраховуємо цінові метрики
        price_metrics = compute_price_metrics(
            total_price_uah=price_info["price_uah"],
            building_area_sqm=area_info["building_area_sqm"],
            land_area_ha=area_info["land_area_ha"],
            uah_per_usd=self._usd_rate,
        )
        
        unified_doc = {
            "source": "olx",
            "source_id": url,
            "source_updated_at": source_updated_at,
            "status": status,
            "page_url": page_url,
            "identifier": identifier,
            "property_type": property_type,
            "title": title,
            "description": description,
            "addresses": addresses,
            "cadastral_numbers": cadastral_numbers,
            "building_area_sqm": area_info["building_area_sqm"],
            "land_area_ha": area_info["land_area_ha"],
            "floor": floor,
            "tags": tags,
            "price_uah": price_info["price_uah"],
            "price_usd": price_info["price_usd"],
            "price_per_m2_uah": price_metrics.get("price_per_m2_uah"),
            "price_per_m2_usd": price_metrics.get("price_per_m2_usd"),
            "price_per_ha_uah": price_metrics.get("price_per_ha_uah"),
            "price_per_ha_usd": price_metrics.get("price_per_ha_usd"),
            "currency_rate": price_info["currency_rate"],
        }
        
        return unified_doc

    def _convert_prozorro_to_unified(self, prozorro_doc: Dict[str, Any]) -> Dict[str, Any]:
        """Конвертує документ ProZorro в уніфікований формат."""
        auction_id = prozorro_doc.get("auction_id", "")
        auction_data = prozorro_doc.get("auction_data", {})
        
        # Дата оновлення в джерелі
        date_modified_str = auction_data.get("dateModified", "")
        source_updated_at = None
        if date_modified_str:
            try:
                if date_modified_str.endswith("Z"):
                    date_modified_str = date_modified_str.replace("Z", "+00:00")
                source_updated_at = datetime.fromisoformat(date_modified_str)
            except Exception:
                pass
        
        if not source_updated_at:
            source_updated_at = datetime.now(timezone.utc)
        
        # Статус
        status_str = auction_data.get("status", "")
        if status_str and status_str.startswith("active"):
            status = "активне"
        else:
            status = "неактивне"
        
        # Заголовок та опис
        title_obj = auction_data.get("title", {})
        if isinstance(title_obj, dict):
            title = title_obj.get("uk_UA") or title_obj.get("en_US") or title_obj.get("ru_RU") or ""
        else:
            title = str(title_obj) if title_obj else ""
        
        if not title:
            title = auction_data.get("id", "")
        
        description_obj = auction_data.get("description", {})
        if isinstance(description_obj, dict):
            description = description_obj.get("uk_UA") or description_obj.get("en_US") or description_obj.get("ru_RU") or ""
        else:
            description = str(description_obj) if description_obj else ""
        
        # Посилання
        page_url = f"https://prozorro.sale/auction/{auction_id}"
        
        # Ідентифікатор
        identifier = auction_id
        
        # Тип оголошення
        property_type = self._determine_property_type(prozorro_doc, "prozorro")
        
        # Адреси
        addresses = self._extract_addresses_from_prozorro(prozorro_doc)
        
        # Кадастрові номери
        cadastral_numbers = self._extract_cadastral_numbers(prozorro_doc, "prozorro")
        
        # Ціна
        price_info = self._extract_price_info(prozorro_doc, "prozorro")
        
        # Площі
        area_info = self._extract_area_info(prozorro_doc, "prozorro")
        
        # Поверх та теги (ProZorro: з auction_data якщо є)
        floor = auction_data.get("floor") or ""
        tags = auction_data.get("tags")
        if not isinstance(tags, list):
            tags = []
        
        # Розраховуємо цінові метрики
        price_metrics = compute_price_metrics(
            total_price_uah=price_info["price_uah"],
            building_area_sqm=area_info["building_area_sqm"],
            land_area_ha=area_info["land_area_ha"],
            uah_per_usd=self._usd_rate,
        )
        
        unified_doc = {
            "source": "prozorro",
            "source_id": auction_id,
            "source_updated_at": source_updated_at,
            "status": status,
            "page_url": page_url,
            "identifier": identifier,
            "property_type": property_type,
            "title": title,
            "description": description,
            "addresses": addresses,
            "cadastral_numbers": cadastral_numbers,
            "building_area_sqm": area_info["building_area_sqm"],
            "land_area_ha": area_info["land_area_ha"],
            "floor": floor,
            "tags": tags,
            "price_uah": price_info["price_uah"],
            "price_usd": price_info["price_usd"],
            "price_per_m2_uah": price_metrics.get("price_per_m2_uah"),
            "price_per_m2_usd": price_metrics.get("price_per_m2_usd"),
            "price_per_ha_uah": price_metrics.get("price_per_ha_uah"),
            "price_per_ha_usd": price_metrics.get("price_per_ha_usd"),
            "currency_rate": price_info["currency_rate"],
        }
        
        return unified_doc
