# -*- coding: utf-8 -*-
"""
Domain-сутність зведеного оголошення (unified_listings).
Об'єднує дані з OLX та ProZorro в єдину структуру.
"""

from typing import Any, Dict, List, Optional

from domain.entities.base import BaseEntity


class UnifiedListing(BaseEntity):
    """
    Сутність зведеного оголошення.
    Методи для отримання властивостей та пов'язаних даних.
    """

    def __init__(self, raw_data: Dict[str, Any]):
        super().__init__(raw_data, "unified_listings")

    @property
    def id(self) -> Optional[str]:
        """Системний _id документа."""
        return self.get_property("_id")

    @property
    def source(self) -> str:
        """Джерело: olx або prozorro."""
        return self.get_property("source") or ""

    @property
    def source_id(self) -> str:
        """ID в джерелі (URL для OLX, auction_id для ProZorro)."""
        return self.get_property("source_id") or ""

    @property
    def composite_id(self) -> str:
        """Складний ідентифікатор: source:source_id."""
        return f"{self.source}:{self.source_id}"

    @property
    def status(self) -> str:
        """Статус: активне / неактивне."""
        return self.get_property("status") or ""

    @property
    def property_type(self) -> str:
        """Тип нерухомості."""
        return self.get_property("property_type") or ""

    @property
    def title(self) -> str:
        """Заголовок оголошення."""
        return self.get_property("title") or ""

    @property
    def description(self) -> str:
        """Опис оголошення."""
        return self.get_property("description") or ""

    @property
    def page_url(self) -> str:
        """Посилання на сторінку оголошення."""
        return self.get_property("page_url") or ""

    def get_addresses(self) -> List[Dict[str, Any]]:
        """Повертає масив нормалізованих адрес."""
        addrs = self.get_property("addresses")
        return list(addrs) if isinstance(addrs, list) else []

    def get_first_address(self) -> Optional[Dict[str, Any]]:
        """Повертає першу адресу або None."""
        addresses = self.get_addresses()
        return addresses[0] if addresses else None

    def get_region(self) -> Optional[str]:
        """Область (root або з першої адреси)."""
        v = self.get_property("region")
        if v:
            return v
        addr = self.get_first_address()
        return addr.get("region") if isinstance(addr, dict) else None

    def get_settlement(self) -> Optional[str]:
        """Місто/населений пункт (root city або з першої адреси)."""
        v = self.get_property("city")
        if v:
            return v
        addr = self.get_first_address()
        return addr.get("settlement") if isinstance(addr, dict) else None

    def get_oblast_raion(self) -> Optional[str]:
        """Район області (root)."""
        return self.get_property("oblast_raion")

    def get_city_district(self) -> Optional[str]:
        """Район міста (root, для великих міст)."""
        return self.get_property("city_district")

    def get_cadastral_numbers(self) -> List[str]:
        """Масив кадастрових номерів."""
        nums = self.get_property("cadastral_numbers")
        return list(nums) if isinstance(nums, list) else []

    @property
    def building_area_sqm(self) -> Optional[float]:
        """Площа нерухомості (м²)."""
        val = self.get_property("building_area_sqm")
        return float(val) if val is not None else None

    @property
    def land_area_sqm(self) -> Optional[float]:
        """Площа земельної ділянки в м². Для відображення в сотках: land_area_sqm / 100."""
        val = self.get_property("land_area_sqm")
        return float(val) if val is not None else None

    @property
    def land_area_sotky(self) -> Optional[float]:
        """Площа земельної ділянки в сотках (для відображення). 1 сотка = 100 м²."""
        sqm = self.land_area_sqm
        if sqm is None or sqm <= 0:
            return None
        return sqm / 100.0

    @property
    def price_per_sotka_uah(self) -> Optional[float]:
        """Ціна за сотку в гривнях (для відображення землі). price_per_ha_uah / 100."""
        ha = self.get_property("price_per_ha_uah")
        if ha is None:
            return None
        try:
            return float(ha) / 100.0
        except (TypeError, ValueError):
            return None

    @property
    def price_uah(self) -> Optional[float]:
        """Ціна в гривнях."""
        val = self.get_property("price_uah")
        return float(val) if val is not None else None

    @property
    def price_usd(self) -> Optional[float]:
        """Ціна в доларах."""
        val = self.get_property("price_usd")
        return float(val) if val is not None else None

    @property
    def price_per_m2_uah(self) -> Optional[float]:
        """Ціна за м² в гривнях."""
        val = self.get_property("price_per_m2_uah")
        return float(val) if val is not None else None

    @property
    def price_per_ha_uah(self) -> Optional[float]:
        """Ціна за га в гривнях."""
        val = self.get_property("price_per_ha_uah")
        return float(val) if val is not None else None

    def get_source_updated_at(self):
        """Дата оновлення в джерелі."""
        return self.get_property("source_updated_at")

    def get_system_updated_at(self):
        """Дата оновлення в системі."""
        return self.get_property("system_updated_at")

    def _format_addresses_for_export(self, addresses: List[Dict[str, Any]]) -> str:
        """Формує адреси у читабельний текстовий вигляд."""
        if not addresses or not isinstance(addresses, list):
            return ""
        parts_list = []
        for addr in addresses:
            if not isinstance(addr, dict):
                continue
            if addr.get("formatted_address"):
                parts_list.append(addr["formatted_address"])
                continue
            p = []
            if addr.get("region"):
                p.append(addr["region"])
            if addr.get("settlement"):
                p.append(addr["settlement"])
            if addr.get("district"):
                p.append(addr["district"])
            if addr.get("city_district"):
                p.append(addr["city_district"])
            if addr.get("street"):
                p.append(addr["street"])
            if addr.get("building"):
                p.append(addr["building"])
            if addr.get("apartment"):
                p.append("кв. " + str(addr["apartment"]))
            if p:
                parts_list.append(", ".join(str(x) for x in p))
        return "; ".join(parts_list) if parts_list else ""

    def to_export_row(self, fields: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Перетворює на рядок для експорту.
        Адреси виводяться у читабельному текстовому вигляді, не сирим JSON.
        """
        default_fields = [
            "source",
            "source_id",
            "status",
            "property_type",
            "building_area_sqm",
            "land_area_sqm",
            "title",
            "description",
            "page_url",
            "price_uah",
            "price_usd",
            "price_per_m2_uah",
            "price_per_ha_uah",
            "addresses",
            "cadastral_numbers",
            "source_updated_at",
        ]
        cols = fields if fields else default_fields
        row = {}
        for field in cols:
            if field == "addresses":
                addrs = self.get_addresses()
                row[field] = self._format_addresses_for_export(addrs)
            else:
                value = self.get_property(field)
                if value is not None and not isinstance(value, (dict, list)):
                    row[field] = value
                elif value is not None:
                    import json
                    row[field] = json.dumps(value, ensure_ascii=False, default=str)
                else:
                    row[field] = ""
        return row

    def get_raw_source_id(self) -> Optional[str]:
        """Повертає ідентифікатор у джерелі для отримання сирих даних (url для OLX, auction_id для ProZorro)."""
        return self.source_id or None
