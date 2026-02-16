# -*- coding: utf-8 -*-
"""
SourceFieldMapper: централізований маппінг логічних полів на фізичні шляхи в документах
для різних джерел даних (prozorro_auctions, olx_listings).
"""

from typing import Dict, Any, Optional


class SourceFieldMapper:
    """
    Централізований маппінг логічних полів на фізичні шляхи в документах.
    
    Використання:
        mapper = SourceFieldMapper()
        field_path = mapper.get_field_path("price", "prozorro_auctions")
        # Поверне: "auction_data.value.amount"
    """
    
    FIELD_MAP: Dict[str, Dict[str, str]] = {
        "prozorro_auctions": {
            "city": "auction_data.address_refs.city.name",
            "region": "auction_data.address_refs.region.name",
            "price": "auction_data.value.amount",
            "date": "auction_data.dateModified",
            "status": "auction_data.status",
            "bids_count": "bids_count",
            "bidders_count": "bidders_count",
            # Fallback поля для адрес
            "city_fallback": "auction_data.items.address.locality.uk_UA",
            "region_fallback": "auction_data.items.address.region.uk_UA",
        },
        "olx_listings": {
            "city": "detail.address_refs.city.name",
            "region": "detail.address_refs.region.name",
            "price": "search_data.price",
            "date": "updated_at",
            "area": "detail.llm.building_area_sqm",
            # Fallback поля для адрес
            "city_fallback": "detail.resolved_locations.address_structured.city",
            "region_fallback": "search_data.location",
        },
        "unified_listings": {
            "city": "addresses.settlement",
            "region": "addresses.region",
            # price завжди означає price_uah (ціна в грн), якщо не вказано інакше
            "price": "price_uah",
            "date": "source_updated_at",
            "status": "status",
            "area": "building_area_sqm",
            "building_area_sqm": "building_area_sqm",
            "land_area_ha": "land_area_ha",
            "price_per_m2": "price_per_m2_uah",
            "price_per_m2_uah": "price_per_m2_uah",
            "average_price_per_sqm": "price_per_m2_uah",
            # unified_listings не має fallback — addresses є основним полем
            "city_fallback": None,
            "region_fallback": None,
        }
    }
    
    @classmethod
    def get_field_path(cls, field: str, source: str) -> str:
        """
        Повертає фізичний шлях до поля в документі залежно від джерела.
        
        Args:
            field: логічна назва поля (city, region, price, тощо)
            source: джерело даних (prozorro_auctions, olx_listings)
            
        Returns:
            Фізичний шлях до поля або саме поле, якщо маппінг не знайдено
        """
        source_map = cls.FIELD_MAP.get(source, {})
        return source_map.get(field, field)
    
    @classmethod
    def get_city_field(cls, source: str) -> str:
        """Повертає шлях до поля міста для джерела."""
        return cls.get_field_path("city", source)
    
    @classmethod
    def get_region_field(cls, source: str) -> str:
        """Повертає шлях до поля регіону для джерела."""
        return cls.get_field_path("region", source)

    @classmethod
    def get_addresses_array_path(cls, source: str) -> Optional[str]:
        """
        Повертає шлях до масиву адрес для $elemMatch.
        Для unified_listings — addresses; для prozorro/olx — address_refs всередині відповідного поля.
        """
        if source == "unified_listings":
            return "addresses"
        field = cls.get_region_field(source)
        if "address_refs" in field:
            parts = field.split(".")
            refs_idx = next((i for i, p in enumerate(parts) if p == "address_refs"), None)
            if refs_idx is not None:
                return ".".join(parts[: refs_idx + 1])
        return None

    @classmethod
    def get_geo_match_keys(cls, source: str) -> tuple:
        """
        Повертає (region_key, city_key) для $elemMatch.
        unified_listings: region, settlement (flat); prozorro/olx: region.name, city.name.
        """
        if source == "unified_listings":
            return ("region", "settlement")
        return ("region.name", "city.name")
    
    @classmethod
    def get_price_field(cls, source: str) -> str:
        """Повертає шлях до поля ціни для джерела."""
        return cls.get_field_path("price", source)
    
    @classmethod
    def get_city_fallback_field(cls, source: str) -> Optional[str]:
        """Повертає fallback шлях до поля міста (якщо є)."""
        return cls.FIELD_MAP.get(source, {}).get("city_fallback")
    
    @classmethod
    def get_region_fallback_field(cls, source: str) -> Optional[str]:
        """Повертає fallback шлях до поля регіону (якщо є)."""
        return cls.FIELD_MAP.get(source, {}).get("region_fallback")
    
    @classmethod
    def get_all_fields_for_source(cls, source: str) -> Dict[str, str]:
        """Повертає всі маппінги полів для джерела."""
        return cls.FIELD_MAP.get(source, {}).copy()
    
    @classmethod
    def is_valid_source(cls, source: str) -> bool:
        """Перевіряє, чи є джерело в маппінгу."""
        return source in cls.FIELD_MAP
