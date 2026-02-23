# -*- coding: utf-8 -*-
"""
Domain-сутність об'єкта нерухомого майна (ОНМ) — real_estate_objects.
Єдина колекція: land_plot, building, premises.
"""

from typing import Any, Dict, List, Optional

from domain.entities.base import BaseEntity


class RealEstateObject(BaseEntity):
    """
    Сутність об'єкта нерухомого майна.
    Методи для отримання властивостей.
    """

    def __init__(self, raw_data: Dict[str, Any]):
        super().__init__(raw_data, "real_estate_objects")

    @property
    def id(self) -> Optional[str]:
        """Системний _id документа."""
        return self.get_property("_id")

    @property
    def type(self) -> str:
        """Тип об'єкта: land_plot, building, premises."""
        return self.get_property("type") or ""

    @property
    def description(self) -> str:
        """Короткий опис (напр. ТРЦ Гулівер, Комерційне приміщення)."""
        return self.get_property("description") or ""

    @property
    def area_sqm(self) -> Optional[float]:
        """Площа в м²."""
        return self.get_property("area_sqm")

    def get_cadastral_info(self) -> Dict[str, Any]:
        """Для land_plot: cadastral_number, purpose, area_sqm тощо."""
        info = self.get_property("cadastral_info")
        return dict(info) if isinstance(info, dict) else {}

    def get_address(self) -> Dict[str, Any]:
        """Для building: region, settlement, street, formatted_address, coordinates."""
        addr = self.get_property("address")
        return dict(addr) if isinstance(addr, dict) else {}

    def get_source_listing_ids(self) -> List[Dict[str, Any]]:
        """Масив {source, source_id} — оголошення, з яких витягнуто об'єкт."""
        refs = self.get_property("source_listing_ids")
        return list(refs) if isinstance(refs, list) else []

    @property
    def building_id(self) -> Optional[str]:
        """Для premises: ID будівлі."""
        return self.get_property("building_id")
