# -*- coding: utf-8 -*-
"""
Domain-сутність оголошення OLX (olx_listings).
"""

from typing import Any, Dict, List, Optional

from domain.entities.base import BaseEntity


class OlxListing(BaseEntity):
    """
    Сутність оголошення OLX.
    Методи для отримання властивостей та пов'язаних даних.
    """

    def __init__(self, raw_data: Dict[str, Any]):
        super().__init__(raw_data, "olx_listings")

    @property
    def id(self) -> Optional[str]:
        """Системний _id документа."""
        return self.get_property("_id")

    @property
    def url(self) -> str:
        """URL оголошення (ідентифікатор)."""
        return self.get_property("url") or ""

    def get_search_data(self) -> Dict[str, Any]:
        """Дані зі сторінки пошуку."""
        data = self.get_property("search_data")
        return dict(data) if isinstance(data, dict) else {}

    def get_detail(self) -> Dict[str, Any]:
        """Дані зі сторінки оголошення (деталі)."""
        data = self.get_property("detail")
        return dict(data) if isinstance(data, dict) else {}

    @property
    def title(self) -> str:
        """Заголовок з search_data."""
        search = self.get_search_data()
        return search.get("title", "") if isinstance(search, dict) else ""

    @property
    def price(self) -> Optional[float]:
        """Ціна (число) з search_data."""
        search = self.get_search_data()
        if not isinstance(search, dict):
            return None
        val = search.get("price") or search.get("price_value")
        return float(val) if val is not None else None

    @property
    def location(self) -> str:
        """Локація з search_data."""
        search = self.get_search_data()
        return search.get("location", "") if isinstance(search, dict) else ""

    def get_llm_data(self) -> Dict[str, Any]:
        """Результат LLM-парсингу з detail.llm."""
        detail = self.get_detail()
        llm = detail.get("llm") if isinstance(detail, dict) else None
        return dict(llm) if isinstance(llm, dict) else {}

    @property
    def property_type(self) -> str:
        """Тип нерухомості з LLM."""
        llm = self.get_llm_data()
        return llm.get("property_type", "") if isinstance(llm, dict) else ""

    def get_resolved_locations(self) -> List[Dict[str, Any]]:
        """Результати геокодування адрес."""
        detail = self.get_detail()
        locs = detail.get("resolved_locations") if isinstance(detail, dict) else None
        return list(locs) if isinstance(locs, list) else []

    def get_updated_at(self):
        """Дата оновлення запису."""
        return self.get_property("updated_at")
