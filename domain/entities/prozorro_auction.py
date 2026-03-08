# -*- coding: utf-8 -*-
"""
Domain-сутність аукціону ProZorro (prozorro_auctions).
"""

from typing import Any, Dict, List, Optional

from domain.entities.base import BaseEntity


class ProzorroAuction(BaseEntity):
    """
    Сутність аукціону ProZorro.
    Методи для отримання властивостей та пов'язаних даних.
    """

    def __init__(self, raw_data: Dict[str, Any]):
        super().__init__(raw_data, "prozorro_auctions")

    @property
    def id(self) -> Optional[str]:
        """Системний _id документа."""
        return self.get_property("_id")

    @property
    def auction_id(self) -> str:
        """Ідентифікатор аукціону з ProZorro."""
        return self.get_property("auction_id") or ""

    def get_auction_data(self) -> Dict[str, Any]:
        """Повертає повні дані аукціону (auction_data)."""
        data = self.get_property("auction_data")
        return dict(data) if isinstance(data, dict) else {}

    def get_status(self) -> str:
        """Статус аукціону."""
        ad = self.get_auction_data()
        return ad.get("status", "") if isinstance(ad, dict) else ""

    def get_value_amount(self) -> Optional[float]:
        """Стартова ціна (amount) в гривнях."""
        ad = self.get_auction_data()
        if not isinstance(ad, dict):
            return None
        value = ad.get("value")
        if isinstance(value, dict):
            amount = value.get("amount")
            return float(amount) if amount is not None else None
        return float(value) if value is not None else None

    def get_date_modified(self) -> Optional[str]:
        """Дата модифікації (ISO рядок)."""
        ad = self.get_auction_data()
        return ad.get("dateModified") if isinstance(ad, dict) else None

    def get_bids(self) -> List[Dict[str, Any]]:
        """Масив заявок (bids)."""
        ad = self.get_auction_data()
        bids = ad.get("bids") if isinstance(ad, dict) else None
        return list(bids) if isinstance(bids, list) else []

    def get_items(self) -> List[Dict[str, Any]]:
        """Масив предметів аукціону (items)."""
        ad = self.get_auction_data()
        items = ad.get("items") if isinstance(ad, dict) else None
        return list(items) if isinstance(items, list) else []

    def get_page_url(self) -> str:
        """Посилання на сторінку аукціону."""
        aid = self.auction_id
        return f"https://prozorro.sale/auction/{aid}" if aid else ""
