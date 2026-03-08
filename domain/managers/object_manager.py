# -*- coding: utf-8 -*-
"""
ObjectManager: робота з конкретними записами БД.
Отримання, оновлення окремого об'єкта за ідентифікатором.
"""

from typing import Any, Dict, Optional, Type

from domain.entities.base import BaseEntity
from domain.gateways.listing_gateway import ListingGateway


class ObjectManager:
    """
    Менеджер об'єкта — робота з конкретним записом.
    Використовує ListingGateway для доступу до даних.
    """

    def __init__(self):
        self._gateway = ListingGateway()

    def get(
        self,
        object_id: str,
        collection: str = "unified_listings"
    ) -> Optional[BaseEntity]:
        """
        Отримати об'єкт за ідентифікатором.
        
        Args:
            object_id: _id або source:source_id
            collection: unified_listings, prozorro_auctions, olx_listings
        
        Returns:
            Domain-сутність або None
        """
        if collection == "unified_listings":
            return self._gateway.get_unified_listing_by_id(object_id)
        if collection == "prozorro_auctions":
            return self._gateway.get_prozorro_auction_by_id(object_id)
        if collection == "olx_listings":
            return self._gateway.get_olx_listing_by_url(object_id)
        return None
