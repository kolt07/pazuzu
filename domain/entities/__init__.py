# -*- coding: utf-8 -*-
"""Domain entities."""

from domain.entities.base import BaseEntity
from domain.entities.unified_listing import UnifiedListing
from domain.entities.prozorro_auction import ProzorroAuction
from domain.entities.olx_listing import OlxListing
from domain.entities.listing_collection import (
    UnifiedListingCollection,
    ProzorroAuctionCollection,
    OlxListingCollection,
)

__all__ = [
    "BaseEntity",
    "UnifiedListing",
    "ProzorroAuction",
    "OlxListing",
    "UnifiedListingCollection",
    "ProzorroAuctionCollection",
    "OlxListingCollection",
]
