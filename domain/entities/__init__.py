# -*- coding: utf-8 -*-
"""Domain entities."""

from domain.entities.base import BaseEntity
from domain.entities.unified_listing import UnifiedListing
from domain.entities.prozorro_auction import ProzorroAuction
from domain.entities.olx_listing import OlxListing
from domain.entities.listing_analytics import ListingAnalytics
from domain.entities.real_estate_object import RealEstateObject
from domain.entities.listing_collection import (
    UnifiedListingCollection,
    ProzorroAuctionCollection,
    OlxListingCollection,
    ListingAnalyticsCollection,
    RealEstateObjectCollection,
)

__all__ = [
    "BaseEntity",
    "UnifiedListing",
    "ProzorroAuction",
    "OlxListing",
    "ListingAnalytics",
    "RealEstateObject",
    "UnifiedListingCollection",
    "ProzorroAuctionCollection",
    "OlxListingCollection",
    "ListingAnalyticsCollection",
    "RealEstateObjectCollection",
]
