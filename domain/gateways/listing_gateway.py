# -*- coding: utf-8 -*-
"""
Domain gateway для отримання сутностей з репозиторіїв.
Перетворює сирі документи з БД на domain-об'єкти (entities та collections).
"""

from typing import TYPE_CHECKING, Any, Dict, List, Optional

from data.repositories.unified_listings_repository import UnifiedListingsRepository

if TYPE_CHECKING:
    from domain.entities.unified_listing import UnifiedListing
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from domain.entities.unified_listing import UnifiedListing
from domain.entities.prozorro_auction import ProzorroAuction
from domain.entities.olx_listing import OlxListing
from domain.entities.listing_collection import (
    UnifiedListingCollection,
    ProzorroAuctionCollection,
    OlxListingCollection,
)

# Підтримувані колекції
COLLECTION_UNIFIED = "unified_listings"
COLLECTION_PROZORRO = "prozorro_auctions"
COLLECTION_OLX = "olx_listings"
EXPORT_COLLECTIONS = (COLLECTION_UNIFIED, COLLECTION_PROZORRO, COLLECTION_OLX)
TEMP_COLLECTION_NAMES = (COLLECTION_UNIFIED, COLLECTION_PROZORRO, COLLECTION_OLX)


class ListingGateway:
    """
    Gateway для отримання domain-сутностей з репозиторіїв.
    Сервіси та MCP-інструменти використовують його замість прямого доступу до репозиторіїв.
    """

    def __init__(self):
        self._unified_repo = UnifiedListingsRepository()
        self._prozorro_repo = ProZorroAuctionsRepository()
        self._olx_repo = OlxListingsRepository()

    def _get_repository(self, collection: str):
        """Повертає репозиторій за назвою колекції."""
        if collection == COLLECTION_UNIFIED:
            return self._unified_repo
        if collection == COLLECTION_PROZORRO:
            return self._prozorro_repo
        if collection == COLLECTION_OLX:
            return self._olx_repo
        raise ValueError(f"Непідтримувана колекція: {collection}")

    def get_unified_listing_by_id(
        self, id_or_composite: str
    ) -> Optional[UnifiedListing]:
        """
        Отримує одне зведене оголошення за _id або source:source_id.
        """
        ids = self._parse_ids(id_or_composite, COLLECTION_UNIFIED)
        if not ids:
            return None
        repo = self._unified_repo
        docs = repo.get_by_ids(ids)
        if not docs:
            return None
        return UnifiedListing(docs[0])

    def get_unified_listings_by_ids(
        self, ids: List[str]
    ) -> UnifiedListingCollection:
        """Отримує колекцію зведених оголошень за списком ідентифікаторів."""
        parsed = self._parse_ids_list(ids, COLLECTION_UNIFIED)
        if not parsed:
            return UnifiedListingCollection.from_raw_list([])
        docs = self._unified_repo.get_by_ids(parsed)
        return UnifiedListingCollection.from_raw_list(docs)

    def get_prozorro_auction_by_id(self, auction_id: str) -> Optional[ProzorroAuction]:
        """Отримує один аукціон ProZorro за auction_id або _id."""
        ids = self._parse_ids(auction_id, COLLECTION_PROZORRO)
        if not ids:
            return None
        docs = self._prozorro_repo.get_by_ids(ids)
        if not docs:
            return None
        return ProzorroAuction(docs[0])

    def get_prozorro_auctions_by_ids(
        self, ids: List[str]
    ) -> ProzorroAuctionCollection:
        """Отримує колекцію аукціонів ProZorro за списком ідентифікаторів."""
        parsed = self._parse_ids_list(ids, COLLECTION_PROZORRO)
        if not parsed:
            return ProzorroAuctionCollection.from_raw_list([])
        docs = self._prozorro_repo.get_by_ids(parsed)
        return ProzorroAuctionCollection.from_raw_list(docs)

    def get_olx_listing_by_url(self, url: str) -> Optional[OlxListing]:
        """Отримує одне оголошення OLX за url або _id."""
        ids = self._parse_ids(url, COLLECTION_OLX)
        if not ids:
            return None
        docs = self._olx_repo.get_by_ids(ids)
        if not docs:
            return None
        return OlxListing(docs[0])

    def get_olx_listings_by_ids(self, ids: List[str]) -> OlxListingCollection:
        """Отримує колекцію оголошень OLX за списком ідентифікаторів."""
        parsed = self._parse_ids_list(ids, COLLECTION_OLX)
        if not parsed:
            return OlxListingCollection.from_raw_list([])
        docs = self._olx_repo.get_by_ids(parsed)
        return OlxListingCollection.from_raw_list(docs)

    def get_listing_collection_by_ids(
        self, ids: List[str], collection: str
    ):
        """
        Універсальний метод: отримує колекцію за ids та назвою колекції.
        Повертає UnifiedListingCollection, ProzorroAuctionCollection або OlxListingCollection.
        """
        if not ids:
            return self._empty_collection(collection)
        parsed = self._parse_ids_list(ids, collection)
        if not parsed:
            return self._empty_collection(collection)
        repo = self._get_repository(collection)
        docs = repo.get_by_ids(parsed)
        return self._docs_to_collection(docs, collection)

    def collection_from_raw_docs(
        self, docs: List[Dict[str, Any]], source_collection: str
    ):
        """
        Обгортає список сирих документів у відповідну domain-колекцію.
        Використовується для результатів execute_query / save_query_to_temp_collection.
        """
        return self._docs_to_collection(docs or [], source_collection)

    def _docs_to_collection(self, docs: List[Dict[str, Any]], collection: str):
        """Перетворює список документів у domain-колекцію."""
        if collection == COLLECTION_UNIFIED:
            return UnifiedListingCollection.from_raw_list(docs)
        if collection == COLLECTION_PROZORRO:
            return ProzorroAuctionCollection.from_raw_list(docs)
        if collection == COLLECTION_OLX:
            return OlxListingCollection.from_raw_list(docs)
        raise ValueError(f"Непідтримувана колекція: {collection}")

    def _empty_collection(self, collection: str):
        """Повертає порожню колекцію за типом."""
        if collection == COLLECTION_UNIFIED:
            return UnifiedListingCollection.from_raw_list([])
        if collection == COLLECTION_PROZORRO:
            return ProzorroAuctionCollection.from_raw_list([])
        if collection == COLLECTION_OLX:
            return OlxListingCollection.from_raw_list([])
        raise ValueError(f"Непідтримувана колекція: {collection}")

    def _parse_ids(self, id_or_composite: str, collection: str) -> List[str]:
        """Парсить один ідентифікатор у список для get_by_ids."""
        s = (id_or_composite or "").strip()
        if not s:
            return []
        return [s]

    def _parse_ids_list(self, ids: List[str], collection: str) -> List[str]:
        """Фільтрує та нормалізує список ідентифікаторів."""
        result = []
        for i in ids or []:
            s = (i or "").strip()
            if s:
                result.append(s)
        return result

    def get_raw_source_document_for_listing(
        self, listing: "UnifiedListing"
    ) -> Optional[Dict[str, Any]]:
        """
        Отримує сирі дані з джерела (OLX або ProZorro) для зведеного оголошення.
        Повертає повний документ з відповідної колекції або None.
        """
        source = listing.source
        source_id = listing.source_id
        if not source or not source_id:
            return None
        if source == "olx":
            return self._olx_repo.find_by_url(source_id)
        if source == "prozorro":
            return self._prozorro_repo.find_by_auction_id(source_id)
        return None
