# -*- coding: utf-8 -*-
"""
Domain-сутності для колекцій об'єктів.
Ізольована робота з масивами оголошень без прямого доступу до БД.
"""

from typing import (
    Any,
    Callable,
    Dict,
    Generic,
    Iterator,
    List,
    Optional,
    TypeVar,
)

from domain.entities.base import BaseEntity
from domain.entities.unified_listing import UnifiedListing
from domain.entities.prozorro_auction import ProzorroAuction
from domain.entities.olx_listing import OlxListing


T = TypeVar("T", bound=BaseEntity)


class ListingCollectionBase(Generic[T]):
    """
    Базова сутність для колекції domain-об'єктів.
    Надає методи для зручної роботи з масивами без доступу до БД.
    """

    def __init__(self, items: List[T], source_collection: str):
        """
        Args:
            items: Список domain-сутностей.
            source_collection: Назва колекції-джерела.
        """
        self._items = list(items)
        self._source_collection = source_collection

    def __len__(self) -> int:
        return len(self._items)

    def __iter__(self) -> Iterator[T]:
        return iter(self._items)

    def __getitem__(self, index: int) -> T:
        return self._items[index]

    def count(self) -> int:
        """Кількість елементів у колекції."""
        return len(self._items)

    def to_list(self) -> List[T]:
        """Повертає копію списку сутностей."""
        return list(self._items)

    def filter(self, predicate: Callable[[T], bool]) -> "ListingCollectionBase[T]":
        """Фільтрує колекцію за предикатом. Повертає нову колекцію."""
        filtered = [item for item in self._items if predicate(item)]
        return self.__class__(filtered)

    def sort_by(
        self,
        key: Callable[[T], Any],
        reverse: bool = False,
    ) -> "ListingCollectionBase[T]":
        """Сортує колекцію за ключем. Повертає нову колекцію."""
        sorted_items = sorted(self._items, key=key, reverse=reverse)
        return self.__class__(sorted_items)

    def limit(self, n: int) -> "ListingCollectionBase[T]":
        """Обмежує кількість елементів. Повертає нову колекцію."""
        return self.__class__(self._items[:n])

    def take(self, n: int) -> List[T]:
        """Повертає перші n елементів."""
        return self._items[:n]

    def get_ids(self) -> List[str]:
        """Повертає список ідентифікаторів (реалізація в підкласах)."""
        return []

    def to_raw_list(self) -> List[Dict[str, Any]]:
        """Повертає список сирих даних (для сумісності з існуючим кодом)."""
        return [item.get_raw_data() for item in self._items]

    def to_export_rows(self, fields: Optional[List[str]] = None) -> List[Dict[str, Any]]:
        """Повертає список рядків для експорту."""
        return [item.to_export_row(fields) for item in self._items]

    @property
    def source_collection(self) -> str:
        """Назва колекції-джерела."""
        return self._source_collection


class UnifiedListingCollection(ListingCollectionBase[UnifiedListing]):
    """Колекція зведених оголошень."""

    def __init__(self, items: List[UnifiedListing]):
        super().__init__(items, "unified_listings")

    @classmethod
    def from_raw_list(cls, raw_docs: List[Dict[str, Any]]) -> "UnifiedListingCollection":
        """Створює колекцію зі списку сирих документів."""
        items = [UnifiedListing(d) for d in (raw_docs or [])]
        return cls(items)

    def get_ids(self) -> List[str]:
        """Повертає список composite_id (source:source_id) або _id."""
        ids = []
        for item in self._items:
            cid = item.composite_id
            if cid:
                ids.append(cid)
            elif item.id:
                ids.append(item.id)
        return ids


class ProzorroAuctionCollection(ListingCollectionBase[ProzorroAuction]):
    """Колекція аукціонів ProZorro."""

    def __init__(self, items: List[ProzorroAuction]):
        super().__init__(items, "prozorro_auctions")

    @classmethod
    def from_raw_list(cls, raw_docs: List[Dict[str, Any]]) -> "ProzorroAuctionCollection":
        """Створює колекцію зі списку сирих документів."""
        items = [ProzorroAuction(d) for d in (raw_docs or [])]
        return cls(items)

    def get_ids(self) -> List[str]:
        """Повертає список auction_id або _id."""
        ids = []
        for item in self._items:
            aid = item.auction_id
            if aid:
                ids.append(aid)
            elif item.id:
                ids.append(item.id)
        return ids


class OlxListingCollection(ListingCollectionBase[OlxListing]):
    """Колекція оголошень OLX."""

    def __init__(self, items: List[OlxListing]):
        super().__init__(items, "olx_listings")

    @classmethod
    def from_raw_list(cls, raw_docs: List[Dict[str, Any]]) -> "OlxListingCollection":
        """Створює колекцію зі списку сирих документів."""
        items = [OlxListing(d) for d in (raw_docs or [])]
        return cls(items)

    def get_ids(self) -> List[str]:
        """Повертає список url або _id."""
        ids = []
        for item in self._items:
            u = item.url
            if u:
                ids.append(u)
            elif item.id:
                ids.append(item.id)
        return ids
