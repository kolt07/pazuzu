# -*- coding: utf-8 -*-
"""
Data Transfer Objects для роботи з API ProZorro.
"""

from dataclasses import dataclass
from typing import Optional, List, Dict, Any
from datetime import datetime, timezone


@dataclass
class ClassificationDTO:
    """DTO для класифікації тендеру."""
    id: str
    scheme: str
    description: str
    description_ua: Optional[str] = None


@dataclass
class TenderDTO:
    """DTO для тендеру з ProZorro."""
    id: str
    date_created: datetime
    date_modified: datetime
    status: str
    title: str
    procurement_method_type: Optional[str] = None
    title_ua: Optional[str] = None
    description: Optional[str] = None
    classification: Optional[ClassificationDTO] = None
    data: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TenderDTO':
        """Створює TenderDTO зі словника."""
        classification = None
        if 'classification' in data:
            cls_data = data['classification']
            classification = ClassificationDTO(
                id=cls_data.get('id', ''),
                scheme=cls_data.get('scheme', ''),
                description=cls_data.get('description', ''),
                description_ua=cls_data.get('description_ua')
            )

        # Парсинг dateCreated з підтримкою різних форматів
        date_created = datetime.now(timezone.utc)
        if 'dateCreated' in data:
            date_str = data['dateCreated']
            try:
                # Замінюємо 'Z' на '+00:00' для сумісності
                if date_str.endswith('Z'):
                    date_str = date_str.replace('Z', '+00:00')
                date_created = datetime.fromisoformat(date_str)
                # Конвертуємо в UTC якщо є timezone
                if date_created.tzinfo:
                    date_created = date_created.astimezone(timezone.utc)
                else:
                    date_created = date_created.replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError) as e:
                print(f"Помилка парсингу dateCreated '{date_str}': {e}")
                date_created = datetime.now(timezone.utc)

        # Парсинг dateModified з підтримкою різних форматів
        date_modified = datetime.now(timezone.utc)
        if 'dateModified' in data:
            date_str = data['dateModified']
            try:
                # Замінюємо 'Z' на '+00:00' для сумісності
                if date_str.endswith('Z'):
                    date_str = date_str.replace('Z', '+00:00')
                date_modified = datetime.fromisoformat(date_str)
                # Конвертуємо в UTC якщо є timezone
                if date_modified.tzinfo:
                    date_modified = date_modified.astimezone(timezone.utc)
                else:
                    date_modified = date_modified.replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError) as e:
                print(f"Помилка парсингу dateModified '{date_str}': {e}")
                date_modified = datetime.now(timezone.utc)

        return cls(
            id=data.get('id', ''),
            date_created=date_created,
            date_modified=date_modified,
            status=data.get('status', ''),
            title=data.get('title', ''),
            procurement_method_type=data.get('procurementMethodType'),
            title_ua=data.get('title_ua'),
            description=data.get('description'),
            classification=classification,
            data=data
        )


@dataclass
class TendersResponseDTO:
    """DTO для відповіді API зі списком тендерів."""
    data: List[TenderDTO]
    next_page: Optional[str] = None
    prev_page: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'TendersResponseDTO':
        """Створює TendersResponseDTO зі словника."""
        tenders = []
        if 'data' in data:
            for tender_data in data['data']:
                tenders.append(TenderDTO.from_dict(tender_data))

        return cls(
            data=tenders,
            next_page=data.get('next_page'),
            prev_page=data.get('prev_page')
        )


@dataclass
class AuctionDTO:
    """DTO для аукціону з ProZorro.Sale."""
    id: str
    date_created: datetime
    date_modified: datetime
    status: str
    title: Optional[str] = None
    title_ua: Optional[str] = None
    description: Optional[str] = None
    procedure_type: Optional[str] = None
    data: Optional[Dict[str, Any]] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AuctionDTO':
        """Створює AuctionDTO зі словника."""
        # Парсинг dateCreated з підтримкою різних форматів
        date_created = datetime.now(timezone.utc)
        if 'dateCreated' in data:
            date_str = data['dateCreated']
            try:
                # Замінюємо 'Z' на '+00:00' для сумісності
                if date_str.endswith('Z'):
                    date_str = date_str.replace('Z', '+00:00')
                date_created = datetime.fromisoformat(date_str)
                # Конвертуємо в UTC якщо є timezone
                if date_created.tzinfo:
                    date_created = date_created.astimezone(timezone.utc)
                else:
                    date_created = date_created.replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError) as e:
                print(f"Помилка парсингу dateCreated '{date_str}': {e}")
                date_created = datetime.now(timezone.utc)

        # Парсинг dateModified з підтримкою різних форматів
        date_modified = datetime.now(timezone.utc)
        if 'dateModified' in data:
            date_str = data['dateModified']
            try:
                # Замінюємо 'Z' на '+00:00' для сумісності
                if date_str.endswith('Z'):
                    date_str = date_str.replace('Z', '+00:00')
                date_modified = datetime.fromisoformat(date_str)
                # Конвертуємо в UTC якщо є timezone
                if date_modified.tzinfo:
                    date_modified = date_modified.astimezone(timezone.utc)
                else:
                    date_modified = date_modified.replace(tzinfo=timezone.utc)
            except (ValueError, AttributeError) as e:
                print(f"Помилка парсингу dateModified '{date_str}': {e}")
                date_modified = datetime.now(timezone.utc)

        # Спробуємо знайти ID в різних місцях
        auction_id = data.get('id') or data.get('_id') or ''
        # Якщо ID все ще порожній, спробуємо знайти в data
        if not auction_id and isinstance(data.get('data'), dict):
            auction_id = data['data'].get('id') or data['data'].get('_id') or ''
        
        return cls(
            id=auction_id,
            date_created=date_created,
            date_modified=date_modified,
            status=data.get('status', ''),
            title=data.get('title'),
            title_ua=data.get('title_ua'),
            description=data.get('description'),
            procedure_type=data.get('procedureType') or data.get('procedure_type'),
            data=data
        )


@dataclass
class AuctionsResponseDTO:
    """DTO для відповіді API зі списком аукціонів."""
    data: List[AuctionDTO]
    next_page: Optional[str] = None
    prev_page: Optional[str] = None

    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'AuctionsResponseDTO':
        """Створює AuctionsResponseDTO зі словника."""
        auctions = []
        if 'data' in data:
            for auction_data in data['data']:
                auctions.append(AuctionDTO.from_dict(auction_data))

        return cls(
            data=auctions,
            next_page=data.get('next_page'),
            prev_page=data.get('prev_page')
        )