# -*- coding: utf-8 -*-
"""
Репозиторій для роботи з колекцією prozorro_auctions.
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class ProZorroAuctionsRepository(BaseRepository):
    """Репозиторій для роботи з аукціонами ProZorro."""
    
    def __init__(self):
        """Ініціалізація репозиторію."""
        super().__init__('prozorro_auctions')
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index('auction_id', unique=True)
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_auction_id(self, auction_id: str) -> Optional[Dict[str, Any]]:
        """
        Знаходить аукціон за ідентифікатором.
        
        Args:
            auction_id: Ідентифікатор аукціону
            
        Returns:
            Документ або None, якщо не знайдено
        """
        self._ensure_indexes()
        return self.find_one({'auction_id': auction_id})
    
    def upsert_auction(
        self,
        auction_id: str,
        auction_data: Dict[str, Any],
        version_hash: str,
        description_hash: Optional[str],
        last_updated: datetime
    ) -> bool:
        """
        Створює або оновлює аукціон.
        
        Args:
            auction_id: Ідентифікатор аукціону
            auction_data: Повні дані аукціону
            version_hash: Хеш версії об'єкта
            description_hash: Хеш опису (опціонально)
            last_updated: Дата останнього оновлення
            
        Returns:
            True якщо успішно
        """
        document = {
            'auction_id': auction_id,
            'auction_data': auction_data,
            'version_hash': version_hash,
            'description_hash': description_hash,
            'last_updated': last_updated,
            'created_at': last_updated  # Для нових записів
        }
        
        # Оновлюємо або створюємо
        existing = self.find_by_auction_id(auction_id)
        if existing:
            # Оновлюємо існуючий
            update_data = {
                '$set': {
                    'auction_data': auction_data,
                    'version_hash': version_hash,
                    'description_hash': description_hash,
                    'last_updated': last_updated
                }
            }
            return self.update_by_id(existing['_id'], update_data)
        else:
            # Створюємо новий
            document['created_at'] = last_updated
            self.create(document)
            return True
    
    def get_auctions_by_version_hash(self, version_hash: str) -> list:
        """
        Знаходить аукціони за хешем версії.
        
        Args:
            version_hash: Хеш версії об'єкта
            
        Returns:
            Список документів
        """
        return self.find_many({'version_hash': version_hash})
    
    def get_auctions_by_description_hash(self, description_hash: str, exclude_auction_id: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Знаходить аукціони за хешем опису.
        
        Args:
            description_hash: Хеш опису
            exclude_auction_id: Ідентифікатор аукціону, який потрібно виключити з результату
            
        Returns:
            Список документів
        """
        self._ensure_indexes()
        filter_dict = {'description_hash': description_hash}
        if exclude_auction_id:
            filter_dict['auction_id'] = {'$ne': exclude_auction_id}
        return self.find_many(filter_dict)
    
    def get_auctions_by_date_range(self, date_from: datetime, date_to: datetime) -> List[Dict[str, Any]]:
        """
        Отримує аукціони, які були створені або змінені в діапазоні дат.
        
        Args:
            date_from: Початкова дата діапазону
            date_to: Кінцева дата діапазону
            
        Returns:
            Список документів з аукціонами
        """
        self._ensure_indexes()
        
        # Переконуємося, що дати мають timezone
        if date_from.tzinfo is None:
            date_from = date_from.replace(tzinfo=timezone.utc)
        if date_to.tzinfo is None:
            date_to = date_to.replace(tzinfo=timezone.utc)
        
        # Отримуємо всі аукціони з БД
        all_auctions = self.find_many()
        
        # Фільтруємо за датою створення або модифікації
        filtered_auctions = []
        for auction in all_auctions:
            auction_data = auction.get('auction_data', {})
            if not auction_data:
                continue
            
            # Витягуємо дати з auction_data
            date_created_str = auction_data.get('dateCreated', '')
            date_modified_str = auction_data.get('dateModified', '')
            
            date_created = None
            date_modified = None
            
            # Парсимо дату створення
            if date_created_str:
                try:
                    if date_created_str.endswith('Z'):
                        date_created_str = date_created_str.replace('Z', '+00:00')
                    date_created = datetime.fromisoformat(date_created_str)
                    if date_created.tzinfo:
                        date_created = date_created.astimezone(timezone.utc)
                    else:
                        date_created = date_created.replace(tzinfo=timezone.utc)
                except (ValueError, AttributeError):
                    pass
            
            # Парсимо дату модифікації
            if date_modified_str:
                try:
                    if date_modified_str.endswith('Z'):
                        date_modified_str = date_modified_str.replace('Z', '+00:00')
                    date_modified = datetime.fromisoformat(date_modified_str)
                    if date_modified.tzinfo:
                        date_modified = date_modified.astimezone(timezone.utc)
                    else:
                        date_modified = date_modified.replace(tzinfo=timezone.utc)
                except (ValueError, AttributeError):
                    pass
            
            # Перевіряємо, чи хоча б одна з дат входить у діапазон
            created_in_range = date_created and (date_from <= date_created <= date_to)
            modified_in_range = date_modified and (date_from <= date_modified <= date_to)
            
            if created_in_range or modified_in_range:
                filtered_auctions.append(auction)
        
        return filtered_auctions
    
    def get_active_auctions(self) -> List[Dict[str, Any]]:
        """
        Отримує всі активні аукціони з бази даних.
        
        Активними вважаються аукціони зі статусами, що починаються з 'active'.
        
        Returns:
            Список документів з активними аукціонами
        """
        self._ensure_indexes()
        all_auctions = self.find_many()
        
        active_statuses = ['active', 'active.tendering', 'active.auction', 'active.qualification', 
                          'active_rectification', 'active_tendering', 'active_auction', 'active_qualification']
        
        active_auctions = []
        for auction in all_auctions:
            auction_data = auction.get('auction_data', {})
            if not auction_data:
                continue
            
            status = auction_data.get('status', '')
            is_active = any(
                status.startswith(active_status.replace('_', '.')) or 
                status == active_status or
                status.startswith(active_status.replace('.', '_'))
                for active_status in active_statuses
            )
            
            if is_active:
                active_auctions.append(auction)
        
        return active_auctions
