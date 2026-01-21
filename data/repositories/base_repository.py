# -*- coding: utf-8 -*-
"""
Базовий репозиторій для роботи з MongoDB.
"""

from typing import List, Dict, Any, Optional
from pymongo.collection import Collection
from pymongo.database import Database
from pymongo.errors import DuplicateKeyError
from bson import ObjectId
from data.database.connection import MongoDBConnection


class BaseRepository:
    """Базовий клас для репозиторіїв MongoDB."""
    
    def __init__(self, collection_name: str):
        """
        Ініціалізація репозиторію.
        
        Args:
            collection_name: Назва колекції в MongoDB
        """
        self.collection_name = collection_name
        self._collection: Optional[Collection] = None
    
    @property
    def collection(self) -> Collection:
        """
        Отримує об'єкт колекції MongoDB.
        
        Returns:
            Об'єкт колекції MongoDB
        """
        if self._collection is None:
            database = MongoDBConnection.get_database()
            self._collection = database[self.collection_name]
        return self._collection
    
    def create(self, document: Dict[str, Any]) -> str:
        """
        Створює новий документ у колекції.
        
        Args:
            document: Словник з даними документа
            
        Returns:
            ID створеного документа
            
        Raises:
            DuplicateKeyError: Якщо документ з таким ключем вже існує
        """
        result = self.collection.insert_one(document)
        return str(result.inserted_id)
    
    def create_many(self, documents: List[Dict[str, Any]]) -> List[str]:
        """
        Створює кілька документів у колекції.
        
        Args:
            documents: Список словників з даними документів
            
        Returns:
            Список ID створених документів
        """
        if not documents:
            return []
        
        result = self.collection.insert_many(documents)
        return [str(id) for id in result.inserted_ids]
    
    def find_by_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """
        Знаходить документ за ID.
        
        Args:
            document_id: ID документа (може бути строкою або ObjectId)
            
        Returns:
            Документ або None, якщо не знайдено
        """
        try:
            obj_id = ObjectId(document_id) if isinstance(document_id, str) else document_id
            document = self.collection.find_one({"_id": obj_id})
            if document:
                document["_id"] = str(document["_id"])
            return document
        except Exception:
            return None
    
    def find_one(self, filter: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """
        Знаходить один документ за фільтром.
        
        Args:
            filter: Словник з умовами пошуку
            
        Returns:
            Документ або None, якщо не знайдено
        """
        document = self.collection.find_one(filter)
        if document:
            document["_id"] = str(document["_id"])
        return document
    
    def find_many(
        self,
        filter: Optional[Dict[str, Any]] = None,
        sort: Optional[List[tuple]] = None,
        limit: Optional[int] = None,
        skip: Optional[int] = None
    ) -> List[Dict[str, Any]]:
        """
        Знаходить кілька документів за фільтром.
        
        Args:
            filter: Словник з умовами пошуку (за замовчуванням порожній)
            sort: Список кортежів для сортування, наприклад [("field", 1)] (1 - зростання, -1 - спадання)
            limit: Максимальна кількість документів
            skip: Кількість документів для пропуску
            
        Returns:
            Список знайдених документів
        """
        if filter is None:
            filter = {}
        
        query = self.collection.find(filter)
        
        if sort:
            query = query.sort(sort)
        
        if skip:
            query = query.skip(skip)
        
        if limit:
            query = query.limit(limit)
        
        documents = list(query)
        for doc in documents:
            doc["_id"] = str(doc["_id"])
        
        return documents
    
    def update_by_id(
        self,
        document_id: str,
        update_data: Dict[str, Any],
        upsert: bool = False
    ) -> bool:
        """
        Оновлює документ за ID.
        
        Args:
            document_id: ID документа
            update_data: Словник з даними для оновлення (використовуйте $set, $unset тощо)
            upsert: Якщо True, створює документ, якщо він не існує
            
        Returns:
            True, якщо документ оновлено/створено, False якщо не знайдено
        """
        try:
            obj_id = ObjectId(document_id) if isinstance(document_id, str) else document_id
            result = self.collection.update_one(
                {"_id": obj_id},
                update_data,
                upsert=upsert
            )
            return result.modified_count > 0 or (upsert and result.upserted_id is not None)
        except Exception:
            return False
    
    def update_many(
        self,
        filter: Dict[str, Any],
        update_data: Dict[str, Any],
        upsert: bool = False
    ) -> int:
        """
        Оновлює кілька документів за фільтром.
        
        Args:
            filter: Словник з умовами пошуку
            update_data: Словник з даними для оновлення
            upsert: Якщо True, створює документ, якщо він не існує
            
        Returns:
            Кількість оновлених документів
        """
        result = self.collection.update_many(filter, update_data, upsert=upsert)
        return result.modified_count
    
    def delete_by_id(self, document_id: str) -> bool:
        """
        Видаляє документ за ID.
        
        Args:
            document_id: ID документа
            
        Returns:
            True, якщо документ видалено, False якщо не знайдено
        """
        try:
            obj_id = ObjectId(document_id) if isinstance(document_id, str) else document_id
            result = self.collection.delete_one({"_id": obj_id})
            return result.deleted_count > 0
        except Exception:
            return False
    
    def delete_many(self, filter: Dict[str, Any]) -> int:
        """
        Видаляє кілька документів за фільтром.
        
        Args:
            filter: Словник з умовами пошуку
            
        Returns:
            Кількість видалених документів
        """
        result = self.collection.delete_many(filter)
        return result.deleted_count
    
    def count(self, filter: Optional[Dict[str, Any]] = None) -> int:
        """
        Підраховує кількість документів за фільтром.
        
        Args:
            filter: Словник з умовами пошуку (за замовчуванням порожній)
            
        Returns:
            Кількість документів
        """
        if filter is None:
            filter = {}
        return self.collection.count_documents(filter)
    
    def exists(self, filter: Dict[str, Any]) -> bool:
        """
        Перевіряє, чи існує хоча б один документ за фільтром.
        
        Args:
            filter: Словник з умовами пошуку
            
        Returns:
            True, якщо документ знайдено, False інакше
        """
        return self.collection.count_documents(filter, limit=1) > 0
