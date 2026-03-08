# -*- coding: utf-8 -*-
"""
Репозиторії для роботи з географічними даними (області, міста, вулиці, будинки).
"""

from typing import Optional, Dict, Any, List
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository


class RegionsRepository(BaseRepository):
    """Репозиторій для роботи з областями."""
    
    def __init__(self):
        super().__init__("regions")
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index("name", unique=True)
            self.collection.create_index("name_normalized")
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_name(self, name: str) -> Optional[Dict[str, Any]]:
        """Знаходить область за назвою."""
        self._ensure_indexes()
        normalized = self._normalize_name(name)
        return self.find_one({"name_normalized": normalized})
    
    def find_or_create(self, name: str) -> Dict[str, Any]:
        """Знаходить або створює область."""
        self._ensure_indexes()
        normalized = self._normalize_name(name)
        
        existing = self.find_one({"name_normalized": normalized})
        if existing:
            return existing
        
        doc = {
            "name": name.strip(),
            "name_normalized": normalized,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        doc_id = self.create(doc)
        # Отримуємо створений документ
        created_doc = self.find_by_id(doc_id)
        return created_doc if created_doc else doc
    
    def get_all(self) -> List[Dict[str, Any]]:
        """Отримує всі області, відсортовані за назвою."""
        self._ensure_indexes()
        return self.find_many(sort=[("name", 1)])
    
    @staticmethod
    def _normalize_name(name: str) -> str:
        """Нормалізує назву області для пошуку."""
        if not name:
            return ""
        # Прибираємо " область", " обл." та приводимо до нижнього регістру
        normalized = name.lower().strip()
        normalized = normalized.replace(" область", "").replace(" обл.", "").strip()
        return normalized


class CitiesRepository(BaseRepository):
    """Репозиторій для роботи з містами."""
    
    def __init__(self):
        super().__init__("cities")
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("region_id", 1), ("name_normalized", 1)], unique=True)
            self.collection.create_index("region_id")
            self.collection.create_index("name_normalized")
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_name_and_region(self, name: str, region_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить місто за назвою та областю."""
        self._ensure_indexes()
        normalized = self._normalize_name(name)
        return self.find_one({
            "region_id": region_id,
            "name_normalized": normalized
        })
    
    def find_or_create(self, name: str, region_id: str) -> Dict[str, Any]:
        """Знаходить або створює місто."""
        self._ensure_indexes()
        normalized = self._normalize_name(name)
        
        existing = self.find_by_name_and_region(name, region_id)
        if existing:
            return existing
        
        doc = {
            "name": name.strip(),
            "name_normalized": normalized,
            "region_id": region_id,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        doc_id = self.create(doc)
        # Отримуємо створений документ
        created_doc = self.find_by_id(doc_id)
        return created_doc if created_doc else doc
    
    def get_by_region(self, region_id: str) -> List[Dict[str, Any]]:
        """Отримує всі міста в області."""
        self._ensure_indexes()
        return self.find_many(
            filter={"region_id": region_id},
            sort=[("name", 1)]
        )
    
    @staticmethod
    def _normalize_name(name: str) -> str:
        """Нормалізує назву міста для пошуку."""
        if not name:
            return ""
        # Прибираємо префікси типу "м.", "с." та приводимо до нижнього регістру
        normalized = name.lower().strip()
        normalized = normalized.replace("м.", "").replace("с.", "").replace("смт.", "").strip()
        return normalized


class StreetsRepository(BaseRepository):
    """Репозиторій для роботи з вулицями."""
    
    def __init__(self):
        super().__init__("streets")
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("city_id", 1), ("name_normalized", 1)], unique=True)
            self.collection.create_index("city_id")
            self.collection.create_index("name_normalized")
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_name_and_city(self, name: str, city_id: str, street_type: Optional[str] = None) -> Optional[Dict[str, Any]]:
        """Знаходить вулицю за назвою та містом."""
        self._ensure_indexes()
        normalized = self._normalize_name(name, street_type)
        return self.find_one({
            "city_id": city_id,
            "name_normalized": normalized
        })
    
    def find_or_create(self, name: str, city_id: str, street_type: Optional[str] = None) -> Dict[str, Any]:
        """Знаходить або створює вулицю."""
        self._ensure_indexes()
        normalized = self._normalize_name(name, street_type)
        
        existing = self.find_by_name_and_city(name, city_id, street_type)
        if existing:
            return existing
        
        doc = {
            "name": name.strip(),
            "name_normalized": normalized,
            "street_type": street_type.strip() if street_type else None,
            "city_id": city_id,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        doc_id = self.create(doc)
        # Отримуємо створений документ
        created_doc = self.find_by_id(doc_id)
        return created_doc if created_doc else doc
    
    def get_by_city(self, city_id: str) -> List[Dict[str, Any]]:
        """Отримує всі вулиці в місті."""
        self._ensure_indexes()
        return self.find_many(
            filter={"city_id": city_id},
            sort=[("name", 1)]
        )
    
    @staticmethod
    def _normalize_name(name: str, street_type: Optional[str] = None) -> str:
        """Нормалізує назву вулиці для пошуку."""
        if not name:
            return ""
        normalized = name.lower().strip()
        # Якщо є тип вулиці, додаємо його до нормалізованої назви
        if street_type:
            normalized = street_type.lower().strip() + " " + normalized
        return normalized


class BuildingsRepository(BaseRepository):
    """Репозиторій для роботи з будинками."""
    
    def __init__(self):
        super().__init__("buildings")
        self._indexes_created = False
    
    def _ensure_indexes(self):
        """Створює індекси, якщо вони ще не створені."""
        if self._indexes_created:
            return
        try:
            self.collection.create_index([("street_id", 1), ("number", 1)], unique=True)
            self.collection.create_index("street_id")
            self._indexes_created = True
        except Exception:
            pass
    
    def find_by_number_and_street(self, number: str, street_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить будинок за номером та вулицею."""
        self._ensure_indexes()
        return self.find_one({
            "street_id": street_id,
            "number": number.strip()
        })
    
    def find_or_create(self, number: str, street_id: str, building_part: Optional[str] = None) -> Dict[str, Any]:
        """Знаходить або створює будинок."""
        self._ensure_indexes()
        
        existing = self.find_by_number_and_street(number, street_id)
        if existing:
            # Оновлюємо building_part якщо він не був встановлений
            if building_part and not existing.get("building_part"):
                self.update_by_id(existing["_id"], {
                    "$set": {
                        "building_part": building_part.strip(),
                        "updated_at": datetime.now(timezone.utc)
                    }
                })
                existing["building_part"] = building_part.strip()
            return existing
        
        doc = {
            "number": number.strip(),
            "building_part": building_part.strip() if building_part else None,
            "street_id": street_id,
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc)
        }
        doc_id = self.create(doc)
        # Отримуємо створений документ
        created_doc = self.find_by_id(doc_id)
        return created_doc if created_doc else doc
    
    def get_by_street(self, street_id: str) -> List[Dict[str, Any]]:
        """Отримує всі будинки на вулиці."""
        self._ensure_indexes()
        return self.find_many(
            filter={"street_id": street_id},
            sort=[("number", 1)]
        )
