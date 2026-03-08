# -*- coding: utf-8 -*-
"""
Сервіс для роботи з географічними даними (області, міста, вулиці, будинки).
Забезпечує нормалізацію та збереження топонімів з посиланнями.
"""

from typing import Optional, Dict, Any, List
from data.repositories.geography_repository import (
    RegionsRepository,
    CitiesRepository,
    StreetsRepository,
    BuildingsRepository
)


class GeographyService:
    """Сервіс для роботи з географічними даними."""
    
    def __init__(self):
        self.regions_repo = RegionsRepository()
        self.cities_repo = CitiesRepository()
        self.streets_repo = StreetsRepository()
        self.buildings_repo = BuildingsRepository()
    
    def resolve_address(self, address_data: Dict[str, Any]) -> Dict[str, Any]:
        """
        Розв'язує адресу, створюючи або знаходячи посилання на топоніми.
        
        Args:
            address_data: Словник з полями:
                - region: назва області
                - settlement/city: назва міста/населеного пункту
                - street_type: тип вулиці (опціонально)
                - street: назва вулиці
                - building: номер будинку
                - building_part: частина будинку (опціонально)
        
        Returns:
            Словник з посиланнями:
                - region_id: ID області
                - city_id: ID міста
                - street_id: ID вулиці (якщо є)
                - building_id: ID будинку (якщо є)
                - address_refs: повна структура посилань
        """
        result = {
            "region_id": None,
            "city_id": None,
            "street_id": None,
            "building_id": None,
            "address_refs": {}
        }
        
        # Область
        region_name = address_data.get("region") or address_data.get("address_region")
        if region_name:
            region = self.regions_repo.find_or_create(region_name)
            result["region_id"] = str(region["_id"])
            result["address_refs"]["region"] = {
                "_id": str(region["_id"]),
                "name": region["name"]
            }
        
        # Місто/населений пункт
        city_name = (
            address_data.get("settlement") or 
            address_data.get("city") or 
            address_data.get("address_city")
        )
        if city_name and result["region_id"]:
            city = self.cities_repo.find_or_create(city_name, result["region_id"])
            result["city_id"] = str(city["_id"])
            result["address_refs"]["city"] = {
                "_id": str(city["_id"]),
                "name": city["name"]
            }
        
        # Вулиця
        street_name = address_data.get("street")
        street_type = address_data.get("street_type")
        if street_name and result["city_id"]:
            street = self.streets_repo.find_or_create(
                street_name,
                result["city_id"],
                street_type
            )
            result["street_id"] = str(street["_id"])
            result["address_refs"]["street"] = {
                "_id": str(street["_id"]),
                "name": street["name"],
                "street_type": street.get("street_type")
            }
        
        # Будинок
        building_number = address_data.get("building") or address_data.get("building_number")
        building_part = address_data.get("building_part")
        if building_number and result["street_id"]:
            building = self.buildings_repo.find_or_create(
                building_number,
                result["street_id"],
                building_part
            )
            result["building_id"] = str(building["_id"])
            result["address_refs"]["building"] = {
                "_id": str(building["_id"]),
                "number": building["number"],
                "building_part": building.get("building_part")
            }
        
        return result
    
    def get_region_by_id(self, region_id: str) -> Optional[Dict[str, Any]]:
        """Отримує область за ID."""
        return self.regions_repo.find_by_id(region_id)
    
    def get_city_by_id(self, city_id: str) -> Optional[Dict[str, Any]]:
        """Отримує місто за ID."""
        return self.cities_repo.find_by_id(city_id)
    
    def get_street_by_id(self, street_id: str) -> Optional[Dict[str, Any]]:
        """Отримує вулицю за ID."""
        return self.streets_repo.find_by_id(street_id)
    
    def get_building_by_id(self, building_id: str) -> Optional[Dict[str, Any]]:
        """Отримує будинок за ID."""
        return self.buildings_repo.find_by_id(building_id)
    
    def get_all_regions(self) -> List[Dict[str, Any]]:
        """Отримує всі області."""
        return self.regions_repo.get_all()
    
    def get_cities_by_region(self, region_id: str) -> List[Dict[str, Any]]:
        """Отримує всі міста в області."""
        return self.cities_repo.get_by_region(region_id)
    
    def get_streets_by_city(self, city_id: str) -> List[Dict[str, Any]]:
        """Отримує всі вулиці в місті."""
        return self.streets_repo.get_by_city(city_id)
    
    def format_address(self, address_refs: Dict[str, Any]) -> str:
        """
        Форматує адресу з посилань у читабельний рядок.
        
        Args:
            address_refs: Словник з посиланнями (результат resolve_address)
        
        Returns:
            Відформатована адреса
        """
        parts = []
        
        if address_refs.get("region"):
            parts.append(address_refs["region"]["name"])
        
        if address_refs.get("city"):
            city_name = address_refs["city"]["name"]
            parts.append(city_name)
        
        if address_refs.get("street"):
            street = address_refs["street"]
            street_name = street["name"]
            if street.get("street_type"):
                street_name = street["street_type"] + " " + street_name
            parts.append(street_name)
        
        if address_refs.get("building"):
            building = address_refs["building"]
            building_str = building["number"]
            if building.get("building_part"):
                building_str += "/" + building["building_part"]
            parts.append(building_str)
        
        return ", ".join(parts)
