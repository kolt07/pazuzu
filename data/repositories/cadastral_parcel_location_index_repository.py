# -*- coding: utf-8 -*-
"""
Репозиторій індексної колекції для пошуку ділянок за топографічною прив'язкою
(область, район, місто) з кадастрового номера.
"""

from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "cadastral_parcel_location_index"


class CadastralParcelLocationIndexRepository(BaseRepository):
    """Робота з індексом місцезнаходження ділянок за кадастровим номером."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def upsert(self, cadastral_number: str, location_data: Dict[str, Any]) -> bool:
        """
        Створює або оновлює запис індексу для ділянки.

        Args:
            cadastral_number: кадастровий номер
            location_data: словник з oblast_code, oblast_name, koatuu_prefix, district_code,
                          city_code, zone, quarter, parcel

        Returns:
            True якщо успішно
        """
        if not cadastral_number or not str(cadastral_number).strip():
            return False
        cn = str(cadastral_number).strip()
        doc = {"cadastral_number": cn, **{k: v for k, v in location_data.items() if v is not None}}
        result = self.collection.update_one(
            {"cadastral_number": cn},
            {"$set": doc},
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def find_by_cadastral_number(self, cadastral_number: str) -> Optional[Dict[str, Any]]:
        """Знаходить запис індексу за кадастровим номером."""
        if not cadastral_number:
            return None
        doc = self.collection.find_one({"cadastral_number": str(cadastral_number).strip()})
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def find_by_oblast(self, oblast_code: Optional[str] = None, oblast_name: Optional[str] = None) -> List[Dict[str, Any]]:
        """
        Пошук ділянок за областю (код або назва).
        """
        flt: Dict[str, Any] = {}
        if oblast_code:
            flt["oblast_code"] = str(oblast_code).strip()
        elif oblast_name:
            flt["oblast_name"] = {"$regex": str(oblast_name).strip(), "$options": "i"}
        if not flt:
            return []
        docs = list(self.collection.find(flt))
        for d in docs:
            if "_id" in d:
                d["_id"] = str(d["_id"])
        return docs

    def find_by_koatuu_prefix(self, koatuu_prefix: str) -> List[Dict[str, Any]]:
        """Пошук за префіксом КОАТУУ (напр. 63101 для Харкова)."""
        if not koatuu_prefix:
            return []
        prefix = str(koatuu_prefix).strip()
        docs = list(self.collection.find({"koatuu_prefix": {"$regex": f"^{prefix}"}}))
        for d in docs:
            if "_id" in d:
                d["_id"] = str(d["_id"])
        return docs

    def ensure_index(self) -> None:
        """Створює індекси для швидкого пошуку."""
        self.collection.create_index("cadastral_number", unique=True)
        self.collection.create_index("oblast_code")
        self.collection.create_index("oblast_name")
        self.collection.create_index("koatuu_prefix")

    def count_total(self) -> int:
        """Загальна кількість записів у індексі."""
        return self.collection.count_documents({})
