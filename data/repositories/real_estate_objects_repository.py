# -*- coding: utf-8 -*-
"""
Репозиторій об'єктів нерухомого майна (ОНМ).
Єдина колекція: land_plot, building, premises.
"""

import re
from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from bson import ObjectId
from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "real_estate_objects"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Повертає документ з _id у вигляді рядка."""
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    if "building_id" in doc and doc["building_id"] and hasattr(doc["building_id"], "binary"):
        doc["building_id"] = str(doc["building_id"])
    for arr_key in ("premises_ids", "land_plot_ids", "related_building_ids"):
        if arr_key in doc and doc[arr_key]:
            doc[arr_key] = [str(x) if hasattr(x, "binary") else x for x in doc[arr_key]]
    return doc


class RealEstateObjectsRepository(BaseRepository):
    """Репозиторій для роботи з об'єктами нерухомого майна."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_id(self, document_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить об'єкт за ID."""
        doc = super().find_by_id(document_id)
        return _normalize_doc(doc)

    def find_by_cadastral_number(self, cadastral_number: str) -> Optional[Dict[str, Any]]:
        """
        Знаходить земельну ділянку за кадастровим номером.

        Args:
            cadastral_number: Кадастровий номер (нормалізований)

        Returns:
            Документ land_plot або None
        """
        if not cadastral_number or not str(cadastral_number).strip():
            return None
        cn = str(cadastral_number).strip()
        doc = self.collection.find_one({
            "type": "land_plot",
            "cadastral_info.cadastral_number": cn,
        })
        return _normalize_doc(doc)

    def find_building_by_address(
        self,
        formatted_address: str,
        region: Optional[str] = None,
        settlement: Optional[str] = None,
    ) -> Optional[Dict[str, Any]]:
        """
        Знаходить будівлю за адресою.

        Args:
            formatted_address: Повна відформатована адреса
            region: Область (опціонально)
            settlement: Населений пункт (опціонально)

        Returns:
            Документ building або None
        """
        if not formatted_address or not str(formatted_address).strip():
            return None
        addr = str(formatted_address).strip()
        addr_escaped = re.escape(addr)
        # Пошук за formatted_address (точний або нечутливий до регістру)
        # Екрануємо спецсимволи regex (напр. дужки в "м. Київ (обласний центр)")
        query: Dict[str, Any] = {
            "type": "building",
            "$or": [
                {"address.formatted_address": addr},
                {"address.formatted_address": {"$regex": f"^{addr_escaped}$", "$options": "i"}},
            ],
        }
        if region and region.strip():
            query["address.region"] = {"$regex": re.escape(region.strip()), "$options": "i"}
        if settlement and settlement.strip():
            query["address.settlement"] = {"$regex": re.escape(settlement.strip()), "$options": "i"}
        doc = self.collection.find_one(query)
        return _normalize_doc(doc)

    def find_building_by_land_plot_id(
        self, land_plot_id: str
    ) -> Optional[Dict[str, Any]]:
        """
        Знаходить будівлю за пов'язаною земельною ділянкою.
        Використовується для групування, коли є кадастровий номер — одна ділянка = одна будівля або комплекс.

        Args:
            land_plot_id: ObjectId земельної ділянки (рядок)

        Returns:
            Документ building або None
        """
        if not land_plot_id or not str(land_plot_id).strip():
            return None
        try:
            lp_oid = ObjectId(land_plot_id) if isinstance(land_plot_id, str) else land_plot_id
        except Exception:
            return None
        doc = self.collection.find_one({
            "type": "building",
            "land_plot_ids": lp_oid,
        })
        return _normalize_doc(doc)

    def find_by_ids(self, ids: List[str]) -> List[Dict[str, Any]]:
        """
        Повертає об'єкти за списком ObjectId.

        Args:
            ids: Список _id (рядки)

        Returns:
            Список документів
        """
        if not ids:
            return []
        obj_ids = []
        for i in ids:
            s = (i or "").strip()
            if not s or len(s) != 24:
                continue
            try:
                obj_ids.append(ObjectId(s))
            except Exception:
                pass
        if not obj_ids:
            return []
        docs = list(self.collection.find({"_id": {"$in": obj_ids}}))
        return [_normalize_doc(d) for d in docs]

    def find_by_listing(self, source: str, source_id: str) -> List[Dict[str, Any]]:
        """
        Витягує об'єкти, пов'язані з оголошенням.

        Args:
            source: Джерело (olx, prozorro)
            source_id: ID в джерелі

        Returns:
            Список документів ОНМ
        """
        docs = self.collection.find({
            "source_listing_ids": {
                "$elemMatch": {"source": source, "source_id": source_id},
            },
        })
        return [_normalize_doc(d) for d in docs]

    def create(
        self,
        obj_type: str,
        description: Optional[str] = None,
        area_sqm: Optional[float] = None,
        **kwargs: Any,
    ) -> str:
        """
        Створює новий об'єкт ОНМ.

        Args:
            obj_type: land_plot, building, premises
            description: Короткий опис
            area_sqm: Площа в м²
            **kwargs: Додаткові поля (cadastral_info, address, building_id тощо)

        Returns:
            ID створеного документа
        """
        now = datetime.now(timezone.utc)
        doc: Dict[str, Any] = {
            "type": obj_type,
            "created_at": now,
            "updated_at": now,
        }
        if description is not None:
            doc["description"] = description
        if area_sqm is not None:
            doc["area_sqm"] = area_sqm
        for k, v in kwargs.items():
            if v is not None:
                doc[k] = v
        return self.create_doc(doc)

    def create_doc(self, doc: Dict[str, Any]) -> str:
        """Створює документ з повною структурою."""
        now = datetime.now(timezone.utc)
        if "created_at" not in doc:
            doc["created_at"] = now
        if "updated_at" not in doc:
            doc["updated_at"] = now
        return super().create(doc)

    def update_object(
        self,
        object_id: str,
        **kwargs: Any,
    ) -> bool:
        """
        Оновлює об'єкт. Передані kwargs додаються через $set.

        Args:
            object_id: ID об'єкта
            **kwargs: Поля для оновлення

        Returns:
            True якщо оновлено
        """
        if not kwargs:
            return False
        now = datetime.now(timezone.utc)
        update_data = {"$set": {**kwargs, "updated_at": now}}
        return self.update_by_id(object_id, update_data)

    def add_source_listing(self, object_id: str, source: str, source_id: str) -> bool:
        """Додає посилання на оголошення до об'єкта."""
        try:
            obj_id = ObjectId(object_id) if isinstance(object_id, str) else object_id
            result = self.collection.update_one(
                {"_id": obj_id},
                {
                    "$addToSet": {
                        "source_listing_ids": {"source": source, "source_id": source_id},
                    },
                    "$set": {"updated_at": datetime.now(timezone.utc)},
                },
            )
            return result.modified_count > 0
        except Exception:
            return False
