# -*- coding: utf-8 -*-
"""
Репозиторій кластерів кадастрових ділянок.
Кластер — група ділянок зі спільними кордонами (або близько), однаковим призначенням та формою власності.
"""

from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "cadastral_parcel_clusters"


class CadastralParcelClustersRepository(BaseRepository):
    """Робота з кластерами ділянок."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def upsert_cluster(
        self,
        cluster_id: str,
        cadastral_numbers: List[str],
        purpose: Optional[str],
        purpose_label: Optional[str],
        ownership_form: Optional[str],
        parcel_count: int,
        centroid: Optional[Dict[str, Any]] = None,
        bounds: Optional[Dict[str, Any]] = None,
        total_area_sqm: Optional[float] = None,
    ) -> bool:
        """
        Створює або оновлює кластер.

        Args:
            cluster_id: унікальний ідентифікатор (напр. hash або first_cadnum)
            cadastral_numbers: список кадастрових номерів у кластері
            purpose, purpose_label, ownership_form: спільні атрибути
            parcel_count: кількість ділянок
            centroid: GeoJSON Point (центр кластера)
            bounds: GeoJSON Polygon (об'єднання меж)
            total_area_sqm: сумарна площа
        """
        if not cluster_id or not cadastral_numbers:
            return False
        doc: Dict[str, Any] = {
            "cluster_id": cluster_id,
            "cadastral_numbers": list(cadastral_numbers),
            "purpose": purpose,
            "purpose_label": purpose_label,
            "ownership_form": ownership_form,
            "parcel_count": parcel_count,
        }
        if centroid:
            doc["centroid"] = centroid
        if bounds:
            doc["bounds"] = bounds
        if total_area_sqm is not None:
            doc["total_area_sqm"] = total_area_sqm

        result = self.collection.update_one(
            {"cluster_id": cluster_id},
            {"$set": doc},
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def find_by_cluster_id(self, cluster_id: str) -> Optional[Dict[str, Any]]:
        """Знаходить кластер за ID."""
        doc = self.collection.find_one({"cluster_id": cluster_id})
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def find_by_cadastral_number(self, cadastral_number: str) -> Optional[Dict[str, Any]]:
        """Знаходить кластер, що містить дану ділянку."""
        doc = self.collection.find_one({"cadastral_numbers": str(cadastral_number).strip()})
        if doc and "_id" in doc:
            doc["_id"] = str(doc["_id"])
        return doc

    def find_by_purpose_and_ownership(
        self,
        purpose: Optional[str] = None,
        ownership_form: Optional[str] = None,
        min_parcel_count: int = 2,
    ) -> List[Dict[str, Any]]:
        """Пошук кластерів за призначенням та формою власності."""
        flt: Dict[str, Any] = {"parcel_count": {"$gte": min_parcel_count}}
        if purpose:
            flt["purpose"] = purpose
        if ownership_form:
            flt["ownership_form"] = ownership_form
        docs = list(self.collection.find(flt))
        for d in docs:
            if "_id" in d:
                d["_id"] = str(d["_id"])
        return docs

    def ensure_index(self) -> None:
        """Створює індекси."""
        self.collection.create_index("cluster_id", unique=True)
        self.collection.create_index("cadastral_numbers")
        self.collection.create_index([("purpose", 1), ("ownership_form", 1)])
        self.collection.create_index("parcel_count")
        try:
            self.collection.create_index([("bounds", "2dsphere")])
        except Exception:
            pass
        try:
            self.collection.create_index([("centroid", "2dsphere")])
        except Exception:
            pass

    def count_total(self) -> int:
        """Загальна кількість кластерів."""
        return self.collection.count_documents({})

    def clear_all(self) -> int:
        """Видаляє всі кластери. Повертає кількість видалених."""
        result = self.collection.delete_many({})
        return result.deleted_count
