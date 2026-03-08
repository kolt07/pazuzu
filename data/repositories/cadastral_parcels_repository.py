# -*- coding: utf-8 -*-
"""
Репозиторій земельних ділянок з кадастру (kadastrova-karta.com).
Ідентифікатор — cadastral_number.
"""

from datetime import datetime, timezone
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

COLLECTION_NAME = "cadastral_parcels"
SOURCE_KADASTROVA = "kadastrova-karta"


def _normalize_doc(doc: Optional[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
    """Повертає документ з _id у вигляді рядка."""
    if doc is None:
        return None
    if "_id" in doc and hasattr(doc["_id"], "binary"):
        doc["_id"] = str(doc["_id"])
    return doc


class CadastralParcelsRepository(BaseRepository):
    """Робота з колекцією земельних ділянок з кадастру."""

    def __init__(self):
        super().__init__(COLLECTION_NAME)

    def find_by_cadastral_number(self, cadastral_number: str) -> Optional[Dict[str, Any]]:
        """Знаходить ділянку за кадастровим номером."""
        if not cadastral_number or not str(cadastral_number).strip():
            return None
        doc = self.collection.find_one({"cadastral_number": str(cadastral_number).strip()})
        return _normalize_doc(doc)

    def upsert_parcel(
        self,
        cadastral_number: str,
        parcel_data: Dict[str, Any],
        source_cell_id: Optional[str] = None,
    ) -> bool:
        """
        Створює або оновлює ділянку за кадастровим номером.
        parcel_data: bounds, purpose, purpose_label, category, area_sqm, ownership_form, тощо.
        """
        if not cadastral_number or not str(cadastral_number).strip():
            return False
        cadastral_number = str(cadastral_number).strip()
        now = datetime.now(timezone.utc)

        doc: Dict[str, Any] = {
            "cadastral_number": cadastral_number,
            "source": SOURCE_KADASTROVA,
            "fetched_at": now,
            **{k: v for k, v in parcel_data.items() if v is not None},
        }
        if source_cell_id:
            doc["source_cell_id"] = source_cell_id

        result = self.collection.update_one(
            {"cadastral_number": cadastral_number},
            {"$set": doc},
            upsert=True,
        )
        return result.upserted_id is not None or result.modified_count > 0

    def upsert_many(
        self,
        parcels: List[Dict[str, Any]],
        source_cell_id: Optional[str] = None,
    ) -> int:
        """
        Масовий upsert ділянок. Повертає кількість успішно збережених.
        Кожен елемент parcels має містити cadastral_number та інші поля.
        """
        if not parcels:
            return 0
        now = datetime.now(timezone.utc)
        count = 0
        for p in parcels:
            cn = (p.get("cadastral_number") or "").strip()
            if not cn:
                continue
            doc = {
                "cadastral_number": cn,
                "source": SOURCE_KADASTROVA,
                "fetched_at": now,
                **{k: v for k, v in p.items() if k != "cadastral_number" and v is not None},
            }
            if source_cell_id:
                doc["source_cell_id"] = source_cell_id
            try:
                result = self.collection.update_one(
                    {"cadastral_number": cn},
                    {"$set": doc},
                    upsert=True,
                )
                if result.upserted_id or result.modified_count:
                    count += 1
            except Exception:
                pass  # Пропускаємо ділянки з невалідною геометрією (2dsphere)
        return count

    def ensure_index(self) -> None:
        """Створює індекси (міграція вже створює їх, але для явного виклику)."""
        self.collection.create_index("cadastral_number", unique=True)
        self.collection.create_index([("bounds", "2dsphere")])
        self.collection.create_index("source_cell_id")
        self.collection.create_index("source")

    def count_total(self) -> int:
        """Загальна кількість ділянок (точна, може бути повільною на великих колекціях)."""
        return self.collection.count_documents({})

    def count_total_estimated(self) -> int:
        """Орієнтовна кількість ділянок (O(1), для прогрес-бару)."""
        return self.collection.estimated_document_count()
