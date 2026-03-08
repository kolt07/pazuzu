# -*- coding: utf-8 -*-
"""
Сервіс побудови та оновлення індексу місцезнаходження ділянок з кадастрового номера.
"""

from typing import Any, Dict, List, Optional

from data.repositories.cadastral_parcel_location_index_repository import (
    CadastralParcelLocationIndexRepository,
)
from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository
from utils.cadastral_code_parser import get_location_for_search


class CadastralLocationIndexService:
    """
    Побудова індексу cadastral_parcel_location_index з існуючих ділянок
    та оновлення при додаванні нових.
    """

    def __init__(
        self,
        parcels_repo: Optional[CadastralParcelsRepository] = None,
        index_repo: Optional[CadastralParcelLocationIndexRepository] = None,
    ):
        self._parcels_repo = parcels_repo or CadastralParcelsRepository()
        self._index_repo = index_repo or CadastralParcelLocationIndexRepository()

    def enrich_parcel_from_cadastral_number(
        self,
        cadastral_number: str,
    ) -> Optional[Dict[str, Any]]:
        """
        Витягує дані місцезнаходження з кадастрового номера та повертає їх.
        Не зберігає в БД — тільки парсинг.
        """
        return get_location_for_search(cadastral_number)

    def index_parcel(self, cadastral_number: str) -> bool:
        """
        Індексує одну ділянку: парсить кадастровий номер та зберігає в location index.
        """
        location = get_location_for_search(cadastral_number)
        if not location:
            return False
        # Видаляємо cadastral_number з location (воно вже є першим аргументом)
        data = {k: v for k, v in location.items() if k != "cadastral_number"}
        return self._index_repo.upsert(cadastral_number, data)

    def build_index_from_parcels(
        self,
        batch_size: int = 5000,
        progress_callback: Optional[Any] = None,
    ) -> Dict[str, int]:
        """
        Побудовує індекс з усіх ділянок у cadastral_parcels.
        Ітерує по cadastral_number, парсить та зберігає в location index.

        Returns:
            {"indexed": N, "skipped": M, "errors": K}
        """
        indexed = 0
        skipped = 0
        errors = 0
        cursor = self._parcels_repo.collection.find(
            {},
            {"cadastral_number": 1},
            no_cursor_timeout=True,
        )
        try:
            batch: List[str] = []
            for doc in cursor:
                cn = doc.get("cadastral_number")
                if not cn:
                    skipped += 1
                    continue
                batch.append(str(cn).strip())
                if len(batch) >= batch_size:
                    for cadnum in batch:
                        try:
                            if self.index_parcel(cadnum):
                                indexed += 1
                            else:
                                skipped += 1
                        except Exception:
                            errors += 1
                    if progress_callback:
                        progress_callback(indexed, skipped, errors)
                    batch = []
            for cadnum in batch:
                try:
                    if self.index_parcel(cadnum):
                        indexed += 1
                    else:
                        skipped += 1
                except Exception:
                    errors += 1
        finally:
            cursor.close()
        return {"indexed": indexed, "skipped": skipped, "errors": errors}
