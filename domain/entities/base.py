# -*- coding: utf-8 -*-
"""
Базовий клас для domain-сутностей.
Обгортає сирі дані з БД, надає методи відстеження джерела та перетворення.
"""

from typing import Any, Dict, Optional


class BaseEntity:
    """
    Базова сутність domain-шару.
    Зберігає сирі дані (_raw_data) та надає методи для роботи з ними.
    """

    def __init__(self, raw_data: Dict[str, Any], source_collection: str):
        """
        Args:
            raw_data: Сирі дані з БД (словник документа).
            source_collection: Назва колекції-джерела (prozorro_auctions, olx_listings, unified_listings).
        """
        self._raw_data = dict(raw_data) if raw_data else {}
        self._source_collection = source_collection

    def get_raw_data(self) -> Dict[str, Any]:
        """Повертає сирі дані документа з БД."""
        return self._raw_data.copy()

    def get_property(self, path: str, default: Any = None) -> Any:
        """
        Отримує властивість за шляхом (dot-notation).
        Наприклад: get_property('auction_data.value.amount').
        """
        if not path or not path.strip():
            return default
        obj = self._raw_data
        for part in path.split("."):
            if obj is None:
                return default
            if isinstance(obj, dict):
                obj = obj.get(part)
            else:
                return default
        return obj if obj is not None else default

    @property
    def source_collection(self) -> str:
        """Назва колекції-джерела."""
        return self._source_collection

    def to_export_row(self, fields: list) -> Dict[str, Any]:
        """
        Перетворює сутність на плоский рядок для експорту.
        fields: список полів у dot-notation.
        """
        row = {}
        for field in fields:
            value = self.get_property(field)
            if value is not None and not isinstance(value, (dict, list)):
                row[field] = value
            elif value is not None:
                import json
                row[field] = json.dumps(value, ensure_ascii=False, default=str)
            else:
                row[field] = ""
        return row
