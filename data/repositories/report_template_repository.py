# -*- coding: utf-8 -*-
"""
Репозиторій для збереження шаблонів звітів користувачів.
"""

import logging
from typing import Any, Dict, List, Optional

from data.repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class ReportTemplateRepository(BaseRepository):
    """Репозиторій для роботи з шаблонами звітів."""

    def __init__(self):
        super().__init__("report_templates")

    def create(
        self,
        user_id: int,
        name: str,
        params: Dict[str, Any],
        is_default: bool = False,
    ) -> str:
        """
        Створює шаблон звіту.

        Args:
            user_id: ID користувача
            name: Назва шаблону
            params: Параметри шаблону (source, date_filter, region, city, тощо)
            is_default: Чи є шаблон системним (не видаляється)

        Returns:
            ID створеного шаблону
        """
        max_order = self._get_max_order(user_id)
        doc = {
            "user_id": user_id,
            "name": name,
            "params": params,
            "is_default": is_default,
            "order": max_order + 1,
        }
        result = self.collection.insert_one(doc)
        return str(result.inserted_id)

    def _get_max_order(self, user_id: int) -> int:
        """Повертає максимальний order для користувача."""
        doc = self.collection.find_one(
            {"user_id": user_id},
            sort=[("order", -1)],
            projection={"order": 1},
        )
        return doc.get("order", -1) if doc else -1

    def list_by_user(self, user_id: int) -> List[Dict[str, Any]]:
        """
        Повертає список шаблонів користувача, відсортованих за order.
        Включає системні шаблони (is_default=True) та користувацькі.
        """
        cursor = self.collection.find(
            {"user_id": user_id}
        ).sort("order", 1)
        result = []
        for doc in cursor:
            doc["_id"] = str(doc["_id"])
            result.append(doc)
        return result

    def get_by_id(self, template_id: str, user_id: int) -> Optional[Dict[str, Any]]:
        """Отримує шаблон за ID, перевіряючи user_id."""
        from bson import ObjectId
        try:
            doc = self.collection.find_one({
                "_id": ObjectId(template_id),
                "user_id": user_id,
            })
            if doc:
                doc["_id"] = str(doc["_id"])
                return doc
        except Exception:
            pass
        return None

    def delete(self, template_id: str, user_id: int) -> bool:
        """
        Видаляє шаблон. Не дозволяє видаляти системні (is_default=True).
        """
        from bson import ObjectId
        doc = self.collection.find_one({
            "_id": ObjectId(template_id),
            "user_id": user_id,
        })
        if not doc:
            return False
        if doc.get("is_default"):
            return False
        result = self.collection.delete_one({"_id": ObjectId(template_id)})
        return result.deleted_count > 0

    def reorder(self, user_id: int, template_ids: List[str]) -> bool:
        """
        Оновлює порядок шаблонів за списком ID.
        """
        from bson import ObjectId
        for i, tid in enumerate(template_ids):
            try:
                self.collection.update_one(
                    {"_id": ObjectId(tid), "user_id": user_id},
                    {"$set": {"order": i}},
                )
            except Exception:
                pass
        return True
