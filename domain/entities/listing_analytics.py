# -*- coding: utf-8 -*-
"""
Domain-сутність LLM-аналітики оголошення (listing_analytics).
Зв'язок з оголошенням через source+source_id.
"""

from typing import Any, Dict, Optional

from domain.entities.base import BaseEntity


class ListingAnalytics(BaseEntity):
    """
    Сутність LLM-аналітики оголошення.
    Методи для отримання властивостей.
    """

    def __init__(self, raw_data: Dict[str, Any]):
        super().__init__(raw_data, "listing_analytics")

    @property
    def id(self) -> Optional[str]:
        """Системний _id документа."""
        return self.get_property("_id")

    @property
    def source(self) -> str:
        """Джерело: olx або prozorro."""
        return self.get_property("source") or ""

    @property
    def source_id(self) -> str:
        """ID оголошення в джерелі (url для OLX, auction_id для ProZorro)."""
        return self.get_property("source_id") or ""

    @property
    def composite_id(self) -> str:
        """Складний ідентифікатор: source:source_id."""
        return f"{self.source}:{self.source_id}"

    @property
    def analysis_text(self) -> str:
        """Текст LLM-аналітики (3 блоки: ціна за одиницю, місцезнаходження, оточення)."""
        return self.get_property("analysis_text") or ""

    @property
    def metadata(self) -> Dict[str, Any]:
        """Додаткові метадані."""
        meta = self.get_property("metadata")
        return dict(meta) if isinstance(meta, dict) else {}

    @property
    def analysis_at(self) -> Optional[str]:
        """Дата та час генерації аналітики."""
        return self.get_property("analysis_at")
