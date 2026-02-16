# -*- coding: utf-8 -*-
"""
Репозиторій для зберігання курсів валют (нас цікавить курс продажу USD з Ощадбанку).
"""

from __future__ import annotations

from datetime import datetime, date, timezone
from typing import Optional, Dict, Any

from data.repositories.base_repository import BaseRepository


class CurrencyRatesRepository(BaseRepository):
    """
    Колекція `currency_rates` зберігає денні курси валют від конкретного джерела.

    Документ має структуру:
    - currency_code: рядок ISO коду валюти, напр. "USD"
    - date: дата, до якої належить курс, у форматі YYYY-MM-DD (рядок)
    - source: ідентифікатор джерела, напр. "oschadbank"
    - rate_sell: курс продажу (float, у гривнях за 1 одиницю валюти)
    - rate_buy: опційно, курс купівлі (float)
    - metadata: опційні службові дані (dict)
    - fetched_at: datetime в UTC, коли курс було отримано
    """

    def __init__(self) -> None:
        super().__init__("currency_rates")
        self._indexes_created = False

    def _ensure_indexes(self) -> None:
        """Створює необхідні індекси (одноразово)."""
        if self._indexes_created:
            return
        try:
            # Унікальний запис на день для пари (currency_code, source)
            self.collection.create_index(
                [("currency_code", 1), ("source", 1), ("date", 1)],
                unique=True,
                name="uniq_currency_source_date",
            )
            # Частий запит: останній курс валюти певного джерела
            self.collection.create_index(
                [("currency_code", 1), ("source", 1), ("date", -1)],
                name="idx_currency_source_date_desc",
            )
            self._indexes_created = True
        except Exception:
            # Не валимо застосунок, якщо щось пішло не так при створенні індексу
            pass

    @staticmethod
    def _normalize_date(d: date | datetime) -> str:
        """Конвертує дату/дату-час у рядок YYYY-MM-DD у часовій зоні UTC."""
        if isinstance(d, datetime):
            if d.tzinfo is None:
                d = d.replace(tzinfo=timezone.utc)
            d = d.astimezone(timezone.utc).date()
        return d.isoformat()

    def get_rate(
        self,
        currency_code: str,
        d: date | datetime,
        source: str = "oschadbank",
    ) -> Optional[Dict[str, Any]]:
        """
        Повертає документ з курсом валюти на конкретну дату (або None).

        Args:
            currency_code: код валюти, напр. "USD"
            d: дата або datetime, до якої належить курс
            source: ідентифікатор джерела, за замовчуванням "oschadbank"
        """
        self._ensure_indexes()
        date_str = self._normalize_date(d)
        doc = self.find_one(
            {
                "currency_code": currency_code.upper().strip(),
                "source": source,
                "date": date_str,
            }
        )
        return doc

    def upsert_rate(
        self,
        currency_code: str,
        d: date | datetime,
        source: str,
        rate_sell: float,
        rate_buy: Optional[float] = None,
        metadata: Optional[Dict[str, Any]] = None,
    ) -> bool:
        """
        Створює або оновлює запис про курс валюти.

        Args:
            currency_code: код валюти (наприклад, "USD")
            d: дата, до якої належить курс
            source:ідентифікатор джерела, напр. "oschadbank"
            rate_sell: курс продажу
            rate_buy: опційно, курс купівлі
            metadata: додаткові дані (наприклад, сирцевий текст)
        """
        self._ensure_indexes()
        date_str = self._normalize_date(d)
        now_utc = datetime.now(timezone.utc)

        update: Dict[str, Any] = {
            "$set": {
                "currency_code": currency_code.upper().strip(),
                "source": source,
                "date": date_str,
                "rate_sell": float(rate_sell),
                "fetched_at": now_utc,
            },
            "$setOnInsert": {
                "created_at": now_utc,
            },
        }
        if rate_buy is not None:
            update["$set"]["rate_buy"] = float(rate_buy)
        if metadata:
            update["$set"]["metadata"] = metadata

        result = self.collection.update_one(
            {
                "currency_code": currency_code.upper().strip(),
                "source": source,
                "date": date_str,
            },
            update,
            upsert=True,
        )
        return bool(result.upserted_id or result.modified_count)

