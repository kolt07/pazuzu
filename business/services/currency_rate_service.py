# -*- coding: utf-8 -*-
"""
Сервіс для отримання курсу продажу USD з Ощадбанку та кешування його в MongoDB.

Основні сценарії:
- При старті застосунку в фоні перевіряємо, чи є курс на сьогодні; якщо ні — завантажуємо.
- Інші сервіси (ProZorro, OLX, Mini App, MCP) можуть викликати get_today_usd_rate(), щоб
  отримати останній відомий курс продажу USD.

За замовчуванням курс береться зі сторінки:
    https://www.oschadbank.ua/currency-rate
Парсинг максимально обережний: шукаємо рядок з USD / "Долар США" і забираємо числові
значення зі стовпців, вважаючи, що останнє число в рядку — курс продажу.
"""

from __future__ import annotations

import logging
from datetime import datetime, date, timezone
from typing import Optional, Tuple

import requests
from bs4 import BeautifulSoup

from config.settings import Settings
from data.repositories.currency_rates_repository import CurrencyRatesRepository


logger = logging.getLogger(__name__)


class CurrencyRateService:
    """Отримання та кешування курсу валют."""

    OSCHAD_BASE_URL = "https://www.oschadbank.ua"
    OSCHAD_CURRENCY_PATH = "/currency-rate"
    SOURCE_OSCHADBANK = "oschadbank"

    def __init__(self, settings: Optional[Settings] = None) -> None:
        self.settings = settings or Settings()
        self._repo = CurrencyRatesRepository()
        self._session = requests.Session()
        self._session.headers.update(
            {
                "User-Agent": self.settings.user_agent,
                "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
            }
        )

    @staticmethod
    def _today_utc() -> date:
        now = datetime.now(timezone.utc)
        return now.date()

    # ---------- Публічні методи ----------

    def get_today_usd_rate(self, allow_fetch: bool = True) -> Optional[float]:
        """
        Повертає курс продажу USD (UAH за 1 USD) на сьогодні.

        1. Спочатку шукає в колекції currency_rates.
        2. Якщо немає і allow_fetch=True — пробує отримати з сайту Ощадбанку та зберегти.
        """
        today = self._today_utc()
        cached = self._repo.get_rate("USD", today, source=self.SOURCE_OSCHADBANK)
        if cached and cached.get("rate_sell") is not None:
            try:
                return float(cached["rate_sell"])
            except (TypeError, ValueError):
                logger.warning("Некоректне значення rate_sell у кеші currency_rates: %r", cached.get("rate_sell"))

        if not allow_fetch:
            return None

        try:
            sell_rate, buy_rate = self._fetch_oschad_usd_rates()
        except Exception as e:
            logger.warning("Не вдалося отримати курс USD з Ощадбанку: %s", e)
            return None

        if sell_rate is None:
            return None

        try:
            self._repo.upsert_rate(
                currency_code="USD",
                d=today,
                source=self.SOURCE_OSCHADBANK,
                rate_sell=sell_rate,
                rate_buy=buy_rate,
                metadata={"source_url": self.OSCHAD_BASE_URL + self.OSCHAD_CURRENCY_PATH},
            )
        except Exception as e:
            # Курс все одно повертаємо, навіть якщо збереження не вдалося
            logger.warning("Не вдалося зберегти курс USD в MongoDB: %s", e)

        return sell_rate

    def ensure_today_usd_rate_if_missing(self) -> None:
        """
        Перевіряє, чи є курс продажу USD на сьогодні; якщо ні — пробує отримати його з Ощадбанку.
        Використовується для фонового оновлення при старті застосунку.
        """
        today = self._today_utc()
        if self._repo.get_rate("USD", today, source=self.SOURCE_OSCHADBANK):
            return
        try:
            self.get_today_usd_rate(allow_fetch=True)
        except Exception as e:
            logger.warning("Фонове оновлення курсу USD завершилося помилкою: %s", e)

    # ---------- Внутрішні методи ----------

    def _fetch_oschad_usd_rates(self) -> Tuple[Optional[float], Optional[float]]:
        """
        Завантажує HTML сторінки курсів валют Ощадбанку та намагається витягнути
        курс купівлі/продажу USD.

        Returns:
            (sell_rate, buy_rate) у гривнях, або (None, None) якщо не вдалося розпарсити.
        """
        url = self.OSCHAD_BASE_URL + self.OSCHAD_CURRENCY_PATH
        resp = self._session.get(url, timeout=15)
        resp.raise_for_status()

        html = resp.text
        soup = BeautifulSoup(html, "lxml")

        # Шукаємо усі рядки таблиць, де згадується USD / Долар США
        rows = soup.find_all("tr")
        best_sell: Optional[float] = None
        best_buy: Optional[float] = None

        for tr in rows:
            text = tr.get_text(" ", strip=True)
            lower = text.lower()
            if "usd" not in lower and "долар" not in lower:
                continue

            cells = [td.get_text(" ", strip=True) for td in tr.find_all("td")]
            if not cells:
                # Можливо, у <th>, пробуємо ще раз
                cells = [th.get_text(" ", strip=True) for th in tr.find_all("th")]
            if not cells:
                continue

            # Витягуємо всі числові значення з рядка (з тис. роздільниками).
            numbers = []
            for cell in cells[1:]:  # перший стовпець — назва валюти
                cleaned = (
                    cell.replace("\u00a0", " ")
                    .replace(",", ".")
                    .replace(" ", " ")
                )
                tokens = cleaned.split()
                for t in tokens:
                    t = t.strip()
                    if not t:
                        continue
                    # допускаємо формат 41.90 або 41
                    if all(ch.isdigit() or ch == "." for ch in t) and any(ch.isdigit() for ch in t):
                        try:
                            numbers.append(float(t))
                        except ValueError:
                            continue

            if not numbers:
                continue

            # Припускаємо, що:
            # - перше число = купівля
            # - останнє число = продаж
            buy = numbers[0]
            sell = numbers[-1]

            # Якщо на сторінці кілька рядків з USD (готівковий / картковий тощо) —
            # обираємо найбільший курс продажу як найбільш консервативний.
            if best_sell is None or sell > best_sell:
                best_sell = sell
                best_buy = buy

        return best_sell, best_buy

