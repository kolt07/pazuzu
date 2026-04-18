# -*- coding: utf-8 -*-
"""
Прототип скрапера OLX: нежитлова (комерційна) нерухомість.
Проходить по перших N сторінках пошуку, зберігає оголошення в MongoDB.
Якщо оголошення нове або без detail або змінились дані з пошуку — відкриває сторінку оголошення
і зберігає блок detail. Затримка між запитами деталей: 2–10 с (рандом).

Запуск з кореня проекту:
  py scripts/olx_scraper/run_prototype.py

Для оновлення даних разом із ProZorro (нежитлова + земельні ділянки) використовуйте:
  py scripts/olx_scraper/run_update.py
  або запуск main.py / кнопки оновлення в Telegram.
"""

import sys
from pathlib import Path

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from scripts.olx_scraper import config as scraper_config
from scripts.olx_scraper.run_update import run_olx_update


def main() -> None:
    """Запуск тільки категорії «Нежитлова нерухомість» (прототип)."""
    categories = [
        {
            "label": "Нежитлова нерухомість",
            "get_list_url": scraper_config.get_commercial_real_estate_list_url,
            "max_pages": scraper_config.MAX_SEARCH_PAGES,
        },
    ]
    run_olx_update(settings=Settings(), categories=categories)


if __name__ == "__main__":
    main()
