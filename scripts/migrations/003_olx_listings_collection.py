# -*- coding: utf-8 -*-
"""
Міграція 003: Колекція olx_listings для скрапера OLX.

Створює колекцію olx_listings та унікальний індекс по полю url.
Перед першим запуском прототипу скрапера доцільно виконати міграції.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.olx_listings_repository import OlxListingsRepository


def run():
    """Виконує міграцію: ініціалізація підключення та створення індексу."""
    settings = Settings()
    MongoDBConnection.initialize(settings)
    repo = OlxListingsRepository()
    repo.ensure_index()
    print("Міграція 003: колекція olx_listings та індекс по url створено/перевірено.")
    return True


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    run()
    return True
