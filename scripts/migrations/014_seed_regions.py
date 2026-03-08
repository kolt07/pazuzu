# -*- coding: utf-8 -*-
"""
Міграція 014: Заповнення колекції regions областями України.
Дозволяє відображати списки областей у фільтрах OLX/ProZorro навіть без даних у листингах.
"""

import sys
from pathlib import Path

project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.geography_repository import RegionsRepository


# Назви областей України (офіційні, без суфікса " область")
UKRAINE_REGIONS = [
    "Вінницька",
    "Волинська",
    "Дніпропетровська",
    "Донецька",
    "Житомирська",
    "Закарпатська",
    "Запорізька",
    "Івано-Франківська",
    "Київська",
    "Кіровоградська",
    "Луганська",
    "Львівська",
    "Миколаївська",
    "Одеська",
    "Полтавська",
    "Рівненська",
    "Сумська",
    "Тернопільська",
    "Харківська",
    "Херсонська",
    "Хмельницька",
    "Черкаська",
    "Чернівецька",
    "Чернігівська",
    "Київ",  # місто зі спеціальним статусом
]


def run_migration():
    """
    Виконує міграцію: заповнює колекцію regions областями України.
    Returns:
        bool: True якщо успішно
    """
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        repo = RegionsRepository()
        for name in UKRAINE_REGIONS:
            repo.find_or_create(name)
        print("Migration 014 completed: Regions seeded (%d names)" % len(UKRAINE_REGIONS))
        return True
    except Exception as e:
        print("Migration 014 failed: %s" % e)
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    sys.exit(0 if run_migration() else 1)
