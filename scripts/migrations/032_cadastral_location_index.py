# -*- coding: utf-8 -*-
"""
Міграція 032: Колекція cadastral_parcel_location_index для пошуку ділянок
за топографічною прив'язкою (область, район) з кадастрового номера.
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.cadastral_parcel_location_index_repository import (
    CadastralParcelLocationIndexRepository,
)


def run_migration() -> bool:
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        repo = CadastralParcelLocationIndexRepository()
        repo.ensure_index()
        print("Міграція 032: колекція cadastral_parcel_location_index створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 032:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
