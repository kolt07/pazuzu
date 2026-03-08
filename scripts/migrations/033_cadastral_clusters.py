# -*- coding: utf-8 -*-
"""
Міграція 033: Колекція cadastral_parcel_clusters для кластерів ділянок
(спільні кордони, однакове призначення та форма власності).
"""

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.cadastral_parcel_clusters_repository import (
    CadastralParcelClustersRepository,
)


def run_migration() -> bool:
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        repo = CadastralParcelClustersRepository()
        repo.ensure_index()
        print("Міграція 033: колекція cadastral_parcel_clusters створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 033:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
