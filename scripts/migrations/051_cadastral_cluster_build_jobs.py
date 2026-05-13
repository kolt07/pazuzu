# -*- coding: utf-8 -*-
"""
Міграція 051: колекція cadastral_cluster_build_jobs (прогрес національної кластеризації).
"""

from __future__ import annotations

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.cadastral_cluster_build_jobs_repository import (
    CadastralClusterBuildJobRepository,
)


def run_migration() -> bool:
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        CadastralClusterBuildJobRepository()._ensure()
        print("Міграція 051: cadastral_cluster_build_jobs ok.")
        return True
    except Exception as e:
        print("Помилка міграції 051:", e)
        return False


if __name__ == "__main__":
    import sys

    sys.exit(0 if run_migration() else 1)
