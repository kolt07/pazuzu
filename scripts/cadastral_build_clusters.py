# -*- coding: utf-8 -*-
"""
Скрипт побудови кластерів кадастрових ділянок.
Запуск: py scripts/cadastral_build_clusters.py [--max-parcels N] [--min-cluster-size N]
"""

import argparse
import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

os.chdir(_PROJECT_ROOT)

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.cadastral_clustering_service import CadastralClusteringService


def main() -> None:
    parser = argparse.ArgumentParser(description="Побудова кластерів кадастрових ділянок")
    parser.add_argument(
        "--max-parcels",
        type=int,
        default=None,
        help="Максимум ділянок для обробки (за замовчуванням — усі)",
    )
    parser.add_argument(
        "--min-cluster-size",
        type=int,
        default=2,
        help="Мінімальна кількість ділянок у кластері (за замовчуванням 2)",
    )
    args = parser.parse_args()

    Settings()
    MongoDBConnection.initialize(Settings())
    service = CadastralClusteringService()

    def progress(processed: int, clusters: int) -> None:
        if processed > 0 and processed % 500 == 0:
            print(f"  Оброблено: {processed}, знайдено кластерів: {clusters}", flush=True)

    print(
        f"Побудова кластерів (max_parcels={args.max_parcels or 'всі'}, min_cluster_size={args.min_cluster_size})...",
        flush=True,
    )
    result = service.build_clusters(
        max_parcels=args.max_parcels,
        min_cluster_size=args.min_cluster_size,
        progress_callback=progress,
    )
    print(
        f"Готово. Оброблено ділянок: {result['parcels_processed']}, "
        f"створено кластерів: {result['clusters_created']}, "
        f"ділянок у кластерах: {result['parcels_in_clusters']}",
        flush=True,
    )


if __name__ == "__main__":
    main()
