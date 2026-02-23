# -*- coding: utf-8 -*-
"""
Тест механізмів кадастрової карти: парсер номера, індекс місцезнаходження, кластеризація.
Запуск: py scripts/test_cadastral_mechanisms.py
"""

import os
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

_PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

os.chdir(_PROJECT_ROOT)


def test_parser() -> bool:
    """Тест парсера кадастрового номера."""
    from utils.cadastral_code_parser import parse_cadastral_number, get_location_for_search

    cases = [
        ("6310138500:10:012:0045", "Харківська", "63"),
        ("6320685503:03:000:0202", "Харківська", "63"),
        ("8000000001:01:001:0001", "Київ", "80"),
        ("3200000000:00:001:0001", "Київська", "32"),
        (" 6310138500 : 10 : 012 : 0045 ", "Харківська", "63"),
    ]
    ok = 0
    for cadnum, expected_oblast, expected_code in cases:
        parsed = parse_cadastral_number(cadnum)
        if parsed and parsed.get("oblast_name") == expected_oblast and parsed.get("oblast_code") == expected_code:
            print(f"  OK: {cadnum} -> {parsed.get('oblast_name')} ({parsed.get('oblast_code')})")
            ok += 1
        else:
            print(f"  FAIL: {cadnum} -> {parsed} (expected {expected_oblast}/{expected_code})")

    loc = get_location_for_search("6310138500:10:012:0045")
    if loc and loc.get("oblast_name") == "Харківська" and "koatuu_prefix" in loc:
        print(f"  OK: get_location_for_search -> {loc.get('oblast_name')}, koatuu={loc.get('koatuu_prefix')}")
        ok += 1
    else:
        print(f"  FAIL: get_location_for_search -> {loc}")

    return ok == len(cases) + 1


def test_location_index() -> bool:
    """Тест побудови індексу місцезнаходження."""
    from config.settings import Settings
    from data.database.connection import MongoDBConnection
    from business.services.cadastral_location_index_service import CadastralLocationIndexService
    from data.repositories.cadastral_parcel_location_index_repository import (
        CadastralParcelLocationIndexRepository,
    )

    Settings()
    MongoDBConnection.initialize(Settings())
    service = CadastralLocationIndexService()
    index_repo = CadastralParcelLocationIndexRepository()
    index_repo.ensure_index()

    # Індексуємо тестові номери
    test_nums = ["6310138500:10:012:0045", "8000000001:01:001:0001"]
    for cn in test_nums:
        service.index_parcel(cn)

    found = index_repo.find_by_oblast(oblast_code="63")
    by_cn = index_repo.find_by_cadastral_number("6310138500:10:012:0045")
    if found and by_cn and by_cn.get("oblast_name") == "Харківська":
        print(f"  OK: індекс місцезнаходження — знайдено {len(found)} по області 63, by_cn={by_cn.get('oblast_name')}")
        return True
    print(f"  FAIL: found={len(found) if found else 0}, by_cn={by_cn}")
    return False


def test_clustering() -> bool:
    """Тест кластеризації (потребує ділянок у БД)."""
    from config.settings import Settings
    from data.database.connection import MongoDBConnection
    from business.services.cadastral_clustering_service import CadastralClusteringService
    from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository

    Settings()
    MongoDBConnection.initialize(Settings())
    parcels_repo = CadastralParcelsRepository()
    count = parcels_repo.count_total()
    if count < 2:
        print("  SKIP: кластеризація потребує мінімум 2 ділянок у cadastral_parcels")
        return True

    service = CadastralClusteringService()
    result = service.build_clusters(max_parcels=500, min_cluster_size=2)
    print(
        f"  OK: кластеризація — оброблено {result['parcels_processed']}, "
        f"кластерів {result['clusters_created']}, ділянок у кластерах {result['parcels_in_clusters']}"
    )
    return True


def main() -> None:
    print("=== Тест парсера кадастрового номера ===")
    parser_ok = test_parser()
    print()

    print("=== Тест індексу місцезнаходження ===")
    index_ok = test_location_index()
    print()

    print("=== Тест кластеризації ===")
    cluster_ok = test_clustering()
    print()

    all_ok = parser_ok and index_ok and cluster_ok
    print("=" * 50)
    print("Результат:", "OK" if all_ok else "FAIL")
    sys.exit(0 if all_ok else 1)


if __name__ == "__main__":
    main()
