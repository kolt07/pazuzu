# -*- coding: utf-8 -*-
"""
Аудит гомонімів НП: одна назва в різних областях.

Не змінює дані — лише звіт. Перевіряє, чи є в unified_listings адреси,
де settlement збігається з каноном іншої області (ризик після старого canon_for).

Запуск:
  py scripts/migrations/059_audit_settlement_homonyms.py
  py scripts/migrations/059_audit_settlement_homonyms.py --limit 50
"""

from __future__ import annotations

import argparse
import sys
from collections import defaultdict
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.geography_repository import active_cities_mongo_clause
from utils.settlement_normalizer import normalize_settlement_key


def main() -> int:
    parser = argparse.ArgumentParser(description="Аудит гомонімів НП (read-only)")
    parser.add_argument("--limit", type=int, default=30, help="Макс. прикладів у звіті")
    args = parser.parse_args()

    MongoDBConnection.initialize(Settings())
    db = MongoDBConnection.get_database()

    by_key: dict[str, list] = defaultdict(list)
    for city in db.cities.find(active_cities_mongo_clause()):
        key = city.get("name_normalized") or normalize_settlement_key(city.get("name"))
        if not key:
            continue
        rid = str(city.get("region_id") or "")
        by_key[key].append((rid, city.get("name"), str(city["_id"])))

    homonym_keys = {k: v for k, v in by_key.items() if len({x[0] for x in v}) > 1}
    print(f"Гомоніми в cities (різні region_id, однаковий ключ): {len(homonym_keys)}")

    regions_by_name = {r["name"]: str(r["_id"]) for r in db.regions.find({}, {"name": 1})}
    canonical = {}
    for city in db.cities.find(active_cities_mongo_clause()):
        key = city.get("name_normalized") or normalize_settlement_key(city.get("name"))
        if not key:
            continue
        rid = str(city.get("region_id") or "")
        canonical[(rid, key)] = city.get("name")

    suspicious = []
    for doc in db.unified_listings.find({}, {"addresses": 1, "region": 1, "city": 1}):
        for addr in doc.get("addresses") or []:
            if not isinstance(addr, dict):
                continue
            settlement = addr.get("settlement") or addr.get("city")
            if not settlement:
                continue
            key = normalize_settlement_key(settlement)
            if key not in homonym_keys:
                continue
            addr_rid = regions_by_name.get(addr.get("region")) or regions_by_name.get(doc.get("region"))
            if not addr_rid:
                continue
            expected = canonical.get((addr_rid, key))
            if expected and expected != settlement:
                suspicious.append({
                    "listing_id": str(doc["_id"]),
                    "addr_region": addr.get("region"),
                    "settlement": settlement,
                    "expected_in_region": expected,
                    "key": key,
                })
        if len(suspicious) >= args.limit:
            break

    print(f"Підозрілі адреси (settlement ≠ канон своєї області): {len(suspicious)}")
    for row in suspicious[: args.limit]:
        print(
            f"  {row['listing_id']}: «{row['settlement']}» в {row['addr_region']!r} "
            f"→ очікувалось «{row['expected_in_region']}»"
        )
    return 0


if __name__ == "__main__":
    sys.exit(main())
