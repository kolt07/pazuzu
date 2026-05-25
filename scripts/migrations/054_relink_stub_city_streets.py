# -*- coding: utf-8 -*-
"""
Міграція 054: перенесення вулиць із stub cities (помилково збережених «районів області»)
на реальні НП та видалення stub-документів.

Підбір цільового НП: utils/rayon_city_resolver (fuzzy + fallback).
Перенос вулиць: логіка як у SettlementDeduplicationService._merge_streets.
Оновлення посилань: _update_address_refs / _update_unified_listings того ж сервісу.

Запуск:
  py scripts/migrations/054_relink_stub_city_streets.py --dry-run
  py scripts/migrations/054_relink_stub_city_streets.py
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.geography_repository import CitiesRepository
from business.services.settlement_deduplication_service import SettlementDeduplicationService
from utils.rayon_city_resolver import pick_target_city_for_stub
from bson import ObjectId


def run_migration(dry_run: bool = False) -> bool:
    print("=" * 60)
    print("Міграція 054: вулиці з stub cities → реальні НП, видалення stub")
    print(f"Режим: {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()
        svc = SettlementDeduplicationService()
        c_repo = CitiesRepository()

        stubs = list(db.cities.find({"is_oblast_rayon_stub": True}))
        print(f"Знайдено stub cities: {len(stubs)}")

        stats = {
            "resolved": 0,
            "skipped_no_target": 0,
            "streets_relinked": 0,
            "streets_merged": 0,
            "refs_updated": 0,
            "listings_updated": 0,
            "stubs_deleted": 0,
        }

        for stub in stubs:
            sid = str(stub["_id"])
            region_id = str(stub.get("region_id") or "")
            ref_rid = stub.get("oblast_rayon_ref_id")
            rayon_name = stub.get("name") or ""
            if ref_rid:
                rod = db.oblast_rayons.find_one({"_id": ObjectId(ref_rid)})
                if rod and rod.get("name"):
                    rayon_name = rod["name"]

            reg_doc = db.regions.find_one({"_id": ObjectId(region_id)}) if region_id else None
            region_name = (reg_doc or {}).get("name") or ""

            candidates = c_repo.find_many(
                filter=CitiesRepository._active_city_filter({"region_id": region_id}),
                sort=[("name", 1)],
            )

            target, score, reason = pick_target_city_for_stub(
                candidates,
                region_name=region_name,
                rayon_display_name=rayon_name,
            )
            tid = str(target["_id"]) if target else None

            n_streets = db.streets.count_documents({"city_id": sid})
            print(
                f"  stub={sid} rayon={rayon_name!r} streets={n_streets} -> "
                f"target={target.get('name') if target else None} "
                f"(score={score:.3f}, {reason})"
            )

            if not target or not tid:
                print(f"    SKIP: немає прийнятного НП (best score={score:.3f})")
                stats["skipped_no_target"] += 1
                continue

            stats["resolved"] += 1
            if dry_run:
                continue

            mstats = svc._merge_streets(sid, tid)
            stats["streets_relinked"] += mstats["relinked"]
            stats["streets_merged"] += mstats["merged"]

            id_map = {sid: tid}
            stats["refs_updated"] += svc._update_address_refs(id_map)
            stats["listings_updated"] += svc._update_unified_listings(id_map)

            db.cities.delete_one({"_id": stub["_id"]})
            stats["stubs_deleted"] += 1

        print("\nПідсумок:", stats)
        return True
    except Exception as e:
        print("Помилка міграції 054:", e)
        import traceback

        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    ok = run_migration(dry_run=args.dry_run)
    sys.exit(0 if ok else 1)
