# -*- coding: utf-8 -*-
"""
Міграція 053: колекції oblast_rayons і geo_circles; перенесення помилкових «районів» з cities.

- Створює oblast_rayons, geo_circles та індекси.
- Для кожного документа cities з назвою-районом області: створює запис у oblast_rayons.
- Оновлює address_refs (city → oblast_rayon) для prozorro_auctions та olx_listings.
- Якщо для міста немає вулиць у streets — видаляє cities.
- Якщо є вулиці — залишає cities як stub (is_oblast_rayon_stub, oblast_rayon_ref_id) для непорожніх streets;

Запуск:
  py scripts/migrations/053_geo_admin_units.py --dry-run
  py scripts/migrations/053_geo_admin_units.py
"""

from __future__ import annotations

import argparse
import sys
from datetime import datetime, timezone
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.geography_repository import OblastRayonsRepository
from utils.settlement_normalizer import is_district_only_name


def _rewrite_refs(refs_obj, old_to_new: dict):
    if isinstance(refs_obj, list):
        out = []
        changed = False
        for ref in refs_obj:
            if not isinstance(ref, dict):
                out.append(ref)
                continue
            nr, c = _rewrite_one_ref(dict(ref), old_to_new)
            out.append(nr)
            changed = changed or c
        return out, changed
    if isinstance(refs_obj, dict):
        return _rewrite_one_ref(dict(refs_obj), old_to_new)
    return refs_obj, False


def _rewrite_one_ref(ref: dict, old_to_new: dict):
    city = ref.get("city") or {}
    cid = str(city.get("_id") or "")
    if cid not in old_to_new:
        return ref, False
    meta = old_to_new[cid]
    new_ref = dict(ref)
    new_ref.pop("city", None)
    new_ref["oblast_rayon"] = {"_id": meta["id"], "name": meta["name"]}
    return new_ref, True


def run_migration(dry_run: bool = False) -> bool:
    print("=" * 60)
    print("Міграція 053: oblast_rayons, geo_circles, перенесення районів з cities")
    print(f"Режим: {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()

        rayons_repo = OblastRayonsRepository()
        rayons_repo._ensure_indexes()
        gc = db["geo_circles"]
        gc.create_index(
            [("scope_bucket", 1), ("kind", 1), ("name_normalized", 1)],
            unique=True,
        )
        gc.create_index("region_id")
        gc.create_index("parent_city_id")
        gc.create_index("kind")
        print("Індекси oblast_rayons / geo_circles перевірено/створено.")

        old_to_new: dict = {}
        deleted = 0
        stubbed = 0

        cursor = db.cities.find({"merged_into": {"$exists": False}})
        for city in cursor:
            name = city.get("name") or ""
            if not is_district_only_name(name):
                continue
            region_id = str(city.get("region_id") or "")
            if not region_id:
                continue

            oid = str(city["_id"])
            street_n = db.streets.count_documents({"city_id": oid})

            if dry_run:
                rz = rayons_repo.find_by_name_and_region(name, region_id)
                print(
                    f"  DRY: rayon={'ok' if rz else 'create'} name={name!r} "
                    f"region={region_id} streets={street_n}"
                )
                continue

            rz_doc = rayons_repo.find_or_create(name, region_id)
            old_to_new[oid] = {"id": str(rz_doc["_id"]), "name": rz_doc["name"]}

            if street_n == 0:
                db.cities.delete_one({"_id": city["_id"]})
                deleted += 1
            else:
                db.cities.update_one(
                    {"_id": city["_id"]},
                    {
                        "$set": {
                            "is_oblast_rayon_stub": True,
                            "oblast_rayon_ref_id": str(rz_doc["_id"]),
                            "updated_at": datetime.now(timezone.utc),
                        }
                    },
                )
                stubbed += 1

        if not dry_run and old_to_new:
            print(f"\nОновлення address_refs для {len(old_to_new)} id міст-районів…")

            for coll_name, path in (
                ("prozorro_auctions", "auction_data.address_refs"),
                ("olx_listings", "detail.address_refs"),
            ):
                parent_field = path.rsplit(".", 1)[0]
                updated = 0
                m = {f"{path}.city._id": {"$in": list(old_to_new.keys())}}
                for doc in db[coll_name].find(m, {"_id": 1, parent_field: 1}):
                    parent = doc
                    for part in parent_field.split("."):
                        parent = (parent or {}).get(part)
                    new_val, ch = _rewrite_refs(parent, old_to_new)
                    if not ch:
                        continue
                    db[coll_name].update_one({"_id": doc["_id"]}, {"$set": {path: new_val}})
                    updated += 1
                print(f"  {coll_name}: оновлено документів {updated}")

        print(
            f"\nПідсумок: map size {len(old_to_new)}, "
            f"deleted cities {deleted}, stubs (were streets) {stubbed}"
        )
        if dry_run:
            print("(dry-run — зміни в БД не записувались)")
        return True
    except Exception as e:
        print("Помилка міграції 053:", e)
        import traceback

        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    ap = argparse.ArgumentParser()
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    sys.exit(0 if run_migration(dry_run=args.dry_run) else 1)
