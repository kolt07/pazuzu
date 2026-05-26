# -*- coding: utf-8 -*-
"""
Міграція 061: гомонім Іваничі (Волинська + Рівненська).

- Виправляє помилкову назву «Іваниничі» → «Іваничі» у Рівненській
- Додає «Іваничі» у довідник Волинської (відсутній у каталозі)
- Аліас «іваниничі» для пошуку в межах області

Запуск:
  py scripts/migrations/061_ensure_ivanichi_homonyms.py --dry-run
  py scripts/migrations/061_ensure_ivanichi_homonyms.py
"""

from __future__ import annotations

import argparse
import json
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.geography_repository import CitiesRepository, RegionsRepository
from utils.settlement_normalizer import normalize_settlement_key, normalize_settlement_name

CANONICAL_NAME = "Іваничі"
CANONICAL_KEY = normalize_settlement_key(CANONICAL_NAME)
TYPO_KEY = "іваниничі"

# Публічні довідкові дані (mista.ua / Вікіпедія) для с.м.т. Волинська
VOLYN_IVANICHI_META: Dict[str, Any] = {
    "population": 6918,
    "area_sq_km": 23.6,
    "settlement_status": "смт",
    "oblast_rayon_name": "Володимирський",
    "postal_code": "45300",
    "source": "migration_061",
}

RIVNE_IVANICHI_META: Dict[str, Any] = {
    "population": 232,
    "settlement_status": "село",
    "oblast_rayon_name": "Рівненський",
    "source": "migration_061",
}


def _merge_aliases(existing: Optional[List[str]], extra: List[str]) -> List[str]:
    seen = set()
    out: List[str] = []
    for raw in (existing or []) + extra:
        key = normalize_settlement_key(raw) or (raw or "").strip().lower()
        if not key or key in seen:
            continue
        seen.add(key)
        out.append(key)
    return out


def run_migration(dry_run: bool = False) -> bool:
    print("=" * 60)
    print("Міграція 061: Іваничі (Волинська + Рівненська)")
    print(f"Режим: {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    stats: Dict[str, Any] = {"dry_run": dry_run, "actions": []}

    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()
        regions_repo = RegionsRepository()
        cities_repo = CitiesRepository()

        volyn = regions_repo.find_by_name("Волинська")
        rivne = regions_repo.find_by_name("Рівненська")
        if not volyn or not rivne:
            raise RuntimeError("Не знайдено області Волинська або Рівненська в regions")

        volyn_id = str(volyn["_id"])
        rivne_id = str(rivne["_id"])

        # --- Рівненська: виправлення typo «Іваниничі» ---
        typo_doc = cities_repo.find_one(
            {
                "region_id": rivne_id,
                "$or": [
                    {"name_normalized": TYPO_KEY},
                    {"name": "Іваниничі"},
                    {"search_aliases": TYPO_KEY},
                ],
            }
        )
        if typo_doc:
            action = {
                "action": "rename_rivne_typo",
                "city_id": str(typo_doc["_id"]),
                "was": typo_doc.get("name"),
                "will_be": CANONICAL_NAME,
            }
            stats["actions"].append(action)
            if not dry_run:
                aliases = _merge_aliases(
                    typo_doc.get("search_aliases"),
                    [TYPO_KEY, CANONICAL_KEY, "Яневичі"],
                )
                updates = {
                    "name": CANONICAL_NAME,
                    "name_normalized": CANONICAL_KEY,
                    "search_aliases": aliases,
                    "updated_at": datetime.now(timezone.utc),
                }
                updates.update({k: v for k, v in RIVNE_IVANICHI_META.items() if v is not None})
                cities_repo.update_by_id(typo_doc["_id"], {"$set": updates})
        else:
            stats["actions"].append({"action": "rename_rivne_typo", "status": "not_found"})

        # --- Рівненська: створити канонічний запис, якщо відсутній у mista/каталозі ---
        rivne_doc = cities_repo.find_by_name_and_region(CANONICAL_NAME, rivne_id)
        if not rivne_doc:
            rivne_doc = cities_repo.find_one(
                {
                    "region_id": rivne_id,
                    "$or": [
                        {"name_normalized": TYPO_KEY},
                        {"name": "Іваниничі"},
                        {"search_aliases": CANONICAL_KEY},
                    ],
                }
            )
        if rivne_doc:
            stats["actions"].append(
                {
                    "action": "update_rivne",
                    "city_id": str(rivne_doc["_id"]),
                    "name": rivne_doc.get("name"),
                }
            )
            if not dry_run:
                aliases = _merge_aliases(
                    rivne_doc.get("search_aliases"),
                    [TYPO_KEY, CANONICAL_KEY, "Яневичі"],
                )
                updates = {
                    "name": CANONICAL_NAME,
                    "name_normalized": CANONICAL_KEY,
                    "search_aliases": aliases,
                    "updated_at": datetime.now(timezone.utc),
                }
                updates.update({k: v for k, v in RIVNE_IVANICHI_META.items() if v is not None})
                cities_repo.update_by_id(rivne_doc["_id"], {"$set": updates})
        else:
            stats["actions"].append({"action": "create_rivne", "name": CANONICAL_NAME, "region_id": rivne_id})
            if not dry_run:
                now = datetime.now(timezone.utc)
                doc = {
                    "name": CANONICAL_NAME,
                    "name_normalized": CANONICAL_KEY,
                    "region_id": rivne_id,
                    "search_aliases": [TYPO_KEY, "Яневичі"],
                    "created_at": now,
                    "updated_at": now,
                    **RIVNE_IVANICHI_META,
                }
                cities_repo.create(doc)

        # --- Волинська: створити або оновити канонічний запис ---
        volyn_doc = cities_repo.find_by_name_and_region(CANONICAL_NAME, volyn_id)
        if not volyn_doc:
            volyn_doc = cities_repo.find_one(
                {
                    "region_id": volyn_id,
                    "$or": [
                        {"name_normalized": TYPO_KEY},
                        {"search_aliases": CANONICAL_KEY},
                    ],
                }
            )

        if volyn_doc:
            action = {
                "action": "update_volyn",
                "city_id": str(volyn_doc["_id"]),
                "name": volyn_doc.get("name"),
            }
            stats["actions"].append(action)
            if not dry_run:
                aliases = _merge_aliases(volyn_doc.get("search_aliases"), [TYPO_KEY])
                updates = {
                    "name": CANONICAL_NAME,
                    "name_normalized": CANONICAL_KEY,
                    "search_aliases": aliases,
                    "updated_at": datetime.now(timezone.utc),
                }
                updates.update({k: v for k, v in VOLYN_IVANICHI_META.items() if v is not None})
                cities_repo.update_by_id(volyn_doc["_id"], {"$set": updates})
        else:
            action = {"action": "create_volyn", "name": CANONICAL_NAME, "region_id": volyn_id}
            stats["actions"].append(action)
            if not dry_run:
                now = datetime.now(timezone.utc)
                doc = {
                    "name": CANONICAL_NAME,
                    "name_normalized": CANONICAL_KEY,
                    "region_id": volyn_id,
                    "search_aliases": [TYPO_KEY],
                    "created_at": now,
                    "updated_at": now,
                    **VOLYN_IVANICHI_META,
                }
                cities_repo.create(doc)

        # --- Перевірка API-списку (як get_cities_by_region) ---
        from business.services.geography_service import GeographyService

        gs = GeographyService()
        volyn_cities = gs.get_cities_by_region(volyn_id)
        rivne_cities = gs.get_cities_by_region(rivne_id)
        volyn_keys = {normalize_settlement_key(c.get("name") or "") for c in volyn_cities}
        rivne_keys = {normalize_settlement_key(c.get("name") or "") for c in rivne_cities}
        stats["verify"] = {
            "volyn_has_ivanichi": CANONICAL_KEY in volyn_keys,
            "rivne_has_ivanichi": CANONICAL_KEY in rivne_keys,
            "rivne_has_typo": TYPO_KEY in rivne_keys,
            "volyn_catalog_size": len(volyn_cities),
            "rivne_catalog_size": len(rivne_cities),
        }

        print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
        return True
    except Exception as e:
        print("Помилка міграції 061:", e)
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
