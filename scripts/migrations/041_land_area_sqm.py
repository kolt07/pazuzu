# -*- coding: utf-8 -*-
"""
Міграція 041: Площа землі в м² (land_area_sqm) замість гектарів.

Усі колекції переходять на зберігання площі земельної ділянки в м².
Відображення для землі: сотки = land_area_sqm/100, ціна за сотку = price_per_ha_uah/100.

Кроки:
1. unified_listings: land_area_ha -> land_area_sqm (значення * 10000), потім видалити land_area_ha.
2. olx_listings: detail.llm.land_area_sqm = land_area_ha * 10000 (якщо є land_area_ha).
3. llm_cache: result.land_area_sqm = result.land_area_ha * 10000 (якщо є).
4. Перезаповнити analytics_extracts з unified_listings.

Запуск: py scripts/migrations/041_land_area_sqm.py
"""

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration() -> bool:
    print("=" * 60)
    print("Міграція 041: Площа землі в м² (land_area_sqm)")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()

        # 1. unified_listings
        coll_ul = db["unified_listings"]
        cursor = coll_ul.find({"land_area_ha": {"$exists": True, "$ne": None}})
        count_ul = 0
        for doc in cursor:
            ha = doc.get("land_area_ha")
            if ha is None:
                continue
            try:
                sqm = float(ha) * 10000.0
            except (TypeError, ValueError):
                continue
            result = coll_ul.update_one(
                {"_id": doc["_id"]},
                {"$set": {"land_area_sqm": sqm}, "$unset": {"land_area_ha": ""}},
            )
            if result.modified_count:
                count_ul += 1
        print(f"unified_listings: оновлено {count_ul} записів (land_area_ha -> land_area_sqm).")

        # 2. olx_listings — detail.llm.land_area_ha -> land_area_sqm
        coll_olx = db["olx_listings"]
        cursor_olx = coll_olx.find({"detail.llm.land_area_ha": {"$exists": True, "$ne": None}})
        count_olx = 0
        for doc in cursor_olx:
            detail = doc.get("detail") or {}
            llm = detail.get("llm") or {}
            ha = llm.get("land_area_ha")
            if ha is None:
                continue
            try:
                sqm = float(ha) * 10000.0
            except (TypeError, ValueError):
                continue
            result = coll_olx.update_one(
                {"_id": doc["_id"]},
                {"$set": {"detail.llm.land_area_sqm": sqm}},
            )
            if result.modified_count:
                count_olx += 1
        print(f"olx_listings: додано detail.llm.land_area_sqm у {count_olx} записів.")

        # 3. llm_cache — result.land_area_ha -> result.land_area_sqm
        coll_llm = db["llm_cache"]
        cursor_llm = coll_llm.find({"result.land_area_ha": {"$exists": True, "$ne": None}})
        count_llm = 0
        for doc in cursor_llm:
            result_obj = doc.get("result") or {}
            ha = result_obj.get("land_area_ha")
            if ha is None:
                continue
            try:
                sqm = float(ha) * 10000.0
            except (TypeError, ValueError):
                continue
            res = coll_llm.update_one(
                {"_id": doc["_id"]},
                {"$set": {"result.land_area_sqm": sqm}},
            )
            if res.modified_count:
                count_llm += 1
        print(f"llm_cache: додано result.land_area_sqm у {count_llm} записів.")

        # 4. Перезаповнити analytics_extracts з unified_listings
        try:
            from business.services.analytics_extracts_populator import rebuild_analytics_extracts
            n = rebuild_analytics_extracts()
            print(f"analytics_extracts: перезаповнено {n} записів.")
        except Exception as e:
            print(f"Увага: перезаповнення analytics_extracts не виконано: {e}")
            print("  Виконайте вручну: PriceAnalyticsService.rebuild_all або rebuild_analytics_extracts().")

        print("\nМіграція 041 завершена успішно.")
        return True
    except Exception as e:
        print(f"Помилка міграції 041: {e}")
        import traceback
        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
