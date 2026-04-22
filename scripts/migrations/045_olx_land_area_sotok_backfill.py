# -*- coding: utf-8 -*-
"""
Міграція 045: виправлення масштабу площі землі OLX за згадками "соток/сотих".

Що робить:
1. Перевіряє заголовок + опис оголошення на наявність площі в сотках.
2. Нормалізує `detail.llm.land_area_sqm/land_area_ha` в `olx_listings`.
3. Оновлює `land_area_sqm` і цінові метрики в `unified_listings` для цього OLX URL.
4. Перераховує агреговану аналітику (`analytics_extracts`, price indicators, aggregates).

Запуск у docker (з кореня проєкту):
  docker compose exec app py scripts/migrations/045_olx_land_area_sotok_backfill.py
"""

import sys
from pathlib import Path
from typing import Any, Dict

PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.price_analytics_service import PriceAnalyticsService
from utils.land_area_utils import (
    coerce_land_area_sqm,
    extract_sotok_area_sqm,
    should_fix_land_area_sqm_by_sotok,
)
from utils.price_metrics import compute_price_metrics


def _build_listing_text(doc: Dict[str, Any]) -> str:
    search = doc.get("search_data") or {}
    detail = doc.get("detail") or {}
    title = (search.get("title") or "").strip()
    description = (detail.get("description") or "").strip()
    return "\n".join(part for part in (title, description) if part)


def _recompute_unified_fields(unified_doc: Dict[str, Any], land_area_sqm: float) -> Dict[str, Any]:
    metrics = compute_price_metrics(
        total_price_uah=unified_doc.get("price_uah"),
        building_area_sqm=unified_doc.get("building_area_sqm"),
        land_area_sqm=land_area_sqm,
        uah_per_usd=unified_doc.get("currency_rate"),
    )
    return {
        "land_area_sqm": land_area_sqm,
        "price_per_m2_uah": metrics.get("price_per_m2_uah"),
        "price_per_m2_usd": metrics.get("price_per_m2_usd"),
        "price_per_ha_uah": metrics.get("price_per_ha_uah"),
        "price_per_ha_usd": metrics.get("price_per_ha_usd"),
    }


def run_migration() -> bool:
    print("=" * 60)
    print("Міграція 045: backfill площі землі OLX за сотками")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()
        olx_coll = db["olx_listings"]
        unified_coll = db["unified_listings"]

        query = {
            "$or": [
                {"search_data.title": {"$regex": "сот", "$options": "i"}},
                {"detail.description": {"$regex": "сот", "$options": "i"}},
            ]
        }
        projection = {
            "_id": 1,
            "url": 1,
            "search_data.title": 1,
            "detail.description": 1,
            "detail.llm.land_area_sqm": 1,
            "detail.llm.land_area_ha": 1,
        }

        scanned = 0
        fixed_olx = 0
        fixed_unified = 0
        skipped_without_url = 0

        cursor = olx_coll.find(query, projection=projection)
        for doc in cursor:
            scanned += 1
            listing_text = _build_listing_text(doc)
            expected_sqm = extract_sotok_area_sqm(listing_text)
            if expected_sqm <= 0:
                continue

            detail = doc.get("detail") or {}
            llm = detail.get("llm") or {}
            current_sqm = coerce_land_area_sqm(llm.get("land_area_sqm"), llm.get("land_area_ha"))
            if not should_fix_land_area_sqm_by_sotok(current_sqm, expected_sqm):
                continue

            land_area_ha = expected_sqm / 10000.0
            url = doc.get("url")
            if not url:
                skipped_without_url += 1
                continue

            unified_doc = unified_coll.find_one(
                {"source": "olx", "source_id": url},
                projection={
                    "_id": 1,
                    "price_uah": 1,
                    "currency_rate": 1,
                    "building_area_sqm": 1,
                },
            )

            olx_update_set: Dict[str, Any] = {
                "detail.llm.land_area_sqm": expected_sqm,
                "detail.llm.land_area_ha": land_area_ha,
            }
            if unified_doc:
                unified_fields = _recompute_unified_fields(unified_doc, expected_sqm)
                olx_update_set["detail.price_metrics"] = compute_price_metrics(
                    total_price_uah=unified_doc.get("price_uah"),
                    building_area_sqm=unified_doc.get("building_area_sqm"),
                    land_area_sqm=expected_sqm,
                    uah_per_usd=unified_doc.get("currency_rate"),
                )
                unified_result = unified_coll.update_one(
                    {"_id": unified_doc["_id"]},
                    {"$set": unified_fields},
                )
                if unified_result.modified_count:
                    fixed_unified += 1

            olx_result = olx_coll.update_one({"_id": doc["_id"]}, {"$set": olx_update_set})
            if olx_result.modified_count:
                fixed_olx += 1

        print(f"Перевірено OLX документів: {scanned}")
        print(f"Виправлено olx_listings: {fixed_olx}")
        print(f"Оновлено unified_listings: {fixed_unified}")
        if skipped_without_url:
            print(f"Пропущено без url: {skipped_without_url}")

        if fixed_olx > 0 or fixed_unified > 0:
            analytics_counts = PriceAnalyticsService().rebuild_all()
            print(f"Аналітика перерахована: {analytics_counts}")
        else:
            print("Змін не виявлено, перерахунок аналітики пропущено.")

        print("\nМіграція 045 завершена успішно.")
        return True
    except Exception as e:
        print(f"Помилка міграції 045: {e}")
        import traceback

        traceback.print_exc()
        return False
    finally:
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
