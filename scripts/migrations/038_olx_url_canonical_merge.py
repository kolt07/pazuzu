# -*- coding: utf-8 -*-
"""
Міграція 038: Об'єднання дублікатів OLX за канонічним URL.

Одне й те саме оголошення може мати різні URL через query-параметри
(search_reason=promoted vs organic). Канонічна форма — без query.
Міграція: об'єднує дублікати в olx_listings, оновлює source_id у unified_listings,
listing_analytics, real_estate_objects.
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
from utils.olx_url import normalize_olx_listing_url


def run_migration() -> bool:
    """Точка входу для run_migrations.py."""
    print("=" * 60)
    print("Міграція 038: Об'єднання дублікатів OLX за канонічним URL")
    print("=" * 60)

    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        db = MongoDBConnection.get_database()

        olx_coll = db["olx_listings"]
        unified_coll = db["unified_listings"]
        analytics_coll = db["listing_analytics"]
        reo_coll = db["real_estate_objects"]

        # 1. olx_listings: знайти дублікати (url з ?)
        urls_with_query = list(olx_coll.find({"url": {"$regex": r"\?"}}, {"url": 1, "detail": 1}))
        print(f"Оголошень OLX з query в URL: {len(urls_with_query)}")

        merged_olx = 0
        updated_olx = 0
        for doc in urls_with_query:
            old_url = doc.get("url")
            if not old_url or "?" not in old_url:
                continue
            canonical = normalize_olx_listing_url(old_url)
            if not canonical or canonical == old_url:
                continue

            existing = olx_coll.find_one({"url": canonical})
            if existing:
                # Дублікат: зберігаємо той, що має більше даних (detail)
                existing_len = len(str(existing.get("detail") or ""))
                current_len = len(str(doc.get("detail") or ""))
                if current_len > existing_len:
                    # Поточний повніший — оновлюємо існуючий і видаляємо старий
                    olx_coll.delete_one({"_id": existing["_id"]})
                    olx_coll.update_one(
                        {"_id": doc["_id"]},
                        {"$set": {"url": canonical}},
                    )
                    merged_olx += 1
                else:
                    # Існуючий повніший — видаляємо поточний
                    olx_coll.delete_one({"_id": doc["_id"]})
                    merged_olx += 1
            else:
                # Немає канонічного — просто оновлюємо url
                olx_coll.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"url": canonical}},
                )
                updated_olx += 1

        print(f"  olx_listings: об'єднано {merged_olx}, оновлено url {updated_olx}")

        # 2. unified_listings: source_id з query -> canonical (можливі дублікати)
        unified_with_query = list(
            unified_coll.find({"source": "olx", "source_id": {"$regex": r"\?"}})
        )
        unified_count = 0
        for doc in unified_with_query:
            old_sid = doc.get("source_id")
            if not old_sid or "?" not in old_sid:
                continue
            canonical = normalize_olx_listing_url(old_sid)
            if not canonical or canonical == old_sid:
                continue
            existing = unified_coll.find_one({"source": "olx", "source_id": canonical})
            if existing and str(existing["_id"]) != str(doc["_id"]):
                # Вже є запис з canonical — видаляємо дублікат
                unified_coll.delete_one({"_id": doc["_id"]})
                unified_count += 1
            else:
                result = unified_coll.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"source_id": canonical, "page_url": canonical}},
                )
                if result.modified_count > 0:
                    unified_count += 1
        print(f"  unified_listings: оновлено/видалено {unified_count}")

        # 3. listing_analytics: source_id for olx
        analytics_updated = list(
            analytics_coll.find({"source": "olx", "source_id": {"$regex": r"\?"}}, {"_id": 1, "source_id": 1})
        )
        analytics_count = 0
        for doc in analytics_updated:
            old_sid = doc.get("source_id")
            if not old_sid or "?" not in old_sid:
                continue
            canonical = normalize_olx_listing_url(old_sid)
            if not canonical or canonical == old_sid:
                continue
            # Можливі дублікати: olx:url1 та olx:url2 -> обидва olx:canonical
            existing = analytics_coll.find_one({"source": "olx", "source_id": canonical})
            if existing and str(existing["_id"]) != str(doc["_id"]):
                analytics_coll.delete_one({"_id": doc["_id"]})
            else:
                analytics_coll.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"source_id": canonical}},
                )
                analytics_count += 1
        print(f"  listing_analytics: оновлено/видалено {analytics_count}")

        # 4. real_estate_objects: source_listing_ids
        reo_cursor = reo_coll.find({"source_listing_ids": {"$exists": True, "$ne": []}})
        reo_count = 0
        for doc in reo_cursor:
            sids = doc.get("source_listing_ids") or []
            if not isinstance(sids, list):
                continue
            changed = False
            new_sids = []
            seen = set()
            for item in sids:
                if not isinstance(item, dict):
                    new_sids.append(item)
                    continue
                src = item.get("source")
                sid = item.get("source_id")
                if src != "olx" or not sid or "?" not in sid:
                    key = (src, sid)
                    if key not in seen:
                        seen.add(key)
                        new_sids.append(item)
                    continue
                canonical = normalize_olx_listing_url(sid)
                if canonical:
                    key = ("olx", canonical)
                    if key not in seen:
                        seen.add(key)
                        new_sids.append({"source": "olx", "source_id": canonical})
                        changed = True
                    else:
                        changed = True
            if changed and new_sids:
                reo_coll.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"source_listing_ids": new_sids}},
                )
                reo_count += 1
        print(f"  real_estate_objects: оновлено source_listing_ids {reo_count}")

        print("\nМіграція 038 завершена успішно.")
        return True

    except Exception as e:
        print(f"Помилка міграції: {e}")
        import traceback
        traceback.print_exc()
        return False


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
