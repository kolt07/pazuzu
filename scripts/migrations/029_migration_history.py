# -*- coding: utf-8 -*-
"""
Міграція 029: Колекція _migration_history для відстеження виконаних міграцій.

Створює системну колекцію _migration_history та опційно заповнює її записами
для міграцій 001-028 (припускаємо, що вони вже виконані на існуючих базах).
"""

from datetime import datetime, timezone

from config.settings import Settings
from data.database.connection import MongoDBConnection

# Міграції, що існували до впровадження _migration_history (для backfill)
_PRE_HISTORY_MIGRATIONS = [
    "001_create_database",
    "002_migrate_to_mongodb_collections",
    "003_olx_listings_collection",
    "005_geocode_cache_collection",
    "006_agent_temp_exports_collection",
    "007_agent_activity_log_collection",
    "008_scheduled_events_collection",
    "009_artifacts_collection",
    "010_export_daily_count_collection",
    "011_session_state_collection",
    "012_geography_collections",
    "013_migrate_addresses_to_references",
    "014_seed_regions",
    "015_olx_price_and_address_fields",
    "016_collection_knowledge_collection",
    "017_pipeline_templates_collection",
    "018_llm_feedback_collection",
    "019_unified_listings_collection",
    "020_unified_listings_area_fields",
    "021_unified_listings_price_per_area",
    "022_price_metrics_land_rules",
    "023_report_templates_collection",
    "024_prozorro_property_type_fix",
    "025_price_analytics_collection",
    "026_prozorro_auction_id_fix",
    "027_chat_sessions_collection",
    "028_property_usage_analysis_collection",
]


def run_migration() -> bool:
    """Створює колекцію _migration_history та backfill для існуючих міграцій."""
    try:
        Settings()
        MongoDBConnection.initialize(Settings())
        db = MongoDBConnection.get_database()
        coll = db["_migration_history"]
        # _id в MongoDB за замовчуванням унікальний, додатковий індекс не потрібен

        # Backfill: якщо колекція порожня або майже порожня, додаємо записи для 001-028
        # (припускаємо, що на існуючій базі вони вже виконані)
        existing = set(coll.distinct("_id"))
        backfill_count = 0
        now = datetime.now(timezone.utc).isoformat()
        for mig_id in _PRE_HISTORY_MIGRATIONS:
            if mig_id not in existing:
                coll.insert_one({
                    "_id": mig_id,
                    "applied_at": now,
                    "config_version_at_apply": "1.0",
                })
                backfill_count += 1

        if backfill_count > 0:
            print(f"Міграція 029: backfill {backfill_count} записів у _migration_history.")
        print("Міграція 029: колекція _migration_history створено/перевірено.")
        return True
    except Exception as e:
        print("Помилка міграції 029:", e)
        return False


if __name__ == "__main__":
    import sys
    sys.exit(0 if run_migration() else 1)
