# -*- coding: utf-8 -*-
"""
Перевірка стану структури БД: наявність очікуваних колекцій та виявлення «зайвих».

Очікувані колекції — ті, що використовуються репозиторіями та міграціями.
Запуск: py scripts/check_db_structure.py

Виводить:
- які очікувані колекції відсутні (потрібні міграції/ініціалізація);
- які колекції є в БД, але не в очікуваному списку (кандидати на ручне видалення, якщо не потрібні).
"""

from __future__ import annotations

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

# Очікувані колекції (репозиторії + службові)
EXPECTED_COLLECTIONS = frozenset({
    # Джерела та зведені дані
    "prozorro_auctions",
    "olx_listings",
    "raw_olx_listings",
    "raw_prozorro_auctions",
    "unified_listings",
    "llm_cache",
    # Аналітика та знання
    "collection_knowledge",
    "analytics_extracts",
    "price_analytics",
    "listing_analytics",
    "property_usage_analysis",
    # Географія
    "regions",
    "cities",
    "streets",
    "buildings",
    # ОНМ та кадастр
    "real_estate_objects",
    "cadastral_parcels",
    "cadastral_scraper_cells",
    "cadastral_parcel_location_index",
    "cadastral_parcel_clusters",
    # Користувачі, сесії, логи
    "users",
    "logs",
    "llm_exchange_logs",
    "llm_feedback",
    "chat_sessions",
    "session_state",
    "agent_activity_log",
    "agent_temp_exports",
    # Планувальник та експорт
    "scheduled_events",
    "artifacts",
    "export_daily_count",
    "pending_export",
    "pipeline_templates",
    # Конфіг та сервіси
    "app_data",
    "report_templates",
    "geocode_cache",
    "currency_rates",
    # Службові
    "_migration_history",
})


def main() -> int:
    try:
        from config.settings import Settings
        from data.database.connection import MongoDBConnection
    except ImportError as e:
        print("Помилка імпорту:", e, file=sys.stderr)
        return 1

    try:
        settings = Settings()
    except Exception as e:
        print("Помилка ініціалізації конфігу (перевірте .env/config):", e, file=sys.stderr)
        return 1

    MongoDBConnection.initialize(settings)
    db = MongoDBConnection.get_database()

    existing = set(db.list_collection_names())
    # Ігноруємо системні колекції
    existing = {c for c in existing if not c.startswith("system.")}

    missing = EXPECTED_COLLECTIONS - existing
    extra = existing - EXPECTED_COLLECTIONS

    ok = len(EXPECTED_COLLECTIONS - missing) == len(EXPECTED_COLLECTIONS)
    print("Перевірка структури БД")
    print("Очікуваних колекцій:", len(EXPECTED_COLLECTIONS))
    print("У БД (без system.*):", len(existing))
    print()

    if missing:
        print("Відсутні очікувані колекції (запустіть міграції або ініціалізацію):")
        for c in sorted(missing):
            print("  -", c)
        print()
    else:
        print("Усі очікувані колекції присутні.")
        print()

    if extra:
        print("Колекції в БД, що не в очікуваному списку (кандидати на ручне видалення, якщо не потрібні):")
        for c in sorted(extra):
            print("  -", c)
        print()
    else:
        print("Зайвих колекцій не виявлено.")
        print()

    return 0 if not missing else 1


if __name__ == "__main__":
    sys.exit(main())
