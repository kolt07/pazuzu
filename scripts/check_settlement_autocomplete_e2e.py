# -*- coding: utf-8 -*-
"""
E2E-перевірка autocomplete НП (як у mini-app): послідовний ввід префікса.

Запуск у Docker:
  docker exec -w /app pazuzu-app python3 scripts/check_settlement_autocomplete_e2e.py
"""
from __future__ import annotations

import sys
from pathlib import Path

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.geography_service import GeographyService


def main() -> int:
    Settings()
    MongoDBConnection.initialize(Settings())
    gs = GeographyService()
    catalog_size = gs.cities_repo.collection.count_documents(gs.cities_repo._active_city_filter())
    print(f"catalog_size (active cities): {catalog_size}\n")
    if catalog_size < 100:
        print("УВАГА: довідник майже порожній. Запустіть:")
        print("  docker exec -w /app pazuzu-app python3 scripts/mista_scraper/run_import.py")
        print("  docker exec -w /app pazuzu-app python3 scripts/migrations/061_ensure_ivanichi_homonyms.py")
        return 1

    sequences = [
        ["Н", "Но", "Нов", "Ново", "Новов", "Новово", "Нововол"],
        ["І", "Ів", "Іва", "Іван", "Івани", "Іванич", "Іваничі"],
    ]
    ok = True
    for seq in sequences:
        print("---", seq[0][:20], "... ---")
        prev = 0
        for q in seq:
            opts = gs.search_settlements_catalog(q, limit=25)
            n = len(opts)
            labels = [o.get("picker_label") or o.get("name") for o in opts[:3]]
            print(f"  {q!r:12} -> {n:3}  {labels}")
            prev = n
        print()

    iv = gs.search_settlements_catalog("Іваничі", limit=25)
    regions = {o.get("region") for o in iv}
    print("Іваничі regions:", sorted(regions))
    if not any("Волинськ" in (r or "") for r in regions) or not any("Рівненськ" in (r or "") for r in regions):
        print("FAIL: очікувались Волинська і Рівненська Іваничі, отримано:", regions)
        ok = False
    else:
        print("OK: обидва гомоніми Іваничі")

    MongoDBConnection.close()
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
