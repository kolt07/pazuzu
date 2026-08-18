# -*- coding: utf-8 -*-
"""
Міграція 065: канонічні назви НП з mista-списку (Дніпро замість Дніпропетровськ тощо).

Проблема: детальна сторінка / URL mista.ua часто містять стару назву, а список пошуку —
сучасну. Імпорт брав URL/detail → у cities потрапляли «Дніпропетровськ», «Кіровоград»,
а оголошення мають «Дніпро» / «Кропивницький» → геофільтр нічого не знаходив.

Кроки:
1. Повторно зібрати назви зі списку mista (без втрати parsed-статусу)
2. Реімпорт у cities з пріоритетом list-name + search_aliases (стара назва)

Запуск:
  py scripts/migrations/065_sync_mista_list_canonical_names.py --dry-run
  py scripts/migrations/065_sync_mista_list_canonical_names.py
  py scripts/migrations/065_sync_mista_list_canonical_names.py --skip-scrape
"""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path
from typing import Any, Dict, Optional

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")

project_root = Path(__file__).resolve().parent.parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration(
    *,
    dry_run: bool = False,
    skip_scrape: bool = False,
    region_filter: Optional[str] = None,
    max_pages: int = 0,
) -> bool:
    print("=" * 60)
    print("Міграція 065: sync канонічних назв НП зі списку mista")
    print(f"Режим: {'DRY-RUN' if dry_run else 'APPLY'}")
    print("=" * 60)

    Settings()
    MongoDBConnection.initialize(Settings())

    stats: Dict[str, Any] = {
        "dry_run": dry_run,
        "skip_scrape": skip_scrape,
        "list_rows": 0,
        "import": {},
    }

    if not skip_scrape:
        from scripts.mista_scraper.run_scrape import scrape_list_by_regions

        if dry_run:
            print("[dry-run] Пропуск HTTP scrape списку (лише імпорт з наявних raw).")
        else:
            print("Скрапінг списку mista.ua (оновлення name, статус parsed зберігається)…")
            stats["list_rows"] = scrape_list_by_regions(
                region_filter=region_filter,
                max_pages_per_region=max_pages,
                log_fn=lambda m: print(f"[mista] {m}", flush=True),
            )
            print(f"Оновлено рядків списку: {stats['list_rows']}")

    from business.services.mista_settlement_import_service import MistaSettlementImportService

    svc = MistaSettlementImportService()
    if dry_run:
        # Показуємо лише кейси, де list-name ≠ поточна cities.name
        from utils.settlement_normalizer import (
            normalize_settlement_key,
            resolve_mista_canonical_and_aliases,
            settlement_name_from_mista_url,
        )

        preview = []
        for doc in svc.raw_repo.find_for_import(limit=0):
            parsed = svc._effective_parsed(doc)
            canon, aliases = resolve_mista_canonical_and_aliases(
                list_name=doc.get("name"),
                detail_name=parsed.get("name"),
                mista_url=doc.get("mista_url") or parsed.get("mista_url"),
                former_names=parsed.get("former_names") or [],
            )
            url_name = settlement_name_from_mista_url(doc.get("mista_url"))
            if not canon or not url_name:
                continue
            if normalize_settlement_key(canon) == normalize_settlement_key(url_name):
                continue
            existing = None
            if doc.get("mista_url"):
                existing = svc.cities_repo.find_one({
                    **svc.cities_repo._active_city_filter(),
                    "mista_url": doc["mista_url"],
                })
            if existing and normalize_settlement_key(existing.get("name") or "") == normalize_settlement_key(
                canon
            ):
                continue
            preview.append({
                "url": doc.get("mista_url"),
                "canonical": canon,
                "url_slug_name": url_name,
                "current_city": (existing or {}).get("name"),
                "aliases": aliases,
            })
            if len(preview) >= 40:
                break
        stats["rename_preview"] = preview
        print(f"Кандидатів на перейменування (перші {len(preview)}):")
        print(json.dumps(preview[:15], ensure_ascii=False, indent=2))
    else:
        stats["import"] = svc.run_import(dry_run=False)
        print("Імпорт:", json.dumps(stats["import"], ensure_ascii=False, indent=2))

        # Швидка перевірка ключових перейменувань
        checks = ["Дніпро", "Кропивницький", "Кам'янське", "Самар", "Покров"]
        for name in checks:
            doc = svc.cities_repo.find_one({
                **svc.cities_repo._active_city_filter(),
                "name": name,
            })
            print(f"  check {name}: {'OK' if doc else 'MISSING'}", flush=True)

    print(json.dumps(stats, ensure_ascii=False, indent=2, default=str))
    print("=" * 60)
    print("Міграція 065 завершена")
    return True


def main() -> int:
    parser = argparse.ArgumentParser(description="Міграція 065: sync mista list canonical names")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--skip-scrape",
        action="store_true",
        help="Не ходити в mista, лише реімпорт з raw_mista_settlements",
    )
    parser.add_argument("--region", type=str, default="", help="Підрядок області для scrape")
    parser.add_argument("--pages", type=int, default=0, help="Макс. сторінок на область (0=всі)")
    args = parser.parse_args()
    ok = run_migration(
        dry_run=args.dry_run,
        skip_scrape=args.skip_scrape,
        region_filter=args.region or None,
        max_pages=args.pages,
    )
    return 0 if ok else 1


if __name__ == "__main__":
    sys.exit(main())
