# -*- coding: utf-8 -*-
"""
Масова переобробка OLX-оголошень з невірним геоконтекстом.

Типові кейси:
- LLM витягнув лише вулицю («вул. Садова»), а НП є в картці;
- detail.location.city = «Одеса, Приморський» / «Київ, Голосіївський» (склейка) → геофільтр по НП пропускає оголошення;
- unified root city порожній при наявному LLM settlement (без resolved_locations).

За замовчуванням — збагачення + геокодування (без повторного LLM).
--resync-only — без Google API: нормалізує картку і пересинхронізує unified з LLM/картки/resolved.
--force-llm — повторний виклик LLM + гео.

Запуск:
  py scripts/reprocess_olx_geo_context.py --dry-run
  py scripts/reprocess_olx_geo_context.py --resync-only --dry-run
  py scripts/reprocess_olx_geo_context.py --resync-only --limit 100
  py scripts/reprocess_olx_geo_context.py --limit 100
  py scripts/reprocess_olx_geo_context.py --region Полтавська --limit 50
  py scripts/reprocess_olx_geo_context.py --url https://www.olx.ua/d/...
  py scripts/reprocess_olx_geo_context.py --url https://www.olx.ua/d/... --resync-only
  py scripts/reprocess_olx_geo_context.py --force-llm --limit 20
"""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from utils.stdio_utf8 import ensure_stdout_utf8

ensure_stdout_utf8()

from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.geo_context_reprocess_service import GeoContextReprocessService


def _print_previews(previews: list) -> None:
    if not previews:
        print("Кандидатів не знайдено.")
        return
    print(f"Приклади кандидатів ({len(previews)}):")
    for index, item in enumerate(previews, start=1):
        ctx = item.get("listing_context") or {}
        settlement = ctx.get("settlement") or "?"
        region = ctx.get("region") or "?"
        reasons = ", ".join(item.get("reasons") or [])
        print(f"\n{index}. {item.get('url', '')[:90]}")
        print(f"   Контекст: {settlement}, {region}")
        print(f"   Причини: {reasons}")
        print(f"   Було:  {item.get('current_query') or '—'}")
        print(f"   Стане: {item.get('enriched_query') or '—'}")


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Переобробка OLX з невірним геоконтекстом (склейка НП+район, street-only, empty root city)",
    )
    parser.add_argument("--limit", type=int, default=0, help="Макс. кількість оголошень (0 = без обмежень)")
    parser.add_argument("--region", type=str, default="", help="Фільтр за областю (напр. Полтавська)")
    parser.add_argument("--url", type=str, default="", help="Обробити одне оголошення за URL")
    parser.add_argument("--force-llm", action="store_true", help="Повторний LLM (кеш ігнорується) + гео")
    parser.add_argument(
        "--resync-only",
        action="store_true",
        help="Без Google Geocoding: нормалізувати картку + sync unified з існуючих resolved_locations",
    )
    parser.add_argument("--dry-run", action="store_true", help="Лише показати кандидатів")
    args = parser.parse_args()

    settings = Settings()
    MongoDBConnection.initialize(settings)
    service = GeoContextReprocessService(settings)
    region = args.region.strip() or None

    if args.url.strip():
        url = args.url.strip()
        if args.dry_run:
            doc = service.olx_repo.find_by_url(url)
            if not doc:
                print(f"Оголошення не знайдено: {url}")
                return
            from utils.address_geo_enrichment import analyze_geo_reprocess_candidate

            analysis = analyze_geo_reprocess_candidate(
                doc.get("search_data") or {},
                doc.get("detail") or {},
            )
            _print_previews([{"url": url, **analysis}])
            return

        result = service.reprocess_url(
            url,
            force_llm=args.force_llm,
            resync_only=args.resync_only,
            log_fn=print,
        )
        print(result)
        return

    limit = args.limit if args.limit > 0 else 0
    preview_limit = limit if limit > 0 else 20

    if args.dry_run:
        urls = service.collect_candidate_urls(limit=limit or preview_limit, region=region)
        print(f"Знайдено кандидатів: {len(urls)}")
        previews = service.preview_candidates(limit=preview_limit, region=region)
        _print_previews(previews)
        return

    urls = service.collect_candidate_urls(limit=limit, region=region)
    print(f"Кандидатів до обробки: {len(urls)}")
    if not urls:
        return

    stats = service.run_batch(
        urls=urls,
        force_llm=args.force_llm,
        resync_only=args.resync_only,
        dry_run=False,
        log_fn=print,
    )
    print(
        "Готово. "
        f"Відібрано: {stats['selected']}, "
        f"оновлено: {stats['updated']}, "
        f"пропущено: {stats['skipped']}, "
        f"помилок: {stats['errors']}"
        + (" (resync-only)" if args.resync_only else "")
    )


if __name__ == "__main__":
    main()
