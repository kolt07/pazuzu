# -*- coding: utf-8 -*-
"""
Backfill-скрипт для семантичного індексу unified_listings.

Пакетно дістає з MongoDB документи без `vector_indexed_at` (або без правильного
hash), генерує ембединги через TEI (BAAI/bge-m3) і робить upsert у Qdrant.

Використання::

    py -m scripts.embeddings.build_vector_index --limit 1000
    py -m scripts.embeddings.build_vector_index --batch 64 --dry-run
    py -m scripts.embeddings.build_vector_index --source olx --limit 500

Скрипт ідемпотентний (повторний запуск пропускає вже проіндексовані документи).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any, Dict, List

from config.settings import Settings
from data.database.connection import MongoDBConnection


logger = logging.getLogger("build_vector_index")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill unified_listings → Qdrant")
    parser.add_argument("--limit", type=int, default=10_000, help="максимум документів за прохід")
    parser.add_argument("--batch", type=int, default=64, help="розмір пакета upsert/embed")
    parser.add_argument(
        "--source",
        type=str,
        default=None,
        choices=["olx", "prozorro"],
        help="опційний фільтр по джерелу",
    )
    parser.add_argument("--dry-run", action="store_true", help="не writeти у Qdrant, тільки порахувати")
    parser.add_argument("--verbose", action="store_true", help="детальні логи")
    return parser.parse_args()


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    settings = Settings()
    MongoDBConnection.initialize(settings)

    from data.repositories.unified_listings_repository import UnifiedListingsRepository
    from business.services.vector_index_service import VectorIndexService

    repo = UnifiedListingsRepository()
    svc = VectorIndexService.get_instance(settings)

    if not svc.is_configured:
        logger.error(
            "VectorIndexService is not configured (qdrant_host=%s, embeddings endpoint=%s). "
            "Перевір docker-compose та config.yaml.",
            settings.qdrant_host,
            settings.embeddings_endpoint,
        )
        return 2

    if not args.dry_run:
        if not svc.ensure_collections():
            logger.error("Qdrant ensure_collections failed — aborting.")
            return 3

    remaining = max(1, int(args.limit))
    batch_size = max(1, int(args.batch))
    total_seen = 0
    total_indexed = 0
    started = time.time()

    while remaining > 0:
        page_size = min(batch_size, remaining)
        docs: List[Dict[str, Any]] = repo.find_without_vector_index(limit=page_size, source=args.source)
        if not docs:
            break
        total_seen += len(docs)
        if args.dry_run:
            logger.info("[dry-run] would index batch of %d (%s)", len(docs), args.source or "any")
            remaining -= len(docs)
            if len(docs) < page_size:
                break
            continue
        try:
            indexed = svc.upsert_listings(docs)
        except Exception as e:
            logger.warning("upsert_listings failed for batch (%d): %s", len(docs), e)
            indexed = 0
        for d in docs:
            src = d.get("source")
            sid = d.get("source_id")
            if src and sid:
                try:
                    repo.set_vector_indexed(src, sid)
                except Exception:
                    pass
        total_indexed += indexed
        logger.info(
            "batch: docs=%d indexed=%d total_seen=%d total_indexed=%d",
            len(docs), indexed, total_seen, total_indexed,
        )
        remaining -= len(docs)
        if len(docs) < page_size:
            break

    elapsed = time.time() - started
    logger.info(
        "Done. seen=%d indexed=%d elapsed=%.1fs (mode=%s)",
        total_seen, total_indexed, elapsed, "dry-run" if args.dry_run else "live",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
