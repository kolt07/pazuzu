# -*- coding: utf-8 -*-
"""
Backfill-скрипт для семантичного індексу кадастрових ділянок та їх кластерів.

Окремий entrypoint, бо кадастр оновлюється рідше за unified_listings і має
іншу пайплайн (скрапер kadastrova-karta.com), тож backfill зручніше робити
вручну/за розкладом.

Використання::

    py -m scripts.embeddings.build_cadastral_vector_index --limit 5000
    py -m scripts.embeddings.build_cadastral_vector_index --batch 64 --clusters-only
    py -m scripts.embeddings.build_cadastral_vector_index --dry-run

`--clusters-only` / `--parcels-only` дозволяють робити по черзі (cluster-семантика
зазвичай корисніша, але парсели — джерело правди).
"""
from __future__ import annotations

import argparse
import logging
import sys
import time
from typing import Any, Dict, List

from config.settings import Settings
from data.database.connection import MongoDBConnection


logger = logging.getLogger("build_cadastral_vector_index")


def _parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Backfill cadastral_parcels + clusters → Qdrant")
    parser.add_argument("--limit", type=int, default=20_000, help="максимум документів за прохід (на колекцію)")
    parser.add_argument("--batch", type=int, default=64, help="розмір пакета upsert/embed")
    parser.add_argument("--parcels-only", action="store_true", help="індексувати тільки парсели")
    parser.add_argument("--clusters-only", action="store_true", help="індексувати тільки кластери")
    parser.add_argument("--dry-run", action="store_true", help="не writeти у Qdrant, тільки порахувати")
    parser.add_argument("--verbose", action="store_true", help="детальні логи")
    return parser.parse_args()


def _index_parcels(svc, batch_size: int, limit: int, dry_run: bool) -> Dict[str, int]:
    from data.database.connection import MongoDBConnection

    db = MongoDBConnection.get_database()
    collection = db.get_collection("cadastral_parcels")
    cursor = collection.find(
        {"$or": [{"vector_indexed_at": {"$exists": False}}, {"vector_indexed_at": None}]},
    ).limit(max(1, int(limit)))

    batch: List[Dict[str, Any]] = []
    total_seen = 0
    total_indexed = 0
    cad_numbers: List[str] = []
    for doc in cursor:
        batch.append(doc)
        cad_numbers.append((doc.get("cadastral_number") or "").strip())
        total_seen += 1
        if len(batch) >= batch_size:
            if dry_run:
                logger.info("[dry-run] would index parcels batch of %d", len(batch))
            else:
                indexed = svc.upsert_parcels(batch)
                total_indexed += indexed
                if cad_numbers:
                    from datetime import datetime, timezone

                    now = datetime.now(timezone.utc)
                    try:
                        collection.update_many(
                            {"cadastral_number": {"$in": [c for c in cad_numbers if c]}},
                            {"$set": {"vector_indexed_at": now}},
                        )
                    except Exception as e:
                        logger.warning("vector_indexed_at update failed: %s", e)
                logger.info("parcels batch: %d -> indexed=%d (total_indexed=%d)", len(batch), indexed, total_indexed)
            batch = []
            cad_numbers = []

    if batch and not dry_run:
        indexed = svc.upsert_parcels(batch)
        total_indexed += indexed
        if cad_numbers:
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc)
            try:
                collection.update_many(
                    {"cadastral_number": {"$in": [c for c in cad_numbers if c]}},
                    {"$set": {"vector_indexed_at": now}},
                )
            except Exception as e:
                logger.warning("vector_indexed_at update failed: %s", e)
        logger.info("parcels tail batch: %d -> indexed=%d", len(batch), indexed)
    elif batch:
        logger.info("[dry-run] would index parcels tail of %d", len(batch))

    return {"seen": total_seen, "indexed": total_indexed}


def _index_clusters(svc, batch_size: int, limit: int, dry_run: bool) -> Dict[str, int]:
    from data.database.connection import MongoDBConnection

    db = MongoDBConnection.get_database()
    collection = db.get_collection("cadastral_parcel_clusters")
    cursor = collection.find(
        {"$or": [{"vector_indexed_at": {"$exists": False}}, {"vector_indexed_at": None}]},
    ).limit(max(1, int(limit)))

    batch: List[Dict[str, Any]] = []
    cluster_ids: List[str] = []
    total_seen = 0
    total_indexed = 0
    for doc in cursor:
        batch.append(doc)
        cluster_ids.append((doc.get("cluster_id") or "").strip())
        total_seen += 1
        if len(batch) >= batch_size:
            if dry_run:
                logger.info("[dry-run] would index clusters batch of %d", len(batch))
            else:
                indexed = svc.upsert_clusters(batch)
                total_indexed += indexed
                if cluster_ids:
                    from datetime import datetime, timezone

                    now = datetime.now(timezone.utc)
                    try:
                        collection.update_many(
                            {"cluster_id": {"$in": [c for c in cluster_ids if c]}},
                            {"$set": {"vector_indexed_at": now}},
                        )
                    except Exception as e:
                        logger.warning("clusters vector_indexed_at update failed: %s", e)
                logger.info("clusters batch: %d -> indexed=%d (total_indexed=%d)", len(batch), indexed, total_indexed)
            batch = []
            cluster_ids = []

    if batch and not dry_run:
        indexed = svc.upsert_clusters(batch)
        total_indexed += indexed
        if cluster_ids:
            from datetime import datetime, timezone

            now = datetime.now(timezone.utc)
            try:
                collection.update_many(
                    {"cluster_id": {"$in": [c for c in cluster_ids if c]}},
                    {"$set": {"vector_indexed_at": now}},
                )
            except Exception as e:
                logger.warning("clusters vector_indexed_at update failed: %s", e)
        logger.info("clusters tail batch: %d -> indexed=%d", len(batch), indexed)
    elif batch:
        logger.info("[dry-run] would index clusters tail of %d", len(batch))

    return {"seen": total_seen, "indexed": total_indexed}


def main() -> int:
    args = _parse_args()
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )

    if args.parcels_only and args.clusters_only:
        logger.error("--parcels-only та --clusters-only взаємовиключні")
        return 1

    settings = Settings()
    MongoDBConnection.initialize(settings)

    from business.services.vector_index_service import VectorIndexService

    svc = VectorIndexService.get_instance(settings)
    if not svc.is_configured:
        logger.error(
            "VectorIndexService is not configured (qdrant_host=%s, embeddings endpoint=%s).",
            settings.qdrant_host,
            settings.embeddings_endpoint,
        )
        return 2
    if not args.dry_run and not svc.ensure_collections():
        logger.error("Qdrant ensure_collections failed — aborting.")
        return 3

    started = time.time()
    parcel_stats = {"seen": 0, "indexed": 0}
    cluster_stats = {"seen": 0, "indexed": 0}
    if not args.clusters_only:
        parcel_stats = _index_parcels(svc, args.batch, args.limit, args.dry_run)
    if not args.parcels_only:
        cluster_stats = _index_clusters(svc, args.batch, args.limit, args.dry_run)

    elapsed = time.time() - started
    logger.info(
        "Done. parcels seen=%d indexed=%d; clusters seen=%d indexed=%d; elapsed=%.1fs (mode=%s)",
        parcel_stats["seen"], parcel_stats["indexed"],
        cluster_stats["seen"], cluster_stats["indexed"],
        elapsed,
        "dry-run" if args.dry_run else "live",
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
