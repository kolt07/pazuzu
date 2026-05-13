# -*- coding: utf-8 -*-
"""
Фонова національна кластеризація кадастрових ділянок по областях (партиції).
"""

from __future__ import annotations

import logging
from typing import Any, Dict

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.cadastral_cluster_build_jobs_repository import (
    CadastralClusterBuildJobRepository,
)
from data.repositories.cadastral_parcel_clusters_repository import (
    CadastralParcelClustersRepository,
)
from data.repositories.cadastral_parcel_location_index_repository import (
    CadastralParcelLocationIndexRepository,
)
from data.repositories.cadastral_parcels_repository import CadastralParcelsRepository
from business.services.cadastral_clustering_service import CadastralClusteringService

logger = logging.getLogger(__name__)


def run_national_cluster_build_job(job_id: str, min_cluster_size: int = 2) -> Dict[str, Any]:
    """
    Для кожної області з location_index: видалити старі кластери партиції,
    завантажити ділянки, побудувати кластери. Прогрес — у cadastral_cluster_build_jobs.
    """
    settings = Settings()
    MongoDBConnection.initialize(settings)
    jobs = CadastralClusterBuildJobRepository()
    max_per = max(10_000, int(getattr(settings, "cadastral_national_partition_max_parcels", 120000)))

    loc = CadastralParcelLocationIndexRepository()
    parcels_repo = CadastralParcelsRepository()
    clustering = CadastralClusteringService(
        parcels_repo=parcels_repo,
        clusters_repo=CadastralParcelClustersRepository(),
    )

    parts = loc.distinct_oblast_codes()
    jobs.patch_job(
        job_id,
        {
            "status": "running",
            "partitions_total": len(parts),
            "partitions_done": 0,
            "parcels_processed": 0,
            "clusters_upserted": 0,
            "message": "Старт національної кластеризації",
        },
    )

    total_p = 0
    total_cl = 0
    try:
        for i, pk in enumerate(parts):
            nums = loc.cadastral_numbers_for_oblast(pk)
            truncated = len(nums) > max_per
            if truncated:
                nums = nums[:max_per]
            parcels = parcels_repo.find_many_by_cadastral_numbers(nums)
            res = clustering.build_clusters_partition(
                pk,
                parcels,
                min_cluster_size=max(1, int(min_cluster_size or 2)),
            )
            total_p += int(res.get("parcels_processed") or 0)
            total_cl += int(res.get("clusters_created") or 0)
            jobs.patch_job(
                job_id,
                {
                    "partitions_done": i + 1,
                    "parcels_processed": total_p,
                    "clusters_upserted": total_cl,
                    "current_partition": pk,
                    "message": (
                        f"Область {pk}: ділянок {res.get('parcels_processed')}, "
                        f"кластерів {res.get('clusters_created')}"
                        + (" (обрізано за лімітом партиції)" if truncated else "")
                    ),
                },
            )

        jobs.patch_job(
            job_id,
            {
                "status": "done",
                "message": (
                    f"Готово. Партицій: {len(parts)}, ділянок: {total_p}, кластерів створено: {total_cl}"
                ),
            },
        )
        return {"ok": True, "partitions": len(parts), "parcels_processed": total_p, "clusters": total_cl}
    except Exception as e:
        logger.exception("run_national_cluster_build_job failed job_id=%s", job_id)
        jobs.patch_job(job_id, {"status": "error", "message": str(e)[:2000]})
        raise
