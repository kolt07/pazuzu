# -*- coding: utf-8 -*-
"""
Повторне розпізнавання: відбір з raw-колекцій, прогін через LLM, геокодування,
синхронізацію unified та пошук об'єктів нерухомого майна.

Якщо увімкнена черга Celery (task_queue_enabled) — OLX/ProZorro LLM ставляться в
llm_processing, як звичайне оновлення даних (паралельно у pazuzu-llm-worker).
Інакше — послідовна обробка в поточному процесі (fallback).
"""

from __future__ import annotations

import logging
import time
import uuid
from typing import Any, Callable, Dict, List, Optional, Set, Tuple

from config.settings import Settings
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
from data.repositories.raw_prozorro_auctions_repository import RawProzorroAuctionsRepository
from business.services.batch_processing.filters import (
    BatchJobFilters,
    build_raw_olx_query,
    build_raw_prozorro_query,
    get_unified_source_ids_by_status,
)
from business.services.prozorro_service import ProZorroService
from business.services.source_data_load_service import (
    _promote_raw_prozorro_to_main,
    _select_prozorro_ids_for_llm,
)
from business.services.task_queue_service import TaskQueueService
from business.services.unified_listings_service import UnifiedListingsService
from business.services.llm_processing_regions_service import is_region_enabled_for_llm

logger = logging.getLogger(__name__)

ProgressFn = Optional[Callable[[Dict[str, Any]], None]]


class RecognitionReprocessService:
    """Групове повторне розпізнавання з raw-колекцій."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.raw_olx_repo = RawOlxListingsRepository()
        self.raw_prozorro_repo = RawProzorroAuctionsRepository()
        self.olx_repo = OlxListingsRepository()
        self.prozorro_repo = ProZorroAuctionsRepository()

    def collect_olx_urls(self, filters: BatchJobFilters) -> List[str]:
        """Відбирає URL з raw_olx_listings."""
        query = build_raw_olx_query(filters)
        limit = filters.limit if filters.limit and filters.limit > 0 else 0
        cursor = self.raw_olx_repo.collection.find(query, {"url": 1}).sort("loaded_at", -1)
        if limit:
            cursor = cursor.limit(limit)

        allowed: Optional[Set[Tuple[str, str]]] = None
        if filters.status:
            allowed = get_unified_source_ids_by_status(filters.status, sources=["olx"])

        urls: List[str] = []
        for doc in cursor:
            url = doc.get("url")
            if not url:
                continue
            if allowed is not None and ("olx", url) not in allowed:
                continue
            urls.append(url)
        return list(dict.fromkeys(urls))

    def collect_prozorro_ids(self, filters: BatchJobFilters) -> List[str]:
        """Відбирає auction_id з raw_prozorro_auctions."""
        query = build_raw_prozorro_query(filters)
        limit = filters.limit if filters.limit and filters.limit > 0 else 0
        cursor = self.raw_prozorro_repo.collection.find(query, {"auction_id": 1}).sort("loaded_at", -1)
        if limit:
            cursor = cursor.limit(limit)

        allowed: Optional[Set[Tuple[str, str]]] = None
        if filters.status:
            allowed = get_unified_source_ids_by_status(filters.status, sources=["prozorro"])

        ids: List[str] = []
        for doc in cursor:
            aid = doc.get("auction_id")
            if not aid:
                continue
            if allowed is not None and ("prozorro", str(aid)) not in allowed:
                continue
            ids.append(str(aid))
        return list(dict.fromkeys(ids))

    def _wait_llm_batch(
        self,
        task_queue: TaskQueueService,
        task_ids: List[str],
        batch_id: str,
        batch_total: int,
        offset: int,
        grand_total: int,
        label: str,
        stats: Dict[str, Any],
        progress: Callable[[Dict[str, Any]], None],
        log: Callable[[str], None],
    ) -> Dict[str, int]:
        """Очікує завершення LLM-задач батчу (легкий poll по batch_id, без завантаження тисяч task_id)."""
        timeout_sec = max(3600, len(task_ids) * 180)
        deadline = time.time() + timeout_sec
        log(
            f"[Recognition] Очікування {len(task_ids)} LLM-задач у черзі ({label}), "
            f"batch_id={batch_id}..."
        )

        def _report(prog: Dict[str, int], queue_snap: Optional[Dict[str, Any]] = None) -> None:
            processed = int(prog.get("processed") or 0)
            success = int(prog.get("success") or 0)
            failed = int(prog.get("failed") or 0)
            in_progress = int(prog.get("in_progress") or 0)
            batch_count = int(prog.get("total") or batch_total)
            msg = (
                f"{label}: {processed}/{batch_count} "
                f"(успішно: {success}, помилок: {failed}, в роботі: {in_progress})"
            )
            if queue_snap and queue_snap.get("rabbit_messages") is not None:
                msg += f" | RabbitMQ: {queue_snap['rabbit_messages']} повідомлень"
            progress({
                "phase": "recognition",
                "current": offset + processed,
                "total": grand_total,
                "message": msg,
                "batch_id": batch_id,
                "batch_progress": prog,
                "llm_queue": queue_snap,
                "queue_mode": True,
                **stats,
            })

        queue_snap = task_queue.get_llm_queue_snapshot()
        prog = task_queue.get_llm_batch_progress(batch_id, total_hint=batch_total)
        _report(prog, queue_snap)

        while time.time() < deadline:
            time.sleep(2.0)
            queue_snap = task_queue.get_llm_queue_snapshot()
            prog = task_queue.get_llm_batch_progress(batch_id, total_hint=batch_total)
            _report(prog, queue_snap)

            batch_count = int(prog.get("total") or batch_total)
            processed = int(prog.get("processed") or 0)
            if batch_count > 0 and processed >= batch_count:
                log(f"[Recognition] Батч {batch_id} завершено: {processed}/{batch_count}.")
                break
        else:
            raise TimeoutError(
                f"Таймаут очікування батчу {batch_id} ({batch_total} задач, {timeout_sec} с)"
            )

        return {
            "success": int(prog.get("success") or 0),
            "failed": int(prog.get("failed") or 0),
        }

    def _process_olx_inline(
        self,
        olx_urls: List[str],
        total: int,
        stats: Dict[str, Any],
        progress: Callable[[Dict[str, Any]], None],
        log: Callable[[str], None],
    ) -> int:
        from scripts.olx_scraper.run_update import _process_single_llm_pending_url
        from business.services.olx_llm_extractor_service import OlxLLMExtractorService
        from business.services.geocoding_service import GeocodingService

        llm_extractor = OlxLLMExtractorService(self.settings)
        geocoding = GeocodingService(self.settings)
        unified_service = UnifiedListingsService(self.settings)
        geography_service = None
        try:
            from business.services.geography_service import GeographyService
            geography_service = GeographyService()
        except ImportError:
            pass
        usd_rate = None
        try:
            from business.services.currency_rate_service import CurrencyRateService
            usd_rate = CurrencyRateService(self.settings).get_today_usd_rate(allow_fetch=True)
        except Exception:
            pass

        olx_total = len(olx_urls)
        progress_every = 1 if olx_total < 500 else (5 if olx_total < 5000 else 25)
        log(f"[Recognition] OLX LLM (inline) для {olx_total} оголошень...")
        olx_processed = 0
        for idx, listing_url in enumerate(olx_urls, start=1):
            try:
                if _process_single_llm_pending_url(
                    listing_url=listing_url,
                    raw_repo=self.raw_olx_repo,
                    main_repo=self.olx_repo,
                    llm_extractor=llm_extractor,
                    geocoding_service=geocoding,
                    unified_service=unified_service,
                    usd_rate=usd_rate,
                    log_fn=log,
                    geography_service=geography_service,
                ):
                    olx_processed += 1
                    stats["olx"]["processed"] = olx_processed
                    stats["processed"] += 1
            except Exception as e:
                stats["errors"] += 1
                stats["olx"]["errors"] += 1
                log(f"Помилка OLX LLM {listing_url[:60]}: {e}")

            if idx == 1 or idx == olx_total or idx % progress_every == 0:
                progress({
                    "phase": "recognition",
                    "current": idx,
                    "total": total,
                    "message": f"Розпізнавання OLX: {idx}/{olx_total} (успішно: {olx_processed})",
                    "queue_mode": False,
                    **stats,
                })
        return olx_processed

    def _process_olx_via_queue(
        self,
        olx_urls: List[str],
        total: int,
        stats: Dict[str, Any],
        progress: Callable[[Dict[str, Any]], None],
        log: Callable[[str], None],
        task_queue: TaskQueueService,
    ) -> int:
        batch_id = f"batch-recognition-olx-{uuid.uuid4().hex[:12]}"
        olx_total = len(olx_urls)
        log(f"[Recognition] Постановка {olx_total} OLX LLM-задач у чергу llm_processing...")
        progress({
            "phase": "recognition",
            "current": 0,
            "total": total,
            "message": f"Постановка в чергу OLX: 0/{olx_total}",
            "queue_mode": True,
            **stats,
        })

        task_ids: List[str] = []
        meta_base = {
            "trigger": "batch_recognition",
            "llm_batch_id": batch_id,
            "llm_batch_total": olx_total,
            "llm_batch_source": "olx",
        }
        for idx, url in enumerate(olx_urls, start=1):
            task_ids.append(task_queue.enqueue_olx_llm(url, metadata=meta_base))
            if idx == olx_total or idx % 50 == 0:
                progress({
                    "phase": "recognition",
                    "current": 0,
                    "total": total,
                    "message": f"Постановка в чергу OLX: {idx}/{olx_total}",
                    "batch_id": batch_id,
                    "queue_mode": True,
                    **stats,
                })

        result = self._wait_llm_batch(
            task_queue, task_ids, batch_id, olx_total,
            offset=0, grand_total=total, label="Розпізнавання OLX (черга)",
            stats=stats, progress=progress, log=log,
        )
        stats["olx"]["processed"] = result["success"]
        stats["processed"] += result["success"]
        stats["errors"] += result["failed"]
        stats["olx"]["errors"] += result["failed"]
        return result["success"]

    def run(
        self,
        filters: BatchJobFilters,
        progress_fn: ProgressFn = None,
        log_fn: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """Виконує повторне розпізнавання для відібраних raw-записів."""
        def log(msg: str) -> None:
            if log_fn:
                log_fn(msg)
            else:
                logger.info("%s", msg)

        def progress(data: Dict[str, Any]) -> None:
            if progress_fn:
                progress_fn(data)

        task_queue = TaskQueueService(self.settings)
        use_queue = task_queue.is_enabled()

        sources = filters.resolved_sources()
        olx_urls: List[str] = []
        prozorro_ids: List[str] = []
        if "olx" in sources:
            olx_urls = self.collect_olx_urls(filters)
        if "prozorro" in sources:
            prozorro_ids = self.collect_prozorro_ids(filters)

        total = len(olx_urls) + len(prozorro_ids)
        stats: Dict[str, Any] = {
            "total_selected": total,
            "processed": 0,
            "errors": 0,
            "queue_mode": use_queue,
            "olx": {"selected": len(olx_urls), "processed": 0, "errors": 0},
            "prozorro": {"selected": len(prozorro_ids), "processed": 0, "errors": 0},
        }
        mode_label = "черга llm_processing" if use_queue else "inline (послідовно)"
        log(f"[Recognition] Відібрано: OLX {len(olx_urls)}, ProZorro {len(prozorro_ids)}. Режим: {mode_label}.")
        progress({
            "phase": "recognition",
            "current": 0,
            "total": total,
            "message": f"Розпізнавання ({mode_label}): 0/{total}",
            **stats,
        })

        if olx_urls:
            if use_queue:
                self._process_olx_via_queue(olx_urls, total, stats, progress, log, task_queue)
            else:
                self._process_olx_inline(olx_urls, total, stats, progress, log)

        if prozorro_ids:
            prozorro_svc = ProZorroService(self.settings)
            _promote_raw_prozorro_to_main(
                self.raw_prozorro_repo,
                self.prozorro_repo,
                prozorro_ids,
                log_fn=log,
            )
            unified_service = UnifiedListingsService(self.settings)
            for aid in prozorro_ids:
                try:
                    unified_service.sync_prozorro_auction(aid)
                except Exception as e:
                    logger.debug("Unified sync ProZorro %s: %s", aid, e)

            ids_for_llm = _select_prozorro_ids_for_llm(self.raw_prozorro_repo, prozorro_ids)
            if filters.force:
                ids_for_llm = prozorro_ids

            if use_queue and ids_for_llm:
                batch_id = f"batch-recognition-pz-{uuid.uuid4().hex[:12]}"
                pz_total = len(ids_for_llm)
                log(f"[Recognition] Постановка {pz_total} ProZorro LLM-задач у чергу...")
                task_ids = []
                meta_base = {
                    "trigger": "batch_recognition",
                    "llm_batch_id": batch_id,
                    "llm_batch_total": pz_total,
                    "llm_batch_source": "prozorro",
                }
                for aid in ids_for_llm:
                    task_ids.append(task_queue.enqueue_prozorro_llm(aid, metadata=meta_base))
                result = self._wait_llm_batch(
                    task_queue, task_ids, batch_id, pz_total,
                    offset=len(olx_urls), grand_total=total,
                    label="Розпізнавання ProZorro (черга)",
                    stats=stats, progress=progress, log=log,
                )
                stats["prozorro"]["processed"] = pz_total
                stats["processed"] += result["success"]
                stats["errors"] += result["failed"]
                stats["prozorro"]["errors"] += result["failed"]
            else:
                llm_count = 0
                for idx, auction_id in enumerate(prozorro_ids, start=1):
                    current = len(olx_urls) + idx
                    try:
                        if auction_id in ids_for_llm or filters.force:
                            doc = self.prozorro_repo.find_by_auction_id(auction_id)
                            if doc and doc.get("auction_data") and prozorro_svc.llm_service:
                                region = prozorro_svc._get_region_from_auction_data(doc["auction_data"])
                                if filters.force or region is None or is_region_enabled_for_llm(region):
                                    prozorro_svc._process_auction_with_llm(doc["auction_data"])
                                    prozorro_svc._sync_auction_to_unified(auction_id)
                                    llm_count += 1
                        stats["prozorro"]["processed"] += 1
                        stats["processed"] += 1
                    except Exception as e:
                        stats["errors"] += 1
                        stats["prozorro"]["errors"] += 1
                        log(f"Помилка ProZorro LLM {auction_id}: {e}")

                    progress({
                        "phase": "recognition",
                        "current": current,
                        "total": total,
                        "message": f"ProZorro: {idx}/{len(prozorro_ids)} (LLM: {llm_count})",
                        **stats,
                    })

        progress({
            "phase": "done",
            "current": total,
            "total": total,
            "message": f"Розпізнавання завершено: оброблено {stats['processed']}, помилок {stats['errors']}",
            **stats,
        })
        return stats
