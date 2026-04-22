# -*- coding: utf-8 -*-
"""
Оркестрація завантаження даних з джерел за pipeline: raw → main + LLM для обраних → аналітика + гео-індекс.

Phase 1: сирі дані в raw_olx_listings / raw_prozorro_auctions (без LLM).
Phase 2: підняття raw у основні колекції; для обраних за налаштуваннями — LLM та синхронізація в unified.
Phase 3: перерахунок аналітики та побудова географічного індексу.
"""

import logging
import threading
import uuid
from typing import Any, Callable, Dict, List, Optional, Set

from config.settings import Settings
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
from data.repositories.raw_prozorro_auctions_repository import RawProzorroAuctionsRepository
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from utils.hash_utils import calculate_object_version_hash, calculate_description_hash, extract_auction_id
from business.services.llm_processing_regions_service import (
    get_enabled_regions,
    is_region_enabled_for_llm,
    normalize_region_name,
)

logger = logging.getLogger(__name__)

_SOURCE_LOAD_LOCK = threading.Lock()
_SOURCE_LOAD_ACTIVE_COUNT = 0


def is_source_load_running() -> bool:
    """Повертає True, якщо хоча б один run_full_pipeline зараз виконується."""
    with _SOURCE_LOAD_LOCK:
        return _SOURCE_LOAD_ACTIVE_COUNT > 0


def get_targeted_update_options() -> Dict[str, Any]:
    """
    Повертає списки областей та типів оголошень OLX для точкового оновлення (UI, API).
    """
    try:
        from business.services.llm_processing_regions_service import get_all_region_names
        regions = get_all_region_names()
    except Exception:
        regions = []
    olx_listing_types: List[str] = []
    try:
        from scripts.olx_scraper.run_update import _get_base_categories
        olx_listing_types = [c.get("label", "") for c in _get_base_categories() if c.get("label")]
    except Exception:
        pass
    return {"regions": regions, "olx_listing_types": olx_listing_types}


def _select_olx_urls_for_llm(
    raw_repo: RawOlxListingsRepository,
    loaded_urls: List[str],
) -> List[str]:
    """Повертає підмножину URL, для яких потрібна LLM-обробка (за llm_processing_regions)."""
    if not loaded_urls:
        return []
    enabled = get_enabled_regions()
    if not enabled:
        return list(loaded_urls)
    selected: List[str] = []
    docs = raw_repo.get_by_urls(loaded_urls)
    by_url = {d["url"]: d for d in docs if d.get("url")}
    for url in loaded_urls:
        doc = by_url.get(url)
        if not doc:
            selected.append(url)
            continue
        region = doc.get("approximate_region") or ""
        if is_region_enabled_for_llm(region):
            selected.append(url)
    return selected


def _select_prozorro_ids_for_llm(
    raw_repo: RawProzorroAuctionsRepository,
    loaded_auction_ids: List[str],
) -> List[str]:
    """Повертає підмножину auction_id для LLM за llm_processing_regions."""
    if not loaded_auction_ids:
        return []
    enabled = get_enabled_regions()
    if not enabled:
        return list(loaded_auction_ids)
    selected: List[str] = []
    docs = raw_repo.get_by_auction_ids(loaded_auction_ids)
    by_id = {d["auction_id"]: d for d in docs if d.get("auction_id")}
    for aid in loaded_auction_ids:
        doc = by_id.get(aid)
        if not doc:
            selected.append(aid)
            continue
        region = doc.get("approximate_region") or ""
        if is_region_enabled_for_llm(region):
            selected.append(aid)
    return selected


def _promote_raw_prozorro_to_main(
    raw_repo: RawProzorroAuctionsRepository,
    main_repo: ProZorroAuctionsRepository,
    loaded_auction_ids: List[str],
    log_fn: Optional[Callable[[str], None]] = None,
) -> None:
    """Копіює сирі записи з raw_prozorro_auctions у prozorro_auctions (version_hash, description_hash)."""
    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            logger.info("%s", msg)
    from datetime import datetime, timezone
    docs = raw_repo.get_by_auction_ids(loaded_auction_ids)
    for doc in docs:
        auction_id = doc.get("auction_id")
        if not auction_id:
            continue
        auction_data = doc.get("auction_data") or {}
        version_hash = calculate_object_version_hash(auction_data)
        description = ""
        if "description" in auction_data:
            desc_obj = auction_data["description"]
            if isinstance(desc_obj, dict):
                description = desc_obj.get("uk_UA", desc_obj.get("en_US", ""))
            elif isinstance(desc_obj, str):
                description = desc_obj
        description_hash = calculate_description_hash(description) if description else None
        now = datetime.now(timezone.utc)
        main_repo.upsert_auction(
            auction_id=auction_id,
            auction_data=auction_data,
            version_hash=version_hash,
            description_hash=description_hash,
            last_updated=now,
        )
    log(f"[Source load] Піднято {len(docs)} записів ProZorro з raw у prozorro_auctions.")


def _run_phase3_post_processing(
    sources: List[str],
    result: Dict[str, Any],
    log_fn: Optional[Callable[[str], None]] = None,
) -> None:
    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            logger.info("%s", msg)

    log("[Source load] Phase 3: перерахунок аналітики та географічного індексу.")
    try:
        log("[Source load] Phase 3: оновлення знань колекцій...")
        from business.services.collection_knowledge_service import refresh_knowledge_after_sources
        refresh_knowledge_after_sources(sources)
        result["phase3"]["collection_knowledge"] = "ok"
        log("[Source load] Phase 3: знання колекцій оновлено.")
    except Exception as e:
        logger.debug("Оновлення знань після source load: %s", e)
        result["phase3"]["collection_knowledge"] = str(e)
    try:
        log("[Source load] Phase 3: price_analytics (перерахунок метрик)...")
        from business.services.price_analytics_service import PriceAnalyticsService
        analytics = PriceAnalyticsService()
        counts = analytics.rebuild_all()
        result["phase3"]["price_analytics"] = counts
        log(f"[Source load] Phase 3: price_analytics готово. {counts}")
    except Exception as e:
        logger.warning("Помилка оновлення price analytics: %s", e)
        result["phase3"]["price_analytics_error"] = str(e)
    try:
        log("[Source load] Phase 3: analytics_extracts (перезаповнення)...")
        from business.services.analytics_extracts_populator import rebuild_analytics_extracts
        n = rebuild_analytics_extracts()
        result["phase3"]["analytics_extracts"] = n
        log(f"[Source load] Phase 3: analytics_extracts готово. Записів: {n}.")
    except Exception as e:
        logger.debug("rebuild_analytics_extracts: %s", e)
        result["phase3"]["analytics_extracts"] = None
    try:
        log("[Source load] Phase 3: гео-індекс (cadastral)...")
        from business.services.cadastral_location_index_service import CadastralLocationIndexService
        idx_svc = CadastralLocationIndexService()
        idx_svc.build_index_from_parcels(batch_size=2000)
        result["phase3"]["geo_index"] = "ok"
        log("[Source load] Phase 3: гео-індекс готово.")
    except Exception as e:
        logger.debug("Побудова гео-індексу (cadastral): %s", e)
        result["phase3"]["geo_index"] = None
    log("[Source load] Phase 3 завершено. Pipeline виконано.")


def process_prozorro_llm_auction(
    auction_id: str,
    settings: Optional[Settings] = None,
    log_fn: Optional[Callable[[str], None]] = None,
) -> bool:
    """Обробляє один ProZorro auction_id через LLM та sync у unified."""
    st = settings or Settings()
    from business.services.prozorro_service import ProZorroService
    from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository

    def log(msg: str) -> None:
        if log_fn:
            log_fn(msg)
        else:
            logger.info("%s", msg)

    repo = ProZorroAuctionsRepository()
    doc = repo.find_by_auction_id(auction_id)
    if not doc or not doc.get("auction_data"):
        log(f"[Source load] ProZorro LLM: auction_id={auction_id} не знайдено у main-колекції.")
        return False
    service = ProZorroService(st)
    if not service.llm_service:
        log("[Source load] ProZorro LLM: сервіс LLM недоступний.")
        return False
    service._process_auction_with_llm(doc["auction_data"])
    service._sync_auction_to_unified(auction_id)
    return True


def run_full_pipeline(
    settings: Optional[Settings] = None,
    sources: Optional[List[str]] = None,
    days: Optional[int] = None,
    log_fn: Optional[Callable[[str], None]] = None,
    regions: Optional[List[str]] = None,
    listing_types: Optional[List[str]] = None,
    olx_phase1_max_threads: Optional[int] = None,
    use_brokered_llm: bool = False,
    llm_wait_heartbeat_fn: Optional[Callable[[], None]] = None,
    run_phase3: bool = True,
) -> Dict[str, Any]:
    """
    Запускає повний pipeline: Phase 1 (raw) → Phase 2 (promote + LLM для обраних) → Phase 3 (аналітика + гео).

    Args:
        settings: налаштування (за замовчуванням новий Settings).
        sources: список джерел ("olx", "prozorro") або None = обидва.
        days: період у днях для ProZorro та OLX cutoff.
        log_fn: опціональна функція логування.
        regions: точкове оновлення — лише ці області (для OLX — обмеження Phase 1; для ProZorro — фільтр Phase 2 по approximate_region).
        listing_types: точкове оновлення OLX — лише категорії, чий label містить один із рядків (напр. «Нежитлова», «Земля»).
        olx_phase1_max_threads: кількість потоків Phase 1 OLX (пул завдань область+категорія); None = з конфігу; 0 = legacy (по області).
        run_phase3: якщо False — пропускає перерахунок аналітик/гео-індексу (Phase 3).

    Returns:
        Словник з результатами по фазах та джерелах.
    """
    from scripts.olx_scraper.run_update import (
        run_olx_update_raw_only,
        _process_llm_pending,
    )
    from business.services.prozorro_service import ProZorroService
    from business.services.unified_listings_service import UnifiedListingsService
    from business.services.olx_llm_extractor_service import OlxLLMExtractorService
    from business.services.geocoding_service import GeocodingService
    from business.services.currency_rate_service import CurrencyRateService
    from utils.price_metrics import compute_price_metrics

    global _SOURCE_LOAD_ACTIVE_COUNT
    with _SOURCE_LOAD_LOCK:
        _SOURCE_LOAD_ACTIVE_COUNT += 1

    try:
        st = settings or Settings()
        if sources is None:
            sources = ["olx", "prozorro"]
        if days is None:
            days = getattr(st, "default_days_range", 1) or 1

        def log(msg: str) -> None:
            if log_fn:
                log_fn(msg)
            else:
                logger.info("%s", msg)

        result: Dict[str, Any] = {
            "phase1": {},
            "phase2": {"olx_llm_processed": 0, "prozorro_llm_processed": 0},
            "phase3": {},
            "core_completed": False,
        }

        # ---------- Phase 1: завантаження сирих даних ----------
        log("[Source load] Phase 1: завантаження сирих даних з джерел (без LLM).")
        olx_loaded_urls: List[str] = []
        prozorro_loaded_ids: List[str] = []
        # LLM має стартувати тільки після повного завершення Phase 1 (raw),
        # тому не робимо inline LLM під час завантаження сирих даних.
        olx_dynamic_llm_processed_urls: Set[str] = set()

        raw_olx = RawOlxListingsRepository()
        raw_prozorro = RawProzorroAuctionsRepository()
        main_olx = OlxListingsRepository()
        main_prozorro = ProZorroAuctionsRepository()
        main_olx.ensure_index()
        main_prozorro._ensure_indexes()

        unified_olx: Optional[UnifiedListingsService] = None
        geocoding_olx: Optional[GeocodingService] = None
        llm_extractor_olx: Optional[OlxLLMExtractorService] = None
        usd_rate_olx: Optional[float] = None
        if "olx" in sources:
            unified_olx = UnifiedListingsService(st)
            geocoding_olx = GeocodingService(st)
            llm_extractor_olx = OlxLLMExtractorService(st)
            try:
                usd_rate_olx = CurrencyRateService(st).get_today_usd_rate(allow_fetch=True)
            except Exception:
                usd_rate_olx = None

        if "olx" in sources:
            r = run_olx_update_raw_only(
                settings=st,
                log_fn=log_fn,
                days=days,
                regions=regions,
                listing_types=listing_types,
                max_workers=olx_phase1_max_threads,
            )
            result["phase1"]["olx"] = r
            olx_loaded_urls = r.get("loaded_urls") or []
            # Для сумісності зі старими результатами, якщо поле існує у відповіді.
            olx_dynamic_llm_processed_urls.update(r.get("llm_processed_urls") or [])

        if "prozorro" in sources:
            prozorro = ProZorroService(st)
            r = prozorro.fetch_and_save_to_raw_only(days=days)
            result["phase1"]["prozorro"] = r
            prozorro_loaded_ids = list(r.get("loaded_auction_ids") or [])
            if regions and prozorro_loaded_ids:
                raw_prozorro = RawProzorroAuctionsRepository()
                docs = raw_prozorro.get_by_auction_ids(prozorro_loaded_ids)
                region_set = {normalize_region_name(r.strip()) or r.strip() for r in regions if r and r.strip()}
                prozorro_loaded_ids = [
                    d["auction_id"] for d in docs
                    if d.get("auction_id") and normalize_region_name(d.get("approximate_region") or "") in region_set
                ]
                log(f"[Source load] ProZorro: після фільтра по областях залишено {len(prozorro_loaded_ids)} аукціонів.")

        # ---------- Phase 2: підняття raw → main, LLM для обраних, sync unified ----------
        log("[Source load] Phase 2: підняття з raw у основні колекції та LLM для обраних.")
        task_queue = None
        brokered_llm_task_ids: List[str] = []
        if use_brokered_llm:
            from business.services.task_queue_service import TaskQueueService
            task_queue = TaskQueueService(st)
            use_brokered_llm = task_queue.is_enabled()
        llm_batch_progress_state = {"processed": -1}
        if "olx" in sources and olx_loaded_urls:
            urls_for_llm = set(_select_olx_urls_for_llm(raw_olx, olx_loaded_urls))
            if urls_for_llm:
                pending_list = [u for u in olx_loaded_urls if u in urls_for_llm and u not in olx_dynamic_llm_processed_urls]
                log(f"[Source load] Phase 2 OLX: LLM-обробка для {len(pending_list)} оголошень (дані в olx_listings та unified тільки після LLM).")
                if use_brokered_llm and task_queue:
                    olx_llm_batch_id = f"phase2-olx-{uuid.uuid4().hex[:12]}"
                    for listing_url in pending_list:
                        brokered_llm_task_ids.append(
                            task_queue.enqueue_olx_llm(
                                listing_url,
                                metadata={
                                    "source": "olx",
                                    "days": days,
                                    "llm_batch_id": olx_llm_batch_id,
                                    "llm_batch_total": len(pending_list),
                                    "llm_batch_source": "olx",
                                },
                            )
                        )
                    n = 0
                    log(f"[Source load] Phase 2 OLX: поставлено в RabbitMQ {len(pending_list)} LLM-задач.")
                else:
                    n = _process_llm_pending(
                        pending_list,
                        raw_olx,
                        main_olx,
                        llm_extractor_olx,
                        geocoding_olx,
                        unified_olx,
                        usd_rate_olx,
                        log_fn,
                    )
                total_olx_processed = (len(pending_list) if use_brokered_llm else n) + len(olx_dynamic_llm_processed_urls)
                result["phase2"]["olx_llm_processed"] = total_olx_processed
                if use_brokered_llm:
                    log(f"[Source load] Phase 2 OLX: LLM-задачі поставлено. Очікується {total_olx_processed}/{len([u for u in olx_loaded_urls if u in urls_for_llm])}.")
                else:
                    log(f"[Source load] Phase 2 OLX: LLM завершено. Оброблено {total_olx_processed}/{len([u for u in olx_loaded_urls if u in urls_for_llm])} оголошень (записано в olx_listings та unified).")
            else:
                log(
                    f"[Source load] Phase 2 OLX: LLM пропущено (завантажено URL: {len(olx_loaded_urls)}, "
                    "обрано для LLM: 0 — перевірте llm_processing_regions.yaml та approximate_region у raw)."
                )
            log("[Source load] Phase 2 OLX завершено.")

        if "prozorro" in sources and prozorro_loaded_ids:
            _promote_raw_prozorro_to_main(raw_prozorro, main_prozorro, prozorro_loaded_ids, log_fn=log_fn)
            log(f"[Source load] Phase 2 ProZorro: синхронізація в unified_listings ({len(prozorro_loaded_ids)} аукціонів)...")
            unified_prozorro = UnifiedListingsService(st)
            for i, aid in enumerate(prozorro_loaded_ids, start=1):
                try:
                    unified_prozorro.sync_prozorro_auction(aid)
                except Exception as e:
                    logger.debug("Unified sync ProZorro %s: %s", aid, e)
                if i % 100 == 0 or i == len(prozorro_loaded_ids):
                    log(f"[Source load] Phase 2 ProZorro: unified — {i}/{len(prozorro_loaded_ids)}.")
            log(f"[Source load] Phase 2 ProZorro: unified готово.")
            ids_for_llm = _select_prozorro_ids_for_llm(raw_prozorro, prozorro_loaded_ids)
            if ids_for_llm:
                log(f"[Source load] Phase 2 ProZorro: LLM-обробка для {len(ids_for_llm)} аукціонів...")
                if use_brokered_llm and task_queue:
                    prozorro_llm_batch_id = f"phase2-prozorro-{uuid.uuid4().hex[:12]}"
                    for auction_id in ids_for_llm:
                        brokered_llm_task_ids.append(
                            task_queue.enqueue_prozorro_llm(
                                auction_id,
                                metadata={
                                    "source": "prozorro",
                                    "days": days,
                                    "llm_batch_id": prozorro_llm_batch_id,
                                    "llm_batch_total": len(ids_for_llm),
                                    "llm_batch_source": "prozorro",
                                },
                            )
                        )
                    result["phase2"]["prozorro_llm_processed"] = len(ids_for_llm)
                    log(f"[Source load] Phase 2 ProZorro: поставлено в RabbitMQ {len(ids_for_llm)} LLM-задач.")
                else:
                    prozorro_svc = ProZorroService(st)
                    if prozorro_svc.llm_service:
                        for auction_id in ids_for_llm:
                            try:
                                doc = main_prozorro.find_by_auction_id(auction_id)
                                if doc and doc.get("auction_data"):
                                    prozorro_svc._process_auction_with_llm(doc["auction_data"])
                                    prozorro_svc._sync_auction_to_unified(auction_id)
                                    result["phase2"]["prozorro_llm_processed"] += 1
                            except Exception as e:
                                logger.warning("ProZorro LLM для %s: %s", auction_id, e)
                        log(f"[Source load] Phase 2 ProZorro: LLM завершено. Оброблено {result['phase2']['prozorro_llm_processed']}/{len(ids_for_llm)}.")
                    else:
                        log("[Source load] Phase 2 ProZorro: LLM недоступний (сервіс не ініціалізовано).")
            else:
                log("[Source load] Phase 2 ProZorro: LLM пропущено (0 кандидатів за регіонами).")
            log("[Source load] Phase 2 ProZorro завершено.")

        if use_brokered_llm and task_queue and brokered_llm_task_ids:
            log(f"[Source load] Phase 2: очікування завершення {len(brokered_llm_task_ids)} LLM-задач із RabbitMQ...")

            def _llm_wait_progress(docs: List[Dict[str, Any]], task_ids: List[str]) -> None:
                by_id = {doc.get("task_id"): doc for doc in docs}
                processed = sum(
                    1
                    for tid in task_ids
                    if str((by_id.get(tid) or {}).get("state") or "").lower() in TaskQueueService.TERMINAL_STATES
                )
                total = len(task_ids)
                if processed != llm_batch_progress_state["processed"]:
                    llm_batch_progress_state["processed"] = processed
                    log(f"[Source load] Phase 2 LLM queue: оброблено {processed} з {total}.")

            task_docs = task_queue.wait_for_all(
                brokered_llm_task_ids,
                timeout_sec=max(1800, len(brokered_llm_task_ids) * 120),
                heartbeat_fn=llm_wait_heartbeat_fn,
                progress_fn=_llm_wait_progress,
            )
            olx_success = 0
            prozorro_success = 0
            for doc in task_docs:
                if str(doc.get("state") or "").lower() != "success":
                    logger.warning("LLM background task failed: %s", doc.get("error") or doc.get("task_id"))
                    continue
                payload = doc.get("payload") or {}
                if doc.get("task_name") == "process_olx_llm_task" and payload.get("listing_url"):
                    olx_success += 1
                elif doc.get("task_name") == "process_prozorro_llm_task" and payload.get("auction_id"):
                    prozorro_success += 1
            result["phase2"]["olx_llm_processed"] = olx_success + len(olx_dynamic_llm_processed_urls)
            result["phase2"]["prozorro_llm_processed"] = prozorro_success
            log(
                f"[Source load] Phase 2: LLM background tasks завершено. "
                f"OLX={result['phase2']['olx_llm_processed']}, ProZorro={result['phase2']['prozorro_llm_processed']}."
            )

        # Зберігаємо дату оновлення ProZorro для get_auctions_from_db_by_period / generate_excel_from_db
        if "prozorro" in sources and days is not None:
            try:
                from datetime import datetime, timezone
                from data.repositories.app_data_repository import AppDataRepository
                now = datetime.now(timezone.utc)
                app_data = AppDataRepository()
                app_data.set_update_date(days, now)
                if days == 7:
                    app_data.set_update_date(1, now)
            except Exception as e:
                logger.debug("Збереження дати оновлення ProZorro: %s", e)

        result["core_completed"] = True
        log("[Source load] Core pipeline завершено: raw + promote/main + LLM виконано успішно.")
        if run_phase3:
            _run_phase3_post_processing(sources, result, log_fn=log_fn)
        else:
            result["phase3"]["skipped"] = True
            log("[Source load] Phase 3 пропущено (run_phase3=False).")

        return result
    finally:
        with _SOURCE_LOAD_LOCK:
            _SOURCE_LOAD_ACTIVE_COUNT = max(0, _SOURCE_LOAD_ACTIVE_COUNT - 1)
