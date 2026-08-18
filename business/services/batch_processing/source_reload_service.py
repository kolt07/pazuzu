# -*- coding: utf-8 -*-
"""
Перезавантаження оголошень з джерел: відбір з unified_listings, fetch з OLX/ProZorro API,
оновлення raw/main колекцій та подальша обробка при зміні даних.
"""

from __future__ import annotations

import logging
from typing import Any, Callable, Dict, List, Optional

from config.settings import Settings
from data.repositories.olx_listings_repository import OlxListingsRepository
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
from data.repositories.raw_prozorro_auctions_repository import RawProzorroAuctionsRepository
from data.repositories.unified_listings_repository import UnifiedListingsRepository
from business.services.batch_processing.filters import BatchJobFilters, build_unified_listing_query
from business.services.prozorro_service import ProZorroService
from business.services.source_data_load_service import run_full_pipeline
from utils.hash_utils import calculate_object_version_hash, calculate_description_hash

logger = logging.getLogger(__name__)

ProgressFn = Optional[Callable[[Dict[str, Any]], None]]


class SourceReloadService:
    """Групове перезавантаження оголошень із зовнішніх джерел."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self.unified_repo = UnifiedListingsRepository()
        self.raw_olx_repo = RawOlxListingsRepository()
        self.raw_prozorro_repo = RawProzorroAuctionsRepository()
        self.olx_repo = OlxListingsRepository()
        self.prozorro_repo = ProZorroAuctionsRepository()

    def collect_listings(self, filters: BatchJobFilters) -> List[Dict[str, Any]]:
        """Відбирає оголошення з unified_listings за фільтрами."""
        query = build_unified_listing_query(filters)
        limit = filters.limit if filters.limit and filters.limit > 0 else None
        docs = self.unified_repo.find_many(
            filter=query,
            sort=[("source_updated_at", -1)],
            limit=limit,
        )
        return docs

    def run(
        self,
        filters: BatchJobFilters,
        progress_fn: ProgressFn = None,
        log_fn: Optional[Callable[[str], None]] = None,
    ) -> Dict[str, Any]:
        """
        Виконує перезавантаження:
        1. Оновлює наявні оголошення з БД (fetch з джерела).
        2. Якщо load_new=True — додатково запускає pipeline завантаження нових з джерел.
        """
        def log(msg: str) -> None:
            if log_fn:
                log_fn(msg)
            else:
                logger.info("%s", msg)

        def progress(data: Dict[str, Any]) -> None:
            if progress_fn:
                progress_fn(data)

        listings = self.collect_listings(filters)
        total = len(listings)
        stats = {
            "total_selected": total,
            "processed": 0,
            "updated": 0,
            "skipped": 0,
            "errors": 0,
            "olx": {"processed": 0, "updated": 0, "skipped": 0, "errors": 0},
            "prozorro": {"processed": 0, "updated": 0, "skipped": 0, "errors": 0},
            "load_new": None,
        }

        log(f"[Source reload] Відібрано {total} оголошень для перезавантаження.")
        progress({
            "phase": "reload_existing",
            "current": 0,
            "total": total,
            "message": f"Перезавантаження з джерел: 0/{total}",
            **stats,
        })

        prozorro_svc = ProZorroService(self.settings)
        olx_browser = None
        try:
            from scripts.olx_scraper.browser_fetcher import BrowserPageFetcher
            olx_browser = BrowserPageFetcher(headless=True)
            olx_browser.__enter__()
        except Exception as e:
            log(f"[Source reload] Browser недоступний для OLX reload: {e}")
            olx_browser = None

        try:
            for idx, doc in enumerate(listings, start=1):
                source = (doc.get("source") or "").strip().lower()
                source_id = doc.get("source_id") or ""
                try:
                    if source == "olx":
                        result = self._reload_olx(source_id, filters.force, log, browser_fetcher=olx_browser)
                        bucket = stats["olx"]
                    elif source == "prozorro":
                        result = self._reload_prozorro(source_id, prozorro_svc, filters.force, log)
                        bucket = stats["prozorro"]
                    else:
                        stats["skipped"] += 1
                        continue

                    bucket["processed"] += 1
                    stats["processed"] += 1
                    if result.get("updated"):
                        bucket["updated"] += 1
                        stats["updated"] += 1
                    elif result.get("skipped"):
                        bucket["skipped"] += 1
                        stats["skipped"] += 1
                    elif result.get("error"):
                        bucket["errors"] += 1
                        stats["errors"] += 1
                except Exception as e:
                    logger.exception("Source reload error %s:%s", source, source_id[:60])
                    stats["errors"] += 1
                    if source == "olx":
                        stats["olx"]["errors"] += 1
                    elif source == "prozorro":
                        stats["prozorro"]["errors"] += 1
                    log(f"Помилка {source} {source_id[:50]}: {e}")

                progress({
                    "phase": "reload_existing",
                    "current": idx,
                    "total": total,
                    "message": f"Перезавантаження: {idx}/{total} (оновлено: {stats['updated']})",
                    **stats,
                })
        finally:
            if olx_browser is not None:
                try:
                    olx_browser.__exit__(None, None, None)
                except Exception:
                    pass

        if filters.load_new:
            log("[Source reload] Запуск завантаження нових оголошень з джерел...")
            progress({
                "phase": "load_new",
                "message": "Завантаження нових оголошень з джерел...",
                **stats,
            })
            sources = filters.resolved_sources()
            days = filters.days if filters.days else 7
            pipeline_result = run_full_pipeline(
                settings=self.settings,
                sources=sources,
                days=days,
                regions=filters.normalized_regions(),
                run_phase3=False,
            )
            stats["load_new"] = pipeline_result
            log("[Source reload] Завантаження нових завершено.")

        progress({
            "phase": "done",
            "current": total,
            "total": total,
            "message": (
                f"Готово: оброблено {stats['processed']}, оновлено {stats['updated']}, "
                f"пропущено {stats['skipped']}, помилок {stats['errors']}"
            ),
            **stats,
        })
        return stats

    def _reload_olx(
        self,
        olx_url: str,
        force: bool,
        log: Callable[[str], None],
        browser_fetcher: Optional[Any] = None,
    ) -> Dict[str, Any]:
        """Перезавантажує одне OLX-оголошення з джерела (reuse browser_fetcher на батч)."""
        from scripts.olx_scraper.browser_fetcher import BrowserPageFetcher
        from scripts.olx_scraper.parser import parse_detail_page
        from scripts.olx_scraper.run_update import _process_single_llm_pending_url
        from business.services.olx_llm_extractor_service import OlxLLMExtractorService
        from business.services.geocoding_service import GeocodingService
        from business.services.unified_listings_service import UnifiedListingsService

        if not olx_url:
            return {"skipped": True, "reason": "empty_url"}

        existing_raw = self.raw_olx_repo.find_by_url(olx_url)
        existing_olx = self.olx_repo.find_by_url(olx_url)
        search_data = {}
        if existing_raw and existing_raw.get("search_data"):
            search_data = dict(existing_raw["search_data"])
        elif existing_olx and existing_olx.get("search_data"):
            search_data = dict(existing_olx["search_data"])
        else:
            olx_doc = self.unified_repo.find_by_source_id("olx", olx_url)
            if olx_doc:
                search_data = {
                    "title": olx_doc.get("title") or "",
                    "price_text": "",
                    "location": (olx_doc.get("addresses") or [{}])[0].get("settlement", "") if olx_doc.get("addresses") else "",
                }

        fetch_filters = (existing_raw or {}).get("fetch_filters") or {"source": "batch_reload"}
        approximate_region = (existing_raw or {}).get("approximate_region")

        own_browser = False
        bf = browser_fetcher
        try:
            if bf is None:
                bf = BrowserPageFetcher(headless=True)
                bf.__enter__()
                own_browser = True
            detail_result = bf.get_detail_page(olx_url)
            detail_data = parse_detail_page(detail_result.text)
            if detail_data.get("_inactive"):
                detail_data.pop("_inactive", None)
        except Exception as e:
            return {"error": True, "message": str(e)}
        finally:
            if own_browser and bf is not None:
                try:
                    bf.__exit__(None, None, None)
                except Exception:
                    pass

        old_detail = (existing_raw or {}).get("detail") or {}
        if not force and existing_raw and old_detail == (detail_data or {}):
            return {"skipped": True, "reason": "unchanged"}

        self.raw_olx_repo.upsert_raw(
            url=olx_url,
            search_data=search_data,
            detail=detail_data or None,
            fetch_filters=fetch_filters,
            approximate_region=approximate_region,
        )

        llm_extractor = OlxLLMExtractorService(self.settings)
        geocoding = GeocodingService(self.settings)
        unified_service = UnifiedListingsService(self.settings)
        usd_rate = None
        try:
            from business.services.currency_rate_service import CurrencyRateService
            usd_rate = CurrencyRateService(self.settings).get_today_usd_rate(allow_fetch=True)
        except Exception:
            pass

        ok = _process_single_llm_pending_url(
            listing_url=olx_url,
            raw_repo=self.raw_olx_repo,
            main_repo=self.olx_repo,
            llm_extractor=llm_extractor,
            geocoding_service=geocoding,
            unified_service=unified_service,
            usd_rate=usd_rate,
            log_fn=log,
        )
        return {"updated": ok, "skipped": not ok}

    def _reload_prozorro(
        self,
        auction_id: str,
        prozorro_svc: ProZorroService,
        force: bool,
        log: Callable[[str], None],
    ) -> Dict[str, Any]:
        """Перезавантажує один аукціон ProZorro з API."""
        from business.services.source_data_load_service import _promote_raw_prozorro_to_main
        from business.services.unified_listings_service import UnifiedListingsService
        from business.services.llm_processing_regions_service import is_region_enabled_for_llm

        if not auction_id:
            return {"skipped": True, "reason": "empty_id"}

        existing = self.prozorro_repo.find_by_auction_id(auction_id)
        proc_id = None
        ad = (existing or {}).get("auction_data") or {}
        if isinstance(ad.get("_id"), str):
            proc_id = ad["_id"]
        elif ad.get("_id") is not None:
            proc_id = str(ad["_id"])

        try:
            auction_data = prozorro_svc.get_auction_details(auction_id, proc_id=proc_id)
        except Exception as e:
            return {"error": True, "message": str(e)}

        new_version_hash = calculate_object_version_hash(auction_data)
        old_version_hash = (existing or {}).get("version_hash")
        if not force and existing and old_version_hash == new_version_hash:
            return {"skipped": True, "reason": "unchanged"}

        region = prozorro_svc._get_region_from_auction_data(auction_data)
        fetch_context = {"trigger": "batch_source_reload", "auction_id": auction_id}
        self.raw_prozorro_repo.upsert_raw(
            auction_id=auction_id,
            auction_data=auction_data,
            fetch_context=fetch_context,
            approximate_region=region,
        )

        _promote_raw_prozorro_to_main(
            self.raw_prozorro_repo,
            self.prozorro_repo,
            [auction_id],
            log_fn=log,
        )
        UnifiedListingsService(self.settings).sync_prozorro_auction(auction_id)

        description = ""
        if "description" in auction_data:
            desc_obj = auction_data["description"]
            if isinstance(desc_obj, dict):
                description = desc_obj.get("uk_UA", desc_obj.get("en_US", ""))
            elif isinstance(desc_obj, str):
                description = desc_obj

        if description and prozorro_svc.llm_service:
            desc_hash = calculate_description_hash(description)
            cached = prozorro_svc.llm_cache_service.repository.find_by_description_hash(desc_hash)
            region_ok = region is None or is_region_enabled_for_llm(region)
            if not cached and region_ok:
                prozorro_svc._process_auction_with_llm(auction_data)
                prozorro_svc._sync_auction_to_unified(auction_id)

        return {"updated": True}
