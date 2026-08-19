# -*- coding: utf-8 -*-
"""
Оркестратор точкового дослідження ринку: фільтри на джерелах → Phase 2 pipeline → постфільтр → статистична довідка.
LLM лише в стандартному парсингу оголошень; аналітична записка детермінована.
"""

from __future__ import annotations

import copy
import logging
import threading
from typing import Any, Dict, List, Optional, Tuple

from business.services.market_research_analytics_service import MarketResearchAnalyticsService
from business.services.prozorro_filtered_search import ProzorroFilteredSearch
from business.services.source_filter_mapper import (
    SourceQueryPlan,
    build_source_query_plan,
    is_broad_plan,
)
from data.repositories.market_research_repository import (
    ACTIVE_STATUSES,
    STATUS_ANALYZING,
    STATUS_CANCELLED,
    STATUS_DONE,
    STATUS_ERROR,
    STATUS_PROCESSING,
    STATUS_QUEUED,
    STATUS_SEARCHING,
    MarketResearchRepository,
)
from domain.services.filter_spec_service import filter_spec_summary, normalize_filter_spec
from utils.deal_type import DEAL_SALE, parse_deal_types

logger = logging.getLogger(__name__)

ERROR_ACTIVE_RUN = "active_run_exists"
ERROR_CONFIRM_BROAD = "confirm_broad_required"
ERROR_EMPTY_DEAL_TYPES = "empty_deal_types"
ERROR_NOT_FOUND = "not_found"
ERROR_FORBIDDEN = "forbidden"
ERROR_NOT_ACTIVE = "not_active"

TABLE_PAGE_SIZE = 20
SAMPLE_LIMIT = 8000
LLM_WAIT_TIMEOUT_SEC = 4 * 3600


class MarketResearchCancelled(Exception):
    """Користувач скасував дослідження."""


class MarketResearchService:
    def __init__(self, settings=None):
        from config.settings import Settings

        self.settings = settings or Settings()
        self.repo = MarketResearchRepository()
        self.analytics = MarketResearchAnalyticsService()

    def start(
        self,
        *,
        user_id: str,
        filter_spec: Optional[Dict[str, Any]],
        deal_types: Optional[List[str]],
        depth_days: Optional[int],
        confirm_broad: bool = False,
    ) -> Dict[str, Any]:
        uid = str(user_id)
        dts = parse_deal_types(deal_types)
        if not dts:
            return {"ok": False, "error": ERROR_EMPTY_DEAL_TYPES, "message": "Оберіть продаж і/або оренду."}

        spec = normalize_filter_spec(filter_spec)
        plan = build_source_query_plan(spec, dts, depth_days)
        if is_broad_plan(plan) and not confirm_broad:
            return {
                "ok": False,
                "error": ERROR_CONFIRM_BROAD,
                "message": "Немає області/НП і глибина без обмежень. Підтвердіть широкий пошук по всій Україні.",
            }

        active = self.repo.find_active_for_user(uid)
        if active:
            return {
                "ok": False,
                "error": ERROR_ACTIVE_RUN,
                "message": "Уже виконується інше дослідження. Дочекайтесь завершення або скасуйте його.",
                "research_id": active.get("research_id"),
            }

        rid = self.repo.create_run(
            user_id=uid,
            filter_spec=spec,
            deal_types=dts,
            depth_days=plan.depth_days,
            confirm_broad=bool(confirm_broad),
        )
        dispatched = self._dispatch(rid)
        self.repo.patch_run(rid, {
            "celery_task_id": dispatched.get("task_id"),
            "message": "У черзі",
            "status": STATUS_QUEUED,
            "phase": STATUS_QUEUED,
        })
        return {
            "ok": True,
            "research_id": rid,
            "status": STATUS_QUEUED,
            "queued": dispatched.get("queued", True),
            "task_id": dispatched.get("task_id"),
        }

    def cancel(self, research_id: str, user_id: str) -> Dict[str, Any]:
        doc = self._owned_run(research_id, user_id)
        if not doc.get("ok"):
            return doc
        run = doc["run"]
        if run.get("status") not in ACTIVE_STATUSES:
            return {"ok": False, "error": ERROR_NOT_ACTIVE, "message": "Дослідження вже завершене."}
        self.repo.patch_run(research_id, {
            "status": STATUS_CANCELLED,
            "phase": STATUS_CANCELLED,
            "message": "Скасовано користувачем",
        })
        return {"ok": True, "research_id": research_id, "status": STATUS_CANCELLED}

    def list_for_user(self, user_id: str, limit: int = 30) -> List[Dict[str, Any]]:
        return [self._public_run(d, include_report=False) for d in self.repo.list_for_user(user_id, limit=limit)]

    def get(
        self,
        research_id: str,
        user_id: str,
        *,
        skip: int = 0,
        limit: int = TABLE_PAGE_SIZE,
    ) -> Dict[str, Any]:
        doc = self._owned_run(research_id, user_id)
        if not doc.get("ok"):
            return doc
        run = doc["run"]
        out = self._public_run(run, include_report=True)
        keys = list(run.get("listing_keys") or [])
        total = len(keys)
        skip = max(0, int(skip))
        limit = max(1, min(int(limit), 100))
        page_keys = keys[skip: skip + limit]
        items: List[Dict[str, Any]] = []
        if page_keys:
            from data.repositories.unified_listings_repository import UnifiedListingsRepository

            docs = UnifiedListingsRepository().find_by_source_keys(page_keys)
            by_key = {(d.get("source"), d.get("source_id")): d for d in docs}
            for k in page_keys:
                d = by_key.get((k.get("source"), k.get("source_id")))
                if d:
                    items.append(d)
        out["items"] = items
        out["total"] = total
        out["skip"] = skip
        out["limit"] = limit
        return {"ok": True, **out}

    def run(self, research_id: str) -> Dict[str, Any]:
        """Виконується в черзі source_load або в fallback-потоці."""
        rid = str(research_id or "").strip()
        run = self.repo.get_run(rid)
        if not run:
            return {"ok": False, "error": ERROR_NOT_FOUND}

        global_count = False
        try:
            import business.services.source_data_load_service as sl
            with sl._SOURCE_LOAD_LOCK:
                sl._SOURCE_LOAD_ACTIVE_COUNT += 1
            global_count = True

            spec = normalize_filter_spec(run.get("filter_spec"))
            dts = list(run.get("deal_types") or [DEAL_SALE])
            plan = build_source_query_plan(spec, dts, run.get("depth_days"))
            self._raise_if_cancelled(rid)

            self._patch(rid, STATUS_SEARCHING, "Пошук у джерелах…")
            olx_urls, pz_ids, found_new = self._search_sources(rid, plan)
            source_keys = (
                [{"source": "olx", "source_id": u} for u in olx_urls]
                + [{"source": "prozorro", "source_id": a} for a in pz_ids]
            )
            self._patch(
                rid,
                STATUS_SEARCHING,
                f"Знайдено {len(source_keys)} оголошень. Обробка…",
                extra={
                    "source_keys": source_keys,
                    "counts": {
                        "found": len(source_keys),
                        "new": found_new,
                        "processed": 0,
                        "olx": len(olx_urls),
                        "prozorro": len(pz_ids),
                    },
                },
            )
            self._raise_if_cancelled(rid)

            self._patch(rid, STATUS_PROCESSING, "Pipeline: LLM і unified_listings…")
            processed_n = self._run_phase2(rid, olx_urls, pz_ids)
            self._raise_if_cancelled(rid)

            self._patch(rid, STATUS_ANALYZING, "Постфільтр і довідка…")
            docs = self._post_filter(spec, source_keys, dts)
            listing_keys = [
                {"source": d.get("source"), "source_id": d.get("source_id")}
                for d in docs
                if d.get("source") and d.get("source_id")
            ]
            limitations = []
            if not plan.settlements and spec:
                limitations.append("На OLX місто не в URL: джерело звужується до області, НП застосовується після гео.")
            if not plan.has_geo and plan.depth_days is None:
                limitations.append("Широкий пошук: область/НП не задані, глибина без обмежень.")
            summary = filter_spec_summary(spec) or "Відбори не задано"
            report = self.analytics.build(
                docs,
                filter_summary=summary,
                deal_types=dts,
                depth_days=plan.depth_days,
                limitations=limitations,
            )
            counts = {
                "found": len(source_keys),
                "new": found_new,
                "processed": processed_n,
                "olx": len(olx_urls),
                "prozorro": len(pz_ids),
                "matched": len(listing_keys),
            }
            self.repo.patch_run(rid, {
                "status": STATUS_DONE,
                "phase": STATUS_DONE,
                "message": f"Готово: {len(listing_keys)} оголошень у вибірці.",
                "listing_keys": listing_keys,
                "source_keys": source_keys,
                "counts": counts,
                "report": report,
                "error": None,
            })
            return {"ok": True, "research_id": rid, "counts": counts}
        except MarketResearchCancelled:
            self.repo.patch_run(rid, {
                "status": STATUS_CANCELLED,
                "phase": STATUS_CANCELLED,
                "message": "Скасовано користувачем",
            })
            return {"ok": True, "cancelled": True, "research_id": rid}
        except Exception as e:
            logger.exception("Market research failed: %s", e)
            self.repo.patch_run(rid, {
                "status": STATUS_ERROR,
                "phase": STATUS_ERROR,
                "message": "Помилка дослідження",
                "error": str(e)[:2000],
            })
            return {"ok": False, "error": str(e)[:2000]}
        finally:
            if global_count:
                try:
                    import business.services.source_data_load_service as sl
                    with sl._SOURCE_LOAD_LOCK:
                        sl._SOURCE_LOAD_ACTIVE_COUNT = max(0, sl._SOURCE_LOAD_ACTIVE_COUNT - 1)
                except Exception:
                    pass

    def _dispatch(self, research_id: str) -> Dict[str, Any]:
        try:
            from business.services.task_queue_service import TaskQueueService
            tq = TaskQueueService(self.settings)
            if tq.is_enabled():
                return tq.enqueue_market_research(research_id)
        except Exception as e:
            logger.warning("Market research queue unavailable, thread fallback: %s", e)

        def _run():
            try:
                self.run(research_id)
            except Exception:
                logger.exception("Market research thread failed")

        threading.Thread(target=_run, name=f"market-research-{research_id[:8]}", daemon=True).start()
        return {"queued": False, "task_id": None}

    def _search_sources(
        self,
        rid: str,
        plan: SourceQueryPlan,
    ) -> Tuple[List[str], List[str], int]:
        olx_urls: List[str] = []
        pz_ids: List[str] = []
        found_new = 0

        def log(msg: str) -> None:
            logger.info("%s", msg)
            self.repo.patch_run(rid, {"message": msg[:1000]})

        if "olx" in plan.sources:
            self._raise_if_cancelled(rid)
            from scripts.olx_scraper.run_update import run_olx_update_raw_only

            result = run_olx_update_raw_only(
                settings=self.settings,
                log_fn=log,
                days=plan.depth_days,
                regions=plan.regions or None,
                listing_types=plan.listing_type_patterns or None,
                deal_types=plan.deal_types,
                extra_query_pairs=plan.extra_olx_query_pairs or None,
                building_area_min=plan.building_area_min,
                building_area_max=plan.building_area_max,
                land_area_sotky_min=plan.land_area_sotky_min,
                land_area_sotky_max=plan.land_area_sotky_max,
            )
            olx_urls = list(dict.fromkeys(result.get("loaded_urls") or []))
            found_new += int(result.get("total_listings") or 0)
            log(f"[Дослідження] OLX: {len(olx_urls)} URL.")

        if "prozorro" in plan.sources:
            self._raise_if_cancelled(rid)
            searcher = ProzorroFilteredSearch()
            hits = searcher.search_hits(plan, log_fn=log, cancel_check=lambda: self._is_cancelled(rid))
            self._raise_if_cancelled(rid)
            pz_ids = searcher.fetch_hits_to_raw(
                hits,
                plan,
                source_load_run_id=f"research-{rid}",
                log_fn=log,
                cancel_check=lambda: self._is_cancelled(rid),
            )
            pz_ids = list(dict.fromkeys(pz_ids))
            found_new += len(pz_ids)
            log(f"[Дослідження] ProZorro: {len(pz_ids)} аукціонів.")

        return olx_urls, pz_ids, found_new

    def _run_phase2(self, rid: str, olx_urls: List[str], pz_ids: List[str]) -> int:
        """Phase 2 для знайдених ID: усі URL/аукціони йдуть у LLM (без регіонального гейту)."""
        from business.services.currency_rate_service import CurrencyRateService
        from business.services.geocoding_service import GeocodingService
        from business.services.olx_llm_extractor_service import OlxLLMExtractorService
        from business.services.source_data_load_service import (
            _promote_raw_prozorro_to_main,
            process_prozorro_llm_auction,
        )
        from business.services.unified_listings_service import UnifiedListingsService
        from data.repositories.olx_listings_repository import OlxListingsRepository
        from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
        from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
        from data.repositories.raw_prozorro_auctions_repository import RawProzorroAuctionsRepository
        from scripts.olx_scraper.run_update import _process_llm_pending

        def log(msg: str) -> None:
            logger.info("%s", msg)
            self.repo.patch_run(rid, {"message": msg[:1000]})

        processed = 0
        brokered_ids: List[str] = []
        task_queue = None
        use_brokered = False
        try:
            from business.services.task_queue_service import TaskQueueService
            task_queue = TaskQueueService(self.settings)
            use_brokered = task_queue.is_enabled()
            if use_brokered and task_queue.get_queue_control_state(TaskQueueService.LLM_QUEUE) == "disabled":
                use_brokered = False
        except Exception:
            use_brokered = False

        if olx_urls:
            raw_olx = RawOlxListingsRepository()
            main_olx = OlxListingsRepository()
            if use_brokered and task_queue:
                batch_id = f"research-olx-{rid[:12]}"
                for url in olx_urls:
                    self._raise_if_cancelled(rid)
                    brokered_ids.append(
                        task_queue.enqueue_olx_llm(
                            url,
                            metadata={
                                "source": "olx",
                                "llm_batch_id": batch_id,
                                "llm_batch_total": len(olx_urls),
                                "llm_batch_source": "olx",
                                "research_id": rid,
                            },
                        )
                    )
                processed += len(olx_urls)
                log(f"[Дослідження] OLX LLM: {len(olx_urls)} задач у черзі.")
            else:
                usd = None
                try:
                    usd = CurrencyRateService().get_usd_uah_rate()
                except Exception:
                    pass
                n = _process_llm_pending(
                    olx_urls,
                    raw_olx,
                    main_olx,
                    OlxLLMExtractorService(self.settings),
                    GeocodingService(),
                    UnifiedListingsService(self.settings),
                    usd,
                    log,
                )
                processed += n
                log(f"[Дослідження] OLX LLM inline: {n}.")

        if pz_ids:
            raw_pz = RawProzorroAuctionsRepository()
            main_pz = ProZorroAuctionsRepository()
            _promote_raw_prozorro_to_main(raw_pz, main_pz, pz_ids, log_fn=log)
            unified = UnifiedListingsService(self.settings)
            for i, aid in enumerate(pz_ids, start=1):
                self._raise_if_cancelled(rid)
                try:
                    unified.sync_prozorro_auction(aid)
                except Exception as e:
                    logger.debug("Unified sync ProZorro %s: %s", aid, e)
                if i % 50 == 0 or i == len(pz_ids):
                    log(f"[Дослідження] ProZorro unified {i}/{len(pz_ids)}")
            if use_brokered and task_queue:
                batch_id = f"research-pz-{rid[:12]}"
                for aid in pz_ids:
                    self._raise_if_cancelled(rid)
                    brokered_ids.append(
                        task_queue.enqueue_prozorro_llm(
                            aid,
                            metadata={
                                "source": "prozorro",
                                "llm_batch_id": batch_id,
                                "llm_batch_total": len(pz_ids),
                                "llm_batch_source": "prozorro",
                                "research_id": rid,
                            },
                        )
                    )
                processed += len(pz_ids)
                log(f"[Дослідження] ProZorro LLM: {len(pz_ids)} задач у черзі.")
            else:
                for aid in pz_ids:
                    self._raise_if_cancelled(rid)
                    try:
                        if process_prozorro_llm_auction(aid, settings=self.settings, log_fn=log):
                            processed += 1
                    except Exception as e:
                        logger.warning("ProZorro LLM %s: %s", aid, e)

        if use_brokered and task_queue and brokered_ids:
            log("[Дослідження] Очікування LLM…")

            def _hb():
                self._raise_if_cancelled(rid)
                self.repo.patch_run(rid, {"message": "Очікування LLM…"})

            task_queue.wait_for_all(
                brokered_ids,
                timeout_sec=LLM_WAIT_TIMEOUT_SEC,
                poll_interval_sec=3.0,
                heartbeat_fn=_hb,
            )
            log("[Дослідження] LLM завершено.")

        return processed

    def _post_filter(
        self,
        filter_spec: Dict[str, Any],
        keys: List[Dict[str, str]],
        deal_types: List[str],
    ) -> List[Dict[str, Any]]:
        if not keys:
            return []
        from domain.services.unified_search_service import find_by_filter_spec

        spec = _spec_constrained_to_keys(filter_spec, keys, deal_types)
        data, _total, err = find_by_filter_spec(
            filter_spec=spec,
            limit=min(SAMPLE_LIMIT, max(len(keys), 50)),
            skip=0,
            default_status_active=True,
            default_deal_type_sale=False,
        )
        if err:
            logger.warning("Market research post-filter: %s", err)
            from data.repositories.unified_listings_repository import UnifiedListingsRepository
            return UnifiedListingsRepository().find_by_source_keys(keys)[:SAMPLE_LIMIT]
        return list(data or [])

    def _owned_run(self, research_id: str, user_id: str) -> Dict[str, Any]:
        run = self.repo.get_run(research_id)
        if not run:
            return {"ok": False, "error": ERROR_NOT_FOUND, "message": "Дослідження не знайдено."}
        if str(run.get("user_id")) != str(user_id):
            return {"ok": False, "error": ERROR_FORBIDDEN, "message": "Немає доступу."}
        return {"ok": True, "run": run}

    def _public_run(self, run: Dict[str, Any], include_report: bool = True) -> Dict[str, Any]:
        created = run.get("created_at")
        updated = run.get("updated_at")
        out = {
            "research_id": run.get("research_id"),
            "status": run.get("status"),
            "phase": run.get("phase"),
            "message": run.get("message"),
            "deal_types": run.get("deal_types") or [DEAL_SALE],
            "depth_days": run.get("depth_days"),
            "counts": run.get("counts") or {},
            "error": run.get("error"),
            "created_at": created.isoformat() if hasattr(created, "isoformat") else created,
            "updated_at": updated.isoformat() if hasattr(updated, "isoformat") else updated,
            "filter_summary": filter_spec_summary(run.get("filter_spec")) or "Відбори не задано",
        }
        if include_report:
            out["report"] = run.get("report")
        return out

    def _patch(self, rid: str, status: str, message: str, extra: Optional[Dict[str, Any]] = None) -> None:
        patch = {"status": status, "phase": status, "message": message}
        if extra:
            patch.update(extra)
        self.repo.patch_run(rid, patch)

    def _is_cancelled(self, rid: str) -> bool:
        run = self.repo.get_run(rid) or {}
        return run.get("status") == STATUS_CANCELLED

    def _raise_if_cancelled(self, rid: str) -> None:
        if self._is_cancelled(rid):
            raise MarketResearchCancelled()


def _spec_constrained_to_keys(
    filter_spec: Optional[Dict[str, Any]],
    keys: List[Dict[str, str]],
    deal_types: List[str],
) -> Dict[str, Any]:
    spec = copy.deepcopy(normalize_filter_spec(filter_spec))
    items = list(spec.get("items") or [])
    dts = [d for d in (deal_types or []) if d]
    if dts:
        if len(dts) == 1:
            items.append({"type": "element", "field": "deal_type", "operator": "eq", "value": dts[0]})
        else:
            items.append({"type": "element", "field": "deal_type", "operator": "in", "value": dts})
    olx_ids = [k.get("source_id") for k in keys if (k or {}).get("source") == "olx" and k.get("source_id")]
    pz_ids = [k.get("source_id") for k in keys if (k or {}).get("source") == "prozorro" and k.get("source_id")]
    or_items: List[Dict[str, Any]] = []
    if olx_ids:
        or_items.append({
            "type": "group",
            "group_type": "and",
            "items": [
                {"type": "element", "field": "source", "operator": "eq", "value": "olx"},
                {"type": "element", "field": "source_id", "operator": "in", "value": list(dict.fromkeys(olx_ids))},
            ],
        })
    if pz_ids:
        or_items.append({
            "type": "group",
            "group_type": "and",
            "items": [
                {"type": "element", "field": "source", "operator": "eq", "value": "prozorro"},
                {"type": "element", "field": "source_id", "operator": "in", "value": list(dict.fromkeys(pz_ids))},
            ],
        })
    if or_items:
        items.append({"type": "group", "group_type": "or", "items": or_items})
    spec["items"] = items
    return spec
