# -*- coding: utf-8 -*-
"""
Оркестратор точкового дослідження ринку: фільтри на джерелах → Phase 2 pipeline → постфільтр → статистична довідка.
LLM лише в стандартному парсингу оголошень; аналітична записка детермінована.
"""

from __future__ import annotations

import logging
import threading
from datetime import datetime, timezone
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
ERROR_NOT_DONE = "not_done"

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

    def cancel(self, research_id: str, user_id: str, *, is_admin: bool = False) -> Dict[str, Any]:
        doc = self._owned_run(research_id, user_id, is_admin=is_admin)
        if not doc.get("ok"):
            return doc
        run = doc["run"]
        if run.get("status") not in ACTIVE_STATUSES:
            return {"ok": False, "error": ERROR_NOT_ACTIVE, "message": "Дослідження вже завершене."}
        self.repo.patch_run(research_id, {
            "status": STATUS_CANCELLED,
            "phase": STATUS_CANCELLED,
            "message": "Скасовано " + ("адміністратором" if is_admin and str(run.get("user_id")) != str(user_id) else "користувачем"),
        })
        return {"ok": True, "research_id": research_id, "status": STATUS_CANCELLED}

    def delete(self, research_id: str, user_id: str, *, is_admin: bool = False) -> Dict[str, Any]:
        doc = self._owned_run(research_id, user_id, is_admin=is_admin)
        if not doc.get("ok"):
            return doc
        run = doc["run"]
        if run.get("status") in ACTIVE_STATUSES:
            return {"ok": False, "error": ERROR_NOT_ACTIVE, "message": "Не можна видалити активне дослідження. Спочатку скасуйте."}
        if is_admin:
            ok = self.repo.delete_run_admin(research_id)
        else:
            ok = self.repo.delete_run(research_id, user_id)
        if not ok:
            return {"ok": False, "error": ERROR_NOT_FOUND, "message": "Не вдалося видалити."}
        return {"ok": True, "research_id": research_id}

    def repeat(self, research_id: str, user_id: str, *, is_admin: bool = False) -> Dict[str, Any]:
        doc = self._owned_run(research_id, user_id, is_admin=is_admin)
        if not doc.get("ok"):
            return doc
        run = doc["run"]
        return self.start(
            user_id=user_id,
            filter_spec=run.get("filter_spec"),
            deal_types=run.get("deal_types"),
            depth_days=run.get("depth_days"),
            confirm_broad=run.get("confirm_broad", False),
        )

    def list_for_user(self, user_id: str, limit: int = 30) -> List[Dict[str, Any]]:
        return [self._public_run(d, include_report=False) for d in self.repo.list_for_user(user_id, limit=limit)]

    def list_all(self, limit: int = 100, user_service=None) -> List[Dict[str, Any]]:
        runs = self.repo.list_all(limit=limit)
        return [self._public_run(d, include_report=False, user_service=user_service) for d in runs]

    def get(
        self,
        research_id: str,
        user_id: str,
        *,
        skip: int = 0,
        limit: int = TABLE_PAGE_SIZE,
        source: Optional[str] = None,
        is_admin: bool = False,
    ) -> Dict[str, Any]:
        doc = self._owned_run(research_id, user_id, is_admin=is_admin)
        if not doc.get("ok"):
            return doc
        run = doc["run"]
        out = self._public_run(run, include_report=True)
        keys = list(run.get("listing_keys") or [])
        src = (source or "").strip().lower()
        if src in ("olx", "prozorro"):
            keys = [k for k in keys if (k or {}).get("source") == src]
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

    def send_via_bot(
        self,
        research_id: str,
        user_id: str,
        *,
        what: str,
        bot_token: str,
        is_admin: bool = False,
    ) -> Dict[str, Any]:
        doc = self._owned_run(research_id, user_id, is_admin=is_admin)
        if not doc.get("ok"):
            return doc
        run = doc["run"]
        if run.get("status") != STATUS_DONE:
            return {"ok": False, "error": ERROR_NOT_DONE, "message": "Файли можна надіслати лише після завершення."}
        kind = (what or "both").strip().lower()
        if kind not in ("listings", "report", "both"):
            return {"ok": False, "error": "bad_what", "message": "Оберіть оголошення, довідку або обидва файли."}

        from datetime import datetime
        from telegram_mini_app.send_via_bot import send_file_via_telegram
        from utils.date_utils import KYIV_TZ

        stamp = datetime.now(KYIV_TZ).strftime("%Y%m%d")
        sent: List[str] = []
        uid = int(user_id)

        if kind in ("listings", "both"):
            content = self._listings_xlsx_bytes(run)
            name = f"Оголошення_дослідження_{stamp}.xlsx"
            if not send_file_via_telegram(uid, content, name, bot_token, caption="Оголошення з дослідження ринку"):
                return {"ok": False, "error": "send_failed", "message": "Не вдалося надіслати таблицю оголошень."}
            sent.append("оголошення (Excel)")

        if kind in ("report", "both"):
            from business.services.market_research_note_service import build_research_note_pdf

            try:
                content = build_research_note_pdf(run.get("report"))
            except Exception as e:
                logger.exception("Research PDF failed: %s", e)
                return {"ok": False, "error": "pdf_failed", "message": "Не вдалося зібрати PDF-довідку."}
            name = f"Довідка_дослідження_{stamp}.pdf"
            if not send_file_via_telegram(uid, content, name, bot_token, caption="Довідка з дослідження ринку"):
                return {"ok": False, "error": "send_failed", "message": "Не вдалося надіслати PDF-довідку."}
            sent.append("довідку (PDF)")

        return {
            "ok": True,
            "message": "Надіслано в чат бота: " + " і ".join(sent) + ".",
        }

    def _listings_xlsx_bytes(self, run: Dict[str, Any]) -> bytes:
        from domain.gateways.listing_gateway import ListingGateway
        from data.repositories.unified_listings_repository import UnifiedListingsRepository
        from utils.file_utils import generate_excel_in_memory

        keys = list(run.get("listing_keys") or [])
        docs = UnifiedListingsRepository().find_by_source_keys(keys) if keys else []
        by_key = {(d.get("source"), d.get("source_id")): d for d in docs}
        ordered = []
        for k in keys:
            d = by_key.get(((k or {}).get("source"), (k or {}).get("source_id")))
            if d:
                ordered.append(d)
        columns = [
            "source", "source_id", "status", "property_type", "deal_type",
            "building_area_sqm", "land_area_sqm", "title", "description", "page_url",
            "price_uah", "price_usd", "addresses", "source_updated_at",
        ]
        headers = {
            "source": "Джерело", "source_id": "ID", "status": "Статус", "property_type": "Тип",
            "deal_type": "Угода", "building_area_sqm": "Площа, м²", "land_area_sqm": "Площа землі, м²",
            "title": "Назва", "description": "Опис", "page_url": "Посилання",
            "price_uah": "Ціна, грн", "price_usd": "Ціна, $", "addresses": "Адреса",
            "source_updated_at": "Оновлено",
        }
        gateway = ListingGateway()
        coll = gateway.collection_from_raw_docs(ordered, "unified_listings")
        rows = coll.to_export_rows(columns)
        if not rows:
            rows = [{"title": "Немає даних"}]
            columns = ["title"]
            headers = {"title": "Назва"}
        return generate_excel_in_memory(rows, columns, headers).getvalue()

    @staticmethod
    def _vast_balance_snapshot() -> Optional[float]:
        try:
            from business.services.vast_billing_service import fetch_vast_available_balance_usd

            bal, err = fetch_vast_available_balance_usd()
            if err:
                logger.debug("Vast balance snapshot: %s", err)
                return None
            return bal
        except Exception as e:
            logger.debug("Vast balance snapshot failed: %s", e)
            return None

    @staticmethod
    def _build_run_metrics(
        run_started_at: Optional[datetime],
        run_finished_at: Optional[datetime],
        vast_balance_start: Optional[float],
        vast_balance_end: Optional[float],
    ) -> Dict[str, Any]:
        metrics: Dict[str, Any] = {}
        if run_started_at and run_finished_at:
            try:
                if run_started_at.tzinfo is None:
                    run_started_at = run_started_at.replace(tzinfo=timezone.utc)
                if run_finished_at.tzinfo is None:
                    run_finished_at = run_finished_at.replace(tzinfo=timezone.utc)
                metrics["duration_sec"] = max(
                    0, int((run_finished_at - run_started_at).total_seconds())
                )
                metrics["run_started_at"] = run_started_at
                metrics["run_finished_at"] = run_finished_at
            except Exception:
                pass
        if vast_balance_start is not None:
            metrics["vast_balance_start_usd"] = round(float(vast_balance_start), 4)
        if vast_balance_end is not None:
            metrics["vast_balance_end_usd"] = round(float(vast_balance_end), 4)
        if vast_balance_start is not None and vast_balance_end is not None:
            metrics["vast_cost_usd"] = round(
                max(0.0, float(vast_balance_start) - float(vast_balance_end)), 4
            )
        return metrics

    @staticmethod
    def _format_duration_sec(sec: Optional[int]) -> Optional[str]:
        if sec is None or sec < 0:
            return None
        sec = int(sec)
        if sec < 60:
            return f"{sec} с"
        minutes, seconds = divmod(sec, 60)
        if minutes < 60:
            return f"{minutes} хв {seconds} с" if seconds else f"{minutes} хв"
        hours, minutes = divmod(minutes, 60)
        parts = [f"{hours} год"]
        if minutes:
            parts.append(f"{minutes} хв")
        return " ".join(parts)

    @staticmethod
    def _format_run_stats_message(metrics: Optional[Dict[str, Any]]) -> str:
        m = metrics or {}
        lines: List[str] = []
        dur = MarketResearchService._format_duration_sec(m.get("duration_sec"))
        if dur:
            lines.append(f"Час обробки: {dur}.")
        cost = m.get("vast_cost_usd")
        bal_start = m.get("vast_balance_start_usd")
        bal_end = m.get("vast_balance_end_usd")
        if cost is not None and bal_start is not None and bal_end is not None:
            lines.append(
                f"Vast: ${float(cost):.2f} "
                f"(баланс ${float(bal_start):.2f} → ${float(bal_end):.2f})."
            )
        elif cost is not None:
            lines.append(f"Vast: ${float(cost):.2f}.")
        return "\n".join(lines)

    def _auto_send_results_via_bot(
        self,
        research_id: str,
        *,
        run_user_id: str,
        counts: Optional[Dict[str, Any]],
        filter_summary: str,
        run_metrics: Optional[Dict[str, Any]] = None,
    ) -> None:
        bot_token = (getattr(self.settings, "telegram_bot_token", None) or "").strip()
        uid = str(run_user_id or "").strip()
        if not bot_token or not uid:
            return

        current = self.repo.get_run(research_id) or {}
        if current.get("bot_notified_at"):
            return

        try:
            from telegram_mini_app.send_via_bot import send_file_via_telegram, send_message_via_telegram
            from utils.date_utils import KYIV_TZ

            chat_id = int(uid)
            stamp = datetime.now(KYIV_TZ).strftime("%Y%m%d")
            sample_count = int((counts or {}).get("matched") or 0)
            stats_lines = self._format_run_stats_message(run_metrics)
            text = (
                "Дослідження ринку завершено.\n"
                f"У вибірці: {sample_count} оголошень.\n"
                + (stats_lines + "\n" if stats_lines else "")
                + f"Умови: {filter_summary or 'Відбори не задано'}"
            )
            if not send_message_via_telegram(chat_id, text, bot_token):
                raise RuntimeError("send_message_failed")

            content = self._listings_xlsx_bytes(current)
            filename = f"Оголошення_дослідження_{stamp}.xlsx"
            if not send_file_via_telegram(
                chat_id,
                content,
                filename,
                bot_token,
                caption="Результати дослідження ринку",
            ):
                raise RuntimeError("send_document_failed")

            self.repo.patch_run(
                research_id,
                {
                    "bot_notified_at": datetime.now(KYIV_TZ),
                    "bot_auto_sent": {"listings_xlsx": True},
                    "bot_auto_send_error": None,
                },
            )
        except Exception as e:
            logger.warning("Research bot auto-send failed for %s: %s", research_id, e)
            self.repo.patch_run(research_id, {"bot_auto_send_error": str(e)[:500]})

    def run(self, research_id: str) -> Dict[str, Any]:
        """Виконується в черзі source_load або в fallback-потоці."""
        rid = str(research_id or "").strip()
        run = self.repo.get_run(rid)
        if not run:
            return {"ok": False, "error": ERROR_NOT_FOUND}

        global_count = False
        run_started_at = datetime.now(timezone.utc)
        vast_balance_start: Optional[float] = None
        self.repo.patch_run(rid, {"run_started_at": run_started_at})
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

            vast_balance_start = self._vast_balance_snapshot()
            processing_extra: Dict[str, Any] = {}
            if vast_balance_start is not None:
                processing_extra["vast_balance_start_usd"] = round(float(vast_balance_start), 4)
            self._patch(
                rid,
                STATUS_PROCESSING,
                "Pipeline: LLM і unified_listings…",
                extra=processing_extra or None,
            )
            processed_n, llm_wait_note = self._run_phase2(rid, olx_urls, pz_ids)
            self._raise_if_cancelled(rid)
            if olx_urls:
                self._ensure_olx_in_unified(olx_urls, log_fn=lambda m: self.repo.patch_run(rid, {"message": m[:1000]}))

            self._patch(rid, STATUS_ANALYZING, "Постфільтр і довідка…")
            docs = self._post_filter(spec, source_keys, dts)
            listing_keys = [
                {"source": d.get("source"), "source_id": d.get("source_id")}
                for d in docs
                if d.get("source") and d.get("source_id")
            ]
            matched_olx = sum(1 for k in listing_keys if k.get("source") == "olx")
            matched_pz = sum(1 for k in listing_keys if k.get("source") == "prozorro")
            limitations = []
            if llm_wait_note:
                limitations.append(llm_wait_note)
            if len(olx_urls) and matched_olx == 0:
                limitations.append(
                    f"OLX: знайдено {len(olx_urls)} оголошень, але жодна картка не потрапила у вибірку "
                    "(немає сирого запису або не вдалося зібрати картку)."
                )
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
                "matched_olx": matched_olx,
                "matched_prozorro": matched_pz,
            }
            run_finished_at = datetime.now(timezone.utc)
            vast_balance_end = self._vast_balance_snapshot()
            run_metrics = self._build_run_metrics(
                run_started_at,
                run_finished_at,
                vast_balance_start,
                vast_balance_end,
            )
            done_message = f"Готово: {len(listing_keys)} оголошень у вибірці."
            dur_label = self._format_duration_sec(run_metrics.get("duration_sec"))
            if dur_label:
                done_message += f" Час: {dur_label}."
            cost_usd = run_metrics.get("vast_cost_usd")
            if cost_usd is not None:
                done_message += f" Vast: ${float(cost_usd):.2f}."
            self.repo.patch_run(rid, {
                "status": STATUS_DONE,
                "phase": STATUS_DONE,
                "message": done_message,
                "listing_keys": listing_keys,
                "source_keys": source_keys,
                "counts": counts,
                "report": report,
                "error": None,
                **run_metrics,
            })
            self._auto_send_results_via_bot(
                rid,
                run_user_id=str(run.get("user_id") or ""),
                counts=counts,
                filter_summary=summary,
                run_metrics=run_metrics,
            )
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
            from utils.olx_url import normalize_olx_listing_url

            result = run_olx_update_raw_only(
                settings=self.settings,
                log_fn=log,
                days=plan.depth_days,
                regions=plan.regions or None,
                listing_types=plan.listing_type_patterns or None,
                max_workers=1,
                deal_types=plan.deal_types,
                extra_query_pairs=plan.extra_olx_query_pairs or None,
                building_area_min=plan.building_area_min,
                building_area_max=plan.building_area_max,
                land_area_sotky_min=plan.land_area_sotky_min,
                land_area_sotky_max=plan.land_area_sotky_max,
                include_unchanged_urls=True,
                apply_default_area_filters=False,
                sort_newest=False,
            )
            olx_urls = list(dict.fromkeys(
                (normalize_olx_listing_url(u) or u) for u in (result.get("loaded_urls") or []) if u
            ))
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

    def _run_phase2(self, rid: str, olx_urls: List[str], pz_ids: List[str]) -> Tuple[int, Optional[str]]:
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
        llm_wait_note: Optional[str] = None
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
            from business.services.task_queue_service import TaskQueueService, TaskWaitTimeoutError

            log("[Дослідження] Очікування LLM…")
            progress_state = {"done": -1}

            def _hb():
                self._raise_if_cancelled(rid)

            def _progress(docs, task_ids):
                snap = TaskQueueService.summarize_wait_state(task_ids, docs)
                done = int(snap.get("done") or 0)
                if done == progress_state["done"]:
                    return
                progress_state["done"] = done
                msg = (
                    "[Дослідження] Очікування LLM: готово "
                    f"{TaskQueueService.format_wait_progress(snap)}"
                )
                log(msg)

            try:
                task_queue.wait_for_all(
                    brokered_ids,
                    timeout_sec=LLM_WAIT_TIMEOUT_SEC,
                    poll_interval_sec=3.0,
                    heartbeat_fn=_hb,
                    progress_fn=_progress,
                )
                log("[Дослідження] LLM завершено.")
            except TaskWaitTimeoutError as e:
                pending = len(e.pending_ids)
                total = len(brokered_ids)
                done = max(0, total - pending)
                llm_wait_note = (
                    f"LLM-черга не завершилась повністю: готово {done}/{total} "
                    f"({e.reason}). Довідка побудована по вже зібраних картках."
                )
                logger.warning("Market research LLM wait interrupted: %s", e)
                log(f"[Дослідження] {llm_wait_note}")

        return processed, llm_wait_note

    def _post_filter(
        self,
        filter_spec: Dict[str, Any],
        keys: List[Dict[str, str]],
        deal_types: List[str],
    ) -> List[Dict[str, Any]]:
        """Лише вже знайдені ключі. Гео/джерело/дефолтний статус пошуку не застосовуємо — інакше OLX зникає."""
        if not keys:
            return []
        from data.repositories.unified_listings_repository import UnifiedListingsRepository

        docs = UnifiedListingsRepository().find_by_source_keys(keys)
        by_key = {(d.get("source"), d.get("source_id")): d for d in docs}
        ordered: List[Dict[str, Any]] = []
        seen = set()
        from utils.olx_url import normalize_olx_listing_url

        for k in keys:
            src = (k or {}).get("source")
            sid = (k or {}).get("source_id")
            if not src or not sid:
                continue
            lookup_ids = [sid]
            if src == "olx":
                can = normalize_olx_listing_url(str(sid))
                if can and can not in lookup_ids:
                    lookup_ids.append(can)
            if any((src, cand) in seen for cand in lookup_ids):
                continue
            doc = None
            for cand in lookup_ids:
                doc = by_key.get((src, cand))
                if doc:
                    break
            if not doc:
                continue
            for cand in lookup_ids:
                seen.add((src, cand))
            seen.add((src, doc.get("source_id")))
            ordered.append(doc)
        kept = [d for d in ordered if _listing_matches_research_attrs(d, filter_spec, deal_types)]
        kept.sort(key=lambda d: str(d.get("source_updated_at") or d.get("system_updated_at") or ""), reverse=True)
        return kept[:SAMPLE_LIMIT]

    def _ensure_olx_in_unified(self, olx_urls: List[str], log_fn) -> None:
        """Після LLM: sync з olx_listings; якщо картки немає — чернетка з raw (ціна/заголовок зі списку)."""
        from data.repositories.olx_listings_repository import OlxListingsRepository
        from data.repositories.raw_olx_listings_repository import RawOlxListingsRepository
        from data.repositories.unified_listings_repository import UnifiedListingsRepository
        from business.services.unified_listings_service import UnifiedListingsService
        from utils.deal_type import deal_type_from_olx_url
        from utils.olx_url import normalize_olx_listing_url

        unified_svc = UnifiedListingsService(self.settings)
        unified_repo = UnifiedListingsRepository()
        olx_repo = OlxListingsRepository()
        raw_repo = RawOlxListingsRepository()
        missing = 0
        stubs = 0
        for url in olx_urls:
            can = normalize_olx_listing_url(url) or url
            if unified_repo.find_by_source_id("olx", can) or unified_repo.find_by_source_id("olx", url):
                continue
            if olx_repo.find_by_url(can):
                try:
                    unified_svc.sync_olx_listing(can, include_other=True, side_effects=False)
                except Exception as e:
                    logger.warning("Research OLX sync %s: %s", can[:80], e)
                if unified_repo.find_by_source_id("olx", can):
                    continue
            raw = raw_repo.find_by_url(can) or raw_repo.find_by_url(url)
            if not raw:
                missing += 1
                continue
            stub = _unified_stub_from_olx_raw(raw, can, deal_type_from_olx_url)
            try:
                unified_repo.upsert_listing(stub)
                stubs += 1
            except Exception as e:
                logger.warning("Research OLX stub %s: %s", can[:80], e)
                missing += 1
        if log_fn:
            log_fn(f"[Дослідження] OLX картки: чернеток {stubs}, без raw {missing}.")

    def _owned_run(self, research_id: str, user_id: str, *, is_admin: bool = False) -> Dict[str, Any]:
        run = self.repo.get_run(research_id)
        if not run:
            return {"ok": False, "error": ERROR_NOT_FOUND, "message": "Дослідження не знайдено."}
        if not is_admin and str(run.get("user_id")) != str(user_id):
            return {"ok": False, "error": ERROR_FORBIDDEN, "message": "Немає доступу."}
        return {"ok": True, "run": run}

    def _public_run(self, run: Dict[str, Any], include_report: bool = True, user_service=None) -> Dict[str, Any]:
        created = run.get("created_at")
        updated = run.get("updated_at")
        out = {
            "research_id": run.get("research_id"),
            "user_id": str(run.get("user_id") or ""),
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
        dur_sec = run.get("duration_sec")
        if dur_sec is not None:
            try:
                out["duration_sec"] = int(dur_sec)
                out["duration_label"] = self._format_duration_sec(int(dur_sec))
            except (TypeError, ValueError):
                pass
        for key in ("vast_cost_usd", "vast_balance_start_usd", "vast_balance_end_usd"):
            if run.get(key) is not None:
                try:
                    out[key] = float(run[key])
                except (TypeError, ValueError):
                    pass
        rs = run.get("run_started_at")
        rf = run.get("run_finished_at")
        if rs is not None:
            out["run_started_at"] = rs.isoformat() if hasattr(rs, "isoformat") else rs
        if rf is not None:
            out["run_finished_at"] = rf.isoformat() if hasattr(rf, "isoformat") else rf
        if user_service:
            try:
                uid = int(run.get("user_id") or 0)
                out["user_name"] = user_service.get_user_nickname(uid) or str(uid)
            except Exception:
                out["user_name"] = str(run.get("user_id") or "")
        if include_report:
            out["filter_spec"] = run.get("filter_spec")
            report = run.get("report")
            if report:
                from business.services.market_research_note_service import conclusions_from_report

                report = dict(report)
                report["conclusions"] = conclusions_from_report(report)
            out["report"] = report
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


def _unified_stub_from_olx_raw(raw: Dict[str, Any], canonical_url: str, deal_type_from_url) -> Dict[str, Any]:
    sd = raw.get("search_data") or {}
    url = str(raw.get("url") or canonical_url)
    price_val = sd.get("price_value")
    currency = str(sd.get("currency") or "UAH").strip().upper() or "UAH"
    price_uah = price_usd = None
    try:
        n = float(price_val) if price_val is not None else None
    except (TypeError, ValueError):
        n = None
    if n is not None:
        if currency == "USD":
            price_usd = n
        else:
            price_uah = n
    path = url.lower()
    property_type = "Земельна ділянка" if "zemlya" in path or "/land" in path else "Комерційна нерухомість"
    return {
        "source": "olx",
        "source_id": canonical_url,
        "page_url": canonical_url,
        "title": sd.get("title") or "",
        "status": "активне",
        "property_type": property_type,
        "deal_type": deal_type_from_url(url),
        "price_uah": price_uah,
        "price_usd": price_usd,
        "region": raw.get("approximate_region") or None,
        "source_updated_at": raw.get("loaded_at") or raw.get("updated_at"),
    }


def _listing_matches_research_attrs(
    doc: Dict[str, Any],
    filter_spec: Optional[Dict[str, Any]],
    deal_types: List[str],
) -> bool:
    from business.services.source_filter_mapper import (
        _collect_eq_in,
        _geo_names,
        _num,
        _range_bounds,
        _text_contains,
    )
    from utils.ukraine_regions import normalize_region_to_canonical

    spec = filter_spec if isinstance(filter_spec, dict) else {}
    dts = [d for d in (deal_types or []) if d]
    dt = str(doc.get("deal_type") or "").strip().lower()
    if dts and dt and dt not in dts:
        return False

    statuses = [str(v) for v in _collect_eq_in(spec, "status")]
    if statuses:
        st = str(doc.get("status") or "").strip()
        if st not in statuses:
            return False

    property_types = [str(v) for v in _collect_eq_in(spec, "property_type")]
    if property_types:
        pt = str(doc.get("property_type") or "").strip()
        if pt not in property_types:
            return False

    def _in_range(val: Optional[float], lo: Optional[float], hi: Optional[float]) -> bool:
        if lo is None and hi is None:
            return True
        if val is None:
            return False
        if lo is not None and val < lo:
            return False
        if hi is not None and val > hi:
            return False
        return True

    bmin, bmax = _range_bounds(spec, "building_area_sqm")
    if not _in_range(_num(doc.get("building_area_sqm")), bmin, bmax):
        return False

    lmin, lmax = _range_bounds(spec, "land_area_sotky")
    land_sqm = _num(doc.get("land_area_sqm"))
    land_sotky = (land_sqm / 100.0) if land_sqm is not None else None
    if not _in_range(land_sotky, lmin, lmax):
        return False

    pmin_uah, pmax_uah = _range_bounds(spec, "price_uah")
    if not _in_range(_num(doc.get("price_uah")), pmin_uah, pmax_uah):
        return False
    pmin_usd, pmax_usd = _range_bounds(spec, "price_usd")
    if not _in_range(_num(doc.get("price_usd")), pmin_usd, pmax_usd):
        return False

    want_regions = _geo_names(spec, "region")
    want_settlements = _geo_names(spec, "settlement")
    if want_regions or want_settlements:
        doc_region_raw = str(doc.get("region") or "").strip()
        doc_city = str(doc.get("city") or "").strip()
        doc_canon = normalize_region_to_canonical(doc_region_raw) or doc_region_raw

        if want_regions:
            want_canons = {
                (normalize_region_to_canonical(r) or str(r).strip())
                for r in want_regions
                if r and str(r).strip()
            }
            region_ok = bool(doc_canon and doc_canon in want_canons)
            if not region_ok:
                return False

        if want_settlements:
            names = {str(s).strip().lower() for s in want_settlements if str(s).strip()}
            cities = set()
            if doc_city:
                cities.add(doc_city.lower())
            addrs = doc.get("addresses") or []
            if isinstance(addrs, list):
                for a in addrs:
                    if not isinstance(a, dict):
                        continue
                    for key in ("city", "settlement", "locality"):
                        v = a.get(key)
                        if v:
                            cities.add(str(v).strip().lower())
            if not (cities & names):
                return False

    title_q = (_text_contains(spec, "title") or "").lower()
    if title_q and title_q not in str(doc.get("title") or "").lower():
        return False
    desc_q = (_text_contains(spec, "description") or "").lower()
    if desc_q and desc_q not in str(doc.get("description") or "").lower():
        return False
    return True

