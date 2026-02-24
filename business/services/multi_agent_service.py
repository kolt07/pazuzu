# -*- coding: utf-8 -*-
"""
Мультиагентний сервіс: оркестрація помічника, планувальника, аналітика, інтерпретатора та агента безпеки.
Запит: перевірка безпеки -> формування контексту -> інтерпретація -> планування -> виконання -> відповідь.
Для складних/розмовних запитів — fallback на одного LangChain-агента.
"""

import logging
import threading
import time
import uuid
from collections import deque
from typing import Optional, Dict, Any, List, Callable, Tuple

# Стани запиту (state machine) для логів та дебагу
REQUEST_STATE_RECEIVED = "RECEIVED"
REQUEST_STATE_SECURITY_CHECKED = "SECURITY_CHECKED"
REQUEST_STATE_INTERPRETED = "INTERPRETED"
REQUEST_STATE_PLANNED = "PLANNED"
REQUEST_STATE_ROUTED_TO_AGENT = "ROUTED_TO_AGENT"
REQUEST_STATE_EXECUTED = "EXECUTED"
REQUEST_STATE_DELIVERED = "DELIVERED"

# Кеш інтерпретації: (normalized_query, context_key) -> (structured, expiry_time)
_INTENT_CACHE: Dict[Tuple[str, str], Tuple[Dict[str, Any], float]] = {}
_INTENT_CACHE_TTL_SECONDS = 300
_INTENT_CACHE_LOCK = threading.Lock()

from config.settings import Settings
from data.repositories.agent_activity_log_repository import AgentActivityLogRepository
from data.repositories.session_state_repository import SessionStateRepository
from data.repositories.pending_export_repository import PendingExportRepository
from business.agents.security_agent import SecurityAgent
from business.agents.interpreter_agent import InterpreterAgent
from business.agents.planner_agent import PlannerAgent
from business.agents.analyst_agent import AnalystAgent
from business.agents.assistant_agent import AssistantAgent
from business.agents.analysis_planner_agent import AnalysisPlannerAgent
from business.agents.intent_detector_agent import IntentDetectorAgent
from business.agents.query_structure_agent import QueryStructureAgent
from business.agents.pipeline_builder_agent import PipelineBuilderAgent
from domain.models import CanonicalQuery
from business.services.langchain_agent_service import LangChainAgentService
from business.services.answer_composer_service import AnswerComposerService
from business.services.analytical_reasoning_service import AnalyticalReasoningService
from business.services.collection_knowledge_service import CollectionKnowledgeService
from business.services.pipeline_interpreter_service import PipelineInterpreterService
from business.services.pipeline_service import PipelineService
from business.services.result_validator_service import ResultValidatorService
from business.services.final_answer_refinement_service import FinalAnswerRefinementService
from utils.query_builder import QueryBuilder

logger = logging.getLogger(__name__)


class MultiAgentService:
    """
    Вхідна точка обробки запиту користувача в мультиагентній архітектурі:
    - Агент безпеки перевіряє запит; при порушенні — сповіщення адмінам.
    - Агент-помічник формує контекст, викликає інтерпретатора та планувальника, виконує пайплайн.
    - Лог наміру та дій під-агентів у БД (agent_activity_log).
    - Для складних запитів делегує повну обробку LangChain-агенту (з інструментами та пам'яттю).
    """

    def __init__(
        self,
        settings: Settings,
        user_service=None,
        notify_admins_fn: Optional[Callable[[str, Optional[str], Optional[str]], None]] = None,
    ):
        self.settings = settings
        self.user_service = user_service
        self.notify_admins_fn = notify_admins_fn or (lambda msg, uid, det: None)

        self.activity_log = AgentActivityLogRepository()
        self.session_state_repo = SessionStateRepository()
        self.pending_export_repo = PendingExportRepository()
        self._langchain_service: Optional[LangChainAgentService] = None
        self._security_agent: Optional[SecurityAgent] = None
        self._assistant_agent: Optional[AssistantAgent] = None
        self._last_excel_files: List[Dict[str, Any]] = []
        self._rate_limit_timestamps: Dict[str, deque] = {}
        self._rate_limit_lock = threading.Lock()

    @property
    def langchain_service(self) -> LangChainAgentService:
        if self._langchain_service is None:
            self._langchain_service = LangChainAgentService(self.settings)
        return self._langchain_service

    @property
    def security_agent(self) -> SecurityAgent:
        if self._security_agent is None:
            self._security_agent = SecurityAgent(notify_admins_fn=self.notify_admins_fn)
        return self._security_agent

    def _get_assistant_agent(self) -> AssistantAgent:
        if self._assistant_agent is not None:
            return self._assistant_agent
        run_tool_fn = self.langchain_service.run_tool
        interpreter = InterpreterAgent(self.settings)
        planner = PlannerAgent(run_tool_fn=run_tool_fn)
        analyst = AnalystAgent(run_tool_fn=run_tool_fn)

        def log_intent(request_id: str, user_id: Optional[str], payload: Dict[str, Any]) -> None:
            self.activity_log.log(
                request_id=request_id,
                user_id=user_id,
                agent_name="assistant",
                step=AgentActivityLogRepository.STEP_INTENT,
                payload=payload,
            )

        def log_action(request_id: str, agent_name: str, payload: Dict[str, Any]) -> None:
            self.activity_log.log(
                request_id=request_id,
                user_id=None,
                agent_name=agent_name,
                step=AgentActivityLogRepository.STEP_ACTION,
                payload=payload,
            )

        self._assistant_agent = AssistantAgent(
            interpreter=interpreter,
            planner=planner,
            analyst=analyst,
            log_intent_fn=log_intent,
            log_action_fn=log_action,
        )
        return self._assistant_agent

    def _log_request_state(self, request_id: str, user_id: Optional[str], state: str, payload: Optional[Dict[str, Any]] = None) -> None:
        """Логує стан запиту (state machine) для трасування."""
        try:
            self.activity_log.log(
                request_id=request_id,
                user_id=user_id,
                agent_name="request",
                step="state",
                payload={"state": state, **(payload or {})},
            )
        except Exception as e:
            logger.debug("_log_request_state: %s", e)

    def _check_rate_limit_and_complexity(self, user_id: Optional[str], user_query: str) -> Optional[str]:
        """
        Security layer: rate limit по user_id та max complexity (довжина запиту).
        Повертає None якщо дозволено, інакше рядок помилки.
        """
        max_len = getattr(self.settings, "max_query_complexity_length", 8000)
        if len(user_query) > max_len:
            return f"Запит занадто довгий (макс. {max_len} символів). Скоротьте його."
        if not user_id:
            return None
        limit = getattr(self.settings, "rate_limit_requests_per_minute", 30)
        now = time.time()
        with self._rate_limit_lock:
            q = self._rate_limit_timestamps.get(user_id)
            if q is None:
                q = deque(maxlen=limit * 2)
                self._rate_limit_timestamps[user_id] = q
            while q and q[0] < now - 60:
                q.popleft()
            if len(q) >= limit:
                return f"Забагато запитів за хвилину (ліміт {limit}). Зачекайте, будь ласка."
            q.append(now)
        return None

    def _try_analytics_aggregation_by_city(
        self,
        user_query: str,
        intent_info: Dict[str, Any],
        request_id: str,
        status_callback: Optional[Callable[[str], None]],
    ) -> Optional[str]:
        """
        Для analytical_text з запитом на порівняння цін по містах (Київ vs Львів тощо) —
        викликає execute_analytics з groupBy: ["city"]. ProZorro, потім OLX як fallback.
        """
        if intent_info.get("response_format") not in ("analytical_text", "text_answer"):
            return None
        q = (user_query or "").strip().lower()
        # Шаблони: "порівняй ціни в Києві та Львові", "середня ціна за м² у Києві і Львові"
        city_keywords = ["києві", "київ", "львові", "львів", "харкові", "харків", "одесі", "одеса", "дніпрі", "дніпро"]
        has_city_comparison = (
            any(p in q for p in ["порівняй", "порівняння", "порівняти"])
            or any(p in q for p in ["середня ціна", "ціна за м", "ціна за кв", "ціна за м²"])
        ) and sum(1 for c in city_keywords if c in q) >= 2
        if not has_city_comparison:
            return None
        # Визначаємо міста з запиту (спрощено — по ключовим словам)
        cities = []
        city_map = [
            ("києві", "Київ"), ("київ", "Київ"),
            ("львові", "Львів"), ("львів", "Львів"),
            ("харкові", "Харків"), ("харків", "Харків"),
            ("одесі", "Одеса"), ("одеса", "Одеса"),
            ("дніпрі", "Дніпро"), ("дніпро", "Дніпро"),
        ]
        for kw, city in city_map:
            if kw in q and city not in cities:
                cities.append(city)
        if len(cities) < 2:
            return None
        # Розширюємо міста варіантами (Київ, м. Київ тощо) для збігу з addresses.settlement / llm_result
        settlement_values = set(cities)
        for c in cities:
            if not (c.startswith("м.") or c.startswith("м ")):
                settlement_values.add("м. " + c)
        settlement_list = list(settlement_values)
        # Фільтр за типом: комерційна нерухомість
        want_commercial = "комерційн" in q or "комерційна" in q
        if status_callback:
            try:
                status_callback("Аналітика по містах...")
            except Exception:
                pass
        try:
            from business.services.relative_date_resolver import RelativeDateResolver
            resolved = RelativeDateResolver().resolve({"period": "last_30_days"})
            # Використовуємо $in для settlement — збігає і "Дніпро", і "м. Дніпро"
            filters = {"city": {"$in": settlement_list}}
            if resolved and isinstance(resolved, dict) and "gte" in resolved and "lte" in resolved:
                filters["auction_data.dateModified"] = {"$gte": resolved["gte"], "$lte": resolved["lte"]}
            if want_commercial:
                filters["property_type"] = "Комерційна нерухомість"
            # 1. ProZorro з groupBy: ["city"]
            analytics_query = {
                "collection": "prozorro_auctions",
                "metric": "average_price_per_m2",
                "groupBy": ["city"],
                "filters": filters,
            }
            result = self.langchain_service.run_tool("execute_analytics", analytics_query)
            if result.get("success"):
                data = result.get("data") or result.get("results") or []
                if data:
                    composer = AnswerComposerService()
                    contract = composer.compose({
                        "data": data,
                        "query_type": "analytical",
                        "row_count": len(data),
                        "has_attachment": False,
                        "response_format": "analytical_text",
                    })
                    summary = contract.get("summary", "")
                    if summary:
                        refined = FinalAnswerRefinementService(self.settings).refine(summary, user_query)
                        return refined if refined else summary
            # 2. Fallback: OLX з groupBy: ["city"]
            filters_olx = {"city": {"$in": settlement_list}}
            if resolved and isinstance(resolved, dict) and "gte" in resolved and "lte" in resolved:
                filters_olx["updated_at"] = {"$gte": resolved["gte"], "$lte": resolved["lte"]}
            if want_commercial:
                filters_olx["property_type"] = "Комерційна нерухомість"
            analytics_query_olx = {
                "collection": "olx_listings",
                "metric": "average_price_per_m2",
                "groupBy": ["city"],
                "filters": filters_olx,
            }
            result_olx = self.langchain_service.run_tool("execute_analytics", analytics_query_olx)
            if result_olx.get("success"):
                data = result_olx.get("data") or result_olx.get("results") or []
                if data:
                    composer = AnswerComposerService()
                    contract = composer.compose({
                        "data": data,
                        "query_type": "analytical",
                        "row_count": len(data),
                        "has_attachment": False,
                        "response_format": "analytical_text",
                    })
                    summary = contract.get("summary", "")
                    if summary:
                        refined = FinalAnswerRefinementService(self.settings).refine(summary, user_query)
                        return refined if refined else summary
            # 3. Fallback: price_analytics (передобчислені агрегати з unified_listings)
            try:
                from datetime import datetime, timezone
                from business.services.price_analytics_service import PriceAnalyticsService
                month_key = datetime.now(timezone.utc).strftime("%Y-%m")
                analytics_svc = PriceAnalyticsService()
                data_from_aggregates = []
                cities_lower = {c.strip().lower() for c in cities}
                # Отримуємо агрегати за місяць (без фільтра по місту), потім фільтруємо по cities
                all_rows = analytics_svc.get_aggregated_analytics(
                    period_type="month",
                    period_key=month_key,
                    property_type="Комерційна нерухомість" if want_commercial else None,
                )
                by_city: Dict[str, Dict[str, Any]] = {}
                for r in (all_rows or []):
                    gc = (r.get("group_city") or (r.get("group_by") or {}).get("city", "") or "").strip()
                    gc_norm = gc.replace("м.", "").replace("м ", "").strip().lower()
                    matched = next((c for c in cities if c.lower() in gc_norm or gc_norm in c.lower()), None)
                    if not matched:
                        continue
                    m = r.get("metrics", {}) or {}
                    pm2 = m.get("price_per_m2_uah") or {}
                    avg_val = pm2.get("avg") if isinstance(pm2, dict) else None
                    cnt = r.get("count", 0)
                    if avg_val is not None and avg_val > 0:
                        prev = by_city.get(matched)
                        if prev is None or cnt > prev.get("count", 0):
                            by_city[matched] = {
                                "city": gc or matched,
                                "value": round(float(avg_val), 2),
                                "average_price_per_m2": round(float(avg_val), 2),
                                "count": cnt,
                                "unit": "UAH/m²",
                            }
                data_from_aggregates = list(by_city.values())
                if data_from_aggregates:
                    composer = AnswerComposerService()
                    contract = composer.compose({
                        "data": data_from_aggregates,
                        "query_type": "analytical",
                        "row_count": len(data_from_aggregates),
                        "has_attachment": False,
                        "response_format": "analytical_text",
                    })
                    summary = contract.get("summary", "")
                    if summary:
                        refined = FinalAnswerRefinementService(self.settings).refine(summary, user_query)
                        return refined if refined else summary
            except Exception as e:
                logger.debug("_try_analytics_aggregation_by_city price_analytics fallback: %s", e)
            # 4. Fallback: execute_aggregation на unified_listings (адреси з addresses.settlement)
            try:
                from business.services.relative_date_resolver import RelativeDateResolver
                from datetime import datetime
                resolved = RelativeDateResolver().resolve({"period": "last_30_days"})
                cutoff_dt = None
                if resolved and isinstance(resolved, dict) and "gte" in resolved:
                    try:
                        cutoff_dt = datetime.fromisoformat(
                            str(resolved["gte"]).replace("Z", "+00:00")
                        )
                    except (ValueError, TypeError):
                        pass
                # Розширюємо cities варіантами (Київ, м. Київ тощо) для збігу з addresses.settlement
                settlement_values = set(cities)
                for c in cities:
                    if not (c.startswith("м.") or c.startswith("м ")):
                        settlement_values.add("м. " + c)
                settlement_list = list(settlement_values)
                base_match = {"addresses": {"$exists": True, "$ne": []}, "status": "активне"}
                if cutoff_dt:
                    base_match["source_updated_at"] = {"$gte": cutoff_dt}
                if want_commercial:
                    base_match["property_type"] = "Комерційна нерухомість"
                pipeline = [
                    {"$match": base_match},
                    {"$unwind": "$addresses"},
                    {"$match": {"addresses.settlement": {"$in": settlement_list}}},
                ]
                pipeline.extend([
                    {"$match": {"price_per_m2_uah": {"$exists": True, "$gt": 0}}},
                    {"$group": {
                        "_id": "$addresses.settlement",
                        "value": {"$avg": "$price_per_m2_uah"},
                        "count": {"$sum": 1},
                    }},
                    {"$project": {"_id": 0, "city": "$_id", "value": 1, "count": 1}},
                ])
                agg_result = self.langchain_service.run_tool(
                    "execute_aggregation",
                    {"collection_name": "unified_listings", "pipeline": pipeline, "limit": 20},
                )
                if agg_result.get("success"):
                    data_agg = agg_result.get("results") or agg_result.get("data") or []
                    if data_agg:
                        formatted = [
                            {
                                "city": r.get("city", "н/д"),
                                "value": round(float(r.get("value", 0)), 2),
                                "average_price_per_m2": round(float(r.get("value", 0)), 2),
                                "count": r.get("count", 0),
                                "unit": "UAH/m²",
                            }
                            for r in data_agg
                        ]
                        composer = AnswerComposerService()
                        contract = composer.compose({
                            "data": formatted,
                            "query_type": "analytical",
                            "row_count": len(formatted),
                            "has_attachment": False,
                            "response_format": "analytical_text",
                        })
                        summary = contract.get("summary", "")
                        if summary:
                            refined = FinalAnswerRefinementService(self.settings).refine(summary, user_query)
                            return refined if refined else summary
            except Exception as e:
                logger.debug("_try_analytics_aggregation_by_city unified_listings fallback: %s", e)
            return None
        except Exception as e:
            logger.debug("_try_analytics_aggregation_by_city: %s", e)
            return None

    def _try_analytics_aggregation_by_region(
        self,
        user_query: str,
        intent_info: Dict[str, Any],
        request_id: str,
        status_callback: Optional[Callable[[str], None]],
    ) -> Optional[str]:
        """
        Для analytical_text з запитом на середню ціну по областях — викликає execute_analytics.
        Повертає відповідь або None якщо не підходить.
        """
        if intent_info.get("response_format") not in ("analytical_text", "text_answer"):
            return None
        q = (user_query or "").lower()
        is_avg_by_region = any(p in q for p in ["середня ціна", "середня ціна за", "ціна за кв", "ціна за м"]) and any(
            p in q for p in ["по областям", "по регіонах", "по областях", "областям", "регіонах"]
        )
        is_top_region = any(p in q for p in ["найдорожчий регіон", "найдешевший регіон", "найдорожча область", "найдешевша область"])
        if not (is_avg_by_region or is_top_region):
            return None
        if status_callback:
            try:
                status_callback("Аналітика по областях...")
            except Exception:
                pass
        try:
            from business.services.relative_date_resolver import RelativeDateResolver
            resolved = RelativeDateResolver().resolve({"period": "last_30_days"})
            filters = {}
            if resolved and isinstance(resolved, dict) and "gte" in resolved and "lte" in resolved:
                filters = {"auction_data.dateModified": {"$gte": resolved["gte"], "$lte": resolved["lte"]}}
            analytics_query = {
                "collection": "prozorro_auctions",
                "metric": "average_price_per_m2",
                "groupBy": ["region"],
                "filters": filters,
            }
            result = self.langchain_service.run_tool("execute_analytics", analytics_query)
            if not result.get("success"):
                logger.info("[request_id=%s] execute_analytics для областей не вдався: %s", request_id, result.get("error"))
                return None
            data = result.get("data") or result.get("results") or []
            if is_top_region and data:
                rev = "найдорожч" in q
                data_sorted = sorted(data, key=lambda x: float((x.get("average_price_per_m2") or x.get("value") or 0) or 0), reverse=rev)
                data = data_sorted[:1]
            if not data:
                composer = AnswerComposerService()
                contract = composer.compose({
                    "data": [],
                    "query_type": "analytical",
                    "row_count": 0,
                    "has_attachment": False,
                    "response_format": "analytical_text",
                })
                return contract.get("summary", "За вказаними критеріями даних не знайдено.")
            if is_top_region and len(data) == 1:
                row = data[0]
                region = row.get("region", "н/д")
                val = row.get("value") or row.get("average_price_per_m2") or 0
                label = "Найдорожчий" if "найдорожч" in q else "Найдешевший"
                return f"{label} регіон за середньою ціною за кв.м.: {region} із середньою ціною {float(val):,.0f} грн/м²."
            composer = AnswerComposerService()
            contract = composer.compose({
                "data": data,
                "query_type": "analytical",
                "row_count": len(data),
                "has_attachment": False,
                "response_format": "analytical_text",
            })
            summary = contract.get("summary", "")
            if summary:
                refined = FinalAnswerRefinementService(self.settings).refine(summary, user_query)
                return refined if refined else summary
            return None
        except Exception as e:
            logger.debug("_try_analytics_aggregation_by_region: %s", e)
            return None

    def _get_context_summary(
        self,
        user_id: Optional[str],
        chat_id: Optional[str] = None,
        listing_context: Optional[Dict[str, Any]] = None,
    ) -> str:
        """Короткий контекст з пам'яті розмови та загальні знання про дані для інтерпретатора."""
        parts = []
        try:
            knowledge_service = CollectionKnowledgeService()
            data_knowledge = knowledge_service.get_knowledge_for_agent(max_length=2000)
            if data_knowledge:
                parts.append(data_knowledge)
        except Exception as e:
            logger.debug("collection knowledge for context: %s", e)
        if listing_context and isinstance(listing_context, dict):
            focus_parts = ["ВАЖЛИВО: Розмова ведеться про КОНКРЕТНЕ оголошення. Усі відповіді мають стосуватися саме цього об'єкта."]
            if listing_context.get("page_url"):
                focus_parts.append(f"Посилання: {listing_context['page_url']}")
            if listing_context.get("summary"):
                focus_parts.append(f"Опис: {listing_context['summary']}")
            parts.insert(0, "\n".join(focus_parts))
        if user_id:
            try:
                memory_key = f"{user_id}:{chat_id}" if chat_id else str(user_id)
                memory = self.langchain_service._get_or_create_memory(memory_key)
                if memory.summary:
                    parts.append(memory.summary[:1500])
                for h, a in memory.buffer[-3:]:
                    parts.append(f"Користувач: {h[:300]}")
                    parts.append(f"Асистент: {a[:300]}")
                if chat_id:
                    from data.repositories.chat_session_repository import ChatSessionRepository
                    chat_repo = ChatSessionRepository()
                    session = chat_repo.get(str(user_id), chat_id)
                    service_data = session.get("service_data") or {}
                    if not listing_context and service_data.get("listing_context"):
                        lc = service_data["listing_context"]
                        focus_parts = ["ВАЖЛИВО: Розмова ведеться про КОНКРЕТНЕ оголошення. Усі відповіді мають стосуватися саме цього об'єкта."]
                        if lc.get("page_url"):
                            focus_parts.append(f"Посилання: {lc['page_url']}")
                        if lc.get("summary"):
                            focus_parts.append(f"Опис: {lc['summary']}")
                        parts.insert(0, "\n".join(focus_parts))
                    temp_colls = service_data.get("temp_collections") or []
                    if temp_colls:
                        last = temp_colls[-1]
                        parts.append(
                            f"Остання вибірка в цьому діалозі: temp_collection_id={last.get('temp_collection_id')}, "
                            f"джерело={last.get('source_collection')}, рядків={last.get('count')}. "
                            "Користувач може сказати «експортуй це» — використай export_from_temp_collection."
                        )
            except Exception as e:
                logger.debug("context summary: %s", e)
        return "\n\n".join(parts) if parts else ""

    def process_query(
        self,
        user_query: str,
        user_id: Optional[str] = None,
        chat_id: Optional[str] = None,
        listing_context: Optional[Dict[str, Any]] = None,
        stream_callback: Optional[Callable[[str], None]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
        thinking_callback: Optional[Callable[[str], None]] = None,
        reply_to_text: Optional[str] = None,
        request_id: Optional[str] = None,
        explicit_intent: Optional[str] = None,
        explicit_params: Optional[Dict[str, Any]] = None,
    ) -> str:
        """
        Обробляє запит: безпека -> двоступенева маршрутизація (rule-based, потім LLM) -> пайплайн або LangChain-агент.
        Повертає текст відповіді. Excel-файли з останнього запуску доступні через get_last_excel_files().
        request_id: опційний ідентифікатор запиту для трасування в логах та agent_activity_log; якщо не передано — генерується.
        explicit_intent, explicit_params: для кнопок/Mini App — гарантований пайплайн без інтерпретації тексту (report_last_day | report_last_week | export_data).
        """
        self._last_excel_files = []
        request_id = request_id or str(uuid.uuid4())
        start_time = time.perf_counter()
        query_preview = (user_query or "").strip()[:200]
        logger.info(
            "[request_id=%s] process_query: query_len=%s, preview=%s",
            request_id,
            len((user_query or "")),
            query_preview if query_preview else "(порожній)",
        )
        self._log_request_state(request_id, user_id, REQUEST_STATE_RECEIVED)
        _scb = status_callback

        def _status(msg: str) -> None:
            if _scb:
                try:
                    _scb(msg)
                except Exception:
                    pass

        if user_query.strip().lower().startswith("confirm_export:"):
            confirm_rid = user_query.strip().split(":", 1)[1].strip()
            pending = self.pending_export_repo.get(confirm_rid)
            if not pending:
                return "Недійсний або прострочений токен підтвердження експорту."
            self.langchain_service._current_request_id = request_id
            self.langchain_service._current_user_id = user_id
            self.langchain_service._current_chat_id = chat_id
            try:
                result = self.langchain_service.run_tool(
                    "export_from_temp_collection",
                    {
                        "temp_collection_id": pending["temp_collection_id"],
                        "filename_prefix": pending.get("filename_prefix", "export"),
                        "format": pending.get("format", "xlsx"),
                        "skip_confirm": True,
                    },
                )
            finally:
                self.langchain_service._current_request_id = None
                self.langchain_service._current_user_id = None
                self.langchain_service._current_chat_id = None
            if result.get("success"):
                self._last_excel_files = [{
                    "file_base64": result.get("file_base64"),
                    "filename": result.get("filename"),
                    "rows_count": result.get("rows_count", 0),
                    "columns_count": result.get("columns_count", 0),
                }]
                return "Готово. Файл надіслано окремим повідомленням."
            return result.get("error", "Помилка експорту.")

        _status("Перевірка безпеки...")
        allowed, security_message = self.security_agent.check(user_query, user_id)
        if not allowed:
            return f"Запит не може бути виконаний: {security_message}"

        rate_err = self._check_rate_limit_and_complexity(user_id, user_query)
        if rate_err:
            return rate_err
        self._log_request_state(request_id, user_id, REQUEST_STATE_SECURITY_CHECKED)

        # Контекст оголошення: зберігаємо при наявності, підставляємо з сесії при відсутності
        effective_listing_context = listing_context
        if user_id and chat_id:
            try:
                from data.repositories.chat_session_repository import ChatSessionRepository
                chat_repo = ChatSessionRepository()
                if effective_listing_context and isinstance(effective_listing_context, dict):
                    chat_repo.set_listing_context(str(user_id), chat_id, effective_listing_context)
                elif not effective_listing_context:
                    stored = chat_repo.get_listing_context(str(user_id), chat_id)
                    if stored:
                        effective_listing_context = stored
                        logger.info("[request_id=%s] listing_context відновлено з сесії чату", request_id)
            except Exception as e:
                logger.debug("listing_context persist/restore: %s", e)

        context_summary = self._get_context_summary(user_id, chat_id, effective_listing_context)
        session_state = self.session_state_repo.get(str(user_id)) if user_id else {}
        params_for_interpret = dict(explicit_params) if explicit_params else {}
        if user_id and session_state:
            if not params_for_interpret.get("region_filter") and session_state.get("last_region"):
                params_for_interpret["region_filter"] = session_state["last_region"]
            if not params_for_interpret.get("period_days") and session_state.get("last_period_days"):
                params_for_interpret["period_days"] = session_state["last_period_days"]
        
        # Новий потік обробки (якщо увімкнено)
        use_new_flow = getattr(self.settings, "use_new_agent_flow", True)
        if use_new_flow:
            try:
                return self._process_query_new_flow(
                    user_query=user_query,
                    user_id=user_id,
                    chat_id=chat_id,
                    listing_context=effective_listing_context,
                    context_summary=context_summary,
                    request_id=request_id,
                    start_time=start_time,
                    explicit_intent=explicit_intent,
                    explicit_params=params_for_interpret,
                    status_callback=status_callback,
                    stream_callback=stream_callback,
                    thinking_callback=thinking_callback,
                )
            except Exception as e:
                logger.exception("Помилка нового потоку обробки, fallback на старий: %s", e)
                # Fallback на старий потік
        
        assistant = self._get_assistant_agent()
        cache_key = (user_query.strip().lower()[:500], (context_summary or "")[:200])
        if not explicit_intent and not params_for_interpret:
            with _INTENT_CACHE_LOCK:
                entry = _INTENT_CACHE.get(cache_key)
                if entry:
                    structured_cached, expiry = entry
                    if time.time() < expiry:
                        structured = dict(structured_cached)
                    else:
                        _INTENT_CACHE.pop(cache_key, None)
                        structured = None
                else:
                    structured = None
        else:
            structured = None
        if structured is None:
            structured = assistant.interpreter.interpret_user_query(
                user_query, context_summary,
                explicit_intent=explicit_intent,
                explicit_params=params_for_interpret if params_for_interpret else None,
            )
            if not explicit_intent and not params_for_interpret:
                with _INTENT_CACHE_LOCK:
                    _INTENT_CACHE[cache_key] = (dict(structured), time.time() + _INTENT_CACHE_TTL_SECONDS)
                    if len(_INTENT_CACHE) > 500:
                        now = time.time()
                        for k in list(_INTENT_CACHE.keys()):
                            if _INTENT_CACHE[k][1] <= now:
                                _INTENT_CACHE.pop(k, None)
        if user_id and not structured.get("region_filter") and session_state.get("last_region"):
            structured["region_filter"] = session_state["last_region"]
        self._log_request_state(request_id, user_id, REQUEST_STATE_INTERPRETED, {"intent": structured.get("intent")})
        intent = structured.get("intent", "query")
        confidence = float(structured.get("confidence", 1.0))
        threshold = getattr(self.settings, "routing_confidence_threshold", 0.7)
        ask_on_low = getattr(self.settings, "routing_ask_on_low_confidence", False)

        if confidence < threshold:
            if ask_on_low:
                return "Не впевнений у точному намірі. Уточніть, будь ласка: звіт за день/тиждень, експорт даних чи інше?"
            use_multi_agent = False
        else:
            use_multi_agent = intent in ("report_last_day", "report_last_week", "export_data") and structured.get("collections")
        # Усі вільні запити (включно з analytical_query) йдуть у LangChain-агента з інструментами —
        # агент сам будує план на основі get_collection_info, execute_aggregation, get_distinct_values тощо.
        use_analytical_pipeline = False

        if use_analytical_pipeline:
            self._log_request_state(request_id, user_id, REQUEST_STATE_PLANNED)
            response_text = self._run_analytical_pipeline(request_id, user_id, structured)
            self._log_request_state(request_id, user_id, REQUEST_STATE_EXECUTED)
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            self.activity_log.log(
                request_id=request_id,
                user_id=user_id,
                agent_name="assistant",
                step=AgentActivityLogRepository.STEP_RESPONSE,
                payload={
                    "response_length": len(response_text),
                    "excel_count": 0,
                    "duration_ms": duration_ms,
                    "path": "analytical_pipeline",
                    "routing_path": structured.get("routing_path", "llm"),
                },
            )
            self._log_request_state(request_id, user_id, REQUEST_STATE_DELIVERED)
            return response_text

        if use_multi_agent:
            _status("Виконання пайплайну...")
            self._log_request_state(request_id, user_id, REQUEST_STATE_PLANNED)
            try:
                self.langchain_service._current_request_id = request_id
                self.langchain_service._current_user_id = user_id
                self.langchain_service._current_chat_id = chat_id
                try:
                    result = assistant.run(
                        user_query=user_query,
                        user_id=user_id,
                        context_summary=context_summary,
                        request_id=request_id,
                        precomputed_structured=structured,
                    )
                    self._last_excel_files = result.get("excel_files") or []
                    response_text = result.get("response_text", "Готово.")
                    self._log_request_state(request_id, user_id, REQUEST_STATE_EXECUTED)
                    if user_id:
                        self.langchain_service._save_response_to_memory(user_id, user_query, response_text, chat_id)
                    duration_ms = int((time.perf_counter() - start_time) * 1000)
                    self.activity_log.log(
                        request_id=request_id,
                        user_id=user_id,
                        agent_name="assistant",
                        step=AgentActivityLogRepository.STEP_RESPONSE,
                        payload={
                            "response_length": len(response_text),
                            "excel_count": len(self._last_excel_files),
                            "duration_ms": duration_ms,
                            "path": "multi_agent",
                            "routing_path": structured.get("routing_path", "rule_based"),
                        },
                    )
                    if user_id:
                        self.session_state_repo.update_fields(
                            str(user_id),
                            last_region=structured.get("region_filter"),
                            last_period_days=structured.get("period_days"),
                            active_collection=structured.get("collections")[0] if structured.get("collections") else None,
                        )
                    self._log_request_state(request_id, user_id, REQUEST_STATE_DELIVERED)
                    return response_text
                finally:
                    self.langchain_service._current_request_id = None
                    self.langchain_service._current_user_id = None
                    self.langchain_service._current_chat_id = None
            except Exception as e:
                self.langchain_service._current_request_id = None
                self.langchain_service._current_user_id = None
                self.langchain_service._current_chat_id = None
                logger.exception("Multi-agent pipeline failed: %s", e)
                use_multi_agent = False

        self._log_request_state(request_id, user_id, REQUEST_STATE_ROUTED_TO_AGENT)
        _status("Агент обробляє запит...")
        logger.info("[request_id=%s] Маршрутизація: виклик LangChain-агента (process_query)", request_id)
        response_text = self.langchain_service.process_query(
            user_query=user_query,
            user_id=user_id,
            chat_id=chat_id,
            listing_context=effective_listing_context,
            stream_callback=stream_callback,
            thinking_callback=thinking_callback,
            reply_to_text=reply_to_text,
            request_id=request_id,
        )
        self._log_request_state(request_id, user_id, REQUEST_STATE_EXECUTED)
        duration_ms = int((time.perf_counter() - start_time) * 1000)
        metrics = getattr(self.langchain_service, "_last_request_metrics", None) or {}
        self._log_request_state(request_id, user_id, REQUEST_STATE_DELIVERED)
        payload = {
            "response_length": len(response_text),
            "excel_count": len(self.get_last_excel_files()),
            "duration_ms": duration_ms,
            "path": "langchain",
            "routing_path": structured.get("routing_path", "llm"),
            "iterations": metrics.get("iterations"),
        }
        if metrics.get("tool_failures_count") is not None:
            payload["tool_failures_count"] = metrics["tool_failures_count"]
        if metrics.get("tool_recovery_attempted") is not None:
            payload["tool_recovery_attempted"] = metrics["tool_recovery_attempted"]
        self.activity_log.log(
            request_id=request_id,
            user_id=user_id,
            agent_name="assistant",
            step=AgentActivityLogRepository.STEP_RESPONSE,
            payload=payload,
        )
        return response_text

    def _run_analytical_pipeline(
        self, request_id: str, user_id: Optional[str], structured: Dict[str, Any]
    ) -> str:
        """Аналітичний пайплайн: single-step (AnalysisPlanner) або multi_step (AnalyticalReasoningService) → виконання → Answer Composer."""
        analysis_intent = structured.get("analysis_intent") or {}
        use_multi_step = analysis_intent.get("multi_step") is True

        if use_multi_step:
            reasoning = AnalyticalReasoningService(settings=self.settings)
            plan = reasoning.build_plan(structured.get("raw") or "", analysis_intent)
            if not plan:
                return "Не вдалося побудувати план для складного запиту. Спробуйте сформулювати простіше."
            ok, err = reasoning.validate_plan(plan)
            if not ok:
                return f"План не пройшов перевірку: {err}."
            results, err = reasoning.execute_plan(
                plan,
                filters=analysis_intent.get("filters"),
                time_range=analysis_intent.get("time_range"),
            )
            if err:
                return f"Помилка виконання плану: {err}."
            results = results or []
            count = len(results)
        else:
            query_builder = QueryBuilder()
            planner = AnalysisPlannerAgent(query_builder=query_builder)
            spec, err = planner.plan(structured)
            if err or not spec:
                return f"Не вдалося побудувати запит: {err or 'невідома помилка'}."
            try:
                result = query_builder.execute_aggregation(
                    spec["collection"],
                    spec["pipeline"],
                    limit=spec.get("limit"),
                )
            except Exception as e:
                logger.exception("Analytical pipeline execute: %s", e)
                return f"Помилка виконання запиту: {e}."
            if not result.get("success"):
                return result.get("error", "Помилка виконання запиту.")
            results = result.get("results") or []
            count = result.get("count", 0)

        if count == 0:
            return "За вказаними умовами результатів не знайдено."

        presentation = analysis_intent.get("presentation") or "list"
        title = (structured.get("raw") or "Результат")[:200]
        execution_result = {
            "data": results,
            "query_type": "analytical",
            "row_count": count,
            "has_attachment": False,
            "title": title,
            "presentation": presentation,
            "attachments": [],
        }
        composer = AnswerComposerService()
        contract = composer.compose(execution_result, client_context={"channel": "telegram"})
        return contract.get("summary", "")

    def _process_query_new_flow(
        self,
        user_query: str,
        user_id: Optional[str],
        chat_id: Optional[str],
        listing_context: Optional[Dict[str, Any]],
        context_summary: str,
        request_id: str,
        start_time: float,
        explicit_intent: Optional[str] = None,
        explicit_params: Optional[Dict[str, Any]] = None,
        status_callback: Optional[Callable[[str], None]] = None,
        stream_callback: Optional[Callable[[str], None]] = None,
        thinking_callback: Optional[Callable[[str], None]] = None,
    ) -> str:
        """
        Новий потік обробки запиту:
        1. IntentDetectorAgent - визначення наміру та формату відповіді
        2. QueryStructureAgent - визначення структурних елементів
        3. PipelineBuilderAgent - конструювання пайплайну
        4. PipelineInterpreterService - виконання пайплайну
        5. ResultValidatorService - валідація результатів
        6. AnswerComposerService - формування фінальної відповіді
        """
        def _status(msg: str) -> None:
            if status_callback:
                try:
                    status_callback(msg)
                except Exception:
                    pass

        log_ctx = f"[request_id={request_id}] [NEW_FLOW]"
        logger.info("%s === ПОЧАТОК НОВОГО ПОТОКУ ОБРОБКИ ===", log_ctx)
        logger.info("%s Запит: %s", log_ctx, user_query[:200])

        _status("Визначення наміру...")
        # 1. Визначення наміру та формату відповіді
        effective_context = context_summary or ""
        listing_analysis_text = ""
        if listing_context and isinstance(listing_context, dict):
            parts = ["Контекст оголошення (предзапит):"]
            if listing_context.get("page_url"):
                parts.append(f"Посилання: {listing_context['page_url']}")
            if listing_context.get("summary"):
                parts.append(f"Короткий опис: {listing_context['summary']}")
            effective_context = "\n".join(parts) + ("\n\n" + effective_context if effective_context else "")
            # Попередній аналіз використання (існуюче використання, геоаналіз, можливі використання зі скорингом)
            detail_source = listing_context.get("detail_source")
            detail_id = listing_context.get("detail_id")
            if not detail_source and not detail_id and listing_context.get("page_url"):
                page_url = listing_context["page_url"]
                if "prozorro.sale" in page_url:
                    detail_source = "prozorro"
                    detail_id = page_url.rstrip("/").split("/")[-1] or ""
                elif "olx" in page_url:
                    detail_source = "olx"
                    detail_id = page_url
            if detail_source and detail_id:
                try:
                    from business.services.property_usage_analysis_service import PropertyUsageAnalysisService
                    analysis_svc = PropertyUsageAnalysisService()
                    analysis = analysis_svc.get_or_create_analysis(detail_source, detail_id)
                    if not analysis.get("error"):
                        listing_analysis_text = analysis_svc.format_analysis_for_llm(analysis)
                        effective_context += "\n\n" + listing_analysis_text
                        listing_context = dict(listing_context)
                        listing_context["_usage_analysis"] = analysis
                except Exception as e:
                    logger.debug("Попередній аналіз використання об'єкта: %s", e)
        logger.info("%s Крок 1: IntentDetectorAgent - визначення наміру та формату", log_ctx)
        intent_detector = IntentDetectorAgent(self.settings)
        intent_info = intent_detector.detect_intent_and_format(user_query, effective_context)
        logger.info("%s IntentDetectorAgent результат: %s", log_ctx, intent_info)
        
        # Перевірка на out_of_scope
        if intent_info.get("response_format") == "out_of_scope":
            composer = AnswerComposerService()
            contract = composer.compose({
                "data": [],
                "query_type": "query",
                "row_count": 0,
                "has_attachment": False,
                "response_format": "out_of_scope"
            })
            return contract.get("summary", "Запит не стосується функціональності системи.")

        # geo_assessment: оцінка придатності приміщення для виду діяльності — LangChain з geocode + search_nearby_places
        if intent_info.get("response_format") == "geo_assessment":
            _status("Гео-аналіз придатності...")
            logger.info("%s Маршрутизація: geo_assessment → LangChain (route=geo_assessment)", log_ctx)
            self.langchain_service._current_request_id = request_id
            self.langchain_service._current_user_id = user_id
            self.langchain_service._current_chat_id = chat_id
            try:
                return self.langchain_service.process_query(
                    user_query=user_query,
                    user_id=user_id,
                    chat_id=chat_id,
                    listing_context=listing_context,
                    stream_callback=stream_callback,
                    thinking_callback=thinking_callback,
                    reply_to_text=None,
                    request_id=request_id,
                    route="geo_assessment",
                )
            finally:
                self.langchain_service._current_request_id = None
                self.langchain_service._current_user_id = None
                self.langchain_service._current_chat_id = None

        # Для analytical_text: спроба execute_analytics для агрегації
        analytics_result = self._try_analytics_aggregation_by_city(
            user_query, intent_info, request_id, status_callback
        )
        if analytics_result is not None:
            return analytics_result
        analytics_result = self._try_analytics_aggregation_by_region(
            user_query, intent_info, request_id, status_callback
        )
        if analytics_result is not None:
            return analytics_result

        self._log_request_state(request_id, user_id, REQUEST_STATE_INTERPRETED, {
            "intent": intent_info.get("intent"),
            "response_format": intent_info.get("response_format")
        })

        _status("Аналіз структури запиту...")
        # 2. Визначення структурних елементів запиту
        logger.info("%s Крок 2: QueryStructureAgent - визначення структурних елементів", log_ctx)
        query_structure_agent = QueryStructureAgent(self.settings)
        raw_query_structure = query_structure_agent.analyze_query_structure(
            user_query,
            intent_info,
            context=effective_context,
            listing_context=listing_context,
        )
        # CanonicalQuery — ізольований від фізичної схеми БД
        canonical_query = CanonicalQuery.from_query_structure(raw_query_structure)
        query_structure = canonical_query.to_query_structure()

        # RelativeDateResolver — детерміноване перетворення відносних періодів у конкретні дати
        if canonical_query.date_range:
            from business.services.relative_date_resolver import RelativeDateResolver
            resolved_date = RelativeDateResolver().resolve(canonical_query.date_range)
            if resolved_date:
                query_structure.setdefault("filter_metrics", {})["date"] = resolved_date
                logger.info("%s RelativeDateResolver: date=%s", log_ctx, resolved_date)

        logger.info("%s QueryStructureAgent результат: sources=%s, filters=%s, sort=%s, limit=%s",
                    log_ctx,
                    query_structure.get("sources"),
                    query_structure.get("filter_metrics"),
                    query_structure.get("sort_metrics"),
                    query_structure.get("limit"))

        _status("Побудова пайплайну...")
        # 3. Конструювання пайплайну
        logger.info("%s Крок 3: PipelineService + PipelineBuilderAgent - конструювання пайплайну", log_ctx)
        self.langchain_service._current_request_id = request_id
        self.langchain_service._current_user_id = user_id
        self.langchain_service._current_chat_id = chat_id
        
        try:
            # Формуємо контекст для пайплайну (структура полів, кеші, методи колекцій)
            pipeline_service = PipelineService(
                self.settings,
                run_tool_fn=self.langchain_service.run_tool,
            )
            pipeline_context = pipeline_service.build_pipeline_context(
                intent_info=intent_info,
                extracted_data=query_structure,
                user_query=user_query,
            )
            pipeline_builder = PipelineBuilderAgent(
                self.settings,
                run_tool_fn=self.langchain_service.run_tool
            )
            pipeline_result = pipeline_builder.build_pipeline(
                query_structure,
                user_query,
                intent_info,
                pipeline_context=pipeline_context,
            )
            
            pipeline = pipeline_result.get("pipeline", {})
            logger.info("%s PipelineBuilderAgent результат: steps_count=%s, pipeline_id=%s, from_cache=%s",
                        log_ctx,
                        len(pipeline.get("steps", [])),
                        pipeline_result.get("pipeline_id"),
                        pipeline_result.get("from_cache", False))
            logger.debug("%s Пайплайн: %s", log_ctx, pipeline)
            self._log_request_state(request_id, user_id, REQUEST_STATE_PLANNED)

            _status("Виконання запиту до даних...")
            # 4. Виконання пайплайну (з рекурсивною обробкою)
            logger.info("%s Крок 4: PipelineInterpreterService - виконання пайплайну", log_ctx)
            pipeline_interpreter = PipelineInterpreterService(
                run_tool_fn=self.langchain_service.run_tool,
                settings=self.settings,
            )
            result_validator = ResultValidatorService(self.settings)
            answer_composer = AnswerComposerService()
            
            max_retries = 3
            retry_count = 0
            final_results = None
            validation_result = None

            last_execution_result = None
            sources = query_structure.get("sources", [])
            logger.info("%s Джерела для обробки: %s", log_ctx, sources)
            
            # Якщо є кілька джерел, обробляємо кожне окремо та об'єднуємо результати
            all_results = []
            if len(sources) > 1:
                logger.info("%s Обробляємо %s джерел окремо", log_ctx, len(sources))
                for idx, source in enumerate(sources):
                    _status("Обробка джерела " + str(idx + 1) + " з " + str(len(sources)) + "...")
                    logger.info("%s Обробка джерела: %s", log_ctx, source)
                    # Підготуємо параметри для пайплайну
                    parameters = self._build_pipeline_parameters(query_structure, source)
                    execution_result = pipeline_interpreter.execute_pipeline(
                        pipeline,
                        initial_collection=source,
                        parameters=parameters
                    )
                    if execution_result.get("success"):
                        source_results = execution_result.get("results", [])
                        # Додаємо мітку джерела до кожного результату
                        for result in source_results:
                            result["_source"] = source
                        all_results.extend(source_results)
                        logger.info("%s Отримано %s результатів з %s", log_ctx, len(source_results), source)
                
                # Об'єднуємо результати та сортуємо заново (якщо потрібно)
                sort_metrics = query_structure.get("sort_metrics", [])
                if sort_metrics and all_results:
                    # Сортуємо об'єднані результати
                    sort_field = sort_metrics[0].get("field") if sort_metrics else None
                    sort_order = sort_metrics[0].get("order", "asc") if sort_metrics else "asc"
                    if sort_field:
                        reverse = sort_order == "desc"
                        try:
                            all_results.sort(key=lambda x: self._get_sort_value(x, sort_field), reverse=reverse)
                        except Exception as e:
                            logger.warning("%s Помилка сортування: %s", log_ctx, e)
                
                # Застосовуємо limit до об'єднаних результатів
                limit = query_structure.get("limit")
                if limit:
                    all_results = all_results[:limit]
                
                execution_result = {
                    "success": True,
                    "results": all_results,
                    "count": len(all_results)
                }
            else:
                # Одне джерело - використовуємо стару логіку
                initial_collection = sources[0] if sources else None
                logger.info("%s Початкова колекція для пайплайну: %s", log_ctx, initial_collection)
                
                while retry_count < max_retries:
                    logger.info("%s Виконання пайплайну (спроба %s/%s)", log_ctx, retry_count + 1, max_retries)
                    # Виконуємо пайплайн
                    # Підготуємо параметри для пайплайну
                    parameters = self._build_pipeline_parameters(query_structure, initial_collection)
                    execution_result = pipeline_interpreter.execute_pipeline(
                        pipeline,
                        initial_collection=initial_collection,
                        parameters=parameters
                    )
                
                    last_execution_result = execution_result
                    logger.info("%s Результат виконання пайплайну: success=%s, count=%s, error=%s",
                                log_ctx,
                                execution_result.get("success"),
                                execution_result.get("count", 0),
                                execution_result.get("error"))
                    
                    if not execution_result.get("success"):
                        # Помилка виконання - повертаємо помилку з діагностичною інформацією
                        error_msg = f"Помилка виконання пайплайну: {execution_result.get('error', 'Невідома помилка')}"
                        diag_info = execution_result.get("diagnostic_info")
                        if diag_info:
                            if diag_info.get("total_documents_in_collection"):
                                error_msg += f"\n\nЗагальна кількість документів у колекції: {diag_info['total_documents_in_collection']}."
                            if diag_info.get("addresses_available") is False:
                                error_msg += "\n⚠️ Поле адрес відсутнє в даних. Можливо, дані ще не оброблені."
                        logger.error("%s Помилка виконання пайплайну: %s", log_ctx, error_msg)
                        return error_msg
                    
                    results = execution_result.get("results", [])
                    logger.info("%s Отримано результатів: %s", log_ctx, len(results))
                
                    _status("Валідація результатів...")
                    # Валідуємо результати
                    logger.info("%s Крок 5: ResultValidatorService - валідація результатів", log_ctx)
                    validation_result = result_validator.validate_results(
                        results,
                        query_structure,
                        user_query,
                        pipeline_result=execution_result
                    )
                    logger.info("%s Результат валідації: valid=%s, should_retry=%s, issues=%s",
                                log_ctx,
                                validation_result.get("valid"),
                                validation_result.get("should_retry"),
                                len(validation_result.get("issues", [])))
                    
                    if not validation_result.get("should_retry"):
                        final_results = results
                        break
                    
                    retry_count += 1
                    logger.warning(
                        "[request_id=%s] Валідація не пройдена, повторна спроба %s/%s: %s",
                        request_id,
                        retry_count,
                        max_retries,
                        validation_result.get("retry_reason")
                    )
                    
                    # Можна тут адаптувати пайплайн на основі помилок валідації
                    # Поки що просто повторюємо
            
            # Якщо обробляли кілька джерел, валідуємо об'єднані результати
            if len(sources) > 1:
                logger.info("%s Валідація об'єднаних результатів з %s джерел", log_ctx, len(sources))
                validation_result = result_validator.validate_results(
                    all_results,
                    query_structure,
                    user_query,
                    pipeline_result=execution_result
                )
                final_results = all_results
                last_execution_result = execution_result
            
            if final_results is None:
                # Всі спроби не вдалися
                error_msg = f"Не вдалося отримати валідні результати після {max_retries} спроб. {validation_result.get('retry_reason', '') if validation_result else 'Невідома помилка'}"
                logger.error("%s %s", log_ctx, error_msg)
                return error_msg
            
            logger.info("%s Фінальні результати: count=%s", log_ctx, len(final_results))
            self._log_request_state(request_id, user_id, REQUEST_STATE_EXECUTED)
            
            # 5. Формування фінальної відповіді
            logger.info("%s Крок 6: AnswerComposerService - формування фінальної відповіді", log_ctx)
            response_format = intent_info.get("response_format", "text_answer")
            
            # Визначаємо, чи потрібен файл
            has_attachment = response_format == "data_export" or len(final_results) > 20
            
            # Якщо потрібен файл, експортуємо у уніфікованому форматі (дата, джерело, адреса, ціна, посилання)
            if has_attachment and response_format == "data_export":
                try:
                    if final_results:
                        sources = query_structure.get("sources", [])
                        source_collection = "unified_listings" if "unified_listings" in sources else (sources[0] if sources else "unified_listings")
                        filename_prefix = self._query_to_filename_prefix(user_query, query_structure)
                        export_result = self.langchain_service.export_results_to_excel_unified(
                            results=final_results,
                            source_collection=source_collection,
                            filename_prefix=filename_prefix,
                        )
                        if export_result.get("success") and export_result.get("file_base64"):
                            self._last_excel_files = [{
                                "file_base64": export_result.get("file_base64"),
                                "filename": export_result.get("filename", "export.xlsx"),
                                "rows_count": export_result.get("rows_count", len(final_results)),
                                "columns_count": export_result.get("columns_count", len(final_results[0].keys()) if final_results else 0)
                            }]
                except Exception as e:
                    logger.exception("Помилка експорту в файл: %s", e)
            
            # Збираємо діагностичну інформацію для інформативнішої відповіді
            diagnostic_info = {}
            filter_info = {}
            
            # Інформація про фільтри
            filter_metrics = query_structure.get("filter_metrics", {})
            if filter_metrics:
                if "region" in filter_metrics:
                    filter_info["region"] = filter_metrics["region"]
                if "city" in filter_metrics:
                    filter_info["city"] = filter_metrics["city"]
                if "date" in filter_metrics:
                    date_val = filter_metrics["date"]
                    if isinstance(date_val, dict):
                        if "period" in date_val:
                            filter_info["date_range"] = date_val["period"]
                        elif "gte" in date_val:
                            filter_info["date_range"] = "вказаний період"
            
            # Отримуємо діагностичну інформацію з результату виконання пайплайну
            if len(final_results) == 0:
                logger.info("%s Немає результатів, збираємо діагностичну інформацію", log_ctx)
                # Отримуємо діагностичну інформацію з останнього виконання пайплайну
                if last_execution_result and last_execution_result.get("diagnostic_info"):
                    # Якщо diagnostic_info - це словник з ключами-джерелами, об'єднуємо
                    exec_diag = last_execution_result.get("diagnostic_info", {})
                    logger.info("%s Діагностична інформація з execution_result: %s", log_ctx, exec_diag)
                    if isinstance(exec_diag, dict):
                        # Перевіряємо, чи це словник з джерелами або загальна інформація
                        if "total_documents_in_collection" in exec_diag or "addresses_available" in exec_diag:
                            # Загальна інформація
                            diagnostic_info.update(exec_diag)
                        else:
                            # Інформація по джерелам
                            diagnostic_info.update(exec_diag)
                
                # Якщо діагностичної інформації недостатньо, збираємо додаткову
                if not diagnostic_info.get("total_documents_in_collection"):
                    try:
                        sources = query_structure.get("sources", [])
                        for source in sources:
                            # Отримуємо загальну кількість документів
                            collection_info = self.langchain_service.run_tool(
                                "get_collection_info",
                                {"collection_name": source}
                            )
                            if collection_info.get("success"):
                                coll_data = collection_info.get("collection", {})
                                total_docs = coll_data.get("total_documents", 0)
                                if not diagnostic_info.get("total_documents_in_collection"):
                                    diagnostic_info["total_documents_in_collection"] = total_docs
                                
                                # Перевіряємо наявність полів адрес
                                schema = coll_data.get("schema", {})
                                if schema:
                                    has_addresses = False
                                    if source == "unified_listings":
                                        has_addresses = "addresses" in str(schema)
                                    elif source == "prozorro_auctions":
                                        has_addresses = "auction_data.address_refs" in str(schema)
                                    elif source == "olx_listings":
                                        has_addresses = "detail.address_refs" in str(schema)
                                    
                                    if not has_addresses and not diagnostic_info.get("addresses_available"):
                                        diagnostic_info["addresses_available"] = False
                                        diagnostic_info["suggestions"] = "Поле адрес відсутнє в даних. Можливо, дані ще не оброблені."
                    except Exception as e:
                        logger.debug("Не вдалося зібрати діагностичну інформацію: %s", e)
            
            _status("Формую відповідь...")
            # Формуємо відповідь
            logger.info("%s Формуємо відповідь: results_count=%s, response_format=%s, has_attachment=%s",
                        log_ctx, len(final_results), response_format, has_attachment)
            execution_result_for_composer = {
                "data": final_results,
                "query_type": "analytical" if response_format == "analytical_text" else "query",
                "row_count": len(final_results),
                "has_attachment": has_attachment,
                "title": user_query[:100],
                "presentation": "list" if len(final_results) <= 10 else "table",
                "response_format": response_format,
                "filter_info": filter_info,
                "sources": query_structure.get("sources", []),
                "diagnostic_info": diagnostic_info,
                "intent_info": intent_info,
                "user_query": user_query,
            }
            logger.debug("%s Дані для AnswerComposer: %s", log_ctx, {
                "row_count": execution_result_for_composer["row_count"],
                "response_format": execution_result_for_composer["response_format"],
                "has_diagnostic_info": bool(diagnostic_info),
                "has_filter_info": bool(filter_info)
            })
            
            contract = answer_composer.compose(
                execution_result_for_composer,
                client_context={"channel": "telegram"}
            )
            
            draft_summary = contract.get("summary", "Результати готові.")
            logger.info("%s AnswerComposer повернув чернетку довжиною %s символів", log_ctx, len(draft_summary))

            # Фінальний крок: LLM аналізує запит з контекстом результатів і формує відповідь
            refinement_service = FinalAnswerRefinementService(self.settings)
            response_text = refinement_service.refine(
                user_query=user_query,
                intent_info=intent_info,
                execution_result=execution_result_for_composer,
                draft_summary=draft_summary,
            )
            logger.info("%s FinalAnswerRefinement повернув відповідь довжиною %s символів", log_ctx, len(response_text))
            
            # Зберігаємо в пам'ять
            if user_id:
                self.langchain_service._save_response_to_memory(user_id, user_query, response_text, chat_id)
            
            duration_ms = int((time.perf_counter() - start_time) * 1000)
            self.activity_log.log(
                request_id=request_id,
                user_id=user_id,
                agent_name="assistant",
                step=AgentActivityLogRepository.STEP_RESPONSE,
                payload={
                    "response_length": len(response_text),
                    "excel_count": len(self._last_excel_files),
                    "duration_ms": duration_ms,
                    "path": "new_flow",
                    "response_format": response_format,
                    "retry_count": retry_count
                },
            )
            
            self._log_request_state(request_id, user_id, REQUEST_STATE_DELIVERED)
            return response_text
            
        finally:
            self.langchain_service._current_request_id = None
            self.langchain_service._current_user_id = None
            self.langchain_service._current_chat_id = None

    def _query_to_filename_prefix(
        self,
        user_query: str,
        query_structure: Dict[str, Any],
    ) -> str:
        """
        Генерує релевантний префікс імені файлу українською — коротко і зрозуміло.
        Наприклад: «оголошення по містам Нововолинськ, Ковель» → Оголошення_Нововолинськ_Ковель.
        """
        filter_metrics = query_structure.get("filter_metrics", {})
        city_val = filter_metrics.get("city")
        region_val = filter_metrics.get("region")
        if city_val:
            cities = (city_val.get("$in") or city_val.get("in")) if isinstance(city_val, dict) else (
                [city_val] if isinstance(city_val, str) else city_val if isinstance(city_val, list) else []
            )
            if cities:
                names = [c.strip() for c in cities[:5] if isinstance(c, str) and c.strip()]
                if names:
                    return "Оголошення_" + "_".join(names)[:55]
        if region_val:
            regions = (region_val.get("$in") or region_val.get("in")) if isinstance(region_val, dict) else (
                [region_val] if isinstance(region_val, str) else region_val if isinstance(region_val, list) else []
            )
            if regions:
                names = [r.strip() for r in regions[:3] if isinstance(r, str) and r.strip()]
                if names:
                    return "Оголошення_область_" + "_".join(names)[:55]
        # Fallback: з запиту — перші 50 символів, санітизовані
        q = (user_query or "").strip()[:60]
        if not q:
            return "Експорт"
        safe = "".join(x if x.isalnum() or x in "._- " else "_" for x in q)
        parts = [s for s in safe.split() if s and len(s) > 1][:6]
        return "_".join(parts)[:50] if parts else "Експорт"

    def _build_pipeline_parameters(
        self,
        query_structure: Dict[str, Any],
        collection: Optional[str] = None
    ) -> Dict[str, Any]:
        """
        Будує словник параметрів для підстановки в параметризований пайплайн.
        """
        parameters = {}
        
        # Джерело даних
        if collection:
            parameters["collection"] = collection
        
        # Фільтри - перевіряємо різні формати
        filter_metrics = query_structure.get("filter_metrics", {})
        
        # Обробляємо різні формати фільтрів
        for key in ["region", "city"]:
            if key in filter_metrics:
                value = filter_metrics[key]
                # Якщо значення - це рядок з параметром, витягуємо його
                if isinstance(value, str) and (value.startswith("$") or value.startswith("{{")):
                    # Це вже параметр, не підставляємо
                    continue
                parameters[key] = value
        
        # Також перевіряємо альтернативні формати (addresses.region тощо)
        for key, value in filter_metrics.items():
            if "region" in key.lower() and "region" not in parameters:
                if isinstance(value, str) and not (value.startswith("$") or value.startswith("{{")):
                    parameters["region"] = value
            if "city" in key.lower() and "city" not in parameters:
                if isinstance(value, str) and not (value.startswith("$") or value.startswith("{{")):
                    parameters["city"] = value
        # Date: якщо ще period — резолвимо через RelativeDateResolver
        if "date" in filter_metrics:
            date_filter = filter_metrics["date"]
            if isinstance(date_filter, dict):
                if "gte" in date_filter and "lte" in date_filter:
                    # Вже конкретні дати — передаємо в conditions, не в parameters
                    pass
                elif "period" in date_filter or (date_filter.get("type") == "relative" and "value" in date_filter):
                    from business.services.relative_date_resolver import RelativeDateResolver
                    resolved = RelativeDateResolver().resolve(date_filter)
                    if resolved:
                        filter_metrics["date"] = resolved
        
        # Сортування
        sort_metrics = query_structure.get("sort_metrics", [])
        if sort_metrics:
            first_sort = sort_metrics[0]
            parameters["sort_field"] = first_sort.get("field", "price")
            parameters["sort_order"] = first_sort.get("order", "desc")
        
        # Обмеження
        limit = query_structure.get("limit")
        if limit:
            parameters["limit"] = limit
        
        return parameters
    
    def _get_sort_value(self, result: Dict[str, Any], field: str) -> Any:
        """Витягує значення для сортування з результату."""
        # Спробуємо знайти значення в різних місцях
        if field in result:
            return result[field]
        
        # Для ціни
        if field == "price":
            if "price_uah" in result:
                return result["price_uah"] or 0
            if "price" in result:
                return result["price"]
            if "auction_data" in result and "value" in result["auction_data"]:
                return result["auction_data"]["value"].get("amount", 0)
            if "detail" in result and "price" in result["detail"]:
                return result["detail"]["price"]
            if "search_data" in result and "price" in result["search_data"]:
                return result["search_data"]["price"]
        
        # Для кількості учасників
        if field in ["bidders_count", "bids_count"]:
            return result.get(field, 0)
        
        return 0
    
    def _belongs_to_source(self, result: Dict[str, Any], source: str) -> bool:
        """Перевіряє, чи результат належить до джерела."""
        if source == "unified_listings":
            return "source" in result and "source_id" in result
        if source == "prozorro_auctions":
            return "auction_id" in result or "auction_data" in result
        if source == "olx_listings":
            return "url" in result or "detail" in result or "search_data" in result
        return True
    
    def get_last_excel_files(self) -> List[Dict[str, Any]]:
        """
        Повертає список Excel-файлів, згенерованих при останньому process_query
        (для відправки в Telegram). Якщо останній запит обробляв LangChain-агент,
        використовує _extract_excel_files_from_history з нього.
        """
        if self._last_excel_files:
            return self._last_excel_files
        if hasattr(self.langchain_service, "_extract_excel_files_from_history"):
            return self.langchain_service._extract_excel_files_from_history()
        return []
