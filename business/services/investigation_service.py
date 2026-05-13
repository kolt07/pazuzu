# -*- coding: utf-8 -*-
"""
InvestigationService — оркестратор досліджувань Flx.

Ключові обов'язки:
- start: створити сесію + згенерувати початковий план + поставити Celery-таску.
- run_one_step: один крок циклу (Plan → Step → tool/ask_user/finish → Notes).
- submit_user_answer: записати відповідь користувача та продовжити.
- get_state / cancel.
- Виконати фінальний звіт (Reporter → HTML через Jinja → ArtifactService) і зробити reflection.
- Тримати tools registry з allow-list і JSON-схемами; кожен tool — це Python-callable, що
  безпечно перевіряється до виклику. Жодного raw SQL/Mongo з боку LLM.
"""

import logging
import re
import uuid
import base64
import hashlib
from datetime import datetime, timezone, timedelta
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional, Tuple

from config.settings import Settings

from business.agents.investigator import (
    InvestigatorContrarianAgent,
    InvestigatorCriticAgent,
    InvestigatorDataExtractorAgent,
    InvestigatorGeoAnalystAgent,
    InvestigatorPlannerAgent,
    InvestigatorStepAgent,
    InvestigatorTrendAgent,
    InvestigatorValuationAgent,
    InvestigatorReflectionAgent,
    InvestigatorReportAgent,
)
from business.domain.flx_research_models import ResearchBudget, ResearchTask
from business.services.evidence_graph_service import EvidenceGraphService
from business.services.flx_memory_adapters_service import (
    PostgresResearchAdapter,
    RedisSessionAdapter,
    VectorKnowledgeAdapter,
)
from business.services.orchestration_backend_service import get_orchestration_backend
from business.services.priority_scoring_service import PriorityScoringService
from business.services.research_coverage_service import DEFAULT_ASPECTS, ResearchCoverageService
from business.domain.flx_strategy_models import StrategyRunResult
from business.services.artifact_service import ArtifactService
from business.services.static_map_service import StaticMapService
from business.services.strategy_evaluation_service import StrategyEvaluationService
from business.services.strategy_registry_service import StrategyRegistryService, strategy_signature_from_candidate
from business.services.web_search_service import WebSearchService
from telegram_mini_app.send_via_bot import send_file_via_telegram

from data.repositories.flx_lessons_repository import FlxLessonsRepository
from data.repositories.investigation_event_repository import InvestigationEventRepository
from data.repositories.investigation_notes_repository import InvestigationNotesRepository
from data.repositories.investigation_session_repository import InvestigationSessionRepository

logger = logging.getLogger(__name__)

# Максимальна кількість символів для serialized observation з tool result, що йде у нотатку
TOOL_OBSERVATION_TRUNCATE = 4000
LOG_TEXT_PREVIEW_LIMIT = 500
SOURCE_WAIT_RECHECK_SECONDS = 60


class InvestigationService:
    """Координує життєвий цикл дослідження Flx."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self._sessions = InvestigationSessionRepository()
        self._notes = InvestigationNotesRepository()
        self._lessons = FlxLessonsRepository()
        self._events = InvestigationEventRepository()
        self._artifacts = ArtifactService()
        self._planner = InvestigatorPlannerAgent(self.settings)
        self._step_agent = InvestigatorStepAgent(self.settings)
        self._critic = InvestigatorCriticAgent(self.settings)
        self._contrarian = InvestigatorContrarianAgent(self.settings)
        self._extractor = InvestigatorDataExtractorAgent(self.settings)
        self._geo = InvestigatorGeoAnalystAgent(self.settings)
        self._trend = InvestigatorTrendAgent(self.settings)
        self._valuation = InvestigatorValuationAgent(self.settings)
        self._reflection = InvestigatorReflectionAgent(self.settings)
        self._reporter = InvestigatorReportAgent(self.settings)
        self._strategy_eval = StrategyEvaluationService()
        self._strategy_registry = StrategyRegistryService(self.settings)
        self._priority = PriorityScoringService()
        self._coverage = ResearchCoverageService()
        self._evidence_graph = EvidenceGraphService()
        self._redis_memory = RedisSessionAdapter()
        self._pg_memory = PostgresResearchAdapter()
        self._vector_memory = VectorKnowledgeAdapter()
        self._orchestration = get_orchestration_backend(self.settings)
        self._tools_registry: Dict[str, Dict[str, Any]] = self._build_tools_registry()

    # ----------------------------- public API -----------------------------

    def start(
        self,
        *,
        user_id: str,
        chat_id: Optional[str],
        query: str,
        request_id: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Створює сесію і ставить Celery-таску першого кроку."""
        session_id = str(uuid.uuid4())
        rid = request_id or str(uuid.uuid4())
        self._sessions.create_session(
            session_id=session_id,
            user_id=str(user_id),
            chat_id=chat_id,
            query=query,
            request_id=rid,
        )
        self._events.push(
            session_id=session_id,
            event_type="status",
            payload={"message": "Дослідження запущено. Будую початковий план...", "phase": "created"},
        )
        # Початковий план будуємо синхронно тут, щоб користувач одразу бачив структуру.
        try:
            self._sessions.set_state(session_id, "planning")
            relevant_lessons = self._fetch_relevant_lessons(query, user_id=user_id, top_k=5)
            registry_hints = StrategyRegistryService.format_hints_for_prompt(
                self._strategy_registry.retrieve_hints_for_planner(query, top_k=5)
            )
            plan = self._planner.build_plan(
                user_query=query,
                allowed_tools=self.allowed_tool_names(),
                relevant_lessons=relevant_lessons,
                registry_hints_block=registry_hints,
                strategy_engine_enabled=bool(getattr(self.settings, "flx_strategy_engine_enabled", False)),
            )
            self._sessions.set_plan(session_id, plan)
            research_state = self._init_research_state(plan)
            self._sessions.update_fields(session_id, research_state)
            if (
                getattr(self.settings, "flx_strategy_engine_enabled", False)
                and isinstance((plan or {}).get("strategy_candidates"), list)
                and len((plan or {}).get("strategy_candidates") or []) > 0
            ):
                self._sessions.update_fields(
                    session_id,
                    {
                        "strategy_engine_active": True,
                        "current_strategy_index": 0,
                        "strategy_run_results": [],
                    },
                )
            self._note_append(
                session_id=session_id,
                kind="decision",
                text="План дослідження побудовано: " + (plan.get("objective") or "")[:300],
                step_index=0,
            )
            self._events.push(
                session_id=session_id,
                event_type="status",
                payload={"message": "План готовий, починаю крок 1.", "phase": "plan_ready", "plan": plan},
            )
            self._sessions.set_state(session_id, "running")
        except Exception as e:
            logger.exception("Failed to build initial plan for session %s", session_id)
            self._sessions.fail(session_id, f"plan_build_failed: {e}")
            self._events.push(session_id=session_id, event_type="error", payload={"message": str(e)})
            return {"session_id": session_id, "ok": False, "error": str(e)}

        # Ставимо в чергу перший крок; якщо Celery вимкнена — виконуємо синхронно до паузи.
        self._dispatch(session_id)
        return {"session_id": session_id, "ok": True, "plan": plan}

    def submit_user_answer(self, session_id: str, answer: str) -> Dict[str, Any]:
        """Записує відповідь користувача та ре-enqueue таски."""
        sess = self._sessions.get_by_session_id(session_id)
        if not sess:
            return {"ok": False, "error": "session_not_found"}
        if sess.get("state") != "awaiting_user":
            return {"ok": False, "error": f"unexpected_state:{sess.get('state')}"}
        self._sessions.submit_user_answer(session_id, answer or "")
        self._note_append(
            session_id=session_id,
            kind="user_answer",
            text=(answer or "")[:1000],
            step_index=int(sess.get("step_index") or 0),
        )
        self._events.push(session_id=session_id, event_type="answer", payload={"answer": (answer or "")[:1000]})
        self._dispatch(session_id)
        return {"ok": True}

    def get_state(self, session_id: str) -> Dict[str, Any]:
        sess = self._sessions.get_by_session_id(session_id)
        if not sess:
            return {"ok": False, "error": "session_not_found"}
        return {"ok": True, "session": sess}

    def cancel(self, session_id: str) -> Dict[str, Any]:
        ok = self._sessions.cancel(session_id)
        if ok:
            self._events.push(session_id=session_id, event_type="status", payload={"message": "Дослідження скасовано користувачем."})
        return {"ok": bool(ok)}

    def list_for_user(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        return self._sessions.list_for_user(user_id, limit=limit)

    def fetch_events(self, session_id: str, since_seq: int = 0, limit: int = 200) -> List[Dict[str, Any]]:
        return self._events.fetch_since(session_id, since_seq=since_seq, limit=limit)

    def fetch_timeline(self, session_id: str, *, limit: int = 300) -> List[Dict[str, Any]]:
        """Події SSE + нотатки (fallback, якщо capped events зрізані) для відновлення UI."""
        lim = max(1, min(int(limit or 300), 500))
        events = self._events.fetch_since(session_id, since_seq=0, limit=lim)
        note_kinds = [
            "thought",
            "observation",
            "hypothesis",
            "decision",
            "question",
            "user_answer",
            "tool_call",
            "tool_error",
        ]
        notes = self._notes.list_for_session(session_id, limit=lim, kinds=note_kinds)
        rows: List[Tuple[datetime, int, Dict[str, Any]]] = []

        def _coerce_dt(val: Any) -> datetime:
            if isinstance(val, datetime):
                return val if val.tzinfo else val.replace(tzinfo=timezone.utc)
            if isinstance(val, str):
                try:
                    s = val.replace("Z", "+00:00")
                    d = datetime.fromisoformat(s)
                    return d if d.tzinfo else d.replace(tzinfo=timezone.utc)
                except Exception:
                    pass
            return datetime.now(timezone.utc)

        for ev in events:
            ts = _coerce_dt(ev.get("ts"))
            seq = int(ev.get("seq") or 0)
            rows.append(
                (
                    ts,
                    seq,
                    {
                        "source": "event",
                        "type": ev.get("type"),
                        "seq": seq,
                        "payload": ev.get("payload") or {},
                        "ts": ev.get("ts"),
                    },
                )
            )

        for n in notes:
            kind = str(n.get("kind") or "")
            ts = _coerce_dt(n.get("created_at"))
            nseq = int(n.get("seq") or 0)
            if kind == "thought":
                ev_type = "thinking"
                payload: Dict[str, Any] = {"content": (n.get("text") or "")[:8000]}
            elif kind == "tool_call":
                ev_type = "tool_call"
                tcs = n.get("tool_call_summary") or {}
                payload = {
                    "name": (tcs.get("name") if isinstance(tcs, dict) else "") or "",
                    "args_preview": (tcs.get("args_preview") if isinstance(tcs, dict) else "") or "",
                    "text": (n.get("text") or "")[:2000],
                }
            elif kind == "tool_error":
                ev_type = "status"
                payload = {"message": (n.get("text") or "")[:2000]}
            else:
                ev_type = "note"
                payload = {"kind": kind, "text": (n.get("text") or "")[:8000]}
            rows.append(
                (
                    ts,
                    1_000_000 + nseq,
                    {
                        "source": "note",
                        "type": ev_type,
                        "seq": None,
                        "payload": payload,
                        "ts": n.get("created_at"),
                        "note_seq": nseq,
                        "note_kind": kind,
                    },
                )
            )

        rows.sort(key=lambda x: (x[0], x[1]))
        return [r[2] for r in rows[:lim]]

    def skip_source_wait(self, session_id: str) -> Dict[str, Any]:
        """Примусово вийти з очікування source_load і продовжити дослідження."""
        sess = self._sessions.get_by_session_id(session_id)
        if not sess:
            return {"ok": False, "error": "session_not_found"}
        if str(sess.get("state") or "") != "awaiting_sources":
            return {"ok": False, "error": "not_awaiting_sources"}
        self._sessions.update_fields(
            session_id,
            {"state": "running", "pending_source_wait": None},
        )
        self._events.push(
            session_id=session_id,
            event_type="status",
            payload={
                "phase": "source_wait_skipped",
                "message": "Очікування оновлення джерел пропущено. Продовжую з наявними даними.",
            },
        )
        self._dispatch(session_id)
        return {"ok": True}

    # --------------------------- core step loop ---------------------------

    def run_one_step(self, session_id: str) -> Dict[str, Any]:
        """Один прохід циклу. Повертає dict зі statusом для celery-логів."""
        sess = self._sessions.get_by_session_id(session_id)
        if not sess:
            return {"ok": False, "error": "session_not_found"}
        request_id = str(sess.get("request_id") or "")
        state = sess.get("state")
        logger.info(
            "[flx-think] step.enter session=%s request_id=%s state=%s step_index=%s iteration=%s",
            session_id,
            request_id or "—",
            state,
            sess.get("step_index"),
            sess.get("iteration"),
        )
        if state in ("done", "failed", "cancelled"):
            return {"ok": True, "state": state, "note": "terminal"}
        if state == "awaiting_sources":
            return self._check_source_wait(session_id, sess)
        if state == "awaiting_user":
            # Якщо є pending_answer — споживемо і поточний step продовжимо
            if not sess.get("pending_answer"):
                logger.info(
                    "[flx-think] step.waiting_user session=%s request_id=%s expected=user_answer pending_question=%s",
                    session_id,
                    request_id or "—",
                    self._preview_text((sess.get("pending_question") or {}).get("question")),
                )
                return {"ok": True, "state": state, "note": "awaiting_user"}

        plan = sess.get("plan") or {"steps": []}
        steps = plan.get("steps") or []
        step_index = int(sess.get("step_index") or 0)
        if self._should_stop_research(sess):
            self._note_append(
                session_id=session_id,
                kind="decision",
                text="Досягнуто критерій завершення research coverage/budget/time. Переходжу до фіналізації.",
                step_index=step_index,
            )
            return self._finalize(self._sessions.get_by_session_id(session_id) or sess)
        dynamic_step = self._select_next_task_step(sess)
        if dynamic_step:
            # КРИТИЧНО: НЕ перетирати step'и плануваьника (без `task_id`) dynamic
            # research_task'ом. Інакше step[0] з goal'ом «Зібрати оголошення в
            # Турійську…» підмінюється абстрактним «Дослідити аспект: demographics»,
            # і LLM втрачає всю локацію (це і був корінь хронічних галюцинацій).
            # research_task'и додаємо ПІСЛЯ того, як plan-steps вичерпано.
            if step_index < len(steps):
                cur = steps[step_index] if step_index < len(steps) else {}
                if not cur.get("task_id"):
                    # Це чистий plan-step — виконуємо його як є, dynamic_step
                    # використаємо коли план дійде до кінця.
                    pass
                else:
                    steps[step_index] = dynamic_step
                    plan["steps"] = steps
                    self._sessions.update_fields(session_id, {"plan": plan})
            else:
                steps.append(dynamic_step)
                plan["steps"] = steps
                self._sessions.update_fields(session_id, {"plan": plan})
        if (
            getattr(self.settings, "flx_strategy_engine_enabled", False)
            and sess.get("strategy_engine_active")
            and steps
            and step_index >= len(steps)
        ):
            phase = self._flx_on_strategy_steps_exhausted(session_id, sess)
            if phase == "done":
                sess2 = self._sessions.get_by_session_id(session_id) or {}
                return self._finalize(sess2)
            if phase == "advanced":
                sess = self._sessions.get_by_session_id(session_id) or sess
                plan = sess.get("plan") or {"steps": []}
                steps = plan.get("steps") or []
                step_index = int(sess.get("step_index") or 0)
        if step_index >= len(steps):
            return self._finalize(sess)

        iteration = self._sessions.increment_iteration(session_id)
        self._run_unknown_discovery(session_id, iteration)
        max_iters = int(getattr(self.settings, "llm_investigator_max_iterations", 40))
        if iteration > max_iters:
            self._note_append(
                session_id=session_id,
                kind="decision",
                text=f"Досягнуто ліміт ітерацій ({max_iters}). Переходжу до фінального звіту.",
                step_index=step_index,
            )
            return self._finalize(self._sessions.get_by_session_id(session_id) or sess)

        current_step = steps[step_index]
        notes_summary = self._notes.summary(session_id, top_k=12)
        pending_answer = self._sessions.consume_pending_answer(session_id) or ""

        # Готуємо контракт інструментів для LLM (тільки allow-list)
        tools_schemas = self.tools_schemas_public()

        strat = self._current_strategy_candidate(sess)
        scope_lock = (plan or {}).get("scope_lock") or {}
        db_knowledge = self._get_db_knowledge_block()
        decision = self._step_agent.decide(
            objective=plan.get("objective") or "",
            step=current_step,
            notes_summary=notes_summary,
            tools_schemas=tools_schemas,
            pending_answer=pending_answer,
            active_strategy_id=str(strat.get("strategy_id") or "") if strat else None,
            active_strategy_name=str(strat.get("name") or "") if strat else None,
            active_strategy_hypothesis=str(strat.get("hypothesis") or "") if strat else None,
            scope_lock=scope_lock if isinstance(scope_lock, dict) else None,
            db_knowledge_block=db_knowledge,
        )
        self._mark_active_task(session_id, current_step, "in_progress")
        logger.info(
            "[flx-think] step.decision session=%s request_id=%s step=%s has_tool_call=%s has_ask_user=%s has_final=%s expected=%s",
            session_id,
            request_id or "—",
            current_step.get("step_id") or step_index + 1,
            "tool_call" in decision,
            "ask_user" in decision,
            "final_for_step" in decision,
            "tool_result" if "tool_call" in decision else ("user_answer" if "ask_user" in decision else ("step_finalize" if "final_for_step" in decision else "fallback_finalize_step")),
        )

        # Anti-hallucination: чистимо ВСІ вихідні текстові поля LLM-рішення
        # (thought, ask_user.question, ask_user.options[*], final_for_step.observation)
        # ПЕРЕД будь-якою обробкою — щоб user не бачив у UI хибних локацій.
        # Працюємо тихо: SSE-event не пушимо, лог пишемо для debug.
        hallucinated = self._sanitize_decision_against_scope(decision, scope_lock)
        thought = (decision.get("thought") or "").strip()
        if hallucinated:
            logger.info(
                "[flx-think] decision.hallucination_guard session=%s removed=%s",
                session_id,
                ", ".join(hallucinated),
            )
        if thought:
            logger.info(
                "[flx-think] step.thought session=%s request_id=%s content=%s",
                session_id,
                request_id or "—",
                self._preview_text(thought),
            )
            self._note_append(
                session_id=session_id,
                kind="thought",
                text=thought,
                step_index=int(current_step.get("step_id") or step_index + 1),
            )
            self._events.push(session_id=session_id, event_type="thinking", payload={"content": thought})

        # Виконуємо одну з трьох гілок
        if "tool_call" in decision:
            tool_call = decision["tool_call"] if isinstance(decision["tool_call"], dict) else {}
            tool_name = str(tool_call.get("name") or "")
            if tool_name == "query_builder.execute_query" and self._detect_repetitive_query_loop(session_id):
                if "flx.targeted_source_search" in self._tools_registry and not self._has_recent_tool_usage(
                    session_id, "flx.targeted_source_search", recent_notes=24
                ):
                    logger.info(
                        "[flx-think] loop_guard session=%s step=%s override=query_builder.execute_query->flx.targeted_source_search",
                        session_id,
                        current_step.get("step_id") or 0,
                    )
                    sess_now = self._sessions.get_by_session_id(session_id) or {}
                    tool_call = {
                        "name": "flx.targeted_source_search",
                        "args": {
                            "query_text": str(sess_now.get("query") or ""),
                            "source": "both",
                            "days": 7,
                        },
                    }
            return self._handle_tool_call(session_id, current_step, tool_call)
        if "ask_user" in decision:
            return self._handle_ask_user(session_id, current_step, decision["ask_user"])
        if "final_for_step" in decision:
            return self._handle_final_for_step(session_id, current_step, decision["final_for_step"], plan)

        # Невідомий формат — фолбек
        self._note_append(
            session_id=session_id,
            kind="decision",
            text="Неочікуваний формат рішення LLM. Завершую крок.",
            step_index=int(current_step.get("step_id") or step_index + 1),
        )
        return self._advance_step(session_id, plan, step_index, finish=False)

    # -------------------------- branch handlers --------------------------

    def _handle_tool_call(self, session_id: str, current_step: Dict[str, Any], tool_call: Dict[str, Any]) -> Dict[str, Any]:
        raw_name = str(tool_call.get("name") or "")
        name = self._resolve_tool_name(raw_name)
        if name and name != raw_name:
            logger.info(
                "[flx-think] tool_name.normalize raw=%s -> canonical=%s",
                raw_name,
                name,
            )
        args = tool_call.get("args") or {}
        if not isinstance(args, dict):
            args = {}
        # Нормалізація аргументів: переймаємо випадки коли LLM пише
        # `city_name` замість `city`, `min_area_sqm` замість `building_area_min`
        # тощо. Без цього тула мовчки ігнорує неіснуючі параметри і повертає
        # все підряд (бачили в логах: `city_name='Одеса'` → applied_filters
        # без city, count=50 «комерц» по всій Україні).
        if name:
            args = self._normalize_tool_args(name, args)
        sess = self._sessions.get_by_session_id(session_id) or {}
        # scope_lock — авторитетне джерело city/region/area: накладається ПОВЕРХ
        # того, що написав LLM-step-агент. Це primary guard rail від галюцинацій
        # типу «Турійськ → Київ / Ужгород / Турка». Silent rewrite (а не block),
        # бо слабкі моделі (flash) не вміють виправлятись через error-feedback —
        # вони просто пробують ще одне випадкове місто.
        scope_lock = (sess.get("plan") or {}).get("scope_lock") or {}
        scope_mismatch_info: Optional[str] = None
        if name and isinstance(scope_lock, dict) and scope_lock:
            scope_mismatch_info = self._detect_scope_mismatch(
                tool_name=name,
                args=args,
                scope_lock=scope_lock,
            )
            args = self._apply_scope_lock_to_args(
                tool_name=name,
                args=args,
                scope_lock=scope_lock,
                session_id=session_id,
                step_index=int(current_step.get("step_id") or 0),
            )
        if name == "flx.targeted_source_search":
            args = self._enrich_targeted_source_search_args(args=args, session_query=str(sess.get("query") or ""))
        logger.info(
            "[flx-think] tool_call.start session=%s step=%s tool=%s args=%s expected=tool_result",
            session_id,
            current_step.get("step_id") or 0,
            name,
            self._safe_args_preview(args),
        )
        if not name or name not in self._tools_registry:
            self._note_append(
                session_id=session_id,
                kind="tool_error",
                text=(
                    f"Інструмент '{raw_name}' не дозволено allow-list'ом "
                    f"(після нормалізації: '{name or '-'}'). "
                    "Доступні тулзи дивись у Available tools у промпті — "
                    "використовуй ТОЧНО ту назву, без префіксу `flx.` і без заміни крапок на підкреслення."
                ),
                step_index=int(current_step.get("step_id") or 0),
                tool_call_summary={"name": raw_name, "args_keys": list(args.keys())},
            )
            self._events.push(session_id=session_id, event_type="status", payload={"message": f"Інструмент {raw_name} не дозволений."})
            return {"ok": True, "state": "running", "note": "tool_not_allowed"}

        tool_def = self._tools_registry[name]
        callable_ = tool_def["callable"]
        # Інжектуємо session_id/user_id у тулзи Flx, де це потрібно — за конвенцією registry.
        extra = self._inject_context(name, args, session_id)
        merged_args = {**args, **extra}

        if name == "cadastral.get_knowledge":
            _cad_after_knowledge = (
                "cadastral.discover_in_area",
                "cadastral.search",
                "cadastral.list_parcels_in_region_polygon",
                "cadastral.list_polygon_query_page",
                "cadastral.cluster_parcels",
            )
            if self._has_recent_tool_usage(session_id, "cadastral.get_knowledge", recent_notes=5):
                if not self._has_recent_tool_usage_any(session_id, _cad_after_knowledge, recent_notes=20):
                    dup_msg = (
                        "Довідник cadastral.get_knowledge уже зібрано в попередніх нотатках. "
                        "Не повторюй get_knowledge: наступний крок — cadastral.discover_in_area або "
                        "cadastral.search / cadastral.list_parcels_in_region_polygon (+ list_polygon_query_page за потреби)."
                    )
                    self._events.push(
                        session_id=session_id,
                        event_type="status",
                        payload={"message": "Пропущено повторний cadastral.get_knowledge."},
                    )
                    self._note_append(
                        session_id=session_id,
                        kind="observation",
                        text=dup_msg,
                        step_index=int(current_step.get("step_id") or 0),
                        tool_call_summary={"name": name, "skipped_duplicate": True},
                    )
                    logger.info("[flx-think] tool_call.skipped_duplicate session=%s tool=%s", session_id, name)
                    return {"ok": True, "state": "running", "note": "get_knowledge_duplicate_skipped"}

        self._events.push(
            session_id=session_id,
            event_type="tool_call",
            payload={"name": name, "args_preview": self._safe_args_preview(merged_args)},
        )

        try:
            result = callable_(**merged_args)
        except TypeError as e:
            logger.warning(
                "[flx-think] tool_call.args_mismatch session=%s step=%s tool=%s error=%s",
                session_id,
                current_step.get("step_id") or 0,
                name,
                self._preview_text(str(e)),
            )
            err = f"args_mismatch: {e}"
            self._note_append(
                session_id=session_id,
                kind="tool_error",
                text=err,
                step_index=int(current_step.get("step_id") or 0),
                tool_call_summary={"name": name, "args_keys": list(merged_args.keys())},
            )
            return {"ok": True, "state": "running", "note": "tool_args_mismatch"}
        except Exception as e:
            logger.warning(
                "[flx-think] tool_call.failed session=%s step=%s tool=%s error=%s",
                session_id,
                current_step.get("step_id") or 0,
                name,
                self._preview_text(str(e)),
            )
            err = f"tool_failed: {e}"
            self._note_append(
                session_id=session_id,
                kind="tool_error",
                text=err,
                step_index=int(current_step.get("step_id") or 0),
                tool_call_summary={"name": name, "args_keys": list(merged_args.keys())},
            )
            return {"ok": True, "state": "running", "note": "tool_failed"}

        # Пишемо observation у нотатки. Префіксуємо помітним warning'ом, якщо
        # LLM написав у args локацію, що НЕ співпадає зі scope (silent rewrite
        # вже застосовано до args, але LLM має бачити що його коригували —
        # це найсильніший сигнал для виправлення на наступному кроці).
        observation = self._serialize_observation(result)
        if scope_mismatch_info:
            observation = (
                "⚠️ SCOPE WARNING: " + scope_mismatch_info + ". "
                "Аргументи були автоматично замінені на значення зі SCOPE. "
                "На наступному виклику бери city/region з LOCKED SCOPE дослівно.\n\n"
                + observation
            )
        logger.info(
            "[flx-think] tool_call.done session=%s step=%s tool=%s observation_preview=%s",
            session_id,
            current_step.get("step_id") or 0,
            name,
            self._preview_text(observation),
        )
        self._note_append(
            session_id=session_id,
            kind="observation",
            text=observation,
            step_index=int(current_step.get("step_id") or 0),
            tool_call_summary={"name": name, "result_size": len(observation)},
        )
        # Structured findings: фіксуємо сирий результат cadastral.search / listings.find_with_fallback
        # окремою нотаткою, щоб reporter мав ДОСТУП до повних cadastral_numbers і списку
        # оголошень без покладання на LLM-парсинг урізаного observation-рядка.
        try:
            self._record_structured_findings(
                session_id=session_id,
                tool_name=name,
                result=result,
                step_index=int(current_step.get("step_id") or 0),
            )
        except Exception as e:
            logger.warning("structured-findings recording failed for %s: %s", name, e)
        self._consume_budget(session_id, self._budget_bucket_for_tool(name))
        self._update_coverage_from_observation(session_id, current_step, observation, source=name)

        # Деякі тулзи (ask_user, report_compose) самі змінюють state — обробляємо особливо.
        if name == "flx.ask_user":
            # Сесія вже в awaiting_user
            logger.info(
                "[flx-think] tool_call.ask_user session=%s step=%s expected=user_answer",
                session_id,
                current_step.get("step_id") or 0,
            )
            return {"ok": True, "state": "awaiting_user", "note": "ask_user"}
        if name == "flx.targeted_source_search":
            mode = str((result or {}).get("mode") or "")
            task_id = str((result or {}).get("task_id") or "")
            if mode == "queued" and task_id:
                wait_payload = {
                    "task_id": task_id,
                    "started_at": datetime.now(timezone.utc).isoformat(),
                    "sources": (result or {}).get("sources") or [],
                    "days": (result or {}).get("days"),
                }
                self._sessions.update_fields(
                    session_id,
                    {
                        "state": "awaiting_sources",
                        "pending_source_wait": wait_payload,
                    },
                )
                self._events.push(
                    session_id=session_id,
                    event_type="status",
                    payload={
                        "phase": "awaiting_sources",
                        "message": "Чекаю завершення таргетного пошуку в джерелах (OLX/Prozorro).",
                        "task_id": task_id,
                    },
                )
                logger.info(
                    "[flx-think] source_wait.start session=%s step=%s task_id=%s queue=source_load expected=wait_for_fresh_data",
                    session_id,
                    current_step.get("step_id") or 0,
                    task_id,
                )
                return {"ok": True, "state": "awaiting_sources", "note": "source_wait_started"}

        return {"ok": True, "state": "running", "note": "tool_done"}

    def _handle_ask_user(self, session_id: str, current_step: Dict[str, Any], ask: Dict[str, Any]) -> Dict[str, Any]:
        payload = {
            "question": str(ask.get("question") or "")[:1000],
            "options": [str(o)[:200] for o in (ask.get("options") or []) if str(o).strip()][:6],
            "allow_freeform": bool(ask.get("allow_freeform", True)),
        }
        logger.info(
            "[flx-think] ask_user session=%s step=%s expected=user_answer question=%s options=%s allow_freeform=%s",
            session_id,
            current_step.get("step_id") or 0,
            self._preview_text(payload["question"]),
            len(payload["options"]),
            payload["allow_freeform"],
        )
        self._sessions.set_pending_question(session_id, payload)
        self._events.push(session_id=session_id, event_type="question", payload=payload)
        self._note_append(
            session_id=session_id,
            kind="question",
            text=payload["question"],
            step_index=int(current_step.get("step_id") or 0),
        )
        return {"ok": True, "state": "awaiting_user"}

    def _handle_final_for_step(
        self,
        session_id: str,
        current_step: Dict[str, Any],
        final: Dict[str, Any],
        plan: Dict[str, Any],
    ) -> Dict[str, Any]:
        observation = str(final.get("observation") or "")[:1000]
        next_action = str(final.get("next_action") or "continue").lower()

        # Anti-loop: якщо LLM повторно фіналізує той самий крок з next_action="continue"
        # і у спостереженні/думці вже звучить «критерії виконані / можна завершити крок» —
        # ескалюємо до "new_step", щоб не крутити той самий крок до max_iterations.
        # Подібний паттерн виник з логів реальної сесії, де крок з пошуком веб-фактів
        # «завершувався» по тексту, але прапорець залишався "continue".
        if next_action == "continue":
            step_id_now = int(current_step.get("step_id") or 0)
            sess_for_loop = self._sessions.get_by_session_id(session_id) or {}
            loop_state = dict(sess_for_loop.get("step_finalize_loop") or {})
            prev_step_id = int(loop_state.get("step_id") or -1)
            prev_count = int(loop_state.get("continue_count") or 0)
            if prev_step_id == step_id_now:
                new_count = prev_count + 1
            else:
                new_count = 1
            obs_lower = (observation or "").lower()
            done_markers = (
                "критерії", "критерий", "можна завершити", "завершити цей крок",
                "перехід до наступного", "наступний крок", "step complete", "criteria",
            )
            looks_done = any(m in obs_lower for m in done_markers)
            if new_count >= 2 or (looks_done and new_count >= 1):
                logger.info(
                    "[flx-think] step.auto_advance session=%s step=%s reason=continue_loop count=%s",
                    session_id,
                    step_id_now,
                    new_count,
                )
                next_action = "new_step"
                self._sessions.update_fields(
                    session_id,
                    {"step_finalize_loop": {"step_id": step_id_now, "continue_count": 0}},
                )
            else:
                self._sessions.update_fields(
                    session_id,
                    {"step_finalize_loop": {"step_id": step_id_now, "continue_count": new_count}},
                )
        else:
            self._sessions.update_fields(session_id, {"step_finalize_loop": {"step_id": 0, "continue_count": 0}})

        logger.info(
            "[flx-think] step.final session=%s step=%s next_action=%s expected=%s observation=%s",
            session_id,
            current_step.get("step_id") or 0,
            next_action,
            "report_generation" if next_action == "finish" else "next_step_decision",
            self._preview_text(observation),
        )
        if observation:
            self._note_append(
                session_id=session_id,
                kind="observation",
                text=observation,
                step_index=int(current_step.get("step_id") or 0),
            )
            self._update_coverage_from_observation(session_id, current_step, observation, source="step_final")
        self._mark_active_task(session_id, current_step, "done")
        self._run_critic_loop(session_id)
        if next_action == "finish":
            self._run_contrarian_once(session_id)
            sess = self._sessions.get_by_session_id(session_id)
            return self._finalize(sess or {})
        steps = plan.get("steps") or []
        sess = self._sessions.get_by_session_id(session_id) or {}
        step_index = int(sess.get("step_index") or 0)
        if next_action in ("new_step", "continue"):
            return self._advance_step(session_id, plan, step_index, finish=False, force_next=(next_action == "new_step"))
        return {"ok": True}

    def _advance_step(
        self,
        session_id: str,
        plan: Dict[str, Any],
        step_index: int,
        finish: bool,
        force_next: bool = False,
    ) -> Dict[str, Any]:
        steps = plan.get("steps") or []
        # max_steps_per_task залишено для майбутньої логіки budget; зараз не використовується тут.
        effective_index = step_index
        if force_next:
            effective_index = min(step_index + 1, len(steps))
            self._sessions.update_fields(session_id, {"step_index": effective_index})
        # ВАЖЛИВО: завершуємо за фактичним (можливо збільшеним) індексом, інакше після
        # переходу через останній крок у "new_step" циклі сесія залишається в "running"
        # назавжди (поки не вб'є max_iterations).
        if finish or effective_index >= len(steps):
            sess = self._sessions.get_by_session_id(session_id)
            return self._finalize(sess or {})
        return {"ok": True, "state": "running"}

    # ------------------------------ finalize ------------------------------

    def _finalize(self, sess: Dict[str, Any]) -> Dict[str, Any]:
        session_id = sess.get("session_id")
        if not session_id:
            return {"ok": False, "error": "no_session_id"}
        try:
            evidence = self._build_evidence(session_id)
            sess_graph = self._sessions.get_by_session_id(session_id) or sess
            evidence_graph = dict(sess_graph.get("evidence_graph") or {})
            if not evidence_graph:
                notes_for_graph = self._notes.list_for_session(session_id, limit=300, kinds=["observation", "user_answer", "decision"])
                evidence_graph = self._evidence_graph.from_notes(notes_for_graph)
                self._sessions.update_fields(session_id, {"evidence_graph": evidence_graph})
            notes_summary = self._notes.summary(session_id, top_k=24)
            user_query = sess.get("query") or ""

            self._events.push(session_id=session_id, event_type="status", payload={"message": "Компоную фінальний звіт..."})
            plan = sess.get("plan") or {}
            scope_lock = plan.get("scope_lock") if isinstance(plan, dict) else None

            def _report_progress(phase: str, payload: Dict[str, Any]) -> None:
                try:
                    pl = dict(payload or {})
                    msg = (pl.pop("message", None) or "").strip()
                    if not msg:
                        if phase == "report.outline_ready":
                            n = int((payload or {}).get("sections_count") or 0)
                            msg = f"Структура звіту: {n} секцій."
                        elif phase == "report.section_ready":
                            title = ((payload or {}).get("title") or "").strip() or "без назви"
                            i = int((payload or {}).get("index") or 0)
                            t = int((payload or {}).get("total") or 0)
                            msg = f"Звіт: секція {i}/{t} — {title}"[:300]
                        elif phase == "report.synthesis_done":
                            c = int((payload or {}).get("claims") or 0)
                            msg = f"Підсумок звіту: {c} тез."
                        else:
                            msg = (phase or "Звіт").replace("report.", "")
                    self._events.push(
                        session_id=session_id,
                        event_type="status",
                        payload={"phase": phase, "message": msg, **pl},
                    )
                except Exception:
                    pass

            structured = self._reporter.compose(
                user_query=user_query,
                notes_summary=notes_summary,
                evidence=evidence,
                evidence_graph=evidence_graph,
                coverage_snapshot=sess.get("research_coverage"),
                strategy_comparison=sess.get("strategy_comparison"),
                scope_lock=scope_lock if isinstance(scope_lock, dict) else None,
                progress_callback=_report_progress,
            )
            try:
                from business.services.flx_report_attachments_builder import (
                    enrich_flx_report_with_attachments,
                )

                structured = enrich_flx_report_with_attachments(
                    structured=structured,
                    evidence=evidence,
                    user_id=str(sess.get("user_id") or "") or None,
                    settings=self.settings,
                )
            except Exception as e:
                logger.warning("Report attachments enrichment failed: %s", e)
            html = self._render_report_html(structured)
            artifact = self._artifacts.register_with_token(
                user_id=str(sess.get("user_id") or ""),
                artifact_type="html_investigation_report",
                content_base64=base64.b64encode(html.encode("utf-8")).decode("ascii"),
                metadata={
                    "filename": "investigation_report.html",
                    "content_type": "text/html",
                    "session_id": session_id,
                },
                ttl_seconds=int(getattr(self.settings, "artifact_ttl_seconds", 0) or 7 * 24 * 3600),
            )
            self._sessions.set_report_artifact(
                session_id,
                artifact_id=artifact["artifact_id"],
                download_token=artifact["download_token"],
            )
            self._events.push(
                session_id=session_id,
                event_type="report",
                payload={
                    "artifact_id": artifact["artifact_id"],
                    "download_token": artifact["download_token"],
                    "title": structured.get("title"),
                },
            )
            self._notify_user_success_in_telegram(
                chat_id=sess.get("chat_id"),
                user_id=str(sess.get("user_id") or ""),
                session_id=session_id,
                artifact_id=str(artifact["artifact_id"]),
                html=html,
            )

            # Reflection — асинхронно у нотатки і у lessons; помилки не блокують done
            try:
                steps_log = self._collect_steps_log(session_id)
                lesson = self._reflection.reflect(
                    user_query=user_query,
                    outcome_summary=structured.get("executive_summary") or "",
                    steps_log=steps_log,
                )
                self._lessons.add_lesson(
                    user_id=str(sess.get("user_id") or "") or None,
                    topic_tags=lesson.get("topic_tags") or [],
                    query_pattern=lesson.get("query_pattern") or user_query,
                    what_worked=lesson.get("what_worked") or [],
                    what_failed=lesson.get("what_failed") or [],
                    recommendations=lesson.get("recommendations") or [],
                    related_session_id=session_id,
                )
            except Exception as e:
                logger.warning("Reflection/save lesson failed for %s: %s", session_id, e)

            self._events.push(session_id=session_id, event_type="done", payload={"artifact_id": artifact["artifact_id"]})
            return {"ok": True, "state": "done", "artifact_id": artifact["artifact_id"]}
        except Exception as e:
            logger.exception("Finalize failed for %s", session_id)
            self._sessions.fail(session_id, f"finalize_failed: {e}")
            self._events.push(session_id=session_id, event_type="error", payload={"message": str(e)})
            return {"ok": False, "error": str(e)}

    def _build_evidence(self, session_id: str) -> List[Dict[str, Any]]:
        notes = self._notes.list_for_session(
            session_id,
            limit=300,
            kinds=["observation", "user_answer", "cadastral_finding", "listings_finding"],
        )
        evidence: List[Dict[str, Any]] = []
        for n in notes:
            kind = n.get("kind") or "observation"
            base: Dict[str, Any] = {"kind": kind, "text": n.get("text") or ""}
            if kind in ("cadastral_finding", "listings_finding"):
                # `payload` несе сирий JSON-словник із tool-результату — reporter
                # використовує його, щоб витягти cadastral_numbers / listings без парсингу text.
                payload = n.get("payload") or {}
                if isinstance(payload, dict) and payload:
                    base["payload"] = payload
            evidence.append(base)
        # Додаємо також зареєстровані артефакти карт у вигляді {kind: 'map', artifact_id: ...}
        try:
            map_events = self._events.fetch_since(session_id, 0, limit=500)
            for ev in map_events:
                if ev.get("type") == "status":
                    payload = ev.get("payload") or {}
                    aid = payload.get("artifact_id")
                    if aid:
                        evidence.append({"kind": "map", "artifact_id": aid, "text": str(payload.get("message") or "")})
        except Exception:
            pass
        sess = self._sessions.get_by_session_id(session_id) or {}
        graph = dict(sess.get("evidence_graph") or {})
        for claim in (graph.get("claims") or [])[:40]:
            if not isinstance(claim, dict):
                continue
            evidence.append(
                {
                    "kind": "claim",
                    "text": str(claim.get("statement") or ""),
                    "confidence": float(claim.get("confidence") or 0.0),
                    "evidence_count": int(claim.get("evidence_count") or 0),
                    "source_diversity": int(claim.get("source_diversity") or 0),
                    "contradiction_status": str(claim.get("contradiction_status") or "none"),
                }
            )
        return evidence

    def _notify_user_success_in_telegram(
        self,
        *,
        chat_id: Optional[str],
        user_id: str,
        session_id: str,
        artifact_id: str,
        html: str,
    ) -> None:
        """Надсилає коротку нотифікацію + HTML-звіт користувачу у Telegram.

        chat_id у сесії може бути UUID Mini App-чату (не Telegram chat_id),
        тому числовий target_chat шукаємо у такому порядку:
        1) chat_id, якщо він парситься як int (приватний Telegram chat == user_id);
        2) user_id (числовий Telegram user_id з валідованої initData) — як fallback.
        Якщо жоден варіант не дав числа — лише логуємо причину і виходимо.
        """
        bot_token = str(getattr(self.settings, "telegram_bot_token", "") or "").strip()
        if not bot_token:
            logger.warning(
                "TG notify skipped for session %s: telegram_bot_token is not configured",
                session_id,
            )
            return

        target_chat: Optional[int] = None
        chat_id_raw = str(chat_id).strip() if chat_id is not None else ""
        if chat_id_raw:
            try:
                target_chat = int(chat_id_raw)
            except (TypeError, ValueError):
                logger.debug(
                    "TG notify: chat_id=%r is not numeric (likely Mini App UUID); falling back to user_id",
                    chat_id_raw,
                )
        if target_chat is None and user_id:
            try:
                target_chat = int(str(user_id).strip())
            except (TypeError, ValueError):
                logger.warning(
                    "TG notify skipped for session %s: user_id=%r is not numeric Telegram id",
                    session_id,
                    user_id,
                )
                return
        if target_chat is None:
            logger.warning(
                "TG notify skipped for session %s: neither chat_id nor user_id is numeric",
                session_id,
            )
            return

        try:
            import requests

            text = (
                "Дослідження Flx успішно завершено.\n"
                f"Session: {session_id}\n"
                "Надсилаю HTML-звіт файлом."
            )
            resp = requests.post(
                f"https://api.telegram.org/bot{bot_token}/sendMessage",
                data={"chat_id": target_chat, "text": text},
                timeout=20,
            )
            if resp.status_code != 200:
                logger.warning(
                    "TG sendMessage failed for session %s: HTTP %s body=%s",
                    session_id,
                    resp.status_code,
                    (resp.text or "")[:200],
                )
        except Exception as e:
            logger.warning("Telegram success message failed for session %s: %s", session_id, e)
        try:
            ok = send_file_via_telegram(
                chat_id=target_chat,
                file_bytes=html.encode("utf-8"),
                filename=f"investigation_report_{artifact_id}.html",
                bot_token=bot_token,
                caption="Готовий звіт Flx",
            )
            if not ok:
                logger.warning(
                    "Telegram report file send returned false for session %s (chat_id=%s)",
                    session_id,
                    target_chat,
                )
            else:
                logger.info(
                    "Telegram report delivered for session %s to chat_id=%s",
                    session_id,
                    target_chat,
                )
        except Exception as e:
            logger.warning("Telegram report file send failed for session %s: %s", session_id, e)

    def _collect_steps_log(self, session_id: str) -> List[Dict[str, Any]]:
        notes = self._notes.list_for_session(session_id, limit=300)
        log: List[Dict[str, Any]] = []
        for n in notes:
            tcs = n.get("tool_call_summary") or {}
            log.append({
                "step_id": n.get("step_index") or 0,
                "tool": tcs.get("name") if isinstance(tcs, dict) else None,
                "ok": n.get("kind") not in ("tool_error",),
                "note": n.get("text"),
            })
        return log

    # ----------------------------- HTML render ----------------------------

    def _render_report_html(self, structured: Dict[str, Any]) -> str:
        """Рендерить HTML на базі Jinja-шаблону. Помилки шаблону → graceful fallback."""
        template_path = Path(__file__).parent.parent.parent / "telegram_mini_app" / "static" / "templates" / "investigation_report.html.j2"
        try:
            from jinja2 import Environment, FileSystemLoader, select_autoescape
            env = Environment(
                loader=FileSystemLoader(str(template_path.parent)),
                autoescape=select_autoescape(enabled_extensions=("html", "j2")),
                trim_blocks=True,
                lstrip_blocks=True,
            )
            tmpl = env.get_template(template_path.name)
            from urllib.parse import quote as _urlquote

            def _artifact_href(aid: str, token: Optional[str] = None) -> str:
                aid_s = str(aid or "").strip()
                if not aid_s:
                    return "#"
                tok = str(token or "").strip()
                path = f"/api/files/artifact/{_urlquote(aid_s, safe='')}"
                if tok:
                    return f"{path}?token={_urlquote(tok, safe='')}"
                return path

            return tmpl.render(
                report=structured,
                generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                static_map_url_prefix="/api/files/artifact/",
                artifact_href=_artifact_href,
            )
        except Exception as e:
            logger.warning("Jinja render failed, fallback HTML: %s", e)
            return self._fallback_html(structured)

    def _fallback_html(self, structured: Dict[str, Any]) -> str:
        title = (structured.get("title") or "Звіт Flx").replace("<", "&lt;")
        summary = (structured.get("executive_summary") or "").replace("<", "&lt;")
        sections_html_parts: List[str] = []
        for s in structured.get("sections") or []:
            t = (s.get("title") or "").replace("<", "&lt;")
            n = (s.get("narrative") or "").replace("<", "&lt;")
            sections_html_parts.append(f"<section><h2>{t}</h2><p>{n}</p></section>")
        sections_html = "".join(sections_html_parts)
        att_parts: List[str] = []
        for a in structured.get("attachments") or []:
            if not isinstance(a, dict):
                continue
            t = str(a.get("title") or "Додаток").replace("<", "&lt;")
            aid = str(a.get("artifact_id") or "").strip()
            tok = str(a.get("download_token") or "").strip()
            if not aid:
                continue
            href = f"/api/files/artifact/{aid}" + (f"?token={tok}" if tok else "")
            att_parts.append(f'<p><a href="{href}">{t}</a></p>')
        att_html = "".join(att_parts)
        return f"""<!doctype html><html lang='uk'><head><meta charset='utf-8'><title>{title}</title>
<style>body{{font-family:system-ui;max-width:920px;margin:24px auto;padding:0 16px;color:#1a1a1a}}
h1{{margin:0 0 12px}} h2{{margin-top:32px}} section{{border-top:1px solid #eee;padding-top:16px}}</style></head>
<body><h1>{title}</h1><p><em>{summary}</em></p>{att_html}{sections_html}</body></html>"""

    # ------------------------- tools registry ----------------------------

    def allowed_tool_names(self) -> List[str]:
        return list(self._tools_registry.keys())

    def tools_schemas_public(self) -> List[Dict[str, Any]]:
        """Повертає публічну схему тулз (name, description, input_schema)."""
        out: List[Dict[str, Any]] = []
        for name, tdef in self._tools_registry.items():
            out.append({
                "name": name,
                "description": tdef.get("description", ""),
                "input_schema": tdef.get("input_schema", {}),
            })
        return out

    def _inject_context(self, name: str, args: Dict[str, Any], session_id: str) -> Dict[str, Any]:
        """Інжектує session_id/user_id у тулзи Flx."""
        if name in (
            "flx.note_write",
            "flx.notes_read",
            "flx.ask_user",
            "flx.report_compose",
            "flx.static_map_render",
        ):
            extra: Dict[str, Any] = {"session_id": session_id}
            sess = self._sessions.get_by_session_id(session_id)
            if sess:
                extra["__session_user_id"] = sess.get("user_id")  # для render
            return extra
        return {}

    # Аліаси неіснуючих/неточних імен аргументів від LLM на справжні параметри тулз.
    # LLM (особливо gemini-flash) часто вигадує імена на кшталт `city_name`,
    # `min_area_sqm` — без аліасинга вони мовчки ігноруються через `**_:Any`,
    # що призводить до того, що user-locale фільтри не застосовуються і тула
    # повертає «все підряд». Нижче — карта { tool_name: { wrong_arg: right_arg or None } }.
    # Значення `None` означає «прибрати з args» (немає еквіваленту в тулі).
    _TOOL_ARG_ALIASES: Dict[str, Dict[str, Optional[str]]] = {
        "listings.find_with_fallback": {
            "city_name": "city",
            "region_name": "region",
            "city_contains": "city",
            "region_contains": "region",
            "min_area_sqm": "building_area_min",
            "max_area_sqm": "building_area_max",
            "min_land_area_sqm": "land_area_min",
            "max_land_area_sqm": "land_area_max",
            "listing_types": None,
            "listing_type": None,
            "days_since_published": None,
            "tags_contains": "tags",
            "semantic_query": "query_text",
        },
    }

    def _normalize_tool_args(self, tool_name: str, args: Dict[str, Any]) -> Dict[str, Any]:
        aliases = self._TOOL_ARG_ALIASES.get(tool_name)
        if not aliases or not isinstance(args, dict):
            return args
        out: Dict[str, Any] = {}
        renamed: List[Tuple[str, Optional[str]]] = []
        for k, v in args.items():
            if k in aliases:
                target = aliases[k]
                renamed.append((k, target))
                if target is None:
                    continue
                if target not in out or out.get(target) in (None, ""):
                    out[target] = v
            else:
                out[k] = v
        if renamed:
            logger.info(
                "[flx-think] tool_args.normalize tool=%s renames=%s",
                tool_name,
                ", ".join(f"{a}->{b or 'DROP'}" for a, b in renamed),
            )
        return out

    # ------------------------------------------------------------------
    # Anti-hallucination guard для `step.thought`.
    # LLM (`gemini-flash`) часто пише в reasoning «Шукаю в Турці Львівської
    # області…», тоді як scope=Турійськ/Волинська. Навіть якщо args потім
    # переписуються scope_lock'ом — користувач бачить хибну думку в UI/timeline.
    # Цей санітайз вирізає згадки чужих міст і замінює на scope city + warning.
    # ------------------------------------------------------------------
    # Корені обласних форм. Будь-яке слово, що починається з кореня + «ськ» — це регіон,
    # не місто. Замінюється на scope_region.
    _UA_REGION_ROOTS = [
        "Львівськ",
        "Закарпатськ",
        "Одеськ",
        "Київськ",
        "Дніпропетровськ",
        "Харківськ",
        "Полтавськ",
        "Вінницьк",
        "Житомирськ",
        "Чернігівськ",
        "Сумськ",
        "Тернопільськ",
        "Хмельницьк",
        "Чернівецьк",
        "Закарпатськ",
        "Івано-Франківськ",
        "Запорізьк",
        "Миколаївськ",
        "Херсонськ",
        "Рівненськ",
        "Черкаськ",
        "Кіровоградськ",
        "Волинськ",
    ]

    # Watchlist маркерів міст. Українські відмінки міняють корінь («Львів» → «Львов-а»,
    # «Львов-і»), тому тримаємо набір варіантів. Negative lookahead `(?!ськ)` після
    # маркера виключає регіональну форму («Львівськ-а» — це область, не місто).
    _UA_CITIES_WATCHLIST = [
        "Турка", "Турк", "Турці",
        "Львів", "Львов",
        "Київ", "Києв",
        "Одес",
        "Харків", "Харков",
        "Дніпр",
        "Запоріжж", "Запорізьк",
        "Полтав",
        "Вінниц",
        "Житомир",
        "Чернігов", "Чернігів",
        "Тернопіл", "Тернопол",
        "Рівн",
        "Чернівц", "Черновц",
        "Ужгород",
        "Мукачев", "Мукачов",
        "Хуст",
        "Івано-Франків",
        "Миколаїв", "Миколаєв",
        "Херсон",
        "Дрогобич",
    ]

    def _sanitize_text_against_scope(
        self,
        text: str,
        scope_lock: Optional[Dict[str, Any]],
    ) -> Tuple[str, List[str]]:
        """Замінює галюциновані локації в довільному тексті на канонічні зі scope_lock.

        Працює для будь-якого вихідного поля LLM-рішення: thought, ask_user.question,
        ask_user.options[*], final_for_step.observation. Повертає (sanitized, removed_tokens).
        """
        if not text or not isinstance(scope_lock, dict):
            return text, []
        loc = scope_lock.get("location") if isinstance(scope_lock.get("location"), dict) else {}
        scope_city = (loc.get("city") or "").strip()
        scope_region = (loc.get("region") or "").strip()
        if not scope_city:
            return text, []
        thought = text

        # Whitelist scope-міста: точна форма + знайомі відмінки.
        allowed_city_lower = {scope_city.lower()}
        if scope_city.endswith("ськ"):
            stem = scope_city[:-3]
            for suf in ("ськ", "ському", "ська", "ськом", "ську", "ське", "ський"):
                allowed_city_lower.add((stem + suf).lower())
        else:
            for suf in ("", "а", "у", "і", "ом", "ою", "е"):
                allowed_city_lower.add((scope_city + suf).lower())

        hallucinated: List[str] = []
        new_thought = thought

        # 1) Спочатку патчимо регіональні форми. Корінь + «ськ» + будь-який суфікс.
        scope_region_root = (scope_region.replace("область", "").strip().lower())[:8]
        for root in self._UA_REGION_ROOTS:
            if scope_region_root and root.lower().startswith(scope_region_root[:6]):
                continue
            pat = re.compile(rf"\b{re.escape(root)}[а-яіїєґʼ'`-]*\b", flags=re.IGNORECASE | re.UNICODE)
            if pat.search(new_thought):
                hallucinated.append(root)
                new_thought = pat.sub(scope_region or "scope_lock", new_thought)

        # 2) Потім міста — з negative lookahead на «ськ» (щоб не зачепити «Львівська»).
        for token in self._UA_CITIES_WATCHLIST:
            if token.lower() in allowed_city_lower:
                continue
            # core + word-suffix; виключаємо суфікс «ськ» (то регіональна форма).
            pat = re.compile(
                rf"\b{re.escape(token)}(?!ськ)[а-яіїєґʼ'`-]*\b",
                flags=re.IGNORECASE | re.UNICODE,
            )
            if pat.search(new_thought):
                hallucinated.append(token)
                new_thought = pat.sub(scope_city, new_thought)

        if hallucinated:
            seen: List[str] = []
            for x in hallucinated:
                if x not in seen:
                    seen.append(x)
            hallucinated = seen
        return new_thought, hallucinated

    def _sanitize_thought_against_scope(
        self,
        thought: str,
        scope_lock: Optional[Dict[str, Any]],
    ) -> Tuple[str, List[str]]:
        """Зворотня сумісність: тонкий wrapper над _sanitize_text_against_scope."""
        return self._sanitize_text_against_scope(thought, scope_lock)

    def _sanitize_decision_against_scope(
        self,
        decision: Dict[str, Any],
        scope_lock: Optional[Dict[str, Any]],
    ) -> List[str]:
        """In-place санітайз ВСІХ текстових полів LLM-рішення проти scope_lock.

        Обробляє: thought, ask_user.question, ask_user.options[*], final_for_step.observation.
        Повертає список усіх замінених токенів (для логування/SSE).
        """
        if not isinstance(decision, dict) or not isinstance(scope_lock, dict):
            return []
        loc = scope_lock.get("location") if isinstance(scope_lock.get("location"), dict) else {}
        if not (loc.get("city") or "").strip():
            return []
        all_removed: List[str] = []

        thought = decision.get("thought")
        if isinstance(thought, str) and thought.strip():
            new_t, removed = self._sanitize_text_against_scope(thought, scope_lock)
            if removed:
                decision["thought"] = new_t
                all_removed.extend(removed)

        au = decision.get("ask_user")
        if isinstance(au, dict):
            q = au.get("question")
            if isinstance(q, str) and q.strip():
                new_q, removed = self._sanitize_text_against_scope(q, scope_lock)
                if removed:
                    au["question"] = new_q
                    all_removed.extend(removed)
            opts = au.get("options")
            if isinstance(opts, list):
                new_opts = []
                for o in opts:
                    if isinstance(o, str) and o.strip():
                        new_o, removed = self._sanitize_text_against_scope(o, scope_lock)
                        if removed:
                            all_removed.extend(removed)
                        new_opts.append(new_o)
                    else:
                        new_opts.append(o)
                au["options"] = new_opts

        fs = decision.get("final_for_step")
        if isinstance(fs, dict):
            obs = fs.get("observation")
            if isinstance(obs, str) and obs.strip():
                new_o, removed = self._sanitize_text_against_scope(obs, scope_lock)
                if removed:
                    fs["observation"] = new_o
                    all_removed.extend(removed)

        # дедуплікуємо
        if all_removed:
            seen: List[str] = []
            for x in all_removed:
                if x not in seen:
                    seen.append(x)
            all_removed = seen
        return all_removed

    # ------------------------------------------------------------------
    # DB knowledge block (для LLM-step prompt). Зразок аналогічний штатному
    # LangChain-агенту: профіль колекцій + топ-значення → у system context.
    # ------------------------------------------------------------------
    _DB_KNOWLEDGE_CACHE_TTL_SEC = 300  # 5 хв

    def _get_db_knowledge_block(self) -> str:
        try:
            now = datetime.now(timezone.utc).timestamp()
            cache = getattr(self, "_db_knowledge_cache", None)
            if cache and (now - cache[0]) < self._DB_KNOWLEDGE_CACHE_TTL_SEC:
                return cache[1]
            from business.services.collection_knowledge_service import CollectionKnowledgeService
            block = CollectionKnowledgeService().get_knowledge_for_agent(max_length=3500) or ""
            self._db_knowledge_cache = (now, block)
            return block
        except Exception as e:
            logger.debug("DB knowledge block unavailable: %s", e)
            return ""

    # ------------------------------------------------------------------
    # scope_lock enforcement
    # ------------------------------------------------------------------
    def _detect_scope_mismatch(
        self,
        *,
        tool_name: str,
        args: Dict[str, Any],
        scope_lock: Dict[str, Any],
    ) -> Optional[str]:
        """Перевіряє чи LLM написав у args значення локації, що НЕ співпадають зі scope.

        Повертає рядок-опис mismatch'у (для error observation), якщо є явний конфлікт.
        Випадок 'args не містить city/region' — НЕ mismatch (заповнимо силено в _apply).
        """
        if not isinstance(args, dict) or not isinstance(scope_lock, dict):
            return None
        loc = scope_lock.get("location") if isinstance(scope_lock.get("location"), dict) else {}
        if not loc:
            return None
        scope_city = (loc.get("city") or "").strip()
        scope_region = (loc.get("region") or "").strip()
        if not scope_city and not scope_region:
            return None

        problems: List[str] = []

        if tool_name == "listings.find_with_fallback":
            arg_city = (str(args.get("city") or "")).strip()
            arg_region = (str(args.get("region") or "")).strip()
            if scope_city and arg_city and arg_city.lower() != scope_city.lower():
                problems.append(f'args.city="{arg_city}" ≠ scope.city="{scope_city}"')
            if scope_region and arg_region:
                # дозволяємо «Волинська» = «Волинська область»
                arg_region_norm = re.sub(r"\s*область\s*$", "", arg_region, flags=re.IGNORECASE).strip()
                if arg_region_norm.lower() != scope_region.lower():
                    problems.append(f'args.region="{arg_region}" ≠ scope.region="{scope_region}"')

        elif tool_name in ("cadastral.discover_in_area", "cadastral.search"):
            # cadastral приймає тільки scope.oblast_name (без top-level city/region) — їх ми silent-видаляємо.
            # Перевіримо scope.oblast_name всередині args.scope.
            scope_in = args.get("scope") if isinstance(args.get("scope"), dict) else {}
            arg_oblast = (str((scope_in or {}).get("oblast_name") or "")).strip()
            if scope_region and arg_oblast:
                arg_oblast_norm = re.sub(r"\s*область\s*$", "", arg_oblast, flags=re.IGNORECASE).strip()
                if arg_oblast_norm.lower() != scope_region.lower():
                    problems.append(f'args.scope.oblast_name="{arg_oblast}" ≠ scope.region="{scope_region}"')
            # top-level keys в cadastral — це окрема, дуже типова помилка LLM. Якщо там значення не зі scope —
            # це теж mismatch. Але цей кейс ми вже акуратно дропаємо у _apply_scope_lock_to_args;
            # тут блокуємо тільки якщо явно протиречить scope:
            for k in ("city", "region", "region_name", "oblast_name"):
                if k in args and args[k]:
                    v = str(args[k]).strip()
                    v_norm = re.sub(r"\s*область\s*$", "", v, flags=re.IGNORECASE).strip()
                    if scope_region and v_norm.lower() != scope_region.lower() and v.lower() != scope_city.lower():
                        problems.append(f'args.{k}="{v}" не співпадає зі scope (city="{scope_city}", region="{scope_region}")')

        if not problems:
            return None
        return "; ".join(problems)

    def _apply_scope_lock_to_args(
        self,
        *,
        tool_name: str,
        args: Dict[str, Any],
        scope_lock: Dict[str, Any],
        session_id: str,
        step_index: int,
    ) -> Dict[str, Any]:
        """Примусово накладає `scope_lock` на args listings/cadastral викликів.

        Якщо LLM написав не те місто/область/площу — переписуємо на значення
        з scope_lock (витягнуті планером при старті сесії). Це працює як guard
        rail від хронічних галюцинацій моделі.
        """
        if not isinstance(args, dict):
            return args
        loc = scope_lock.get("location") if isinstance(scope_lock.get("location"), dict) else {}
        prop = scope_lock.get("property") if isinstance(scope_lock.get("property"), dict) else {}
        if not loc and not prop:
            return args

        overrides: List[Tuple[str, Any, Any]] = []

        def _override(key: str, new_val: Any) -> None:
            old = args.get(key)
            if old == new_val or new_val in (None, ""):
                return
            overrides.append((key, old, new_val))
            args[key] = new_val

        if tool_name == "listings.find_with_fallback":
            if loc.get("city"):
                _override("city", loc["city"])
            if loc.get("region"):
                _override("region", loc["region"])
            if prop.get("type_keyword") and not args.get("property_type_contains"):
                _override("property_type_contains", prop["type_keyword"])
            if prop.get("min_area_sqm") and not args.get("building_area_min"):
                _override("building_area_min", prop["min_area_sqm"])
            if prop.get("max_area_sqm") and not args.get("building_area_max"):
                _override("building_area_max", prop["max_area_sqm"])

        elif tool_name in ("cadastral.discover_in_area", "cadastral.search"):
            scope_in = args.get("scope") if isinstance(args.get("scope"), dict) else {}
            new_scope = dict(scope_in)
            has_geo = bool(new_scope.get("latitude") and new_scope.get("longitude"))

            # LLM часто кладе oblast_name / region / region_name / city як top-level args.
            # Tool схема їх не приймає, тому переносимо у scope (overrides) і видаляємо
            # з верхнього рівня. Це робить scope_lock реально efective для cadastral.
            top_level_keys = ("city", "region", "region_name", "oblast_name", "oblast_code")
            stray: Dict[str, Any] = {}
            for k in top_level_keys:
                if k in args:
                    stray[k] = args.pop(k)
                    overrides.append((f"DROP top-level `{k}`", stray[k], None))

            scope_oblast_from_stray = (
                stray.get("oblast_name")
                or stray.get("region_name")
                or stray.get("region")
            )

            if not has_geo:
                # 1) Якщо LLM поклав область неправильно top-level — переносимо у scope (тільки
                #    якщо вона збігається або обертається на scope_lock.region; інакше беремо саме scope_lock).
                target_oblast = loc.get("region") or (
                    re.sub(r"\s*область\s*$", "", str(scope_oblast_from_stray)).strip()
                    if scope_oblast_from_stray else None
                )
                if target_oblast and new_scope.get("oblast_name") != target_oblast:
                    old = new_scope.get("oblast_name")
                    new_scope["oblast_name"] = target_oblast
                    overrides.append(("scope.oblast_name", old, target_oblast))

            if new_scope != scope_in:
                args["scope"] = new_scope
            if tool_name == "cadastral.search":
                filters_in = args.get("filters") if isinstance(args.get("filters"), dict) else {}
                new_filters = dict(filters_in)
                filt_changed = False
                if prop.get("min_area_sqm") and not new_filters.get("min_area_sqm"):
                    new_filters["min_area_sqm"] = prop["min_area_sqm"]
                    filt_changed = True
                    overrides.append(("filters.min_area_sqm", None, prop["min_area_sqm"]))
                if prop.get("max_area_sqm") and not new_filters.get("max_area_sqm"):
                    new_filters["max_area_sqm"] = prop["max_area_sqm"]
                    filt_changed = True
                    overrides.append(("filters.max_area_sqm", None, prop["max_area_sqm"]))
                if filt_changed:
                    args["filters"] = new_filters

        if overrides:
            preview = ", ".join(f"{k}: {self._fmt_val(o)}→{self._fmt_val(n)}" for k, o, n in overrides)
            # Лише в логи. У notes/events НЕ пишемо: інакше LLM бачить кожну корекцію
            # у `notes_summary` і це reinforce-loop'ить помилкову поведінку. Замість
            # цього warning іде у префікс observation (бачить _handle_tool_call).
            logger.info(
                "[flx-think] scope_lock.apply session=%s tool=%s overrides=%s",
                session_id,
                tool_name,
                preview,
            )
        return args

    @staticmethod
    def _fmt_val(v: Any) -> str:
        if v is None:
            return "—"
        s = str(v)
        return s if len(s) <= 60 else (s[:60] + "…")

    def _resolve_tool_name(self, raw_name: str) -> str:
        """Нормалізує ім'я тулзи від LLM до канонічного імені у registry.

        LLM-моделі (особливо `gemini-2.5-flash`) часто додають префікс `flx.`
        до тул, що його не мають, замінюють крапки на підкреслення, або
        навпаки. Цей метод намагається у такому порядку:
            1) точна назва — `name`
            2) `name` з префіксом `flx.` (якщо в registry таке є)
            3) `name` без префікса `flx.`
            4) replace('_', '.') для будь-якого з варіантів вище
        Якщо нічого не зматчилось — повертає вихідне ім'я (виклик відхилить
        allow-check з детальним повідомленням про помилку).
        """
        raw = (raw_name or "").strip()
        if not raw:
            return ""
        registry = self._tools_registry
        candidates: List[str] = []
        candidates.append(raw)
        if raw.startswith("flx."):
            candidates.append(raw[4:])
        else:
            candidates.append(f"flx.{raw}")
        # додатково — заміна підкреслень на крапки (для випадку `flx.cadastral_get_knowledge`)
        for c in list(candidates):
            if "_" in c:
                # замінюємо лише останні підкреслення в "domain_method" → "domain.method"
                # для імен типу `cadastral_get_knowledge` → `cadastral.get_knowledge`
                parts = c.split("_", 1)
                if len(parts) == 2:
                    candidates.append(parts[0] + "." + parts[1])
                candidates.append(c.replace("_", "."))
        for c in candidates:
            if c in registry:
                return c
        return raw

    @staticmethod
    def _safe_args_preview(args: Dict[str, Any]) -> Dict[str, Any]:
        out: Dict[str, Any] = {}
        for k, v in (args or {}).items():
            if k.startswith("__"):
                continue
            if isinstance(v, (dict, list)):
                out[k] = f"<{type(v).__name__} keys={len(v)}>"
            else:
                vs = str(v)
                out[k] = vs if len(vs) <= 200 else vs[:200] + "…"
        return out

    @staticmethod
    def _serialize_observation(value: Any) -> str:
        try:
            import json
            text = json.dumps(value, ensure_ascii=False, default=str)
        except Exception:
            text = str(value)
        if len(text) > TOOL_OBSERVATION_TRUNCATE:
            text = text[:TOOL_OBSERVATION_TRUNCATE].rstrip() + "…"
        return text

    # ----------------------- Structured findings ------------------------

    def _record_structured_findings(
        self,
        *,
        session_id: str,
        tool_name: str,
        result: Any,
        step_index: int,
    ) -> None:
        """Зберігає сирий результат cadastral.search / listings.find_with_fallback
        у структурованій нотатці з повним списком cadastral_numbers / listings.

        Reporter-агент у `_build_evidence` отримує цю нотатку з payload,
        тому навіть якщо observation-рядок обрізаний — повний інвентар знахідок
        потрапляє у звіт.
        """
        if not isinstance(result, dict) or not result.get("ok"):
            return
        if tool_name == "cadastral.search":
            payload = self._compose_cadastral_finding_payload(result)
            if not payload:
                return
            text = self._cadastral_finding_text(payload)
            self._note_append(
                session_id=session_id,
                kind="cadastral_finding",
                text=text,
                step_index=step_index,
                tool_call_summary={"name": tool_name, "kind": "structured"},
                payload=payload,
            )
        elif tool_name == "listings.find_with_fallback":
            payload = self._compose_listings_finding_payload(result)
            if not payload:
                return
            text = self._listings_finding_text(payload)
            self._note_append(
                session_id=session_id,
                kind="listings_finding",
                text=text,
                step_index=step_index,
                tool_call_summary={"name": tool_name, "kind": "structured"},
                payload=payload,
            )

    @staticmethod
    def _compose_cadastral_finding_payload(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        clusters = result.get("clusters") or []
        singles = result.get("single_parcels") or []
        if not clusters and not singles:
            return None
        max_clusters = 15
        max_singles = 15
        max_cns_per_group = 50

        def _slim_group(g: Dict[str, Any]) -> Dict[str, Any]:
            cns = list(g.get("cadastral_numbers") or [])
            return {
                "cluster_id": g.get("cluster_id"),
                "is_singleton": bool(g.get("is_singleton")),
                "parcel_count": g.get("parcel_count"),
                "total_area_sqm": g.get("total_area_sqm"),
                "purpose": g.get("purpose"),
                "purpose_label": g.get("purpose_label"),
                "ownership_form": g.get("ownership_form"),
                "centroid": g.get("centroid"),
                "cadastral_numbers": cns[:max_cns_per_group],
                "cadastral_numbers_total": len(cns),
            }

        return {
            "scope": result.get("scope"),
            "applied_filters": result.get("applied_filters"),
            "scope_total_parcels": result.get("scope_total_parcels"),
            "filtered_total_parcels": result.get("filtered_total_parcels"),
            "clusters": [_slim_group(c) for c in clusters[:max_clusters]],
            "single_parcels": [_slim_group(s) for s in singles[:max_singles]],
            "semantic_used": bool(result.get("semantic_used")),
        }

    @staticmethod
    def _cadastral_finding_text(payload: Dict[str, Any]) -> str:
        clusters = payload.get("clusters") or []
        singles = payload.get("single_parcels") or []
        bits: List[str] = []
        if clusters:
            top = clusters[0]
            bits.append(
                f"Кадастр: {len(clusters)} кластер(ів). Топ: "
                f"{top.get('parcel_count')} діл., "
                f"{round(float(top.get('total_area_sqm') or 0), 1)} м², "
                f"{top.get('purpose_label') or top.get('purpose')}, "
                f"{top.get('ownership_form')}."
            )
        if singles:
            bits.append(f"Одиночних ділянок: {len(singles)}.")
        if not bits:
            bits.append("Кадастр: знахідок 0 за фільтрами (див. landscape).")
        return " ".join(bits)[:8000]

    @staticmethod
    def _compose_listings_finding_payload(result: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        items = result.get("items") or []
        if not items and not result.get("landscape"):
            return None
        max_items = 30

        def _slim_item(it: Dict[str, Any]) -> Dict[str, Any]:
            contact = ""
            for key in (
                "seller_phone",
                "phone",
                "contact_phone",
                "owner_phone",
                "seller_name",
                "contact_name",
            ):
                v = it.get(key)
                if v is not None and str(v).strip():
                    contact = str(v).strip()[:200]
                    break
            return {
                "source": it.get("source"),
                "source_id": it.get("source_id"),
                "url": it.get("url"),
                "title": it.get("title"),
                "address": it.get("address") or it.get("address_text"),
                "city": it.get("city"),
                "region": it.get("region"),
                "property_type": it.get("property_type"),
                "price_uah": it.get("price_uah"),
                "price_usd": it.get("price_usd"),
                "building_area_sqm": it.get("building_area_sqm"),
                "land_area_sqm": it.get("land_area_sqm"),
                "status": it.get("status"),
                "contact_hint": contact or None,
            }

        return {
            "applied_strategy": result.get("applied_strategy"),
            "applied_filters": result.get("applied_filters"),
            "original_filters": result.get("original_filters"),
            "count": int(result.get("count") or 0),
            "items": [_slim_item(it) for it in items[:max_items] if isinstance(it, dict)],
            "landscape": result.get("landscape"),
            "attempts": [
                {k: a.get(k) for k in ("strategy", "count", "filters_used", "error", "skipped") if k in a}
                for a in (result.get("attempts") or [])
            ],
        }

    @staticmethod
    def _listings_finding_text(payload: Dict[str, Any]) -> str:
        n = int(payload.get("count") or 0)
        strategy = payload.get("applied_strategy") or "none"
        return (
            f"Listings: {n} оголошень через стратегію `{strategy}`. "
            f"Фільтри: {payload.get('applied_filters') or {}}."
        )[:8000]

    def _build_tools_registry(self) -> Dict[str, Dict[str, Any]]:
        """Будує реєстр тулз із allow-list settings.flx_allowed_tools."""
        allowed = set(self.settings.flx_allowed_tools or [])
        registry: Dict[str, Dict[str, Any]] = {}

        def reg(name: str, desc: str, schema: Dict[str, Any], fn: Callable[..., Any]) -> None:
            if name in allowed:
                registry[name] = {"description": desc, "input_schema": schema, "callable": fn}

        # ---- query_builder ----
        reg(
            "query_builder.execute_query",
            "Безпечний запит до колекції з allow-list (prozorro_auctions, olx_listings, unified_listings) через QueryBuilder.",
            {"type": "object", "properties": {"query": {"type": "object"}}, "required": ["query"]},
            self._tool_qb_execute_query,
        )
        reg(
            "query_builder.execute_aggregation",
            "Безпечний aggregation pipeline через QueryBuilder.",
            {"type": "object", "properties": {"collection": {"type": "string"}, "pipeline": {"type": "array"}}, "required": ["collection", "pipeline"]},
            self._tool_qb_execute_aggregation,
        )
        reg(
            "query_builder.save_query_to_temp_collection",
            "Зберігає результати запиту у тимчасову колекцію (для подальшого експорту).",
            {"type": "object", "properties": {"query": {"type": "object"}}, "required": ["query"]},
            self._tool_qb_save_temp,
        )
        reg(
            "query_builder.get_distinct_values",
            "Повертає унікальні значення поля колекції (city, region, tags тощо).",
            {"type": "object", "properties": {"collection": {"type": "string"}, "field": {"type": "string"}, "unwrap_array": {"type": "boolean"}}, "required": ["collection", "field"]},
            self._tool_qb_distinct,
        )
        # ---- analytics ----
        reg(
            "analytics.execute_analytics",
            "Виконує аналітичний запит з метриками (avg/sum/count/distribution/trend), groupBy, filters.",
            {"type": "object", "properties": {"query": {"type": "object"}}, "required": ["query"]},
            self._tool_analytics_execute,
        )
        reg(
            "analytics.list_metrics",
            "Список доступних метрик для execute_analytics.",
            {"type": "object", "properties": {}},
            self._tool_analytics_list_metrics,
        )
        # ---- schema ----
        reg(
            "schema.get_collection_info",
            "Повертає опис колекції з Data Dictionary.",
            {"type": "object", "properties": {"collection_name": {"type": "string"}}, "required": ["collection_name"]},
            self._tool_schema_collection_info,
        )
        reg(
            "schema.get_data_dictionary",
            "Повертає повний Data Dictionary (логічні поля → physical fields).",
            {"type": "object", "properties": {}},
            self._tool_schema_data_dictionary,
        )
        # ---- geocoding ----
        reg(
            "geocoding.geocode_address",
            "Геокодує адресу або топонім (назва ЖК/ТРЦ/вулиця/місто) → координати, formatted_address, place_id.",
            {"type": "object", "properties": {"address_or_place": {"type": "string"}, "region": {"type": "string"}}, "required": ["address_or_place"]},
            self._tool_geocode,
        )
        reg(
            "geocoding.search_nearby_places",
            "Шукає POI поблизу координат (pharmacy, restaurant, supermarket тощо).",
            {"type": "object", "properties": {"latitude": {"type": "number"}, "longitude": {"type": "number"}, "place_types": {"type": "array"}, "radius_meters": {"type": "integer"}}, "required": ["latitude", "longitude", "place_types"]},
            self._tool_places_nearby,
        )
        # ---- flx ----
        reg(
            "flx.note_write",
            "Записує нотатку (думку/спостереження/гіпотезу/рішення) для активного дослідження.",
            {"type": "object", "properties": {"kind": {"type": "string"}, "text": {"type": "string"}}, "required": ["kind", "text"]},
            self._tool_flx_note_write,
        )
        reg(
            "flx.notes_read",
            "Повертає короткий summary останніх нотаток сесії.",
            {"type": "object", "properties": {"top_k": {"type": "integer"}}},
            self._tool_flx_notes_read,
        )
        reg(
            "flx.lessons_search",
            "Пошук схожих lessons-learned по тексту запиту.",
            {"type": "object", "properties": {"query": {"type": "string"}, "top_k": {"type": "integer"}}, "required": ["query"]},
            self._tool_flx_lessons_search,
        )
        reg(
            "flx.lessons_save",
            "Зберігає lesson-learned (зазвичай викликається наприкінці; orchestrator робить це автоматично).",
            {"type": "object", "properties": {"topic_tags": {"type": "array"}, "query_pattern": {"type": "string"}, "what_worked": {"type": "array"}, "what_failed": {"type": "array"}, "recommendations": {"type": "array"}}, "required": ["topic_tags", "query_pattern"]},
            self._tool_flx_lessons_save,
        )
        reg(
            "flx.static_map_render",
            "Рендерить Google Static Maps PNG → artifact_id для звіту.",
            {"type": "object", "properties": {"center_lat": {"type": "number"}, "center_lng": {"type": "number"}, "bbox": {"type": "array"}, "markers": {"type": "array"}, "heatmap_points": {"type": "array"}, "paths": {"type": "array"}, "size": {"type": "string"}, "zoom": {"type": "integer"}}},
            self._tool_flx_static_map,
        )
        reg(
            "flx.ask_user",
            "Записує відкрите питання користувачу та паузить дослідження до відповіді.",
            {"type": "object", "properties": {"question": {"type": "string"}, "options": {"type": "array"}, "allow_freeform": {"type": "boolean"}}, "required": ["question"]},
            self._tool_flx_ask_user,
        )
        reg(
            "flx.report_compose",
            "Кладе попередньо складений structured-report у сесію (для пізнішого рендеру у HTML).",
            {"type": "object", "properties": {"sections": {"type": "array"}, "title": {"type": "string"}, "executive_summary": {"type": "string"}, "warnings": {"type": "array"}, "sources": {"type": "array"}}, "required": ["sections"]},
            self._tool_flx_report_compose,
        )
        reg(
            "flx.web_search",
            "Веб-пошук (DuckDuckGo) для доповнення даних, коли БД не вистачає.",
            {"type": "object", "properties": {"query": {"type": "string"}, "top_k": {"type": "integer"}}, "required": ["query"]},
            self._tool_flx_web_search,
        )
        reg(
            "flx.targeted_source_search",
            "Ініціює таргетний пошук/оновлення в джерелах OLX/Prozorro через pipeline (queue або sync fallback).",
            {
                "type": "object",
                "properties": {
                    "query_text": {"type": "string"},
                    "source": {"type": "string", "enum": ["olx", "prozorro", "both"]},
                    "days": {"type": "integer"},
                    "regions": {"type": "array"},
                    "listing_types": {"type": "array"},
                },
            },
            self._tool_flx_targeted_source_search,
        )
        # ---- vector (semantic search через Qdrant) ----
        # ---- listings (fallback search) ----
        reg(
            "listings.find_with_fallback",
            (
                "Пошук unified_listings з автоматичним розширенням фільтрів. Стратегії: "
                "exact → fuzzy_type (property_type як substring) → relaxed_area (без area-bounds) → "
                "region_wide (без city) → without_tags → status_any → country_wide (без region — для ринкового орієнтиру) → "
                "semantic (через Qdrant). "
                "ВАЖЛИВО: у БД `property_type` зберігається як «комерційна нерухомість», «земельна "
                "ділянка» тощо — повними фразами. Передавай property_type_contains='комерц' замість "
                "точного значення. Якщо все одно 0 — поверне landscape з реальними значеннями БД."
            ),
            {
                "type": "object",
                "properties": {
                    "query_text": {"type": "string"},
                    "region": {"type": "string"},
                    "city": {"type": "string"},
                    "property_type": {"type": "string"},
                    "property_type_contains": {"type": "string"},
                    "building_area_min": {"type": "number"},
                    "building_area_max": {"type": "number"},
                    "land_area_min": {"type": "number"},
                    "land_area_max": {"type": "number"},
                    "price_uah_min": {"type": "number"},
                    "price_uah_max": {"type": "number"},
                    "tags": {"type": "array", "items": {"type": "string"}},
                    "source": {"type": "string", "enum": ["olx", "prozorro"]},
                    "status": {"type": "string"},
                    "limit": {"type": "integer", "minimum": 1, "maximum": 200},
                },
            },
            self._tool_listings_find_with_fallback,
        )
        reg(
            "vector.semantic_search_listings",
            (
                "Семантичний пошук по unified_listings (OLX+ProZorro). Приймає природний запит "
                "('Київ під АЗС', 'комерційна нерухомість біля траси') та опційні логічні фільтри "
                "(region, city, property_type, price_uah_min/max, тощо). Повертає top-K з payload + score."
            ),
            {
                "type": "object",
                "properties": {
                    "query_text": {"type": "string"},
                    "top_k": {"type": "integer", "minimum": 1, "maximum": 50},
                    "filters": {
                        "type": "object",
                        "properties": {
                            "source": {"type": "string"},
                            "region": {"type": "string"},
                            "city": {"type": "string"},
                            "property_type": {"type": "string"},
                            "status": {"type": "string"},
                            "price_uah_min": {"type": "number"},
                            "price_uah_max": {"type": "number"},
                            "price_usd_min": {"type": "number"},
                            "price_usd_max": {"type": "number"},
                            "building_area_min": {"type": "number"},
                            "building_area_max": {"type": "number"},
                            "land_area_min": {"type": "number"},
                            "land_area_max": {"type": "number"},
                        },
                    },
                },
                "required": ["query_text"],
            },
            self._tool_vector_search_listings,
        )
        # ---- cadastral domain API (єдина обгортка) ----
        reg(
            "cadastral.get_knowledge",
            (
                "Доменний каталог КВЦПЗ + статистика реальних значень у БД. ОБОВ'ЯЗКОВИЙ "
                "перший крок будь-якої кадастрової роботи: повертає `classification.codes` "
                "(офіційні КВЦПЗ-коди), `classification.business_groups` (commercial / "
                "industrial / residential / agricultural / ...), а також `db_stats` — які з "
                "цих кодів РЕАЛЬНО присутні у `cadastral_parcels` (з лічильниками). Опційно "
                "обмеж scope (oblast) — тоді db_stats буде по конкретній області."
            ),
            {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "object",
                        "properties": {
                            "oblast_code": {"type": "string"},
                            "oblast_name": {"type": "string"},
                        },
                    },
                    "top_n_codes": {"type": "integer", "minimum": 1, "maximum": 200},
                },
            },
            self._tool_cadastral_get_knowledge,
        )
        reg(
            "cadastral.discover_in_area",
            (
                "Reconnaissance: краєвид кадастру у конкретній локації БЕЗ жодних фільтрів. "
                "Повертає `total_parcels`, розподіли `by_business_group` / `by_purpose` / "
                "`by_purpose_label_top` / `by_ownership_form` / `by_area_bucket` + статистику "
                "площ. Плюс `suggested_filters` — топ-3 групи з готовими `purpose_codes` для "
                "наступного виклику `cadastral.search`. Викликай це ПЕРЕД search, щоб бачити, "
                "що взагалі є у scope, а не вгадувати."
            ),
            {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "object",
                        "description": (
                            "Або (latitude, longitude, radius_meters) для радіального scope, "
                            "або (oblast_code|oblast_name) для пошуку по області."
                        ),
                        "properties": {
                            "latitude": {"type": "number"},
                            "longitude": {"type": "number"},
                            "radius_meters": {"type": "number", "minimum": 1, "maximum": 50000},
                            "oblast_code": {"type": "string"},
                            "oblast_name": {"type": "string"},
                        },
                    },
                    "limit": {"type": "integer", "minimum": 1, "maximum": 2000},
                },
                "required": ["scope"],
            },
            self._tool_cadastral_discover,
        )
        reg(
            "cadastral.search",
            (
                "Основний пошуковий інструмент. Приймає `scope` (координати+радіус або oblast) "
                "та `filters` високого рівня: business_groups (commercial/industrial/...), "
                "purpose_codes (точні КВЦПЗ), purpose_label_contains (substring), "
                "ownership_form[_contains], min/max_area_sqm, min_cluster_area_sqm, "
                "min_cluster_parcels, semantic_query (пере-ранжування через Qdrant). "
                "Повертає `clusters[]` + `single_parcels[]` з ПОВНИМ переліком `cadastral_numbers`, "
                "`total_area_sqm`, `centroid`, plus `landscape` (для контексту) і "
                "`note_when_empty` (якщо фільтри занадто строгі — підказка з landscape). "
                "Жодних сирих $geoWithin / regex не задавай — усе доменно."
            ),
            {
                "type": "object",
                "properties": {
                    "scope": {
                        "type": "object",
                        "properties": {
                            "latitude": {"type": "number"},
                            "longitude": {"type": "number"},
                            "radius_meters": {"type": "number", "minimum": 1, "maximum": 50000},
                            "oblast_code": {"type": "string"},
                            "oblast_name": {"type": "string"},
                        },
                    },
                    "filters": {
                        "type": "object",
                        "properties": {
                            "business_groups": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Імена бізнес-груп зі списку classification.business_groups",
                            },
                            "purpose_codes": {
                                "type": "array",
                                "items": {"type": "string"},
                                "description": "Точні КВЦПЗ-коди (напр. '03.07', '11.01')",
                            },
                            "purpose_label_contains": {"type": "string"},
                            "ownership_form": {"type": "string"},
                            "ownership_form_contains": {"type": "string"},
                            "min_area_sqm": {"type": "number"},
                            "max_area_sqm": {"type": "number"},
                            "min_cluster_area_sqm": {"type": "number"},
                            "min_cluster_parcels": {"type": "integer", "minimum": 1},
                            "semantic_query": {"type": "string"},
                        },
                    },
                    "output": {
                        "type": "object",
                        "properties": {
                            "top_n": {"type": "integer", "minimum": 1, "maximum": 100},
                            "include_singles": {"type": "boolean"},
                            "include_landscape": {"type": "boolean"},
                        },
                    },
                },
                "required": ["scope"],
            },
            self._tool_cadastral_search,
        )
        reg(
            "cadastral.list_parcels_in_region_polygon",
            (
                "Ділянки в межах топоніма (населений пункт / район / область як текст): межі через "
                "Google Geocoding + Places Details (viewport-полігон), потім Mongo $geoIntersects. "
                "Повертає query_id та preview; повний список — cadastral.list_polygon_query_page. "
                "boundary_kind зазвичай viewport_rectangle (наближено). Фільтри як у search: "
                "business_groups, purpose_codes, purpose_label_contains, ownership_form, min/max_area_sqm."
            ),
            {
                "type": "object",
                "properties": {
                    "toponym": {"type": "string", "description": "Назва НП, району, області України"},
                    "region": {"type": "string", "description": "Регіон геокоду (за замовч. ua)"},
                    "filters": {
                        "type": "object",
                        "properties": {
                            "business_groups": {"type": "array", "items": {"type": "string"}},
                            "purpose_codes": {"type": "array", "items": {"type": "string"}},
                            "purpose_label_contains": {"type": "string"},
                            "ownership_form": {"type": "string"},
                            "min_area_sqm": {"type": "number"},
                            "max_area_sqm": {"type": "number"},
                        },
                    },
                },
                "required": ["toponym"],
            },
            self._tool_cadastral_list_parcels_in_region_polygon,
        )
        reg(
            "cadastral.list_polygon_query_page",
            "Сторінка кадастрових номерів за query_id з кешу полігон-запиту (list_parcels_in_region_polygon).",
            {
                "type": "object",
                "properties": {
                    "query_id": {"type": "string"},
                    "page": {"type": "integer", "minimum": 0},
                    "page_size": {"type": "integer", "minimum": 1, "maximum": 2000},
                },
                "required": ["query_id"],
            },
            self._tool_cadastral_list_polygon_query_page,
        )
        reg(
            "cadastral.cluster_parcels",
            (
                "Кластеризує переданий список cadastral_number: суміжність / перетин / ≤5 м, "
                "за замовчуванням однаковий purpose + ownership. purpose_group_rules — список груп "
                "КВЦПЗ для спільного кластера; ignore_ownership_for_grouping — ігнорувати форму власності."
            ),
            {
                "type": "object",
                "properties": {
                    "cadastral_numbers": {
                        "type": "array",
                        "items": {"type": "string"},
                    },
                    "purpose_group_rules": {
                        "type": "array",
                        "items": {"type": "array", "items": {"type": "string"}},
                    },
                    "min_cluster_size": {"type": "integer", "minimum": 1},
                    "ignore_ownership_for_grouping": {"type": "boolean"},
                },
                "required": ["cadastral_numbers"],
            },
            self._tool_cadastral_cluster_parcels,
        )
        reg(
            "cadastral.get_cluster_meta",
            "Метадані збереженого кластера cadastral_parcel_clusters за cluster_id (без повного списку номерів).",
            {
                "type": "object",
                "properties": {
                    "cluster_id": {"type": "string"},
                    "cadastral_preview_limit": {"type": "integer", "minimum": 1, "maximum": 500},
                },
                "required": ["cluster_id"],
            },
            self._tool_cadastral_get_cluster_meta,
        )
        reg(
            "cadastral.list_cluster_parcels_page",
            "Пагінація кадастрових номерів із кластера в БД (cluster_id).",
            {
                "type": "object",
                "properties": {
                    "cluster_id": {"type": "string"},
                    "page": {"type": "integer", "minimum": 0},
                    "page_size": {"type": "integer", "minimum": 1, "maximum": 2000},
                },
                "required": ["cluster_id"],
            },
            self._tool_cadastral_list_cluster_parcels_page,
        )
        reg(
            "cadastral.get_parcel_summary",
            "Компактна картка ділянки за cadastral_number (без GeoJSON bounds).",
            {
                "type": "object",
                "properties": {"cadastral_number": {"type": "string"}},
                "required": ["cadastral_number"],
            },
            self._tool_cadastral_get_parcel_summary,
        )
        reg(
            "cadastral.get_parcel_full",
            "Повний документ ділянки з БД включно з bounds (важкий JSON — лише коли потрібна геометрія).",
            {
                "type": "object",
                "properties": {"cadastral_number": {"type": "string"}},
                "required": ["cadastral_number"],
            },
            self._tool_cadastral_get_parcel_full,
        )
        return registry

    # ----------------------------- tool wrappers --------------------------

    def _tool_qb_execute_query(self, query: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        from utils.query_builder import QueryBuilder
        return QueryBuilder().execute_query(query)

    def _tool_qb_execute_aggregation(self, collection: str, pipeline: List[Dict[str, Any]], **_: Any) -> Dict[str, Any]:
        from utils.query_builder import QueryBuilder
        return QueryBuilder().execute_aggregation(collection, pipeline)

    def _tool_qb_save_temp(self, query: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        # Делегуємо MCP-логіку: просто виконуємо запит та повертаємо temp_collection_id
        from utils.query_builder import QueryBuilder
        from data.repositories.agent_temp_exports_repository import AgentTempExportsRepository
        qb = QueryBuilder()
        result = qb.execute_query(query)
        if not result.get("success"):
            return result
        results = result.get("results") or []
        collection_name = (query or {}).get("collection") or ""
        if collection_name not in ("prozorro_auctions", "olx_listings", "unified_listings"):
            return {"success": False, "error": "save_query_to_temp_collection: unsupported collection"}
        temp_id = str(uuid.uuid4())
        count = AgentTempExportsRepository().insert_batch(temp_id, collection_name, results)
        return {"success": True, "temp_collection_id": temp_id, "count": count, "source_collection": collection_name}

    def _tool_qb_distinct(
        self,
        collection: str,
        field: Optional[str] = None,
        field_path: Optional[str] = None,
        unwrap_array: bool = False,
        **_: Any,
    ) -> Dict[str, Any]:
        from utils.query_builder import QueryBuilder
        path = field_path or field
        if not path:
            return {"success": False, "error": "field або field_path обов'язкові"}
        return QueryBuilder().get_distinct_values(collection, path, unwrap_array=unwrap_array)

    def _tool_analytics_execute(self, query: Dict[str, Any], **_: Any) -> Dict[str, Any]:
        from utils.analytics_builder import AnalyticsBuilder
        return AnalyticsBuilder().execute_analytics_query(query)

    def _tool_analytics_list_metrics(self, **_: Any) -> Dict[str, Any]:
        from utils.analytics_metrics import AnalyticsMetrics
        return {"success": True, "metrics": AnalyticsMetrics.list_metrics()}

    def _tool_schema_collection_info(self, collection_name: str, **_: Any) -> Dict[str, Any]:
        from utils.data_dictionary import DataDictionary
        dd = DataDictionary()
        info = dd.get_collection(collection_name)
        # CollectionDefinition може бути dataclass; серіалізуємо безпечно
        info_dict: Optional[Dict[str, Any]] = None
        if info is not None:
            try:
                if hasattr(info, "to_dict"):
                    info_dict = info.to_dict()
                elif hasattr(info, "__dict__"):
                    info_dict = {k: v for k, v in vars(info).items() if not k.startswith("_")}
                else:
                    info_dict = {"raw": str(info)}
            except Exception:
                info_dict = {"raw": str(info)}
        return {"success": info_dict is not None, "collection_name": collection_name, "info": info_dict}

    def _tool_schema_data_dictionary(self, **_: Any) -> Dict[str, Any]:
        from utils.data_dictionary import DataDictionary
        dd = DataDictionary()
        try:
            collections = dd.list_collections()
        except Exception:
            collections = []
        return {
            "success": True,
            "logical_fields": ["city", "region", "district", "price", "date", "status", "source", "property_type", "area"],
            "collections": collections,
        }

    def _tool_geocode(self, address_or_place: str, region: str = "ua", **_: Any) -> Dict[str, Any]:
        from business.services.geocoding_service import GeocodingService
        return GeocodingService(self.settings).geocode(query=address_or_place, region=region, caller="flx")

    def _tool_places_nearby(
        self,
        latitude: float,
        longitude: float,
        place_types: List[str],
        radius_meters: int = 500,
        **_: Any,
    ) -> Dict[str, Any]:
        from business.services.places_service import PlacesService
        return PlacesService(self.settings).search_nearby(
            latitude=float(latitude),
            longitude=float(longitude),
            place_types=list(place_types or []),
            radius_meters=int(radius_meters or 500),
        )

    def _tool_flx_note_write(self, session_id: str, kind: str, text: str, step_index: int = 0, **_: Any) -> Dict[str, Any]:
        doc = self._note_append(session_id=session_id, kind=kind, text=text, step_index=step_index)
        try:
            self._events.push(session_id=session_id, event_type="note", payload={
                "kind": doc.get("kind"), "text": doc.get("text"), "step_index": doc.get("step_index"), "seq": doc.get("seq"),
            })
        except Exception:
            pass
        return {"ok": True, "seq": doc.get("seq")}

    def _tool_flx_notes_read(self, session_id: str, top_k: int = 12, **_: Any) -> Dict[str, Any]:
        return {"ok": True, "summary": self._notes.summary(session_id, top_k=top_k)}

    def _tool_flx_lessons_search(self, query: str, top_k: int = 5, **_: Any) -> Dict[str, Any]:
        items = self._lessons.search_text(query=query, top_k=top_k)
        return {"ok": True, "items": items, "count": len(items)}

    def _tool_flx_lessons_save(
        self,
        topic_tags: List[str],
        query_pattern: str,
        what_worked: Optional[List[str]] = None,
        what_failed: Optional[List[str]] = None,
        recommendations: Optional[List[str]] = None,
        related_session_id: Optional[str] = None,
        __session_user_id: Optional[str] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        lid = self._lessons.add_lesson(
            user_id=__session_user_id,
            topic_tags=topic_tags or [],
            query_pattern=query_pattern,
            what_worked=what_worked or [],
            what_failed=what_failed or [],
            recommendations=recommendations or [],
            related_session_id=related_session_id,
        )
        return {"ok": True, "lesson_id": lid}

    def _tool_flx_static_map(
        self,
        session_id: str,
        center_lat: Optional[float] = None,
        center_lng: Optional[float] = None,
        bbox: Optional[List[float]] = None,
        markers: Optional[List[Dict[str, Any]]] = None,
        heatmap_points: Optional[List[Dict[str, Any]]] = None,
        paths: Optional[List[Dict[str, Any]]] = None,
        size: Optional[str] = None,
        zoom: Optional[int] = None,
        __session_user_id: Optional[str] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        center = (
            (float(center_lat), float(center_lng))
            if center_lat is not None and center_lng is not None
            else None
        )
        bbox_t = tuple(bbox) if isinstance(bbox, list) and len(bbox) == 4 else None
        result = StaticMapService(self.settings).render_to_artifact(
            user_id=__session_user_id,
            center=center,
            bbox=bbox_t,
            markers=markers or [],
            heatmap_points=heatmap_points or [],
            paths=paths or [],
            size=size,
            zoom=zoom,
        )
        if result.get("ok"):
            try:
                self._events.push(session_id=session_id, event_type="status", payload={"message": "Карту згенеровано", "artifact_id": result.get("artifact_id")})
            except Exception:
                pass
        return result

    def _tool_flx_ask_user(
        self,
        session_id: str,
        question: str,
        options: Optional[List[str]] = None,
        allow_freeform: bool = True,
        **_: Any,
    ) -> Dict[str, Any]:
        payload = {"question": str(question)[:1000], "options": list(options or [])[:6], "allow_freeform": bool(allow_freeform)}
        self._sessions.set_pending_question(session_id, payload)
        self._events.push(session_id=session_id, event_type="question", payload=payload)
        return {"ok": True, "state": "awaiting_user"}

    def _tool_flx_report_compose(
        self,
        session_id: str,
        sections: List[Dict[str, Any]],
        title: str = "",
        executive_summary: str = "",
        warnings: Optional[List[str]] = None,
        sources: Optional[List[str]] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        report = {
            "title": str(title or "")[:200] or "Звіт дослідження Flx",
            "executive_summary": str(executive_summary or "")[:2000],
            "sections": sections or [],
            "warnings": warnings or [],
            "sources": sources or [],
        }
        self._sessions.update_fields(session_id, {"report_structured": report})
        return {"ok": True, "ready_to_render": True}

    def _tool_flx_web_search(self, query: str, top_k: int = 5, **_: Any) -> Dict[str, Any]:
        return WebSearchService(self.settings).search(query=query, top_k=top_k)

    def _tool_flx_targeted_source_search(
        self,
        query_text: str = "",
        source: str = "both",
        days: int = 7,
        regions: Optional[List[str]] = None,
        listing_types: Optional[List[str]] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        src = str(source or "both").strip().lower()
        if src not in ("olx", "prozorro", "both"):
            src = "both"
        sources = ["olx", "prozorro"] if src == "both" else [src]
        regions_list = [str(r).strip() for r in (regions or []) if str(r).strip()]
        listing_types_list = [str(v).strip() for v in (listing_types or []) if str(v).strip()]
        safe_days = max(1, min(int(days or 7), 14))
        payload = {
            "sources": sources,
            "days": safe_days,
            "regions": regions_list or None,
            "listing_types": listing_types_list or None,
            "trigger": "flx_targeted_source_search",
            "query_text": str(query_text or "")[:300],
        }
        try:
            from business.services.task_queue_service import TaskQueueService

            tq = TaskQueueService(self.settings)
            if tq.is_enabled():
                dispatched = tq.enqueue_source_load(
                    days=safe_days,
                    sources=sources,
                    regions=regions_list or None,
                    listing_types=listing_types_list or None,
                    metadata=payload,
                )
                return {
                    "ok": True,
                    "mode": "queued",
                    "task_id": dispatched.get("task_id"),
                    "queue": dispatched.get("queue"),
                    "sources": sources,
                    "days": safe_days,
                    "regions": regions_list,
                    "message": "Таргетний пошук по джерелах ініційовано в черзі. Після оновлення даних повтори вибірку.",
                }
        except Exception as e:
            logger.warning("flx.targeted_source_search queue failed: %s", e)
        try:
            from business.services.source_data_load_service import run_full_pipeline

            result = run_full_pipeline(
                settings=self.settings,
                sources=sources,
                days=safe_days,
                regions=regions_list or None,
                listing_types=listing_types_list or None,
            )
            return {
                "ok": True,
                "mode": "sync",
                "sources": sources,
                "days": safe_days,
                "regions": regions_list,
                "result": result,
            }
        except Exception as e:
            return {"ok": False, "error": f"targeted_source_search_failed:{e}", "sources": sources}

    # ----------------------------- vector tools ---------------------------

    def _tool_vector_search_listings(
        self,
        query_text: str = "",
        top_k: int = 10,
        filters: Optional[Dict[str, Any]] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        text = str(query_text or "").strip()
        if not text:
            return {"ok": False, "error": "query_text is required"}
        try:
            from business.services.vector_index_service import VectorIndexService

            svc = VectorIndexService.get_instance(self.settings)
            if not svc.is_configured:
                return {"ok": False, "error": "vector_index_not_configured", "items": []}
            items = svc.search_listings(text, top_k=int(top_k or 10), filters=filters or {})
            return {"ok": True, "query_text": text, "items": items, "count": len(items)}
        except Exception as e:
            logger.warning("vector.semantic_search_listings failed: %s", e)
            return {"ok": False, "error": str(e), "items": []}

    # ----------------------------- listings (fallback search) ------------

    def _tool_listings_find_with_fallback(
        self,
        query_text: Optional[str] = None,
        region: Optional[str] = None,
        city: Optional[str] = None,
        property_type: Optional[str] = None,
        property_type_contains: Optional[str] = None,
        building_area_min: Optional[float] = None,
        building_area_max: Optional[float] = None,
        land_area_min: Optional[float] = None,
        land_area_max: Optional[float] = None,
        price_uah_max: Optional[float] = None,
        price_uah_min: Optional[float] = None,
        tags: Optional[List[str]] = None,
        source: Optional[str] = None,
        status: Optional[str] = "активне",
        limit: int = 50,
        **_: Any,
    ) -> Dict[str, Any]:
        """
        Багатоступеневий пошук оголошень у unified_listings з автоматичним
        розширенням фільтрів. Стратегії (по черзі):

            1) exact          — усі надані фільтри (точне співпадіння property_type);
            2) fuzzy_type     — property_type через substring (case-insensitive),
               status зберігається;
            3) relaxed_area   — без area-bounds (building/land);
            4) region_wide    — без city (тільки область) і без area-bounds;
            5) without_tags   — без tags + без city + без area-bounds;
            6) status_any     — додатково знімаємо status;
            7) semantic       — Qdrant semantic_search_listings із seed-запитом
               з полів (property_type, city, region, tags, query_text).

        Якщо нічого не знайдено — повертає `landscape` (count по property_type/
        region/city/source у межах region/status), щоб агент бачив фактичні
        значення в БД і міг скорегувати наступний виклик.
        """
        from data.repositories.unified_listings_repository import UnifiedListingsRepository

        repo = UnifiedListingsRepository()

        original: Dict[str, Any] = {
            "status": status,
            "region": region,
            "city": city,
            "property_type": property_type,
            "property_type_contains": property_type_contains,
            "building_area_min": building_area_min,
            "building_area_max": building_area_max,
            "land_area_min": land_area_min,
            "land_area_max": land_area_max,
            "price_uah_min": price_uah_min,
            "price_uah_max": price_uah_max,
            "tags": tags or None,
            "source": source,
        }

        # Якщо `property_type_contains` не передано, але є `property_type`,
        # автоматично використовуємо його як substring у fuzzy-стратегії.
        auto_pt_contains = property_type_contains or property_type or None

        strategies: List[Dict[str, Any]] = [
            {"name": "exact", "filters": dict(original)},
            {
                "name": "fuzzy_type",
                "filters": {**original, "property_type": None, "property_type_contains": auto_pt_contains},
            },
            {
                "name": "relaxed_area",
                "filters": {
                    **original, "property_type": None, "property_type_contains": auto_pt_contains,
                    "building_area_min": None, "building_area_max": None,
                    "land_area_min": None, "land_area_max": None,
                },
            },
            {
                "name": "region_wide",
                "filters": {
                    **original, "city": None, "property_type": None, "property_type_contains": auto_pt_contains,
                    "building_area_min": None, "building_area_max": None,
                    "land_area_min": None, "land_area_max": None,
                },
            },
            {
                "name": "without_tags",
                "filters": {
                    **original, "city": None, "tags": None,
                    "property_type": None, "property_type_contains": auto_pt_contains,
                    "building_area_min": None, "building_area_max": None,
                    "land_area_min": None, "land_area_max": None,
                },
            },
            {
                "name": "status_any",
                "filters": {
                    **original, "status": None, "city": None, "tags": None,
                    "property_type": None, "property_type_contains": auto_pt_contains,
                    "building_area_min": None, "building_area_max": None,
                    "land_area_min": None, "land_area_max": None,
                },
            },
            # Ескалація для оцінки цінової політики: якщо в обраному регіоні
            # нічого нема — дивимося ширше (всі регіони України), залишивши тип
            # нерухомості та статус. Це не «локальні» результати, але дають
            # ринковий орієнтир для розділу «Цінова мапа».
            {
                "name": "country_wide",
                "filters": {
                    **original, "region": None, "city": None, "tags": None,
                    "property_type": None, "property_type_contains": auto_pt_contains,
                    "building_area_min": None, "building_area_max": None,
                    "land_area_min": None, "land_area_max": None,
                },
            },
        ]

        def _clean(d: Dict[str, Any]) -> Dict[str, Any]:
            return {k: v for k, v in d.items() if v not in (None, "", [])}

        attempts: List[Dict[str, Any]] = []
        for strat in strategies:
            kwargs = {k: v for k, v in strat["filters"].items() if v not in (None, "", [])}
            try:
                items = repo.find_listings(limit=int(limit or 50), **kwargs)
            except Exception as e:
                attempts.append({"strategy": strat["name"], "error": str(e), "count": 0})
                continue
            count = len(items)
            attempts.append({"strategy": strat["name"], "count": count, "filters_used": kwargs})
            if count > 0:
                return {
                    "ok": True,
                    "applied_strategy": strat["name"],
                    "applied_filters": kwargs,
                    "original_filters": _clean(original),
                    "count": count,
                    "items": items,
                    "attempts": attempts,
                }

        sem_seed: List[str] = []
        if query_text:
            sem_seed.append(str(query_text).strip())
        for v in (property_type, property_type_contains, city, region):
            if v:
                sem_seed.append(str(v))
        for t in (tags or []):
            sem_seed.append(str(t))
        sem_query = " ".join([p for p in sem_seed if p]).strip()
        sem_filters: Dict[str, Any] = _clean({
            "region": region, "city": city, "source": source, "status": status,
            "price_uah_min": price_uah_min, "price_uah_max": price_uah_max,
            "building_area_min": building_area_min, "building_area_max": building_area_max,
            "land_area_min": land_area_min, "land_area_max": land_area_max,
        })
        if sem_query:
            try:
                from business.services.vector_index_service import VectorIndexService

                svc = VectorIndexService.get_instance(self.settings)
                if svc.is_configured:
                    sem_items = svc.search_listings(sem_query, top_k=int(limit or 50), filters=sem_filters)
                    attempts.append({"strategy": "semantic", "count": len(sem_items),
                                     "query_text": sem_query, "filters_used": sem_filters})
                    if sem_items:
                        return {
                            "ok": True,
                            "applied_strategy": "semantic",
                            "applied_filters": sem_filters,
                            "original_filters": _clean(original),
                            "count": len(sem_items),
                            "items": sem_items,
                            "attempts": attempts,
                            "note": "Знайдено через семантичний пошук — точні фільтри не дали результату.",
                        }
                else:
                    attempts.append({"strategy": "semantic", "skipped": "vector_index_not_configured"})
            except Exception as e:
                attempts.append({"strategy": "semantic", "error": str(e), "count": 0})

        try:
            landscape = repo.landscape_summary(
                region=region,
                status=None,  # без статусу — щоб побачити справжній обсяг
                source=source,
                max_per_field=15,
            )
        except Exception as e:
            landscape = {"error": str(e)}

        return {
            "ok": True,
            "applied_strategy": "none",
            "applied_filters": {},
            "original_filters": _clean(original),
            "count": 0,
            "items": [],
            "attempts": attempts,
            "landscape": landscape,
            "note": (
                "Жодна стратегія (exact → fuzzy_type → relaxed_area → region_wide → without_tags → "
                "status_any → semantic) не дала результатів. Подивись landscape: які property_type / "
                "region / city реально є у БД для цього region/source — повтори виклик зі скоригованим "
                "значенням (можливо, property_type_contains=частина фрази). Якщо потрібні саме свіжі дані — "
                "викликай flx.targeted_source_search для оновлення джерел."
            ),
        }

    # ----------------------------- cadastral domain tools ----------------
    #
    # ВСЯ робота з кадастром — через CadastralDomainService. Сирі parcels,
    # raw $geoWithin, ручні regex-фільтри для агента закриті: він має лише
    # три тулзи (knowledge / discover_in_area / search). Це гарантує, що
    # LLM не вгадує неіснуючі коди й завжди отримує конкретні
    # cadastral_numbers + кластеризацію в одному виклику.

    def _cadastral_domain(self) -> Any:
        if getattr(self, "_cadastral_domain_cached", None) is None:
            from business.services.cadastral_domain_service import (
                CadastralDomainService,
            )
            self._cadastral_domain_cached = CadastralDomainService()
        return self._cadastral_domain_cached

    def _tool_cadastral_get_knowledge(
        self,
        scope: Optional[Dict[str, Any]] = None,
        top_n_codes: int = 50,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().get_knowledge(
                scope=scope, top_n_codes=int(top_n_codes or 50)
            )
        except Exception as e:
            logger.warning("cadastral.get_knowledge failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_discover(
        self,
        scope: Dict[str, Any],
        limit: int = 800,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().discover_in_area(
                scope=scope or {}, limit=int(limit or 800)
            )
        except Exception as e:
            logger.warning("cadastral.discover_in_area failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_search(
        self,
        scope: Dict[str, Any],
        filters: Optional[Dict[str, Any]] = None,
        output: Optional[Dict[str, Any]] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().search(
                scope=scope or {}, filters=filters or {}, output=output or {}
            )
        except Exception as e:
            logger.warning("cadastral.search failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_list_parcels_in_region_polygon(
        self,
        toponym: str,
        region: str = "ua",
        filters: Optional[Dict[str, Any]] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().list_parcels_in_region_polygon(
                toponym=toponym,
                filters=filters or {},
                region=region or "ua",
            )
        except Exception as e:
            logger.warning("cadastral.list_parcels_in_region_polygon failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_list_polygon_query_page(
        self,
        query_id: str,
        page: int = 0,
        page_size: Optional[int] = None,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().list_polygon_query_page(
                query_id=query_id,
                page=int(page or 0),
                page_size=page_size,
            )
        except Exception as e:
            logger.warning("cadastral.list_polygon_query_page failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_cluster_parcels(
        self,
        cadastral_numbers: List[str],
        purpose_group_rules: Optional[List[List[str]]] = None,
        min_cluster_size: int = 1,
        ignore_ownership_for_grouping: bool = False,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().cluster_parcels(
                cadastral_numbers=cadastral_numbers or [],
                purpose_group_rules=purpose_group_rules,
                min_cluster_size=int(min_cluster_size or 1),
                ignore_ownership_for_grouping=bool(ignore_ownership_for_grouping),
            )
        except Exception as e:
            logger.warning("cadastral.cluster_parcels failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_get_cluster_meta(
        self,
        cluster_id: str,
        cadastral_preview_limit: int = 50,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().get_cluster_meta(
                cluster_id=cluster_id,
                cadastral_preview_limit=int(cadastral_preview_limit or 50),
            )
        except Exception as e:
            logger.warning("cadastral.get_cluster_meta failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_list_cluster_parcels_page(
        self,
        cluster_id: str,
        page: int = 0,
        page_size: int = 100,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().list_cluster_parcels_page(
                cluster_id=cluster_id,
                page=int(page or 0),
                page_size=int(page_size or 100),
            )
        except Exception as e:
            logger.warning("cadastral.list_cluster_parcels_page failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_get_parcel_summary(
        self,
        cadastral_number: str,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().get_parcel_summary(cadastral_number=cadastral_number)
        except Exception as e:
            logger.warning("cadastral.get_parcel_summary failed: %s", e)
            return {"ok": False, "error": str(e)}

    def _tool_cadastral_get_parcel_full(
        self,
        cadastral_number: str,
        **_: Any,
    ) -> Dict[str, Any]:
        try:
            return self._cadastral_domain().get_parcel_full(cadastral_number=cadastral_number)
        except Exception as e:
            logger.warning("cadastral.get_parcel_full failed: %s", e)
            return {"ok": False, "error": str(e)}

    # ----------------------------- legacy cadastral (видалено) ------------
    #
    # _tool_cadastral_find_parcels_near, _tool_cadastral_find_clusters_near,
    # _tool_cadastral_find_by_oblast, _tool_cadastral_analyze_near,
    # _tool_cadastral_cluster_in_radius, _tool_vector_search_cadastre — усі
    # видалено разом із FLX-тулзами. Працювати з кадастром можна лише через
    # CadastralDomainService (cadastral.get_knowledge / discover_in_area / search
    # та полігон/cluster/parcel тулзи). Старі функції залишилися б мертвим кодом, який LLM міг
    # «знайти» через подібні імена — кращe прибрати.

    # ----------------------------- helpers --------------------------------

    def _note_append(
        self,
        session_id: str,
        *,
        kind: str,
        text: str,
        step_index: int = 0,
        tool_call_summary: Optional[Dict[str, Any]] = None,
        payload: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        sess = self._sessions.get_by_session_id(session_id) or {}
        kw: Dict[str, Any] = {
            "session_id": session_id,
            "kind": kind,
            "text": text,
            "step_index": step_index,
            "tool_call_summary": tool_call_summary,
        }
        sid = self._current_strategy_id(sess)
        if sid:
            kw["strategy_id"] = sid
        if payload is not None:
            kw["payload"] = payload
        return self._notes.append(**kw)

    @staticmethod
    def _current_strategy_candidate(sess: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        if not sess.get("strategy_engine_active"):
            return None
        plan = sess.get("plan") or {}
        cands = plan.get("strategy_candidates") or []
        idx = int(sess.get("current_strategy_index") or 0)
        if 0 <= idx < len(cands) and isinstance(cands[idx], dict):
            return cands[idx]
        return None

    @staticmethod
    def _current_strategy_id(sess: Dict[str, Any]) -> Optional[str]:
        c = InvestigationService._current_strategy_candidate(sess)
        if not c:
            return None
        sid = str(c.get("strategy_id") or "").strip()
        return sid or None

    def _flx_on_strategy_steps_exhausted(self, session_id: str, sess: Dict[str, Any]) -> str:
        """Після завершення кроків поточної стратегії: накопичити результат, перейти далі або завершити."""
        plan = sess.get("plan") or {}
        candidates = plan.get("strategy_candidates") or []
        idx = int(sess.get("current_strategy_index") or 0)
        if not candidates or idx >= len(candidates):
            return "noop"

        sess = self._sessions.get_by_session_id(session_id) or sess
        candidates = (sess.get("plan") or {}).get("strategy_candidates") or candidates
        idx = int(sess.get("current_strategy_index") or idx)

        cand = candidates[idx]
        cid = str(cand.get("strategy_id") or f"s{idx}")
        notes_all = self._notes.list_for_session(session_id, limit=400)
        notes_filtered = [n for n in notes_all if (n.get("strategy_id") or "") == cid]

        run = self._strategy_eval.build_run_result_from_notes(
            strategy_id=cid,
            name=str(cand.get("name") or "")[:300],
            hypothesis=str(cand.get("hypothesis") or "")[:800],
            notes=notes_filtered,
            expected_signals=list(cand.get("expected_signals") or []) if isinstance(cand.get("expected_signals"), list) else [],
            planned_tools=list(cand.get("planned_tools") or []) if isinstance(cand.get("planned_tools"), list) else [],
        )
        results = list(sess.get("strategy_run_results") or [])
        results.append(run.to_dict())
        self._sessions.update_fields(session_id, {"strategy_run_results": results})

        next_idx = idx + 1
        if next_idx >= len(candidates):
            run_objs = [StrategyRunResult.from_dict(r) for r in results]
            ranked = self._strategy_eval.rank_runs(run_objs)
            winner_id = ranked[0].strategy_id if ranked else None
            comparison: Dict[str, Any] = {
                "winner_strategy_id": winner_id,
                "ranked": [
                    {
                        "strategy_id": r.strategy_id,
                        "name": r.name,
                        "score": round(r.score, 4),
                        "score_breakdown": r.score_breakdown,
                        "evidence_items": r.evidence_items,
                        "failures": r.failures,
                    }
                    for r in ranked
                ],
                "strategies_evaluated": len(ranked),
            }
            self._sessions.update_fields(
                session_id,
                {
                    "strategy_comparison": comparison,
                    "strategy_engine_active": False,
                    "current_strategy_index": next_idx,
                },
            )
            user_q = str(sess.get("query") or "")
            cand_by_id = {str(c.get("strategy_id") or ""): c for c in candidates if isinstance(c, dict)}
            payloads: List[Dict[str, Any]] = []
            for r in ranked:
                cmeta = cand_by_id.get(r.strategy_id) or {}
                merged = {**cmeta, "strategy_id": r.strategy_id}
                payloads.append(
                    {
                        "strategy_id": r.strategy_id,
                        "strategy_signature": strategy_signature_from_candidate(merged),
                        "name": r.name,
                        "hypothesis": r.hypothesis,
                        "score": r.score,
                        "tool_calls": r.tool_calls,
                        "planned_tools": list(cmeta.get("planned_tools") or []),
                        "domain_tags": list(cmeta.get("domain_tags") or []) if isinstance(cmeta.get("domain_tags"), list) else [],
                    }
                )
            try:
                self._strategy_registry.record_session_outcome(
                    user_query=user_q,
                    ranked_results=payloads,
                    winner_strategy_id=winner_id,
                )
            except Exception as e:
                logger.warning("Strategy registry update failed: %s", e)
            logger.info(
                "[flx-strategy] session=%s winner=%s scores=%s",
                session_id,
                winner_id,
                [round(x.score, 3) for x in ranked],
            )
            return "done"

        next_cand = candidates[next_idx]
        new_plan = dict(plan)
        new_plan["steps"] = list(next_cand.get("steps") or [])
        self._sessions.update_fields(
            session_id,
            {
                "plan": new_plan,
                "step_index": 0,
                "current_strategy_index": next_idx,
            },
        )
        self._note_append(
            session_id=session_id,
            kind="decision",
            text=(
                f"Перехід до стратегії «{next_cand.get('name') or next_idx}» ({next_cand.get('strategy_id')}). "
                f"Гіпотеза: {(next_cand.get('hypothesis') or '')[:240]}"
            ),
            step_index=0,
        )
        self._events.push(
            session_id=session_id,
            event_type="status",
            payload={
                "phase": "strategy_switch",
                "strategy_id": next_cand.get("strategy_id"),
                "index": next_idx,
            },
        )
        return "advanced"

    def _fetch_relevant_lessons(self, query: str, user_id: Optional[str] = None, top_k: int = 5) -> List[Dict[str, Any]]:
        try:
            return self._lessons.search_text(query=query, user_id=user_id, top_k=top_k) or []
        except Exception as e:
            logger.debug("Lessons search failed: %s", e)
            return []

    def _source_wait_timed_out(self, wait: Dict[str, Any]) -> bool:
        started = str((wait or {}).get("started_at") or "").strip()
        if not started:
            return False
        try:
            s = started.replace("Z", "+00:00")
            st = datetime.fromisoformat(s)
            if st.tzinfo is None:
                st = st.replace(tzinfo=timezone.utc)
        except Exception:
            return False
        limit_sec = max(60, int(getattr(self.settings, "flx_source_wait_timeout_seconds", 3600) or 3600))
        if datetime.now(timezone.utc) - st > timedelta(seconds=limit_sec):
            return True
        return False

    def _check_source_wait(self, session_id: str, sess: Dict[str, Any]) -> Dict[str, Any]:
        wait = (sess or {}).get("pending_source_wait") or {}
        task_id = str(wait.get("task_id") or "")
        if not task_id:
            self._sessions.update_fields(session_id, {"state": "running", "pending_source_wait": None})
            return {"ok": True, "state": "running", "note": "source_wait_missing_task_id"}
        if self._source_wait_timed_out(wait):
            self._sessions.update_fields(
                session_id,
                {
                    "state": "running",
                    "pending_source_wait": None,
                },
            )
            self._events.push(
                session_id=session_id,
                event_type="status",
                payload={
                    "phase": "source_wait_timeout",
                    "message": "Таймаут очікування оновлення джерел. Продовжую з наявними даними.",
                    "task_id": task_id,
                },
            )
            logger.warning("[flx-think] source_wait.timeout session=%s task_id=%s", session_id, task_id)
            self._dispatch(session_id)
            return {"ok": True, "state": "running", "note": "source_wait_timeout"}
        try:
            from business.services.task_queue_service import TaskQueueService

            status = TaskQueueService(self.settings).get_task_status(task_id)
            state = str(status.get("state") or "").lower()
            if state in ("queued", "received", "started", "running", "retry", "pending"):
                logger.info(
                    "[flx-think] source_wait.poll session=%s task_id=%s state=%s expected=wait",
                    session_id,
                    task_id,
                    state,
                )
                return {"ok": True, "state": "awaiting_sources", "note": f"source_wait:{state}"}
            if state in ("success",):
                self._sessions.update_fields(
                    session_id,
                    {
                        "state": "running",
                        "pending_source_wait": None,
                    },
                )
                self._events.push(
                    session_id=session_id,
                    event_type="status",
                    payload={
                        "phase": "source_refresh_done",
                        "message": "Таргетний пошук в джерелах завершено. Продовжую аналіз на оновлених даних.",
                        "task_id": task_id,
                    },
                )
                logger.info(
                    "[flx-think] source_wait.done session=%s task_id=%s next=resume_analysis",
                    session_id,
                    task_id,
                )
                return {"ok": True, "state": "running", "note": "source_wait_done"}
            self._sessions.update_fields(
                session_id,
                {
                    "state": "running",
                    "pending_source_wait": None,
                },
            )
            self._events.push(
                session_id=session_id,
                event_type="status",
                payload={
                    "phase": "source_refresh_failed",
                    "message": "Таргетний пошук в джерелах завершився помилкою. Продовжую з наявними даними.",
                    "task_id": task_id,
                    "state": state,
                },
            )
            logger.warning("[flx-think] source_wait.failed session=%s task_id=%s state=%s", session_id, task_id, state)
            return {"ok": True, "state": "running", "note": f"source_wait_failed:{state}"}
        except Exception as e:
            logger.warning("[flx-think] source_wait.poll_error session=%s task_id=%s error=%s", session_id, task_id, e)
            return {"ok": True, "state": "awaiting_sources", "note": "source_wait_poll_error"}

    def _has_recent_tool_usage(self, session_id: str, tool_name: str, recent_notes: int = 24) -> bool:
        notes = self._notes.list_latest_for_session(session_id, limit=max(1, recent_notes))
        for n in notes:
            tcs = n.get("tool_call_summary") or {}
            if isinstance(tcs, dict) and str(tcs.get("name") or "") == str(tool_name):
                return True
        return False

    def _has_recent_tool_usage_any(
        self, session_id: str, tool_names: Tuple[str, ...], recent_notes: int = 24
    ) -> bool:
        names = frozenset(str(x) for x in tool_names if str(x).strip())
        if not names:
            return False
        notes = self._notes.list_latest_for_session(session_id, limit=max(1, recent_notes))
        for n in notes:
            tcs = n.get("tool_call_summary") or {}
            if isinstance(tcs, dict) and str(tcs.get("name") or "") in names:
                return True
        return False

    def _enrich_targeted_source_search_args(self, args: Dict[str, Any], session_query: str) -> Dict[str, Any]:
        """
        Нормалізує/доповнює args для flx.targeted_source_search.
        Якщо LLM не передав regions/listing_types/source/days, намагаємось витягнути
        значення з тексту запиту користувача, щоб уникати загального (не таргетного) оновлення.
        """
        safe_args = dict(args or {})
        query_text = str(session_query or "")
        existing_query_text = str(safe_args.get("query_text") or "").strip()
        if not existing_query_text and query_text:
            safe_args["query_text"] = query_text

        source_raw = str(safe_args.get("source") or "").strip().lower()
        if source_raw not in ("olx", "prozorro", "both"):
            q = query_text.lower()
            if "olx" in q and "prozorro" not in q:
                safe_args["source"] = "olx"
            elif "prozorro" in q and "olx" not in q:
                safe_args["source"] = "prozorro"
            else:
                safe_args["source"] = "both"

        if not isinstance(safe_args.get("days"), int):
            safe_args["days"] = 7

        provided_regions = safe_args.get("regions")
        provided_listing_types = safe_args.get("listing_types")
        if (
            isinstance(provided_regions, list) and provided_regions
            and isinstance(provided_listing_types, list) and provided_listing_types
        ):
            return safe_args

        try:
            from business.services.source_data_load_service import get_targeted_update_options
            options = get_targeted_update_options() or {}
        except Exception:
            options = {}

        if not (isinstance(provided_regions, list) and provided_regions):
            regions_found: List[str] = []
            region_names = [str(r).strip() for r in (options.get("regions") or []) if str(r).strip()]
            query_lower = query_text.lower()
            for region_name in region_names:
                if region_name.lower() in query_lower:
                    regions_found.append(region_name)
            if regions_found:
                safe_args["regions"] = regions_found

        if not (isinstance(provided_listing_types, list) and provided_listing_types):
            listing_types_found: List[str] = []
            listing_type_names = [str(v).strip() for v in (options.get("olx_listing_types") or []) if str(v).strip()]
            query_lower = query_text.lower()
            for listing_type in listing_type_names:
                if listing_type.lower() in query_lower:
                    listing_types_found.append(listing_type)
            if not listing_types_found:
                # Часті узагальнення у запитах користувачів.
                if "нежитлов" in query_lower:
                    listing_types_found.append("Нежитлова нерухомість")
                if "земл" in query_lower:
                    listing_types_found.extend([v for v in listing_type_names if v.lower().startswith("земля")])
            if listing_types_found:
                safe_args["listing_types"] = list(dict.fromkeys(listing_types_found))

        return safe_args

    def _detect_repetitive_query_loop(self, session_id: str) -> bool:
        notes = self._notes.list_latest_for_session(session_id, limit=40, kinds=["observation"])
        sigs: List[str] = []
        for n in notes:
            tcs = n.get("tool_call_summary") or {}
            if not isinstance(tcs, dict):
                continue
            if str(tcs.get("name") or "") != "query_builder.execute_query":
                continue
            text = str(n.get("text") or "")
            head = text[:500]
            sig = hashlib.sha1(head.encode("utf-8", errors="ignore")).hexdigest()
            sigs.append(sig)
            if len(sigs) >= 12:
                break
        if len(sigs) < 3:
            return False
        newest_three = sigs[:3]
        return len(set(newest_three)) == 1

    def _init_research_state(self, plan: Dict[str, Any]) -> Dict[str, Any]:
        tasks = self._seed_tasks_from_plan(plan)
        budget = ResearchBudget(
            web_searches=int(getattr(self.settings, "flx_budget_web_searches", 40)),
            deep_analysis=int(getattr(self.settings, "flx_budget_deep_analysis", 12)),
            api_calls=int(getattr(self.settings, "flx_budget_api_calls", 500)),
            critic_iterations=int(getattr(self.settings, "flx_budget_critic_iterations", 6)),
        )
        return {
            "research_tasks": [t.to_dict() for t in tasks],
            "research_coverage": self._coverage.empty_snapshot(),
            "research_budget": budget.to_dict(),
            "research_budget_initial": budget.to_dict(),
            "research_branches": sorted({t.branch for t in tasks}),
            "critic_iterations": 0,
            "contrarian_done": False,
            "unknown_discovery_done": False,
            "research_started_at": datetime.now(timezone.utc).isoformat(),
        }

    def _seed_tasks_from_plan(self, plan: Dict[str, Any]) -> List[ResearchTask]:
        tasks: List[ResearchTask] = []
        steps = list((plan or {}).get("steps") or [])
        for idx, step in enumerate(steps, start=1):
            goal = str((step or {}).get("goal") or f"Крок {idx}")
            branch = self._infer_branch(goal)
            tasks.append(
                ResearchTask(
                    task_id=f"task_{idx}",
                    goal=goal[:500],
                    branch=branch,
                    priority=max(0.2, 1.0 - idx * 0.08),
                    dependencies=[],
                    status="pending",
                    assigned_role=self._infer_role(branch),
                    dedup_key=goal.lower()[:120],
                )
            )
        return tasks

    @staticmethod
    def _infer_branch(goal: str) -> str:
        text = (goal or "").lower()
        if any(k in text for k in ("крим", "crime", "ризик", "legal", "юрид")):
            return "risk"
        if any(k in text for k in ("транспорт", "район", "інфра", "еколог", "школ")):
            return "area"
        if any(k in text for k in ("roi", "окуп", "оренд", "ліквід")):
            return "investment"
        return "market"

    @staticmethod
    def _infer_role(branch: str) -> str:
        mapping = {
            "risk": "RiskAnalyst",
            "area": "GeoAnalyst",
            "investment": "ValuationAgent",
            "market": "TrendAgent",
        }
        return mapping.get(branch, "Researcher")

    def _select_next_task_step(self, sess: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        tasks_raw = list(sess.get("research_tasks") or [])
        if not tasks_raw:
            return None
        coverage = dict(sess.get("research_coverage") or {})
        candidates: List[ResearchTask] = []
        for t in tasks_raw:
            task = ResearchTask.from_dict(t)
            if task.status not in ("pending", "blocked"):
                continue
            if task.dependencies and any(
                not any((x.get("task_id") == dep and x.get("status") == "done") for x in tasks_raw) for dep in task.dependencies
            ):
                continue
            aspect = self._aspect_for_branch(task.branch)
            aspect_score = float((coverage.get(aspect) or {}).get("score") or 0.0)
            breakdown = self._priority.score_map(task, aspect_score=aspect_score, contradictions=int((coverage.get(aspect) or {}).get("contradiction_count") or 0))
            task.priority = float(breakdown.get("priority") or task.priority)
            candidates.append(task)
        if not candidates:
            return None
        candidates.sort(key=lambda x: x.priority, reverse=True)
        top = candidates[0]

        # Інжектуємо контекст дослідження (objective + user_query) у goal/success_criteria.
        # Без цього step-агент бачить абстрактний `top.goal` (наприклад «Дослідити аспект:
        # demographics») і починає галюцинувати локацію — Київ/Львів/Одесу замість Турійська.
        objective = str(((sess.get("plan") or {}).get("objective")) or "").strip()
        original_query = str(sess.get("query") or "").strip()
        goal = (top.goal or "").strip()
        if objective and "дослідженн" not in goal.lower():
            goal = f"{goal} — у рамках мети дослідження: «{objective[:300]}»"
        elif original_query and len(goal) < 200:
            goal = f"{goal} — у рамках запиту: «{original_query[:200]}»"
        success = (
            "Є докази з принаймні 2 джерел або зафіксований блокер. "
            "Усі результати ОБОВ'ЯЗКОВО стосуються локації/предмета з мети дослідження — "
            "не перемикайся на інші міста/області/типи нерухомості."
        )
        return {
            "step_id": int(sess.get("step_index") or 0) + 1,
            "goal": goal[:600],
            "candidate_tools": [],
            "success_criteria": success,
            "task_id": top.task_id,
            "branch": top.branch,
            "assigned_role": top.assigned_role,
        }

    @staticmethod
    def _aspect_for_branch(branch: str) -> str:
        mapping = {
            "risk": "crime",
            "area": "infrastructure",
            "investment": "rental_yield",
            "market": "pricing",
        }
        return mapping.get(branch, "pricing")

    def _mark_active_task(self, session_id: str, step: Dict[str, Any], status: str) -> None:
        task_id = str(step.get("task_id") or "").strip()
        if not task_id:
            return
        sess = self._sessions.get_by_session_id(session_id) or {}
        tasks = list(sess.get("research_tasks") or [])
        changed = False
        for idx, raw in enumerate(tasks):
            if str(raw.get("task_id") or "") != task_id:
                continue
            raw = dict(raw)
            raw["status"] = status
            tasks[idx] = raw
            changed = True
            break
        if changed:
            self._sessions.update_fields(session_id, {"research_tasks": tasks})

    def _should_stop_research(self, sess: Dict[str, Any]) -> bool:
        coverage = dict(sess.get("research_coverage") or {})
        coverage_score = self._coverage.aggregate_score(coverage)
        threshold = float(getattr(self.settings, "flx_coverage_threshold", 0.75))
        contradictions = self._coverage.unresolved_contradictions(coverage)
        budget = dict(sess.get("research_budget") or {})
        budget_exhausted = all(int(budget.get(k) or 0) <= 0 for k in ("web_searches", "deep_analysis", "api_calls"))
        started = str(sess.get("research_started_at") or "")
        time_limit_sec = int(getattr(self.settings, "llm_investigator_time_budget_seconds", 900))
        time_exhausted = False
        if started:
            try:
                dt = datetime.fromisoformat(started.replace("Z", "+00:00"))
                time_exhausted = (datetime.now(timezone.utc) - dt).total_seconds() > time_limit_sec
            except Exception:
                time_exhausted = False
        return bool((coverage_score >= threshold and contradictions == 0) or budget_exhausted or time_exhausted)

    @staticmethod
    def _budget_bucket_for_tool(tool_name: str) -> str:
        if tool_name.startswith("flx.web_search"):
            return "web_searches"
        if tool_name.startswith("query_builder.") or tool_name.startswith("analytics.") or tool_name.startswith("schema.") or tool_name.startswith("geocoding."):
            return "api_calls"
        return "deep_analysis"

    def _consume_budget(self, session_id: str, bucket: str, amount: int = 1) -> None:
        sess = self._sessions.get_by_session_id(session_id) or {}
        budget = ResearchBudget(**dict(sess.get("research_budget") or {}))
        budget.consume(bucket, amount=amount)
        self._sessions.update_fields(session_id, {"research_budget": budget.to_dict()})

    def _update_coverage_from_observation(self, session_id: str, step: Dict[str, Any], text: str, source: str) -> None:
        aspect = self._aspect_for_branch(str(step.get("branch") or self._infer_branch(str(step.get("goal") or "")))
        )
        confidence = min(0.9, 0.35 + min(0.45, len(str(text or "")) / 2500.0))
        sess = self._sessions.get_by_session_id(session_id) or {}
        coverage = dict(sess.get("research_coverage") or self._coverage.empty_snapshot())
        coverage = self._coverage.update_aspect(
            coverage,
            aspect=aspect,
            confidence=confidence,
            evidence_count=1,
            source_diversity=1 if source else 0,
            contradiction_delta=-1 if "спрост" in str(text).lower() else 0,
        )
        self._sessions.update_fields(session_id, {"research_coverage": coverage})
        self._redis_memory.set_snapshot(session_id, {"coverage": coverage})
        self._pg_memory.store_fact(session_id, {"aspect": aspect, "confidence": confidence, "source": source})
        self._vector_memory.upsert_document(session_id=session_id, text=str(text), metadata={"aspect": aspect, "source": source})

    def _run_critic_loop(self, session_id: str) -> None:
        sess = self._sessions.get_by_session_id(session_id) or {}
        budget = dict(sess.get("research_budget") or {})
        if int(budget.get("critic_iterations") or 0) <= 0:
            return
        payload = self._critic.run(
            user_query=str(sess.get("query") or ""),
            notes_summary=self._notes.summary(session_id, top_k=16),
            coverage_snapshot=self._serialize_observation(sess.get("research_coverage") or {}),
            evidence_graph_snapshot=self._serialize_observation(sess.get("evidence_graph") or {}),
        )
        self._consume_budget(session_id, "critic_iterations", 1)
        self._sessions.update_fields(session_id, {"critic_iterations": int(sess.get("critic_iterations") or 0) + 1})
        self._append_tasks_from_agent(session_id, payload.get("new_tasks") or [])

    def _run_contrarian_once(self, session_id: str) -> None:
        sess = self._sessions.get_by_session_id(session_id) or {}
        if sess.get("contrarian_done"):
            return
        payload = self._contrarian.run(
            user_query=str(sess.get("query") or ""),
            current_conclusions=self._notes.summary(session_id, top_k=12),
            coverage_snapshot=self._serialize_observation(sess.get("research_coverage") or {}),
        )
        self._append_tasks_from_agent(session_id, payload.get("new_tasks") or [])
        self._sessions.update_fields(session_id, {"contrarian_done": True})

    def _append_tasks_from_agent(self, session_id: str, tasks_payload: List[Dict[str, Any]]) -> None:
        if not isinstance(tasks_payload, list) or not tasks_payload:
            return
        sess = self._sessions.get_by_session_id(session_id) or {}
        existing = list(sess.get("research_tasks") or [])
        dedup = {str(t.get("dedup_key") or t.get("goal") or "").strip().lower() for t in existing}
        next_idx = len(existing) + 1
        for item in tasks_payload[:10]:
            if not isinstance(item, dict):
                continue
            goal = str(item.get("goal") or "").strip()
            if not goal:
                continue
            key = goal.lower()[:120]
            if key in dedup:
                continue
            dedup.add(key)
            task = ResearchTask(
                task_id=f"task_{next_idx}",
                goal=goal[:500],
                branch=str(item.get("branch") or self._infer_branch(goal)),
                priority=float(item.get("priority_hint") or 0.6),
                status="pending",
                confidence=0.0,
                dedup_key=key,
                assigned_role=str(item.get("assigned_role") or "Researcher"),
            )
            existing.append(task.to_dict())
            next_idx += 1
        self._sessions.update_fields(session_id, {"research_tasks": existing})

    def _run_unknown_discovery(self, session_id: str, iteration: int) -> None:
        if not getattr(self.settings, "flx_unknown_discovery_enabled", True):
            return
        sess = self._sessions.get_by_session_id(session_id) or {}
        if sess.get("unknown_discovery_done") or iteration < 3:
            return
        unseen = []
        coverage = dict(sess.get("research_coverage") or {})
        for aspect in DEFAULT_ASPECTS:
            if float((coverage.get(aspect) or {}).get("score") or 0.0) < 0.4:
                unseen.append(aspect)
        # Локація з objective або з оригінального query — щоб task не був абстрактним
        # («Дослідити аспект demographics» без міста → step-агент губить контекст).
        plan_objective = str(((sess.get("plan") or {}).get("objective")) or "").strip()
        query = str(sess.get("query") or "").strip()
        loc_hint = (plan_objective or query)[:200]
        suffix = f" (контекст: {loc_hint})" if loc_hint else ""
        tasks = [
            {
                "goal": f"Дослідити часто пропущений аспект: {a}{suffix}",
                "branch": "area" if a in ("infrastructure", "transport") else "market",
                "priority_hint": 0.72,
                "assigned_role": "Researcher",
            }
            for a in unseen[:3]
        ]
        self._append_tasks_from_agent(session_id, tasks)
        self._sessions.update_fields(session_id, {"unknown_discovery_done": True})

    def _dispatch(self, session_id: str) -> None:
        """Стартує продовження циклу. Якщо Celery увімкнено — відправляє таску; інакше виконує
        run_loop синхронно у поточному потоці. Викликається лише з public API (start/submit_user_answer).
        Внутрішні гілки run_one_step повертають керування назад наверх — не викликаються повторно тут.
        """
        try:
            if getattr(self.settings, "task_queue_enabled", False):
                self._orchestration.enqueue(session_id, countdown_sec=0)
                logger.info("[flx-think] dispatch.enqueued session=%s via=celery queue=llm_processing", session_id)
                return
        except Exception as e:
            logger.debug("Celery enqueue failed (will run inline): %s", e)
        # Синхронний fallback — корисно у dev і тестах
        logger.info("[flx-think] dispatch.inline session=%s via=sync_fallback", session_id)
        self.run_loop(session_id)

    def run_loop(self, session_id: str) -> Dict[str, Any]:
        """Виконує до max_steps_per_task ітерацій run_one_step або до досягнення термінального стану.

        Викликається Celery-таскою. Повертає підсумок останньої ітерації.
        """
        max_steps = max(1, int(getattr(self.settings, "llm_investigator_max_steps_per_task", 5)))
        last: Dict[str, Any] = {"ok": True, "state": "running"}
        logger.info("[flx-think] loop.start session=%s max_steps=%s", session_id, max_steps)
        for _ in range(max_steps):
            last = self.run_one_step(session_id) or {}
            state = (last.get("state") or "running").lower()
            if state in ("done", "failed", "awaiting_user", "cancelled"):
                logger.info("[flx-think] loop.stop session=%s state=%s note=%s", session_id, state, last.get("note"))
                return last
            if state == "awaiting_sources":
                try:
                    if getattr(self.settings, "task_queue_enabled", False):
                        self._orchestration.enqueue(session_id, countdown_sec=SOURCE_WAIT_RECHECK_SECONDS)
                        logger.info(
                            "[flx-think] loop.wait_sources session=%s recheck_in_sec=%s queue=llm_processing",
                            session_id,
                            SOURCE_WAIT_RECHECK_SECONDS,
                        )
                except Exception as e:
                    logger.debug("source wait re-enqueue failed: %s", e)
                return last
        # Перевищили бюджет одного таску — re-enqueue, якщо Celery увімкнено
        try:
            if getattr(self.settings, "task_queue_enabled", False):
                self._orchestration.enqueue(session_id, countdown_sec=0)
                logger.info("[flx-think] loop.reenqueue session=%s reason=max_steps_budget queue=llm_processing", session_id)
        except Exception as e:
            logger.debug("re-enqueue failed: %s", e)
        return last

    @staticmethod
    def _preview_text(value: Any, limit: int = LOG_TEXT_PREVIEW_LIMIT) -> str:
        text = str(value or "").replace("\n", " ").strip()
        if len(text) <= limit:
            return text
        return text[:limit].rstrip() + "…"
