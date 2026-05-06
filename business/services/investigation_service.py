# -*- coding: utf-8 -*-
"""
InvestigationService — оркестратор розслідувань Flx.

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
import uuid
import base64
import hashlib
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, List, Optional

from config.settings import Settings

from business.agents.investigator import (
    InvestigatorPlannerAgent,
    InvestigatorStepAgent,
    InvestigatorReflectionAgent,
    InvestigatorReportAgent,
)
from business.services.artifact_service import ArtifactService
from business.services.static_map_service import StaticMapService
from business.services.web_search_service import WebSearchService

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
    """Координує життєвий цикл розслідування Flx."""

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self._sessions = InvestigationSessionRepository()
        self._notes = InvestigationNotesRepository()
        self._lessons = FlxLessonsRepository()
        self._events = InvestigationEventRepository()
        self._artifacts = ArtifactService()
        self._planner = InvestigatorPlannerAgent(self.settings)
        self._step_agent = InvestigatorStepAgent(self.settings)
        self._reflection = InvestigatorReflectionAgent(self.settings)
        self._reporter = InvestigatorReportAgent(self.settings)
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
            payload={"message": "Розслідування запущено. Будую початковий план...", "phase": "created"},
        )
        # Початковий план будуємо синхронно тут, щоб користувач одразу бачив структуру.
        try:
            self._sessions.set_state(session_id, "planning")
            relevant_lessons = self._fetch_relevant_lessons(query, user_id=user_id, top_k=5)
            plan = self._planner.build_plan(
                user_query=query,
                allowed_tools=self.allowed_tool_names(),
                relevant_lessons=relevant_lessons,
            )
            self._sessions.set_plan(session_id, plan)
            self._notes.append(
                session_id=session_id,
                kind="decision",
                text="План розслідування побудовано: " + (plan.get("objective") or "")[:300],
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
        self._notes.append(
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
            self._events.push(session_id=session_id, event_type="status", payload={"message": "Розслідування скасовано користувачем."})
        return {"ok": bool(ok)}

    def list_for_user(self, user_id: str, limit: int = 50) -> List[Dict[str, Any]]:
        return self._sessions.list_for_user(user_id, limit=limit)

    def fetch_events(self, session_id: str, since_seq: int = 0, limit: int = 200) -> List[Dict[str, Any]]:
        return self._events.fetch_since(session_id, since_seq=since_seq, limit=limit)

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
        if step_index >= len(steps):
            return self._finalize(sess)

        iteration = self._sessions.increment_iteration(session_id)
        max_iters = int(getattr(self.settings, "llm_investigator_max_iterations", 40))
        if iteration > max_iters:
            self._notes.append(
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

        decision = self._step_agent.decide(
            objective=plan.get("objective") or "",
            step=current_step,
            notes_summary=notes_summary,
            tools_schemas=tools_schemas,
            pending_answer=pending_answer,
        )
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

        # Нотатка з думкою
        thought = (decision.get("thought") or "").strip()
        if thought:
            logger.info(
                "[flx-think] step.thought session=%s request_id=%s content=%s",
                session_id,
                request_id or "—",
                self._preview_text(thought),
            )
            self._notes.append(
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
        self._notes.append(
            session_id=session_id,
            kind="decision",
            text="Неочікуваний формат рішення LLM. Завершую крок.",
            step_index=int(current_step.get("step_id") or step_index + 1),
        )
        return self._advance_step(session_id, plan, step_index, finish=False)

    # -------------------------- branch handlers --------------------------

    def _handle_tool_call(self, session_id: str, current_step: Dict[str, Any], tool_call: Dict[str, Any]) -> Dict[str, Any]:
        name = str(tool_call.get("name") or "")
        args = tool_call.get("args") or {}
        if not isinstance(args, dict):
            args = {}
        if (
            name == "flx.web_search"
            and "flx.targeted_source_search" in self._tools_registry
            and not self._has_recent_tool_usage(session_id, "flx.targeted_source_search", recent_notes=24)
        ):
            sess = self._sessions.get_by_session_id(session_id) or {}
            logger.info(
                "[flx-think] tool_call.override session=%s step=%s from=flx.web_search to=flx.targeted_source_search reason=no_targeted_search_yet",
                session_id,
                current_step.get("step_id") or 0,
            )
            name = "flx.targeted_source_search"
            args = {
                "query_text": str(args.get("query") or sess.get("query") or ""),
                "source": "both",
                "days": 7,
            }
        logger.info(
            "[flx-think] tool_call.start session=%s step=%s tool=%s args=%s expected=tool_result",
            session_id,
            current_step.get("step_id") or 0,
            name,
            self._safe_args_preview(args),
        )
        if name not in self._tools_registry:
            self._notes.append(
                session_id=session_id,
                kind="tool_error",
                text=f"Інструмент '{name}' не дозволено allow-list'ом.",
                step_index=int(current_step.get("step_id") or 0),
                tool_call_summary={"name": name, "args_keys": list(args.keys())},
            )
            self._events.push(session_id=session_id, event_type="status", payload={"message": f"Інструмент {name} не дозволений."})
            return {"ok": True, "state": "running", "note": "tool_not_allowed"}

        tool_def = self._tools_registry[name]
        callable_ = tool_def["callable"]
        # Інжектуємо session_id/user_id у тулзи Flx, де це потрібно — за конвенцією registry.
        extra = self._inject_context(name, args, session_id)
        merged_args = {**args, **extra}

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
            self._notes.append(
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
            self._notes.append(
                session_id=session_id,
                kind="tool_error",
                text=err,
                step_index=int(current_step.get("step_id") or 0),
                tool_call_summary={"name": name, "args_keys": list(merged_args.keys())},
            )
            return {"ok": True, "state": "running", "note": "tool_failed"}

        # Пишемо observation у нотатки
        observation = self._serialize_observation(result)
        logger.info(
            "[flx-think] tool_call.done session=%s step=%s tool=%s observation_preview=%s",
            session_id,
            current_step.get("step_id") or 0,
            name,
            self._preview_text(observation),
        )
        self._notes.append(
            session_id=session_id,
            kind="observation",
            text=observation,
            step_index=int(current_step.get("step_id") or 0),
            tool_call_summary={"name": name, "result_size": len(observation)},
        )

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
        self._notes.append(
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
        logger.info(
            "[flx-think] step.final session=%s step=%s next_action=%s expected=%s observation=%s",
            session_id,
            current_step.get("step_id") or 0,
            next_action,
            "report_generation" if next_action == "finish" else "next_step_decision",
            self._preview_text(observation),
        )
        if observation:
            self._notes.append(
                session_id=session_id,
                kind="observation",
                text=observation,
                step_index=int(current_step.get("step_id") or 0),
            )
        if next_action == "finish":
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
        max_steps_per_task = int(getattr(self.settings, "llm_investigator_max_steps_per_task", 5))
        if force_next:
            new_index = min(step_index + 1, len(steps))
            self._sessions.update_fields(session_id, {"step_index": new_index})
        if finish or step_index >= len(steps):
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
            notes_summary = self._notes.summary(session_id, top_k=24)
            user_query = sess.get("query") or ""

            self._events.push(session_id=session_id, event_type="status", payload={"message": "Компоную фінальний звіт..."})
            structured = self._reporter.compose(
                user_query=user_query,
                notes_summary=notes_summary,
                evidence=evidence,
            )
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
        notes = self._notes.list_for_session(session_id, limit=300, kinds=["observation", "user_answer"])
        evidence: List[Dict[str, Any]] = []
        for n in notes:
            evidence.append({"kind": n.get("kind"), "text": n.get("text") or ""})
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
        return evidence

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
            return tmpl.render(
                report=structured,
                generated_at=datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC"),
                static_map_url_prefix="/api/files/artifact/",
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
        return f"""<!doctype html><html lang='uk'><head><meta charset='utf-8'><title>{title}</title>
<style>body{{font-family:system-ui;max-width:920px;margin:24px auto;padding:0 16px;color:#1a1a1a}}
h1{{margin:0 0 12px}} h2{{margin-top:32px}} section{{border-top:1px solid #eee;padding-top:16px}}</style></head>
<body><h1>{title}</h1><p><em>{summary}</em></p>{sections_html}</body></html>"""

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
            "Записує нотатку (думку/спостереження/гіпотезу/рішення) для активного розслідування.",
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
            "Записує відкрите питання користувачу та паузить розслідування до відповіді.",
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
        doc = self._notes.append(session_id=session_id, kind=kind, text=text, step_index=step_index)
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
            "title": str(title or "")[:200] or "Звіт розслідування Flx",
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

    # ----------------------------- helpers --------------------------------

    def _fetch_relevant_lessons(self, query: str, user_id: Optional[str] = None, top_k: int = 5) -> List[Dict[str, Any]]:
        try:
            return self._lessons.search_text(query=query, user_id=user_id, top_k=top_k) or []
        except Exception as e:
            logger.debug("Lessons search failed: %s", e)
            return []

    def _check_source_wait(self, session_id: str, sess: Dict[str, Any]) -> Dict[str, Any]:
        wait = (sess or {}).get("pending_source_wait") or {}
        task_id = str(wait.get("task_id") or "")
        if not task_id:
            self._sessions.update_fields(session_id, {"state": "running", "pending_source_wait": None})
            return {"ok": True, "state": "running", "note": "source_wait_missing_task_id"}
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
        notes = self._notes.list_for_session(session_id, limit=max(1, recent_notes))
        for n in notes:
            tcs = n.get("tool_call_summary") or {}
            if isinstance(tcs, dict) and str(tcs.get("name") or "") == str(tool_name):
                return True
        return False

    def _detect_repetitive_query_loop(self, session_id: str) -> bool:
        notes = self._notes.list_for_session(session_id, limit=12, kinds=["observation"])
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
        if len(sigs) < 3:
            return False
        tail = sigs[-3:]
        return len(set(tail)) == 1

    def _dispatch(self, session_id: str) -> None:
        """Стартує продовження циклу. Якщо Celery увімкнено — відправляє таску; інакше виконує
        run_loop синхронно у поточному потоці. Викликається лише з public API (start/submit_user_answer).
        Внутрішні гілки run_one_step повертають керування назад наверх — не викликаються повторно тут.
        """
        try:
            if getattr(self.settings, "task_queue_enabled", False):
                from business.tasks import run_investigation_step  # type: ignore
                run_investigation_step.apply_async(args=[session_id], countdown=0, queue="llm_processing")
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
                        from business.tasks import run_investigation_step  # type: ignore
                        run_investigation_step.apply_async(
                            args=[session_id],
                            countdown=SOURCE_WAIT_RECHECK_SECONDS,
                            queue="llm_processing",
                        )
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
                from business.tasks import run_investigation_step  # type: ignore
                run_investigation_step.apply_async(args=[session_id], countdown=0, queue="llm_processing")
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
