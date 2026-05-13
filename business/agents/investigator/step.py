# -*- coding: utf-8 -*-
"""
InvestigatorStepAgent — на кожній ітерації обирає одну дію.

Вихід — рівно одна з трьох форм (Shape A/B/C):
- tool_call: викликати тулзу зі схеми (executor виконає)
- ask_user: задати питання користувачу (executor паузить дослідження)
- final_for_step: завершити крок (continue/new_step/finish)
"""

import logging
from typing import Any, Dict, List, Optional

from config.settings import Settings

from business.agents.investigator._common import (
    call_llm_json,
    get_llm_service,
    get_prompt_template,
    render_template,
)

logger = logging.getLogger(__name__)


class InvestigatorStepAgent:
    """LLM-агент, що ухвалює одне рішення на крок."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm = None

    @property
    def llm(self):
        if self._llm is None:
            self._llm = get_llm_service(self.settings)
        return self._llm

    def decide(
        self,
        *,
        objective: str,
        step: Dict[str, Any],
        notes_summary: str,
        tools_schemas: List[Dict[str, Any]],
        pending_answer: Optional[str] = None,
        active_strategy_id: Optional[str] = None,
        active_strategy_name: Optional[str] = None,
        active_strategy_hypothesis: Optional[str] = None,
        scope_lock: Optional[Dict[str, Any]] = None,
        db_knowledge_block: Optional[str] = None,
    ) -> Dict[str, Any]:
        """Повертає нормалізоване рішення з єдиним з ключів: tool_call, ask_user, final_for_step.

        Якщо LLM повернув некоректний JSON або відсутні дії — fallback у `final_for_step` із
        next_action=`continue` (оркестратор зробить ще одну ітерацію або вирішить завершити).
        """
        template = get_prompt_template("investigator_step")
        if not template:
            return self._fallback_decision()

        schemas_block = self._format_schemas(tools_schemas)
        strat_block = self._format_strategy_context(
            active_strategy_id,
            active_strategy_name,
            active_strategy_hypothesis,
        )
        scope_block = self._format_scope_lock(scope_lock)
        db_block = (db_knowledge_block or "").strip() or "(знання про колекції не завантажено)"
        prompt = render_template(
            template,
            objective=(objective or "")[:500],
            step_id=int(step.get("step_id") or 0),
            step_goal=str(step.get("goal") or "")[:500],
            success_criteria=str(step.get("success_criteria") or "")[:500],
            notes_summary=(notes_summary or "(порожньо)")[:6000],
            pending_answer=(pending_answer or "")[:2000],
            tools_schemas=schemas_block[:8000],
            strategy_context_block=strat_block[:2000],
            scope_lock_block=scope_block[:2000],
            db_knowledge_block=db_block[:4000],
        )

        data = call_llm_json(self.llm, prompt=prompt, caller="flx.step")
        if not data:
            return self._fallback_decision()
        return self._normalize_decision(data)

    @staticmethod
    def _format_scope_lock(scope_lock: Optional[Dict[str, Any]]) -> str:
        if not isinstance(scope_lock, dict) or not scope_lock:
            return "(scope_lock не заданий — використовуй ЛОКАЦІЮ ТА ПРЕДМЕТ з PRIMARY OBJECTIVE)"
        loc = scope_lock.get("location") or {}
        prop = scope_lock.get("property") or {}
        tk = scope_lock.get("task_keywords") or []
        lines: List[str] = []
        city = loc.get("city")
        region = loc.get("region")
        raw = loc.get("raw")
        if city:
            lines.append(f'city = "{city}"  (копіюй у args[`city`] дослівно: {city})')
        if region:
            lines.append(f'region = "{region}"  (для listings → args[`region`], для cadastral → args[`scope.oblast_name`]; дослівно: {region})')
        if raw and not city:
            lines.append(f'location raw = "{raw}"  (city НЕ нормалізований — викликай Shape B, не вгадуй назву)')
        type_kw = prop.get("type_keyword")
        if type_kw:
            lines.append(f'property_type_contains = "{type_kw}"  (substring пошук, дослівно)')
        intent = prop.get("intent")
        if intent:
            lines.append(f'listing_intent = "{intent}"')
        min_a = prop.get("min_area_sqm")
        max_a = prop.get("max_area_sqm")
        if min_a:
            lines.append(f"min area = {min_a} м²  → listings `building_area_min={min_a}`, cadastral `filters.min_area_sqm={min_a}`")
        if max_a:
            lines.append(f"max area = {max_a} м²  → listings `building_area_max={max_a}`, cadastral `filters.max_area_sqm={max_a}`")
        if tk:
            kws = ", ".join(f'"{x}"' for x in tk[:6])
            lines.append(f"task_keywords = [{kws}]  → у `query_text` / `semantic_query`")
        if not lines:
            return "(scope_lock порожній — параметри не витягнуто)"
        header = (
            'Значення у подвійних лапках — це КАНОНІЧНІ рядки з запиту користувача.\n'
            'Копіюй їх у tool args ДОСЛІВНО, без змін/перекладу/відмінювання/синонімів/нормалізації.\n'
            'Якщо назва незнайома (наприклад, маленьке містечко) — НЕ заміняй її на схожу/популярнішу.\n'
        )
        return header + "\n".join(f"- {ln}" for ln in lines)

    @staticmethod
    def _format_strategy_context(
        sid: Optional[str],
        name: Optional[str],
        hypothesis: Optional[str],
    ) -> str:
        if not sid and not name:
            return "(контекст стратегії не заданий — одиночний план)"
        parts = []
        if sid:
            parts.append(f"id={sid}")
        if name:
            parts.append(f"назва={name}")
        head = "Активна стратегія: " + "; ".join(parts)
        if hypothesis:
            head += f". Гіпотеза: {hypothesis[:400]}"
        return head

    def _format_schemas(self, schemas: List[Dict[str, Any]]) -> str:
        if not schemas:
            return "(інструменти недоступні)"
        lines: List[str] = []
        for s in schemas[:30]:
            name = str(s.get("name") or "")
            desc = str(s.get("description") or "").strip()
            if len(desc) > 220:
                desc = desc[:220].rstrip() + "…"
            input_schema = s.get("input_schema") or {}
            properties = input_schema.get("properties") or {}
            required = [str(v) for v in (input_schema.get("required") or []) if str(v).strip()]
            args_parts: List[str] = []
            for k in list(properties.keys())[:8]:
                prop = properties.get(k) or {}
                t = prop.get("type")
                enum_vals = prop.get("enum")
                type_label = str(t) if isinstance(t, str) else "any"
                enum_label = ""
                if isinstance(enum_vals, list) and enum_vals:
                    enum_preview = "|".join(str(v) for v in enum_vals[:4])
                    enum_label = f"[{enum_preview}]"
                req_label = "*" if k in required else ""
                args_parts.append(f"{k}:{type_label}{enum_label}{req_label}")
            args_str = ", ".join(args_parts)
            req_str = ", ".join(required[:6]) if required else "-"
            lines.append(f"- {name}({args_str}) required=[{req_str}]: {desc}")
        return "\n".join(lines)

    def _fallback_decision(self) -> Dict[str, Any]:
        return {
            "thought": "LLM відмовив або повернув порожню відповідь — продовжуємо стратегію.",
            "final_for_step": {
                "observation": "Не вдалося отримати рішення LLM на цьому кроці.",
                "next_action": "continue",
            },
        }

    def _normalize_decision(self, data: Dict[str, Any]) -> Dict[str, Any]:
        thought = str(data.get("thought") or "").strip()[:500]
        # Пріоритет shape: tool_call > ask_user > final_for_step
        tool_call = data.get("tool_call")
        ask_user = data.get("ask_user")
        final_step = data.get("final_for_step")

        if isinstance(tool_call, dict) and tool_call.get("name"):
            return {
                "thought": thought,
                "tool_call": {
                    "name": str(tool_call.get("name"))[:120],
                    "args": tool_call.get("args") if isinstance(tool_call.get("args"), dict) else {},
                },
            }
        if isinstance(ask_user, dict) and ask_user.get("question"):
            opts = ask_user.get("options") or []
            opts = [str(o)[:200] for o in opts if str(o).strip()][:6]
            return {
                "thought": thought,
                "ask_user": {
                    "question": str(ask_user.get("question"))[:1000],
                    "options": opts,
                    "allow_freeform": bool(ask_user.get("allow_freeform", True)),
                },
            }
        if isinstance(final_step, dict):
            next_action = str(final_step.get("next_action") or "continue").lower()
            if next_action not in ("continue", "new_step", "finish"):
                next_action = "continue"
            return {
                "thought": thought,
                "final_for_step": {
                    "observation": str(final_step.get("observation") or "")[:1000],
                    "next_action": next_action,
                },
            }
        return self._fallback_decision()
