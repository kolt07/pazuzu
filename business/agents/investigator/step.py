# -*- coding: utf-8 -*-
"""
InvestigatorStepAgent — на кожній ітерації обирає одну дію.

Вихід — рівно одна з трьох форм (Shape A/B/C):
- tool_call: викликати тулзу зі схеми (executor виконає)
- ask_user: задати питання користувачу (executor паузить розслідування)
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
    ) -> Dict[str, Any]:
        """Повертає нормалізоване рішення з єдиним з ключів: tool_call, ask_user, final_for_step.

        Якщо LLM повернув некоректний JSON або відсутні дії — fallback у `final_for_step` із
        next_action=`continue` (оркестратор зробить ще одну ітерацію або вирішить завершити).
        """
        template = get_prompt_template("investigator_step")
        if not template:
            return self._fallback_decision()

        schemas_block = self._format_schemas(tools_schemas)
        prompt = render_template(
            template,
            objective=(objective or "")[:500],
            step_id=int(step.get("step_id") or 0),
            step_goal=str(step.get("goal") or "")[:500],
            success_criteria=str(step.get("success_criteria") or "")[:500],
            notes_summary=(notes_summary or "(порожньо)")[:6000],
            pending_answer=(pending_answer or "")[:2000],
            tools_schemas=schemas_block[:8000],
        )

        data = call_llm_json(self.llm, prompt=prompt, caller="flx.step")
        if not data:
            return self._fallback_decision()
        return self._normalize_decision(data)

    def _format_schemas(self, schemas: List[Dict[str, Any]]) -> str:
        if not schemas:
            return "(інструменти недоступні)"
        lines: List[str] = []
        for s in schemas[:30]:
            name = str(s.get("name") or "")
            desc = str(s.get("description") or "").strip()
            if len(desc) > 220:
                desc = desc[:220].rstrip() + "…"
            args_keys = list((s.get("input_schema") or {}).get("properties", {}).keys())
            args_str = ", ".join(args_keys[:8])
            lines.append(f"- {name}({args_str}): {desc}")
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
