# -*- coding: utf-8 -*-
"""
InvestigatorPlannerAgent — будує початковий JSON-план розслідування Flx.

LLM-only: жодних викликів MCP, жодних побічних ефектів. Вихід — структура з
переліком кроків, які потім виконує InvestigatorStepAgent через executor.
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


class InvestigatorPlannerAgent:
    """Початковий планувальник розслідування."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm = None

    @property
    def llm(self):
        if self._llm is None:
            self._llm = get_llm_service(self.settings)
        return self._llm

    def build_plan(
        self,
        *,
        user_query: str,
        allowed_tools: List[str],
        relevant_lessons: Optional[List[Dict[str, Any]]] = None,
    ) -> Dict[str, Any]:
        """Будує детермінований JSON-план розслідування.

        Повертає dict з ключами: objective, assumptions, open_questions, steps[].
        У разі помилки LLM повертає мінімальний fallback-план з одним кроком.
        """
        template = get_prompt_template("investigator_planner")
        if not template:
            return self._fallback_plan(user_query)

        lessons_block = self._format_lessons(relevant_lessons or [])
        prompt = render_template(
            template,
            user_query=(user_query or "").strip()[:2000],
            lessons_block=lessons_block,
            allowed_tools=", ".join(allowed_tools or []),
        )

        data = call_llm_json(self.llm, prompt=prompt, caller="flx.planner")
        if not data:
            return self._fallback_plan(user_query)

        steps = data.get("steps")
        if not isinstance(steps, list) or not steps:
            return self._fallback_plan(user_query)

        return {
            "objective": str(data.get("objective") or "")[:500],
            "assumptions": [str(x)[:500] for x in (data.get("assumptions") or []) if x][:10],
            "open_questions": [str(x)[:500] for x in (data.get("open_questions") or []) if x][:10],
            "steps": [self._normalize_step(idx + 1, s) for idx, s in enumerate(steps)][:12],
        }

    def _format_lessons(self, lessons: List[Dict[str, Any]]) -> str:
        if not lessons:
            return "(немає релевантних попередніх кейсів)"
        lines: List[str] = []
        for i, lsn in enumerate(lessons[:5], start=1):
            tags = ", ".join((lsn.get("topic_tags") or [])[:5])
            recs = "; ".join((lsn.get("recommendations") or [])[:3])
            ww = "; ".join((lsn.get("what_worked") or [])[:3])
            lines.append(
                f"{i}. tags=[{tags}] recommendations: {recs}; what_worked: {ww}"
            )
        return "\n".join(lines)

    def _normalize_step(self, idx: int, step: Any) -> Dict[str, Any]:
        if not isinstance(step, dict):
            step = {}
        return {
            "step_id": int(step.get("step_id") or idx),
            "goal": str(step.get("goal") or "")[:500],
            "candidate_tools": [
                str(t)[:80]
                for t in (step.get("candidate_tools") or [])
                if str(t).strip()
            ][:8],
            "success_criteria": str(step.get("success_criteria") or "")[:500],
        }

    def _fallback_plan(self, user_query: str) -> Dict[str, Any]:
        """Мінімальний план, якщо LLM не доступний/відмовив. Дозволяє оркестратору не зависнути."""
        return {
            "objective": "Зібрати початкові дані щодо запиту користувача.",
            "assumptions": [],
            "open_questions": [str(user_query or "")[:500]],
            "steps": [
                {
                    "step_id": 1,
                    "goal": "Уточнити запит у користувача та зібрати базові дані по локації.",
                    "candidate_tools": ["flx.ask_user", "schema.get_data_dictionary"],
                    "success_criteria": "Є чітка локація і тип нерухомості.",
                }
            ],
        }
