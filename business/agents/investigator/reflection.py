# -*- coding: utf-8 -*-
"""
InvestigatorReflectionAgent — після завершення дослідження генерує lessons-learned.

Вхід — короткий лог кроків і нотаток. Вихід — JSON для flx_lessons_learned, без записування.
Збереженням опікується викликач (InvestigationService → FlxLessonsRepository).
"""

import logging
from typing import Any, Dict, List

from config.settings import Settings

from business.agents.investigator._common import (
    call_llm_json,
    get_llm_service,
    get_prompt_template,
    render_template,
)

logger = logging.getLogger(__name__)


class InvestigatorReflectionAgent:
    """Генерує lesson-learned для довгострокової пам'яті Flx."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm = None

    @property
    def llm(self):
        if self._llm is None:
            self._llm = get_llm_service(self.settings)
        return self._llm

    def reflect(
        self,
        *,
        user_query: str,
        outcome_summary: str,
        steps_log: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """Повертає dict для FlxLessonsRepository.add_lesson(...).

        У разі помилки повертає мінімально валідний об'єкт без рекомендацій.
        """
        template = get_prompt_template("investigator_reflection")
        if not template:
            return self._minimal_lesson(user_query, outcome_summary)

        steps_text = self._format_steps(steps_log)
        prompt = render_template(
            template,
            user_query=(user_query or "")[:1500],
            outcome_summary=(outcome_summary or "")[:1500],
            steps_log=steps_text[:6000],
        )

        data = call_llm_json(self.llm, prompt=prompt, caller="flx.reflection")
        if not data:
            return self._minimal_lesson(user_query, outcome_summary)

        return {
            "topic_tags": [str(t).strip()[:40] for t in (data.get("topic_tags") or []) if str(t).strip()][:8],
            "query_pattern": str(data.get("query_pattern") or "")[:1500],
            "what_worked": [str(x)[:500] for x in (data.get("what_worked") or [])][:10],
            "what_failed": [str(x)[:500] for x in (data.get("what_failed") or [])][:10],
            "recommendations": [str(x)[:500] for x in (data.get("recommendations") or [])][:10],
        }

    def _format_steps(self, steps_log: List[Dict[str, Any]]) -> str:
        if not steps_log:
            return "(немає логу кроків)"
        out: List[str] = []
        for s in steps_log[:30]:
            sid = s.get("step_id")
            tool = s.get("tool")
            ok = s.get("ok")
            note = s.get("note") or ""
            if isinstance(note, str) and len(note) > 200:
                note = note[:200].rstrip() + "…"
            out.append(f"step={sid} tool={tool} ok={ok} note={note}")
        return "\n".join(out)

    def _minimal_lesson(self, user_query: str, outcome_summary: str) -> Dict[str, Any]:
        return {
            "topic_tags": ["investigation"],
            "query_pattern": (user_query or "")[:300],
            "what_worked": [],
            "what_failed": [],
            "recommendations": [],
        }
