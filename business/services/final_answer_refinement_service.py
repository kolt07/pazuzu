# -*- coding: utf-8 -*-
"""
FinalAnswerRefinementService: фінальний крок — LLM аналізує запит з контекстом результатів
та формує цілісну відповідь для людини або пояснює, чому запит не може бути виконаний.
"""

import logging
import json
from typing import Dict, Any, Optional

from config.settings import Settings
from business.services.llm_service import LLMService

logger = logging.getLogger(__name__)


class FinalAnswerRefinementService:
    """
    Формує фінальну відповідь на основі запиту, наміру та реальних результатів.
    LLM аналізує контекст і виробляє цілісну, людяну відповідь.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._llm_service: Optional[LLMService] = None

    @property
    def llm_service(self) -> Optional[LLMService]:
        if self._llm_service is None:
            try:
                self._llm_service = LLMService(self.settings)
            except Exception as e:
                logger.warning("FinalAnswerRefinementService: LLM недоступний: %s", e)
        return self._llm_service

    def refine(
        self,
        user_query: str,
        intent_info: Dict[str, Any],
        execution_result: Dict[str, Any],
        draft_summary: str,
    ) -> str:
        """
        Аналізує запит з контекстом результатів і формує фінальну відповідь.

        Args:
            user_query: оригінальний запит користувача
            intent_info: intent, response_format, response_template
            execution_result: data, row_count, filter_info, diagnostic_info
            draft_summary: поточний варіант відповіді від AnswerComposer

        Returns:
            Фінальна відповідь для користувача
        """
        if not self.llm_service:
            return draft_summary

        try:
            prompt = self._build_refinement_prompt(
                user_query, intent_info, execution_result, draft_summary
            )
            response = self.llm_service.generate_text(
                prompt=prompt,
                system_prompt=None,
                temperature=0.2,
            )
            result = response.strip()
            if result and len(result) < 10000:
                return result
            return draft_summary
        except Exception as e:
            logger.exception("FinalAnswerRefinementService: %s", e)
            return draft_summary

    def _build_refinement_prompt(
        self,
        user_query: str,
        intent_info: Dict[str, Any],
        execution_result: Dict[str, Any],
        draft_summary: str,
    ) -> str:
        row_count = execution_result.get("row_count", 0)
        filter_info = execution_result.get("filter_info", {})
        diagnostic_info = execution_result.get("diagnostic_info", {})
        data_sample = (execution_result.get("data") or [])[:3]

        parts = [
            "Form a coherent, human-like answer to the user based on the query, intent and selection results. The final answer must be in Ukrainian.",
            "",
            "## User query:",
            user_query,
            "",
            "## Detected intent:",
            json.dumps(intent_info, ensure_ascii=False, indent=2),
            "",
            "## Results:",
            f"- Rows found: {row_count}",
            f"- Filters applied: {filter_info}",
            "",
            "## Current draft:",
            draft_summary,
            "",
        ]

        if data_sample and row_count > 0:
            parts.append("## Sample data (first records):")
            parts.append(json.dumps(data_sample, ensure_ascii=False, indent=2, default=str)[:1500])
            parts.append("")

        if row_count == 0 and diagnostic_info:
            parts.append("## Diagnostics:")
            parts.append(json.dumps(diagnostic_info, ensure_ascii=False, indent=2, default=str)[:500])
            parts.append("")

        response_format = intent_info.get("response_format", "")
        q_lower = (user_query or "").lower()
        is_list_query = (
            response_format == "text_answer"
            and row_count > 0
            and row_count <= 20
            and any(
                kw in q_lower
                for kw in ("топ", "найвигідніш", "найдорожч", "найдешевш", "найкращ", "найвищ", "найнижч")
            )
        )
        instruction_lines = [
            "## Task:",
            "1. If the query is about count — the answer must contain one number.",
            "2. If 0 results — explain why (filters, period) and what to try.",
            "3. If many results — give a short summary (count, key figures).",
            "4. The answer must be in Ukrainian, concise, no extra detail.",
            "5. For normal summaries — do not repeat full structured lists.",
        ]
        if is_list_query and data_sample:
            instruction_lines.append(
                "6. IMPORTANT: for 'top-N', 'найвигідніші', 'найдорожчі' — the answer MUST contain a list with a short description of each item (location, price, link). Do not replace the list with 'found X, ready to view' — the user must see the data."
            )
        instruction_lines.extend(["", "Return ONLY the final answer text, no extra explanations. Answer in Ukrainian."])
        parts.extend(instruction_lines)

        return "\n".join(parts)
