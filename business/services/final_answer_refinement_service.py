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
            "Тобі необхідно сформувати цілісну, людяну відповідь користувачу на основі запиту, наміру та результатів вибірки.",
            "",
            "## Запит користувача:",
            user_query,
            "",
            "## Визначений намір:",
            json.dumps(intent_info, ensure_ascii=False, indent=2),
            "",
            "## Результати:",
            f"- Кількість знайдених записів: {row_count}",
            f"- Застосовані фільтри: {filter_info}",
            "",
            "## Поточна чернетка відповіді:",
            draft_summary,
            "",
        ]

        if data_sample and row_count > 0:
            parts.append("## Приклад даних (перші записи):")
            parts.append(json.dumps(data_sample, ensure_ascii=False, indent=2, default=str)[:1500])
            parts.append("")

        if row_count == 0 and diagnostic_info:
            parts.append("## Діагностика:")
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
            "## Закріплення завдання — інструкції:",
            "1. Якщо запит про кількість — відповідь має містити одне число.",
            "2. Якщо результатів 0 — поясни чому (фільтри, період) та що можна спробувати.",
            "3. Якщо багато результатів — дай короткий підсумок (кількість, ключові цифри).",
            "4. Відповідь має бути українською, стислою, без зайвих деталей.",
            "5. Для звичайних підсумків — не дублюй повністю структуровані списки.",
        ]
        if is_list_query and data_sample:
            instruction_lines.append(
                "6. ВАЖЛИВО: для запитів типу «топ-N», «найвигідніші», «найдорожчі» — "
                "відповідь ОБОВ'ЯЗКОВО має містити список з коротким описом кожного об'єкта "
                "(локація, ціна, посилання). НЕ замінюй список фразою «знайшов X, готові до ознайомлення» — користувач має побачити дані."
            )
        instruction_lines.extend(["", "Поверни ТІЛЬКИ текст фінальної відповіді, без додаткових пояснень:"])
        parts.extend(instruction_lines)

        return "\n".join(parts)
