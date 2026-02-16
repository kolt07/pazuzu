# -*- coding: utf-8 -*-
"""
Агент-помічник: спілкування з користувачем, формування контексту, виклик інтерпретатора та
планувальника, виконання пайплайну через аналітика, формування фінальної відповіді.
"""

import logging
import time
from typing import Dict, Any, List, Optional, Callable

from business.agents.interpreter_agent import InterpreterAgent
from business.agents.planner_agent import PlannerAgent
from business.agents.analyst_agent import AnalystAgent
from business.agents.plan_step_schema import validate_plan

logger = logging.getLogger(__name__)


class AssistantAgent:
    """
    Агент-помічник: оркеструє мультиагентний пайплайн для явних звітів/експортів за період.
    Потік: контекст (історія) → інтерпретатор (структурований намір; мінімальний роутинг) →
    планувальник (список кроків) → аналітик (виконання кроків через MCP) → відповідь користувачу.
    Для вільних або складних запитів система передає керування LangChain-агенту, а не планувальнику.
    """

    def __init__(
        self,
        interpreter: InterpreterAgent,
        planner: PlannerAgent,
        analyst: AnalystAgent,
        log_intent_fn: Optional[Callable[..., None]] = None,
        log_action_fn: Optional[Callable[..., None]] = None,
    ):
        self.interpreter = interpreter
        self.planner = planner
        self.analyst = analyst
        self.log_intent_fn = log_intent_fn
        self.log_action_fn = log_action_fn

    def run(
        self,
        user_query: str,
        user_id: Optional[str],
        context_summary: Optional[str],
        request_id: str,
        precomputed_structured: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Повний цикл: інтерпретація (або precomputed_structured) -> лог наміру -> планування -> виконання кроків -> збір файлів та тексту.

        precomputed_structured: якщо передано (наприклад із MultiAgentService після двоступеневої маршрутизації),
        повторно не викликається інтерпретатор.

        Returns:
            {
                "success": bool,
                "response_text": str,
                "excel_files": [{"file_base64", "filename", "rows_count", "columns_count"}, ...],
                "error": str | None,
            }
        """
        if precomputed_structured is not None:
            structured = precomputed_structured
        else:
            structured = self.interpreter.interpret_user_query(user_query, context_summary)
        if self.log_intent_fn:
            try:
                self.log_intent_fn(request_id, user_id, structured)
            except Exception as e:
                logger.warning("log_intent_fn: %s", e)

        steps = self.planner.plan(structured)
        if self.log_action_fn:
            try:
                self.log_action_fn(request_id, "planner", {"steps_count": len(steps), "steps": steps})
            except Exception as e:
                logger.warning("log_action_fn planner: %s", e)

        plan_ok, plan_error = validate_plan(steps)
        if not plan_ok:
            if self.log_action_fn:
                try:
                    self.log_action_fn(request_id, "assistant", {"validation": "failed", "error": plan_error})
                except Exception as e:
                    logger.warning("log_action_fn assistant: %s", e)
            return {
                "success": False,
                "response_text": f"Помилка плану виконання: {plan_error}",
                "excel_files": [],
                "error": plan_error,
            }

        previous_results: List[Dict[str, Any]] = []
        excel_files: List[Dict[str, Any]] = []

        for i, step in enumerate(steps):
            t0 = time.perf_counter()
            result = self.analyst.run_step(step, i, previous_results)
            duration_ms = int((time.perf_counter() - t0) * 1000)
            previous_results.append(result)
            if self.log_action_fn:
                try:
                    payload = {
                        "step_index": i,
                        "action": step.get("action"),
                        "success": result.get("success"),
                        "duration_ms": duration_ms,
                        "input_summary": {"action": step.get("action"), "params_keys": list((step.get("params") or {}).keys())},
                        "output_summary": {"success": result.get("success"), "rows_count": result.get("rows_count"), "count": result.get("count")},
                    }
                    self.log_action_fn(request_id, "analyst", payload)
                except Exception as e:
                    logger.warning("log_action_fn analyst: %s", e)
            if result.get("needs_confirmation"):
                return {
                    "success": True,
                    "response_text": result.get("message", "Підтвердіть експорт у наступному повідомленні."),
                    "excel_files": [],
                    "error": None,
                }
            if result.get("success") and result.get("file_base64") and result.get("filename"):
                excel_files.append({
                    "file_base64": result["file_base64"],
                    "filename": result["filename"],
                    "rows_count": result.get("rows_count", 0),
                    "columns_count": result.get("columns_count", 0),
                })

        if excel_files:
            total_rows = sum(f.get("rows_count", 0) for f in excel_files)
            response_text = (
                f"Готово. Сформовано файлів: {len(excel_files)}. "
                f"Усього записів у файлах: {total_rows}. Вони надіслані окремими повідомленнями."
            )
        else:
            response_text = self._format_response_from_results(previous_results, structured)

        return {
            "success": True,
            "response_text": response_text,
            "excel_files": excel_files,
            "error": None,
        }

    def _format_response_from_results(
        self,
        results: List[Dict[str, Any]],
        structured: Dict[str, Any],
    ) -> str:
        success_count = sum(1 for r in results if isinstance(r, dict) and r.get("success"))
        if success_count == 0 and results:
            last_err = results[-1].get("error", "Невідома помилка") if isinstance(results[-1], dict) else "Помилка"
            return f"Під час виконання виникла помилка: {last_err}"

        # Числові дані для підкріплення відповіді (за наявності результатів вибірки)
        numbers_parts: List[str] = []
        for r in results:
            if not isinstance(r, dict) or not r.get("success"):
                continue
            count = r.get("count")
            rows = r.get("rows_count")
            if count is not None and count >= 0:
                numbers_parts.append(f"{count} записів")
            if rows is not None and rows >= 0:
                numbers_parts.append(f"{rows} рядків")

        if structured.get("intent") in ("report_last_day", "report_last_week", "export_data"):
            base = "Запит оброблено. Якщо очікувалися файли — перевірте, чи є дані за обраний період у вибраних колекціях."
            if numbers_parts:
                base += f" Числа: {', '.join(numbers_parts)}."
            return base
        base = "Запит виконано. Якщо потрібні додаткові дії — уточніть, будь ласка."
        if numbers_parts:
            base += f" Підсумок: {', '.join(numbers_parts)}."
        return base
