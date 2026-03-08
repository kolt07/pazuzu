# -*- coding: utf-8 -*-
"""
Сервіс інтерпретації пайплайнів обробки даних.
Виконує пайплайни виключно через методи домен-шару (PipelineExecutor).
Жодних прямих операцій з MongoDB — тільки через domain/services/pipeline_executor.py.
Після виконання застосовує географічні метрики (напр. відстань до точки) через GeoFilterService.
"""

import logging
import json
from typing import Dict, Any, Optional, List, Callable

from config.settings import Settings
from domain.services.pipeline_executor import execute_pipeline

logger = logging.getLogger(__name__)


class PipelineInterpreterService:
    """
    Сервіс для інтерпретації та виконання пайплайнів обробки даних.
    Делегує виконання PipelineExecutor (домен-шар). Прямий доступ до БД заборонено.
    """

    def __init__(
        self,
        run_tool_fn: Optional[Callable] = None,
        settings: Optional[Settings] = None,
    ):
        """
        Args:
            run_tool_fn: Функція для виклику MCP tools (get_collection_info). Опційно.
            settings: Налаштування (для GeoFilterService при add_metric). Опційно.
        """
        self._temp_collections: Dict[str, List[Dict[str, Any]]] = {}
        self.run_tool_fn = run_tool_fn
        self._settings = settings
        self._geo_filter_service = None

    def _get_geo_filter_service(self):
        """Лінива ініціалізація GeoFilterService для add_metric кроків."""
        if self._geo_filter_service is None and self._settings:
            try:
                from business.services.geo_filter_service import GeoFilterService
                self._geo_filter_service = GeoFilterService(self._settings)
            except Exception as e:
                logger.debug("PipelineInterpreter: GeoFilterService недоступний: %s", e)
        return self._geo_filter_service

    def execute_pipeline(
        self,
        pipeline: Dict[str, Any],
        initial_collection: Optional[str] = None,
        parameters: Optional[Dict[str, Any]] = None
    ) -> Dict[str, Any]:
        """
        Виконує пайплайн обробки даних через домен-шар (CollectionManager).

        Args:
            pipeline: Пайплайн у власному форматі з полем 'steps'
            initial_collection: Початкова колекція для виконання
            parameters: Параметри для підстановки в пайплайн

        Returns:
            Словник з результатами виконання:
            - success: чи успішно виконано
            - results: список результатів
            - count: кількість результатів
            - error: повідомлення про помилку (якщо є)
            - diagnostic_info: діагностична інформація
        """
        try:
            logger.info("PipelineInterpreter.execute_pipeline: Початок виконання пайплайну")

            if parameters:
                logger.info("PipelineInterpreter: Підставляємо параметри: %s", parameters)
                pipeline_before = json.dumps(pipeline, ensure_ascii=False, indent=2)
                pipeline = self._substitute_parameters(pipeline, parameters)
                pipeline_after = json.dumps(pipeline, ensure_ascii=False, indent=2)
                logger.debug("PipelineInterpreter: Пайплайн до підстановки:\n%s", pipeline_before)
                logger.debug("PipelineInterpreter: Пайплайн після підстановки:\n%s", pipeline_after)

            steps = pipeline.get("steps", [])
            logger.info("PipelineInterpreter.execute_pipeline: Знайдено %s кроків", len(steps))

            collection = initial_collection
            if not collection:
                for step in steps:
                    if step.get("collection"):
                        collection = step.get("collection")
                        break
                if not collection:
                    collection = "unified_listings"
                    logger.warning(
                        "PipelineInterpreter.execute_pipeline: Не вдалося визначити колекцію, "
                        "використовується %s",
                        collection
                    )

            logger.info("PipelineInterpreter.execute_pipeline: Колекція: %s", collection)

            result = execute_pipeline(
                steps=steps,
                collection=collection,
                parameters=parameters
            )

            if result.get("success"):
                # Застосовуємо add_metric кроки (відстань до точки тощо)
                result = self._apply_add_metric_steps(result, steps, parameters or {})
                logger.info(
                    "PipelineInterpreter.execute_pipeline: Успішно, результатів: %s",
                    result.get("count", 0)
                )
            else:
                logger.error(
                    "PipelineInterpreter.execute_pipeline: Помилка: %s",
                    result.get("error", "невідома")
                )

            return result

        except Exception as e:
            logger.exception("PipelineInterpreter.execute_pipeline: Помилка: %s", e)
            return {
                "success": False,
                "error": str(e),
                "results": [],
                "count": 0,
                "diagnostic_info": {"pipeline_stages": len(pipeline.get("steps", []))}
            }

    def _apply_add_metric_steps(
        self,
        result: Dict[str, Any],
        steps: List[Dict[str, Any]],
        parameters: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Застосовує add_metric кроки (відстань до точки тощо) через GeoFilterService.
        """
        add_metric_steps = [s for s in steps if s.get("type") == "add_metric"]
        if not add_metric_steps:
            return result
        results_list = result.get("results", [])
        if not results_list:
            return result
        geo_svc = self._get_geo_filter_service()
        if not geo_svc:
            return result
        for step in add_metric_steps:
            metric = step.get("metric")
            ref_point = step.get("reference_point") or step.get("reference")
            if metric == "distance_km" and ref_point:
                ref = ref_point
                if isinstance(ref_point, str) and ref_point.startswith("$"):
                    ref = parameters.get(ref_point[1:], ref_point)
                if isinstance(ref, str) and ref:
                    results_list = geo_svc.add_distance_metric(
                        results_list,
                        reference_point=ref,
                        output_field="distance_km",
                        coord_fields=("addresses", "coordinates"),
                    )
                    result["results"] = results_list
                    result["count"] = len(results_list)
        return result

    def _substitute_parameters(
        self,
        obj: Any,
        parameters: Dict[str, Any]
    ) -> Any:
        """
        Рекурсивно підставляє параметри в об'єкт.
        Параметри можуть бути у форматі $param або {{param}}.
        """
        if isinstance(obj, dict):
            result = {}
            for key, value in obj.items():
                substituted_key = self._substitute_string(key, parameters)
                result[substituted_key] = self._substitute_parameters(value, parameters)
            return result
        elif isinstance(obj, list):
            return [self._substitute_parameters(item, parameters) for item in obj]
        elif isinstance(obj, str):
            return self._substitute_string(obj, parameters)
        else:
            return obj

    def _substitute_string(
        self,
        text: str,
        parameters: Dict[str, Any]
    ) -> Any:
        """
        Підставляє параметри в рядок.
        Підтримує формати: $param, {{param}}
        """
        if not isinstance(text, str):
            return text

        if text.startswith("$") and len(text) > 1:
            param_name = text[1:]
            if param_name in parameters:
                return parameters[param_name]
            logger.debug(
                "PipelineInterpreter: Параметр '%s' не знайдено в parameters, залишаємо як є",
                param_name
            )
            return text

        if text.startswith("{{") and text.endswith("}}") and len(text) > 4:
            param_name = text[2:-2].strip()
            if param_name in parameters:
                return parameters[param_name]
            logger.debug(
                "PipelineInterpreter: Параметр '{{%s}}' не знайдено в parameters, залишаємо як є",
                param_name
            )
            return text

        if "$" in text or "{{" in text:
            result = text
            import re
            for param_name, param_value in parameters.items():
                result = re.sub(r'\$' + re.escape(param_name) + r'(?!\w)', str(param_value), result)
                result = result.replace(f"{{{{{param_name}}}}}", str(param_value))
            return result

        return text
