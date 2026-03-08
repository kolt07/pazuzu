# -*- coding: utf-8 -*-
"""
Агент-аналітик даних: виконує кроки пайплайну (запити до БД, збереження в тимчасові колекції, експорт у файли)
через виклик інструментів (MCP-backed). Може ітеративно оброблювати дані.
"""

import logging
from typing import Dict, Any, List, Optional, Callable

logger = logging.getLogger(__name__)


class AnalystAgent:
    """
    Аналітик у мультиагентному пайплайні: отримує кроки від планувальника (PlannerAgent) і виконує
    їх через run_tool_fn (MCP-інструменти). Не приймає рішень про те, які кроки виконувати — лише
    виконує задані дії (save_query_to_temp_collection, export_from_temp_collection тощо). Результати
    повертає помічнику для формування відповіді користувачу.
    """

    def __init__(self, run_tool_fn: Callable[[str, Dict[str, Any]], Any]):
        """
        Args:
            run_tool_fn: Функція (tool_name, tool_args) -> result (наприклад langchain_agent_service.run_tool).
        """
        self.run_tool_fn = run_tool_fn

    def run_step(
        self,
        step: Dict[str, Any],
        step_index: int,
        previous_results: List[Dict[str, Any]],
    ) -> Dict[str, Any]:
        """
        Виконує один крок пайплайну. Для export_from_temp_collection використовує
        temp_collection_id з результату попереднього кроку, якщо вказано temp_collection_id_from_step.

        Returns:
            Результат виконання інструмента (зазвичай dict з success, та можливо file_base64, filename).
        """
        action = step.get("action", "")
        params = dict(step.get("params", {}))
        from_step = step.get("temp_collection_id_from_step")
        if from_step is not None and 0 <= from_step < len(previous_results):
            prev = previous_results[from_step]
            if isinstance(prev, dict) and prev.get("success") and prev.get("temp_collection_id"):
                params["temp_collection_id"] = prev["temp_collection_id"]

        tool_name = self._action_to_tool(action)
        if not tool_name:
            return {"success": False, "error": f"Невідома дія: {action}"}
        try:
            result = self.run_tool_fn(tool_name, params)
            logger.info("AnalystAgent step %s: %s -> success=%s", step_index, action, result.get("success"))
            return result
        except Exception as e:
            logger.exception("AnalystAgent run_step: %s", e)
            return {"success": False, "error": str(e)}

    def _action_to_tool(self, action: str) -> Optional[str]:
        mapping = {
            "trigger_data_update": "trigger_data_update",
            "save_query_to_temp_collection": "save_query_to_temp_collection",
            "export_from_temp_collection": "export_from_temp_collection",
            "execute_query": "execute_query",
            "execute_aggregation": "execute_aggregation",
            "execute_analytics": "execute_analytics",
            "analytics_extracts_aggregate": "analytics_extracts_aggregate",
            "get_allowed_collections": "get_allowed_collections",
            "get_database_schema": "get_database_schema",
            "get_collection_info": "get_collection_info",
            "get_data_dictionary": "get_data_dictionary",
            "list_metrics": "list_metrics",
            "export_listings_to_file": "export_listings_to_file",
            "geocode_address": "geocode_address",
        }
        return mapping.get(action)
