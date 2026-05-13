# -*- coding: utf-8 -*-
"""Застосування runtime-прапорців LLM-агента з Mongo до Settings (Mini App / процес)."""

from __future__ import annotations

import logging
from typing import Any, Dict

from data.repositories.agent_runtime_settings_repository import AgentRuntimeSettingsRepository

logger = logging.getLogger(__name__)


class AgentRuntimeSettingsService:
    """Читає `agent_runtime_settings` і оновлює об'єкт Settings у пам'яті."""

    def __init__(self) -> None:
        self._repo = AgentRuntimeSettingsRepository()

    def apply_mongo_to_settings(self, settings: Any) -> None:
        """Викликати після завантаження YAML/env (наприклад при старті mini app)."""
        try:
            doc = self._repo.get_document()
            if not doc:
                return
            if "llm_agent_tool_retrieval_enabled" in doc:
                settings.llm_agent_tool_retrieval_enabled = bool(doc["llm_agent_tool_retrieval_enabled"])
        except Exception as e:
            logger.debug("apply_mongo_to_settings: %s", e)

    def get_effective_flags(self, settings: Any) -> Dict[str, Any]:
        return {
            "llm_agent_tool_retrieval_enabled": bool(getattr(settings, "llm_agent_tool_retrieval_enabled", False)),
        }

    def set_tool_retrieval_enabled(self, settings: Any, value: bool) -> Dict[str, Any]:
        self._repo.upsert_partial({"llm_agent_tool_retrieval_enabled": bool(value)})
        settings.llm_agent_tool_retrieval_enabled = bool(value)
        return self.get_effective_flags(settings)
