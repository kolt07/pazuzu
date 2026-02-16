# -*- coding: utf-8 -*-
"""
Сервіс метаданих застосунку: формує повний контекст про ціль застосунку,
дані, джерела та доступні інструменти для LLM агентів.
"""

import logging
import yaml
from pathlib import Path
from typing import Dict, Any, Optional, List
from config.settings import Settings

logger = logging.getLogger(__name__)


class AppMetadataService:
    """
    Сервіс для роботи з метаданими застосунку.
    Читає базові метаданні з YAML та доповнює їх динамічною інформацією
    про MCP tools та структуру колекцій.
    """
    
    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self._base_metadata: Optional[Dict[str, Any]] = None
        self._metadata_path = Path(__file__).parent.parent.parent / "config" / "app_metadata.yaml"
    
    def _load_base_metadata(self) -> Dict[str, Any]:
        """Завантажує базові метаданні з YAML файлу."""
        if self._base_metadata is not None:
            return self._base_metadata
        
        try:
            with open(self._metadata_path, 'r', encoding='utf-8') as f:
                self._base_metadata = yaml.safe_load(f)
            logger.debug("Завантажено базові метаданні з %s", self._metadata_path)
        except Exception as e:
            logger.error("Помилка завантаження метаданих: %s", e)
            self._base_metadata = {}
        
        return self._base_metadata
    
    def _get_mcp_tools_info(self) -> List[Dict[str, Any]]:
        """
        Формує інформацію про доступні MCP tools.
        Може бути доповнено динамічно з LangChainAgentService.
        """
        return [
            {
                "name": "get_database_schema",
                "description": "Отримує повну схему метаданих всіх колекцій бази даних",
                "category": "schema"
            },
            {
                "name": "get_collection_info",
                "description": "Отримує детальну інформацію про конкретну колекцію",
                "category": "schema",
                "parameters": ["collection_name"]
            },
            {
                "name": "get_data_dictionary",
                "description": "Отримує повний Data Dictionary з описом всіх колекцій та полів",
                "category": "schema"
            },
            {
                "name": "execute_query",
                "description": "Виконує безпечний запит до MongoDB з фільтрами та проекцією",
                "category": "query",
                "parameters": ["collection", "filters", "projection", "limit"],
                "limitations": ["$regex оператор заборонений", "Максимум 5000 результатів"]
            },
            {
                "name": "execute_aggregation",
                "description": "Виконує MongoDB aggregation pipeline для складних запитів",
                "category": "query",
                "parameters": ["collection_name", "pipeline", "limit"],
                "limitations": ["Заборонені stages: $out, $merge", "Максимальна глибина вкладеності: 5"]
            },
            {
                "name": "get_distinct_values",
                "description": "Отримує унікальні значення поля в колекції для аналізу перед фільтрацією",
                "category": "query",
                "parameters": ["collection_name", "field_path", "unwrap_array"]
            },
            {
                "name": "save_query_to_temp_collection",
                "description": "Виконує запит та зберігає результати в тимчасову вибірку для подальшого експорту",
                "category": "export",
                "parameters": ["collection", "filters", "aggregation_pipeline", "limit"]
            },
            {
                "name": "export_from_temp_collection",
                "description": "Експортує дані з тимчасової вибірки у Excel файл",
                "category": "export",
                "parameters": ["temp_collection_id", "format", "filename_prefix"]
            },
            {
                "name": "execute_analytics",
                "description": "Виконує аналітичні запити з метриками та агрегаціями",
                "category": "analytics",
                "parameters": ["collection", "metric", "groupBy", "filters"],
                "limitations": ["Використовує тільки попередньо визначені метрики"]
            },
            {
                "name": "list_metrics",
                "description": "Повертає список доступних метрик аналітики",
                "category": "analytics"
            },
            {
                "name": "generate_report",
                "description": "Генерує звіт у вказаному форматі з джерела даних",
                "category": "report",
                "parameters": ["format", "dataSource", "columns", "template"]
            },
            {
                "name": "geocode_address",
                "description": "Геокодує адресу або топонім у координати та форматує адресу",
                "category": "geocoding",
                "parameters": ["address", "region"],
                "limitations": ["Результати кешуються"]
            },
            {
                "name": "trigger_data_update",
                "description": "Ініціює оновлення даних з джерел (ProZorro або OLX)",
                "category": "data_update",
                "parameters": ["source", "days"]
            }
        ]
    
    def get_full_metadata(self) -> Dict[str, Any]:
        """
        Повертає повні метаданні застосунку, включаючи:
        - Базові метаданні з YAML
        - Інформацію про MCP tools
        - Структуру колекцій (якщо доступна)
        """
        base = self._load_base_metadata()
        
        metadata = {
            "app": base.get("app", {}),
            "data_sources": base.get("data_sources", {}),
            "collections": base.get("collections", {}),
            "available_operations": base.get("available_operations", []),
            "response_formats": base.get("response_formats", {}),
            "mcp_tools": self._get_mcp_tools_info()
        }
        
        return metadata
    
    def get_metadata_for_llm(self, max_length: Optional[int] = None) -> str:
        """
        Формує текстовий опис метаданих для включення в системний промпт LLM.
        
        Args:
            max_length: Максимальна довжина тексту (якщо вказано, текст буде обрізано)
        
        Returns:
            Текстовий опис метаданих
        """
        metadata = self.get_full_metadata()
        parts = []
        
        # Ціль застосунку
        app_info = metadata.get("app", {})
        if app_info.get("purpose"):
            parts.append(f"# Ціль застосунку\n\n{app_info['name']}: {app_info['purpose']}")
        
        # Джерела даних
        parts.append("\n# Джерела даних\n")
        for source_key, source_info in metadata.get("data_sources", {}).items():
            parts.append(f"## {source_info.get('name', source_key)}")
            parts.append(f"{source_info.get('description', '')}")
            parts.append(f"Колекція: {source_info.get('collection', 'N/A')}")
            if source_info.get("key_fields"):
                parts.append("Ключові поля:")
                for field in source_info["key_fields"]:
                    parts.append(f"  - {field}")
            parts.append("")
        
        # Колекції та їх можливості
        parts.append("# Доступні колекції та їх можливості\n")
        for coll_key, coll_info in metadata.get("collections", {}).items():
            parts.append(f"## {coll_key}")
            parts.append(f"{coll_info.get('description', '')}")
            if coll_info.get("key_metrics"):
                parts.append("Ключові метрики:")
                for metric in coll_info["key_metrics"]:
                    parts.append(f"  - {metric}")
            if coll_info.get("filtering_capabilities"):
                parts.append("Можливості фільтрації:")
                for cap in coll_info["filtering_capabilities"]:
                    parts.append(f"  - {cap}")
            if coll_info.get("limitations"):
                parts.append("Обмеження:")
                for lim in coll_info["limitations"]:
                    parts.append(f"  - {lim}")
            parts.append("")
        
        # Доступні операції
        operations = metadata.get("available_operations", [])
        if operations:
            parts.append("# Доступні операції\n")
            for op in operations:
                parts.append(f"- {op}")
            parts.append("")
        
        # Формати відповіді
        response_formats = metadata.get("response_formats", {})
        if response_formats:
            parts.append("# Формати відповіді\n")
            for fmt_key, fmt_info in response_formats.items():
                parts.append(f"## {fmt_key}")
                parts.append(f"{fmt_info.get('description', '')}")
                if fmt_info.get("use_cases"):
                    parts.append("Випадки використання:")
                    for case in fmt_info["use_cases"]:
                        parts.append(f"  - {case}")
                parts.append("")
        
        # MCP Tools
        tools = metadata.get("mcp_tools", [])
        if tools:
            parts.append("# Доступні інструменти (MCP Tools)\n")
            tools_by_category = {}
            for tool in tools:
                category = tool.get("category", "other")
                if category not in tools_by_category:
                    tools_by_category[category] = []
                tools_by_category[category].append(tool)
            
            for category, category_tools in tools_by_category.items():
                parts.append(f"## {category.title()}")
                for tool in category_tools:
                    parts.append(f"### {tool['name']}")
                    parts.append(f"{tool.get('description', '')}")
                    if tool.get("parameters"):
                        parts.append(f"Параметри: {', '.join(tool['parameters'])}")
                    if tool.get("limitations"):
                        parts.append("Обмеження:")
                        for lim in tool["limitations"]:
                            parts.append(f"  - {lim}")
                    parts.append("")
        
        result = "\n".join(parts)
        
        if max_length and len(result) > max_length:
            result = result[:max_length - 50] + "\n\n... (обрізано)"
        
        return result
    
    def get_collections_summary(self) -> str:
        """Повертає короткий опис колекцій для контексту."""
        metadata = self.get_full_metadata()
        parts = []
        
        for coll_key, coll_info in metadata.get("collections", {}).items():
            parts.append(f"- **{coll_key}**: {coll_info.get('description', '')}")
        
        return "\n".join(parts)
    
    def get_data_sources_summary(self) -> str:
        """Повертає короткий опис джерел даних."""
        metadata = self.get_full_metadata()
        parts = []
        
        for source_key, source_info in metadata.get("data_sources", {}).items():
            parts.append(f"- **{source_info.get('name', source_key)}**: {source_info.get('description', '')[:200]}...")
        
        return "\n".join(parts)
