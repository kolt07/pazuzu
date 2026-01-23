# -*- coding: utf-8 -*-
"""
MCP сервер для виконання аналітичних запитів з метриками та агрегаціями.
"""

import json
import sys
from typing import Any, Dict
from mcp.server.fastmcp import FastMCP
from config.settings import Settings
from data.database.connection import MongoDBConnection
from utils.analytics_builder import AnalyticsBuilder
from utils.analytics_metrics import AnalyticsMetrics

# Ініціалізуємо MCP сервер
mcp = FastMCP("analytics-mcp", json_response=True)

# Глобальні змінні
_analytics_builder: AnalyticsBuilder = None


def initialize_connection():
    """Ініціалізує підключення до бази даних."""
    global _analytics_builder
    
    if _analytics_builder is None:
        try:
            settings = Settings()
            MongoDBConnection.initialize(settings)
            _analytics_builder = AnalyticsBuilder()
        except Exception as e:
            print(f"Помилка ініціалізації підключення до БД: {e}", file=sys.stderr)
            raise


@mcp.tool()
def execute_analytics(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Виконує аналітичний запит з метриками та агрегаціями.
    
    Приймає аналітичний запит у форматі:
    {
        "metric": "average_price_per_m2",
        "groupBy": ["region"],
        "filters": {
            "status": "finished",
            "dateEnd": {
                "from": "2024-01-01",
                "to": "2024-12-31"
            }
        }
    }
    
    Args:
        query: Аналітичний запит у форматі JSON
        
    Returns:
        Словник з результатами виконання запиту
    """
    global _analytics_builder
    
    try:
        initialize_connection()
        
        # Якщо query передано як рядок, парсимо його
        if isinstance(query, str):
            query = json.loads(query)
        
        # Виконуємо запит
        result = _analytics_builder.execute_analytics_query(query)
        
        return result
    except json.JSONDecodeError as e:
        return {
            'success': False,
            'error': f'Помилка парсингу JSON: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Помилка виконання запиту: {str(e)}'
        }


@mcp.tool()
def validate_analytics_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Валідує аналітичний запит без його виконання.
    
    Args:
        query: Аналітичний запит у форматі JSON
        
    Returns:
        Словник з результатом валідації
    """
    global _analytics_builder
    
    try:
        initialize_connection()
        
        # Якщо query передано як рядок, парсимо його
        if isinstance(query, str):
            query = json.loads(query)
        
        # Валідуємо запит
        is_valid, error = _analytics_builder.validate_analytics_query(query)
        
        if is_valid:
            return {
                'success': True,
                'valid': True,
                'message': 'Запит валідний'
            }
        else:
            return {
                'success': True,
                'valid': False,
                'error': error
            }
    except json.JSONDecodeError as e:
        return {
            'success': False,
            'error': f'Помилка парсингу JSON: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Помилка валідації: {str(e)}'
        }


@mcp.tool()
def list_metrics() -> Dict[str, Any]:
    """
    Повертає список доступних метрик.
    
    Returns:
        Словник зі списком метрик
    """
    metrics = AnalyticsMetrics.list_metrics()
    
    return {
        'success': True,
        'metrics': metrics
    }


@mcp.tool()
def get_metric_info(metric_name: str) -> Dict[str, Any]:
    """
    Отримує детальну інформацію про метрику.
    
    Args:
        metric_name: Назва метрики
        
    Returns:
        Словник з інформацією про метрику
    """
    metric = AnalyticsMetrics.get_metric(metric_name)
    
    if not metric:
        available_metrics = [m['name'] for m in AnalyticsMetrics.list_metrics()]
        return {
            'success': False,
            'error': f'Метрика "{metric_name}" не існує. Доступні метрики: {", ".join(available_metrics)}'
        }
    
    return {
        'success': True,
        'metric': {
            'name': metric.name,
            'description': metric.description,
            'unit': metric.unit,
            'required_fields': metric.required_fields
        }
    }


@mcp.tool()
def get_allowed_group_by_fields() -> Dict[str, Any]:
    """
    Повертає список дозволених полів для групування.
    
    Returns:
        Словник зі списком полів
    """
    return {
        'success': True,
        'group_by_fields': AnalyticsMetrics.ALLOWED_GROUP_BY_FIELDS
    }


def main():
    """Головна функція для запуску MCP сервера."""
    # Використовуємо stdio transport для MCP
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
