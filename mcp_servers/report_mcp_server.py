# -*- coding: utf-8 -*-
"""
MCP сервер для генерації звітів у різних форматах.
"""

import json
import sys
from typing import Any, Dict
from mcp.server.fastmcp import FastMCP
from config.settings import Settings
from data.database.connection import MongoDBConnection
from utils.report_generator import ReportGenerator
from utils.report_templates import ReportTemplates

# Ініціалізуємо MCP сервер
mcp = FastMCP("report-mcp", json_response=True)

# Глобальні змінні
_report_generator: ReportGenerator = None


def initialize_connection():
    """Ініціалізує підключення до бази даних."""
    global _report_generator
    
    if _report_generator is None:
        try:
            settings = Settings()
            MongoDBConnection.initialize(settings)
            _report_generator = ReportGenerator()
        except Exception as e:
            print(f"Помилка ініціалізації підключення до БД: {e}", file=sys.stderr)
            raise


@mcp.tool()
def generate_report(request: Dict[str, Any], return_base64: bool = True) -> Dict[str, Any]:
    """
    Генерує звіт у вказаному форматі на основі шаблону та джерела даних.
    
    Приймає запит у форматі:
    {
        "format": "xlsx",
        "template": "auction_summary",
        "dataSource": "analytics-mcp:{\"metric\":\"average_price_per_m2\",\"groupBy\":[\"region\"]}",
        "columns": ["region", "avg_price_m2", "auctions_count"]
    }
    
    Або спрощений формат:
    {
        "format": "xlsx",
        "template": "auction_summary",
        "dataSource": "analytics-mcp:average_price_per_m2",
        "columns": ["region", "value"]
    }
    
    Args:
        request: Запит на генерацію звіту
        return_base64: Чи повертати файл у base64 (True) або зберегти та повернути URL (False)
        
    Returns:
        Словник з результатом генерації
    """
    global _report_generator
    
    try:
        initialize_connection()
        
        # Якщо request передано як рядок, парсимо його
        if isinstance(request, str):
            request = json.loads(request)
        
        # Генеруємо звіт
        result = _report_generator.generate_report(request, return_base64=return_base64)
        
        return result
    except json.JSONDecodeError as e:
        return {
            'success': False,
            'error': f'Помилка парсингу JSON: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Помилка генерації звіту: {str(e)}'
        }


@mcp.tool()
def validate_report_request(request: Dict[str, Any]) -> Dict[str, Any]:
    """
    Валідує запит на генерацію звіту без його виконання.
    
    Args:
        request: Запит на генерацію звіту
        
    Returns:
        Словник з результатом валідації
    """
    global _report_generator
    
    try:
        initialize_connection()
        
        # Якщо request передано як рядок, парсимо його
        if isinstance(request, str):
            request = json.loads(request)
        
        # Валідуємо запит
        is_valid, error = _report_generator.validate_report_request(request)
        
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
def list_templates() -> Dict[str, Any]:
    """
    Повертає список доступних шаблонів звітів.
    
    Returns:
        Словник зі списком шаблонів
    """
    templates = ReportTemplates.list_templates()
    
    return {
        'success': True,
        'templates': templates
    }


@mcp.tool()
def get_template_info(template_name: str) -> Dict[str, Any]:
    """
    Отримує детальну інформацію про шаблон.
    
    Args:
        template_name: Назва шаблону
        
    Returns:
        Словник з інформацією про шаблон
    """
    template = ReportTemplates.get_template(template_name)
    
    if not template:
        available_templates = [t['name'] for t in ReportTemplates.list_templates()]
        return {
            'success': False,
            'error': f'Шаблон "{template_name}" не існує. Доступні шаблони: {", ".join(available_templates)}'
        }
    
    return {
        'success': True,
        'template': {
            'name': template.name,
            'description': template.description,
            'format': template.format,
            'default_columns': template.default_columns,
            'column_headers': template.column_headers,
            'required_columns': template.required_columns or []
        }
    }


@mcp.tool()
def get_supported_formats() -> Dict[str, Any]:
    """
    Повертає список підтримуваних форматів файлів.
    
    Returns:
        Словник зі списком форматів
    """
    return {
        'success': True,
        'formats': [
            {
                'name': 'xlsx',
                'description': 'Microsoft Excel (OpenXML)',
                'mime_type': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
            },
            {
                'name': 'csv',
                'description': 'Comma-Separated Values',
                'mime_type': 'text/csv'
            },
            {
                'name': 'json',
                'description': 'JavaScript Object Notation',
                'mime_type': 'application/json'
            }
        ]
    }


def main():
    """Головна функція для запуску MCP сервера."""
    # Використовуємо stdio transport для MCP
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
