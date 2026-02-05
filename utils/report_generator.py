# -*- coding: utf-8 -*-
"""
Модуль для генерації звітів у різних форматах.
"""

import json
import base64
import csv
from io import BytesIO, StringIO
from typing import Dict, Any, List, Optional, Tuple
from pathlib import Path
from datetime import datetime
from utils.report_templates import ReportTemplates, ReportTemplate
from utils.date_utils import KYIV_TZ
from utils.analytics_builder import AnalyticsBuilder
from utils.query_builder import QueryBuilder
from utils.file_utils import generate_excel_in_memory


class ReportGenerator:
    """Клас для генерації звітів."""
    
    def __init__(self):
        """Ініціалізація генератора звітів."""
        self.analytics_builder = AnalyticsBuilder()
        self.query_builder = QueryBuilder()
        self.temp_dir = Path('temp/reports')
        self.temp_dir.mkdir(parents=True, exist_ok=True)
    
    def validate_report_request(self, request: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Валідує запит на генерацію звіту.
        
        Args:
            request: Запит на генерацію звіту
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        # Перевірка обов'язкових полів
        if 'format' not in request:
            return False, "Поле 'format' є обов'язковим"
        
        format_name = request['format']
        if not ReportTemplates.is_valid_format(format_name):
            return False, f"Формат '{format_name}' не підтримується. Доступні формати: xlsx, csv, json"
        
        # Перевірка шаблону
        if 'template' in request:
            template_name = request['template']
            if not ReportTemplates.is_valid_template(template_name):
                available_templates = [t['name'] for t in ReportTemplates.list_templates()]
                return False, f"Шаблон '{template_name}' не існує. Доступні шаблони: {', '.join(available_templates)}"
        
        # Перевірка джерела даних
        if 'dataSource' not in request:
            return False, "Поле 'dataSource' є обов'язковим"
        
        data_source = request['dataSource']
        if not isinstance(data_source, str):
            return False, "Поле 'dataSource' має бути рядком у форматі 'mcp-server:query'"
        
        # Перевірка колонок
        if 'columns' not in request:
            return False, "Поле 'columns' є обов'язковим"
        
        if not isinstance(request['columns'], list):
            return False, "Поле 'columns' має бути списком"
        
        return True, None
    
    def _parse_data_source(self, data_source: str) -> Tuple[str, Dict[str, Any]]:
        """
        Парсить джерело даних у форматі 'mcp-server:query'.
        
        Підтримує формати:
        - 'analytics-mcp:average_price_per_m2' - простий формат з назвою метрики
        - 'analytics-mcp:{"metric":"average_price_per_m2","groupBy":["region"]}' - повний JSON запит
        - 'query-builder-mcp:{"collection":"prozorro_auctions","filters":{"status":"finished"}}' - запит до query-builder
        
        Args:
            data_source: Джерело даних у форматі 'mcp-server:query'
            
        Returns:
            Кортеж (server_name, query)
        """
        if ':' not in data_source:
            raise ValueError(f"Невірний формат dataSource: {data_source}. Очікується формат 'mcp-server:query'")
        
        parts = data_source.split(':', 1)
        server_name = parts[0]
        query_str = parts[1]
        
        # Перевірка підтримуваних серверів
        if server_name not in ['analytics-mcp', 'query-builder-mcp']:
            raise ValueError(f"Невідомий MCP сервер: {server_name}. Підтримувані сервери: analytics-mcp, query-builder-mcp")
        
        # Парсимо query
        try:
            # Спробуємо як JSON
            query = json.loads(query_str)
        except json.JSONDecodeError:
            # Якщо не JSON, спробуємо інтерпретувати як назву метрики (для analytics-mcp)
            if server_name == 'analytics-mcp':
                query = {'metric': query_str}
            else:
                raise ValueError(f"Невірний формат query в dataSource: {query_str}. Для query-builder-mcp потрібен JSON запит.")
        
        return server_name, query
    
    def _fetch_data_from_source(self, server_name: str, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Отримує дані з джерела.
        
        Args:
            server_name: Назва MCP сервера
            query: Запит до сервера
            
        Returns:
            Список даних
        """
        if server_name == 'analytics-mcp':
            # Виконуємо аналітичний запит
            result = self.analytics_builder.execute_analytics_query(query)
            if not result.get('success'):
                raise ValueError(f"Помилка отримання даних: {result.get('error', 'Невідома помилка')}")
            
            # Перетворюємо результати в список словників
            results = result.get('results', [])
            data = []
            for item in results:
                row = {}
                # Додаємо всі поля з результату
                for key, value in item.items():
                    if key != 'unit':  # Пропускаємо unit, він не потрібен в таблиці
                        row[key] = value
                data.append(row)
            
            return data
        
        elif server_name == 'query-builder-mcp':
            # Виконуємо запит
            result = self.query_builder.execute_query(query)
            if not result.get('success'):
                raise ValueError(f"Помилка отримання даних: {result.get('error', 'Невідома помилка')}")
            
            return result.get('results', [])
        
        else:
            raise ValueError(f"Невідомий MCP сервер: {server_name}")
    
    def _generate_excel(self, data: List[Dict[str, Any]], columns: List[str], 
                       column_headers: Dict[str, str]) -> BytesIO:
        """
        Генерує Excel файл.
        
        Args:
            data: Дані для звіту
            columns: Список колонок
            column_headers: Мапінг колонок на назви
            
        Returns:
            BytesIO з Excel файлом
        """
        # Підготовка даних
        formatted_data = []
        for row in data:
            formatted_row = {}
            for col in columns:
                formatted_row[col] = row.get(col, '')
            formatted_data.append(formatted_row)
        
        # Генеруємо Excel
        return generate_excel_in_memory(formatted_data, columns, column_headers)
    
    def _generate_csv(self, data: List[Dict[str, Any]], columns: List[str],
                     column_headers: Dict[str, str]) -> BytesIO:
        """
        Генерує CSV файл.
        
        Args:
            data: Дані для звіту
            columns: Список колонок
            column_headers: Мапінг колонок на назви
            
        Returns:
            BytesIO з CSV файлом
        """
        output = StringIO()
        writer = csv.DictWriter(output, fieldnames=columns, extrasaction='ignore')
        
        # Записуємо заголовки
        headers = {col: column_headers.get(col, col) for col in columns}
        writer.writerow(headers)
        
        # Записуємо дані
        for row in data:
            formatted_row = {}
            for col in columns:
                value = row.get(col, '')
                formatted_row[col] = str(value) if value is not None else ''
            writer.writerow(formatted_row)
        
        # Конвертуємо в BytesIO
        output.seek(0)
        csv_bytes = BytesIO(output.getvalue().encode('utf-8-sig'))  # BOM для Excel
        return csv_bytes
    
    def _generate_json(self, data: List[Dict[str, Any]], columns: List[str]) -> BytesIO:
        """
        Генерує JSON файл.
        
        Args:
            data: Дані для звіту
            columns: Список колонок
            
        Returns:
            BytesIO з JSON файлом
        """
        # Фільтруємо дані за колонками
        filtered_data = []
        for row in data:
            filtered_row = {col: row.get(col) for col in columns}
            filtered_data.append(filtered_row)
        
        # Генеруємо JSON
        json_str = json.dumps(filtered_data, ensure_ascii=False, indent=2, default=str)
        return BytesIO(json_str.encode('utf-8'))
    
    def generate_report(self, request: Dict[str, Any], return_base64: bool = True) -> Dict[str, Any]:
        """
        Генерує звіт на основі запиту.
        
        Args:
            request: Запит на генерацію звіту
            return_base64: Чи повертати файл у base64 (True) або зберегти та повернути URL (False)
            
        Returns:
            Словник з результатом генерації
        """
        # Валідація
        is_valid, error = self.validate_report_request(request)
        if not is_valid:
            return {
                'success': False,
                'error': error
            }
        
        try:
            # Отримуємо шаблон
            template = None
            if 'template' in request:
                template = ReportTemplates.get_template(request['template'])
            
            # Парсимо джерело даних
            server_name, query = self._parse_data_source(request['dataSource'])
            
            # Отримуємо дані
            data = self._fetch_data_from_source(server_name, query)
            
            if not data:
                return {
                    'success': False,
                    'error': 'Немає даних для генерації звіту'
                }
            
            # Визначаємо колонки
            columns = request.get('columns', [])
            if not columns and template:
                columns = template.default_columns
            
            # Визначаємо заголовки колонок
            column_headers = {}
            if template:
                column_headers = template.column_headers.copy()
            # Додаємо заголовки для колонок, яких немає в шаблоні
            for col in columns:
                if col not in column_headers:
                    column_headers[col] = col.replace('_', ' ').title()
            
            # Генеруємо файл
            format_name = request['format']
            if format_name == 'xlsx':
                file_data = self._generate_excel(data, columns, column_headers)
                mime_type = 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet'
                extension = 'xlsx'
            elif format_name == 'csv':
                file_data = self._generate_csv(data, columns, column_headers)
                mime_type = 'text/csv'
                extension = 'csv'
            elif format_name == 'json':
                file_data = self._generate_json(data, columns)
                mime_type = 'application/json'
                extension = 'json'
            else:
                return {
                    'success': False,
                    'error': f'Невідомий формат: {format_name}'
                }
            
            # Повертаємо результат
            if return_base64:
                # Конвертуємо в base64
                file_data.seek(0)
                file_base64 = base64.b64encode(file_data.read()).decode('utf-8')
                
                return {
                    'success': True,
                    'format': format_name,
                    'mime_type': mime_type,
                    'data': file_base64,
                    'encoding': 'base64',
                    'size': len(file_base64)
                }
            else:
                # Зберігаємо файл та повертаємо URL
                timestamp = datetime.now(KYIV_TZ).strftime('%Y%m%d_%H%M%S')
                filename = f"report_{timestamp}.{extension}"
                file_path = self.temp_dir / filename
                
                file_data.seek(0)
                with open(file_path, 'wb') as f:
                    f.write(file_data.read())
                
                # Повертаємо відносний шлях
                return {
                    'success': True,
                    'format': format_name,
                    'mime_type': mime_type,
                    'url': str(file_path),
                    'filename': filename,
                    'size': file_path.stat().st_size
                }
        
        except Exception as e:
            return {
                'success': False,
                'error': f'Помилка генерації звіту: {str(e)}'
            }
