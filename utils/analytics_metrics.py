# -*- coding: utf-8 -*-
"""
Модуль для визначення метрик та їх формул для аналітики.
"""

from typing import Dict, Any, List, Callable, Optional
from datetime import datetime


class MetricDefinition:
    """Визначення метрики з формулою та описом."""
    
    def __init__(
        self,
        name: str,
        formula: Callable[[Dict[str, Any]], Optional[float]],
        description: str,
        unit: str = "",
        required_fields: List[str] = None
    ):
        """
        Ініціалізація визначення метрики.
        
        Args:
            name: Назва метрики
            formula: Функція для обчислення метрики з документа
            description: Опис метрики
            unit: Одиниця виміру
            required_fields: Список обов'язкових полів для обчислення
        """
        self.name = name
        self.formula = formula
        self.description = description
        self.unit = unit
        self.required_fields = required_fields or []
    
    def calculate(self, document: Dict[str, Any]) -> Optional[float]:
        """
        Обчислює значення метрики для документа.
        
        Args:
            document: Документ з даними
            
        Returns:
            Значення метрики або None, якщо не можна обчислити
        """
        try:
            return self.formula(document)
        except (KeyError, TypeError, ValueError, ZeroDivisionError):
            return None


class AnalyticsMetrics:
    """Клас для визначення та управління метриками аналітики."""
    
    # Допоміжні функції для витягування даних з auction_data
    @staticmethod
    def _get_nested_value(data: Dict[str, Any], path: List[str], default=None):
        """Отримує значення з вкладеної структури за шляхом."""
        current = data
        for key in path:
            if isinstance(current, dict):
                current = current.get(key)
            else:
                return default
            if current is None:
                return default
        return current
    
    @staticmethod
    def _parse_number(value: Any) -> Optional[float]:
        """Парсить число з різних форматів."""
        if value is None:
            return None
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            # Видаляємо пробіли та замінюємо кому на крапку
            cleaned = value.replace(' ', '').replace(',', '.')
            try:
                return float(cleaned)
            except ValueError:
                return None
        return None
    
    @staticmethod
    def _get_price_final(document: Dict[str, Any]) -> Optional[float]:
        """Отримує фінальну ціну з документа."""
        auction_data = document.get('auction_data', {})
        
        # Шукаємо в різних місцях
        paths = [
            ['value', 'amount'],  # Стандартний шлях ProZorro
            ['value', 'valueAddedTaxIncluded'],
            ['data', 'value', 'amount'],
            ['data', 'value', 'valueAddedTaxIncluded'],
        ]
        
        for path in paths:
            value = AnalyticsMetrics._get_nested_value(auction_data, path)
            if value is not None:
                return AnalyticsMetrics._parse_number(value)
        
        return None
    
    @staticmethod
    def _get_base_price(document: Dict[str, Any]) -> Optional[float]:
        """Отримує стартову ціну з документа."""
        auction_data = document.get('auction_data', {})
        
        # Шукаємо в різних місцях
        paths = [
            ['value', 'amount'],
            ['data', 'value', 'amount'],
            ['minimalStep', 'amount'],
            ['data', 'minimalStep', 'amount'],
        ]
        
        for path in paths:
            value = AnalyticsMetrics._get_nested_value(auction_data, path)
            if value is not None:
                return AnalyticsMetrics._parse_number(value)
        
        return None
    
    @staticmethod
    def _get_area(document: Dict[str, Any]) -> Optional[float]:
        """Отримує площу з документа."""
        auction_data = document.get('auction_data', {})
        
        # Шукаємо в різних місцях
        paths = [
            ['items', 0, 'quantity'],  # Можливо в items
            ['data', 'items', 0, 'quantity'],
        ]
        
        for path in paths:
            value = AnalyticsMetrics._get_nested_value(auction_data, path)
            if value is not None:
                parsed = AnalyticsMetrics._parse_number(value)
                if parsed and parsed > 0:
                    return parsed
        
        # Також шукаємо в LLM результаті, якщо є join
        if 'llm_result' in document:
            llm_result = document.get('llm_result', {})
            if isinstance(llm_result, dict):
                result_data = llm_result.get('result', {})
                if isinstance(result_data, dict):
                    # Шукаємо building_area_sqm або land_area_ha
                    building_area = AnalyticsMetrics._parse_number(
                        result_data.get('building_area_sqm')
                    )
                    if building_area and building_area > 0:
                        return building_area
                    
                    land_area = AnalyticsMetrics._parse_number(
                        result_data.get('land_area_ha')
                    )
                    if land_area and land_area > 0:
                        # Конвертуємо з гектарів в квадратні метри
                        return land_area * 10000
        
        return None
    
    @staticmethod
    def _get_building_area(document: Dict[str, Any]) -> Optional[float]:
        """Отримує площу будівлі з документа."""
        auction_data = document.get('auction_data', {})
        
        # Шукаємо в LLM результаті
        if 'llm_result' in document:
            llm_result = document.get('llm_result', {})
            if isinstance(llm_result, dict):
                result_data = llm_result.get('result', {})
                if isinstance(result_data, dict):
                    return AnalyticsMetrics._parse_number(
                        result_data.get('building_area_sqm')
                    )
        
        return None
    
    @staticmethod
    def _get_land_area(document: Dict[str, Any]) -> Optional[float]:
        """Отримує площу земельної ділянки з документа."""
        auction_data = document.get('auction_data', {})
        
        # Шукаємо в LLM результаті
        if 'llm_result' in document:
            llm_result = document.get('llm_result', {})
            if isinstance(llm_result, dict):
                result_data = llm_result.get('result', {})
                if isinstance(result_data, dict):
                    # Конвертуємо з гектарів в квадратні метри
                    land_area_ha = AnalyticsMetrics._parse_number(
                        result_data.get('land_area_ha')
                    )
                    if land_area_ha:
                        return land_area_ha * 10000
        
        return None
    
    @staticmethod
    def _get_region(document: Dict[str, Any]) -> Optional[str]:
        """Отримує регіон з документа."""
        auction_data = document.get('auction_data', {})
        
        # Шукаємо в LLM результаті
        if 'llm_result' in document:
            llm_result = document.get('llm_result', {})
            if isinstance(llm_result, dict):
                result_data = llm_result.get('result', {})
                if isinstance(result_data, dict):
                    addresses = result_data.get('addresses', [])
                    if addresses and isinstance(addresses, list) and len(addresses) > 0:
                        first_address = addresses[0]
                        if isinstance(first_address, dict):
                            return first_address.get('region')
        
        return None
    
    # Визначення метрик
    METRICS: Dict[str, MetricDefinition] = {
        'average_price_per_m2': MetricDefinition(
            name='average_price_per_m2',
            formula=lambda doc: (
                AnalyticsMetrics._get_price_final(doc) / AnalyticsMetrics._get_area(doc)
                if AnalyticsMetrics._get_area(doc) and AnalyticsMetrics._get_area(doc) > 0
                else None
            ),
            description='Середня ціна за квадратний метр',
            unit='UAH/m²',
            required_fields=['priceFinal', 'area']
        ),
        'total_price': MetricDefinition(
            name='total_price',
            formula=lambda doc: AnalyticsMetrics._get_price_final(doc),
            description='Загальна ціна',
            unit='UAH',
            required_fields=['priceFinal']
        ),
        'base_price': MetricDefinition(
            name='base_price',
            formula=lambda doc: AnalyticsMetrics._get_base_price(doc),
            description='Стартова ціна',
            unit='UAH',
            required_fields=['basePrice']
        ),
        'area': MetricDefinition(
            name='area',
            formula=lambda doc: AnalyticsMetrics._get_area(doc),
            description='Площа',
            unit='m²',
            required_fields=['area']
        ),
        'building_area': MetricDefinition(
            name='building_area',
            formula=lambda doc: AnalyticsMetrics._get_building_area(doc),
            description='Площа будівлі',
            unit='m²',
            required_fields=['buildingArea']
        ),
        'land_area': MetricDefinition(
            name='land_area',
            formula=lambda doc: AnalyticsMetrics._get_land_area(doc),
            description='Площа земельної ділянки',
            unit='m²',
            required_fields=['landArea']
        ),
        'count': MetricDefinition(
            name='count',
            formula=lambda doc: 1,
            description='Кількість записів',
            unit='шт',
            required_fields=[]
        ),
    }
    
    # Дозволені поля для групування
    ALLOWED_GROUP_BY_FIELDS = [
        'region',
        'city',
        'property_type',
        'status',
        'year',
        'month',
        'quarter'
    ]
    
    @classmethod
    def get_metric(cls, metric_name: str) -> Optional[MetricDefinition]:
        """
        Отримує визначення метрики за назвою.
        
        Args:
            metric_name: Назва метрики
            
        Returns:
            Визначення метрики або None
        """
        return cls.METRICS.get(metric_name)
    
    @classmethod
    def list_metrics(cls) -> List[Dict[str, Any]]:
        """
        Повертає список всіх доступних метрик.
        
        Returns:
            Список словників з інформацією про метрики
        """
        return [
            {
                'name': metric.name,
                'description': metric.description,
                'unit': metric.unit,
                'required_fields': metric.required_fields
            }
            for metric in cls.METRICS.values()
        ]
    
    @classmethod
    def is_valid_metric(cls, metric_name: str) -> bool:
        """
        Перевіряє, чи існує метрика.
        
        Args:
            metric_name: Назва метрики
            
        Returns:
            True, якщо метрика існує
        """
        return metric_name in cls.METRICS
    
    @classmethod
    def is_valid_group_by(cls, field: str) -> bool:
        """
        Перевіряє, чи дозволено групування за полем.
        
        Args:
            field: Назва поля
            
        Returns:
            True, якщо групування дозволено
        """
        return field in cls.ALLOWED_GROUP_BY_FIELDS
