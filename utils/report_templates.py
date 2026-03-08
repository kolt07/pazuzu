# -*- coding: utf-8 -*-
"""
Модуль для визначення шаблонів звітів.
"""

from typing import Dict, Any, List, Optional
from dataclasses import dataclass


@dataclass
class ReportTemplate:
    """Визначення шаблону звіту."""
    name: str
    description: str
    format: str  # xlsx, csv, json
    default_columns: List[str]
    column_headers: Dict[str, str]  # Мапінг колонок на українські назви
    required_columns: List[str] = None


class ReportTemplates:
    """Клас для управління шаблонами звітів."""
    
    TEMPLATES: Dict[str, ReportTemplate] = {
        'auction_summary': ReportTemplate(
            name='auction_summary',
            description='Звіт з підсумками аукціонів',
            format='xlsx',
            default_columns=['region', 'avg_price_m2', 'auctions_count'],
            column_headers={
                'region': 'Область',
                'avg_price_m2': 'Середня ціна за м²',
                'auctions_count': 'Кількість аукціонів'
            },
            required_columns=['region']
        ),
        'price_analysis': ReportTemplate(
            name='price_analysis',
            description='Аналіз цін по регіонах',
            format='xlsx',
            default_columns=['region', 'total_price', 'avg_price_m2', 'min_price', 'max_price', 'count'],
            column_headers={
                'region': 'Область',
                'total_price': 'Загальна ціна',
                'avg_price_m2': 'Середня ціна за м²',
                'min_price': 'Мінімальна ціна',
                'max_price': 'Максимальна ціна',
                'count': 'Кількість'
            },
            required_columns=['region']
        ),
        'property_types': ReportTemplate(
            name='property_types',
            description='Аналіз по типах нерухомості',
            format='xlsx',
            default_columns=['property_type', 'avg_price_m2', 'count'],
            column_headers={
                'property_type': 'Тип нерухомості',
                'avg_price_m2': 'Середня ціна за м²',
                'count': 'Кількість'
            },
            required_columns=['property_type']
        ),
        'time_series': ReportTemplate(
            name='time_series',
            description='Динаміка по часу',
            format='xlsx',
            default_columns=['year', 'month', 'avg_price_m2', 'count'],
            column_headers={
                'year': 'Рік',
                'month': 'Місяць',
                'avg_price_m2': 'Середня ціна за м²',
                'count': 'Кількість'
            },
            required_columns=['year', 'month']
        ),
        'simple_list': ReportTemplate(
            name='simple_list',
            description='Простий список даних',
            format='xlsx',
            default_columns=[],
            column_headers={},
            required_columns=[]
        )
    }
    
    @classmethod
    def get_template(cls, template_name: str) -> Optional[ReportTemplate]:
        """
        Отримує шаблон за назвою.
        
        Args:
            template_name: Назва шаблону
            
        Returns:
            Шаблон або None
        """
        return cls.TEMPLATES.get(template_name)
    
    @classmethod
    def list_templates(cls) -> List[Dict[str, Any]]:
        """
        Повертає список всіх доступних шаблонів.
        
        Returns:
            Список словників з інформацією про шаблони
        """
        return [
            {
                'name': template.name,
                'description': template.description,
                'format': template.format,
                'default_columns': template.default_columns,
                'column_headers': template.column_headers,
                'required_columns': template.required_columns or []
            }
            for template in cls.TEMPLATES.values()
        ]
    
    @classmethod
    def is_valid_template(cls, template_name: str) -> bool:
        """
        Перевіряє, чи існує шаблон.
        
        Args:
            template_name: Назва шаблону
            
        Returns:
            True, якщо шаблон існує
        """
        return template_name in cls.TEMPLATES
    
    @classmethod
    def is_valid_format(cls, format_name: str) -> bool:
        """
        Перевіряє, чи підтримується формат.
        
        Args:
            format_name: Назва формату
            
        Returns:
            True, якщо формат підтримується
        """
        return format_name in ['xlsx', 'csv', 'json']
