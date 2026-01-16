# -*- coding: utf-8 -*-
"""
Налаштування застосунку.
"""

import os


class Settings:
    """Клас для зберігання налаштувань застосунку."""

    def __init__(self):
        """Ініціалізація налаштувань."""
        # Налаштування API ProZorro (тендери)
        self.prozorro_api_base_url = os.getenv(
            'PROZORRO_API_BASE_URL',
            'https://api.prozorro.gov.ua/api/2.5'
        )
        # Налаштування API ProZorro.Sale (аукціони)
        self.prozorro_sale_api_base_url = os.getenv(
            'PROZORRO_SALE_API_BASE_URL',
            'https://public.api.ea.openprocurement.org/api/2'
        )
        # Базовий URL для ендпоінта search/byDateModified (нова ЦБД)
        self.prozorro_sale_search_api_base_url = os.getenv(
            'PROZORRO_SALE_SEARCH_API_BASE_URL',
            'https://procedure.prozorro.sale/api'
        )
        self.prozorro_api_timeout = int(os.getenv('PROZORRO_API_TIMEOUT', '30'))
        
        # Категорія нерухомості (ДК 021:2015)
        self.real_estate_cpv_code = '70000000-1'
        
        # Налаштування для збереження даних
        self.temp_directory = os.getenv('TEMP_DIRECTORY', 'temp')
        
        # User-Agent для HTTP запитів
        self.user_agent = os.getenv(
            'USER_AGENT',
            'Prozzorro-Parser/1.0'
        )
        
        # Кількість днів для виборки оголошень (за замовчуванням 1)
        self.default_days_range = int(os.getenv('DEFAULT_DAYS_RANGE', '1'))
        
        # Список активних статусів для аукціонів
        self.active_auction_statuses = os.getenv(
            'ACTIVE_AUCTION_STATUSES',
            'active,active.tendering,active.auction,active.qualification'
        ).split(',')

