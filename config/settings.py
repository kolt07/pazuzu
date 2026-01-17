# -*- coding: utf-8 -*-
"""
Налаштування застосунку.
"""

import os
import yaml
from pathlib import Path
from typing import Optional, Dict, Any


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
        
        # Завантаження конфігурації з YAML файлу
        self._load_config()
        
        # Налаштування LLM
        self.llm_provider = os.getenv('LLM_PROVIDER', 'gemini')
        self.llm_rate_limit_calls_per_minute = int(os.getenv('LLM_RATE_LIMIT_CALLS_PER_MINUTE', '15'))
        self.llm_api_keys = {
            'gemini': os.getenv('LLM_API_KEY_GEMINI', ''),
            'openai': os.getenv('LLM_API_KEY_OPENAI', ''),
            'anthropic': os.getenv('LLM_API_KEY_ANTHROPIC', '')
        }
    
    def _load_config(self) -> None:
        """
        Завантажує конфігурацію з YAML файлу, якщо він існує.
        Конфігурація з файлу має пріоритет над змінними оточення.
        """
        config_path = Path(__file__).parent / 'config.yaml'
        if config_path.exists():
            try:
                with open(config_path, 'r', encoding='utf-8') as f:
                    config = yaml.safe_load(f)
                    if config and 'llm' in config:
                        llm_config = config['llm']
                        if 'provider' in llm_config:
                            self.llm_provider = llm_config['provider']
                        if 'rate_limit' in llm_config and 'calls_per_minute' in llm_config['rate_limit']:
                            self.llm_rate_limit_calls_per_minute = llm_config['rate_limit']['calls_per_minute']
                        if 'api_keys' in llm_config:
                            api_keys = llm_config['api_keys']
                            if 'gemini' in api_keys:
                                self.llm_api_keys['gemini'] = api_keys['gemini']
                            if 'openai' in api_keys:
                                self.llm_api_keys['openai'] = api_keys['openai']
                            if 'anthropic' in api_keys:
                                self.llm_api_keys['anthropic'] = api_keys['anthropic']
            except Exception as e:
                print(f"Попередження: не вдалося завантажити конфігурацію з {config_path}: {e}")
                print("Використовуються значення за замовчуванням або змінні оточення")

