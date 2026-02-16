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
        
        # Налаштування LLM (ініціалізуємо перед завантаженням конфігурації)
        self.llm_provider = os.getenv('LLM_PROVIDER', 'gemini')
        self.llm_model_name = os.getenv('LLM_MODEL_NAME', 'gemini-2.5-flash')  # Актуальна модель Gemini
        # Для безкоштовного тарифу Gemini ліміт 5 запитів/хвилину, тому використовуємо 4 для безпеки
        self.llm_rate_limit_calls_per_minute = int(os.getenv('LLM_RATE_LIMIT_CALLS_PER_MINUTE', '4'))
        self.llm_api_keys = {
            'gemini': os.getenv('LLM_API_KEY_GEMINI', ''),
            'openai': os.getenv('LLM_API_KEY_OPENAI', ''),
            'anthropic': os.getenv('LLM_API_KEY_ANTHROPIC', '')
        }
        # Параметри циклу агента (ітерації, токени, температура, time budget)
        self.llm_agent_max_iterations = int(os.getenv('LLM_AGENT_MAX_ITERATIONS', '10'))
        self.llm_agent_max_output_tokens = int(os.getenv('LLM_AGENT_MAX_OUTPUT_TOKENS', '8192'))
        self.llm_agent_temperature = float(os.getenv('LLM_AGENT_TEMPERATURE', '0.7'))
        _tb = os.getenv('LLM_AGENT_TIME_BUDGET_SECONDS', '')
        self.llm_agent_time_budget_seconds = int(_tb) if _tb and _tb.isdigit() else None
        
        # Налаштування Telegram бота
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.telegram_users_config_path = os.getenv(
            'TELEGRAM_USERS_CONFIG_PATH',
            str(Path(__file__).parent / 'users.yaml')
        )
        
        # Google Maps API (геокодування адрес і топонімів)
        self.google_maps_api_key = os.getenv('GOOGLE_MAPS_API_KEY', '')

        # Налаштування MongoDB
        self.mongodb_host = os.getenv('MONGODB_HOST', 'localhost')
        self.mongodb_port = int(os.getenv('MONGODB_PORT', '27017'))
        self.mongodb_database_name = os.getenv('MONGODB_DATABASE_NAME', 'pazuzu')
        self.mongodb_username = os.getenv('MONGODB_USERNAME', '')
        self.mongodb_password = os.getenv('MONGODB_PASSWORD', '')
        self.mongodb_auth_source = os.getenv('MONGODB_AUTH_SOURCE', 'admin')

        # Регламентне фонове оновлення даних (інтервал у хвилинах; 0 = вимкнено)
        self.background_update_interval_minutes = int(
            os.getenv('BACKGROUND_UPDATE_INTERVAL_MINUTES', '10')
        )

        # Telegram Mini App (веб-застосунок у Telegram)
        self.mini_app_port = int(os.getenv('MINI_APP_PORT', '8000'))
        self.mini_app_base_url = os.getenv('MINI_APP_BASE_URL', '')  # HTTPS URL для BotFather, напр. https://example.com

        # Маршрутизація: confidence threshold та уточнення при низькій впевненості
        self.routing_confidence_threshold = float(os.getenv('ROUTING_CONFIDENCE_THRESHOLD', '0.7'))
        
        # Новий потік обробки агентів (за замовчуванням увімкнено)
        self.use_new_agent_flow = os.getenv('USE_NEW_AGENT_FLOW', 'true').lower() == 'true'
        self.routing_ask_on_low_confidence = os.getenv('ROUTING_ASK_ON_LOW_CONFIDENCE', 'false').lower() in ('true', '1', 'yes')

        # Security layer: rate limit та max complexity запиту
        self.rate_limit_requests_per_minute = int(os.getenv('RATE_LIMIT_REQUESTS_PER_MINUTE', '30'))
        self.max_query_complexity_length = int(os.getenv('MAX_QUERY_COMPLEXITY_LENGTH', '8000'))

        # Ліміти експорту та артефактів
        self.export_max_rows = int(os.getenv('EXPORT_MAX_ROWS', '50000'))
        _ems = os.getenv('EXPORT_MAX_FILE_SIZE_MB', '50')
        self.export_max_file_size_mb = int(_ems) if _ems.isdigit() else 50
        self.exports_per_user_per_day = int(os.getenv('EXPORTS_PER_USER_PER_DAY', '20'))
        self.artifact_ttl_seconds = int(os.getenv('ARTIFACT_TTL_SECONDS', '3600'))
        self.export_confirm_rows_threshold = int(os.getenv('EXPORT_CONFIRM_ROWS_THRESHOLD', '50000'))
        
        # Завантаження конфігурації з YAML файлу (перезаписує значення за замовчуванням)
        self._load_config()
    
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
                    if config:
                        # Налаштування LLM
                        if 'llm' in config:
                            llm_config = config['llm']
                            if 'provider' in llm_config:
                                self.llm_provider = llm_config['provider']
                            if 'model_name' in llm_config:
                                self.llm_model_name = llm_config['model_name']
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
                            if 'agent' in llm_config:
                                agent_config = llm_config['agent']
                                if 'max_iterations' in agent_config:
                                    self.llm_agent_max_iterations = int(agent_config['max_iterations'])
                                if 'max_output_tokens' in agent_config:
                                    self.llm_agent_max_output_tokens = int(agent_config['max_output_tokens'])
                                if 'temperature' in agent_config:
                                    self.llm_agent_temperature = float(agent_config['temperature'])
                                if 'time_budget_seconds' in agent_config:
                                    self.llm_agent_time_budget_seconds = int(agent_config['time_budget_seconds'])

                        # Налаштування Telegram бота
                        if 'telegram' in config:
                            telegram_config = config['telegram']
                            if 'bot_token' in telegram_config:
                                self.telegram_bot_token = telegram_config['bot_token']
                            if 'users_config_path' in telegram_config:
                                self.telegram_users_config_path = telegram_config['users_config_path']
                        
                        # Google Maps
                        if 'google_maps' in config:
                            gm = config['google_maps']
                            if isinstance(gm.get('api_key'), str):
                                self.google_maps_api_key = gm['api_key']

                        # Налаштування MongoDB
                        if 'mongodb' in config:
                            mongodb_config = config['mongodb']
                            if 'host' in mongodb_config:
                                self.mongodb_host = mongodb_config['host']
                            if 'port' in mongodb_config:
                                self.mongodb_port = int(mongodb_config['port'])
                            if 'database_name' in mongodb_config:
                                self.mongodb_database_name = mongodb_config['database_name']
                            if 'username' in mongodb_config:
                                self.mongodb_username = mongodb_config['username']
                            if 'password' in mongodb_config:
                                self.mongodb_password = mongodb_config['password']
                            if 'auth_source' in mongodb_config:
                                self.mongodb_auth_source = mongodb_config['auth_source']

                        # Регламентне фонове оновлення даних
                        if 'background_update' in config:
                            bu = config['background_update']
                            if 'interval_minutes' in bu:
                                self.background_update_interval_minutes = int(bu['interval_minutes'])

                        # Telegram Mini App (base_url — повний HTTPS URL, який відкриває Telegram, напр. ngrok)
                        if 'mini_app' in config:
                            ma = config['mini_app']
                            if 'port' in ma:
                                self.mini_app_port = int(ma['port'])
                            if ma.get('base_url'):
                                self.mini_app_base_url = str(ma['base_url']).strip()
                                print("Конфіг: mini_app base_url встановлено з config.yaml")

                        # Маршрутизація (confidence, уточнення)
                        if 'routing' in config:
                            r = config['routing']
                            if 'confidence_threshold' in r:
                                self.routing_confidence_threshold = float(r['confidence_threshold'])
                            if 'ask_on_low_confidence' in r:
                                self.routing_ask_on_low_confidence = bool(r['ask_on_low_confidence'])

                        if 'security_layer' in config:
                            sl = config['security_layer']
                            if 'rate_limit_requests_per_minute' in sl:
                                self.rate_limit_requests_per_minute = int(sl['rate_limit_requests_per_minute'])
                            if 'max_query_complexity_length' in sl:
                                self.max_query_complexity_length = int(sl['max_query_complexity_length'])

                        if 'limits' in config:
                            lim = config['limits']
                            if 'export_max_rows' in lim:
                                self.export_max_rows = int(lim['export_max_rows'])
                            if 'export_max_file_size_mb' in lim:
                                self.export_max_file_size_mb = int(lim['export_max_file_size_mb'])
                            if 'exports_per_user_per_day' in lim:
                                self.exports_per_user_per_day = int(lim['exports_per_user_per_day'])
                            if 'artifact_ttl_seconds' in lim:
                                self.artifact_ttl_seconds = int(lim['artifact_ttl_seconds'])
                            if 'export_confirm_rows_threshold' in lim:
                                self.export_confirm_rows_threshold = int(lim['export_confirm_rows_threshold'])
            except Exception as e:
                print(f"Попередження: не вдалося завантажити конфігурацію з {config_path}: {e}")
                print("Використовуються значення за замовчуванням або змінні оточення")

