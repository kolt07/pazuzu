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
        # Окремо: асистент (діалог, tools) — Gemini; парсинг описів — Ollama
        self.llm_assistant_provider = os.getenv('LLM_ASSISTANT_PROVIDER', 'gemini')
        self.llm_assistant_model_name = os.getenv('LLM_ASSISTANT_MODEL_NAME', 'gemini-2.5-flash')
        self.llm_parsing_provider = os.getenv('LLM_PARSING_PROVIDER', 'ollama')
        self.llm_parsing_model_name = os.getenv('LLM_PARSING_MODEL_NAME', 'gemma3:27b')
        # Flx — агент-інвестігейтор (дослідження локацій, побудова HTML-звітів)
        self.llm_investigator_provider = os.getenv('LLM_INVESTIGATOR_PROVIDER', 'gemini')
        self.llm_investigator_model_name = os.getenv('LLM_INVESTIGATOR_MODEL_NAME', 'gemini-2.5-pro')
        self.llm_investigator_temperature = float(os.getenv('LLM_INVESTIGATOR_TEMPERATURE', '0.4'))
        self.llm_investigator_max_iterations = int(os.getenv('LLM_INVESTIGATOR_MAX_ITERATIONS', '40'))
        self.llm_investigator_time_budget_seconds = int(os.getenv('LLM_INVESTIGATOR_TIME_BUDGET_SECONDS', '900'))
        self.llm_investigator_google_grounding = os.getenv('LLM_INVESTIGATOR_GOOGLE_GROUNDING', 'true').lower() in ('true', '1', 'yes')
        self.llm_investigator_max_steps_per_task = int(os.getenv('LLM_INVESTIGATOR_MAX_STEPS_PER_TASK', '5'))
        self.llm_investigator_web_search_provider = os.getenv('LLM_INVESTIGATOR_WEB_SEARCH_PROVIDER', 'duckduckgo').strip().lower()
        self.llm_investigator_web_search_model_name = os.getenv('LLM_INVESTIGATOR_WEB_SEARCH_MODEL_NAME', '').strip()
        self.llm_investigator_web_search_enabled = os.getenv('LLM_INVESTIGATOR_WEB_SEARCH_ENABLED', 'true').lower() in ('true', '1', 'yes')
        self.flx_source_wait_timeout_seconds = int(os.getenv('FLX_SOURCE_WAIT_TIMEOUT_SECONDS', '3600'))
        self.flx_static_maps_default_size = os.getenv('FLX_STATIC_MAPS_SIZE', '800x600')
        self.flx_static_maps_default_zoom = int(os.getenv('FLX_STATIC_MAPS_ZOOM', '13'))
        # Список дозволених інструментів для Flx (allow-list, без '*')
        _allowed_tools_raw = os.getenv(
            'FLX_ALLOWED_TOOLS',
            'flx.note_write,flx.notes_read,flx.lessons_search,flx.lessons_save,'
            'flx.static_map_render,flx.ask_user,flx.report_compose,flx.targeted_source_search,flx.web_search,'
            'query_builder.execute_query,query_builder.execute_aggregation,'
            'query_builder.save_query_to_temp_collection,query_builder.get_distinct_values,'
            'analytics.execute_analytics,analytics.list_metrics,'
            'schema.get_collection_info,schema.get_data_dictionary,'
            'geocoding.geocode_address,geocoding.search_nearby_places,'
            'vector.semantic_search_listings,'
            'cadastral.get_knowledge,cadastral.discover_in_area,cadastral.search,'
            'cadastral.list_parcels_in_region_polygon,cadastral.list_polygon_query_page,'
            'cadastral.cluster_parcels,cadastral.get_cluster_meta,cadastral.list_cluster_parcels_page,'
            'cadastral.get_parcel_summary,cadastral.get_parcel_full,'
            'listings.find_with_fallback',
        )
        self.flx_allowed_tools = [t.strip() for t in _allowed_tools_raw.split(',') if t.strip()]
        # FLX Strategy Engine — кілька стратегій, крос-порівняння, реєстр ефективності
        self.flx_strategy_engine_enabled = os.getenv('FLX_STRATEGY_ENGINE_ENABLED', 'true').lower() in ('true', '1', 'yes')
        self.flx_strategy_registry_enabled = os.getenv('FLX_STRATEGY_REGISTRY_ENABLED', 'true').lower() in ('true', '1', 'yes')
        self.flx_strategy_max_candidates = int(os.getenv('FLX_STRATEGY_MAX_CANDIDATES', '4'))
        self.flx_strategy_registry_ewma_alpha = float(os.getenv('FLX_STRATEGY_REGISTRY_EWMA_ALPHA', '0.35'))
        self.flx_orchestration_backend = os.getenv('FLX_ORCHESTRATION_BACKEND', 'celery').strip().lower()
        self.flx_coverage_threshold = float(os.getenv('FLX_COVERAGE_THRESHOLD', '0.75'))
        # Multi-stage звіт: outline → секція_за_секцією → executive summary.
        # Це усуває «один промпт з ВСЕ» і дає розгорнутий документ. Вимкнути
        # → старий single-pass.
        self.flx_multistage_report = os.getenv('FLX_MULTISTAGE_REPORT', 'true').lower() in ('true', '1', 'yes')
        self.flx_report_max_sections = int(os.getenv('FLX_REPORT_MAX_SECTIONS', '8'))
        self.flx_parallel_branches = int(os.getenv('FLX_PARALLEL_BRANCHES', '2'))
        self.flx_budget_web_searches = int(os.getenv('FLX_BUDGET_WEB_SEARCHES', '40'))
        self.flx_budget_deep_analysis = int(os.getenv('FLX_BUDGET_DEEP_ANALYSIS', '12'))
        self.flx_budget_api_calls = int(os.getenv('FLX_BUDGET_API_CALLS', '500'))
        self.flx_budget_critic_iterations = int(os.getenv('FLX_BUDGET_CRITIC_ITERATIONS', '6'))
        self.flx_unknown_discovery_enabled = os.getenv('FLX_UNKNOWN_DISCOVERY_ENABLED', 'true').lower() in ('true', '1', 'yes')
        # Для безкоштовного тарифу Gemini ліміт 5 запитів/хвилину, тому використовуємо 4 для безпеки
        self.llm_rate_limit_calls_per_minute = int(os.getenv('LLM_RATE_LIMIT_CALLS_PER_MINUTE', '0'))  # 0 = без обмежень
        self.llm_api_keys = {
            'gemini': os.getenv('LLM_API_KEY_GEMINI', ''),
            'openai': os.getenv('LLM_API_KEY_OPENAI', ''),
            'anthropic': os.getenv('LLM_API_KEY_ANTHROPIC', ''),
            'ollama': os.getenv('LLM_API_KEY_OLLAMA', ''),  # Для Ollama не потрібен, залишається порожнім
            'vllm_remote': os.getenv('LLM_API_KEY_VLLM_REMOTE', ''),
        }
        # Параметри циклу агента (ітерації, токени, температура, time budget)
        self.llm_agent_max_iterations = int(os.getenv('LLM_AGENT_MAX_ITERATIONS', '10'))
        self.llm_agent_max_output_tokens = int(os.getenv('LLM_AGENT_MAX_OUTPUT_TOKENS', '8192'))
        self.llm_agent_temperature = float(os.getenv('LLM_AGENT_TEMPERATURE', '0.7'))
        _tb = os.getenv('LLM_AGENT_TIME_BUDGET_SECONDS', '')
        self.llm_agent_time_budget_seconds = int(_tb) if _tb and _tb.isdigit() else None
        # Thinking mode та Google Search grounding (лише для AI-асистента, не для парсингу/інших LLM)
        _tbudget = os.getenv('LLM_AGENT_THINKING_BUDGET', '8192')
        self.llm_agent_thinking_budget = int(_tbudget) if (_tbudget or '').lstrip('-').isdigit() else 8192  # 0 = вимкнено
        self.llm_agent_google_search_grounding = os.getenv('LLM_AGENT_GOOGLE_SEARCH_GROUNDING', 'false').lower() in ('true', '1', 'yes')
        self.llm_agent_include_thoughts = os.getenv('LLM_AGENT_INCLUDE_THOUGHTS', 'true').lower() in ('true', '1', 'yes')
        self.llm_agent_use_langgraph = os.getenv('LLM_AGENT_USE_LANGGRAPH', 'false').lower() in ('true', '1', 'yes')
        # Когнітивний шар LangChain-агента (explicit state, Long Chain, reflection, tool retrieval)
        self.llm_agent_cognitive_enabled = os.getenv('LLM_AGENT_COGNITIVE_ENABLED', 'true').lower() in ('true', '1', 'yes')
        self.llm_agent_tool_retrieval_enabled = os.getenv('LLM_AGENT_TOOL_RETRIEVAL_ENABLED', 'false').lower() in ('true', '1', 'yes')
        self.llm_agent_tool_retrieval_top_k = int(os.getenv('LLM_AGENT_TOOL_RETRIEVAL_TOP_K', '14'))
        _core = os.getenv(
            'LLM_AGENT_TOOL_RETRIEVAL_CORE',
            'get_allowed_collections,get_data_dictionary,get_collection_info,get_database_schema',
        )
        self.llm_agent_tool_retrieval_core = [x.strip() for x in _core.split(',') if x.strip()]
        self.llm_agent_reflection_enabled = os.getenv('LLM_AGENT_REFLECTION_ENABLED', 'true').lower() in ('true', '1', 'yes')
        self.llm_agent_reflection_every_n = int(os.getenv('LLM_AGENT_REFLECTION_EVERY_N', '3'))
        self.llm_agent_reflect_on_tool_failure = os.getenv('LLM_AGENT_REFLECT_ON_TOOL_FAILURE', 'true').lower() in (
            'true',
            '1',
            'yes',
        )
        self.llm_agent_chain_compression_threshold = int(os.getenv('LLM_AGENT_CHAIN_COMPRESSION_THRESHOLD', '24'))
        self.llm_agent_semantic_memory_enabled = os.getenv('LLM_AGENT_SEMANTIC_MEMORY_ENABLED', 'true').lower() in (
            'true',
            '1',
            'yes',
        )
        
        # Налаштування Telegram бота
        self.telegram_bot_token = os.getenv('TELEGRAM_BOT_TOKEN', '')
        self.telegram_users_config_path = os.getenv(
            'TELEGRAM_USERS_CONFIG_PATH',
            str(Path(__file__).parent / 'users.yaml')
        )
        # HTTP-клієнт до api.telegram.org (python-telegram-bot / HTTPXRequest)
        def _float_env(name: str, default: float) -> float:
            raw = os.getenv(name, '').strip()
            if not raw:
                return default
            try:
                return float(raw)
            except ValueError:
                return default

        self.telegram_http_connect_timeout = _float_env('TELEGRAM_HTTP_CONNECT_TIMEOUT', 30.0)
        self.telegram_http_read_timeout = _float_env('TELEGRAM_HTTP_READ_TIMEOUT', 60.0)
        self.telegram_http_write_timeout = _float_env('TELEGRAM_HTTP_WRITE_TIMEOUT', 30.0)
        self.telegram_http_pool_timeout = _float_env('TELEGRAM_HTTP_POOL_TIMEOUT', 30.0)
        self.telegram_http_version = (os.getenv('TELEGRAM_HTTP_VERSION', '1.1') or '1.1').strip()
        
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

        # Brokered background tasks (RabbitMQ + Celery)
        self.task_queue_enabled = os.getenv('TASK_QUEUE_ENABLED', 'false').lower() in ('true', '1', 'yes')
        self.task_queue_broker_url = os.getenv('TASK_QUEUE_BROKER_URL', '').strip()
        self.task_queue_result_backend = os.getenv('TASK_QUEUE_RESULT_BACKEND', 'rpc://').strip()
        self.task_queue_llm_worker_threads = int(os.getenv('TASK_QUEUE_LLM_WORKER_THREADS', '3'))
        self.task_queue_source_worker_threads = int(os.getenv('TASK_QUEUE_SOURCE_WORKER_THREADS', '1'))
        self.rabbitmq_host = os.getenv('RABBITMQ_HOST', 'localhost').strip()
        self.rabbitmq_port = int(os.getenv('RABBITMQ_PORT', '5672'))
        self.rabbitmq_user = os.getenv('RABBITMQ_USER', 'guest').strip()
        self.rabbitmq_password = os.getenv('RABBITMQ_PASSWORD', 'guest').strip()
        self.rabbitmq_vhost = os.getenv('RABBITMQ_VHOST', 'pazuzu').strip() or 'pazuzu'

        # Telegram Mini App (веб-застосунок у Telegram)
        self.mini_app_port = int(os.getenv('MINI_APP_PORT', '8000'))
        self.mini_app_base_url = os.getenv('MINI_APP_BASE_URL', '')  # HTTPS URL для BotFather, напр. https://example.com

        # Маршрутизація: confidence threshold та уточнення при низькій впевненості
        self.routing_confidence_threshold = float(os.getenv('ROUTING_CONFIDENCE_THRESHOLD', '0.7'))
        
        # Новий потік обробки агентів (за замовчуванням увімкнено)
        self.use_new_agent_flow = os.getenv('USE_NEW_AGENT_FLOW', 'true').lower() == 'true'
        self.routing_ask_on_low_confidence = os.getenv('ROUTING_ASK_ON_LOW_CONFIDENCE', 'false').lower() in ('true', '1', 'yes')

        # Security layer: rate limit та max complexity запиту
        self.rate_limit_requests_per_minute = int(os.getenv('RATE_LIMIT_REQUESTS_PER_MINUTE', '0'))  # 0 = без обмежень
        self.max_query_complexity_length = int(os.getenv('MAX_QUERY_COMPLEXITY_LENGTH', '8000'))

        # Ліміти експорту та артефактів
        self.export_max_rows = int(os.getenv('EXPORT_MAX_ROWS', '50000'))
        _ems = os.getenv('EXPORT_MAX_FILE_SIZE_MB', '50')
        self.export_max_file_size_mb = int(_ems) if _ems.isdigit() else 50
        self.exports_per_user_per_day = int(os.getenv('EXPORTS_PER_USER_PER_DAY', '20'))
        self.artifact_ttl_seconds = int(os.getenv('ARTIFACT_TTL_SECONDS', '3600'))
        self.export_confirm_rows_threshold = int(os.getenv('EXPORT_CONFIRM_ROWS_THRESHOLD', '50000'))

        # Векторна БД (Qdrant) — окрема нода у docker-compose; зберігає semantic-індекси
        # по unified_listings та cadastral_parcels для FLX-tools `vector.semantic_search_*`.
        self.qdrant_host = os.getenv('QDRANT_HOST', 'localhost').strip() or 'localhost'
        self.qdrant_port = int(os.getenv('QDRANT_PORT', '6333'))
        self.qdrant_grpc_port = int(os.getenv('QDRANT_GRPC_PORT', '6334'))
        self.qdrant_use_grpc = os.getenv('QDRANT_USE_GRPC', 'false').lower() in ('true', '1', 'yes')
        self.qdrant_api_key = os.getenv('QDRANT_API_KEY', '').strip()
        self.vector_collection_listings = os.getenv('VECTOR_COLLECTION_LISTINGS', 'unified_listings_vec').strip()
        self.vector_collection_cadastral = os.getenv('VECTOR_COLLECTION_CADASTRAL', 'cadastral_parcels_vec').strip()
        self.vector_collection_mcp_tools = os.getenv('VECTOR_COLLECTION_MCP_TOOLS', 'mcp_tools_vec').strip()
        self.vector_size = int(os.getenv('VECTOR_SIZE', '1024'))
        self.vector_distance = os.getenv('VECTOR_DISTANCE', 'cosine').strip().lower()

        # Кадастр: налаштовувані ліміти вибірок для CadastralDomainService.
        # search → серверна фільтрація + clustering, тягне до search_max_parcels;
        # discover → reconnaissance, без фільтрів, тягне до discover_max_parcels;
        # hard_cap — захисна стеля, вище якої жоден агент не зможе підняти ліміт.
        self.cadastral_search_max_parcels = int(os.getenv('CADASTRAL_SEARCH_MAX_PARCELS', '5000'))
        self.cadastral_discover_max_parcels = int(os.getenv('CADASTRAL_DISCOVER_MAX_PARCELS', '2000'))
        self.cadastral_search_hard_cap = int(os.getenv('CADASTRAL_SEARCH_HARD_CAP', '20000'))
        self.cadastral_max_radius_meters = float(os.getenv('CADASTRAL_MAX_RADIUS_METERS', '50000'))
        self.cadastral_polygon_max_parcels = int(os.getenv('CADASTRAL_POLYGON_MAX_PARCELS', '80000'))
        self.cadastral_polygon_page_size_default = int(os.getenv('CADASTRAL_POLYGON_PAGE_SIZE', '200'))
        self.cadastral_national_partition_max_parcels = int(
            os.getenv('CADASTRAL_NATIONAL_PARTITION_MAX_PARCELS', '120000')
        )

        # Embeddings (за замовчуванням — HuggingFace Text Embeddings Inference з bge-m3)
        self.embeddings_endpoint = os.getenv('EMBEDDINGS_ENDPOINT', 'http://localhost:8089').strip()
        self.embeddings_model = os.getenv('EMBEDDINGS_MODEL', 'BAAI/bge-m3').strip()
        self.embeddings_batch_size = int(os.getenv('EMBEDDINGS_BATCH_SIZE', '16'))
        self.embeddings_timeout_sec = int(os.getenv('EMBEDDINGS_TIMEOUT_SEC', '30'))
        self.embeddings_cache_enabled = os.getenv('EMBEDDINGS_CACHE_ENABLED', 'true').lower() in ('true', '1', 'yes')
        self.embeddings_cache_ttl_days = int(os.getenv('EMBEDDINGS_CACHE_TTL_DAYS', '30'))
        self.embeddings_max_retries = int(os.getenv('EMBEDDINGS_MAX_RETRIES', '3'))

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
                            # Окремо асистент (Gemini) та парсинг (Ollama)
                            if 'assistant' in llm_config:
                                a = llm_config['assistant']
                                if 'provider' in a:
                                    self.llm_assistant_provider = a['provider']
                                if 'model_name' in a:
                                    self.llm_assistant_model_name = a['model_name']
                            else:
                                self.llm_assistant_provider = self.llm_provider
                                self.llm_assistant_model_name = self.llm_model_name
                            if 'parsing' in llm_config:
                                p = llm_config['parsing']
                                if 'provider' in p:
                                    self.llm_parsing_provider = p['provider']
                                if 'model_name' in p:
                                    self.llm_parsing_model_name = p['model_name']
                            elif 'model_name' in llm_config:
                                # Якщо llm.parsing не задано — використовувати загальну модель для парсингу
                                self.llm_parsing_model_name = llm_config['model_name']
                            if 'investigator' in llm_config:
                                inv = llm_config['investigator']
                                if 'provider' in inv:
                                    self.llm_investigator_provider = inv['provider']
                                if 'model_name' in inv:
                                    self.llm_investigator_model_name = inv['model_name']
                                if 'temperature' in inv:
                                    self.llm_investigator_temperature = float(inv['temperature'])
                                if 'max_iterations' in inv:
                                    self.llm_investigator_max_iterations = int(inv['max_iterations'])
                                if 'time_budget_seconds' in inv:
                                    self.llm_investigator_time_budget_seconds = int(inv['time_budget_seconds'])
                                if 'google_grounding' in inv:
                                    self.llm_investigator_google_grounding = bool(inv['google_grounding'])
                                if 'max_steps_per_task' in inv:
                                    self.llm_investigator_max_steps_per_task = int(inv['max_steps_per_task'])
                                if 'web_search_provider' in inv:
                                    self.llm_investigator_web_search_provider = str(inv['web_search_provider'] or 'duckduckgo').strip().lower()
                                if 'web_search_model_name' in inv:
                                    self.llm_investigator_web_search_model_name = str(inv.get('web_search_model_name') or '').strip()
                                if 'web_search_enabled' in inv:
                                    self.llm_investigator_web_search_enabled = bool(inv['web_search_enabled'])
                                if 'source_wait_timeout_seconds' in inv:
                                    self.flx_source_wait_timeout_seconds = int(inv['source_wait_timeout_seconds'])
                                if 'allowed_tools' in inv and isinstance(inv['allowed_tools'], list):
                                    self.flx_allowed_tools = [str(t).strip() for t in inv['allowed_tools'] if str(t).strip()]
                                if 'static_maps_size' in inv:
                                    self.flx_static_maps_default_size = str(inv['static_maps_size'])
                                if 'static_maps_zoom' in inv:
                                    self.flx_static_maps_default_zoom = int(inv['static_maps_zoom'])
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
                                if 'ollama' in api_keys:
                                    self.llm_api_keys['ollama'] = api_keys['ollama']
                                if 'vllm_remote' in api_keys:
                                    self.llm_api_keys['vllm_remote'] = api_keys['vllm_remote']
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
                                if 'thinking_budget' in agent_config:
                                    self.llm_agent_thinking_budget = int(agent_config['thinking_budget'])
                                if 'google_search_grounding' in agent_config:
                                    self.llm_agent_google_search_grounding = bool(agent_config['google_search_grounding'])
                                if 'include_thoughts' in agent_config:
                                    self.llm_agent_include_thoughts = bool(agent_config['include_thoughts'])
                                if 'use_langgraph' in agent_config:
                                    self.llm_agent_use_langgraph = bool(agent_config['use_langgraph'])
                                if 'cognitive_enabled' in agent_config:
                                    self.llm_agent_cognitive_enabled = bool(agent_config['cognitive_enabled'])
                                if 'tool_retrieval_enabled' in agent_config:
                                    self.llm_agent_tool_retrieval_enabled = bool(agent_config['tool_retrieval_enabled'])
                                if 'tool_retrieval_top_k' in agent_config:
                                    self.llm_agent_tool_retrieval_top_k = int(agent_config['tool_retrieval_top_k'])
                                if 'tool_retrieval_core' in agent_config and isinstance(agent_config['tool_retrieval_core'], list):
                                    self.llm_agent_tool_retrieval_core = [
                                        str(x).strip() for x in agent_config['tool_retrieval_core'] if str(x).strip()
                                    ]
                                if 'reflection_enabled' in agent_config:
                                    self.llm_agent_reflection_enabled = bool(agent_config['reflection_enabled'])
                                if 'reflection_every_n' in agent_config:
                                    self.llm_agent_reflection_every_n = int(agent_config['reflection_every_n'])
                                if 'reflect_on_tool_failure' in agent_config:
                                    self.llm_agent_reflect_on_tool_failure = bool(agent_config['reflect_on_tool_failure'])
                                if 'chain_compression_threshold' in agent_config:
                                    self.llm_agent_chain_compression_threshold = int(agent_config['chain_compression_threshold'])
                                if 'semantic_memory_enabled' in agent_config:
                                    self.llm_agent_semantic_memory_enabled = bool(agent_config['semantic_memory_enabled'])

                        # Налаштування Telegram бота (в т.ч. парсинг — може перевизначити llm.parsing)
                        if 'telegram' in config:
                            telegram_config = config['telegram']
                            if 'bot_token' in telegram_config:
                                self.telegram_bot_token = telegram_config['bot_token']
                            if 'users_config_path' in telegram_config:
                                self.telegram_users_config_path = telegram_config['users_config_path']
                            if 'http_connect_timeout' in telegram_config:
                                self.telegram_http_connect_timeout = float(telegram_config['http_connect_timeout'])
                            if 'http_read_timeout' in telegram_config:
                                self.telegram_http_read_timeout = float(telegram_config['http_read_timeout'])
                            if 'http_write_timeout' in telegram_config:
                                self.telegram_http_write_timeout = float(telegram_config['http_write_timeout'])
                            if 'http_pool_timeout' in telegram_config:
                                self.telegram_http_pool_timeout = float(telegram_config['http_pool_timeout'])
                            if 'http_version' in telegram_config:
                                self.telegram_http_version = str(telegram_config['http_version'] or '1.1').strip()
                            if 'parsing' in telegram_config:
                                p = telegram_config['parsing']
                                if 'provider' in p:
                                    self.llm_parsing_provider = p['provider']
                                if 'model_name' in p:
                                    self.llm_parsing_model_name = p['model_name']
                        
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

                        if 'task_queue' in config:
                            tq = config['task_queue']
                            if 'enabled' in tq:
                                self.task_queue_enabled = bool(tq['enabled'])
                            if 'broker_url' in tq:
                                self.task_queue_broker_url = str(tq['broker_url'] or '').strip()
                            if 'result_backend' in tq:
                                self.task_queue_result_backend = str(tq['result_backend'] or '').strip()
                            if 'llm_worker_threads' in tq:
                                self.task_queue_llm_worker_threads = int(tq['llm_worker_threads'])
                            if 'source_worker_threads' in tq:
                                self.task_queue_source_worker_threads = int(tq['source_worker_threads'])
                            if 'rabbitmq_host' in tq:
                                self.rabbitmq_host = str(tq['rabbitmq_host'] or '').strip()
                            if 'rabbitmq_port' in tq:
                                self.rabbitmq_port = int(tq['rabbitmq_port'])
                            if 'rabbitmq_user' in tq:
                                self.rabbitmq_user = str(tq['rabbitmq_user'] or '').strip()
                            if 'rabbitmq_password' in tq:
                                self.rabbitmq_password = str(tq['rabbitmq_password'] or '').strip()
                            if 'rabbitmq_vhost' in tq:
                                self.rabbitmq_vhost = str(tq['rabbitmq_vhost'] or '').strip() or self.rabbitmq_vhost

                        # Telegram Mini App (base_url — повний HTTPS URL, який відкриває Telegram, напр. ngrok)
                        if 'mini_app' in config:
                            ma = config['mini_app']
                            if 'port' in ma:
                                self.mini_app_port = int(ma['port'])
                            if ma.get('base_url'):
                                self.mini_app_base_url = str(ma['base_url']).strip()

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

                        # Vector DB (Qdrant) — окрема нода у docker-compose
                        if 'vector_db' in config:
                            vdb = config['vector_db'] or {}
                            if 'host' in vdb:
                                self.qdrant_host = str(vdb['host']).strip() or self.qdrant_host
                            if 'port' in vdb:
                                self.qdrant_port = int(vdb['port'])
                            if 'grpc_port' in vdb:
                                self.qdrant_grpc_port = int(vdb['grpc_port'])
                            if 'use_grpc' in vdb:
                                self.qdrant_use_grpc = bool(vdb['use_grpc'])
                            if 'api_key' in vdb:
                                self.qdrant_api_key = str(vdb['api_key'] or '').strip()
                            if 'collection_listings' in vdb:
                                self.vector_collection_listings = str(vdb['collection_listings']).strip()
                            if 'collection_cadastral' in vdb:
                                self.vector_collection_cadastral = str(vdb['collection_cadastral']).strip()
                            if 'collection_mcp_tools' in vdb:
                                self.vector_collection_mcp_tools = str(vdb['collection_mcp_tools']).strip()
                            if 'vector_size' in vdb:
                                self.vector_size = int(vdb['vector_size'])
                            if 'distance' in vdb:
                                self.vector_distance = str(vdb['distance']).strip().lower()

                        # Embeddings (TEI / OpenAI-compatible)
                        if 'embeddings' in config:
                            emb = config['embeddings'] or {}
                            if 'endpoint' in emb:
                                self.embeddings_endpoint = str(emb['endpoint']).strip()
                            if 'model_name' in emb:
                                self.embeddings_model = str(emb['model_name']).strip()
                            if 'batch_size' in emb:
                                self.embeddings_batch_size = int(emb['batch_size'])
                            if 'timeout_sec' in emb:
                                self.embeddings_timeout_sec = int(emb['timeout_sec'])
                            if 'cache_enabled' in emb:
                                self.embeddings_cache_enabled = bool(emb['cache_enabled'])
                            if 'cache_ttl_days' in emb:
                                self.embeddings_cache_ttl_days = int(emb['cache_ttl_days'])
                            if 'max_retries' in emb:
                                self.embeddings_max_retries = int(emb['max_retries'])

                        # Cadastral domain limits
                        if 'cadastral' in config:
                            cad = config['cadastral'] or {}
                            if 'search_max_parcels' in cad:
                                self.cadastral_search_max_parcels = int(cad['search_max_parcels'])
                            if 'discover_max_parcels' in cad:
                                self.cadastral_discover_max_parcels = int(cad['discover_max_parcels'])
                            if 'search_hard_cap' in cad:
                                self.cadastral_search_hard_cap = int(cad['search_hard_cap'])
                            if 'max_radius_meters' in cad:
                                self.cadastral_max_radius_meters = float(cad['max_radius_meters'])
                            if 'polygon_max_parcels' in cad:
                                self.cadastral_polygon_max_parcels = int(cad['polygon_max_parcels'])
                            if 'polygon_page_size' in cad:
                                self.cadastral_polygon_page_size_default = int(cad['polygon_page_size'])
                            if 'national_partition_max_parcels' in cad:
                                self.cadastral_national_partition_max_parcels = int(
                                    cad['national_partition_max_parcels']
                                )
            except Exception as e:
                print(f"Попередження: не вдалося завантажити конфігурацію з {config_path}: {e}")
                print("Використовуються значення за замовчуванням або змінні оточення")

        if not self.task_queue_broker_url:
            self.task_queue_broker_url = (
                f"amqp://{self.rabbitmq_user}:{self.rabbitmq_password}"
                f"@{self.rabbitmq_host}:{self.rabbitmq_port}/{self.rabbitmq_vhost}"
            )
        self.task_queue_llm_worker_threads = max(1, int(self.task_queue_llm_worker_threads or 3))
        self.task_queue_source_worker_threads = max(1, int(self.task_queue_source_worker_threads or 1))

