# -*- coding: utf-8 -*-
"""
Сервіс для роботи з LangChain агентом, який використовує MCP інструменти.
Архітектурні принципи:
- LLM НІКОЛИ не мають прямого доступу до баз даних, API або файлової системи
- УСІ операції з даними виконуються ВИКЛЮЧНО через MCP-сервери (tools)
- LangChain використовується ЛИШЕ як runtime для агентів та оркестрації
- Бізнес-логіка, валідація, безпека та обмеження реалізуються на сервері
"""

import json
import re
import logging
import time
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable, Tuple, Union

from config.settings import Settings
from utils.data_dictionary import DataDictionary
from utils.analytics_builder import AnalyticsBuilder
from utils.query_builder import QueryBuilder
from utils.report_generator import ReportGenerator
from data.database.connection import MongoDBConnection

# LangChain imports
try:
    from langchain.tools import StructuredTool
    from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_openai import ChatOpenAI
    from langchain_anthropic import ChatAnthropic
    LANGCHAIN_AVAILABLE = True
except ImportError:
    LANGCHAIN_AVAILABLE = False

# Налаштування логування
logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.DEBUG)
    formatter = logging.Formatter(
        '[LangChain Agent] %(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    logger.propagate = False


class LangChainAgentService:
    """
    Сервіс для роботи з LangChain агентом з підтримкою MCP інструментів.
    
    Архітектурні принципи:
    - Явний цикл агента (plan → act → observe)
    - Всі операції з даними через MCP tools
    - Детальне логування всіх дій
    - Безпека на рівні сервісу
    """
    
    # Максимальна кількість ітерацій агента
    MAX_ITERATIONS = 10
    
    # Максимальна кількість викликів tools за одну ітерацію
    MAX_TOOL_CALLS_PER_ITERATION = 5
    
    def __init__(self, settings: Settings):
        """
        Ініціалізація сервісу.
        
        Args:
            settings: Налаштування застосунку
        """
        if not LANGCHAIN_AVAILABLE:
            raise ImportError(
                "Для використання LangChain потрібно встановити залежності: "
                "pip install langchain langchain-google-genai langchain-openai langchain-anthropic"
            )
        
        self.settings = settings
        
        # Ініціалізуємо утиліти (те саме, що використовують MCP-сервери)
        self.data_dictionary = DataDictionary()
        self.analytics_builder = AnalyticsBuilder()
        self.query_builder = QueryBuilder()
        self.report_generator = ReportGenerator()
        
        # Ініціалізуємо підключення до БД
        MongoDBConnection.initialize(settings)
        
        # Ініціалізуємо LLM
        self.llm = self._create_llm()
        
        # Створюємо tools
        self.tools = self._create_tools()
        
        # Створюємо систему промптів
        self.system_prompt = self._get_system_prompt()
        
        # Історія розмови (лише для поточного запиту)
        self.conversation_history: List[Any] = []
    
    def _create_llm(self):
        """Створює LLM на основі налаштувань."""
        provider_name = self.settings.llm_provider.lower()
        api_key = self.settings.llm_api_keys.get(provider_name, '')
        
        if not api_key:
            raise ValueError(f"API ключ для провайдера {provider_name} не вказано")
        
        model_name = getattr(self.settings, 'llm_model_name', 'gemini-2.5-flash')
        
        if provider_name == 'gemini':
            return ChatGoogleGenerativeAI(
                model=model_name,
                google_api_key=api_key,
                temperature=0.7,
                max_output_tokens=8192
            )
        elif provider_name == 'openai':
            return ChatOpenAI(
                model=model_name,
                api_key=api_key,
                temperature=0.7,
                max_tokens=8192
            )
        elif provider_name == 'anthropic':
            return ChatAnthropic(
                model=model_name,
                api_key=api_key,
                temperature=0.7,
                max_tokens=8192
            )
        else:
            raise ValueError(f"Невідомий провайдер LLM: {provider_name}")
    
    def _load_glossary(self) -> str:
        """Завантажує глосарій розробника."""
        try:
            project_root = Path(__file__).parent.parent.parent
            glossary_path = project_root / 'docs' / 'developer_glossary.md'
            
            if glossary_path.exists():
                with open(glossary_path, 'r', encoding='utf-8') as f:
                    return f.read()
            return ""
        except Exception:
            return ""
    
    def _get_system_prompt(self) -> str:
        """Формує системний промпт з урахуванням архітектурних правил."""
        base_prompt = """You are a data analyst assistant for a real estate auction database.

CRITICAL ARCHITECTURAL RULES:
1. You do NOT have direct access to databases, APIs, or file systems.
2. ALL data operations MUST be performed EXCLUSIVELY through MCP tools.
3. You MUST use tools to discover:
   - collections and their schemas
   - available fields and relationships
   - available analytics and metrics
4. You are NOT allowed to invent fields or collections.
5. If needed data is unavailable, explicitly state so.

WORKFLOW:
1. Analyze the user's query
2. Identify what information is missing
3. Call MCP tools to inspect schemas and capabilities
4. Build a plan using ONLY available tools
5. Execute tools step by step
6. Use ONLY MCP results in your final answer

AVAILABLE TOOLS:
- schema_mcp tools: Inspect database structure (collections, fields, relationships)
- analytics_mcp tools: Get aggregated data and metrics
- query_builder_mcp tools: 
  * execute_query: Query specific data (with security restrictions)
  * execute_aggregation: Execute MongoDB aggregation pipeline with grouping, unwinding arrays, and aggregations. Use for complex queries that require grouping by fields (e.g., finding participants who participated in more than one auction)
- report_mcp tools: Generate files for download

IMPORTANT - Security:
- $regex operator is FORBIDDEN in query-builder-mcp
- Use analytics-mcp for status filtering (it handles "active" correctly)
- Use analytics-mcp for region/city filtering (it handles joins automatically)

IMPORTANT - Data Access:
- For filtering by region/city/property_type: ALWAYS use analytics-mcp
- For computed fields and sorting: use query-builder-mcp execute_query with join to llm_cache
- For complex queries with grouping (e.g., finding participants in multiple auctions): use query-builder-mcp execute_aggregation
- For generating reports: use report-mcp with analytics-mcp as data source

IMPORTANT - JSON Format:
- When passing JSON strings (for addFields, sort, or pipeline), use "null" (not "None") for null values
- Python None is not valid JSON and will cause parsing errors
- Example: {"field": null} is correct, {"field": None} is incorrect

IMPORTANT - execute_aggregation tool:
- Use execute_aggregation for queries that require:
  * Grouping by fields ($group stage)
  * Unwinding arrays ($unwind stage)
  * Counting unique values ($addToSet, $size)
  * Complex aggregations
- Example: Finding participants who participated in more than one auction:
  {
    "collection_name": "prozorro_auctions",
    "pipeline": [
      {"$unwind": "$auction_data.bids"},
      {"$unwind": "$auction_data.bids.bidders"},
      {
        "$group": {
          "_id": "$auction_data.bids.bidders.identifier.id",
          "auctions_count": {"$addToSet": "$auction_id"}
        }
      },
      {
        "$addFields": {
          "auctions_count": {"$size": "$auctions_count"}
        }
      },
      {
        "$match": {
          "auctions_count": {"$gt": 1}
        }
      },
      {"$sort": {"auctions_count": -1}},
      {"$limit": 100}
    ],
    "limit": 100
  }
- Pipeline is an array of stage objects (dictionaries)
- Each stage has one key (stage name like "$group", "$unwind", "$match", etc.)
- Allowed stages: $match, $project, $group, $unwind, $sort, $limit, $lookup, $addFields, etc.
- Forbidden stages: $out, $merge (for security)

You MUST explain (in logs/comments, NOT to user):
- Why each MCP tool was called
- What assumptions were verified
- What limitations the result has

Always respond in Ukrainian language."""
        
        glossary = self._load_glossary()
        if glossary:
            base_prompt += f"""

## Project Terminology (Developer Glossary)

Use this terminology when responding and working with data:

{glossary}

Important: Use terms from the glossary correctly."""
        
        return base_prompt
    
    def _create_tools(self) -> List[StructuredTool]:
        """Створює LangChain Tools для всіх MCP-серверів."""
        tools = []
        
        # Schema MCP tools
        tools.extend([
            StructuredTool.from_function(
                func=self._get_database_schema,
                name="get_database_schema",
                description="Отримує повну схему метаданих всіх колекцій бази даних. Використовуй для дослідження структури БД.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._get_collection_info,
                name="get_collection_info",
                description="Отримує детальну інформацію про конкретну колекцію (prozorro_auctions або llm_cache).",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._get_data_dictionary,
                name="get_data_dictionary",
                description="Отримує повний Data Dictionary з описом всіх колекцій та полів.",
                return_direct=False
            )
        ])
        
        # Query Builder MCP tools
        tools.extend([
            StructuredTool.from_function(
                func=self._execute_query,
                name="execute_query",
                description="Виконує безпечний запит до MongoDB. ВАЖЛИВО: $regex оператор ЗАБОРОНЕНИЙ. Для фільтрації за статусом або регіоном/містом використовуй analytics-mcp замість query-builder-mcp.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._execute_aggregation,
                name="execute_aggregation",
                description="Виконує MongoDB aggregation pipeline з групуванням, розгортанням масивів та агрегаціями. Використовуй для складних запитів, які потребують групування за полями (наприклад, знаходження учасників, що брали участь більше ніж в одному аукціоні). Pipeline - це масив stages (словників).",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._get_allowed_collections,
                name="get_allowed_collections",
                description="Отримує список дозволених колекцій для запитів.",
                return_direct=False
            )
        ])
        
        # Analytics MCP tools
        tools.extend([
            StructuredTool.from_function(
                func=self._execute_analytics,
                name="execute_analytics",
                description="Виконує аналітичний запит з метриками та агрегаціями. Використовуй для отримання статистики та аналітики.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._list_metrics,
                name="list_metrics",
                description="Отримує список доступних метрик для аналітики.",
                return_direct=False
            )
        ])
        
        # Report MCP tools
        tools.extend([
            StructuredTool.from_function(
                func=self._generate_report,
                name="generate_report",
                description="Генерує звіт у вказаному форматі (xlsx, csv, json). Використовуй для створення файлів з даними.",
                return_direct=False
            ),
            StructuredTool.from_function(
                func=self._list_templates,
                name="list_templates",
                description="Отримує список доступних шаблонів звітів.",
                return_direct=False
            )
        ])
        
        return tools
    
    # Schema MCP tool implementations
    def _get_database_schema(self) -> Dict[str, Any]:
        """Отримує повну схему метаданих всіх колекцій."""
        logger.info("🔍 [Schema MCP] Виклик get_database_schema")
        try:
            schema = self.data_dictionary.to_schema_dict()
            schema['generated_at'] = datetime.utcnow().isoformat()
            logger.info("✓ [Schema MCP] Схема отримана успішно")
            return {'success': True, 'schema': schema}
        except Exception as e:
            logger.error(f"✗ [Schema MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _get_collection_info(self, collection_name: str) -> Dict[str, Any]:
        """Отримує детальну інформацію про колекцію."""
        logger.info(f"🔍 [Schema MCP] Виклик get_collection_info для {collection_name}")
        try:
            collection = self.data_dictionary.get_collection(collection_name)
            if collection:
                result = {
                    'success': True,
                    'collection': {
                        'collection_name': collection.mongo_collection,
                        'description': collection.description,
                        'schema': self.data_dictionary.to_schema_dict()['collections'][collection_name]['schema'],
                        'indexes': collection.indexes,
                        'relationships': collection.relationships
                    }
                }
                logger.info(f"✓ [Schema MCP] Інформація про колекцію {collection_name} отримана")
                return result
            logger.warning(f"⚠ [Schema MCP] Колекція {collection_name} не знайдена")
            return {'success': False, 'error': f'Колекція {collection_name} не знайдена'}
        except Exception as e:
            logger.error(f"✗ [Schema MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _get_data_dictionary(self) -> Dict[str, Any]:
        """Отримує повний Data Dictionary."""
        logger.info("🔍 [Schema MCP] Виклик get_data_dictionary")
        try:
            result = {
                'success': True,
                'data_dictionary': self.data_dictionary.to_schema_dict(),
                'metadata': self.data_dictionary.get_metadata()
            }
            logger.info("✓ [Schema MCP] Data Dictionary отримано")
            return result
        except Exception as e:
            logger.error(f"✗ [Schema MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    # Query Builder MCP tool implementations
    def _execute_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Виконує безпечний запит до MongoDB."""
        logger.info(f"🔍 [Query Builder MCP] Виклик execute_query")
        logger.debug(f"Запит: {json.dumps(query, indent=2, ensure_ascii=False, default=str)}")
        
        try:
            # Парсимо addFields та sort якщо вони передані як string
            if 'addFields' in query and isinstance(query['addFields'], str):
                try:
                    add_fields_str = query['addFields'].strip()
                    # Замінюємо Python None на JSON null перед парсингом
                    add_fields_str = re.sub(r'\bNone\b', 'null', add_fields_str)
                    query['addFields'] = json.loads(add_fields_str)
                except json.JSONDecodeError as e:
                    logger.error(f"✗ [Query Builder MCP] Помилка парсингу addFields: {e}")
                    return {'success': False, 'error': f'Помилка парсингу addFields: {str(e)}. Переконайтеся, що використовується null замість None.'}
            
            if 'sort' in query and isinstance(query['sort'], str):
                try:
                    sort_str = query['sort'].strip()
                    # Замінюємо Python None на JSON null перед парсингом
                    sort_str = re.sub(r'\bNone\b', 'null', sort_str)
                    query['sort'] = json.loads(sort_str)
                except json.JSONDecodeError as e:
                    logger.error(f"✗ [Query Builder MCP] Помилка парсингу sort: {e}")
                    return {'success': False, 'error': f'Помилка парсингу sort: {str(e)}. Переконайтеся, що використовується null замість None.'}
            
            result = self.query_builder.execute_query(query)
            if result.get('success'):
                logger.info(f"✓ [Query Builder MCP] Запит виконано успішно. Знайдено записів: {len(result.get('results', []))}")
            else:
                logger.error(f"✗ [Query Builder MCP] Помилка виконання: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Query Builder MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _execute_aggregation(
        self,
        collection_name: str,
        pipeline: List[Dict[str, Any]],
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """Виконує MongoDB aggregation pipeline."""
        logger.info(f"🔍 [Query Builder MCP] Виклик execute_aggregation для колекції {collection_name}")
        logger.debug(f"Pipeline: {json.dumps(pipeline, indent=2, ensure_ascii=False, default=str)}")
        
        try:
            result = self.query_builder.execute_aggregation(
                collection_name=collection_name,
                pipeline=pipeline,
                limit=limit
            )
            if result.get('success'):
                logger.info(f"✓ [Query Builder MCP] Aggregation виконано успішно. Знайдено записів: {len(result.get('results', []))}")
            else:
                logger.error(f"✗ [Query Builder MCP] Помилка виконання: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Query Builder MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _get_allowed_collections(self) -> Dict[str, Any]:
        """Отримує список дозволених колекцій."""
        logger.info("🔍 [Query Builder MCP] Виклик get_allowed_collections")
        try:
            result = {
                'success': True,
                'collections': list(self.query_builder.ALLOWED_COLLECTIONS),
                'max_results': self.query_builder.MAX_RESULTS
            }
            logger.info(f"✓ [Query Builder MCP] Дозволені колекції: {result['collections']}")
            return result
        except Exception as e:
            logger.error(f"✗ [Query Builder MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    # Analytics MCP tool implementations
    def _execute_analytics(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Виконує аналітичний запит."""
        logger.info(f"🔍 [Analytics MCP] Виклик execute_analytics")
        logger.debug(f"Запит: {json.dumps(query, indent=2, ensure_ascii=False, default=str)}")
        
        try:
            result = self.analytics_builder.execute_analytics_query(query)
            if result.get('success'):
                logger.info(f"✓ [Analytics MCP] Аналітика виконана успішно")
            else:
                logger.error(f"✗ [Analytics MCP] Помилка: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Analytics MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _list_metrics(self) -> Dict[str, Any]:
        """Отримує список доступних метрик."""
        logger.info("🔍 [Analytics MCP] Виклик list_metrics")
        try:
            from utils.analytics_metrics import AnalyticsMetrics
            metrics = AnalyticsMetrics.list_metrics()
            result = {
                'success': True,
                'metrics': metrics[:20] if len(metrics) > 20 else metrics,
                'total_count': len(metrics)
            }
            logger.info(f"✓ [Analytics MCP] Знайдено метрик: {len(metrics)}")
            return result
        except Exception as e:
            logger.error(f"✗ [Analytics MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    # Report MCP tool implementations
    def _generate_report(self, request: Dict[str, Any], return_base64: bool = True) -> Dict[str, Any]:
        """Генерує звіт."""
        logger.info(f"🔍 [Report MCP] Виклик generate_report")
        logger.debug(f"Запит: {json.dumps(request, indent=2, ensure_ascii=False, default=str)}")
        
        try:
            result = self.report_generator.generate_report(request, return_base64=return_base64)
            if result.get('success'):
                logger.info(f"✓ [Report MCP] Звіт згенеровано успішно")
            else:
                logger.error(f"✗ [Report MCP] Помилка: {result.get('error')}")
            return result
        except Exception as e:
            logger.exception(f"✗ [Report MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _list_templates(self) -> Dict[str, Any]:
        """Отримує список доступних шаблонів."""
        logger.info("🔍 [Report MCP] Виклик list_templates")
        try:
            from utils.report_templates import ReportTemplates
            templates = ReportTemplates.list_templates()
            result = {'success': True, 'templates': templates}
            logger.info(f"✓ [Report MCP] Знайдено шаблонів: {len(templates)}")
            return result
        except Exception as e:
            logger.error(f"✗ [Report MCP] Помилка: {e}")
            return {'success': False, 'error': str(e)}
    
    def _validate_query(self, user_query: str) -> Tuple[bool, Optional[str]]:
        """
        Валідує запит користувача на рівні сервісу.
        
        Args:
            user_query: Запит користувача
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        if not user_query or not user_query.strip():
            return False, "Запит не може бути порожнім"
        
        # Перевірка на спроби прямого доступу до БД
        forbidden_patterns = [
            'db.', 'collection.', 'mongodb://', 'mongo://',
            'pymongo', 'MongoClient', 'find_one', 'find(',
            'insert', 'update', 'delete', 'drop'
        ]
        
        query_lower = user_query.lower()
        for pattern in forbidden_patterns:
            if pattern in query_lower:
                logger.warning(f"Виявлено заборонений паттерн у запиті: {pattern}")
                return False, f"Запит містить заборонені операції. Використовуйте MCP tools для роботи з даними."
        
        return True, None
    
    def process_query(
        self,
        user_query: str,
        stream_callback: Optional[Callable[[str], None]] = None
    ) -> str:
        """
        Обробляє запит користувача з використанням LangChain агента.
        
        Використовує явний цикл агента:
        1. Plan: Агент аналізує запит та планує дії
        2. Act: Агент викликає tools
        3. Observe: Агент аналізує результати та приймає рішення
        
        Args:
            user_query: Запит користувача
            stream_callback: Функція для трансляції проміжних результатів
            
        Returns:
            Відповідь агента
        """
        logger.info("="*80)
        logger.info("ПОЧАТОК ОБРОБКИ ЗАПИТУ (LangChain Agent)")
        logger.info(f"Запит користувача: {user_query}")
        logger.info("="*80)
        
        # Валідація запиту
        is_valid, error_msg = self._validate_query(user_query)
        if not is_valid:
            logger.warning(f"Запит не пройшов валідацію: {error_msg}")
            return f"Помилка: {error_msg}"
        
        # Очищаємо історію для нового запиту
        self.conversation_history = []
        
        # Додаємо системний промпт
        system_msg = SystemMessage(content=self.system_prompt)
        self.conversation_history.append(system_msg)
        
        # Додаємо контекст поточної дати/часу
        now = datetime.now()
        context_info = f"""
## Контекст поточної дати та часу:
- Поточна дата та час: {now.strftime("%Y-%m-%d %H:%M:%S")}
- Поточна дата: {now.strftime("%Y-%m-%d")}
- Поточний час: {now.strftime("%H:%M:%S")}

## Корисні діапазони дат:
- За останню добу (24 години): від {(now - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")} до {now.strftime("%Y-%m-%dT%H:%M:%S")}
- За останній тиждень (7 днів): від {(now - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")} до {now.strftime("%Y-%m-%dT%H:%M:%S")}
- За останній місяць (30 днів): від {(now - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")} до {now.strftime("%Y-%m-%dT%H:%M:%S")}
"""
        
        context_msg = SystemMessage(content=context_info)
        self.conversation_history.append(context_msg)
        
        # Додаємо запит користувача
        user_msg = HumanMessage(content=user_query)
        self.conversation_history.append(user_msg)
        
        # Явний цикл агента (plan → act → observe)
        iteration = 0
        max_iterations = self.MAX_ITERATIONS
        
        while iteration < max_iterations:
            iteration += 1
            logger.info(f"--- Ітерація {iteration}/{max_iterations} ---")
            
            try:
                # Bind tools до LLM
                llm_with_tools = self.llm.bind_tools(self.tools)
                
                # Отримуємо відповідь від LLM
                logger.info("Відправляю запит до LLM...")
                response = llm_with_tools.invoke(self.conversation_history)
                
                logger.info(f"Отримано відповідь від LLM. Тип: {type(response)}")
                
                # Додаємо відповідь до історії
                self.conversation_history.append(response)
                
                # Перевіряємо, чи є виклики tools
                # У LangChain tool_calls може бути списком або атрибутом об'єкта
                tool_calls = []
                if hasattr(response, 'tool_calls'):
                    tool_calls_raw = response.tool_calls
                    if tool_calls_raw:
                        tool_calls = tool_calls_raw if isinstance(tool_calls_raw, list) else [tool_calls_raw]
                
                logger.info(f"Знайдено викликів tools: {len(tool_calls)}")
                
                # Якщо немає викликів tools - перевіряємо, чи є текстова відповідь
                if not tool_calls:
                    # Перевіряємо, чи є текстова відповідь
                    response_content = None
                    if hasattr(response, 'content'):
                        response_content = response.content
                        if isinstance(response_content, list):
                            # Якщо content - список, шукаємо текст
                            text_parts = [str(item) for item in response_content if isinstance(item, str)]
                            response_content = ' '.join(text_parts) if text_parts else None
                        elif not isinstance(response_content, str):
                            response_content = str(response_content) if response_content else None
                    
                    if response_content and response_content.strip():
                        logger.info("Знайдено фінальну текстову відповідь")
                        return response_content
                
                # Обмежуємо кількість викликів tools за ітерацію
                if len(tool_calls) > self.MAX_TOOL_CALLS_PER_ITERATION:
                    logger.warning(
                        f"Кількість викликів tools ({len(tool_calls)}) перевищує ліміт "
                        f"({self.MAX_TOOL_CALLS_PER_ITERATION}). Обмежую до {self.MAX_TOOL_CALLS_PER_ITERATION}."
                    )
                    tool_calls = tool_calls[:self.MAX_TOOL_CALLS_PER_ITERATION]
                
                # Виконуємо tools
                tool_messages = []
                for tool_call in tool_calls:
                    # Обробляємо різні формати tool_call
                    if isinstance(tool_call, dict):
                        tool_name = tool_call.get('name', '')
                        tool_args = tool_call.get('args', {})
                        tool_call_id = tool_call.get('id', '')
                    else:
                        # Якщо це об'єкт з атрибутами
                        tool_name = getattr(tool_call, 'name', '')
                        tool_args = getattr(tool_call, 'args', {})
                        tool_call_id = getattr(tool_call, 'id', '')
                    
                    logger.info(f"🔧 Виклик tool: {tool_name}")
                    logger.debug(f"Аргументи: {json.dumps(tool_args, indent=2, ensure_ascii=False, default=str)}")
                    
                    # Знаходимо відповідний tool
                    tool_func = None
                    for tool in self.tools:
                        if tool.name == tool_name:
                            tool_func = tool.func
                            break
                    
                    if not tool_func:
                        logger.error(f"Tool {tool_name} не знайдено")
                        tool_result = {'success': False, 'error': f'Tool {tool_name} не знайдено'}
                    else:
                        # Додаткова валідація для query_builder (безпека)
                        if tool_name == 'execute_query' and isinstance(tool_args, dict):
                            query = tool_args.get('query', tool_args)
                            if isinstance(query, dict):
                                # Перевірка на заборонені оператори
                                query_str = json.dumps(query, default=str)
                                if '$regex' in query_str.lower():
                                    logger.warning("Виявлено спробу використати $regex оператор (заборонено)")
                                    tool_result = {
                                        'success': False,
                                        'error': '$regex оператор заборонений з міркувань безпеки. Використовуйте analytics-mcp для фільтрації за статусом.'
                                    }
                                else:
                                    # Викликаємо tool
                                    try:
                                        tool_result = tool_func(query)
                                    except Exception as e:
                                        logger.exception(f"Помилка виконання tool {tool_name}: {e}")
                                        tool_result = {'success': False, 'error': str(e)}
                            else:
                                # Викликаємо tool
                                try:
                                    tool_result = tool_func(query)
                                except Exception as e:
                                    logger.exception(f"Помилка виконання tool {tool_name}: {e}")
                                    tool_result = {'success': False, 'error': str(e)}
                        else:
                            # Викликаємо tool
                            try:
                                # Обробляємо аргументи для tools, що приймають словники
                                if tool_name in ['execute_analytics']:
                                    # Ці tools приймають словник як перший аргумент
                                    if isinstance(tool_args, dict):
                                        if 'query' in tool_args:
                                            tool_result = tool_func(tool_args['query'])
                                        else:
                                            tool_result = tool_func(tool_args)
                                    else:
                                        tool_result = tool_func(tool_args)
                                elif tool_name == 'execute_aggregation':
                                    # execute_aggregation приймає collection_name, pipeline, limit
                                    if isinstance(tool_args, dict):
                                        collection_name = tool_args.get('collection_name', '')
                                        pipeline = tool_args.get('pipeline', [])
                                        limit = tool_args.get('limit', None)
                                        tool_result = tool_func(collection_name, pipeline, limit)
                                    else:
                                        tool_result = {'success': False, 'error': 'Invalid arguments for execute_aggregation'}
                                elif tool_name == 'generate_report':
                                    # generate_report приймає request та return_base64
                                    if isinstance(tool_args, dict):
                                        request = tool_args.get('request', tool_args)
                                        return_base64 = tool_args.get('return_base64', True)
                                        tool_result = tool_func(request, return_base64)
                                    else:
                                        tool_result = tool_func(tool_args, True)
                                else:
                                    # Інші tools приймають keyword arguments
                                    if isinstance(tool_args, dict):
                                        tool_result = tool_func(**tool_args)
                                    else:
                                        tool_result = tool_func(tool_args)
                            except Exception as e:
                                logger.exception(f"Помилка виконання tool {tool_name}: {e}")
                                tool_result = {'success': False, 'error': str(e)}
                    
                    logger.info(f"Результат tool {tool_name}: success={tool_result.get('success', False)}")
                    
                    # Створюємо ToolMessage для LangChain
                    tool_message = ToolMessage(
                        content=json.dumps(tool_result, ensure_ascii=False, default=str),
                        tool_call_id=tool_call_id
                    )
                    tool_messages.append(tool_message)
                
                # Додаємо результати tools до історії
                self.conversation_history.extend(tool_messages)
                
                # Продовжуємо цикл для отримання наступної відповіді
                continue
                
            except Exception as e:
                logger.exception(f"Помилка в ітерації {iteration}: {e}")
                error_msg = f"Помилка обробки запиту: {str(e)}"
                return error_msg
        
        # Якщо досягнуто максимум ітерацій
        logger.warning(f"Досягнуто максимум ітерацій ({max_iterations})")
        
        # Спробуємо отримати фінальну відповідь
        try:
            llm_with_tools = self.llm.bind_tools(self.tools)
            final_response = llm_with_tools.invoke(self.conversation_history)
            
            if hasattr(final_response, 'content') and final_response.content:
                return final_response.content
            else:
                return "Не вдалося сформувати відповідь після максимальної кількості ітерацій."
        except Exception as e:
            logger.exception(f"Помилка формування фінальної відповіді: {e}")
            return f"Помилка формування відповіді: {str(e)}"
        
        finally:
            logger.info("="*80)
            logger.info("ЗАВЕРШЕНО ОБРОБКУ ЗАПИТУ (LangChain Agent)")
            logger.info("="*80)
