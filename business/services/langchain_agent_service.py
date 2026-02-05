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
from typing import Dict, Any, List, Optional, Callable, Tuple, Union, TYPE_CHECKING

from config.settings import Settings
from utils.data_dictionary import DataDictionary
from utils.date_utils import format_date_display, format_datetime_display
from utils.analytics_builder import AnalyticsBuilder
from utils.query_builder import QueryBuilder
from utils.report_generator import ReportGenerator
from data.database.connection import MongoDBConnection

# LangChain imports
try:
    from langchain_core.tools import StructuredTool
    from langchain_core.messages import HumanMessage, SystemMessage, ToolMessage
    from langchain_google_genai import ChatGoogleGenerativeAI
    from langchain_openai import ChatOpenAI
    from langchain_anthropic import ChatAnthropic
    LANGCHAIN_AVAILABLE = True
except ImportError:
    StructuredTool = None  # type: ignore
    HumanMessage = None  # type: ignore
    SystemMessage = None  # type: ignore
    ToolMessage = None  # type: ignore
    ChatGoogleGenerativeAI = None  # type: ignore
    ChatOpenAI = None  # type: ignore
    ChatAnthropic = None  # type: ignore
    LANGCHAIN_AVAILABLE = False

# Налаштування логування
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
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
        
        # Флаг для відстеження, чи вже згенеровано Excel файл для поточного запиту
        self.excel_generated = False
    
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
  * generate_report: Generate reports from analytics-mcp or query-builder-mcp data sources
  * save_query_results_to_excel: Save query results directly to Excel file. Use this when user asks for Excel table with specific query results. Takes results from execute_query or execute_aggregation and saves them to Excel with specified columns and headers.

IMPORTANT - Security:
- $regex operator is FORBIDDEN in query-builder-mcp
- Use analytics-mcp for status filtering (it handles "active" correctly)
- Use analytics-mcp for region/city filtering (it handles joins automatically)

IMPORTANT - Data Access:
- For filtering by region/city/property_type/building_area_sqm/land_area_ha/status: ALWAYS use analytics-mcp FIRST
  * analytics-mcp supports filtering by building_area_sqm with MongoDB operators (e.g., {"building_area_sqm": {"$lte": 200}})
  * analytics-mcp automatically handles joins with llm_cache for these fields
  * analytics-mcp correctly handles status filtering (use "active" as status value)
  * Example: execute_analytics with metric="count", filters={"status": "active", "building_area_sqm": {"$lte": 200}}
  * **CRITICAL - Region and City Filtering:**
    - For region: use value WITHOUT "область" suffix (e.g., "Київська" NOT "Київська область", "Львівська" NOT "Львівська область")
    - For city: use city name directly (e.g., "Київ", "Львів", "Харків")
    - For Kyiv and Kyiv region: use $or to search for both:
      * Example: execute_analytics with filters={"status": "active", "$or": [{"city": "Київ"}, {"region": "Київська"}]}
      * This will find auctions in Kyiv city OR Kyiv region
    - Kyiv (м. Київ) is NOT part of Kyiv region - it's a separate city, so you need $or to search both
    - You can also use $in for multiple values: {"region": {"$in": ["Київська", "Львівська"]}}
- For getting detailed records with specific fields: use query-builder-mcp execute_aggregation AFTER getting count from analytics-mcp
  * Use execute_aggregation only when you need full records, not just counts
  * Always use the same filters that worked in analytics-mcp
  * For region/city filtering in execute_aggregation: use $lookup with llm_cache and filter by llm_cache_data.result.addresses
    * IMPORTANT: After $lookup and $unwind, use $match with $elemMatch to filter addresses array
    * Example pipeline for Kyiv and Kyiv region:
      [
        {"$match": {"auction_data.status": {"$regex": "^active"}}},
        {"$lookup": {"from": "llm_cache", "localField": "description_hash", "foreignField": "description_hash", "as": "llm_cache_data"}},
        {"$unwind": {"path": "$llm_cache_data", "preserveNullAndEmptyArrays": false}},
        {"$match": {"$or": [
          {"llm_cache_data.result.addresses": {"$elemMatch": {"settlement": "Київ"}}},
          {"llm_cache_data.result.addresses": {"$elemMatch": {"region": "Київська"}}}
        ]}},
        {"$limit": 1000}
      ]
    * CRITICAL: Always use preserveNullAndEmptyArrays: false in $unwind to exclude records without llm_cache data
    * CRITICAL: Filter addresses array using $elemMatch, not direct field access
- For complex queries with grouping (e.g., finding participants in multiple auctions): use query-builder-mcp execute_aggregation
- For generating reports (Excel, CSV, JSON): use report-mcp tools
  * generate_report: Use for reports from analytics-mcp or query-builder-mcp data sources
    Example: {"dataSource": "analytics-mcp:{\"metric\":\"count\",\"filters\":{\"status\":\"active\",\"building_area_sqm\":{\"$lte\":200}}}", "format": "xlsx", "columns": ["value", "region"]}
  * save_query_results_to_excel: Use for saving query results directly to Excel (RECOMMENDED for user requests like "сформуй відповідь в таблицю ексель")
    - First, execute query using execute_query or execute_aggregation to get results
    - Then, call save_query_results_to_excel with the results
    - Example workflow:
      1. execute_query with filters for active auctions, building_area_sqm <= 200
      2. save_query_results_to_excel with results, columns, and column_headers
    - This tool automatically handles Excel generation and returns base64-encoded file
  * IMPORTANT: analytics-mcp supports filtering by building_area_sqm with MongoDB operators like {"$lte": 200}, {"$gte": 100}, etc.
  * When user asks for Excel table: use save_query_results_to_excel with results from execute_query or execute_aggregation

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

IMPORTANT - Excel File Generation (CRITICAL - READ CAREFULLY):
- When user asks for Excel table/file ("сформуй відповідь в таблицю ексель", "збережи в Excel", etc.):
  **MANDATORY WORKFLOW - FOLLOW EXACTLY:**
  **OPTION 1 (RECOMMENDED): Direct execute_aggregation**
  1. Call execute_aggregation directly with pipeline that filters for active auctions and building_area_sqm <= 200
     * Example pipeline for "активні аукціони площею до 200 кв. м":
       [
         {"$match": {"auction_data.status": {"$regex": "^active"}}},
         {"$lookup": {"from": "llm_cache", "localField": "description_hash", "foreignField": "description_hash", "as": "llm_cache_data"}},
         {"$unwind": {"path": "$llm_cache_data", "preserveNullAndEmptyArrays": false}},
         {"$match": {"llm_cache_data.result.building_area_sqm": {"$lte": 200}}},
         {"$limit": 1000}
       ]
  2. **IMMEDIATELY AFTER** execute_aggregation returns results (check "data" or "results" field has records), call save_query_results_to_excel:
     * results: the "data" or "results" field from execute_aggregation response (MUST be a list of dictionaries)
     * columns: list of column names (e.g., ["auction_id", "building_area_sqm", "region", "city", "price"])
     * column_headers: dictionary with Ukrainian names (e.g., {"auction_id": "ID аукціону", "building_area_sqm": "Площа (м²)"})
  
  **OPTION 2: If execute_analytics is used first**
  1. Use execute_analytics to check if data exists (optional)
  2. **ALWAYS** call execute_aggregation even if execute_analytics returns 0 - it might be a filter issue
  3. **IMMEDIATELY AFTER** execute_aggregation returns results, call save_query_results_to_excel
  
- **CRITICAL RULES:**
  * After execute_aggregation returns data (even if count is small), you MUST call save_query_results_to_excel in the NEXT tool call
  * **DO NOT call execute_aggregation multiple times** - if you see results with "excel_file" field in the response, Excel is already generated. Use that result and provide final answer to user.
  * **DO NOT repeat the same query** - check conversation history to see if execute_aggregation was already called with similar parameters
  * If execute_aggregation returns 0 results, try different status filters (e.g., {"$regex": "^active"} instead of "active")
  * Always use $lookup to join with llm_cache when filtering by building_area_sqm or land_area_ha
  * **If tool_result contains "excel_file" field, Excel is already generated. Do NOT call execute_aggregation again. Provide final answer to user with information about the Excel file.**

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
    
    def _create_tools(self) -> List[Any]:  # type: ignore
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
                description="Виконує аналітичний запит з метриками та агрегаціями. Використовуй для отримання статистики та аналітики. Підтримує фільтрацію за region, city, property_type, status, building_area_sqm (з MongoDB операторами, наприклад {\"$lte\": 200}), land_area_ha. Автоматично виконує join з llm_cache для цих полів. ВАЖЛИВО: Якщо користувач просить Excel-таблицю, після execute_analytics (якщо total_count > 0) ОБОВ'ЯЗКОВО викликай execute_aggregation для отримання повних записів, а потім save_query_results_to_excel.",
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
            ),
            StructuredTool.from_function(
                func=self._save_query_results_to_excel,
                name="save_query_results_to_excel",
                description="Зберігає результати запиту у Excel файл. ВИКОРИСТОВУЙ ЦЕЙ TOOL ПІСЛЯ ОТРИМАННЯ РЕЗУЛЬТАТІВ З execute_query або execute_aggregation, коли користувач просить Excel-таблицю. Приймає список результатів (поле 'data' або 'results' з відповіді execute_query/execute_aggregation), колонки та заголовки колонок. За замовчуванням використовується стандартний формат Excel (як для файлів за день/тиждень) з українськими заголовками. Якщо користувач не зазначив інше, використовуй стандартний формат. Повертає base64-кодований Excel файл, який автоматично відправляється користувачу. ВИКЛИКАЙ ЦЕЙ TOOL НЕГАЙНО ПІСЛЯ ОТРИМАННЯ РЕЗУЛЬТАТІВ, не продовжуй запитувати дані.",
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
    
    def _get_standard_excel_format(self) -> Tuple[List[str], Dict[str, str]]:
        """
        Повертає стандартний формат Excel файлу (fieldnames та column_headers),
        який використовується для файлів за день/тиждень.
        
        Returns:
            Кортеж (fieldnames, column_headers)
        """
        fieldnames = [
            'date_updated',                      # Дата оновлення
            'address_region',                    # Область
            'address_city',                      # Населений пункт
            'address',                           # Адреса
            'property_type',                     # Тип нерухомості
            'cadastral_number',                  # Кадастровий номер
            'building_area_sqm',                 # Площа нерухомості (кв. м.)
            'land_area_ha',                      # Площа земельної ділянки (га)
            'base_price',                        # Стартова ціна
            'deposit_amount',                    # Розмір взносу
            'auction_start_date',                # Дата торгів
            'document_submission_deadline',      # Дата фінальної подачі документів
            'min_participants_count',            # Мінімальна кількість учасників
            'participants_count',                # Кількість зареєстрованих учасників
            'arrests_info',                      # Арешти
            'description',                       # Опис
            'auction_url',                       # Посилання
            'classification_code',               # Код класифікатора
            'is_repeat_auction',                 # Повторний аукціон
            'previous_auctions_links'            # Посилання на минулі аукціони
        ]
        
        column_headers = {
            'date_updated': 'Дата оновлення',
            'address_region': 'Область',
            'address_city': 'Населений пункт',
            'address': 'Адреса',
            'property_type': 'Тип нерухомості',
            'cadastral_number': 'Кадастровий номер',
            'building_area_sqm': 'Площа нерухомості (кв. м.)',
            'land_area_ha': 'Площа земельної ділянки (га)',
            'base_price': 'Стартова ціна',
            'deposit_amount': 'Розмір взносу',
            'auction_start_date': 'Дата торгів',
            'document_submission_deadline': 'Дата фінальної подачі документів',
            'min_participants_count': 'Мінімальна кількість учасників',
            'participants_count': 'Кількість зареєстрованих учасників',
            'arrests_info': 'Арешти',
            'description': 'Опис',
            'auction_url': 'Посилання',
            'classification_code': 'Код класифікатора',
            'is_repeat_auction': 'Повторний аукціон',
            'previous_auctions_links': 'Посилання на минулі аукціони'
        }
        
        return fieldnames, column_headers
    
    def _extract_field_value(self, row: Dict[str, Any], field_name: str) -> Any:
        """
        Витягує значення поля з рядка даних, враховуючи різні можливі структури.
        
        Args:
            row: Рядок даних з execute_aggregation
            field_name: Назва поля в стандартному форматі Excel
            
        Returns:
            Значення поля або порожній рядок
        """
        # Спочатку перевіряємо, чи поле є безпосередньо в рядку
        if field_name in row:
            return row[field_name]
        
        # Мапінг стандартних полів на можливі шляхи в даних
        field_mapping = {
            'date_updated': [
                'date_updated',
                'auction_data.dateModified',
                'auction_data.dateCreated'
            ],
            'address_region': [
                'address_region',
                'llm_cache_data.result.addresses.0.region',
                'llm_result.result.addresses.0.region',
                'addresses.0.region'
            ],
            'address_city': [
                'address_city',
                'llm_cache_data.result.addresses.0.settlement',
                'llm_result.result.addresses.0.settlement',
                'addresses.0.settlement'
            ],
            'address': [
                'address',
                'formatted_address'
            ],
            'property_type': [
                'property_type',
                'llm_cache_data.result.property_type',
                'llm_result.result.property_type'
            ],
            'cadastral_number': [
                'cadastral_number',
                'llm_cache_data.result.cadastral_number',
                'llm_result.result.cadastral_number'
            ],
            'building_area_sqm': [
                'building_area_sqm',
                'llm_cache_data.result.building_area_sqm',
                'llm_result.result.building_area_sqm'
            ],
            'land_area_ha': [
                'land_area_ha',
                'llm_cache_data.result.land_area_ha',
                'llm_result.result.land_area_ha'
            ],
            'base_price': [
                'base_price',
                'amount',
                'auction_data.value.amount',
                'value.amount'
            ],
            'deposit_amount': [
                'deposit_amount',
                'auction_data.guarantee.amount',
                'guarantee.amount'
            ],
            'auction_start_date': [
                'auction_start_date',
                'auction_data.auctionPeriod.startDate',
                'auctionPeriod.startDate'
            ],
            'document_submission_deadline': [
                'document_submission_deadline',
                'auction_data.enquiryPeriod.endDate',
                'enquiryPeriod.endDate'
            ],
            'min_participants_count': [
                'min_participants_count',
                'auction_data.minNumberOfQualifiedBids',
                'minNumberOfQualifiedBids'
            ],
            'participants_count': [
                'participants_count',
                'bids_count'
            ],
            'arrests_info': [
                'arrests_info',
                'auction_data.arrests',
                'arrests'
            ],
            'description': [
                'description',
                'auction_data.description.uk_UA',
                'auction_data.description.en_US'
            ],
            'auction_url': [
                'auction_url',
                'url'
            ],
            'classification_code': [
                'classification_code',
                'auction_data.items.0.classification.id',
                'items.0.classification.id'
            ],
            'is_repeat_auction': [
                'is_repeat_auction'
            ],
            'previous_auctions_links': [
                'previous_auctions_links'
            ]
        }
        
        # Шукаємо значення за мапінгом
        if field_name in field_mapping:
            for path in field_mapping[field_name]:
                value = self._get_nested_value(row, path)
                if value is not None and value != '':
                    # Додаткова обробка для специфічних полів
                    if field_name == 'auction_url' and value and not value.startswith('http'):
                        # Якщо це auction_id, формуємо URL
                        auction_id = str(value)
                        return f"https://prozorro.sale/auction/{auction_id}"
                    elif field_name == 'description' and isinstance(value, dict):
                        # Опис може бути об'єктом з uk_UA або en_US
                        return value.get('uk_UA', value.get('en_US', ''))
                    elif field_name in ['auction_start_date', 'document_submission_deadline', 'date_updated'] and value:
                        # Форматуємо дату в київському часі
                        if hasattr(value, 'year'):
                            return format_datetime_display(value, '%d.%m.%Y %H:%M')
                        return format_date_display(str(value), '%d.%m.%Y %H:%M')
                    return value
        
        return ''
    
    def _get_nested_value(self, data: Dict[str, Any], path: str) -> Any:
        """
        Отримує значення з вкладеного словника за шляхом (наприклад, "a.b.c").
        
        Args:
            data: Словник з даними
            path: Шлях до значення (наприклад, "a.b.c" або "a.0.b" для масивів)
            
        Returns:
            Значення або None
        """
        parts = path.split('.')
        current = data
        
        for part in parts:
            if part.isdigit():
                # Індекс масиву
                idx = int(part)
                if isinstance(current, list) and 0 <= idx < len(current):
                    current = current[idx]
                else:
                    return None
            else:
                # Ключ словника
                if isinstance(current, dict):
                    current = current.get(part)
                else:
                    return None
                
                if current is None:
                    return None
        
        return current
    
    def _save_query_results_to_excel(
        self,
        results: List[Dict[str, Any]],
        columns: Optional[List[str]] = None,
        column_headers: Optional[Dict[str, str]] = None,
        filename: Optional[str] = None,
        use_standard_format: bool = True
    ) -> Dict[str, Any]:
        """
        Зберігає результати запиту у Excel файл.
        
        Args:
            results: Список словників з результатами запиту
            columns: Список колонок для включення (опціонально, якщо не вказано - використовуються стандартні або всі ключі з першого запису)
            column_headers: Словник з українськими назвами колонок (ключ -> назва, опціонально)
            filename: Назва файлу (опціонально)
            use_standard_format: Якщо True, використовує стандартний формат Excel (як для файлів за день/тиждень)
            
        Returns:
            Словник з результатом: success, file_base64, filename, error
        """
        logger.info("🔍 [Report MCP] Виклик save_query_results_to_excel")
        logger.info(f"Аргументи: results type={type(results)}, columns={columns}, column_headers={column_headers}, filename={filename}, use_standard_format={use_standard_format}")
        
        try:
            # Перевіряємо, чи results - це список
            if not isinstance(results, list):
                logger.error(f"results має бути списком, але отримано: {type(results)}")
                return {'success': False, 'error': f'results має бути списком, але отримано {type(results)}'}
            
            if not results:
                return {'success': False, 'error': 'Немає результатів для збереження'}
            
            logger.info(f"Кількість результатів: {len(results)}")
            
            # Визначаємо колонки, якщо не вказано
            if columns is None:
                if use_standard_format:
                    # Використовуємо стандартний формат
                    standard_fieldnames, _ = self._get_standard_excel_format()
                    # Перевіряємо, які з стандартних полів є в даних
                    available_keys = set(results[0].keys()) if results else set()
                    # Використовуємо стандартні поля, які є в даних, плюс додаємо інші поля з даних
                    columns = [col for col in standard_fieldnames if col in available_keys]
                    # Додаємо інші поля з даних, які не в стандартному форматі
                    for key in available_keys:
                        if key not in columns:
                            columns.append(key)
                    logger.info(f"Використовую стандартний формат. Колонки: {columns}")
                else:
                    # Використовуємо всі ключі з першого запису
                    columns = list(results[0].keys())
                    logger.info(f"Автоматично визначено колонки: {columns}")
            
            # Визначаємо заголовки колонок, якщо не вказано
            if column_headers is None:
                if use_standard_format:
                    # Використовуємо стандартні заголовки
                    _, standard_headers = self._get_standard_excel_format()
                    column_headers = {}
                    for col in columns:
                        if col in standard_headers:
                            column_headers[col] = standard_headers[col]
                        else:
                            # Для нестандартних колонок використовуємо ключ як заголовок
                            column_headers[col] = col
                    logger.info(f"Використовую стандартні заголовки")
                else:
                    # Використовуємо ключі як заголовки
                    column_headers = {col: col for col in columns}
            
            # Підготовка даних з мапінгом на стандартний формат
            formatted_data = []
            for row in results:
                formatted_row = {}
                for col in columns:
                    value = self._extract_field_value(row, col)
                    
                    # Обробляємо None та складні типи
                    if value is None:
                        formatted_row[col] = ''
                    elif isinstance(value, (dict, list)):
                        # Для складних типів конвертуємо в JSON рядок
                        formatted_row[col] = json.dumps(value, ensure_ascii=False, default=str)
                    else:
                        formatted_row[col] = value
                formatted_data.append(formatted_row)
            
            # Генеруємо Excel
            from utils.file_utils import generate_excel_in_memory
            import base64
            
            excel_bytes = generate_excel_in_memory(formatted_data, columns, column_headers)
            excel_bytes.seek(0)
            
            # Конвертуємо в base64
            file_base64 = base64.b64encode(excel_bytes.read()).decode('utf-8')
            
            # Генеруємо назву файлу, якщо не вказано
            if filename is None:
                from datetime import timezone
                timestamp = format_datetime_display(datetime.now(timezone.utc), '%Y%m%d_%H%M%S')
                filename = f'query_results_{timestamp}.xlsx'
            
            result = {
                'success': True,
                'file_base64': file_base64,
                'filename': filename,
                'rows_count': len(formatted_data),
                'columns_count': len(columns)
            }
            
            logger.info(f"✓ [Report MCP] Excel файл згенеровано: {filename}, рядків: {len(formatted_data)}, колонок: {len(columns)}")
            return result
            
        except Exception as e:
            logger.exception(f"✗ [Report MCP] Помилка: {e}")
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
    
    def _extract_excel_files_from_history(self) -> List[Dict[str, Any]]:
        """
        Витягує Excel файли з історії conversation (з результатів tools).
        
        Returns:
            Список словників з інформацією про Excel файли: [{'file_base64': ..., 'filename': ..., 'rows_count': ...}, ...]
        """
        excel_files = []
        logger.info(f"Перевіряю conversation_history для Excel файлів. Кількість повідомлень: {len(self.conversation_history)}")
        
        for i, msg in enumerate(self.conversation_history):
            if isinstance(msg, ToolMessage):
                try:
                    result = json.loads(msg.content)
                    logger.debug(f"ToolMessage {i}: success={result.get('success')}, filename={result.get('filename')}")
                    
                    if result.get('success') and result.get('file_base64') and result.get('filename'):
                        # Перевіряємо, чи це Excel файл
                        filename = result.get('filename', '')
                        if filename.endswith('.xlsx') or 'excel' in filename.lower():
                            logger.info(f"Знайдено Excel файл: {filename}")
                            excel_files.append({
                                'file_base64': result['file_base64'],
                                'filename': result['filename'],
                                'rows_count': result.get('rows_count', 0),
                                'columns_count': result.get('columns_count', 0)
                            })
                except (json.JSONDecodeError, KeyError) as e:
                    logger.debug(f"Помилка парсингу ToolMessage {i}: {e}")
                    continue
        
        logger.info(f"Знайдено Excel файлів: {len(excel_files)}")
        return excel_files
    
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
            Відповідь агента (текст)
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
        
        # Зберігаємо запит користувача для використання в циклі
        self.current_user_query = user_query
        
        # Скидаємо флаг генерації Excel для нового запиту
        self.excel_generated = False
        
        # Очищаємо історію для нового запиту
        self.conversation_history = []
        
        # Додаємо системний промпт
        system_msg = SystemMessage(content=self.system_prompt)
        self.conversation_history.append(system_msg)
        
        # Додаємо контекст поточної дати/часу (київський час)
        from datetime import timezone
        now_utc = datetime.now(timezone.utc)
        context_info = f"""
## Контекст поточної дати та часу (Київ):
- Поточна дата та час: {format_datetime_display(now_utc, "%Y-%m-%d %H:%M:%S")}
- Поточна дата: {format_datetime_display(now_utc, "%Y-%m-%d")}
- Поточний час: {format_datetime_display(now_utc, "%H:%M:%S")}

## Корисні діапазони дат (UTC для запитів):
- За останню добу (24 години): від {(now_utc - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}
- За останній тиждень (7 днів): від {(now_utc - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}
- За останній місяць (30 днів): від {(now_utc - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}
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
                    logger.info(f"Аргументи: {json.dumps(tool_args, indent=2, ensure_ascii=False, default=str)[:500]}...")  # Обмежуємо довжину для читабельності
                    
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
                                elif tool_name == 'save_query_results_to_excel':
                                    # save_query_results_to_excel приймає results, columns, column_headers, filename
                                    if isinstance(tool_args, dict):
                                        # Перевіряємо, чи results передано як список
                                        if 'results' in tool_args and not isinstance(tool_args['results'], list):
                                            logger.warning(f"results не є списком: {type(tool_args['results'])}")
                                            # Спробуємо конвертувати в список, якщо це можливо
                                            if isinstance(tool_args['results'], str):
                                                try:
                                                    tool_args['results'] = json.loads(tool_args['results'])
                                                except json.JSONDecodeError:
                                                    pass
                                        tool_result = tool_func(**tool_args)
                                    else:
                                        logger.error(f"Invalid arguments type for save_query_results_to_excel: {type(tool_args)}")
                                        tool_result = {'success': False, 'error': f'Invalid arguments for save_query_results_to_excel: expected dict, got {type(tool_args)}'}
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
                    
                    # Логуємо, якщо отримано результати для Excel
                    if tool_name in ['execute_query', 'execute_aggregation'] and tool_result.get('success'):
                        data_count = 0
                        results_data = []
                        if 'data' in tool_result:
                            results_data = tool_result.get('data', [])
                            data_count = len(results_data)
                        elif 'results' in tool_result:
                            results_data = tool_result.get('results', [])
                            data_count = len(results_data)
                        
                        if data_count > 0:
                            logger.warning(f"⚠️⚠️⚠️ ОТРИМАНО {data_count} РЕЗУЛЬТАТІВ З {tool_name}! Якщо користувач просив Excel, НЕГАЙНО викликай save_query_results_to_excel!")
                            
                            # АВТОМАТИЧНЕ ЗБЕРЕЖЕННЯ В EXCEL ВИМКНЕНО (тимчасово)
                            # Перевіряємо, чи користувач просив Excel
                            # user_query_lower = getattr(self, 'current_user_query', '').lower()
                            # excel_keywords = ['excel', 'ексель', 'таблицю', 'таблиця', 'файл', 'збережи', 'зберегти']
                            # user_wants_excel = any(keyword in user_query_lower for keyword in excel_keywords)
                            
                            # if user_wants_excel and not self.excel_generated:
                            #     logger.info(f"🔍 Користувач просив Excel. Автоматично викликаю save_query_results_to_excel...")
                            #     ... (код вимкнено)
                            
                            logger.info(f"ℹ️ Автоматичне збереження в Excel вимкнено. Користувач може викликати save_query_results_to_excel вручну.")
                        else:
                            logger.info(f"ℹ️ {tool_name} повернув 0 результатів. Можливо, потрібно перевірити фільтри або використати execute_analytics для перевірки наявності даних.")
                    
                    # Логуємо результати execute_analytics для діагностики
                    if tool_name == 'execute_analytics' and tool_result.get('success'):
                        result_data = tool_result.get('data', [])
                        total_count = tool_result.get('total_count', 0)
                        logger.info(f"ℹ️ execute_analytics результат: total_count={total_count}, data records={len(result_data) if result_data else 0}")
                        if total_count > 0 or (result_data and len(result_data) > 0):
                            logger.warning(f"⚠️⚠️⚠️ execute_analytics знайшов {total_count if total_count > 0 else len(result_data)} записів! Якщо користувач просив Excel, НЕГАЙНО викликай execute_aggregation для отримання повних записів, а потім save_query_results_to_excel!")
                    
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
