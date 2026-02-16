# -*- coding: utf-8 -*-
"""
Сервіс для роботи з LLM агентом, який використовує MCP інструменти.
"""

import json
import re
import time
import logging
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Dict, Any, List, Optional, Callable, AsyncGenerator
from config.settings import Settings
from utils.data_dictionary import DataDictionary
from utils.date_utils import format_datetime_display, to_kyiv
from utils.analytics_builder import AnalyticsBuilder
from utils.query_builder import QueryBuilder
from utils.report_generator import ReportGenerator
from data.database.connection import MongoDBConnection

try:
    from google import genai
    from google.genai import types
    GENAI_AVAILABLE = True
except ImportError:
    GENAI_AVAILABLE = False


# Налаштування логування
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Створюємо handler для виводу в консоль, якщо його ще немає
if not logger.handlers:
    console_handler = logging.StreamHandler()
    console_handler.setLevel(logging.INFO)
    formatter = logging.Formatter(
        '[LLM Agent] %(asctime)s - %(levelname)s - %(message)s',
        datefmt='%Y-%m-%d %H:%M:%S'
    )
    console_handler.setFormatter(formatter)
    logger.addHandler(console_handler)
    # Запобігаємо дублюванню логів через батьківські логери
    logger.propagate = False


class LLMAgentService:
    """Сервіс для роботи з LLM агентом з підтримкою MCP інструментів."""
    
    @staticmethod
    def _load_glossary() -> str:
        """
        Завантажує глосарій розробника з файлу.
        
        Returns:
            Текст глосарію або порожній рядок, якщо файл не знайдено
        """
        try:
            project_root = Path(__file__).parent.parent.parent
            glossary_path = project_root / 'docs' / 'developer_glossary.md'
            
            if glossary_path.exists():
                with open(glossary_path, 'r', encoding='utf-8') as f:
                    return f.read()
            return ""
        except Exception:
            return ""
    
    @classmethod
    def _get_system_prompt(cls) -> str:
        """
        Формує системний промпт з урахуванням глосарію.
        
        Returns:
            Системний промпт з глосарієм
        """
        base_prompt = """You are a data analyst assistant for a real estate auction database.

You do NOT know the database structure in advance.
You MUST use provided tools to discover:
- collections
- fields
- relations
- available analytics

You are NOT allowed to invent fields or collections.
If needed data is unavailable, explicitly say so.

You can:
1. Inspect schema - use schema-mcp tools to understand database structure
2. Request analytics - use analytics-mcp tools to get aggregated data
3. Request report generation - use report-mcp tools to generate files
4. Query data - use query-builder-mcp execute_query to query specific data
5. Execute aggregation - use query-builder-mcp execute_aggregation for complex queries with grouping (e.g., finding participants in multiple auctions)

IMPORTANT - Fields for Analytics and Reports:
The database stores raw data in nested structures (auction_data, llm_cache.result), but for analytics and reports, use these FLATTENED FIELDS:

HOW TO ACCESS FLATTENED FIELDS:

1. For filtering by region/city/property_type: ALWAYS use analytics-mcp with filters:
   - Example for count with region: {"metric": "count", "filters": {"status": "active", "region": "Львівська"}}
   - Example for count with region and city: {"metric": "count", "filters": {"status": "active", "region": "Львівська", "city": "Львів"}}
   - analytics-mcp automatically joins with llm_cache and filters correctly - you don't need to handle joins manually
   - For region: use filter "region": "Львівська" (без слова "область")
   - For city: use filter "city": "Львів"
   - For property_type: use filter "property_type": "Нерухомість"
   - DO NOT use query-builder-mcp for region/city/property_type filtering - use analytics-mcp instead!

2. For status filtering: Use analytics-mcp with status filter:
   - Filter by: "status": "active" - this will match all statuses starting with "active" (active_tendering, active_rectification, active_qualification, active_awarded, active_auction)
   - DO NOT use query-builder-mcp with $regex for status filtering - use analytics-mcp instead!
   - analytics-mcp automatically handles status filtering correctly

3. For analytics queries: Use analytics-mcp which automatically:
   - Joins with llm_cache when needed (for region, city, property_type, building_area, land_area)
   - Extracts flattened fields from llm_cache.result
   - Calculates metrics (priceFinal, building_area_sqm, land_area_ha)
   - Groups by region, city, property_type, status, year, month, quarter

4. For queries requiring computed fields and sorting (like "top 10 by price per m²"):
   - IMPORTANT: For filtering by region/city/property_type, ALWAYS prefer using analytics-mcp instead of query-builder-mcp, as analytics-mcp handles joins automatically and is optimized for these queries.
   - Use query-builder-mcp with join, addFields, and sort ONLY when you need:
     * Computed fields (like price_per_m2)
     * Specific sorting of individual records
     * Raw data with custom calculations
   - Example for "top 10 by price per m² in Lviv region and city":
     {
       "collection": "prozorro_auctions",
       "join": [{"collection": "llm_cache", "on": ["description_hash", "description_hash"]}],
       "filters": {
         "auction_data.status": {"$regex": "^active"},
         "llm_result.result.addresses": {
           "$elemMatch": {
             "region": "Львівська",
             "settlement": "Львів"
           }
         }
       },
       "addFields": "{\"price_per_m2\": {\"$cond\": {\"if\": {\"$or\": [{\"$gt\": [{\"$convert\": {\"input\": \"$llm_result.result.building_area_sqm\", \"to\": \"double\", \"onError\": null, \"onNull\": null}}, 0]}, {\"$gt\": [{\"$multiply\": [{\"$convert\": {\"input\": \"$llm_result.result.land_area_ha\", \"to\": \"double\", \"onError\": null, \"onNull\": null}}, 10000]}, 0]}]}, \"then\": {\"$divide\": [{\"$convert\": {\"input\": {\"$ifNull\": [{\"$arrayElemAt\": [\"$auction_data.contracts.value.amount\", 0]}, {\"$ifNull\": [{\"$arrayElemAt\": [\"$auction_data.awards.value.amount\", 0]}, \"$auction_data.value.amount\"]}]}, \"to\": \"double\", \"onError\": null, \"onNull\": null}}, {\"$ifNull\": [{\"$convert\": {\"input\": \"$llm_result.result.building_area_sqm\", \"to\": \"double\", \"onError\": null, \"onNull\": null}}, {\"$multiply\": [{\"$convert\": {\"input\": \"$llm_result.result.land_area_ha\", \"to\": \"double\", \"onError\": null, \"onNull\": null}}, 10000]}]}]}, \"else\": null}}}",
       "sort": "{\"price_per_m2\": 1}",
       "limit": 10,
       "projection": ["auction_id", "price_per_m2", "llm_result.result.building_area_sqm", "llm_result.result.land_area_ha", "llm_result.result.property_type"]
     }
     CRITICAL: addFields and sort must be JSON STRINGS (escaped), not objects! Pass them as escaped JSON strings like "{\"field\": \"value\"}".
     IMPORTANT: In JSON strings, use "null" (not "None") for null values. Python None is not valid JSON.
     NOTE: Use "auction_id" (top level field), NOT "auction_data.id" (doesn't exist). For price_per_m2: use building_area_sqm if available, otherwise use land_area_ha * 10000
   - IMPORTANT: To generate auction links, use auction_id or auction_data._id field:
     * Link format: https://prozorro.sale/auction/{auction_id} or https://prozorro.sale/auction/{auction_data._id}
     * Always include "auction_id" or "auction_data._id" in projection when user asks for links
     * Example projection: ["auction_id", "price_per_m2", "llm_result.result.building_area_sqm"]
     * Note: auction_data.id does NOT exist - use auction_id (top level) or auction_data._id instead
   - Note: After join with llm_cache, use "llm_result.result.addresses" with $elemMatch for region/city filtering
   - For status filtering, you can use $regex "^active" but analytics-mcp is preferred
   - When returning results with links, format them as: "1. [Link text](https://prozorro.sale/auction/{id}) - Price: {price_per_m2} UAH/m²"
   - To join prozorro_auctions with llm_cache, use EXACT format:
     {
       "collection": "prozorro_auctions",
       "join": [{
         "collection": "llm_cache",
         "on": ["description_hash", "description_hash"]
       }],
       "filters": {...}
     }
   - CRITICAL: The "on" field MUST be an array with exactly 2 elements: [localField, foreignField]
     - Correct: ["description_hash", "description_hash"]
     - Wrong: {"localField": "description_hash", "foreignField": "description_hash"}
     - Wrong: "description_hash"
   - For filtering by region/city after join, use filters on the joined data:
     - After joining with llm_cache, you can filter by "llm_result.result.addresses.region"
     - But remember: analytics-mcp is better for this!

AVAILABLE FLATTENED FIELDS (see Data Dictionary for details):
- address_region: Область (from llm_cache.result.addresses[].region or auction_data.items[].address.region.uk_UA)
- address_city: Місто (from llm_cache.result.addresses[].settlement or auction_data.items[].address.locality.uk_UA)
- property_type: Тип нерухомості (from llm_cache.result.property_type or auction_data.items[].classification.id)
- status: Статус (from auction_data.status)
- building_area_sqm: Площа нерухомості в м² (from llm_cache.result.building_area_sqm or auction_data.items[].quantity.value)
- land_area_ha: Площа землі в га (from llm_cache.result.land_area_ha or auction_data.items[].quantity.value)
- priceFinal: Фінальна ціна в UAH (from auction_data.contracts[].value.amount or auction_data.awards[].value.amount)
- participants_count: Кількість унікальних зареєстрованих учасників (from auction_data.bids[].bidders[].identifier.id - унікальні значення)

For filtering by region/city, use analytics-mcp or query-builder-mcp with join to llm_cache.

IMPORTANT - Counting Participants:
- When user asks for auctions with "most registered participants" or "найбільша кількість зареєстрованих учасників":
  1. Use query-builder-mcp with join to llm_cache (for region filtering)
  2. Use addFields to calculate participants_count from auction_data.bids[].bidders[].identifier.id
  3. Sort by participants_count descending
  4. Return auction_id values
- Structure: 
  * auction_data.bids[] - масив заявок (bids)
  * Each bid has bidders[] - масив учасників у заявці
  * Each bidder has identifier.id - унікальний ідентифікатор учасника
- IMPORTANT: Count UNIQUE participants (by identifier.id), not just the number of bids!
- CRITICAL: For addFields with complex MongoDB expressions, use SIMPLE format first:
  * Try simple calculations first (e.g., {"bids_count": {"$size": "$auction_data.bids"}})
  * If you need complex expressions, build them step by step
  * The addFields parameter accepts any MongoDB $addFields expression as a JSON object
- Example query for "аукціони у Львівській області з найбільшою кількістю учасників":
  {
    "collection": "prozorro_auctions",
    "filters": {
      "auction_data.status": {"$regex": "^active"},
      "llm_result.result.addresses": {
        "$elemMatch": {"region": "Львівська"}
      }
    },
    "join": [{"collection": "llm_cache", "on": ["description_hash", "description_hash"]}],
    "addFields": "{\"participants_count\": {\"$size\": {\"$setUnion\": [{\"$reduce\": {\"input\": {\"$ifNull\": [\"$auction_data.bids\", []]}, \"initialValue\": [], \"in\": {\"$concatArrays\": [\"$$value\", {\"$map\": {\"input\": {\"$ifNull\": [\"$$this.bidders\", []]}, \"as\": \"bidder\", \"in\": {\"$ifNull\": [\"$$bidder.identifier.id\", \"\"]}}}]}}}]}}}",
    "sort": "{\"participants_count\": -1}",
    "limit": 10,
    "projection": ["auction_id", "participants_count"]
  }
  CRITICAL: addFields and sort must be JSON STRINGS (escaped), not objects!
  IMPORTANT: When creating the addFields JSON string, make sure:
  - The entire JSON object is properly escaped
  - No extra characters after the closing brace
  - The $setUnion operator receives an array (result of $reduce), not an object
  - If you get "Extra data" error, check that the JSON string ends exactly at the closing brace
- CRITICAL: Always include "auction_id" in projection when user asks for "ідентифікатори аукціонів"
- The participants_count field counts UNIQUE participants by their identifier.id across all bids
- Format response as a list with VALID links. auction_id for prozorro.sale MUST be format like LSE001-UA-20260112-18611 (NOT 24-char hex _id).
  Example: "1. [Аукціон](https://prozorro.sale/auction/LSE001-UA-20260112-18611) — 150 учасників"
- NOTE: If you get MALFORMED_FUNCTION_CALL error, try simplifying the addFields expression or use a simpler calculation

Always explain what you're doing and why.
If you need to use multiple tools, do it step by step and explain each step.
When generating reports, explain what data will be included.

IMPORTANT - Returning Results with Links:
- When user asks for auction links (like "виведи посилання на 10 аукціонів"), you MUST:
  1. Use query-builder-mcp with join to llm_cache
  2. Include "auction_id" in the projection - REQUIRED for links. Use top-level auction_id (format LSE001-UA-...), NOT _id (MongoDB ObjectId)
     * CRITICAL: _id (24 hex chars) is INVALID for prozorro.sale URLs — use auction_id only
  3. Calculate price_per_m2 using addFields (use $convert for type safety)
  4. Sort by price_per_m2 (ascending for lowest price)
  5. Filter by region/city using llm_result fields (AFTER join, not before)
  6. IMPORTANT: Only filter by property_type if user explicitly asks for a specific type (e.g., "нерухомість")
     - If user says "по нерухомості" - filter by "Нерухомість"
     - If user just says "у Львівській області" - do NOT filter by property_type, include all types
  7. Format links as: https://prozorro.sale/auction/{auction_id}
  8. Present results in a numbered list with clickable links
  9. Include relevant information (price per m², area, property type, etc.)
  10. For price_per_m2 calculation, handle both building_area_sqm (for buildings) and land_area_ha (for land):
      - Use building_area_sqm if available
      - Otherwise use land_area_ha * 10000 (convert hectares to m²)
      - This allows calculating price per m² for both "Нерухомість" and "Земля під будівництво"
- Example query structure:
  {
    "collection": "prozorro_auctions",
    "join": [{"collection": "llm_cache", "on": ["description_hash", "description_hash"]}],
    "filters": {
      "auction_data.status": {"$regex": "^active"},
      "llm_result.result.addresses": {"$elemMatch": {"region": "Львівська", "settlement": "Львів"}}
      // IMPORTANT: Only filter by property_type if user explicitly asks for a specific type
      // Otherwise, include all property types in the region/city
    },
    "addFields": {"price_per_m2": "..."},
    "sort": {"price_per_m2": 1},
    "limit": 10,
    "projection": ["auction_data.id", "price_per_m2", "llm_result.result.building_area_sqm"]
  }
- Example response format:
  "Знайдено 10 аукціонів з найнижчою ціною за м² у Львівській області, м. Львів:
  1. [Аукціон](https://prozorro.sale/auction/696b8887ee733fae22328a9f) - 2386.05 UAH/m², площа землі: 0.043 га, тип: Земля під будівництво
  2. [Аукціон](https://prozorro.sale/auction/696b8887ee733fae22328a9g) - 3000.00 UAH/m², площа: 60 м², тип: Нерухомість
  ..."
- CRITICAL: Always use "auction_id" field (top level) for links, NOT "auction_data.id" (field doesn't exist)
- NEVER return empty responses - always explain what you found or why you couldn't find data
- If query returns empty results, explain possible reasons and suggest alternatives

IMPORTANT - Interpreting Results:
- If analytics returns empty results (count: 0), it means no data matches the filters, NOT an error
- Always report the actual count/value from results, even if it's 0
- If results are empty, explain that no data was found matching the criteria
- For region filters, use the region name WITHOUT "область" (e.g., "Львівська" not "Львівська область")

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

IMPORTANT - Security Restrictions:
- The $regex operator is FORBIDDEN in query-builder-mcp for security reasons
- DO NOT attempt to use $regex in filters - it will be rejected
- For status filtering that requires pattern matching (like "active*"), use analytics-mcp instead
- analytics-mcp automatically handles status filtering with "active" to match all active_* statuses
- If you need to filter by status, ALWAYS use analytics-mcp, not query-builder-mcp

IMPORTANT - Current Date and Time Context:
You have access to the current date and time. Use this information to calculate date ranges for queries like "за останню добу" (last 24 hours), "за останній тиждень" (last 7 days), etc.
When you receive the current date/time in the query context, use it to:
- Calculate "за останню добу" = from (current_datetime - 24 hours) to current_datetime
- Calculate "за останній тиждень" = from (current_datetime - 7 days) to current_datetime
- Calculate "за останній місяць" = from (current_datetime - 30 days) to current_datetime
- Format dates in ISO format (YYYY-MM-DDTHH:MM:SS) or date format (YYYY-MM-DD) for filters

Respond in Ukrainian language."""
        
        glossary = cls._load_glossary()
        if glossary:
            base_prompt += f"""

## Термінологія проекту (Developer Glossary)

Використовуй цю термінологію при відповідях та роботі з даними:

{glossary}

Важливо: Використовуй терміни з глосарію правильно. Наприклад:
- "тендер" = процедура закупівлі через ProZorro API
- "аукціон" = процедура продажу майна через ProZorro.Sale API
- "оголошення" = тендер або аукціон
- "status" = статус тендера/аукціону (active, active.tendering, active.auction, complete, cancelled, unsuccessful)
"""
        
        return base_prompt
    
    # SYSTEM_PROMPT буде формуватися динамічно через _get_system_prompt()

    def __init__(self, settings: Settings):
        """
        Ініціалізація сервісу.
        
        Args:
            settings: Налаштування застосунку
        """
        self.settings = settings
        self.data_dictionary = DataDictionary()
        self.analytics_builder = AnalyticsBuilder()
        self.query_builder = QueryBuilder()
        self.report_generator = ReportGenerator()
        
        # Ініціалізуємо підключення до БД
        MongoDBConnection.initialize(settings)
        
        # Ініціалізуємо LLM провайдер
        self._init_llm_provider()
    
    def _init_llm_provider(self):
        """Ініціалізує LLM провайдер (Gemini)."""
        if not GENAI_AVAILABLE:
            raise ImportError("Для використання Gemini потрібно встановити google-genai: pip install google-genai")
        
        from business.services.llm_service import RateLimiter
        
        api_key = self.settings.llm_api_keys.get('gemini', '')
        if not api_key:
            raise ValueError("Gemini API key не вказано в налаштуваннях")
        
        self.rate_limiter = RateLimiter(self.settings.llm_rate_limit_calls_per_minute)
        self.client = genai.Client(api_key=api_key)
        self.model_name = self.settings.llm_model_name
    
    def get_tools_descriptions(self) -> List[Dict[str, Any]]:
        """
        Повертає описи всіх доступних інструментів для LLM.
        
        Returns:
            Список словників з описом інструментів
        """
        tools = []
        
        # Schema MCP tools
        tools.extend([
            {
                'name': 'get_database_schema',
                'description': 'Отримує повну схему метаданих всіх колекцій бази даних. Використовуй для дослідження структури БД.',
                'parameters': {
                    'type': 'object',
                    'properties': {},
                    'required': []
                }
            },
            {
                'name': 'get_collection_info',
                'description': 'Отримує детальну інформацію про конкретну колекцію (prozorro_auctions або llm_cache).',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'collection_name': {
                            'type': 'string',
                            'enum': ['prozorro_auctions', 'llm_cache'],
                            'description': 'Назва колекції'
                        }
                    },
                    'required': ['collection_name']
                }
            },
            {
                'name': 'get_data_dictionary',
                'description': 'Отримує повний Data Dictionary з описом всіх колекцій та полів.',
                'parameters': {
                    'type': 'object',
                    'properties': {},
                    'required': []
                }
            }
        ])
        
        # Query Builder MCP tools
        tools.extend([
            {
                'name': 'execute_query',
                'description': 'Виконує безпечний запит до MongoDB. Використовуй для отримання конкретних даних. ВАЖЛИВО: $regex оператор ЗАБОРОНЕНИЙ. Для фільтрації за статусом "active" або за регіоном/містом використовуй analytics-mcp замість query-builder-mcp. Для join з llm_cache використовуй формат: {"collection": "prozorro_auctions", "join": [{"collection": "llm_cache", "on": ["description_hash", "description_hash"]}], "filters": {...}}. Поле "on" має бути масивом з рівно 2 елементів: [localField, foreignField].',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'query': {
                            'type': 'object',
                            'description': 'Абстрактний запит у форматі: {"collection": "prozorro_auctions", "filters": {...}, "limit": 10}',
                            'properties': {
                                'collection': {
                                    'type': 'string',
                                    'enum': ['prozorro_auctions', 'llm_cache']
                                },
                                'filters': {
                                    'type': 'object',
                                    'description': 'Фільтри для пошуку'
                                },
                                'join': {
                                    'type': 'array',
                                    'description': 'Список join операцій',
                                    'items': {
                                        'type': 'object',
                                        'description': 'Join операція',
                                        'properties': {
                                            'collection': {
                                                'type': 'string',
                                                'description': 'Назва колекції для join'
                                            },
                                            'on': {
                                                'type': 'array',
                                                'description': 'Поля для з\'єднання у форматі [localField, foreignField]. Має містити рівно 2 елементи. Приклад: ["description_hash", "description_hash"]',
                                                'minItems': 2,
                                                'maxItems': 2,
                                                'items': {
                                                    'type': 'string',
                                                    'description': 'Назва поля'
                                                }
                                            }
                                        }
                                    }
                                },
                                'projection': {
                                    'type': 'array',
                                    'description': 'Список полів для повернення',
                                    'items': {
                                        'type': 'string',
                                        'description': 'Назва поля'
                                    }
                                },
                                'limit': {
                                    'type': 'integer',
                                    'description': 'Максимальна кількість результатів (максимум 100)'
                                },
                                'addFields': {
                                    'type': 'string',
                                    'description': 'Обчислені поля для додавання (MongoDB $addFields format) як JSON-рядок. Використовуй для обчислення метрик. Може містити складні MongoDB оператори ($cond, $ifNull, $reduce, $setUnion тощо). Приклад простого: \'{"bids_count": {"$size": "$auction_data.bids"}}\'. Приклад складного: \'{"participants_count": {"$size": {"$setUnion": {"$reduce": {"input": {"$ifNull": ["$auction_data.bids", []]}, "initialValue": [], "in": {"$concatArrays": ["$$value", {"$map": {"input": {"$ifNull": ["$$this.bidders", []]}, "as": "bidder", "in": {"$ifNull": ["$$bidder.identifier.id", ""]}}}]}}}}}}}\'. ВАЖЛИВО: Передавай як валідний JSON-рядок (string), не об\'єкт!'
                                },
                                'sort': {
                                    'type': 'string',
                                    'description': 'Сортування результатів (MongoDB $sort format) як JSON-рядок. 1 = зростання, -1 = спадання. Приклад: \'{"price_per_m2": 1}\' або \'{"participants_count": -1}\'. ВАЖЛИВО: Передавай як валідний JSON-рядок (string), не об\'єкт!'
                                }
                            },
                            'required': ['collection']
                        }
                    },
                    'required': ['query']
                }
            },
            {
                'name': 'execute_aggregation',
                'description': 'Виконує MongoDB aggregation pipeline з групуванням, розгортанням масивів та агрегаціями. Використовуй для складних запитів, які потребують групування за полями (наприклад, знаходження учасників, що брали участь більше ніж в одному аукціоні). Pipeline - це масив stages (словників). Приклад: {"collection_name": "prozorro_auctions", "pipeline": [{"$unwind": "$auction_data.bids"}, {"$unwind": "$auction_data.bids.bidders"}, {"$group": {"_id": "$auction_data.bids.bidders.identifier.id", "auctions_count": {"$addToSet": "$auction_id"}}}, {"$addFields": {"auctions_count": {"$size": "$auctions_count"}}}, {"$match": {"auctions_count": {"$gt": 1}}}, {"$sort": {"auctions_count": -1}}, {"$limit": 100}], "limit": 100}',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'collection_name': {
                            'type': 'string',
                            'enum': ['prozorro_auctions', 'llm_cache'],
                            'description': 'Назва колекції'
                        },
                        'pipeline': {
                            'type': 'array',
                            'description': 'Список aggregation stages (масив словників). Кожен stage - це словник з одним ключем (назвою stage, наприклад "$group", "$unwind", "$match").',
                            'items': {
                                'type': 'object',
                                'description': 'Aggregation stage'
                            }
                        },
                        'limit': {
                            'type': 'integer',
                            'description': 'Максимальна кількість результатів (опціонально, максимум 100)'
                        }
                    },
                    'required': ['collection_name', 'pipeline']
                }
            },
            {
                'name': 'get_allowed_collections',
                'description': 'Отримує список дозволених колекцій для запитів.',
                'parameters': {
                    'type': 'object',
                    'properties': {},
                    'required': []
                }
            }
        ])
        
        # Analytics MCP tools
        tools.extend([
            {
                'name': 'execute_analytics',
                'description': 'Виконує аналітичний запит з метриками та агрегаціями. Використовуй для отримання статистики та аналітики.',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'query': {
                            'type': 'object',
                            'description': 'Аналітичний запит: {"metric": "average_price_per_m2", "groupBy": ["region"], "filters": {...}}',
                            'properties': {
                                'metric': {
                                    'type': 'string',
                                    'enum': ['average_price_per_m2', 'total_price', 'base_price', 'area', 'building_area', 'land_area', 'count'],
                                    'description': 'Назва метрики'
                                },
                                'groupBy': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'string',
                                        'enum': ['region', 'city', 'property_type', 'status', 'year', 'month', 'quarter']
                                    },
                                    'description': 'Поля для групування'
                                },
                                'filters': {
                                    'type': 'object',
                                    'description': 'Фільтри для пошуку'
                                }
                            },
                            'required': ['metric']
                        }
                    },
                    'required': ['query']
                }
            },
            {
                'name': 'list_metrics',
                'description': 'Отримує список доступних метрик для аналітики.',
                'parameters': {
                    'type': 'object',
                    'properties': {},
                    'required': []
                }
            }
        ])
        
        # Report MCP tools
        tools.extend([
            {
                'name': 'generate_report',
                'description': 'Генерує звіт у вказаному форматі (xlsx, csv, json). Використовуй для створення файлів з даними.',
                'parameters': {
                    'type': 'object',
                    'properties': {
                        'request': {
                            'type': 'object',
                            'description': 'Запит на генерацію звіту',
                            'properties': {
                                'format': {
                                    'type': 'string',
                                    'enum': ['xlsx', 'csv', 'json'],
                                    'description': 'Формат файлу'
                                },
                                'template': {
                                    'type': 'string',
                                    'enum': ['auction_summary', 'price_analysis', 'property_types', 'time_series', 'simple_list'],
                                    'description': 'Назва шаблону (опціонально)'
                                },
                                'dataSource': {
                                    'type': 'string',
                                    'description': 'Джерело даних у форматі: analytics-mcp:metric_name або analytics-mcp:{"metric":"..."}'
                                },
                                'columns': {
                                    'type': 'array',
                                    'items': {
                                        'type': 'string',
                                        'description': 'Назва колонки'
                                    },
                                    'description': 'Список колонок для включення в звіт'
                                }
                            },
                            'required': ['format', 'dataSource', 'columns']
                        },
                        'return_base64': {
                            'type': 'boolean',
                            'description': 'Чи повертати файл у base64 (за замовчуванням: true)',
                            'default': True
                        }
                    },
                    'required': ['request']
                }
            },
            {
                'name': 'list_templates',
                'description': 'Отримує список доступних шаблонів звітів.',
                'parameters': {
                    'type': 'object',
                    'properties': {},
                    'required': []
                }
            }
        ])
        
        return tools
    
    def _call_tool(self, tool_name: str, arguments: Dict[str, Any]) -> Dict[str, Any]:
        """
        Викликає інструмент за назвою.
        
        Args:
            tool_name: Назва інструменту
            arguments: Аргументи для інструменту
            
        Returns:
            Результат виконання інструменту
        """
        logger.debug(f"_call_tool: {tool_name} з аргументами: {json.dumps(arguments, indent=2, ensure_ascii=False, default=str)}")
        try:
            # Schema MCP tools
            if tool_name == 'get_database_schema':
                schema = self.data_dictionary.to_schema_dict()
                schema['generated_at'] = format_datetime_display(datetime.now(timezone.utc), '%Y-%m-%dT%H:%M:%S')
                return {'success': True, 'schema': schema}
            
            elif tool_name == 'get_collection_info':
                collection_name = arguments.get('collection_name')
                collection = self.data_dictionary.get_collection(collection_name)
                if collection:
                    return {
                        'success': True,
                        'collection': {
                            'collection_name': collection.mongo_collection,
                            'description': collection.description,
                            'schema': self.data_dictionary.to_schema_dict()['collections'][collection_name]['schema'],
                            'indexes': collection.indexes,
                            'relationships': collection.relationships
                        }
                    }
                return {'success': False, 'error': f'Колекція {collection_name} не знайдена'}
            
            elif tool_name == 'get_data_dictionary':
                return {
                    'success': True,
                    'data_dictionary': self.data_dictionary.to_schema_dict(),
                    'metadata': self.data_dictionary.get_metadata()
                }
            
            # Query Builder MCP tools
            elif tool_name == 'execute_aggregation':
                collection_name = arguments.get('collection_name')
                pipeline = arguments.get('pipeline', [])
                limit = arguments.get('limit', None)
                logger.debug(f"Отримано aggregation запит: collection={collection_name}, pipeline stages={len(pipeline)}")
                result = self.query_builder.execute_aggregation(
                    collection_name=collection_name,
                    pipeline=pipeline,
                    limit=limit
                )
                return result
            
            elif tool_name == 'execute_query':
                import json as json_module  # Використовуємо явний імпорт для уникнення конфліктів
                query = arguments.get('query', {})
                logger.debug(f"Отримано запит execute_query: {query}")
                
                # Парсимо addFields якщо він переданий як string
                if 'addFields' in query and isinstance(query['addFields'], str):
                    try:
                        add_fields_str = query['addFields'].strip()
                        logger.debug(f"Парсю addFields як JSON string (довжина: {len(add_fields_str)}): {add_fields_str[:200]}...")
                        
                        # Замінюємо Python None на JSON null перед парсингом
                        # Використовуємо regex для безпечної заміни (тільки якщо це не частина рядка)
                        # Замінюємо None (як окреме слово) на null
                        add_fields_str = re.sub(r'\bNone\b', 'null', add_fields_str)
                        
                        # Спробуємо знайти перший валідний JSON об'єкт (на випадок, якщо є зайві символи)
                        if add_fields_str.startswith('{'):
                            # Знаходимо закриваючу дужку для першого об'єкта
                            brace_count = 0
                            end_pos = -1
                            for i, char in enumerate(add_fields_str):
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        end_pos = i + 1
                                        break
                            if end_pos > 0:
                                add_fields_str = add_fields_str[:end_pos]
                                logger.debug(f"Обрізано addFields до позиції {end_pos}: {add_fields_str[:200]}...")
                        query['addFields'] = json_module.loads(add_fields_str)
                        logger.debug(f"addFields успішно розпарсено: {type(query['addFields'])}")
                    except (json_module.JSONDecodeError, ValueError) as e:
                        logger.error(f"Помилка парсингу addFields JSON: {e}. Рядок: {query['addFields'][:500]}")
                        return {
                            'success': False,
                            'error': f'Помилка парсингу addFields: {str(e)}. Перевірте формат JSON. Переконайтеся, що використовується null замість None.'
                        }
                
                # Парсимо sort якщо він переданий як string
                if 'sort' in query and isinstance(query['sort'], str):
                    try:
                        sort_str = query['sort'].strip()
                        logger.debug(f"Парсю sort як JSON string: {sort_str}")
                        
                        # Замінюємо Python None на JSON null перед парсингом
                        sort_str = re.sub(r'\bNone\b', 'null', sort_str)
                        
                        # Спробуємо знайти перший валідний JSON об'єкт
                        if sort_str.startswith('{'):
                            brace_count = 0
                            end_pos = -1
                            for i, char in enumerate(sort_str):
                                if char == '{':
                                    brace_count += 1
                                elif char == '}':
                                    brace_count -= 1
                                    if brace_count == 0:
                                        end_pos = i + 1
                                        break
                            if end_pos > 0:
                                sort_str = sort_str[:end_pos]
                                logger.debug(f"Обрізано sort до позиції {end_pos}: {sort_str}")
                        query['sort'] = json_module.loads(sort_str)
                        logger.debug(f"sort успішно розпарсено: {type(query['sort'])}")
                    except (json_module.JSONDecodeError, ValueError) as e:
                        logger.error(f"Помилка парсингу sort JSON: {e}. Рядок: {query['sort']}")
                        return {
                            'success': False,
                            'error': f'Помилка парсингу sort: {str(e)}. Перевірте формат JSON. Переконайтеся, що використовується null замість None.'
                        }
                
                logger.debug(f"Виконую запит з query: {query}")
                result = self.query_builder.execute_query(query)
                return result
            
            elif tool_name == 'get_allowed_collections':
                return {
                    'success': True,
                    'collections': list(self.query_builder.ALLOWED_COLLECTIONS),
                    'max_results': self.query_builder.MAX_RESULTS
                }
            
            # Analytics MCP tools
            elif tool_name == 'execute_analytics':
                query = arguments.get('query', {})
                result = self.analytics_builder.execute_analytics_query(query)
                return result
            
            elif tool_name == 'list_metrics':
                from utils.analytics_metrics import AnalyticsMetrics
                metrics = AnalyticsMetrics.list_metrics()
                # Обмежуємо кількість метрик для безпеки (якщо їх занадто багато)
                # Повертаємо у форматі, зручному для LLM
                return {
                    'success': True,
                    'metrics': metrics[:20] if len(metrics) > 20 else metrics,  # Обмежуємо до 20 метрик
                    'total_count': len(metrics)
                }
            
            # Report MCP tools
            elif tool_name == 'generate_report':
                request = arguments.get('request', {})
                return_base64 = arguments.get('return_base64', True)
                result = self.report_generator.generate_report(request, return_base64=return_base64)
                return result
            
            elif tool_name == 'list_templates':
                from utils.report_templates import ReportTemplates
                templates = ReportTemplates.list_templates()
                return {'success': True, 'templates': templates}
            
            else:
                logger.error(f"Невідомий інструмент: {tool_name}")
                return {'success': False, 'error': f'Невідомий інструмент: {tool_name}'}
        
        except Exception as e:
            logger.exception(f"Помилка виконання інструменту {tool_name}: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return {'success': False, 'error': f'Помилка виконання інструменту {tool_name}: {str(e)}'}
    
    def process_query(
        self,
        user_query: str,
        stream_callback: Optional[Callable[[str], None]] = None
    ) -> str:
        """
        Обробляє запит користувача з використанням LLM та інструментів.
        
        Args:
            user_query: Запит користувача
            stream_callback: Функція для трансляції проміжних результатів
            
        Returns:
            Відповідь LLM
        """
        logger.info("="*80)
        logger.info(f"ПОЧАТОК ОБРОБКИ ЗАПИТУ")
        logger.info(f"Запит користувача: {user_query}")
        logger.info("="*80)
        
        self.rate_limiter.wait_if_needed()
        
        # Отримуємо описи інструментів
        tools = self.get_tools_descriptions()
        
        # Конвертуємо в формат Gemini
        function_declarations = []
        for tool in tools:
            function_declarations.append(
                types.FunctionDeclaration(
                    name=tool['name'],
                    description=tool['description'],
                    parameters=types.Schema(
                        type=types.Type.OBJECT,
                        properties={
                            k: self._convert_schema_property(v)
                            for k, v in tool['parameters'].get('properties', {}).items()
                        },
                        required=tool['parameters'].get('required', [])
                    )
                )
            )
        
        # Створюємо Tool
        tool = types.Tool(function_declarations=function_declarations)
        
        # Конфігурація
        temperature = getattr(self.settings, 'llm_agent_temperature', 0.7)
        max_output_tokens = getattr(self.settings, 'llm_agent_max_output_tokens', 8192)
        config = types.GenerateContentConfig(
            tools=[tool],
            temperature=temperature,
            max_output_tokens=max_output_tokens
        )
        
        try:
            # Створюємо повідомлення з системним промптом
            # Використовуємо метод для отримання актуального промпту (з глосарієм)
            system_prompt = self._get_system_prompt()
            logger.debug(f"Системний промпт (довжина: {len(system_prompt)} символів)")
            logger.debug(f"Перші 500 символів промпту: {system_prompt[:500]}...")
            
            # Додаємо поточну дату та час до контексту (київський час)
            now_utc = datetime.now(timezone.utc)
            now_kyiv = to_kyiv(now_utc)
            current_date_time = format_datetime_display(now_utc, "%Y-%m-%d %H:%M:%S")
            current_date = format_datetime_display(now_utc, "%Y-%m-%d")
            current_time = format_datetime_display(now_utc, "%H:%M:%S")
            weekday_names = ['Понеділок', 'Вівторок', 'Середа', 'Четвер', 'П\'ятниця', 'Субота', 'Неділя']
            weekday_name = weekday_names[now_kyiv.weekday()]
            
            # Обчислюємо діапазони для зручності (UTC для запитів)
            last_24h = (now_utc - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S")
            last_7d = (now_utc - timedelta(days=7)).strftime("%Y-%m-%dT%H:%M:%S")
            last_30d = (now_utc - timedelta(days=30)).strftime("%Y-%m-%dT%H:%M:%S")
            
            context_info = f"""
## Контекст поточної дати та часу (Київ):
- Поточна дата та час: {current_date_time}
- Поточна дата: {current_date}
- Поточний час: {current_time}
- День тижня: {weekday_name}

## Корисні діапазони дат (UTC для запитів):
- За останню добу (24 години): від {last_24h} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}
- За останній тиждень (7 днів): від {last_7d} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}
- За останній місяць (30 днів): від {last_30d} до {now_utc.strftime("%Y-%m-%dT%H:%M:%S")}

Використовуй цю інформацію для обчислення діапазонів дат у запитах типу "за останню добу", "за останній тиждень" тощо.
"""
            
            full_prompt = f"{system_prompt}\n\n{context_info}\n\nКористувач запитує: {user_query}"
            logger.debug(f"Повний промпт (довжина: {len(full_prompt)} символів)")
            logger.debug(f"Контекст дати/часу: {context_info.strip()}")
            
            # Виконуємо запит
            logger.info("Відправляю запит до Gemini API...")
            response = self.client.models.generate_content(
                model=self.model_name,
                contents=full_prompt,
                config=config
            )
            logger.info(f"Отримано відповідь від Gemini API. Кількість кандидатів: {len(response.candidates) if response.candidates else 0}")
            
            # Детальне логування структури відповіді
            logger.debug(f"Тип response: {type(response)}")
            logger.debug(f"Атрибути response: {dir(response)}")
            if hasattr(response, 'prompt_feedback'):
                logger.debug(f"prompt_feedback: {response.prompt_feedback}")
                if response.prompt_feedback:
                    logger.debug(f"prompt_feedback type: {type(response.prompt_feedback)}")
                    logger.debug(f"prompt_feedback attributes: {dir(response.prompt_feedback)}")
                    if hasattr(response.prompt_feedback, 'block_reason'):
                        logger.warning(f"Block reason: {response.prompt_feedback.block_reason}")
                    if hasattr(response.prompt_feedback, 'block_reason_message'):
                        logger.warning(f"Block reason message: {response.prompt_feedback.block_reason_message}")
            
            # Обробляємо відповідь
            if not response.candidates:
                # Перевіряємо feedback
                feedback_info = ""
                if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                    feedback_info = f" Feedback: {response.prompt_feedback}"
                    if hasattr(response.prompt_feedback, 'block_reason'):
                        feedback_info += f", Block reason: {response.prompt_feedback.block_reason}"
                    if hasattr(response.prompt_feedback, 'block_reason_message'):
                        feedback_info += f", Block message: {response.prompt_feedback.block_reason_message}"
                logger.error(f"Немає кандидатів у відповіді. Feedback: {feedback_info}")
                return f"Не вдалося отримати відповідь від LLM.{feedback_info}"
            
            candidate = response.candidates[0]
            logger.info(f"Тип candidate: {type(candidate)}")
            logger.info(f"Атрибути candidate: {[attr for attr in dir(candidate) if not attr.startswith('_')]}")
            
            # Перевіряємо finish_reason та safety_ratings
            if hasattr(candidate, 'finish_reason'):
                logger.warning(f"Finish reason: {candidate.finish_reason}")
            if hasattr(candidate, 'safety_ratings') and candidate.safety_ratings:
                logger.warning(f"Safety ratings: {candidate.safety_ratings}")
                for rating in candidate.safety_ratings:
                    if hasattr(rating, 'category') and hasattr(rating, 'probability'):
                        logger.warning(f"  - {rating.category}: {rating.probability}")
            
            logger.debug(f"candidate.content: {candidate.content if hasattr(candidate, 'content') else 'N/A'}")
            if hasattr(candidate, 'content') and candidate.content:
                logger.debug(f"candidate.content.parts: {candidate.content.parts if hasattr(candidate.content, 'parts') else 'N/A'}")
            
            if not hasattr(candidate, 'content') or not candidate.content:
                logger.error("Кандидат не має атрибута 'content'")
                # Перевіряємо, чи є feedback про помилку
                feedback_info = ""
                if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                    feedback_info = f" Feedback: {response.prompt_feedback}"
                    if hasattr(response.prompt_feedback, 'block_reason'):
                        feedback_info += f", Block reason: {response.prompt_feedback.block_reason}"
                    if hasattr(response.prompt_feedback, 'block_reason_message'):
                        feedback_info += f", Block message: {response.prompt_feedback.block_reason_message}"
                if hasattr(candidate, 'finish_reason'):
                    finish_reason = candidate.finish_reason
                    feedback_info += f", Finish reason: {finish_reason}"
                    # Спеціальна обробка для MALFORMED_FUNCTION_CALL
                    if str(finish_reason) == "FinishReason.MALFORMED_FUNCTION_CALL":
                        feedback_info += " (Помилка формату виклику функції - можливо, складний addFields або sort)"
                        logger.error("MALFORMED_FUNCTION_CALL - LLM не може правильно сформувати виклик функції зі складними addFields/sort")
                if hasattr(candidate, 'safety_ratings') and candidate.safety_ratings:
                    feedback_info += f", Safety ratings: {candidate.safety_ratings}"
                logger.error(f"Кандидат не має контенту. Feedback: {feedback_info}")
                if feedback_info:
                    return f"Відповідь порожня.{feedback_info}"
                return "Відповідь порожня."
            
            if not hasattr(candidate.content, 'parts') or not candidate.content.parts:
                logger.error("Кандидат не має parts")
                # Перевіряємо, чи є feedback про помилку
                feedback_info = ""
                if hasattr(response, 'prompt_feedback') and response.prompt_feedback:
                    feedback_info = f" Feedback: {response.prompt_feedback}"
                    if hasattr(response.prompt_feedback, 'block_reason'):
                        feedback_info += f", Block reason: {response.prompt_feedback.block_reason}"
                    if hasattr(response.prompt_feedback, 'block_reason_message'):
                        feedback_info += f", Block message: {response.prompt_feedback.block_reason_message}"
                if hasattr(candidate, 'finish_reason'):
                    feedback_info += f", Finish reason: {candidate.finish_reason}"
                if hasattr(candidate, 'safety_ratings') and candidate.safety_ratings:
                    feedback_info += f", Safety ratings: {candidate.safety_ratings}"
                logger.error(f"Кандидат не має parts. Feedback: {feedback_info}")
                if feedback_info:
                    return f"Відповідь порожня.{feedback_info}"
                return "Відповідь порожня."
            
            # Перевіряємо, чи є виклики функцій
            function_calls = []
            text_parts = []
            
            logger.debug(f"Аналізую parts кандидата. Кількість parts: {len(candidate.content.parts)}")
            for i, part in enumerate(candidate.content.parts):
                if hasattr(part, 'function_call') and part.function_call:
                    logger.info(f"Part {i}: знайдено function_call - {part.function_call.name if hasattr(part.function_call, 'name') else 'unknown'}")
                    function_calls.append(part.function_call)
                elif hasattr(part, 'text') and part.text:
                    logger.info(f"Part {i}: знайдено text (довжина: {len(part.text)} символів)")
                    logger.debug(f"Part {i} text: {part.text[:200]}...")
                    text_parts.append(part.text)
                else:
                    logger.warning(f"Part {i}: невідомий тип - {type(part)}")
            
            logger.info(f"Знайдено function_calls: {len(function_calls)}, text_parts: {len(text_parts)}")
            
            if function_calls:
                logger.info(f"Починаю обробку {len(function_calls)} викликів функцій")
                # Виконуємо функції та продовжуємо діалог
                
                # Створюємо conversation history
                conversation_parts = [
                    types.Content(
                        role='user',
                        parts=[types.Part(text=full_prompt)]
                    )
                ]
                
                # Цикл для обробки кількох викликів функцій
                max_iterations = getattr(self.settings, 'llm_agent_max_iterations', 5)
                iteration = 0
                
                while iteration < max_iterations:
                    iteration += 1
                    logger.info(f"--- Ітерація {iteration}/{max_iterations} ---")
                    
                    # Отримуємо поточну відповідь (перша ітерація - з першого запиту, далі - з наступних)
                    if iteration == 1:
                        current_response = response
                    else:
                        # Виконуємо запит з поточною conversation history
                        current_response = self.client.models.generate_content(
                            model=self.model_name,
                            contents=conversation_parts,
                            config=config
                        )
                    
                    if not current_response.candidates or not current_response.candidates[0].content:
                        break
                    
                    # Перевіряємо, чи є виклики функцій
                    function_calls = []
                    text_parts = []
                    
                    for part in current_response.candidates[0].content.parts:
                        if hasattr(part, 'function_call') and part.function_call:
                            function_calls.append(part.function_call)
                        elif hasattr(part, 'text') and part.text:
                            text_parts.append(part.text)
                    
                    logger.debug(f"Після обробки: function_calls={len(function_calls)}, text_parts={len(text_parts)}")
                    
                    # Якщо є текстова відповідь без викликів функцій - повертаємо її
                    if text_parts and not function_calls:
                        response_text = " ".join(text_parts)
                        logger.info(f"Знайдено текстову відповідь без викликів функцій (довжина: {len(response_text)} символів)")
                        # Перевіряємо, чи відповідь не порожня
                        if response_text.strip():
                            logger.info("Повертаю фінальну відповідь")
                            logger.debug(f"Фінальна відповідь: {response_text[:500]}...")
                            return response_text
                        # Якщо відповідь порожня, продовжуємо обробку
                        logger.warning("Текстова відповідь порожня, продовжую обробку")
                    
                    # Якщо немає викликів функцій та немає тексту - виходимо з циклу
                    if not function_calls and not text_parts:
                        logger.warning("Немає викликів функцій та немає тексту - виходжу з циклу")
                        break
                    
                    # Якщо немає викликів функцій, але є текст - перевіряємо, чи текст не порожній
                    if not function_calls:
                        if text_parts:
                            response_text = " ".join(text_parts)
                            logger.info(f"Знайдено текст без викликів функцій (довжина: {len(response_text)} символів)")
                            if response_text.strip():
                                logger.info("Повертаю фінальну відповідь")
                                logger.debug(f"Фінальна відповідь: {response_text[:500]}...")
                                return response_text
                        # Якщо текст порожній, продовжуємо до наступної ітерації
                        logger.warning("Текст порожній, продовжую до наступної ітерації")
                        continue
                    
                    # Додаємо відповідь моделі до conversation
                    conversation_parts.append(current_response.candidates[0].content)
                    
                    # Виконуємо функції
                    function_responses = []
                    
                    for func_call in function_calls:
                        func_name = func_call.name
                        # Отримуємо аргументи функції
                        if hasattr(func_call, 'args'):
                            if isinstance(func_call.args, dict):
                                func_args = func_call.args
                            elif isinstance(func_call.args, str):
                                func_args = json.loads(func_call.args)
                            else:
                                func_args = {}
                        else:
                            func_args = {}
                        
                        logger.info(f"Виклик інструменту: {func_name}")
                        logger.debug(f"Аргументи інструменту {func_name}: {json.dumps(func_args, indent=2, ensure_ascii=False, default=str)}")
                        
                        # Викликаємо інструмент
                        result = self._call_tool(func_name, func_args)
                        
                        logger.info(f"Результат інструменту {func_name}: success={result.get('success', False)}")
                        if result.get('success'):
                            logger.debug(f"Успішний результат {func_name}: {json.dumps(result, indent=2, ensure_ascii=False, default=str)[:500]}...")
                        else:
                            logger.error(f"Помилка інструменту {func_name}: {result.get('error', 'Невідома помилка')}")
                        
                        # Додаємо відповідь функції
                        # FunctionResponse.response очікує словник (dict), а не JSON-рядок
                        # Обмежуємо розмір відповіді для безпеки
                        response_data = result
                        if isinstance(result, dict) and 'metrics' in result and isinstance(result['metrics'], list):
                            # Якщо це список метрик, обмежуємо його розмір
                            if len(result['metrics']) > 20:
                                response_data = {
                                    **result,
                                    'metrics': result['metrics'][:20],
                                    '_truncated': True,
                                    '_total_count': len(result['metrics'])
                                }
                        
                        function_responses.append(
                            types.Part(function_response=types.FunctionResponse(
                                name=func_name,
                                response=response_data
                            ))
                        )
                    
                    # Додаємо результати функцій до conversation
                    if function_responses:
                        conversation_parts.append(
                            types.Content(
                                role='function',
                                parts=function_responses
                            )
                        )
                    
                    # Продовжуємо цикл для отримання наступної відповіді від моделі
                    continue
                
                # Після завершення циклу отримуємо фінальну відповідь
                
                try:
                    logger.info("Отримую фінальну відповідь від LLM...")
                    # Отримуємо фінальну відповідь
                    final_response = self.client.models.generate_content(
                        model=self.model_name,
                        contents=conversation_parts,
                        config=types.GenerateContentConfig(
                            tools=[tool],  # Передаємо tools для можливості додаткових викликів
                            temperature=temperature,
                            max_output_tokens=max_output_tokens
                        )
                    )
                    
                    logger.info(f"Отримано фінальну відповідь. Кількість кандидатів: {len(final_response.candidates) if final_response.candidates else 0}")
                    
                    if final_response.candidates and final_response.candidates[0].content:
                        final_text = ""
                        for part in final_response.candidates[0].content.parts:
                            if hasattr(part, 'text') and part.text:
                                final_text += str(part.text)
                            # Перевіряємо, чи є додаткові виклики функцій
                            if hasattr(part, 'function_call') and part.function_call:
                                logger.warning(f"Модель намагається викликати функцію {part.function_call.name} повторно")
                        
                        logger.info(f"Фінальний текст (довжина: {len(final_text)} символів): {final_text[:500]}...")
                        
                        # Перевіряємо feedback
                        if hasattr(final_response, 'prompt_feedback') and final_response.prompt_feedback:
                            feedback_info = f"Feedback: {final_response.prompt_feedback}"
                            if hasattr(final_response.prompt_feedback, 'block_reason'):
                                feedback_info += f", Block reason: {final_response.prompt_feedback.block_reason}"
                            logger.warning(f"Prompt feedback: {feedback_info}")
                        
                        if final_text and final_text.strip():
                            logger.info("Повертаю фінальну відповідь")
                            return final_text
                        else:
                            # Формуємо детальне повідомлення
                            logger.error("Фінальний текст порожній")
                            error_msg = "Отримано результати від інструментів, але не вдалося сформувати текстову відповідь."
                            if hasattr(final_response, 'prompt_feedback') and final_response.prompt_feedback:
                                if hasattr(final_response.prompt_feedback, 'block_reason'):
                                    error_msg += f" Feedback: {final_response.prompt_feedback.block_reason}"
                            error_msg += " Перевірте, чи дані відповідають критеріям пошуку."
                            return error_msg
                    else:
                        logger.error("Немає кандидатів у фінальній відповіді")
                        error_msg = "Не вдалося отримати відповідь від LLM після виконання функцій."
                        if hasattr(final_response, 'prompt_feedback') and final_response.prompt_feedback:
                            if hasattr(final_response.prompt_feedback, 'block_reason'):
                                error_msg += f" Feedback: {final_response.prompt_feedback.block_reason}"
                        error_msg += " Можливо, дані не знайдено або запит потребує уточнення."
                        return error_msg
                
                except Exception as e:
                    logger.exception(f"Помилка при формуванні відповіді: {str(e)}")
                    error_msg = f"Помилка при формуванні відповіді: {str(e)}"
                    return error_msg
            else:
                # Звичайна текстова відповідь
                response_text = " ".join(text_parts) if text_parts else "Не вдалося отримати відповідь від LLM."
                logger.info(f"Повертаю звичайну текстову відповідь (довжина: {len(response_text)} символів)")
                return response_text
        
        except Exception as e:
            logger.exception(f"Помилка обробки запиту: {str(e)}")
            import traceback
            logger.error(f"Traceback: {traceback.format_exc()}")
            return f"Помилка обробки запиту: {str(e)}"
        finally:
            logger.info("="*80)
            logger.info("ЗАВЕРШЕНО ОБРОБКУ ЗАПИТУ")
            logger.info("="*80)
    
    def _convert_schema_property(self, prop: Dict[str, Any]) -> types.Schema:
        """
        Конвертує властивість схеми в формат Gemini.
        
        Args:
            prop: Властивість схеми
            
        Returns:
            Schema об'єкт Gemini
        """
        prop_type = prop.get('type', 'string')
        
        # Конвертуємо тип
        type_mapping = {
            'string': types.Type.STRING,
            'integer': types.Type.INTEGER,
            'number': types.Type.NUMBER,
            'boolean': types.Type.BOOLEAN,
            'array': types.Type.ARRAY,
            'object': types.Type.OBJECT
        }
        
        schema_type = type_mapping.get(prop_type, types.Type.STRING)
        
        # Створюємо схему
        schema = types.Schema(type=schema_type)
        
        # Додаємо опис
        if 'description' in prop:
            schema.description = prop['description']
        
        # Обробляємо enum
        if 'enum' in prop:
            schema.enum = prop['enum']
        
        # Обробляємо вкладені властивості для object
        if prop_type == 'object':
            if 'properties' in prop:
                schema.properties = {
                    k: self._convert_schema_property(v)
                    for k, v in prop['properties'].items()
                }
            # Якщо additionalProperties вказано, дозволяємо додаткові властивості
            # У Gemini це реалізується через відсутність обмежень на properties
            # Але для складних об'єктів (як addFields) ми просто не вказуємо properties
        
        # Обробляємо items для array
        if prop_type == 'array' and 'items' in prop:
            schema.items = self._convert_schema_property(prop['items'])
        
        return schema
