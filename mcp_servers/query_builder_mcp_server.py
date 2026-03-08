# -*- coding: utf-8 -*-
"""
MCP сервер для безпечного виконання запитів до MongoDB через абстрактний API.
Підтримує створення тимчасової вибірки (save_query_to_temp_collection) для подальшого експорту в файл.
"""

import json
import sys
import uuid
from typing import Any, Dict, List, Optional
from mcp.server.fastmcp import FastMCP
from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.agent_temp_exports_repository import AgentTempExportsRepository
from utils.query_builder import QueryBuilder

# Ініціалізуємо MCP сервер
mcp = FastMCP("query-builder-mcp", json_response=True)

# Глобальні змінні
_query_builder: Optional[QueryBuilder] = None
_temp_exports_repo: Optional[AgentTempExportsRepository] = None


def initialize_connection():
    """Ініціалізує підключення до бази даних та репозиторії."""
    global _query_builder, _temp_exports_repo
    if _query_builder is None:
        try:
            settings = Settings()
            MongoDBConnection.initialize(settings)
            _query_builder = QueryBuilder()
            _temp_exports_repo = AgentTempExportsRepository()
        except Exception as e:
            print(f"Помилка ініціалізації підключення до БД: {e}", file=sys.stderr)
            raise


@mcp.tool()
def execute_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Виконує безпечний запит до MongoDB через абстрактний API.
    
    Приймає абстрактний запит у форматі:
    {
        "collection": "prozorro_auctions",
        "filters": { "auction_data.dateModified": { "$gte": "...", "$lte": "..." } },
        "projection": ["auction_id", "auction_data.dateModified"],
        "limit": 100
    }
    Для експорту «усіх оголошень за період» завжди передавай limit: 100 (максимум). За замовчуванням використовується 100.
    
    Args:
        query: Абстрактний запит у форматі JSON
        
    Returns:
        Словник з результатами виконання запиту
    """
    global _query_builder
    
    try:
        initialize_connection()
        
        # Якщо query передано як рядок, парсимо його
        if isinstance(query, str):
            query = json.loads(query)
        
        # Виконуємо запит
        result = _query_builder.execute_query(query)
        
        return result
    except json.JSONDecodeError as e:
        return {
            'success': False,
            'error': f'Помилка парсингу JSON: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Помилка виконання запиту: {str(e)}'
        }


@mcp.tool()
def save_query_to_temp_collection(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Виконує запит та зберігає результати в тимчасову вибірку. Повертає temp_collection_id.
    Далі викликай export_from_temp_collection(temp_collection_id) для експорту в файл.
    Результати запиту не повертаються в контекст — тільки ідентифікатор вибірки.
    Підтримуються колекції: prozorro_auctions, olx_listings (limit до 100).
    """
    global _query_builder, _temp_exports_repo
    try:
        initialize_connection()
        if isinstance(query, str):
            query = json.loads(query)
        # Нормалізація: можливий вкладений "query"; "filter" → "filters" (QueryBuilder очікує верхньорівневі collection, filters)
        query = dict(query) if isinstance(query, dict) else {}
        inner = query.get("query", query)
        if isinstance(inner, dict):
            inner = dict(inner)
            if "filter" in inner and "filters" not in inner:
                inner["filters"] = inner.pop("filter", {})
            query = {k: v for k, v in inner.items() if k != "query"}
        if isinstance(query, dict) and "filter" in query and "filters" not in query:
            query["filters"] = query.pop("filter", {})
        result = _query_builder.execute_query(query)
        if not result.get("success"):
            return result
        results = result.get("results") or []
        collection_name = (query or {}).get("collection") or ""
        if collection_name not in ("prozorro_auctions", "olx_listings", "unified_listings"):
            return {
                "success": False,
                "error": "Тимчасову вибірку підтримано лише для prozorro_auctions, olx_listings та unified_listings.",
            }
        batch_id = str(uuid.uuid4())
        count = _temp_exports_repo.insert_batch(batch_id, collection_name, results)
        return {
            "success": True,
            "temp_collection_id": batch_id,
            "count": count,
            "source_collection": collection_name,
        }
    except json.JSONDecodeError as e:
        return {"success": False, "error": f"Помилка парсингу JSON: {str(e)}"}
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def validate_query(query: Dict[str, Any]) -> Dict[str, Any]:
    """
    Валідує абстрактний запит без його виконання.
    
    Args:
        query: Абстрактний запит у форматі JSON
        
    Returns:
        Словник з результатом валідації
    """
    global _query_builder
    
    try:
        initialize_connection()
        
        # Якщо query передано як рядок, парсимо його
        if isinstance(query, str):
            query = json.loads(query)
        
        # Валідуємо запит
        is_valid, error = _query_builder.validate_query(query)
        
        if is_valid:
            return {
                'success': True,
                'valid': True,
                'message': 'Запит валідний'
            }
        else:
            return {
                'success': True,
                'valid': False,
                'error': error
            }
    except json.JSONDecodeError as e:
        return {
            'success': False,
            'error': f'Помилка парсингу JSON: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Помилка валідації: {str(e)}'
        }


# Короткі описи колекцій для агента (повна схема — get_collection_info у schema-mcp)
_COLLECTION_DESCRIPTIONS = {
    "prozorro_auctions": (
        "Аукціони ProZorro.Sale. Поля: auction_id, auction_data (dateModified, dateCreated — рядки ISO 8601; "
        "value, status, items, bids — масив заявок; price_metrics.total_price_uah / total_price_usd / "
        "price_per_m2_*, price_per_ha_* — цінові метрики в UAH/USD). ВАЖЛИВО: auction_data.numberOfBids не існує; "
        "кількість учасників обчислюй через $addFields: {\"bids_count\": {\"$size\": {\"$ifNull\": "
        "[\"$auction_data.bids\", []]}}}, потім $sort по bids_count (-1). Для фільтра за датою: "
        "auction_data.dateModified з $gte/$lte (рядки)."
    ),
    "olx_listings": (
        "Оголошення OLX (нежитлова нерухомість, земля). Поля: url (ід для експорту), search_data "
        "(title, price, location, area_m2), detail (description, llm, resolved_locations, parameters, "
        "address_refs — масив {region: {_id, name}, city: {_id, name}}), updated_at (BSON Date). "
        "У execute_aggregation у $match: updated_at приймає ISO-рядки (сервер перетворює на дату); "
        "фільтр за регіоном/містом — detail.address_refs з $elemMatch: region.name/city.name (напр. «Київ», «Київська») "
        "або region._id/city._id. «Київ та область» = $or: [{\"detail.address_refs\": {\"$elemMatch\": {\"city.name\": \"Київ\"}}}, "
        "{\"detail.address_refs\": {\"$elemMatch\": {\"region.name\": \"Київська\"}}}]. Повна структура — get_collection_info('olx_listings')."
    ),
    "llm_cache": "Кеш результатів LLM-парсингу описів; description_hash, result (property_type, addresses, тощо).",
    "unified_listings": (
        "Зведена таблиця оголошень, що об'єднує дані з OLX та ProZorro в єдину структуру. "
        "Поля: source (olx/prozorro), source_id, status (активне/неактивне), property_type "
        "(Земельна ділянка/Комерційна нерухомість/Земельна ділянка з нерухомістю/інше), "
        "title, description, addresses (масив нормалізованих адрес з координатами та is_complete), "
        "cadastral_numbers, price_uah, price_usd, price_per_m2_uah/usd, price_per_ha_uah/usd, "
        "currency_rate, source_updated_at, system_updated_at. "
        "Фільтр за регіоном/містом: addresses з $elemMatch: {\"region\": \"...\"} або {\"settlement\": \"...\"}. "
        "Це основна колекція для пошуку та аналітики — використовуй її замість olx_listings/prozorro_auctions, "
        "якщо потрібні уніфіковані дані."
    ),
    "listing_analytics": (
        "LLM-згенерована аналітика оголошень. Поля: source (olx/prozorro), source_id (url або auction_id), "
        "analysis_text (3 блоки: ціна за одиницю, місцезнаходження, оточення), analysis_at, updated_at. "
        "Зв'язок з unified_listings через source+source_id. Використовуй для запитів про «аналітику оголошень»."
    ),
    "real_estate_objects": (
        "Об'єкти нерухомого майна (ОНМ). Поля: type (land_plot, building, premises), area_sqm, "
        "cadastral_info (для land_plot: cadastral_number, purpose), address (для building: region, settlement, street), "
        "source_listing_ids (зв'язок з оголошеннями). unified_listings.real_estate_refs[].object_id посилається на _id. "
        "Використовуй для запитів про «об'єкти нерухомості», «кадастрові ділянки», «будівлі»."
    ),
    "price_analytics": (
        "Зведена аналітика цін: агреговані метрики (avg, std, q1–q4) за періодами та індикатори (квартилі по містах). "
        "Поля: period_type, period_key, group_region, group_city, metric, q1, q2, q3, q4."
    ),
}


@mcp.tool()
def get_allowed_collections() -> Dict[str, Any]:
    """
    Повертає список дозволених колекцій для запитів та короткий опис полів.
    Для повної схеми та опису полів використовуй get_collection_info(collection_name) з schema-mcp.
    """
    collections = [
        {"id": c, "description": _COLLECTION_DESCRIPTIONS.get(c, "")}
        for c in sorted(QueryBuilder.ALLOWED_COLLECTIONS)
    ]
    return {
        "success": True,
        "collections": collections,
        "max_results": QueryBuilder.MAX_RESULTS,
    }


@mcp.tool()
def get_distinct_values(
    collection_name: str,
    field_path: str,
    limit: int = 300,
    unwrap_array: bool = False,
) -> Dict[str, Any]:
    """
    Повертає унікальні значення поля в колекції. Використовуй перед фільтрацією за регіоном/локацією,
    щоб побачити які значення реально є в даних (напр. «Київська область», «Київська обл.»), і побудувати $in/$or.
    Для полів-масивів (напр. detail.llm.tags — теги оголошень) передай unwrap_array=True, щоб отримати список унікальних елементів.
    Параметри: collection_name (olx_listings, prozorro_auctions, llm_cache), field_path (напр. search_data.location або detail.llm.tags), unwrap_array (для масивів).
    """
    global _query_builder
    try:
        initialize_connection()
        return _query_builder.get_distinct_values(
            collection_name=collection_name,
            field_path=field_path,
            limit=limit,
            unwrap_array=unwrap_array,
        )
    except Exception as e:
        return {"success": False, "error": str(e), "values": []}


@mcp.tool()
def get_allowed_operators() -> Dict[str, Any]:
    """
    Повертає список дозволених операторів для фільтрів.
    
    Returns:
        Словник зі списком дозволених операторів
    """
    return {
        'success': True,
        'allowed_operators': sorted(list(QueryBuilder.ALLOWED_OPERATORS)),
        'forbidden_operators': sorted(list(QueryBuilder.FORBIDDEN_OPERATORS))
    }


@mcp.tool()
def execute_aggregation(
    collection_name: str,
    pipeline: List[Dict[str, Any]],
    limit: Optional[int] = None
) -> Dict[str, Any]:
    """
    Виконує MongoDB aggregation pipeline з валідацією та обмеженнями безпеки.
    
    Дозволяє виконувати складні запити з групуванням, розгортанням масивів та агрегаціями.
    
    Приклад для знаходження учасників, що брали участь більше ніж в одному аукціоні:
    {
        "collection_name": "prozorro_auctions",
        "pipeline": [
            {"$unwind": "$auction_data.bids"},
            {"$unwind": "$auction_data.bids.bidders"},
            {
                "$group": {
                    "_id": "$auction_data.bids.bidders.identifier.id",
                    "auctions_count": {"$addToSet": "$auction_id"},
                    "auction_ids": {"$push": "$auction_id"}
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
    
    Args:
        collection_name: Назва колекції (prozorro_auctions або llm_cache)
        pipeline: Список aggregation stages (масив словників)
        limit: Максимальна кількість результатів (опціонально, максимум 100)
        
    Returns:
        Словник з результатами виконання aggregation
    """
    global _query_builder
    
    try:
        initialize_connection()
        
        # Якщо pipeline передано як рядок, парсимо його
        if isinstance(pipeline, str):
            pipeline = json.loads(pipeline)
        
        # Виконуємо aggregation
        result = _query_builder.execute_aggregation(
            collection_name=collection_name,
            pipeline=pipeline,
            limit=limit
        )
        
        return result
    except json.JSONDecodeError as e:
        return {
            'success': False,
            'error': f'Помилка парсингу JSON: {str(e)}'
        }
    except Exception as e:
        return {
            'success': False,
            'error': f'Помилка виконання aggregation: {str(e)}'
        }


def main():
    """Головна функція для запуску MCP сервера."""
    # Використовуємо stdio transport для MCP
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
