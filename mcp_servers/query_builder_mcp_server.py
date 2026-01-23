# -*- coding: utf-8 -*-
"""
MCP сервер для безпечного виконання запитів до MongoDB через абстрактний API.
"""

import json
import sys
from typing import Any, Dict, List, Optional
from mcp.server.fastmcp import FastMCP
from config.settings import Settings
from data.database.connection import MongoDBConnection
from utils.query_builder import QueryBuilder

# Ініціалізуємо MCP сервер
mcp = FastMCP("query-builder-mcp", json_response=True)

# Глобальні змінні
_query_builder: QueryBuilder = None


def initialize_connection():
    """Ініціалізує підключення до бази даних."""
    global _query_builder
    
    if _query_builder is None:
        try:
            settings = Settings()
            MongoDBConnection.initialize(settings)
            _query_builder = QueryBuilder()
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
        "filters": {
            "status": "finished",
            "region": "Київська"
        },
        "join": [
            {
                "collection": "llm_cache",
                "on": ["description_hash", "description_hash"],
                "as": "llm_result",
                "unwrap": true
            }
        ],
        "projection": ["auction_id", "status", "llm_result"],
        "limit": 10
    }
    
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


@mcp.tool()
def get_allowed_collections() -> Dict[str, Any]:
    """
    Повертає список дозволених колекцій для запитів.
    
    Returns:
        Словник зі списком дозволених колекцій
    """
    return {
        'success': True,
        'collections': list(QueryBuilder.ALLOWED_COLLECTIONS),
        'max_results': QueryBuilder.MAX_RESULTS
    }


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
