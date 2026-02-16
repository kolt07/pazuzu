# -*- coding: utf-8 -*-
"""
MCP сервер для надання схеми метаданих колекцій бази даних.
"""

import json
import sys
from datetime import datetime
from typing import Any, Optional
from mcp.server.fastmcp import FastMCP
from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.collection_knowledge_repository import CollectionKnowledgeRepository
from utils.schema_analyzer import SchemaAnalyzer
from utils.data_dictionary import DataDictionary
from utils.mongodb_validator import MongoDBValidator

# Ініціалізуємо MCP сервер
mcp = FastMCP("schema-mcp", json_response=True)

# Глобальні змінні для кешування
_schema_cache: dict = None
_analyzer: SchemaAnalyzer = None
_data_dictionary: DataDictionary = None
_validator: MongoDBValidator = None


def initialize_connection():
    """Ініціалізує підключення до бази даних."""
    global _analyzer, _data_dictionary, _validator
    
    if _analyzer is None:
        try:
            settings = Settings()
            MongoDBConnection.initialize(settings)
            _analyzer = SchemaAnalyzer()
            _data_dictionary = DataDictionary()
            _validator = MongoDBValidator(_data_dictionary)
        except Exception as e:
            print(f"Помилка ініціалізації підключення до БД: {e}", file=sys.stderr)
            raise


@mcp.resource("mongodb://schema")
def get_database_schema() -> str:
    """
    Ресурс, який повертає схему метаданих колекцій бази даних.
    Використовує Data Dictionary як основне джерело, доповнюючи реальними даними з БД.
    
    Returns:
        JSON рядок зі схемою метаданих
    """
    global _schema_cache, _analyzer, _data_dictionary
    
    try:
        initialize_connection()
        
        # Отримуємо схему з Data Dictionary
        dict_schema = _data_dictionary.to_schema_dict()
        
        # Доповнюємо реальними даними з БД (статистика)
        real_schema = _analyzer.generate_schema()
        
        # Об'єднуємо: Data Dictionary як основа, реальні дані для статистики
        for collection_name in dict_schema['collections'].keys():
            if collection_name in real_schema.get('collections', {}):
                real_collection = real_schema['collections'][collection_name]
                dict_collection = dict_schema['collections'][collection_name]
                
                # Додаємо статистику з реальних даних
                dict_collection['total_documents'] = real_collection.get('total_documents', 0)
                dict_collection['analyzed_documents'] = real_collection.get('analyzed_documents', 0)
        
        # Додаємо метадані
        dict_schema['generated_at'] = datetime.utcnow().isoformat()
        dict_schema['metadata'] = _data_dictionary.get_metadata()
        
        # Конвертуємо в JSON з красивим форматуванням
        schema_json = json.dumps(dict_schema, indent=2, ensure_ascii=False, default=str)
        
        return schema_json
    except Exception as e:
        error_response = {
            'error': str(e),
            'message': 'Не вдалося згенерувати схему метаданих'
        }
        return json.dumps(error_response, indent=2, ensure_ascii=False)


@mcp.tool()
def refresh_schema_cache() -> dict:
    """
    Оновлює кеш схеми метаданих.
    
    Returns:
        Словник з результатом операції
    """
    global _schema_cache, _analyzer
    
    try:
        initialize_connection()
        
        # Генеруємо нову схему
        schema = _analyzer.generate_schema()
        _schema_cache = schema
        
        return {
            'success': True,
            'message': 'Кеш схеми оновлено успішно',
            'generated_at': schema.get('generated_at', '')
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Не вдалося оновити кеш схеми'
        }


@mcp.tool()
def get_data_dictionary() -> dict:
    """
    Повертає повний Data Dictionary.
    
    Returns:
        Словник з Data Dictionary
    """
    global _data_dictionary
    
    try:
        initialize_connection()
        
        schema = _data_dictionary.to_schema_dict()
        metadata = _data_dictionary.get_metadata()
        
        return {
            'success': True,
            'data_dictionary': schema,
            'metadata': metadata
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': 'Не вдалося отримати Data Dictionary'
        }


@mcp.tool()
def apply_validation_schema(collection_name: str) -> dict:
    """
    Застосовує validation schema до колекції на основі Data Dictionary.
    
    Args:
        collection_name: Назва колекції
        
    Returns:
        Словник з результатом операції
    """
    global _validator
    
    try:
        initialize_connection()
        
        success, error = _validator.apply_validation_schema(collection_name)
        
        if success:
            return {
                'success': True,
                'message': f'Validation schema успішно застосовано до колекції {collection_name}'
            }
        else:
            return {
                'success': False,
                'error': error
            }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Не вдалося застосувати validation schema до колекції {collection_name}'
        }


@mcp.tool()
def validate_collection(collection_name: str) -> dict:
    """
    Валідує всі документи в колекції на основі Data Dictionary.
    
    Args:
        collection_name: Назва колекції
        
    Returns:
        Словник з результатами валідації
    """
    global _validator
    
    try:
        initialize_connection()
        
        result = _validator.validate_collection(collection_name)
        return result
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Не вдалося валідувати колекцію {collection_name}'
        }


@mcp.tool()
def get_collection_info(collection_name: str) -> dict:
    """
    Отримує детальну інформацію про конкретну колекцію.
    
    Args:
        collection_name: Назва колекції (prozorro_auctions або llm_cache)
    
    Returns:
        Словник з інформацією про колекцію
    """
    global _analyzer, _data_dictionary
    
    allowed_collections = ['prozorro_auctions', 'llm_cache', 'olx_listings', 'unified_listings']
    
    if collection_name not in allowed_collections:
        return {
            'success': False,
            'error': f'Колекція {collection_name} не доступна через MCP сервер. Доступні колекції: {", ".join(allowed_collections)}'
        }
    
    try:
        initialize_connection()
        
        # Отримуємо інформацію з Data Dictionary
        collection_def = _data_dictionary.get_collection(collection_name)
        if collection_def:
            # Доповнюємо реальними даними
            real_info = _analyzer.analyze_collection(collection_name)
            
            # Об'єднуємо дані
            collection_info = {
                'collection_name': collection_def.mongo_collection,
                'description': collection_def.description,
                'from_data_dictionary': True,
                'total_documents': real_info.get('total_documents', 0),
                'analyzed_documents': real_info.get('analyzed_documents', 0),
                'schema': _data_dictionary.to_schema_dict()['collections'][collection_name]['schema'],
                'indexes': collection_def.indexes,
                'relationships': collection_def.relationships
            }
        else:
            # Якщо немає в Data Dictionary, використовуємо тільки реальні дані
            collection_info = _analyzer.analyze_collection(collection_name)
            collection_info['from_data_dictionary'] = False
        
        return {
            'success': True,
            'collection': collection_info
        }
    except Exception as e:
        return {
            'success': False,
            'error': str(e),
            'message': f'Не вдалося отримати інформацію про колекцію {collection_name}'
        }


@mcp.resource("mongodb://collection-knowledge")
def get_collection_knowledge_resource() -> str:
    """
    Ресурс із згуртованими знаннями про дані в колекціях (результати автоматичного
    дослідження: кількість документів, статистика по полях, топ повторюваних значень).
    """
    try:
        initialize_connection()
        repo = CollectionKnowledgeRepository()
        all_latest = repo.get_all_latest(
            collection_names=['prozorro_auctions', 'olx_listings', 'llm_cache']
        )
        return json.dumps(all_latest, indent=2, ensure_ascii=False, default=str)
    except Exception as e:
        return json.dumps({"error": str(e)}, ensure_ascii=False)


@mcp.tool()
def get_collection_knowledge(collection_name: Optional[str] = None) -> dict:
    """
    Повертає збережені знання про дані в колекції(ах): статистика по полях (min, max, avg
    для числових; топ значень та кардинальність для категоріальних). Якщо collection_name
    не вказано — повертає профілі для prozorro_auctions, olx_listings, llm_cache.
    """
    try:
        initialize_connection()
        repo = CollectionKnowledgeRepository()
        if collection_name:
            doc = repo.get_latest(collection_name)
            result = {"collections": {collection_name: doc} if doc else {}}
        else:
            result = {"collections": repo.get_all_latest()}
        return {"success": True, **result}
    except Exception as e:
        return {"success": False, "error": str(e)}
    """Головна функція для запуску MCP сервера."""
    # Використовуємо stdio transport для MCP
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
