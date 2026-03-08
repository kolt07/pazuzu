# -*- coding: utf-8 -*-
"""
Модуль для роботи з Data Dictionary - єдиним джерелом правди про структуру даних.
"""

import yaml
from pathlib import Path
from typing import Dict, Any, List, Optional
from dataclasses import dataclass, field


@dataclass
class FieldDefinition:
    """Визначення поля в Data Dictionary."""
    name: str
    type: str
    required: bool = False
    description: str = ""
    unit: str = ""
    example: str = ""
    indexed: bool = False
    mongo_generated: bool = False
    enum: List[str] = field(default_factory=list)
    nested_fields: Dict[str, 'FieldDefinition'] = field(default_factory=dict)
    item_type: Optional[str] = None


@dataclass
class CollectionDefinition:
    """Визначення колекції в Data Dictionary."""
    name: str
    description: str
    mongo_collection: str
    fields: Dict[str, FieldDefinition] = field(default_factory=dict)
    indexes: List[Dict[str, Any]] = field(default_factory=list)
    relationships: List[Dict[str, Any]] = field(default_factory=list)
    flattened_fields: Dict[str, Dict[str, Any]] = field(default_factory=dict)


class DataDictionary:
    """Клас для роботи з Data Dictionary."""
    
    def __init__(self, dictionary_path: Optional[Path] = None):
        """
        Ініціалізація Data Dictionary.
        
        Args:
            dictionary_path: Шлях до YAML файлу з Data Dictionary
        """
        if dictionary_path is None:
            # Шлях за замовчуванням
            dictionary_path = Path(__file__).parent.parent / 'config' / 'data_dictionary.yaml'
        
        self.dictionary_path = dictionary_path
        self._data: Dict[str, Any] = {}
        self._collections: Dict[str, CollectionDefinition] = {}
        self._load()
    
    def _load(self) -> None:
        """Завантажує Data Dictionary з YAML файлу."""
        try:
            with open(self.dictionary_path, 'r', encoding='utf-8') as f:
                self._data = yaml.safe_load(f)
            
            # Парсимо колекції
            collections_data = self._data.get('collections', {})
            for collection_name, collection_data in collections_data.items():
                self._collections[collection_name] = self._parse_collection(
                    collection_name, collection_data
                )
        except Exception as e:
            raise ValueError(f"Помилка завантаження Data Dictionary: {e}")
    
    def _parse_collection(self, name: str, data: Dict[str, Any]) -> CollectionDefinition:
        """
        Парсить визначення колекції.
        
        Args:
            name: Назва колекції
            data: Дані колекції з YAML
            
        Returns:
            Визначення колекції
        """
        fields = {}
        fields_data = data.get('fields', {})
        
        for field_name, field_data in fields_data.items():
            fields[field_name] = self._parse_field(field_name, field_data)
        
        return CollectionDefinition(
            name=name,
            description=data.get('description', ''),
            mongo_collection=data.get('mongo_collection', name),
            fields=fields,
            indexes=data.get('indexes', []),
            relationships=data.get('relationships', []),
            flattened_fields=data.get('flattened_fields', {})
        )
    
    def _parse_field(self, name: str, data: Dict[str, Any]) -> FieldDefinition:
        """
        Парсить визначення поля.
        
        Args:
            name: Назва поля
            data: Дані поля з YAML
            
        Returns:
            Визначення поля
        """
        nested_fields = {}
        if 'nested_fields' in data:
            for nested_name, nested_data in data['nested_fields'].items():
                nested_fields[nested_name] = self._parse_field(nested_name, nested_data)
        
        return FieldDefinition(
            name=name,
            type=data.get('type', 'string'),
            required=data.get('required', False),
            description=data.get('description', ''),
            unit=data.get('unit', ''),
            example=data.get('example', ''),
            indexed=data.get('indexed', False),
            mongo_generated=data.get('mongo_generated', False),
            enum=data.get('enum', []),
            nested_fields=nested_fields,
            item_type=data.get('item_type')
        )
    
    def get_collection(self, collection_name: str) -> Optional[CollectionDefinition]:
        """
        Отримує визначення колекції.
        
        Args:
            collection_name: Назва колекції
            
        Returns:
            Визначення колекції або None
        """
        return self._collections.get(collection_name)
    
    def list_collections(self) -> List[str]:
        """
        Повертає список назв колекцій.
        
        Returns:
            Список назв колекцій
        """
        return list(self._collections.keys())
    
    def get_field(self, collection_name: str, field_name: str) -> Optional[FieldDefinition]:
        """
        Отримує визначення поля в колекції.
        
        Args:
            collection_name: Назва колекції
            field_name: Назва поля
            
        Returns:
            Визначення поля або None
        """
        collection = self.get_collection(collection_name)
        if collection:
            return collection.fields.get(field_name)
        return None
    
    def get_mongo_validation_schema(self, collection_name: str) -> Dict[str, Any]:
        """
        Генерує MongoDB validation schema для колекції.
        
        Args:
            collection_name: Назва колекції
            
        Returns:
            MongoDB validation schema
        """
        collection = self.get_collection(collection_name)
        if not collection:
            return {}
        
        properties = {}
        required = []
        
        for field_name, field_def in collection.fields.items():
            # Пропускаємо MongoDB згенеровані поля
            if field_def.mongo_generated:
                continue
            
            # Визначаємо тип для MongoDB
            mongo_type = self._get_mongo_type(field_def.type)
            
            field_schema = {
                'bsonType': mongo_type,
                'description': field_def.description
            }
            
            # Додаємо enum, якщо є
            if field_def.enum:
                field_schema['enum'] = field_def.enum
            
            # Обробляємо вкладені поля
            if field_def.type == 'object' and field_def.nested_fields:
                nested_properties = {}
                nested_required = []
                
                for nested_name, nested_def in field_def.nested_fields.items():
                    nested_mongo_type = self._get_mongo_type(nested_def.type)
                    nested_schema = {
                        'bsonType': nested_mongo_type,
                        'description': nested_def.description
                    }
                    
                    if nested_def.enum:
                        nested_schema['enum'] = nested_def.enum
                    
                    nested_properties[nested_name] = nested_schema
                    if nested_def.required:
                        nested_required.append(nested_name)
                
                field_schema['properties'] = nested_properties
                if nested_required:
                    field_schema['required'] = nested_required
            
            # Обробляємо масиви
            if field_def.type == 'array':
                if field_def.item_type:
                    item_mongo_type = self._get_mongo_type(field_def.item_type)
                    field_schema['items'] = {'bsonType': item_mongo_type}
                elif field_def.nested_fields:
                    # Масив об'єктів
                    nested_properties = {}
                    for nested_name, nested_def in field_def.nested_fields.items():
                        nested_mongo_type = self._get_mongo_type(nested_def.type)
                        nested_properties[nested_name] = {
                            'bsonType': nested_mongo_type,
                            'description': nested_def.description
                        }
                    field_schema['items'] = {
                        'bsonType': 'object',
                        'properties': nested_properties
                    }
            
            properties[field_name] = field_schema
            
            if field_def.required:
                required.append(field_name)
        
        schema = {
            'bsonType': 'object',
            'properties': properties
        }
        
        if required:
            schema['required'] = required
        
        return schema
    
    def _get_mongo_type(self, field_type: str) -> str:
        """
        Конвертує тип поля в MongoDB bsonType.
        
        Args:
            field_type: Тип поля
            
        Returns:
            MongoDB bsonType
        """
        type_mapping = {
            'string': 'string',
            'number': ['int', 'long', 'double', 'decimal'],
            'boolean': 'bool',
            'datetime': 'date',
            'ObjectId': 'objectId',
            'object': 'object',
            'array': 'array'
        }
        
        mongo_type = type_mapping.get(field_type, 'string')
        if isinstance(mongo_type, list):
            return mongo_type[0]  # Використовуємо перший тип для number
        return mongo_type
    
    def to_schema_dict(self) -> Dict[str, Any]:
        """
        Конвертує Data Dictionary в формат схеми для MCP серверів.
        
        Returns:
            Словник зі схемою
        """
        schema = {
            'generated_from': 'data_dictionary',
            'collections': {}
        }
        
        for collection_name, collection in self._collections.items():
            collection_schema = {
                'collection_name': collection.mongo_collection,
                'description': collection.description,
                'schema': {},
                'indexes': collection.indexes,
                'relationships': collection.relationships
            }
            
            # Конвертуємо поля в схему
            for field_name, field_def in collection.fields.items():
                field_schema = {
                    'type': field_def.type,
                    'required': field_def.required,
                    'description': field_def.description
                }
                
                if field_def.unit:
                    field_schema['unit'] = field_def.unit
                
                if field_def.example:
                    field_schema['example'] = field_def.example
                
                if field_def.enum:
                    field_schema['enum'] = field_def.enum
                
                if field_def.nested_fields:
                    nested_schema = {}
                    for nested_name, nested_def in field_def.nested_fields.items():
                        nested_schema[nested_name] = {
                            'type': nested_def.type,
                            'description': nested_def.description
                        }
                        if nested_def.unit:
                            nested_schema[nested_name]['unit'] = nested_def.unit
                    field_schema['nested_structure'] = nested_schema
                
                if field_def.item_type:
                    field_schema['item_type'] = field_def.item_type
                
                collection_schema['schema'][field_name] = field_schema
            
            # Додаємо вирівняні поля (flattened_fields) до схеми
            if collection.flattened_fields:
                collection_schema['flattened_fields'] = collection.flattened_fields
                collection_schema['flattened_fields_note'] = (
                    "ВАЖЛИВО: Ці поля формуються з auction_data та llm_cache.result при експорті. "
                    "Для фільтрації та аналітики використовуй join з llm_cache через description_hash. "
                    "Наприклад, для фільтрації за регіоном використовуй: "
                    "join з llm_cache, потім фільтр по llm_result.result.addresses[].region"
                )
            
            schema['collections'][collection_name] = collection_schema
        
        return schema
    
    def get_metadata(self) -> Dict[str, Any]:
        """
        Отримує метадані Data Dictionary.
        
        Returns:
            Словник з метаданими
        """
        return self._data.get('metadata', {})
