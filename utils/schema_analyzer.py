# -*- coding: utf-8 -*-
"""
Модуль для аналізу структури даних у колекціях MongoDB та генерації схеми метаданих.
"""

from typing import Dict, Any, List, Set, Optional
from collections import defaultdict
from datetime import datetime
from data.database.connection import MongoDBConnection
from data.repositories.prozorro_auctions_repository import ProZorroAuctionsRepository
from data.repositories.llm_cache_repository import LLMCacheRepository


class SchemaAnalyzer:
    """Клас для аналізу структури даних у колекціях MongoDB."""
    
    # Колекції, які не повинні бути доступні через MCP
    EXCLUDED_COLLECTIONS = {'users', 'logs', 'app_data'}
    
    def __init__(self):
        """Ініціалізація аналізатора."""
        self.db = None
    
    def _get_database(self):
        """Отримує об'єкт бази даних."""
        if self.db is None:
            self.db = MongoDBConnection.get_database()
        return self.db
    
    def analyze_field_type(self, value: Any, path: str = "") -> Dict[str, Any]:
        """
        Аналізує тип поля та його структуру.
        
        Args:
            value: Значення поля
            path: Шлях до поля (для вкладених структур)
            
        Returns:
            Словник з інформацією про тип та структуру
        """
        field_info = {
            'type': None,
            'nullable': False,
            'examples': [],
            'nested_structure': None
        }
        
        if value is None:
            field_info['nullable'] = True
            field_info['type'] = 'null'
            return field_info
        
        python_type = type(value).__name__
        
        # Визначаємо тип
        if isinstance(value, bool):
            field_info['type'] = 'boolean'
        elif isinstance(value, int):
            field_info['type'] = 'integer'
        elif isinstance(value, float):
            field_info['type'] = 'number'
        elif isinstance(value, str):
            field_info['type'] = 'string'
            # Зберігаємо приклад значення (обмежуємо довжину)
            if len(value) <= 100:
                field_info['examples'].append(value)
        elif isinstance(value, datetime):
            field_info['type'] = 'datetime'
        elif isinstance(value, list):
            field_info['type'] = 'array'
            field_info['length'] = len(value)
            if value:
                # Аналізуємо тип елементів масиву
                element_types = set()
                element_examples = []
                nested_structures = []
                
                for item in value[:10]:  # Аналізуємо перші 10 елементів
                    if item is not None:
                        element_type = type(item).__name__
                        element_types.add(element_type)
                        if isinstance(item, dict):
                            nested_structure = self.analyze_structure(item, f"{path}[]")
                            nested_structures.append(nested_structure)
                        elif isinstance(item, (str, int, float, bool)):
                            if len(element_examples) < 5:
                                element_examples.append(item)
                        elif isinstance(item, list):
                            # Вкладені масиви
                            element_types.add('array')
                
                if len(element_types) == 1:
                    # Всі елементи одного типу
                    element_type_name = list(element_types)[0]
                    if element_type_name == 'dict':
                        # Об'єднуємо всі вкладені структури
                        if nested_structures:
                            field_info['item_structure'] = self._merge_structures(nested_structures)
                    else:
                        field_info['item_type'] = element_type_name
                        if element_examples:
                            field_info['examples'] = element_examples
                else:
                    # Змішаний тип
                    field_info['item_type'] = 'mixed'
                    field_info['item_types'] = list(element_types)
                    if nested_structures:
                        # Якщо є об'єкти, об'єднуємо їх структури
                        field_info['item_structure'] = self._merge_structures(nested_structures)
                    if element_examples:
                        field_info['examples'] = element_examples
        elif isinstance(value, dict):
            field_info['type'] = 'object'
            field_info['nested_structure'] = self.analyze_structure(value, path)
        else:
            field_info['type'] = python_type
        
        return field_info
    
    def analyze_structure(self, document: Dict[str, Any], base_path: str = "") -> Dict[str, Any]:
        """
        Аналізує структуру документа.
        
        Args:
            document: Документ для аналізу
            base_path: Базовий шлях для вкладених полів
            
        Returns:
            Словник з описом структури
        """
        structure = {}
        
        for key, value in document.items():
            # Пропускаємо MongoDB _id
            if key == '_id':
                continue
            
            field_path = f"{base_path}.{key}" if base_path else key
            field_info = self.analyze_field_type(value, field_path)
            structure[key] = field_info
        
        return structure
    
    def _merge_structures(self, structures: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Об'єднує кілька структур в одну, враховуючи всі можливі поля.
        
        Args:
            structures: Список структур для об'єднання
            
        Returns:
            Об'єднана структура
        """
        merged = {}
        field_occurrences = defaultdict(int)
        field_types = defaultdict(set)
        field_examples = defaultdict(list)
        field_nullable = defaultdict(bool)
        nested_structures = defaultdict(list)
        
        for structure in structures:
            for field_name, field_info in structure.items():
                field_occurrences[field_name] += 1
                field_types[field_name].add(field_info.get('type'))
                field_nullable[field_name] = field_nullable[field_name] or field_info.get('nullable', False)
                
                if 'examples' in field_info:
                    field_examples[field_name].extend(field_info['examples'])
                
                # Обробляємо вкладені структури (для об'єктів)
                if 'nested_structure' in field_info:
                    nested_structures[field_name].append(field_info['nested_structure'])
                
                # Обробляємо структури елементів масивів
                if 'item_structure' in field_info:
                    nested_structures[field_name].append(field_info['item_structure'])
        
        # Формуємо об'єднану структуру
        for field_name in field_occurrences.keys():
            occurrences = field_occurrences[field_name]
            types = list(field_types[field_name])
            
            merged_field = {
                'type': types[0] if len(types) == 1 else 'mixed',
                'nullable': field_nullable[field_name],
                'occurrence_rate': occurrences / len(structures),
                'possible_types': types if len(types) > 1 else None
            }
            
            if field_examples[field_name]:
                # Видаляємо дублікати, зберігаючи порядок
                seen = set()
                unique_examples = []
                for ex in field_examples[field_name]:
                    if ex not in seen:
                        seen.add(ex)
                        unique_examples.append(ex)
                        if len(unique_examples) >= 5:
                            break
                merged_field['examples'] = unique_examples
            
            if nested_structures[field_name]:
                # Об'єднуємо вкладені структури
                merged_nested = self._merge_structures(nested_structures[field_name])
                # Для масивів використовуємо item_structure, для об'єктів - nested_structure
                if merged_field.get('type') == 'array':
                    merged_field['item_structure'] = merged_nested
                else:
                    merged_field['nested_structure'] = merged_nested
            
            merged[field_name] = merged_field
        
        return merged
    
    def analyze_collection(self, collection_name: str, sample_size: int = 100) -> Dict[str, Any]:
        """
        Аналізує структуру колекції.
        
        Args:
            collection_name: Назва колекції
            sample_size: Кількість документів для аналізу
            
        Returns:
            Словник з описом структури колекції
        """
        db = self._get_database()
        collection = db[collection_name]
        
        # Отримуємо загальну статистику
        total_count = collection.count_documents({})
        
        if total_count == 0:
            return {
                'collection_name': collection_name,
                'total_documents': 0,
                'schema': {},
                'indexes': []
            }
        
        # Вибірка документів для аналізу
        sample_size = min(sample_size, total_count)
        sample_documents = list(collection.find().limit(sample_size))
        
        # Аналізуємо структуру кожного документа
        structures = []
        for doc in sample_documents:
            structure = self.analyze_structure(doc)
            structures.append(structure)
        
        # Об'єднуємо структури
        merged_schema = self._merge_structures(structures)
        
        # Аналізуємо індекси
        indexes = []
        try:
            for index in collection.list_indexes():
                index_info = {
                    'name': index.get('name', ''),
                    'keys': dict(index.get('key', {}))
                }
                if 'unique' in index:
                    index_info['unique'] = index['unique']
                indexes.append(index_info)
        except Exception:
            pass
        
        # Знаходимо зв'язки між колекціями
        relationships = self._find_relationships(collection_name, merged_schema)
        
        return {
            'collection_name': collection_name,
            'total_documents': total_count,
            'analyzed_documents': sample_size,
            'schema': merged_schema,
            'indexes': indexes,
            'relationships': relationships
        }
    
    def _find_relationships(self, collection_name: str, schema: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Знаходить зв'язки між колекціями на основі схеми.
        
        Args:
            collection_name: Назва колекції
            schema: Схема колекції
            
        Returns:
            Список зв'язків
        """
        relationships = []
        
        # Для prozorro_auctions шукаємо description_hash, який може зв'язувати з llm_cache
        if collection_name == 'prozorro_auctions':
            if 'description_hash' in schema:
                relationships.append({
                    'type': 'reference',
                    'field': 'description_hash',
                    'target_collection': 'llm_cache',
                    'target_field': 'description_hash',
                    'description': 'Зв\'язок між аукціоном та кешованим результатом LLM парсингу опису'
                })
        
        # Для llm_cache шукаємо description_hash, який може зв'язувати з prozorro_auctions
        if collection_name == 'llm_cache':
            relationships.append({
                'type': 'reference',
                'field': 'description_hash',
                'target_collection': 'prozorro_auctions',
                'target_field': 'description_hash',
                'description': 'Зв\'язок між кешованим результатом LLM та аукціонами з таким же хешем опису'
            })
        
        return relationships
    
    def generate_schema(self, collection_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Генерує повну схему для вказаних колекцій.
        
        Args:
            collection_names: Список назв колекцій для аналізу. Якщо None, аналізує всі доступні колекції.
            
        Returns:
            Словник з повною схемою
        """
        db = self._get_database()
        
        if collection_names is None:
            # Отримуємо список всіх колекцій, виключаючи системні та заборонені
            all_collections = db.list_collection_names()
            collection_names = [
                name for name in all_collections 
                if not name.startswith('system.') and name not in self.EXCLUDED_COLLECTIONS
            ]
        
        # Фільтруємо тільки дозволені колекції
        allowed_collections = ['prozorro_auctions', 'llm_cache']
        collection_names = [name for name in collection_names if name in allowed_collections]
        
        schema = {
            'generated_at': datetime.utcnow().isoformat(),
            'collections': {}
        }
        
        for collection_name in collection_names:
            try:
                collection_schema = self.analyze_collection(collection_name)
                schema['collections'][collection_name] = collection_schema
            except Exception as e:
                schema['collections'][collection_name] = {
                    'collection_name': collection_name,
                    'error': str(e)
                }
        
        # Додаємо загальну інформацію про зв'язки між колекціями
        schema['global_relationships'] = self._analyze_global_relationships(schema['collections'])
        
        return schema
    
    def _analyze_global_relationships(self, collections: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Аналізує глобальні зв'язки між колекціями.
        
        Args:
            collections: Словник зі схемами колекцій
            
        Returns:
            Список глобальних зв'язків
        """
        relationships = []
        
        # Зв'язок між prozorro_auctions та llm_cache через description_hash
        if 'prozorro_auctions' in collections and 'llm_cache' in collections:
            prozorro_schema = collections['prozorro_auctions'].get('schema', {})
            llm_cache_schema = collections['llm_cache'].get('schema', {})
            
            if 'description_hash' in prozorro_schema and 'description_hash' in llm_cache_schema:
                relationships.append({
                    'type': 'one_to_many',
                    'from_collection': 'llm_cache',
                    'from_field': 'description_hash',
                    'to_collection': 'prozorro_auctions',
                    'to_field': 'description_hash',
                    'description': 'Один кешований результат LLM може бути пов\'язаний з кількома аукціонами, які мають однаковий опис (через description_hash)'
                })
        
        return relationships
