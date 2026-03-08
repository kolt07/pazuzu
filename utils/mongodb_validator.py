# -*- coding: utf-8 -*-
"""
Модуль для MongoDB валідації на основі Data Dictionary.
"""

from typing import Dict, Any, List, Optional, Tuple
from data.database.connection import MongoDBConnection
from utils.data_dictionary import DataDictionary


class MongoDBValidator:
    """Клас для валідації MongoDB колекцій на основі Data Dictionary."""
    
    def __init__(self, data_dictionary: Optional[DataDictionary] = None):
        """
        Ініціалізація валідатора.
        
        Args:
            data_dictionary: Екземпляр Data Dictionary. Якщо None, створюється новий.
        """
        self.data_dictionary = data_dictionary or DataDictionary()
        self.db = None
    
    def _get_database(self):
        """Отримує об'єкт бази даних."""
        if self.db is None:
            self.db = MongoDBConnection.get_database()
        return self.db
    
    def apply_validation_schema(self, collection_name: str) -> Tuple[bool, Optional[str]]:
        """
        Застосовує validation schema до колекції на основі Data Dictionary.
        
        Args:
            collection_name: Назва колекції
            
        Returns:
            Кортеж (success, error_message)
        """
        collection_def = self.data_dictionary.get_collection(collection_name)
        if not collection_def:
            return False, f"Колекція '{collection_name}' не знайдена в Data Dictionary"
        
        try:
            db = self._get_database()
            collection = db[collection_def.mongo_collection]
            
            # Генеруємо validation schema
            validation_schema = self.data_dictionary.get_mongo_validation_schema(collection_name)
            
            if not validation_schema:
                return False, f"Не вдалося згенерувати validation schema для '{collection_name}'"
            
            # Застосовуємо validation schema
            db.command({
                'collMod': collection_def.mongo_collection,
                'validator': {
                    '$jsonSchema': validation_schema
                },
                'validationLevel': 'moderate',  # Валідація тільки для нових та оновлених документів
                'validationAction': 'error'  # Помилка при невідповідності
            })
            
            return True, None
        except Exception as e:
            return False, f"Помилка застосування validation schema: {str(e)}"
    
    def validate_document(self, collection_name: str, document: Dict[str, Any]) -> Tuple[bool, List[str]]:
        """
        Валідує документ на основі Data Dictionary.
        
        Args:
            collection_name: Назва колекції
            document: Документ для валідації
            
        Returns:
            Кортеж (is_valid, list_of_errors)
        """
        collection_def = self.data_dictionary.get_collection(collection_name)
        if not collection_def:
            return False, [f"Колекція '{collection_name}' не знайдена в Data Dictionary"]
        
        errors = []
        
        # Перевірка обов'язкових полів
        for field_name, field_def in collection_def.fields.items():
            if field_def.required and not field_def.mongo_generated:
                if field_name not in document:
                    errors.append(f"Обов'язкове поле '{field_name}' відсутнє")
                elif document[field_name] is None:
                    errors.append(f"Поле '{field_name}' не може бути null")
        
        # Перевірка типів та значень
        for field_name, field_value in document.items():
            if field_name == '_id':
                continue  # Пропускаємо MongoDB _id
            
            field_def = collection_def.fields.get(field_name)
            if not field_def:
                # Поле не визначене в Data Dictionary - це попередження, але не помилка
                continue
            
            # Перевірка типу
            if not self._validate_field_type(field_value, field_def):
                errors.append(f"Поле '{field_name}' має невірний тип. Очікується: {field_def.type}")
            
            # Перевірка enum
            if field_def.enum and field_value not in field_def.enum:
                errors.append(f"Поле '{field_name}' має невірне значення. Дозволені: {', '.join(field_def.enum)}")
            
            # Перевірка вкладених полів
            if field_def.type == 'object' and isinstance(field_value, dict) and field_def.nested_fields:
                nested_errors = self._validate_nested_fields(field_value, field_def.nested_fields, f"{field_name}.")
                errors.extend(nested_errors)
        
        return len(errors) == 0, errors
    
    def _validate_field_type(self, value: Any, field_def) -> bool:
        """
        Перевіряє тип значення поля.
        
        Args:
            value: Значення для перевірки
            field_def: Визначення поля
            
        Returns:
            True, якщо тип вірний
        """
        if value is None:
            return not field_def.required
        
        type_mapping = {
            'string': str,
            'number': (int, float),
            'boolean': bool,
            'datetime': (str, type(None)),  # Може бути рядок або datetime об'єкт
            'ObjectId': str,  # В Python це рядок
            'object': dict,
            'array': list
        }
        
        expected_type = type_mapping.get(field_def.type)
        if expected_type is None:
            return True  # Невідомий тип - пропускаємо
        
        if isinstance(expected_type, tuple):
            return isinstance(value, expected_type)
        return isinstance(value, expected_type)
    
    def _validate_nested_fields(self, obj: Dict[str, Any], nested_fields: Dict, prefix: str = "") -> List[str]:
        """
        Валідує вкладені поля.
        
        Args:
            obj: Об'єкт для валідації
            nested_fields: Визначення вкладених полів
            prefix: Префікс для повідомлень про помилки
            
        Returns:
            Список помилок
        """
        errors = []
        
        for field_name, field_def in nested_fields.items():
            full_field_name = f"{prefix}{field_name}"
            
            if field_def.required:
                if field_name not in obj:
                    errors.append(f"Обов'язкове поле '{full_field_name}' відсутнє")
                    continue
                elif obj[field_name] is None:
                    errors.append(f"Поле '{full_field_name}' не може бути null")
                    continue
            
            if field_name in obj:
                value = obj[field_name]
                
                # Перевірка типу
                if not self._validate_field_type(value, field_def):
                    errors.append(f"Поле '{full_field_name}' має невірний тип. Очікується: {field_def.type}")
                
                # Перевірка enum
                if field_def.enum and value not in field_def.enum:
                    errors.append(f"Поле '{full_field_name}' має невірне значення. Дозволені: {', '.join(field_def.enum)}")
        
        return errors
    
    def validate_collection(self, collection_name: str) -> Dict[str, Any]:
        """
        Валідує всі документи в колекції.
        
        Args:
            collection_name: Назва колекції
            
        Returns:
            Словник з результатами валідації
        """
        collection_def = self.data_dictionary.get_collection(collection_name)
        if not collection_def:
            return {
                'success': False,
                'error': f"Колекція '{collection_name}' не знайдена в Data Dictionary"
            }
        
        try:
            db = self._get_database()
            collection = db[collection_def.mongo_collection]
            
            total_documents = collection.count_documents({})
            valid_documents = 0
            invalid_documents = 0
            errors_summary = {}
            
            # Валідуємо кожен документ
            for document in collection.find():
                is_valid, errors = self.validate_document(collection_name, document)
                
                if is_valid:
                    valid_documents += 1
                else:
                    invalid_documents += 1
                    for error in errors:
                        errors_summary[error] = errors_summary.get(error, 0) + 1
            
            return {
                'success': True,
                'collection': collection_name,
                'total_documents': total_documents,
                'valid_documents': valid_documents,
                'invalid_documents': invalid_documents,
                'errors_summary': errors_summary
            }
        except Exception as e:
            return {
                'success': False,
                'error': f"Помилка валідації колекції: {str(e)}"
            }
