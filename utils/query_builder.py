# -*- coding: utf-8 -*-
"""
Модуль для валідації та трансформації абстрактних запитів у MongoDB запити.
"""

import copy
from datetime import datetime, timezone
from typing import Dict, Any, List, Optional, Set, Tuple
from pymongo.collection import Collection
from data.database.connection import MongoDBConnection


class QueryBuilder:
    """Клас для побудови безпечних MongoDB запитів з абстрактних запитів."""
    
    # Дозволені колекції для запитів
    ALLOWED_COLLECTIONS = {'prozorro_auctions', 'llm_cache', 'olx_listings', 'unified_listings'}
    
    # Дозволені оператори для фільтрів
    ALLOWED_OPERATORS = {
        '$eq', '$ne', '$gt', '$gte', '$lt', '$lte',
        '$in', '$nin', '$exists', '$and', '$or', '$not', '$elemMatch'
    }

    # MCP deny-rules: верхньорівневі ключі запиту, які ніколи не приймаються
    DENY_TOP_LEVEL_KEYS = frozenset({'raw_query', '$where', '$eval', '$function', '$expr'})

    # Заборонені оператори (безпека)
    FORBIDDEN_OPERATORS = {
        '$where', '$eval', '$function', '$expr', '$regex', '$text'
    }
    
    # Максимальна кількість результатів
    MAX_RESULTS = 5000
    
    # Максимальна глибина вкладеності
    MAX_NESTING_DEPTH = 5
    
    # Дозволені aggregation stages
    ALLOWED_AGGREGATION_STAGES = {
        '$match', '$project', '$group', '$unwind', '$sort', '$limit', 
        '$skip', '$lookup', '$addFields', '$set', '$unset', '$replaceRoot',
        '$count', '$facet', '$bucket', '$bucketAuto', '$sample'
    }
    
    # Заборонені aggregation stages (безпека)
    FORBIDDEN_AGGREGATION_STAGES = {
        '$out', '$merge', '$indexStats', '$currentOp', '$listLocalSessions',
        '$listSessions', '$planCacheStats', '$collStats', '$dbStats'
    }
    
    # Дозволені оператори в $group
    ALLOWED_GROUP_OPERATORS = {
        '$sum', '$avg', '$min', '$max', '$first', '$last', '$push', '$addToSet',
        '$stdDevPop', '$stdDevSamp', '$count', '$size'
    }
    
    def __init__(self):
        """Ініціалізація білдера запитів."""
        self.db = None
    
    def _get_database(self):
        """Отримує об'єкт бази даних."""
        if self.db is None:
            self.db = MongoDBConnection.get_database()
        return self.db
    
    def validate_query(self, query: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Валідує абстрактний запит.
        
        Args:
            query: Абстрактний запит
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        # MCP deny-rules: не приймати заборонені верхньорівневі ключі
        for key in query:
            if key in self.DENY_TOP_LEVEL_KEYS:
                return False, f"Параметр '{key}' заборонений (MCP deny-rules). Використовуйте абстрактний запит (collection, filters, limit)."

        # Перевірка обов'язкових полів
        if 'collection' not in query:
            return False, "Поле 'collection' є обов'язковим"

        collection_name = query['collection']
        
        # Перевірка дозволених колекцій
        if collection_name not in self.ALLOWED_COLLECTIONS:
            return False, f"Колекція '{collection_name}' не доступна. Дозволені колекції: {', '.join(self.ALLOWED_COLLECTIONS)}"
        
        # Перевірка фільтрів
        if 'filters' in query:
            is_valid, error = self._validate_filters(query['filters'], depth=0)
            if not is_valid:
                return False, error
        
        # Перевірка join
        if 'join' in query:
            if not isinstance(query['join'], list):
                return False, "Поле 'join' має бути списком"
            
            for join_item in query['join']:
                is_valid, error = self._validate_join(join_item)
                if not is_valid:
                    return False, error
        
        # Перевірка projection
        if 'projection' in query:
            if not isinstance(query['projection'], list):
                return False, "Поле 'projection' має бути списком"
        
        # Перевірка limit
        if 'limit' in query:
            limit = query['limit']
            if not isinstance(limit, int) or limit <= 0:
                return False, "Поле 'limit' має бути додатнім цілим числом"
            if limit > self.MAX_RESULTS:
                return False, f"Поле 'limit' не може перевищувати {self.MAX_RESULTS}"
        
        return True, None
    
    def _validate_filters(self, filters: Dict[str, Any], depth: int = 0) -> Tuple[bool, Optional[str]]:
        """
        Валідує фільтри запиту.
        
        Args:
            filters: Словник з фільтрами
            depth: Поточна глибина вкладеності
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        if depth > self.MAX_NESTING_DEPTH:
            return False, f"Максимальна глибина вкладеності ({self.MAX_NESTING_DEPTH}) перевищена"
        
        if not isinstance(filters, dict):
            return False, "Фільтри мають бути словником"
        
        for key, value in filters.items():
            # Перевірка на заборонені оператори
            # Дозволяємо $regex тільки для полів статусу (перевірка буде в _transform_filters)
            if key == '$regex':
                # Дозволяємо на рівні валідації, детальна перевірка в _transform_filters
                pass
            elif key in self.FORBIDDEN_OPERATORS:
                return False, f"Оператор '{key}' заборонений з міркувань безпеки"
            
            # Перевірка дозволених операторів
            if key.startswith('$'):
                if key not in self.ALLOWED_OPERATORS and key != '$regex':
                    return False, f"Оператор '{key}' не дозволений. Дозволені оператори: {', '.join(sorted(self.ALLOWED_OPERATORS))}"
            
            # Рекурсивна перевірка для $and, $or
            if key in ('$and', '$or'):
                if not isinstance(value, list):
                    return False, f"Оператор '{key}' вимагає список умов"
                for condition in value:
                    is_valid, error = self._validate_filters(condition, depth + 1)
                    if not is_valid:
                        return False, error
            elif key == '$not':
                is_valid, error = self._validate_filters(value, depth + 1)
                if not is_valid:
                    return False, error
            elif isinstance(value, dict):
                # Вкладені умови
                # Перевіряємо, чи є $regex в значенні
                if '$regex' in value:
                    # Дозволяємо $regex для полів статусу та fallback полів location (для пошуку за адресами)
                    allowed_regex_fields = ['status', 'location', 'resolved_locations', 'locality', 'region']
                    key_lower = key.lower()
                    is_allowed = (
                        'status' in key_lower or 
                        key.endswith('.status') or
                        any(field in key_lower for field in allowed_regex_fields)
                    )
                    if not is_allowed:
                        return False, f"Оператор '$regex' дозволений тільки для полів статусу та location (fallback для пошуку за адресами). Для поля '{key}' використовуйте analytics-mcp або точне порівняння."
                is_valid, error = self._validate_filters(value, depth + 1)
                if not is_valid:
                    return False, error
        
        return True, None
    
    def _validate_join(self, join_item: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Валідує join запит.
        
        Args:
            join_item: Елемент join
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        if not isinstance(join_item, dict):
            return False, "Елемент join має бути словником"
        
        if 'collection' not in join_item:
            return False, "Поле 'collection' є обов'язковим для join"
        
        collection_name = join_item['collection']
        
        if collection_name not in self.ALLOWED_COLLECTIONS:
            return False, f"Колекція '{collection_name}' не доступна для join"
        
        if 'on' not in join_item:
            return False, "Поле 'on' є обов'язковим для join"
        
        on_fields = join_item['on']
        if not isinstance(on_fields, list) or len(on_fields) != 2:
            return False, "Поле 'on' має бути списком з двох елементів [localField, foreignField]"
        
        return True, None
    
    def _parse_iso_to_utc(self, s: Any):
        """Парсить ISO-рядок у datetime (UTC). Повертає None при помилці."""
        if not isinstance(s, str) or not s.strip():
            return None
        try:
            s = s.strip().replace('Z', '+00:00')
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
            return dt
        except (ValueError, TypeError):
            return None
    
    def _normalize_date_filters(self, collection_name: str, filters: Dict[str, Any]) -> None:
        """
        Модифікує filters in-place: для полів-дат, збережених у БД як BSON Date,
        перетворює значення-рядки (ISO) на datetime, щоб $match коректно порівнював.
        olx_listings: updated_at — BSON Date.
        """
        if collection_name != 'olx_listings':
            return
        if 'updated_at' not in filters or not isinstance(filters['updated_at'], dict):
            return
        for op in ('$gte', '$lte', '$gt', '$lt'):
            if op in filters['updated_at'] and isinstance(filters['updated_at'][op], str):
                parsed = self._parse_iso_to_utc(filters['updated_at'][op])
                if parsed is not None:
                    filters['updated_at'][op] = parsed

    def _normalize_match_date_value(self, collection_name: str, obj: Dict[str, Any], date_key: str) -> None:
        """
        Рекурсивно знаходить у obj ключ date_key (напр. updated_at) і перетворює
        ISO-рядки в $gte/$lte/$gt/$lt на datetime. Модифікує obj in-place.
        """
        if collection_name != 'olx_listings' or date_key != 'updated_at':
            return
        if date_key not in obj or not isinstance(obj[date_key], dict):
            return
        for op in ('$gte', '$lte', '$gt', '$lt'):
            if op in obj[date_key] and isinstance(obj[date_key][op], str):
                parsed = self._parse_iso_to_utc(obj[date_key][op])
                if parsed is not None:
                    obj[date_key][op] = parsed

    def _normalize_aggregation_match_dates(self, collection_name: str, pipeline: List[Dict[str, Any]]) -> None:
        """
        Проходить по всіх $match stages у pipeline і перетворює рядкові дати (ISO)
        на datetime для полів, збережених у БД як BSON Date (olx_listings: updated_at).
        Модифікує pipeline in-place.
        """
        date_fields = ('updated_at',) if collection_name == 'olx_listings' else ()
        if not date_fields:
            return

        def walk(d: Any) -> None:
            if isinstance(d, dict):
                for key in date_fields:
                    self._normalize_match_date_value(collection_name, d, key)
                for v in d.values():
                    walk(v)
            elif isinstance(d, list):
                for item in d:
                    walk(item)

        for stage in pipeline:
            if isinstance(stage, dict) and '$match' in stage:
                walk(stage['$match'])
    
    def build_mongodb_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Трансформує абстрактний запит у MongoDB aggregation pipeline.
        
        Args:
            query: Абстрактний запит
            
        Returns:
            MongoDB aggregation pipeline
        """
        pipeline = []
        collection_name = query['collection']
        collection = self._get_database()[collection_name]
        
        # Розділяємо фільтри на ті, що до join, і ті, що після join
        filters = query.get('filters', {})
        pre_join_filters = {}
        post_join_filters = {}
        
        if filters:
            for key, value in filters.items():
                # Фільтри, що стосуються llm_result, мають бути після join
                if 'llm_result' in key or key.startswith('llm_'):
                    post_join_filters[key] = value
                else:
                    pre_join_filters[key] = value
        
        # Нормалізація дат: у olx_listings поле updated_at — BSON Date; рядки ISO перетворюємо на datetime
        self._normalize_date_filters(collection_name, pre_join_filters)
        
        # Додаємо match stage для фільтрів ДО join
        if pre_join_filters:
            match_stage = {'$match': self._transform_filters(pre_join_filters)}
            pipeline.append(match_stage)
        
        # Додаємо lookup stages для join
        if 'join' in query:
            for join_item in query['join']:
                lookup_stages = self._build_lookup(join_item, collection_name)
                if isinstance(lookup_stages, list):
                    pipeline.extend(lookup_stages)
                else:
                    pipeline.append(lookup_stages)
        
        # Додаємо match stage для фільтрів ПІСЛЯ join
        if post_join_filters:
            match_stage = {'$match': self._transform_filters(post_join_filters)}
            pipeline.append(match_stage)
        
        # Додаємо addFields stage для обчислення полів
        if 'addFields' in query:
            add_fields_stage = {'$addFields': query['addFields']}
            pipeline.append(add_fields_stage)
        
        # Додаємо sort stage. Для olx_listings за замовчуванням — за зменшенням дати оновлення
        if 'sort' in query:
            sort_stage = {'$sort': query['sort']}
            pipeline.append(sort_stage)
        elif collection_name == 'olx_listings':
            pipeline.append({'$sort': {'updated_at': -1}})
        
        # Додаємо projection stage лише якщо задано непорожній список полів (порожній projection = повні документи)
        if 'projection' in query and query['projection']:
            projection_stage = self._build_projection(query['projection'], query.get('join', []))
            pipeline.append(projection_stage)
        
        # Додаємо limit
        limit = min(query.get('limit', self.MAX_RESULTS), self.MAX_RESULTS)
        pipeline.append({'$limit': limit})
        
        return {
            'collection': collection_name,
            'pipeline': pipeline
        }
    
    def _transform_filters(self, filters: Dict[str, Any]) -> Dict[str, Any]:
        """
        Трансформує фільтри у MongoDB формат.
        
        Args:
            filters: Фільтри у абстрактному форматі
            
        Returns:
            Фільтри у форматі MongoDB
        """
        mongo_filters = {}
        
        for key, value in filters.items():
            if key.startswith('$'):
                # Оператор MongoDB
                if key in ('$and', '$or'):
                    mongo_filters[key] = [
                        self._transform_filters(condition) 
                        for condition in value
                    ]
                elif key == '$not':
                    mongo_filters[key] = self._transform_filters(value)
                else:
                    mongo_filters[key] = value
            elif isinstance(value, dict):
                # Вкладені умови
                # Дозволяємо $regex тільки для полів статусу (безпечно)
                if '$regex' in value:
                    # Перевіряємо, чи це поле статусу
                    if 'status' in key.lower() or key.endswith('.status'):
                        # Дозволяємо $regex для статусів
                        mongo_filters[key] = value
                    else:
                        # Для інших полів $regex заборонений
                        raise ValueError(f"Оператор '$regex' заборонений для поля '{key}'. Використовуйте analytics-mcp для фільтрації за регіоном/містом.")
                else:
                    mongo_filters[key] = self._transform_filters(value)
            else:
                # Просте порівняння (еквівалент $eq)
                mongo_filters[key] = value
        
        return mongo_filters
    
    def _build_lookup(self, join_item: Dict[str, Any], from_collection: str) -> List[Dict[str, Any]]:
        """
        Створює $lookup stage для join.
        
        Args:
            join_item: Елемент join
            from_collection: Назва початкової колекції
            
        Returns:
            Список stages для pipeline ($lookup та опціонально $unwind)
        """
        to_collection = join_item['collection']
        on_fields = join_item['on']
        local_field = on_fields[0]
        foreign_field = on_fields[1]
        as_field = join_item.get('as', f'{to_collection}_joined')
        
        stages = []
        
        # Додаємо $lookup stage
        lookup_stage = {
            '$lookup': {
                'from': to_collection,
                'localField': local_field,
                'foreignField': foreign_field,
                'as': as_field
            }
        }
        stages.append(lookup_stage)
        
        # Для llm_cache завжди робимо unwind та addFields для зручності
        if to_collection == 'llm_cache':
            # Unwind
            stages.append({
                '$unwind': {
                    'path': f'${as_field}',
                    'preserveNullAndEmptyArrays': True
                }
            })
            # AddFields для зручності доступу
            stages.append({
                '$addFields': {
                    'llm_result': f'${as_field}'
                }
            })
        elif join_item.get('unwrap', False):
            # Якщо потрібен unwrap (один елемент замість масиву)
            unwind_stage = {
                '$unwind': {
                    'path': f'${as_field}',
                    'preserveNullAndEmptyArrays': True
                }
            }
            stages.append(unwind_stage)
        
        return stages
    
    def _build_projection(self, projection: List[str], joins: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Створює $project stage.
        
        Args:
            projection: Список полів для проекції
            joins: Список join операцій
            
        Returns:
            $project stage
        """
        project_fields = {}
        
        # Додаємо _id за замовчуванням
        project_fields['_id'] = 1
        
        # Додаємо поля з projection
        for field in projection:
            project_fields[field] = 1
        
        # Додаємо поля з join
        for join_item in joins:
            as_field = join_item.get('as', f"{join_item['collection']}_joined")
            project_fields[as_field] = 1
        
        return {'$project': project_fields}
    
    def execute_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Виконує абстрактний запит та повертає результати.
        
        Args:
            query: Абстрактний запит
            
        Returns:
            Словник з результатами виконання
        """
        # Валідація
        is_valid, error = self.validate_query(query)
        if not is_valid:
            return {
                'success': False,
                'error': error
            }
        
        # Побудова MongoDB запиту
        mongo_query = self.build_mongodb_query(query)
        
        # Виконання запиту
        try:
            collection = self._get_database()[mongo_query['collection']]
            results = list(collection.aggregate(mongo_query['pipeline']))
            
            # Обмежуємо кількість результатів
            results = results[:self.MAX_RESULTS]
            
            # Конвертуємо ObjectId в рядки
            for result in results:
                if '_id' in result:
                    result['_id'] = str(result['_id'])
                # Обробляємо вкладені документи
                self._convert_object_ids(result)
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'Помилка виконання запиту: {str(e)}'
            }
    
    def _convert_object_ids(self, obj: Any) -> None:
        """
        Рекурсивно конвертує ObjectId в рядки.
        
        Args:
            obj: Об'єкт для обробки
        """
        if isinstance(obj, dict):
            for key, value in obj.items():
                if key == '_id' and hasattr(value, '__str__'):
                    obj[key] = str(value)
                elif isinstance(value, (dict, list)):
                    self._convert_object_ids(value)
        elif isinstance(obj, list):
            for item in obj:
                if isinstance(item, (dict, list)):
                    self._convert_object_ids(item)
    
    def validate_aggregation_pipeline(self, pipeline: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
        """
        Валідує MongoDB aggregation pipeline.
        
        Args:
            pipeline: Список aggregation stages
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        if not isinstance(pipeline, list):
            return False, "Pipeline має бути списком stages"
        
        if len(pipeline) == 0:
            return False, "Pipeline не може бути порожнім"
        
        if len(pipeline) > 20:  # Обмеження на кількість stages
            return False, "Pipeline не може містити більше 20 stages"
        
        for i, stage in enumerate(pipeline):
            if not isinstance(stage, dict):
                return False, f"Stage {i} має бути словником"
            
            if len(stage) != 1:
                return False, f"Stage {i} має містити рівно один ключ (назву stage)"
            
            stage_name = list(stage.keys())[0]
            
            # Перевірка на заборонені stages
            if stage_name in self.FORBIDDEN_AGGREGATION_STAGES:
                return False, f"Stage '{stage_name}' заборонений з міркувань безпеки"
            
            # Перевірка на дозволені stages
            if stage_name not in self.ALLOWED_AGGREGATION_STAGES:
                return False, f"Stage '{stage_name}' не дозволений. Дозволені stages: {', '.join(sorted(self.ALLOWED_AGGREGATION_STAGES))}"
            
            # Спеціальна валідація для $group
            if stage_name == '$group':
                is_valid, error = self._validate_group_stage(stage['$group'])
                if not is_valid:
                    return False, f"Помилка в $group stage: {error}"
            
            # Спеціальна валідація для $lookup
            if stage_name == '$lookup':
                is_valid, error = self._validate_lookup_stage(stage['$lookup'])
                if not is_valid:
                    return False, f"Помилка в $lookup stage: {error}"
        
        return True, None
    
    def _validate_group_stage(self, group_spec: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Валідує $group stage.
        
        Args:
            group_spec: Специфікація $group
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        if not isinstance(group_spec, dict):
            return False, "$group має бути словником"
        
        if '_id' not in group_spec:
            return False, "$group має містити поле '_id'"
        
        # Перевіряємо оператори в accumulators
        for field, accumulator in group_spec.items():
            if field == '_id':
                continue
            
            if not isinstance(accumulator, dict):
                return False, f"Accumulator для поля '{field}' має бути словником"
            
            if len(accumulator) != 1:
                return False, f"Accumulator для поля '{field}' має містити рівно один оператор"
            
            operator = list(accumulator.keys())[0]
            
            if operator not in self.ALLOWED_GROUP_OPERATORS:
                return False, f"Оператор '{operator}' не дозволений в $group. Дозволені оператори: {', '.join(sorted(self.ALLOWED_GROUP_OPERATORS))}"
        
        return True, None
    
    def _validate_lookup_stage(self, lookup_spec: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Валідує $lookup stage.
        
        Args:
            lookup_spec: Специфікація $lookup
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        if not isinstance(lookup_spec, dict):
            return False, "$lookup має бути словником"
        
        required_fields = ['from', 'localField', 'foreignField', 'as']
        for field in required_fields:
            if field not in lookup_spec:
                return False, f"$lookup має містити поле '{field}'"
        
        from_collection = lookup_spec['from']
        if from_collection not in self.ALLOWED_COLLECTIONS:
            return False, f"Колекція '{from_collection}' не доступна для $lookup"
        
        return True, None
    
    def execute_aggregation(
        self,
        collection_name: str,
        pipeline: List[Dict[str, Any]],
        limit: Optional[int] = None
    ) -> Dict[str, Any]:
        """
        Виконує MongoDB aggregation pipeline з валідацією.
        
        Args:
            collection_name: Назва колекції
            pipeline: Aggregation pipeline
            limit: Максимальна кількість результатів (опціонально)
            
        Returns:
            Словник з результатами виконання
        """
        # Перевірка колекції
        if collection_name not in self.ALLOWED_COLLECTIONS:
            return {
                'success': False,
                'error': f"Колекція '{collection_name}' не доступна. Дозволені колекції: {', '.join(self.ALLOWED_COLLECTIONS)}"
            }
        
        # Валідація pipeline
        is_valid, error = self.validate_aggregation_pipeline(pipeline)
        if not is_valid:
            return {
                'success': False,
                'error': error
            }
        
        # Глибока копія pipeline та нормалізація дат у $match (для olx_listings updated_at — BSON Date)
        final_pipeline = copy.deepcopy(pipeline)
        self._normalize_aggregation_match_dates(collection_name, final_pipeline)

        # Додаємо limit в кінець pipeline, якщо вказано
        if limit is not None:
            if limit <= 0:
                return {
                    'success': False,
                    'error': "Limit має бути додатнім числом"
                }
            if limit > self.MAX_RESULTS:
                limit = self.MAX_RESULTS
            # Перевіряємо, чи немає вже $limit в pipeline
            has_limit = any('$limit' in stage for stage in final_pipeline)
            if not has_limit:
                final_pipeline.append({'$limit': limit})
        
        # Виконання aggregation
        try:
            collection = self._get_database()[collection_name]
            results = list(collection.aggregate(final_pipeline))
            
            # Обмежуємо кількість результатів
            if limit is None:
                results = results[:self.MAX_RESULTS]
            else:
                results = results[:limit]
            
            # Конвертуємо ObjectId в рядки
            for result in results:
                if '_id' in result:
                    # Якщо _id - це ObjectId, конвертуємо
                    if hasattr(result['_id'], '__str__') and not isinstance(result['_id'], (str, dict, list)):
                        result['_id'] = str(result['_id'])
                # Обробляємо вкладені документи
                self._convert_object_ids(result)
            
            return {
                'success': True,
                'results': results,
                'count': len(results)
            }
        except Exception as e:
            return {
                'success': False,
                'error': f'Помилка виконання aggregation: {str(e)}'
            }

    def get_distinct_values(
        self,
        collection_name: str,
        field_path: str,
        limit: int = 300,
        unwrap_array: bool = False,
    ) -> Dict[str, Any]:
        """
        Повертає список унікальних значень поля в колекції (для аналізу перед фільтрацією).
        field_path — шлях до поля, наприклад "search_data.location" або "detail.llm.tags".
        unwrap_array: якщо True, поле вважається масивом — виконується $unwind, потім $group по елементу;
          використовуй для полів-масивів (наприклад detail.llm.tags), щоб отримати список унікальних тегів.
        """
        if collection_name not in self.ALLOWED_COLLECTIONS:
            return {
                'success': False,
                'error': f"Колекція '{collection_name}' не доступна.",
                'values': [],
            }
        if not field_path or not isinstance(field_path, str):
            return {'success': False, 'error': "field_path обов'язковий (наприклад search_data.location).", 'values': []}
        if not all(c.isalnum() or c in '._' for c in field_path):
            return {'success': False, 'error': "field_path може містити лише літери, цифри, крапку та підкреслення.", 'values': []}
        limit = max(1, min(limit, 500))
        dollar_path = '$' + field_path
        if unwrap_array:
            pipeline = [
                {'$unwind': {'path': dollar_path, 'preserveNullAndEmptyArrays': False}},
                {'$group': {'_id': dollar_path}},
                {'$match': {'_id': {'$nin': [None, '']}}},
                {'$sort': {'_id': 1}},
                {'$limit': limit},
            ]
        else:
            pipeline = [
                {'$group': {'_id': dollar_path}},
                {'$match': {'_id': {'$nin': [None, '']}}},
                {'$sort': {'_id': 1}},
                {'$limit': limit},
            ]
        result = self.execute_aggregation(collection_name, pipeline, limit=limit)
        if not result.get('success'):
            return {'success': False, 'error': result.get('error', ''), 'values': []}
        values = []
        for r in result.get('results') or []:
            vid = r.get('_id')
            if vid is not None and vid != '':
                values.append(vid if isinstance(vid, str) else str(vid))
        return {'success': True, 'values': values, 'count': len(values)}
