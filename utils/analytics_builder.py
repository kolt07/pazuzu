# -*- coding: utf-8 -*-
"""
Модуль для побудови aggregation pipeline для аналітичних запитів.
"""

from typing import Dict, Any, List, Optional, Tuple
from datetime import datetime
from data.database.connection import MongoDBConnection
from utils.analytics_metrics import AnalyticsMetrics, MetricDefinition
from utils.query_builder import QueryBuilder


class AnalyticsBuilder:
    """Клас для побудови aggregation pipeline для аналітичних запитів."""
    
    def __init__(self):
        """Ініціалізація білдера."""
        self.db = None
        self.query_builder = QueryBuilder()
    
    def _get_database(self):
        """Отримує об'єкт бази даних."""
        if self.db is None:
            self.db = MongoDBConnection.get_database()
        return self.db
    
    def validate_analytics_query(self, query: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
        """
        Валідує аналітичний запит.
        
        Args:
            query: Аналітичний запит
            
        Returns:
            Кортеж (is_valid, error_message)
        """
        # Перевірка обов'язкових полів
        if 'metric' not in query:
            return False, "Поле 'metric' є обов'язковим"
        
        metric_name = query['metric']
        
        # Перевірка метрики
        if not AnalyticsMetrics.is_valid_metric(metric_name):
            available_metrics = [m['name'] for m in AnalyticsMetrics.list_metrics()]
            return False, f"Метрика '{metric_name}' не існує. Доступні метрики: {', '.join(available_metrics)}"
        
        # Перевірка groupBy
        if 'groupBy' in query:
            if not isinstance(query['groupBy'], list):
                return False, "Поле 'groupBy' має бути списком"
            
            for field in query['groupBy']:
                if not AnalyticsMetrics.is_valid_group_by(field):
                    available_fields = AnalyticsMetrics.ALLOWED_GROUP_BY_FIELDS
                    return False, f"Поле '{field}' не дозволено для групування. Дозволені поля: {', '.join(available_fields)}"
        
        # Перевірка фільтрів
        if 'filters' in query:
            is_valid, error = self.query_builder._validate_filters(query['filters'], depth=0)
            if not is_valid:
                return False, error
        
        # Перевірка діапазонів дат
        if 'filters' in query:
            filters = query['filters']
            for key, value in filters.items():
                if isinstance(value, dict) and 'from' in value and 'to' in value:
                    # Перевірка формату дат
                    try:
                        datetime.fromisoformat(value['from'])
                        datetime.fromisoformat(value['to'])
                    except (ValueError, TypeError):
                        return False, f"Невірний формат дати в полі '{key}'. Використовуйте формат ISO (YYYY-MM-DD)"
        
        return True, None
    
    def build_pipeline(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Будує aggregation pipeline для аналітичного запиту.
        
        Args:
            query: Аналітичний запит
            
        Returns:
            Aggregation pipeline
        """
        pipeline = []
        metric_name = query['metric']
        metric = AnalyticsMetrics.get_metric(metric_name)
        group_by = query.get('groupBy', [])
        filters = query.get('filters', {})
        
        # Перевіряємо, чи потрібен lookup для llm_cache
        needs_llm_lookup = False
        
        # Метрики, що потребують llm_cache
        metrics_needing_llm = ['average_price_per_m2', 'area', 'building_area', 'land_area']
        
        if metric:
            # Перевіряємо required_fields та назву метрики
            needs_llm_lookup = (
                any(field in ['building_area', 'land_area', 'region', 'city', 'property_type'] 
                    for field in metric.required_fields + group_by) or
                metric_name in metrics_needing_llm
            )
        
        # Перевіряємо фільтри на наявність полів, що потребують llm_cache
        if filters:
            filter_keys = list(filters.keys()) if isinstance(filters, dict) else []
            if any(key in ['region', 'city', 'property_type'] for key in filter_keys):
                needs_llm_lookup = True
        
        # Для метрики count з фільтрами за region/city/property_type також потрібен lookup
        if metric_name == 'count' and filters:
            filter_keys = list(filters.keys()) if isinstance(filters, dict) else []
            if any(key in ['region', 'city', 'property_type'] for key in filter_keys):
                needs_llm_lookup = True
        
        # 1. Match stage для фільтрів (тільки ті, що не потребують llm_cache)
        if filters:
            match_filters = self._build_match_filters(filters, skip_llm_fields=True)
            if match_filters:
                pipeline.append({'$match': match_filters})
        
        # 2. Lookup для llm_cache, якщо потрібно
        if needs_llm_lookup:
            pipeline.append({
                '$lookup': {
                    'from': 'llm_cache',
                    'localField': 'description_hash',
                    'foreignField': 'description_hash',
                    'as': 'llm_result_array'
                }
            })
            pipeline.append({
                '$unwind': {
                    'path': '$llm_result_array',
                    'preserveNullAndEmptyArrays': True
                }
            })
            pipeline.append({
                '$addFields': {
                    'llm_result': '$llm_result_array'
                }
            })
            
            # 2.1. Match stage для фільтрів, що потребують llm_cache (після lookup)
            # Також фільтруємо записи, де llm_result існує (для метрик, що потребують llm_cache)
            if filters:
                llm_match_filters = self._build_match_filters(filters, only_llm_fields=True)
                if llm_match_filters:
                    pipeline.append({'$match': llm_match_filters})
            
            # Для метрик, що потребують llm_cache, фільтруємо записи без llm_result
            if metric_name in ['average_price_per_m2', 'area', 'building_area', 'land_area']:
                pipeline.append({
                    '$match': {
                        'llm_result': {'$ne': None},
                        'llm_result.result': {'$exists': True}
                    }
                })
        
        # 3. AddFields для обчислення метрики та полів групування
        add_fields = {}
        
        # Додаємо обчислення метрики
        if metric_name == 'average_price_per_m2':
            # Використовуємо priceFinal (з contracts/awards) або value.amount як fallback
            # Обчислюємо все в одному виразі
            add_fields['_metric_value'] = {
                '$cond': {
                    'if': {
                        '$and': [
                            {'$ne': ['$llm_result.result.building_area_sqm', None]},
                            {'$gt': ['$llm_result.result.building_area_sqm', 0]}
                        ]
                    },
                    'then': {
                        '$divide': [
                            {
                                '$ifNull': [
                                    # Спробуємо отримати з contracts[0].value.amount
                                    {'$let': {
                                        'vars': {
                                            'contract': {'$arrayElemAt': ['$auction_data.contracts', 0]}
                                        },
                                        'in': {
                                            '$cond': {
                                                'if': {
                                                    '$and': [
                                                        {'$ne': ['$$contract', None]},
                                                        {'$ne': ['$$contract.value.amount', None]},
                                                        {'$ne': ['$$contract.value.amount', '']}
                                                    ]
                                                },
                                                'then': {
                                                    '$cond': {
                                                        'if': {
                                                            '$or': [
                                                                {'$eq': ['$$contract.value.amount', '']},
                                                                {'$eq': [{'$type': '$$contract.value.amount'}, 'null']}
                                                            ]
                                                        },
                                                        'then': None,
                                                        'else': {
                                                            '$convert': {
                                                                'input': '$$contract.value.amount',
                                                                'to': 'double',
                                                                'onError': None,
                                                                'onNull': None
                                                            }
                                                        }
                                                    }
                                                },
                                                'else': None
                                            }
                                        }
                                    }},
                                    # Якщо немає contracts, спробуємо awards[0].value.amount
                                    {'$let': {
                                        'vars': {
                                            'award': {'$arrayElemAt': ['$auction_data.awards', 0]}
                                        },
                                        'in': {
                                            '$cond': {
                                                'if': {
                                                    '$and': [
                                                        {'$ne': ['$$award', None]},
                                                        {'$ne': ['$$award.value.amount', None]},
                                                        {'$ne': ['$$award.value.amount', '']}
                                                    ]
                                                },
                                                'then': {
                                                    '$cond': {
                                                        'if': {
                                                            '$or': [
                                                                {'$eq': ['$$award.value.amount', '']},
                                                                {'$eq': [{'$type': '$$award.value.amount'}, 'null']}
                                                            ]
                                                        },
                                                        'then': None,
                                                        'else': {
                                                            '$convert': {
                                                                'input': '$$award.value.amount',
                                                                'to': 'double',
                                                                'onError': None,
                                                                'onNull': None
                                                            }
                                                        }
                                                    }
                                                },
                                                'else': None
                                            }
                                        }
                                    }},
                                    # Якщо немає contracts/awards, використовуємо value.amount
                                    {'$cond': {
                                        'if': {
                                            '$and': [
                                                {'$ne': ['$auction_data.value.amount', None]},
                                                {'$ne': ['$auction_data.value.amount', '']}
                                            ]
                                        },
                                        'then': {
                                            '$convert': {
                                                'input': '$auction_data.value.amount',
                                                'to': 'double',
                                                'onError': None,
                                                'onNull': None
                                            }
                                        },
                                        'else': None
                                    }}
                                ]
                            },
                            {
                                '$cond': {
                                    'if': {
                                        '$or': [
                                            {'$eq': ['$llm_result.result.building_area_sqm', '']},
                                            {'$eq': [{'$type': '$llm_result.result.building_area_sqm'}, 'null']}
                                        ]
                                    },
                                    'then': None,
                                    'else': {
                                        '$convert': {
                                            'input': '$llm_result.result.building_area_sqm',
                                            'to': 'double',
                                            'onError': None,
                                            'onNull': None
                                        }
                                    }
                                }
                            }
                        ]
                    },
                    'else': None
                }
            }
        elif metric_name == 'total_price':
            # Використовуємо priceFinal (з contracts/awards) або value.amount як fallback
            add_fields['_metric_value'] = {
                '$ifNull': [
                    {'$arrayElemAt': ['$auction_data.contracts.value.amount', 0]},
                    {'$ifNull': [
                        {'$arrayElemAt': ['$auction_data.awards.value.amount', 0]},
                        {'$toDouble': '$auction_data.value.amount'}
                    ]}
                ]
            }
        elif metric_name == 'base_price':
            add_fields['_metric_value'] = {
                '$toDouble': '$auction_data.value.amount'
            }
        elif metric_name == 'area':
            add_fields['_metric_value'] = {
                '$toDouble': '$llm_result.result.building_area_sqm'
            }
        elif metric_name == 'building_area':
            add_fields['_metric_value'] = {
                '$toDouble': '$llm_result.result.building_area_sqm'
            }
        elif metric_name == 'land_area':
            add_fields['_metric_value'] = {
                '$multiply': [
                    {'$toDouble': '$llm_result.result.land_area_ha'},
                    10000
                ]
            }
        elif metric_name == 'count':
            # Для count просто рахуємо документи
            add_fields['_metric_value'] = 1
        
        # Додаємо поля для групування
        for field in group_by:
            if field == 'region':
                add_fields[f'_group_{field}'] = {
                    '$ifNull': [
                        {'$arrayElemAt': [
                            '$llm_result.result.addresses.region',
                            0
                        ]},
                        'Unknown'
                    ]
                }
            elif field == 'city':
                add_fields[f'_group_{field}'] = {
                    '$ifNull': [
                        {'$arrayElemAt': [
                            '$llm_result.result.addresses.settlement',
                            0
                        ]},
                        'Unknown'
                    ]
                }
            elif field == 'property_type':
                add_fields[f'_group_{field}'] = {
                    '$arrayElemAt': [
                        {'$ifNull': [
                            '$llm_result.result.property_type',
                            ['Unknown']
                        ]},
                        0
                    ]
                }
            elif field == 'status':
                add_fields[f'_group_{field}'] = '$auction_data.status'
            elif field == 'year':
                add_fields[f'_group_{field}'] = {
                    '$year': {
                        '$dateFromString': {
                            'dateString': '$auction_data.dateModified',
                            'onError': None
                        }
                    }
                }
            elif field == 'month':
                add_fields[f'_group_{field}'] = {
                    '$month': {
                        '$dateFromString': {
                            'dateString': '$auction_data.dateModified',
                            'onError': None
                        }
                    }
                }
            elif field == 'quarter':
                add_fields[f'_group_{field}'] = {
                    '$ceil': {
                        '$divide': [
                            {
                                '$month': {
                                    '$dateFromString': {
                                        'dateString': '$auction_data.dateModified',
                                        'onError': None
                                    }
                                }
                            },
                            3
                        ]
                    }
                }
        
        if add_fields:
            pipeline.append({'$addFields': add_fields})
        
        # 4. Match для фільтрації null значень метрики
        pipeline.append({
            '$match': {
                '_metric_value': {'$ne': None}
            }
        })
        
        # 5. Group stage
        group_id = {}
        for field in group_by:
            group_id[field] = f'$_group_{field}'
        
        if not group_id:
            group_id = None
        
        # Визначаємо функцію агрегації
        if metric_name == 'average_price_per_m2':
            agg_func = '$avg'
        elif metric_name in ['total_price', 'base_price', 'area', 'building_area', 'land_area', 'count']:
            agg_func = '$sum'
        else:
            agg_func = '$avg'
        
        group_stage = {
            '$group': {
                '_id': group_id,
                'value': {agg_func: '$_metric_value'}
            }
        }
        
        # Додаємо поля групування до результату
        if group_by:
            for field in group_by:
                group_stage['$group'][f'{field}'] = {'$first': f'$_group_{field}'}
        
        pipeline.append(group_stage)
        
        # 6. Project stage для форматування результату
        project_fields = {}
        if group_by:
            for field in group_by:
                project_fields[field] = 1
        project_fields['value'] = 1
        project_fields['_id'] = 0
        
        pipeline.append({'$project': project_fields})
        
        # 7. Sort stage
        if group_by:
            sort_fields = {field: 1 for field in group_by}
            pipeline.append({'$sort': sort_fields})
        else:
            pipeline.append({'$sort': {'value': -1}})
        
        return pipeline
    
    def _build_match_filters(self, filters: Dict[str, Any], skip_llm_fields: bool = False, only_llm_fields: bool = False) -> Dict[str, Any]:
        """
        Будує фільтри для match stage з урахуванням діапазонів дат.
        
        Args:
            filters: Фільтри у абстрактному форматі
            
        Returns:
            Фільтри у форматі MongoDB
        """
        mongo_filters = {}
        
        for key, value in filters.items():
            if isinstance(value, dict) and 'from' in value and 'to' in value:
                # Діапазон дат
                from_date = datetime.fromisoformat(value['from'])
                to_date = datetime.fromisoformat(value['to'])
                
                # Визначаємо шлях до поля
                if key == 'dateEnd':
                    field_path = 'auction_data.period.endDate'
                elif key == 'dateStart':
                    field_path = 'auction_data.period.startDate'
                elif key == 'dateModified':
                    field_path = 'auction_data.dateModified'
                elif key == 'dateCreated':
                    field_path = 'auction_data.dateCreated'
                else:
                    field_path = f'auction_data.{key}'
                
                mongo_filters[field_path] = {
                    '$gte': from_date,
                    '$lte': to_date
                }
            elif isinstance(value, dict):
                # Вкладені умови
                mongo_filters[key] = self._build_match_filters(value)
            else:
                # Просте порівняння
                if key == 'status':
                    if not only_llm_fields:
                        # Якщо статус "active", шукаємо всі статуси, що починаються з "active"
                        if value == 'active':
                            mongo_filters['auction_data.status'] = {'$regex': '^active'}
                        else:
                            mongo_filters['auction_data.status'] = value
                elif key == 'region':
                    # Для регіону потрібен lookup з llm_cache
                    if only_llm_fields:
                        # Фільтруємо після lookup - шукаємо в масиві addresses
                        mongo_filters['llm_result.result.addresses'] = {
                            '$elemMatch': {
                                'region': value
                            }
                        }
                elif key == 'city':
                    # Для міста потрібен lookup з llm_cache
                    if only_llm_fields:
                        # Фільтруємо після lookup - шукаємо в масиві addresses
                        mongo_filters['llm_result.result.addresses'] = {
                            '$elemMatch': {
                                'settlement': value
                            }
                        }
                elif key == 'property_type':
                    # Для типу нерухомості потрібен lookup з llm_cache
                    if only_llm_fields:
                        mongo_filters['llm_result.result.property_type'] = value
                else:
                    if not only_llm_fields:
                        mongo_filters[f'auction_data.{key}'] = value
        
        return mongo_filters
    
    def execute_analytics_query(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """
        Виконує аналітичний запит та повертає результати.
        
        Args:
            query: Аналітичний запит
            
        Returns:
            Словник з результатами виконання запиту
        """
        # Валідація
        is_valid, error = self.validate_analytics_query(query)
        if not is_valid:
            return {
                'success': False,
                'error': error
            }
        
        # Побудова pipeline
        pipeline = self.build_pipeline(query)
        
        # Виконання запиту
        try:
            collection = self._get_database()['prozorro_auctions']
            results = list(collection.aggregate(pipeline))
            
            # Форматування результатів
            metric = AnalyticsMetrics.get_metric(query['metric'])
            formatted_results = []
            
            for result in results:
                value = result.get('value')
                # Фільтруємо null значення для метрик, що потребують даних
                if value is None and query['metric'] in ['average_price_per_m2', 'total_price', 'area', 'building_area', 'land_area']:
                    continue
                
                formatted_result = {
                    'value': round(value, 2) if value is not None else 0,
                    'unit': metric.unit if metric else ''
                }
                
                # Додаємо поля групування
                for field in query.get('groupBy', []):
                    formatted_result[field] = result.get(field, 'Unknown')
                
                formatted_results.append(formatted_result)
            
            # Якщо немає результатів, додаємо інформацію про це
            response = {
                'success': True,
                'metric': query['metric'],
                'metric_description': metric.description if metric else '',
                'unit': metric.unit if metric else '',
                'group_by': query.get('groupBy', []),
                'results': formatted_results,
                'count': len(formatted_results)
            }
            
            # Додаємо інформацію про застосовані фільтри
            if query.get('filters'):
                response['filters_applied'] = query['filters']
            
            # Якщо результатів немає, додаємо підказку
            if len(formatted_results) == 0:
                response['message'] = 'Результатів не знайдено. Можливі причини: немає даних, що відповідають фільтрам, або дані не оброблені через LLM (немає description_hash).'
            
            return response
        except Exception as e:
            return {
                'success': False,
                'error': f'Помилка виконання запиту: {str(e)}'
            }
