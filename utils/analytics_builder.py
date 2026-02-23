# -*- coding: utf-8 -*-
"""
Модуль для побудови aggregation pipeline для аналітичних запитів.
Підтримує prozorro_auctions та olx_listings (ціна за м² за регіоном/датою).
"""

from typing import Dict, Any, List, Optional, Tuple, Union
from datetime import datetime
from data.database.connection import MongoDBConnection
from utils.analytics_metrics import AnalyticsMetrics, MetricDefinition
from utils.analytics_formula import (
    formula_to_mongo_expr,
    formula_references_llm,
    FormulaParseError,
)
from utils.query_builder import QueryBuilder


def _is_custom_metric(metric: Any) -> bool:
    """Чи є метрика кастомною (словник з формулою)."""
    return isinstance(metric, dict) and isinstance(metric.get('formula'), str)


def _normalize_metric_spec(query: Dict[str, Any]) -> Tuple[Union[str, Dict], Optional[MetricDefinition], bool, str]:
    """
    Повертає (metric_spec, builtin_metric, is_custom, aggregation).
    metric_spec: для вбудованих — канонічна назва (str), для кастомних — dict.
    aggregation: 'avg' | 'sum' | 'min' | 'max' (для $group).
    """
    raw = query.get('metric')
    if _is_custom_metric(raw):
        agg = (raw.get('aggregation') or 'avg').lower()
        if agg not in ('avg', 'sum', 'min', 'max'):
            agg = 'avg'
        return raw, None, True, agg
    name = AnalyticsMetrics._resolve_metric_name(raw)
    return name, AnalyticsMetrics.get_metric(name), False, (
        'avg' if name == 'average_price_per_m2' else 'sum'
    )


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
        
        collection = query.get('collection', 'prozorro_auctions')
        if collection == 'olx_listings':
            # OLX: лише метрика ціна за м² та groupBy date; фільтри region/city та дата
            raw_metric = query['metric']
            if _is_custom_metric(raw_metric):
                formula = (raw_metric.get('formula') or '').strip()
                if not formula or 'building_area_sqm' not in formula or 'price' not in formula.lower():
                    return False, "Для olx_listings використовуй метрику ціни за м²: metric: { name: 'price_per_sqm', formula: 'search_data.price / detail.llm.building_area_sqm', aggregation: 'avg' } або metric: 'average_price_per_m2'."
            elif not AnalyticsMetrics.is_valid_metric(raw_metric) and AnalyticsMetrics._resolve_metric_name(raw_metric) != 'average_price_per_m2':
                return False, "Для olx_listings підтримується лише метрика average_price_per_m2 (ціна за м²)."
            group_by = query.get('groupBy') or []
            allowed_olx_group = {'date', 'city', 'region'}
            if group_by and not set(group_by).issubset(allowed_olx_group):
                return False, f"Для olx_listings дозволено groupBy: ['date'], ['city'] або ['region']. Отримано: {group_by}"
            if query.get('filters'):
                is_valid, error = self.query_builder._validate_filters(query['filters'], depth=0)
                if not is_valid:
                    return False, error
            return True, None
        
        raw_metric = query['metric']
        if _is_custom_metric(raw_metric):
            formula = raw_metric.get('formula', '').strip()
            if not formula:
                return False, "Кастомна метрика має містити поле 'formula' (вираз над полями auction_data.*, llm_result.result.*)"
            try:
                formula_to_mongo_expr(formula)
            except FormulaParseError as e:
                return False, f"Невірна формула метрики: {e}"
        else:
            if not AnalyticsMetrics.is_valid_metric(raw_metric):
                available = [m['name'] for m in AnalyticsMetrics.list_metrics()]
                return False, f"Метрика '{raw_metric}' не існує. Доступні: {', '.join(available)}. Або задайте кастомну: metric={{ name, formula, aggregation? }}."
        
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
        metric_spec, builtin_metric, is_custom, aggregation = _normalize_metric_spec(query)
        metric_name = metric_spec.get('name', 'custom') if is_custom else metric_spec
        metric = builtin_metric
        group_by = query.get('groupBy', [])
        filters = query.get('filters', {})
        
        # Перевіряємо, чи потрібен lookup для llm_cache
        if is_custom:
            needs_llm_lookup = formula_references_llm(metric_spec['formula'])
        else:
            needs_llm_lookup = False
            metrics_needing_llm = ['average_price_per_m2', 'area', 'building_area', 'land_area']
        
        if not is_custom and metric:
            # Перевіряємо required_fields та назву метрики
            needs_llm_lookup = (
                any(field in ['building_area', 'land_area', 'region', 'city', 'property_type'] 
                    for field in metric.required_fields + group_by) or
                metric_name in metrics_needing_llm
            )
        
        # Перевіряємо фільтри на наявність полів, що потребують llm_cache
        llm_filter_fields = ['region', 'city', 'property_type', 'building_area_sqm', 'land_area_ha', 'building_area', 'land_area']
        if filters:
            filter_keys = list(filters.keys()) if isinstance(filters, dict) else []
            if any(key in llm_filter_fields for key in filter_keys):
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
        if is_custom:
            add_fields['_metric_value'] = formula_to_mongo_expr(metric_spec['formula'])
        elif metric_name == 'average_price_per_m2':
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
            elif field == 'date':
                # День у форматі YYYY-MM-DD (auction_data.dateModified — рядок ISO)
                add_fields[f'_group_{field}'] = {
                    '$substr': [
                        {'$ifNull': ['$auction_data.dateModified', '']},
                        0,
                        10
                    ]
                }
        
        if add_fields:
            pipeline.append({'$addFields': add_fields})
        
        # 4. Match для фільтрації null значень метрики та порожніх дат при groupBy date
        match_metric = {'_metric_value': {'$ne': None}}
        if 'date' in group_by:
            match_metric['_group_date'] = {'$nin': [None, '']}
        pipeline.append({'$match': match_metric})
        
        # 5. Group stage
        group_id = {}
        for field in group_by:
            group_id[field] = f'$_group_{field}'
        
        if not group_id:
            group_id = None
        
        # Визначаємо функцію агрегації
        if is_custom:
            agg_func = f'${aggregation}'
        elif metric_name == 'average_price_per_m2':
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
    
    def _parse_iso_to_datetime(self, value: Any):
        """Повертає datetime з ISO-рядка (з підтримкою суфікса Z)."""
        if value is None:
            return None
        if isinstance(value, datetime):
            return value
        s = str(value).strip().replace('Z', '+00:00')
        try:
            return datetime.fromisoformat(s)
        except (ValueError, TypeError):
            return None
    
    def _build_olx_region_city_match(self, filters: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """З фільтрів (region, city або $or) будує $match для OLX: resolved_locations або search_data.location."""
        or_conditions = []
        if filters.get('$or') and isinstance(filters['$or'], list):
            for item in filters['$or']:
                if not isinstance(item, dict):
                    continue
                region = item.get('region')
                city = item.get('city')
                if region:
                    # resolved_locations[i].results[j].address_structured.region
                    or_conditions.append({
                        'detail.resolved_locations': {
                            '$elemMatch': {'results.address_structured.region': {'$regex': str(region).strip(), '$options': 'i'}}
                        }
                    })
                    or_conditions.append({'search_data.location': {'$regex': str(region).strip(), '$options': 'i'}})
                if city:
                    # resolved_locations[i].results[j].address_structured.city або .settlement
                    or_conditions.append({
                        'detail.resolved_locations': {
                            '$elemMatch': {
                                '$or': [
                                    {'results.address_structured.city': {'$regex': str(city).strip(), '$options': 'i'}},
                                    {'results.address_structured.settlement': {'$regex': str(city).strip(), '$options': 'i'}},
                                ]
                            }
                        }
                    })
                    or_conditions.append({'search_data.location': {'$regex': str(city).strip(), '$options': 'i'}})
        if not or_conditions:
            return None
        return {'$or': or_conditions}
    
    def _build_olx_price_per_sqm_pipeline(self, query: Dict[str, Any]) -> List[Dict[str, Any]]:
        """Будує aggregation pipeline для OLX: середня ціна за м² за днями/містом/регіоном."""
        filters = query.get('filters') or {}
        group_by = query.get('groupBy') or ['date']
        pipeline = []
        
        # 1. Match: діапазон дат (updated_at) — опційно для groupBy city/region
        date_range = filters.get('auction_data.dateModified') or filters.get('updated_at')
        if isinstance(date_range, dict) and '$gte' in date_range and '$lte' in date_range:
            gte = self._parse_iso_to_datetime(date_range['$gte'])
            lte = self._parse_iso_to_datetime(date_range['$lte'])
            if gte and lte:
                pipeline.append({'$match': {'updated_at': {'$gte': gte, '$lte': lte}}})
        
        # 2. Match: регіон/місто (Київ, Київська тощо)
        region_city = self._build_olx_region_city_match(filters)
        if region_city:
            pipeline.append({'$match': region_city})
        # 2b. Match: property_type (Комерційна нерухомість тощо)
        prop_type = filters.get('property_type')
        if prop_type and isinstance(prop_type, str):
            pt = str(prop_type).strip()
            pipeline.append({'$match': {'detail.llm.property_type': pt}})
        
        # 3. AddFields: ціна, площа, ціна за м², _group_city, _group_region
        _safe_double = lambda field: {
            '$convert': {'input': field, 'to': 'double', 'onError': None, 'onNull': None}
        }
        pipeline.append({
            '$addFields': {
                '_price': {'$cond': [
                    {'$and': [
                        {'$ne': ['$search_data.price', None]},
                        {'$ne': ['$search_data.price', '']},
                        {'$gt': [_safe_double('$search_data.price'), 0]}
                    ]},
                    _safe_double('$search_data.price'),
                    {'$cond': [
                        {'$and': [
                            {'$ne': ['$search_data.price_value', None]},
                            {'$ne': ['$search_data.price_value', '']},
                            {'$gt': [_safe_double('$search_data.price_value'), 0]}
                        ]},
                        _safe_double('$search_data.price_value'),
                        None
                    ]}
                ]},
                '_area': {'$cond': [
                    {'$and': [
                        {'$ne': ['$detail.llm.building_area_sqm', None]},
                        {'$ne': ['$detail.llm.building_area_sqm', '']},
                        {'$gt': [_safe_double('$detail.llm.building_area_sqm'), 0]}
                    ]},
                    _safe_double('$detail.llm.building_area_sqm'),
                    None
                ]},
            }
        })
        pipeline.append({
            '$addFields': {
                '_price_per_sqm': {
                    '$cond': {
                        'if': {'$and': [
                            {'$ne': ['$_price', None]},
                            {'$ne': ['$_area', None]},
                            {'$gt': ['$_area', 0]}
                        ]},
                        'then': {'$divide': ['$_price', '$_area']},
                        'else': None
                    }
                }
            }
        })
        pipeline.append({'$match': {'_price_per_sqm': {'$ne': None, '$gt': 0}}})
        
        # 4. AddFields: поля для групування (date, city, region)
        # resolved_locations[i].results[j].address_structured.{city,region,settlement}
        pipeline.append({
            '$addFields': {
                '_group_date': {'$dateToString': {'format': '%Y-%m-%d', 'date': '$updated_at'}},
                '_first_loc': {'$arrayElemAt': [{'$ifNull': ['$detail.resolved_locations', []]}, 0]},
            }
        })
        pipeline.append({
            '$addFields': {
                '_first_result': {'$arrayElemAt': [{'$ifNull': ['$_first_loc.results', []]}, 0]},
            }
        })
        pipeline.append({
            '$addFields': {
                '_group_city': {
                    '$trim': {
                        'input': {
                            '$ifNull': [
                                {'$ifNull': [
                                    '$_first_result.address_structured.city',
                                    '$_first_result.address_structured.settlement',
                                    '$_first_loc.address_structured.city',
                                    '$_first_loc.address_structured.settlement',
                                ]},
                                '$search_data.location',
                                'н/д'
                            ]
                        }
                    }
                },
                '_group_region': {
                    '$trim': {
                        'input': {
                            '$ifNull': [
                                '$_first_result.address_structured.region',
                                '$_first_loc.address_structured.region',
                                'н/д'
                            ]
                        }
                    }
                },
            }
        })
        
        # 5. $group: динамічно за groupBy
        group_id = {}
        if 'date' in group_by:
            group_id['date'] = '$_group_date'
        if 'city' in group_by:
            group_id['city'] = '$_group_city'
        if 'region' in group_by:
            group_id['region'] = '$_group_region'
        if not group_id:
            group_id = {'date': '$_group_date'}
        group_stage = {
            '_id': group_id,
            'value': {'$avg': '$_price_per_sqm'},
            'count': {'$sum': 1},
        }
        for f in ['date', 'city', 'region']:
            if f in group_id:
                group_stage[f] = {'$first': f'$_group_{f}'}
        pipeline.append({'$group': group_stage})
        pipeline.append({'$project': {'_id': 0, 'value': 1, 'count': 1, **{f: 1 for f in group_by if f in ['date', 'city', 'region']}}})
        sort_fields = {f: 1 for f in group_by if f in ['date', 'city', 'region']}
        pipeline.append({'$sort': sort_fields if sort_fields else {'date': 1}})
        return pipeline
    
    def _execute_olx_analytics(self, query: Dict[str, Any]) -> Dict[str, Any]:
        """Виконує аналітику ціни за м² по OLX (olx_listings). Повертає той самий формат, що й execute_analytics для ProZorro."""
        try:
            filters = query.get('filters') or {}
            date_range = filters.get('auction_data.dateModified') or filters.get('updated_at')
            if not isinstance(date_range, dict) or not date_range.get('$gte') or not date_range.get('$lte'):
                return {
                    'success': False,
                    'error': 'Для OLX потрібен діапазон дат у фільтрах: auction_data.dateModified або updated_at з $gte та $lte (ISO).'
                }
            pipeline = self._build_olx_price_per_sqm_pipeline(query)
            if not pipeline:
                return {
                    'success': False,
                    'error': 'Потрібні фільтри: діапазон дат (auction_data.dateModified або updated_at з $gte/$lte) та опційно регіон/місто ($or: [{region, city}]).'
                }
            coll = self._get_database()['olx_listings']
            results = list(coll.aggregate(pipeline))
            group_by = query.get('groupBy', ['date'])
            formatted = []
            for r in results:
                val = round(float(r.get('value', 0)), 2)
                row = {'value': val, 'average_price_per_m2': val, 'unit': 'UAH/m²'}
                if 'date' in group_by:
                    row['date'] = r.get('date', 'Unknown')
                if 'city' in group_by:
                    row['city'] = r.get('city', 'н/д')
                if 'region' in group_by:
                    row['region'] = r.get('region', 'н/д')
                formatted.append(row)
            out = {
                'success': True,
                'metric': query.get('metric') or 'average_price_per_m2',
                'metric_description': 'Середня ціна за м² (OLX)',
                'unit': 'UAH/m²',
                'group_by': group_by,
                'results': formatted,
                'count': len(formatted),
                'data_source': 'olx_listings',
            }
            if len(formatted) == 0:
                out['message'] = (
                    'За обраними критеріями (регіон/дата) в OLX результатів не знайдено. '
                    'Можливі причини: немає оголошень з ціною та площею (detail.llm.building_area_sqm), '
                    'або фільтр регіону/міста не збігається з даними (resolved_locations, search_data.location).'
                )
            return out
        except Exception as e:
            return {
                'success': False,
                'error': f'Помилка аналітики OLX: {str(e)}'
            }
    
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
            # Обробка $or оператора (має бути на верхньому рівні)
            if key == '$or':
                if isinstance(value, list):
                    mongo_filters['$or'] = [self._build_match_filters(item, skip_llm_fields, only_llm_fields) for item in value]
                continue
            elif isinstance(value, dict) and 'from' in value and 'to' in value:
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
                        # Підтримуємо $in для множинних значень
                        if isinstance(value, dict) and '$in' in value:
                            mongo_filters['llm_result.result.addresses'] = {
                                '$elemMatch': {
                                    'region': {'$in': value['$in']}
                                }
                            }
                        else:
                            # Фільтруємо після lookup - шукаємо в масиві addresses
                            mongo_filters['llm_result.result.addresses'] = {
                                '$elemMatch': {
                                    'region': value
                                }
                            }
                elif key == 'city':
                    # Для міста потрібен lookup з llm_cache
                    if only_llm_fields:
                        # Підтримуємо $in для множинних значень
                        if isinstance(value, dict) and '$in' in value:
                            mongo_filters['llm_result.result.addresses'] = {
                                '$elemMatch': {
                                    'settlement': {'$in': value['$in']}
                                }
                            }
                        else:
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
                elif key in ['building_area_sqm', 'land_area_ha', 'building_area', 'land_area']:
                    # Для площі потрібен lookup з llm_cache
                    if only_llm_fields:
                        # Підтримуємо MongoDB оператори ($lte, $gte, $lt, $gt, $eq)
                        if isinstance(value, dict):
                            # Якщо значення - словник з операторами (наприклад, {"$lte": 200})
                            field_name = 'building_area_sqm' if key in ['building_area_sqm', 'building_area'] else 'land_area_ha'
                            mongo_filters[f'llm_result.result.{field_name}'] = value
                        else:
                            # Якщо просте значення, використовуємо $eq
                            field_name = 'building_area_sqm' if key in ['building_area_sqm', 'building_area'] else 'land_area_ha'
                            mongo_filters[f'llm_result.result.{field_name}'] = value
                elif key in ('auction_data.dateModified', 'auction_data.dateCreated'):
                    if not only_llm_fields and isinstance(value, dict):
                        mongo_filters[key] = value
                else:
                    if not only_llm_fields:
                        if key.startswith('auction_data.'):
                            mongo_filters[key] = value
                        else:
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
        
        collection = query.get('collection', 'prozorro_auctions')
        if collection == 'olx_listings':
            return self._execute_olx_analytics(query)
        
        # Побудова pipeline
        pipeline = self.build_pipeline(query)
        
        # Виконання запиту
        try:
            is_custom = _is_custom_metric(query['metric'])
            if is_custom:
                resolved_metric = None
                metric_desc = query['metric'].get('description', '')
                metric_unit = query['metric'].get('unit', '')
                metric_display = query['metric'].get('name', 'custom')
            else:
                resolved_metric = AnalyticsMetrics._resolve_metric_name(query['metric'])
                metric = AnalyticsMetrics.get_metric(query['metric'])
                metric_desc = metric.description if metric else ''
                metric_unit = metric.unit if metric else ''
                metric_display = query['metric']
            collection = self._get_database()['prozorro_auctions']
            results = list(collection.aggregate(pipeline))
            
            formatted_results = []
            for result in results:
                value = result.get('value')
                if value is None and resolved_metric in ('average_price_per_m2', 'total_price', 'area', 'building_area', 'land_area'):
                    continue
                formatted_result = {
                    'value': round(value, 2) if value is not None else 0,
                    'unit': metric_unit
                }
                for field in query.get('groupBy', []):
                    formatted_result[field] = result.get(field, 'Unknown')
                formatted_results.append(formatted_result)
            
            response = {
                'success': True,
                'metric': metric_display,
                'metric_description': metric_desc,
                'unit': metric_unit,
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
