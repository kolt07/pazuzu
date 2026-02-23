# -*- coding: utf-8 -*-
"""
Агент конструювання пайплайнів обробки даних.
Створює пайплайн у вигляді блок-схеми: кожен крок — одна операція (вибірка, мердж, метрики).
Основним джерелом є зведена таблиця (unified_listings); джерела — лише якщо потрібні поля, що не містяться в зведеній.
Географічні фільтри (топоніми) обробляються через GeoFilterService.
"""

import logging
import json
from typing import Dict, Any, Optional, List, Callable
from config.settings import Settings
from business.services.llm_service import LLMService
from business.services.app_metadata_service import AppMetadataService
from business.services.pipeline_service import UNIFIED_COLLECTION, SOURCE_COLLECTIONS, PIPELINE_COLLECTIONS
from data.repositories.pipeline_repository import PipelineRepository

logger = logging.getLogger(__name__)

# Типи кроків пайплайну (блок-схема)
STEP_TYPES = frozenset((
    "select",      # Вибірка з колекції
    "merge",       # Об'єднання результатів з кількох джерел
    "sum_fields",  # Сумування полів
    "add_metric",  # Додаткові метрики (в т.ч. географічні: відстань до точки)
    "filter",      # Фільтрація (негео — звичайні умови; гео — через GeoFilterService)
    "sort",
    "limit",
))


class PipelineBuilderAgent:
    """
    Агент для конструювання пайплайнів обробки даних.
    Отримує структурний опис запиту та використовує MCP tools для дослідження даних,
    після чого конструює пайплайн у власному форматі.
    """
    
    def __init__(
        self,
        settings: Settings,
        run_tool_fn: Callable[[str, Optional[Dict[str, Any]]], Any]
    ):
        self.settings = settings
        self.run_tool = run_tool_fn
        self._llm_service: Optional[LLMService] = None
        self.metadata_service = AppMetadataService(settings)
        self.pipeline_repo = PipelineRepository()
    
    @property
    def llm_service(self) -> Optional[LLMService]:
        """Лінива ініціалізація LLM сервісу."""
        if self._llm_service is None:
            try:
                self._llm_service = LLMService(self.settings)
            except Exception as e:
                logger.warning("PipelineBuilderAgent: LLM недоступний: %s", e)
        return self._llm_service
    
    def build_pipeline(
        self,
        query_structure: Dict[str, Any],
        user_query: str,
        intent_info: Optional[Dict[str, Any]] = None,
        pipeline_context: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Конструює пайплайн обробки даних.
        
        При наявності pipeline_context (від PipelineService) використовує контекст
        для побудови flowchart-пайплайну з unified_listings як основною колекцією.
        
        Args:
            query_structure: Результат від QueryStructureAgent (extracted_data)
            user_query: Оригінальний запит користувача
            intent_info: Результат від IntentDetectorAgent (опціонально)
            pipeline_context: Контекст від PipelineService (field_structure, caches, methods)
        
        Returns:
            Словник з полями:
            - pipeline: пайплайн у власному форматі (steps з use_geo_filter для вибірок)
            - description: опис пайплайну
            - pipeline_id: ID збереженого пайплайну (якщо збережено)
        """
        # Якщо є контекст — використовуємо його для flowchart-пайплайну
        if pipeline_context:
            return self._build_pipeline_from_context(pipeline_context)
        
        # Спочатку перевіряємо, чи є схожий пайплайн
        similar = self.pipeline_repo.find_similar_pipeline(query_structure)
        if similar:
            logger.info("Знайдено схожий пайплайн: %s", similar.get("_id"))
            # Можна використати існуючий або адаптувати його
            existing_pipeline = similar.get("pipeline", {})
            # Адаптуємо під поточний запит
            adapted = self._adapt_pipeline(existing_pipeline, query_structure)
            return {
                "pipeline": adapted,
                "description": f"Адаптовано з існуючого: {similar.get('description', '')}",
                "pipeline_id": similar.get("_id"),
                "from_cache": True
            }
        
        # Досліджуємо дані через MCP tools
        exploration_results = self._explore_data(query_structure)
        
        # Конструюємо пайплайн через LLM або правила
        if self.llm_service:
            pipeline = self._build_pipeline_with_llm(
                query_structure,
                exploration_results,
                user_query,
                intent_info
            )
        else:
            pipeline = self._build_pipeline_with_rules(
                query_structure,
                exploration_results
            )
        
        # Зберігаємо пайплайн
        description = self._generate_description(query_structure, user_query)
        try:
            pipeline_id = self.pipeline_repo.save_pipeline(
                pipeline=pipeline,
                description=description,
                query_structure=query_structure,
                metadata={"user_query": user_query, "intent_info": intent_info}
            )
        except Exception as e:
            logger.warning("Не вдалося зберегти пайплайн: %s", e)
            pipeline_id = None
        
        return {
            "pipeline": pipeline,
            "description": description,
            "pipeline_id": pipeline_id,
            "from_cache": False
        }
    
    def _explore_data(self, query_structure: Dict[str, Any]) -> Dict[str, Any]:
        """
        Досліджує дані через MCP tools для кращого розуміння структури.
        
        Returns:
            Результати дослідження (наявність полів, унікальні значення тощо)
        """
        results = {}
        sources = query_structure.get("sources", [])
        
        for source in sources:
            source_results = {}
            
            # Отримуємо інформацію про колекцію
            try:
                collection_info = self.run_tool("get_collection_info", {
                    "collection_name": source
                })
                if collection_info.get("success"):
                    source_results["collection_info"] = collection_info.get("collection", {})
            except Exception as e:
                logger.debug("Помилка отримання інформації про колекцію %s: %s", source, e)
            
            # Перевіряємо наявність ключових полів для фільтрів
            filter_metrics = query_structure.get("filter_metrics", {})
            for field, value in filter_metrics.items():
                if field in ["region", "city"]:
                    # Отримуємо унікальні значення
                    field_path = self._get_field_path_for_filter(source, field)
                    if field_path:
                        try:
                            distinct = self.run_tool("get_distinct_values", {
                                "collection_name": source,
                                "field_path": field_path,
                                "limit": 100
                            })
                            if distinct.get("success"):
                                source_results[f"{field}_values"] = distinct.get("values", [])
                        except Exception as e:
                            logger.debug("Помилка отримання унікальних значень для %s.%s: %s", source, field_path, e)
            
            results[source] = source_results
        
        return results
    
    def _get_field_path_for_filter(self, collection: str, field: str) -> Optional[str]:
        """Повертає шлях до поля для фільтрації залежно від колекції. Агент працює з addresses (unified_listings)."""
        if collection != "unified_listings":
            return None
        if field == "region":
            return "addresses.region"
        if field == "city":
            return "addresses.settlement"
        return None
    
    def _build_pipeline_with_llm(
        self,
        query_structure: Dict[str, Any],
        exploration_results: Dict[str, Any],
        user_query: str,
        intent_info: Optional[Dict[str, Any]]
    ) -> Dict[str, Any]:
        """Конструює пайплайн через LLM."""
        prompt = self._build_pipeline_prompt(
            query_structure,
            exploration_results,
            user_query,
            intent_info
        )
        
        try:
            response_text = self.llm_service.generate_text(
                prompt=prompt,
                system_prompt=None,
                temperature=0.3
            )
            
            # Парсимо відповідь
            pipeline = self._parse_pipeline_response(response_text)
            
            # Валідуємо та нормалізуємо
            pipeline = self._validate_pipeline(pipeline, query_structure)
            
            return pipeline
            
        except Exception as e:
            logger.exception("Помилка конструювання пайплайну через LLM: %s", e)
            return self._build_pipeline_with_rules(query_structure, exploration_results)
    
    def _build_pipeline_prompt(
        self,
        query_structure: Dict[str, Any],
        exploration_results: Dict[str, Any],
        user_query: str,
        intent_info: Optional[Dict[str, Any]]
    ) -> str:
        """Формує промпт для конструювання пайплайну."""
        metadata_summary = self.metadata_service.get_metadata_for_llm(max_length=1500)
        
        prompt_parts = [
            "Тобі необхідно сконструювати пайплайн обробки даних у форматі JSON на основі структурного опису запиту.",
            "",
            "## Контекст застосунку:",
            metadata_summary,
            "",
            "## Запит користувача:",
            user_query,
            "",
            "## Структурний опис запиту:",
            json.dumps(query_structure, ensure_ascii=False, indent=2),
            "",
            "## Результати дослідження даних:",
            json.dumps(exploration_results, ensure_ascii=False, indent=2),
        ]
        
        if intent_info:
            prompt_parts.extend([
                "",
                "## Визначений намір:",
                json.dumps(intent_info, ensure_ascii=False, indent=2)
            ])
        
        prompt_parts.extend([
            "",
            "## Закріплення завдання — формат пайплайну:",
            "Пайплайн має бути ПАРАМЕТРИЗОВАНИМ ШАБЛОНОМ у форматі JSON з полем 'steps' (масив кроків).",
            "ВАЖЛИВО: Пайплайн НЕ має містити конкретні значення (регіони, міста, дати), а використовувати ПАРАМЕТРИ.",
            "",
            "Параметри позначаються як $param_name або {{param_name}}.",
            "Доступні параметри:",
            "- $collection або {{collection}} - джерело даних (unified_listings, prozorro_auctions, olx_listings)",
            "- $region або {{region}} - регіон для фільтрації",
            "- $city або {{city}} - місто для фільтрації",
            "- $date_from або {{date_from}} - початкова дата",
            "- $date_to або {{date_to}} - кінцева дата",
            "- $sort_field або {{sort_field}} - поле для сортування",
            "- $sort_order або {{sort_order}} - порядок сортування (asc/desc)",
            "- $limit або {{limit}} - обмеження кількості результатів",
            "",
            "Типи кроків:",
            "- filter: фільтрація даних {type: 'filter', collection: '$collection', conditions: {region: '$region', city: '$city'}}",
            "  Для unified_listings використовуй field price -> price_uah, addresses (region, settlement).",
            "  ВАЖЛИВО: Якщо в conditions є і region, і city - це означає OR (або регіон, або місто),",
            "  а не AND! Наприклад, 'Київ та область' = або Київ, або Київська область.",
            "- calculate: розрахунок нових метрик {type: 'calculate', metric: '...', formula: '...'}",
            "- aggregate: ГРУПУВАННЯ з агрегацією метрик — для запитів «середня ціна по областях», «найдорожчий регіон».",
            "  Формат: {type: 'aggregate', group_by: ['region'], metrics: [{field: 'price_per_m2_uah', aggregation: 'avg'}]}",
            "  aggregation: avg|sum|min|max|count. group_by — поля для групування (region, city, property_type).",
            "  metrics — поля для агрегації. Для «найдорожчий регіон» — group_by: ['region'], metrics: [{field: 'price_per_m2_uah', aggregation: 'avg'}], потім sort desc, limit 1.",
            "- join: об'єднання з іншими колекціями {type: 'join', collection: '$collection', on: '...'}",
            "- sort: сортування {type: 'sort', fields: [{field: '$sort_field', order: '$sort_order'}]}",
            "- limit: обмеження кількості {type: 'limit', count: '$limit'}",
            "- save_temp: збереження в тимчасову колекцію {type: 'save_temp', temp_id: '...'}",
            "- load_temp: завантаження з тимчасової колекції {type: 'load_temp', temp_id: '...'}",
            "",
            "Приклад ПАРАМЕТРИЗОВАНОГО пайплайну:",
            json.dumps({
                "steps": [
                    {
                        "type": "filter",
                        "collection": "$collection",
                        "conditions": {
                            "region": "$region",
                            "city": "$city"
                        }
                    },
                    {
                        "type": "sort",
                        "fields": [{"field": "$sort_field", "order": "$sort_order"}]
                    },
                    {
                        "type": "limit",
                        "count": "$limit"
                    }
                ]
            }, ensure_ascii=False, indent=2),
            "",
            "Поверни тільки JSON з ПАРАМЕТРИЗОВАНИМ пайплайном без коментарів."
        ])
        
        return "\n".join(prompt_parts)
    
    def _parse_pipeline_response(self, response_text: str) -> Dict[str, Any]:
        """Парсить відповідь LLM у формат пайплайну."""
        response_text = response_text.strip()
        
        # Шукаємо JSON блок
        start_idx = response_text.find("{")
        end_idx = response_text.rfind("}")
        
        if start_idx == -1 or end_idx == -1 or end_idx <= start_idx:
            logger.warning("Не знайдено JSON у відповіді LLM: %s", response_text[:200])
            return {"steps": []}
        
        json_text = response_text[start_idx:end_idx + 1]
        
        try:
            result = json.loads(json_text)
            if "steps" not in result:
                result = {"steps": [result]} if result else {"steps": []}
            return result
        except json.JSONDecodeError as e:
            logger.warning("Помилка парсингу JSON: %s. Текст: %s", e, json_text[:200])
            return {"steps": []}
    
    def _validate_pipeline(
        self,
        pipeline: Dict[str, Any],
        query_structure: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Валідує та нормалізує пайплайн."""
        if "steps" not in pipeline:
            pipeline["steps"] = []
        
        # Перевіряємо кожен крок
        validated_steps = []
        has_filter = False
        sources = query_structure.get("sources", [])
        filter_metrics = query_structure.get("filter_metrics", {})
        
        for step in pipeline["steps"]:
            if not isinstance(step, dict) or "type" not in step:
                continue
            
            step_type = step["type"]
            
            # Валідуємо залежно від типу
            if step_type == "filter":
                validated = self._validate_filter_step(step, query_structure)
                if validated:
                    validated_steps.append(validated)
                    has_filter = True
            elif step_type == "calculate":
                validated = self._validate_calculate_step(step)
                if validated:
                    validated_steps.append(validated)
            elif step_type == "aggregate":
                validated = self._validate_aggregate_step(step)
                if validated:
                    validated_steps.append(validated)
            elif step_type in ["sort", "limit", "join", "save_temp", "load_temp"]:
                validated_steps.append(step)
        
        # Якщо є фільтри в query_structure, але немає кроку filter - додаємо його
        if filter_metrics and not has_filter:
            # Для кожного джерела додаємо filter крок з параметрами
            if len(sources) == 1:
                # Один джерело - додаємо один filter крок
                validated_steps.insert(0, {
                    "type": "filter",
                    "collection": "$collection",
                    "conditions": {
                        key: f"${key}" if key in ["region", "city"] else value
                        for key, value in filter_metrics.items()
                    }
                })
            else:
                # Кілька джерел - filter буде застосовано для кожного окремо
                # Додаємо filter крок з параметрами
                validated_steps.insert(0, {
                    "type": "filter",
                    "collection": "$collection",
                    "conditions": {
                        key: f"${key}" if key in ["region", "city"] else value
                        for key, value in filter_metrics.items()
                    }
                })
            logger.info("PipelineBuilderAgent: Додано відсутній крок filter з параметрами")
        
        pipeline["steps"] = validated_steps
        
        # Додаємо обмеження, якщо вказано в query_structure
        if query_structure.get("limit") and not any(s.get("type") == "limit" for s in validated_steps):
            validated_steps.append({
                "type": "limit",
                "count": "$limit" if query_structure.get("limit") else query_structure.get("limit")
            })
            pipeline["steps"] = validated_steps
        
        return pipeline
    
    def _validate_filter_step(
        self,
        step: Dict[str, Any],
        query_structure: Dict[str, Any]
    ) -> Optional[Dict[str, Any]]:
        """Валідує крок фільтрації."""
        collection = step.get("collection")
        # Якщо collection - це параметр ($collection), залишаємо як є
        if collection and (collection.startswith("$") or collection.startswith("{{")):
            # Це параметр, залишаємо
            pass
        elif not collection or collection not in ["unified_listings", "prozorro_auctions", "olx_listings"]:
            # Якщо collection не вказано або невалідний, використовуємо параметр
            collection = "$collection"
        
        conditions = step.get("conditions", {})
        if not conditions:
            # Використовуємо фільтри з query_structure та параметризуємо їх
            filter_metrics = query_structure.get("filter_metrics", {})
            if filter_metrics:
                # Параметризуємо фільтри
                conditions = {
                    key: f"${key}" if key in ["region", "city"] else value
                    for key, value in filter_metrics.items()
                }
        else:
            # Перевіряємо, чи умови вже параметризовані
            param_conditions = {}
            for key, value in conditions.items():
                if isinstance(value, str) and (value.startswith("$") or value.startswith("{{")):
                    # Вже параметр
                    param_conditions[key] = value
                elif key in ["region", "city"]:
                    # Параметризуємо
                    param_conditions[key] = f"${key}"
                else:
                    param_conditions[key] = value
            conditions = param_conditions
        
        validated = {
            "type": "filter",
            "collection": collection,
            "conditions": conditions
        }
        if step.get("use_geo_filter") is not None:
            validated["use_geo_filter"] = step["use_geo_filter"]
        return validated
    
    def _validate_calculate_step(self, step: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Валідує крок розрахунку."""
        metric = step.get("metric")
        formula = step.get("formula")
        
        if not metric or not formula:
            return None
        
        return {
            "type": "calculate",
            "metric": metric,
            "formula": formula
        }
    
    def _validate_aggregate_step(self, step: Dict[str, Any]) -> Optional[Dict[str, Any]]:
        """Валідує крок агрегації. metrics: [{field, aggregation}] для avg, sum, min, max, count."""
        group_by = step.get("group_by", [])
        metrics = step.get("metrics", [])
        
        if not group_by:
            return None

        metrics_ok = []
        for m in metrics if isinstance(metrics, list) else []:
            if isinstance(m, dict) and m.get("field"):
                metrics_ok.append({
                    "field": m["field"],
                    "aggregation": m.get("aggregation", "avg").lower()
                })
            elif isinstance(m, str):
                metrics_ok.append({"field": m, "aggregation": "avg"})
        if not metrics_ok:
            # За замовчуванням — avg для price_per_m2
            metrics_ok = [{"field": "price_per_m2_uah", "aggregation": "avg"}]

        return {
            "type": "aggregate",
            "group_by": group_by if isinstance(group_by, list) else [],
            "metrics": metrics_ok
        }
    
    def _build_pipeline_with_rules(
        self,
        query_structure: Dict[str, Any],
        exploration_results: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Конструює пайплайн на основі правил (fallback без LLM)."""
        steps = []
        sources = query_structure.get("sources", [])
        filter_metrics = query_structure.get("filter_metrics", {})
        sort_metrics = query_structure.get("sort_metrics", [])
        limit = query_structure.get("limit")
        
        # Для кожного джерела створюємо окремий пайплайн або об'єднуємо
        if len(sources) == 1:
            # Один джерело - простий пайплайн
            collection = sources[0]
            
            # Крок фільтрації
            if filter_metrics:
                steps.append({
                    "type": "filter",
                    "collection": collection,
                    "conditions": filter_metrics
                })
            
            # Крок сортування
            if sort_metrics:
                steps.append({
                    "type": "sort",
                    "fields": sort_metrics
                })
            
            # Обмеження
            if limit:
                steps.append({
                    "type": "limit",
                    "count": limit
                })
        else:
            # Кілька джерел - потрібно об'єднати
            # Спочатку обробляємо кожне джерело окремо, потім об'єднуємо
            for collection in sources:
                temp_id = f"temp_{collection}"
                
                # Фільтрація для кожного джерела
                if filter_metrics:
                    steps.append({
                        "type": "filter",
                        "collection": collection,
                        "conditions": filter_metrics
                    })
                
                # Збереження в тимчасову колекцію
                steps.append({
                    "type": "save_temp",
                    "temp_id": temp_id
                })
            
            # Об'єднання тимчасових колекцій
            steps.append({
                "type": "join",
                "temp_ids": [f"temp_{s}" for s in sources]
            })
            
            # Сортування та обмеження після об'єднання
            if sort_metrics:
                steps.append({
                    "type": "sort",
                    "fields": sort_metrics
                })
            
            if limit:
                steps.append({
                    "type": "limit",
                    "count": limit
                })
        
        return {"steps": steps}
    
    def _build_pipeline_from_context(
        self,
        context: Dict[str, Any],
    ) -> Dict[str, Any]:
        """
        Створює пайплайн у форматі блок-схеми з контексту PipelineService.
        Основна колекція — unified_listings; джерела — лише якщо потрібні поля поза зведеною.
        Кроки вибірки з географією позначають use_geo_filter=True.
        """
        extracted_data = context.get("extracted_data", {})
        user_query = context.get("user_query", "")
        intent_info = context.get("intent_info", {})
        field_structure = context.get("field_structure", {})
        collection_methods = context.get("collection_manager_methods", {})
        main_collection = context.get("main_collection", UNIFIED_COLLECTION)
        
        sources = extracted_data.get("sources", [main_collection])
        filter_metrics = extracted_data.get("filter_metrics", {})
        sort_metrics = extracted_data.get("sort_metrics", [])
        limit = extracted_data.get("limit")
        response_metrics = extracted_data.get("response_metrics", [])
        
        # Основна колекція — unified_listings; джерела лише якщо явно вказані і потрібні
        steps = []
        has_geo_filters = any(k in ("region", "city") for k in filter_metrics.keys())
        
        # Визначаємо колекцію: за замовчуванням unified_listings; або з sources якщо в PIPELINE_COLLECTIONS
        collection = main_collection
        if sources and sources[0] in PIPELINE_COLLECTIONS:
            collection = sources[0]
        
        filter_step = {
            "type": "filter",
            "collection": collection if len(sources) == 1 else "$collection",
            "conditions": {
                k: f"${k}" if k in ("region", "city") else v
                for k, v in filter_metrics.items()
            },
            "use_geo_filter": has_geo_filters,
        }
        if filter_metrics:
            steps.append(filter_step)
        
        # Крок 2: Агрегація (group_by + metrics) — для «середня ціна по областях», «найдорожчий регіон»
        aggregation_needed = extracted_data.get("aggregation_needed", False)
        aggregation_group_by = extracted_data.get("aggregation_group_by", [])
        aggregation_metrics = extracted_data.get("aggregation_metrics", [])
        if aggregation_needed and aggregation_group_by:
            agg_metrics = aggregation_metrics if aggregation_metrics else [{"field": "price_per_m2_uah", "aggregation": "avg"}]
            steps.append({
                "type": "aggregate",
                "group_by": aggregation_group_by,
                "metrics": agg_metrics,
            })
        
        # Крок 3: Додаткові метрики (add_metric) — напр. відстань до центру міста
        if "відстань" in user_query.lower() or "distance" in user_query.lower():
            steps.append({
                "type": "add_metric",
                "metric": "distance_km",
                "reference_point": "$center_address",
                "description": "Відстань до референсної точки (км)",
            })
        
        # Крок 4: Сортування
        if sort_metrics:
            steps.append({
                "type": "sort",
                "fields": sort_metrics,
            })
        
        # Крок 5: Обмеження
        if limit:
            steps.append({
                "type": "limit",
                "count": "$limit" if isinstance(limit, str) and "limit" in limit else limit,
            })
        
        pipeline = {"steps": steps}
        
        # Валідація та збереження
        pipeline = self._validate_pipeline(pipeline, extracted_data)
        description = self._generate_description(extracted_data, user_query)
        try:
            pipeline_id = self.pipeline_repo.save_pipeline(
                pipeline=pipeline,
                description=description,
                query_structure=extracted_data,
                metadata={"user_query": user_query, "intent_info": intent_info},
            )
        except Exception as e:
            logger.warning("Не вдалося зберегти пайплайн: %s", e)
            pipeline_id = None
        
        return {
            "pipeline": pipeline,
            "description": description,
            "pipeline_id": pipeline_id,
            "from_cache": False,
        }
    
    def _adapt_pipeline(
        self,
        existing_pipeline: Dict[str, Any],
        query_structure: Dict[str, Any]
    ) -> Dict[str, Any]:
        """Адаптує параметризований пайплайн під новий запит - підставляє параметри."""
        # Пайплайн вже параметризований, просто повертаємо його
        # Параметри будуть підставлені при виконанні в PipelineInterpreterService
        return existing_pipeline
    
    def _generate_description(
        self,
        query_structure: Dict[str, Any],
        user_query: str
    ) -> str:
        """Генерує опис пайплайну."""
        sources = query_structure.get("sources", [])
        sources_str = ", ".join(sources)
        
        filter_metrics = query_structure.get("filter_metrics", {})
        filters_str = ", ".join(filter_metrics.keys()) if filter_metrics else "без фільтрів"
        
        return f"Пайплайн для {sources_str} з фільтрами: {filters_str}"
