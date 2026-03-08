# -*- coding: utf-8 -*-
"""
Репозиторій для збереження та пошуку пайплайнів обробки даних.
"""

import logging
from typing import Dict, Any, Optional, List
from datetime import datetime, timezone
from data.repositories.base_repository import BaseRepository

logger = logging.getLogger(__name__)


class PipelineRepository(BaseRepository):
    """
    Репозиторій для роботи з пайплайнами обробки даних.
    Зберігає пайплайни в колекції pipeline_templates для повторного використання.
    """
    
    def __init__(self):
        super().__init__("pipeline_templates")
    
    def save_pipeline(
        self,
        pipeline: Dict[str, Any],
        description: str,
        query_structure: Optional[Dict[str, Any]] = None,
        metadata: Optional[Dict[str, Any]] = None
    ) -> str:
        """
        Зберігає пайплайн у колекцію.
        
        Args:
            pipeline: Пайплайн у власному форматі
            description: Опис пайплайну
            query_structure: Структурні елементи запиту (для пошуку)
            metadata: Додаткові метаданні
        
        Returns:
            ID збереженого пайплайну
        """
        # Зберігаємо структуру для пошуку (без конкретних значень)
        structure_for_search = {}
        if query_structure:
            structure_for_search["sources"] = query_structure.get("sources", [])
            structure_for_search["response_metrics"] = query_structure.get("response_metrics", [])
            # Зберігаємо тільки ключі фільтрів, не значення
            filter_metrics = query_structure.get("filter_metrics", {})
            if filter_metrics:
                structure_for_search["filter_keys"] = sorted(filter_metrics.keys())
            # Зберігаємо поля сортування
            sort_metrics = query_structure.get("sort_metrics", [])
            if sort_metrics:
                structure_for_search["sort_fields"] = [
                    s.get("field") for s in sort_metrics 
                    if isinstance(s, dict) and "field" in s
                ]
        
        doc = {
            "pipeline": pipeline,  # Параметризований пайплайн
            "description": description,
            "query_structure": query_structure or {},  # Повна структура для довідки
            "structure_for_search": structure_for_search,  # Структура для пошуку (без значень)
            "metadata": metadata or {},
            "created_at": datetime.now(timezone.utc),
            "updated_at": datetime.now(timezone.utc),
            "usage_count": 0,
            "last_used_at": None
        }
        
        try:
            result = self.collection.insert_one(doc)
            logger.info("Збережено пайплайн: %s", result.inserted_id)
            return str(result.inserted_id)
        except Exception as e:
            logger.exception("Помилка збереження пайплайну: %s", e)
            raise
    
    def find_similar_pipeline(
        self,
        query_structure: Dict[str, Any],
        threshold: float = 0.7
    ) -> Optional[Dict[str, Any]]:
        """
        Знаходить схожий пайплайн за структурними елементами запиту.
        Шукає за СТРУКТУРОЮ (джерела, типи фільтрів, метрики), а не за конкретними значеннями.
        
        Args:
            query_structure: Структурні елементи запиту для пошуку
            threshold: Поріг схожості (0-1)
        
        Returns:
            Знайдений пайплайн або None
        """
        try:
            # Шукаємо за структурою, а не за конкретними значеннями
            sources = query_structure.get("sources", [])
            response_metrics = query_structure.get("response_metrics", [])
            filter_metrics = query_structure.get("filter_metrics", {})
            sort_metrics = query_structure.get("sort_metrics", [])
            
            # Будуємо запит за структурою (джерела, типи фільтрів, метрики)
            query = {}
            
            # Джерела - точне співпадіння
            if sources:
                query["structure_for_search.sources"] = {
                    "$all": sources,
                    "$size": len(sources)
                }
            
            # Типи метрик для відповіді (не значення, а типи)
            if response_metrics:
                query["structure_for_search.response_metrics"] = {
                    "$all": response_metrics if isinstance(response_metrics, list) else [response_metrics],
                    "$size": len(response_metrics) if isinstance(response_metrics, list) else 1
                }
            
            # Типи фільтрів (ключі, не значення) - шукаємо пайплайни з такими ж типами фільтрів
            if filter_metrics:
                filter_keys = sorted(filter_metrics.keys())
                query["structure_for_search.filter_keys"] = filter_keys
            
            # Типи сортування
            if sort_metrics:
                sort_fields = [s.get("field") for s in sort_metrics if isinstance(s, dict)]
                if sort_fields:
                    query["structure_for_search.sort_fields"] = {"$in": sort_fields}
            
            # Сортуємо за кількістю використань (найбільш використовувані спочатку)
            candidates = list(self.collection.find(
                query,
                sort=[("usage_count", -1), ("created_at", -1)]
            ).limit(10))
            
            if candidates:
                # Беремо найбільш використовуваний
                best_match = candidates[0]
                best_match["_id"] = str(best_match["_id"])
                logger.info("Знайдено схожий пайплайн за структурою: %s", best_match["_id"])
                return best_match
            
            return None
            
        except Exception as e:
            logger.exception("Помилка пошуку пайплайну: %s", e)
            return None
    
    def get_pipeline(self, pipeline_id: str) -> Optional[Dict[str, Any]]:
        """
        Отримує пайплайн за ID.
        
        Args:
            pipeline_id: ID пайплайну
        
        Returns:
            Пайплайн або None
        """
        try:
            from bson import ObjectId
            doc = self.collection.find_one({"_id": ObjectId(pipeline_id)})
            if doc:
                doc["_id"] = str(doc["_id"])
                return doc
            return None
        except Exception as e:
            logger.exception("Помилка отримання пайплайну: %s", e)
            return None
    
    def increment_usage(self, pipeline_id: str) -> None:
        """
        Збільшує лічильник використань пайплайну.
        
        Args:
            pipeline_id: ID пайплайну
        """
        try:
            from bson import ObjectId
            self.collection.update_one(
                {"_id": ObjectId(pipeline_id)},
                {
                    "$inc": {"usage_count": 1},
                    "$set": {"last_used_at": datetime.now(timezone.utc)}
                }
            )
        except Exception as e:
            logger.exception("Помилка оновлення лічильника використань: %s", e)
    
    def clear_cache(self) -> int:
        """
        Очищає кеш пайплайнів — видаляє всі записи з pipeline_templates.
        Викликати після зміни структури даних або логіки пайплайнів.
        
        Returns:
            Кількість видалених записів
        """
        try:
            result = self.collection.delete_many({})
            count = result.deleted_count
            logger.info("Очищено кеш пайплайнів: %s записів", count)
            return count
        except Exception as e:
            logger.exception("Помилка очищення кешу пайплайнів: %s", e)
            return 0

    def list_pipelines(
        self,
        limit: int = 50,
        skip: int = 0
    ) -> List[Dict[str, Any]]:
        """
        Повертає список пайплайнів.
        
        Args:
            limit: Максимальна кількість результатів
            skip: Кількість пропущених результатів
        
        Returns:
            Список пайплайнів
        """
        try:
            cursor = self.collection.find().sort("created_at", -1).skip(skip).limit(limit)
            pipelines = []
            for doc in cursor:
                doc["_id"] = str(doc["_id"])
                pipelines.append(doc)
            return pipelines
        except Exception as e:
            logger.exception("Помилка отримання списку пайплайнів: %s", e)
            return []
