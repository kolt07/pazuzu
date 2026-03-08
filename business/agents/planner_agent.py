# -*- coding: utf-8 -*-
"""
Агент-планувальник: формує пайплайн кроків обробки (вибірки, агрегації, оновлення, експорт у файли)
на основі структурованого наміру від помічника. Кроки відповідають контракту plan_step_schema.
"""

import logging
from datetime import datetime, timezone, timedelta
from typing import Dict, Any, List, Optional, Callable

from business.agents.plan_step_schema import ACTION_BY_STEP
from utils.schema_filter_resolver import resolve_geo_filter, region_filter_to_geo_filter

logger = logging.getLogger(__name__)


def _olx_region_city_match(region_filter: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """З region_filter (region, city) будує $match для OLX через schema-aware resolver."""
    geo = region_filter_to_geo_filter(region_filter)
    return resolve_geo_filter(geo or {}, "olx_listings") if geo else None


def _prozorro_region_city_match(region_filter: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """З region_filter (region, city) будує $match для ProZorro через schema-aware resolver."""
    geo = region_filter_to_geo_filter(region_filter)
    return resolve_geo_filter(geo or {}, "prozorro_auctions") if geo else None


class PlannerAgent:
    """
    Планувальник у мультиагентному пайплайні: отримує структурований намір від агента-помічника
    (після інтерпретатора) і повертає список кроків (вибірка, експорт). Викликається лише для явних
    звітів/експортів за період; вільні запити обробляє LangChain-агент. Виконує кроки не сам — їх
    виконує аналітик (AnalystAgent) через run_tool_fn.
    """

    def __init__(self, run_tool_fn: Optional[Callable[[str, Dict[str, Any]], Any]] = None):
        """
        Args:
            run_tool_fn: Функція (tool_name, tool_args) -> result для виклику інструментів
                        (наприклад дослідження структури перед плануванням).
        """
        self.run_tool_fn = run_tool_fn

    def plan(self, structured_intent: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Повертає пайплайн кроків для виконання (контракт plan_step_schema).

        Кожен крок: {"step": "<type>", "action": "<tool_name>", "params": {...}}.
        Типи: update_data, query, export, get_collections.
        """
        intent = structured_intent.get("intent", "query")
        period_days = structured_intent.get("period_days")
        if intent in ("report_last_day", "report_last_week", "export_data") and period_days is None:
            period_days = 1
        collections = structured_intent.get("collections") or []
        region_filter = structured_intent.get("region_filter")
        base_filters = structured_intent.get("filters") or {}
        need_update_first = structured_intent.get("need_update_first", False)
        steps = []

        if need_update_first:
            steps.append({
                "step": "update_data",
                "action": ACTION_BY_STEP["update_data"],
                "params": {"source": "olx", "days": period_days or 1},
            })
            steps.append({
                "step": "update_data",
                "action": ACTION_BY_STEP["update_data"],
                "params": {"source": "prozorro", "days": None},
            })

        if intent in ("report_last_day", "report_last_week", "export_data") and collections:
            for coll in collections:
                query_params = self._build_save_query_params(
                    coll,
                    period_days or 1,
                    region_filter,
                    base_filters=base_filters,
                )
                steps.append({
                    "step": "query",
                    "action": ACTION_BY_STEP["query"],
                    "params": {"query": query_params},
                })
            for i, coll in enumerate(collections):
                prefix = "prozorro" if coll == "prozorro_auctions" else "olx"
                steps.append({
                    "step": "export",
                    "action": ACTION_BY_STEP["export"],
                    "params": {
                        "format": "xlsx",
                        "filename_prefix": prefix,
                    },
                    "temp_collection_id_from_step": i,
                })
            return steps

        steps.append({
            "step": "get_collections",
            "action": ACTION_BY_STEP["get_collections"],
            "params": {},
        })
        return steps

    def _date_filters(self, collection: str, period_days: int) -> Dict[str, Any]:
        now = datetime.now(timezone.utc)
        start = (now - timedelta(days=period_days)).strftime("%Y-%m-%dT%H:%M:%S.000Z")
        end = now.strftime("%Y-%m-%dT23:59:59.999Z")
        if collection == "prozorro_auctions":
            return {"auction_data.dateModified": {"$gte": start, "$lte": end}}
        if collection == "olx_listings":
            return {"updated_at": {"$gte": start, "$lte": end}}
        return {}

    def _parse_iso_to_datetime(self, s: Any) -> Optional[datetime]:
        """Парсує ISO-рядок у datetime (UTC). Для aggregation pipeline з BSON датами."""
        if not s or not isinstance(s, str):
            return None
        try:
            s = s.strip().replace("Z", "+00:00")
            dt = datetime.fromisoformat(s)
            return dt.astimezone(timezone.utc) if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
        except (ValueError, TypeError):
            return None

    def _build_save_query_params(
        self,
        collection: str,
        period_days: int,
        region_filter: Optional[Dict[str, Any]],
        base_filters: Optional[Dict[str, Any]] = None,
    ) -> Dict[str, Any]:
        """
        Збирає параметри для save_query_to_temp_collection: фільтри за датою та опційно за регіоном.
        Для olx_listings з регіоном — aggregation_pipeline (дата + регіон). Для prozorro з регіоном — join з llm_cache.
        """
        date_filters = self._date_filters(collection, period_days)
        if not date_filters:
            return {"collection": collection, "filters": dict(base_filters or {}), "limit": 5000}

        base_filters = base_filters or {}
        property_type_value = base_filters.get("property_type")

        if collection == "olx_listings" and region_filter:
            now = datetime.now(timezone.utc)
            start = (now - timedelta(days=period_days)).replace(hour=0, minute=0, second=0, microsecond=0)
            end = now
            start_dt = self._parse_iso_to_datetime(
                start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            )
            end_dt = self._parse_iso_to_datetime(
                end.strftime("%Y-%m-%dT23:59:59.999Z")
            )
            if not start_dt or not end_dt:
                merged_filters = dict(date_filters)
                merged_filters.update(base_filters)
                return {"collection": collection, "filters": merged_filters, "limit": 5000}
            pipeline = [{"$match": {"updated_at": {"$gte": start_dt, "$lte": end_dt}}}]
            region_match = _olx_region_city_match(region_filter)
            if region_match:
                pipeline.append({"$match": region_match})
            if property_type_value:
                # Для OLX конкретне поле типу об'єкта може відрізнятися в різних колекціях;
                # тут свідомо НЕ додаємо жорсткий фільтр, щоб уникнути помилок схеми.
                # Фільтрація за типом об'єкта для OLX має відбуватися через окремі інструменти/аналітику.
                pass
            pipeline.append({"$limit": 5000})
            return {
                "collection": collection,
                "aggregation_pipeline": pipeline,
                "limit": 5000,
            }

        if collection == "prozorro_auctions" and (region_filter or property_type_value):
            # Використовуємо aggregation pipeline для підтримки всіх fallback умов через resolve_geo_filter
            now = datetime.now(timezone.utc)
            start = (now - timedelta(days=period_days)).replace(hour=0, minute=0, second=0, microsecond=0)
            end = now
            start_iso = start.strftime("%Y-%m-%dT%H:%M:%S.000Z")
            end_iso = end.strftime("%Y-%m-%dT23:59:59.999Z")
            
            pipeline = [{"$match": {"auction_data.dateModified": {"$gte": start_iso, "$lte": end_iso}}}]
            
            # Якщо потрібен фільтр за типом нерухомості, додаємо join з llm_cache перед гео-фільтрами
            # (щоб fallback на llm_result.result.addresses працював)
            if property_type_value:
                pipeline.append({
                    "$lookup": {
                        "from": "llm_cache",
                        "localField": "description_hash",
                        "foreignField": "description_hash",
                        "as": "llm_result"
                    }
                })
            
            # Додаємо гео-фільтри через resolve_geo_filter (з усіма fallback умовами)
            if region_filter:
                region_match = _prozorro_region_city_match(region_filter)
                if region_match:
                    pipeline.append({"$match": region_match})
            
            # Фільтр за типом нерухомості через llm_cache (якщо потрібно)
            if property_type_value:
                pipeline.append({
                    "$match": {
                        "llm_result.result.property_type": property_type_value
                    }
                })
            
            pipeline.append({"$limit": 5000})
            return {
                "collection": collection,
                "aggregation_pipeline": pipeline,
                "limit": 5000,
            }

        merged_filters = dict(date_filters)
        merged_filters.update(base_filters)
        return {
            "collection": collection,
            "filters": merged_filters,
            "limit": 5000,
        }
