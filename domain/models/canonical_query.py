# -*- coding: utf-8 -*-
"""
Canonical Query Model: ізольований від фізичної схеми БД опис запиту.
Агенти повертають тільки логічні поля; маппінг на фізичні шляхи — у FieldMappingService.
"""

from dataclasses import dataclass, field
from typing import Dict, Any, List, Optional


@dataclass
class CanonicalQuery:
    """
    Канонічний опис запиту з логічними полями.
    PipelineInterpreter/FieldMappingService трансформують його у MongoDB pipeline.
    """
    collection: str
    logical_filters: Dict[str, Any]
    sort: List[Dict[str, str]]
    limit: Optional[int]
    response_metrics: List[str]
    sources: List[str] = field(default_factory=list)
    join_metrics: Optional[Dict[str, Any]] = None
    aggregation_needed: bool = False
    aggregation_group_by: List[str] = field(default_factory=list)
    aggregation_metrics: List[Dict[str, Any]] = field(default_factory=list)
    date_range: Optional[Dict[str, Any]] = None

    @classmethod
    def from_query_structure(cls, query_structure: Dict[str, Any]) -> "CanonicalQuery":
        """
        Створює CanonicalQuery з результату QueryStructureAgent.
        date_range витягується з filter_metrics.date або окремого ключа date_range.
        """
        sources = query_structure.get("sources") or []
        collection = sources[0] if sources else "unified_listings"
        filter_metrics = dict(query_structure.get("filter_metrics") or {})

        date_range = query_structure.get("date_range")
        if date_range is None and "date" in filter_metrics:
            date_val = filter_metrics["date"]
            if isinstance(date_val, dict) and ("period" in date_val or "type" in date_val):
                date_range = date_val

        sort_raw = query_structure.get("sort_metrics") or []
        response_raw = query_structure.get("response_metrics") or []

        return cls(
            collection=collection,
            logical_filters=filter_metrics,
            sort=list(sort_raw),
            limit=query_structure.get("limit"),
            response_metrics=list(response_raw),
            sources=sources,
            join_metrics=query_structure.get("join_metrics"),
            aggregation_needed=query_structure.get("aggregation_needed", False),
            aggregation_group_by=query_structure.get("aggregation_group_by") or [],
            aggregation_metrics=query_structure.get("aggregation_metrics") or [],
            date_range=date_range,
        )

    def to_query_structure(self) -> Dict[str, Any]:
        """
        Повертає словник, сумісний з поточним query_structure (для зворотної сумісності).
        """
        return {
            "sources": self.sources,
            "filter_metrics": self.logical_filters,
            "sort_metrics": self.sort,
            "limit": self.limit,
            "response_metrics": self.response_metrics,
            "join_metrics": self.join_metrics,
            "aggregation_needed": self.aggregation_needed,
            "aggregation_group_by": self.aggregation_group_by,
            "aggregation_metrics": self.aggregation_metrics,
            "date_range": self.date_range,
        }
