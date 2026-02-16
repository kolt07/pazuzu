# -*- coding: utf-8 -*-
"""
Сервіс дослідження даних: автоматичне профілювання колекцій (статистика по полях,
топ повторюваних значень, середні для числових полів) та надання згуртованого
контексту для агента.
"""

import logging
from collections import Counter
from typing import Dict, Any, List, Optional, Tuple

from data.database.connection import MongoDBConnection
from data.repositories.collection_knowledge_repository import CollectionKnowledgeRepository

logger = logging.getLogger(__name__)

# Колекції, які профілюємо; відповідність logical name -> mongo collection
PROFILE_COLLECTIONS = ["unified_listings", "prozorro_auctions", "olx_listings", "llm_cache"]
DEFAULT_SAMPLE_SIZE = 5000
MAX_TOP_VALUES = 50

# Конфіг полів для профілювання: (шлях у документі, тип статистики)
# Шлях може містити масив — тоді збираються значення з усіх елементів (наприклад addresses.region)
FIELD_PROFILE_CONFIG: Dict[str, List[Tuple[str, str]]] = {
    "unified_listings": [
        ("price_uah", "numeric"),
        ("building_area_sqm", "numeric"),
        ("land_area_ha", "numeric"),
        ("property_type", "categorical"),
        ("status", "categorical"),
        ("addresses.region", "categorical"),
        ("addresses.settlement", "categorical"),
    ],
    "prozorro_auctions": [
        ("auction_data.value.amount", "numeric"),
        ("auction_data.status", "categorical"),
        ("auction_data.dateModified", "categorical"),  # діапазони/префікси можна аналізувати
        ("auction_data.address_refs.region.name", "categorical"),  # з масиву
        ("auction_data.address_refs.city.name", "categorical"),
    ],
    "olx_listings": [
        ("detail.price", "numeric"),
        ("search_data.price", "numeric"),
        ("detail.llm.building_area_sqm", "numeric"),
        ("detail.llm.land_area_ha", "numeric"),
        ("detail.llm.property_type", "categorical"),
        ("detail.llm.tags", "categorical"),  # масив — кожен тег окремо
        ("search_data.location", "categorical"),
    ],
    "llm_cache": [
        ("result.building_area_sqm", "numeric"),
        ("result.land_area_ha", "numeric"),
        ("result.property_type", "categorical"),
        ("result.addresses.region", "categorical"),
        ("result.addresses.city", "categorical"),
    ],
}


def _get_value_at_path(doc: Dict[str, Any], path: str) -> List[Any]:
    """
    Повертає значення (або список значень з масиву) за шляхом у документі.
    Шлях "a.b.c": для масиву b збираються значення c з кожного елемента.
    """
    if not path or not doc:
        return []
    parts = [p for p in path.split(".") if p]

    def collect(node: Any, idx: int) -> List[Any]:
        if idx >= len(parts):
            return [node] if node is not None else []
        if node is None:
            return []
        key = parts[idx]
        if isinstance(node, dict):
            if key not in node:
                return []
            return collect(node[key], idx + 1)
        if isinstance(node, list):
            out = []
            for item in node:
                out.extend(collect(item, idx))
            return out
        return []

    values = collect(doc, 0)
    # Нормалізуємо: один елемент -> список з одного; прибираємо None
    result = []
    for v in values:
        if v is None:
            continue
        if isinstance(v, list):
            result.extend(x for x in v if x is not None)
        else:
            result.append(v)
    return result


def _to_number(value: Any) -> Optional[float]:
    """Перетворює значення на число для статистики."""
    if value is None:
        return None
    if isinstance(value, (int, float)):
        if isinstance(value, bool):
            return None
        return float(value)
    if isinstance(value, str):
        try:
            return float(value.replace(",", ".").strip())
        except ValueError:
            return None
    return None


class CollectionKnowledgeService:
    """
    Дослідження даних: профілювання колекцій (зразок документів → статистика по полях)
    та формування текстового контексту «загальні знання про дані» для агента.
    """

    def __init__(
        self,
        sample_size: int = DEFAULT_SAMPLE_SIZE,
        max_top_values: int = MAX_TOP_VALUES,
    ):
        self.sample_size = sample_size
        self.max_top_values = max_top_values
        self._repo = CollectionKnowledgeRepository()
        self._db = None

    def _get_db(self):
        if self._db is None:
            self._db = MongoDBConnection.get_database()
        return self._db

    def run_profiling(self, collection_names: Optional[List[str]] = None) -> Dict[str, Any]:
        """
        Запускає профілювання для вказаних колекцій (або всіх PROFILE_COLLECTIONS).
        Повертає результат по кожній колекції: success, total_documents, sample_size, error.
        """
        names = collection_names or PROFILE_COLLECTIONS
        results = {}
        for name in names:
            if name not in FIELD_PROFILE_CONFIG:
                results[name] = {"success": False, "error": "Немає конфігу полів для профілювання"}
                continue
            try:
                stats = self._profile_collection(name)
                if stats is not None:
                    self._repo.save(
                        collection_name=name,
                        total_documents=stats["total_documents"],
                        sample_size=stats["sample_size"],
                        field_stats=stats["field_stats"],
                    )
                    results[name] = {
                        "success": True,
                        "total_documents": stats["total_documents"],
                        "sample_size": stats["sample_size"],
                    }
                else:
                    results[name] = {"success": False, "error": "Порожня колекція або немає даних"}
            except Exception as e:
                logger.exception("Помилка профілювання колекції %s: %s", name, e)
                results[name] = {"success": False, "error": str(e)}
        return results

    def _profile_collection(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """Збирає статистику по одній колекції з вибірки документів."""
        db = self._get_db()
        mongo_name = collection_name
        coll = db[mongo_name]
        total = coll.count_documents({})
        if total == 0:
            return None
        sample_size = min(self.sample_size, total)
        cursor = coll.find({}).limit(sample_size)
        config = FIELD_PROFILE_CONFIG.get(collection_name, [])
        # path -> list of values (flattened)
        by_path: Dict[str, List[Any]] = {path: [] for path, _ in config}
        for doc in cursor:
            for path, stat_type in config:
                values = _get_value_at_path(doc, path)
                if stat_type == "categorical" and path.endswith(".tags"):
                    # теги — кожен елемент масиву окремо
                    for v in values:
                        if isinstance(v, str):
                            by_path[path].append(v)
                        elif v is not None:
                            by_path[path].append(str(v))
                else:
                    for v in values:
                        if v is not None:
                            by_path[path].append(v)
        field_stats = {}
        for (path, stat_type) in config:
            values = by_path.get(path, [])
            if stat_type == "numeric":
                nums = [_to_number(v) for v in values]
                nums = [n for n in nums if n is not None]
                if nums:
                    field_stats[path] = {
                        "type": "numeric",
                        "min": min(nums),
                        "max": max(nums),
                        "avg": round(sum(nums) / len(nums), 2),
                        "count": len(nums),
                    }
                else:
                    field_stats[path] = {"type": "numeric", "count": 0}
            else:
                counter = Counter(str(v) for v in values if v is not None and str(v).strip())
                total_cat = sum(counter.values())
                top = counter.most_common(self.max_top_values)
                field_stats[path] = {
                    "type": "categorical",
                    "cardinality": len(counter),
                    "total_values": total_cat,
                    "top_values": [{"value": v, "count": c} for v, c in top],
                }
        return {
            "total_documents": total,
            "sample_size": sample_size,
            "field_stats": field_stats,
        }

    def get_knowledge_for_agent(
        self,
        collection_names: Optional[List[str]] = None,
        max_length: Optional[int] = 4000,
    ) -> str:
        """
        Повертає текстовий блок «загальні знання про дані в колекціях» для вставки
        у контекст агента (system prompt або окреме повідомлення).
        """
        names = collection_names or PROFILE_COLLECTIONS
        latest = self._repo.get_all_latest(names)
        if not latest:
            return ""
        parts = ["## Загальні знання про дані в колекціях\n"]
        for cname, doc in latest.items():
            if not doc:
                continue
            total = doc.get("total_documents", 0)
            sample = doc.get("sample_size", 0)
            field_stats = doc.get("field_stats") or {}
            lines = [f"### {cname}", f"- Документів у колекції: {total} (профіль за вибіркою {sample})."]
            for path, stat in field_stats.items():
                if stat.get("type") == "numeric" and stat.get("count", 0) > 0:
                    lines.append(
                        f"- **{path}**: min={stat.get('min')}, max={stat.get('max')}, "
                        f"avg={stat.get('avg')}, значень у вибірці: {stat.get('count')}."
                    )
                elif stat.get("type") == "categorical" and stat.get("top_values"):
                    top = stat["top_values"][:10]
                    vals = ", ".join(f"{t['value']!r}({t['count']})" for t in top)
                    lines.append(f"- **{path}**: унікальних {stat.get('cardinality')}; топ: {vals}.")
            parts.append("\n".join(lines))
        text = "\n\n".join(parts)
        if max_length and len(text) > max_length:
            text = text[: max_length - 50] + "\n\n...(обрізано за довжиною)"
        return text

    def get_latest(self, collection_name: str) -> Optional[Dict[str, Any]]:
        """Повертає останній профіль для однієї колекції."""
        return self._repo.get_latest(collection_name)


def refresh_knowledge_after_sources(
    sources: List[str],
    sample_size: int = DEFAULT_SAMPLE_SIZE,
) -> Dict[str, Any]:
    """
    Оновлює знання про колекції після завантаження даних із джерел.
    Викликати після fetch_and_save (ProZorro) та/або run_olx_update (OLX).

    Args:
        sources: Список джерел, з яких щойно завантажили дані: "prozorro", "olx".
        sample_size: Розмір вибірки для профілювання.

    Returns:
        Результат run_profiling для відповідних колекцій.
    """
    source_to_collections: Dict[str, List[str]] = {
        "prozorro": ["prozorro_auctions"],
        "olx": ["olx_listings"],
        "unified": ["unified_listings"],
    }
    collection_names: List[str] = []
    for s in (sources or []):
        collection_names.extend(source_to_collections.get(s, []))
    if not collection_names:
        return {}
    service = CollectionKnowledgeService(sample_size=sample_size)
    return service.run_profiling(collection_names=collection_names)
