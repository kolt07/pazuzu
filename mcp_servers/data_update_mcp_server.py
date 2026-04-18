# -*- coding: utf-8 -*-
"""
MCP сервер для ініціювання оновлення даних у базі (OLX, інформація про ProZorro).
Агент або інші клієнти викликають інструменти, щоб запустити оновлення оголошень OLX у БД.
"""

from typing import Any, Dict, Optional

from mcp.server.fastmcp import FastMCP
from business.services.task_queue_service import TaskQueueService
from config.settings import Settings
from data.database.connection import MongoDBConnection
from business.services.source_data_load_service import run_full_pipeline

mcp = FastMCP("data-update-mcp", json_response=True)


@mcp.tool()
def trigger_olx_update(
    days: Optional[int] = None,
    regions: Optional[list] = None,
    listing_types: Optional[list] = None,
) -> Dict[str, Any]:
    """
    Ініціює оновлення оголошень OLX через pipeline: Phase 1 — сирі дані без LLM, Phase 2 — promote у olx_listings + unified, LLM для обраних регіонів.

    Args:
        days: 1 або 7 — збір за період; за замовчуванням 1.
        regions: точкове оновлення — лише ці області (список назв, напр. ["Київська", "Львівська"]).
        listing_types: точкове оновлення — лише ці типи оголошень (напр. ["Нежитлова нерухомість", "Земля"]).

    Returns:
        Словник: success, message, phase1, phase2; або success=False та error.
    """
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        task_queue = TaskQueueService(settings)
        if task_queue.is_enabled():
            dispatched = task_queue.enqueue_source_load(
                days=days or 1,
                sources=["olx"],
                regions=regions if isinstance(regions, list) and regions else None,
                listing_types=listing_types if isinstance(listing_types, list) and listing_types else None,
                metadata={"trigger": "mcp"},
            )
            return {
                "success": True,
                "message": "OLX update queued via RabbitMQ/Celery.",
                "task_id": dispatched["task_id"],
                "queue": dispatched["queue"],
            }
        result = run_full_pipeline(
            settings=settings,
            sources=["olx"],
            days=days or 1,
            regions=regions if isinstance(regions, list) and regions else None,
            listing_types=listing_types if isinstance(listing_types, list) and listing_types else None,
        )
        p1 = result.get("phase1", {}).get("olx", {})
        p2 = result.get("phase2", {})
        try:
            from business.services.collection_knowledge_service import CollectionKnowledgeService
            CollectionKnowledgeService().run_profiling(collection_names=["olx_listings"])
        except Exception:
            pass
        return {
            "success": True,
            "message": (
                f"OLX: raw {p1.get('total_listings', 0)} огол., синхронізовано в unified; LLM оброблено: {p2.get('olx_llm_processed', 0)}."
            ),
            "phase1": p1,
            "phase2": p2,
        }
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_data_update_sources() -> Dict[str, Any]:
    """
    Повертає список джерел, для яких можна ініціювати оновлення даних.
    """
    return {
        "success": True,
        "sources": [
            {"id": "olx", "description": "Оголошення OLX (нежитлова нерухомість, земельні ділянки)", "tool": "trigger_olx_update"},
            {"id": "prozorro", "description": "Оновлення ProZorro виконується через головний пайплайн застосунку або Telegram", "tool": None},
        ],
    }


@mcp.tool()
def get_targeted_update_options() -> Dict[str, Any]:
    """
    Повертає опції для точкового оновлення: список областей та типів оголошень OLX.
    """
    try:
        from business.services.source_data_load_service import get_targeted_update_options as get_options
        return get_options()
    except Exception as e:
        return {"regions": [], "olx_listing_types": [], "error": str(e)}


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
