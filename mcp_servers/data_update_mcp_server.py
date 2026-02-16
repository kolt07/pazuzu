# -*- coding: utf-8 -*-
"""
MCP сервер для ініціювання оновлення даних у базі (OLX, інформація про ProZorro).
Агент або інші клієнти викликають інструменти, щоб запустити оновлення оголошень OLX у БД.
"""

from typing import Any, Dict, Optional

from mcp.server.fastmcp import FastMCP
from config.settings import Settings
from data.database.connection import MongoDBConnection
from scripts.olx_scraper.run_update import run_olx_update

mcp = FastMCP("data-update-mcp", json_response=True)


@mcp.tool()
def trigger_olx_update(days: Optional[int] = None) -> Dict[str, Any]:
    """
    Ініціює оновлення оголошень OLX у базі даних (нежитлова нерухомість, земельні ділянки).
    Скрапер обходить сторінки пошуку OLX, завантажує деталі оголошень, обробляє через LLM та геокодування, зберігає в колекцію olx_listings.

    Args:
        days: Опційно. Якщо 1 або 7 — збір обмежується періодом (оголошення за останню добу або тиждень). Якщо не вказано — збір за кількістю сторінок (за замовчуванням категорії).

    Returns:
        Словник: success, message, total_listings, total_detail_fetches, by_category; або success=False та error.
    """
    try:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        result = run_olx_update(
            settings=settings,
            categories=None,
            log_fn=None,
            days=days,
        )
        try:
            from business.services.collection_knowledge_service import CollectionKnowledgeService
            CollectionKnowledgeService().run_profiling(collection_names=["olx_listings"])
        except Exception:
            pass
        return {
            "success": True,
            "message": (
                f"Оновлення OLX завершено. Оголошень оброблено: {result.get('total_listings', 0)}, "
                f"завантажено деталей: {result.get('total_detail_fetches', 0)}."
            ),
            "total_listings": result.get("total_listings", 0),
            "total_detail_fetches": result.get("total_detail_fetches", 0),
            "by_category": result.get("by_category", []),
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


def main() -> None:
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
