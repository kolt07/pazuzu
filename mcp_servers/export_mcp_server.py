# -*- coding: utf-8 -*-
"""
MCP сервер для експорту оголошень та аукціонів у файл.
Підтримує: експорт за списком ids; експорт з тимчасової вибірки (temp_collection_id від save_query_to_temp_collection).
"""

import base64
from pathlib import Path
from typing import Any, Dict, List, Optional

from mcp.server.fastmcp import FastMCP
from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.agent_temp_exports_repository import AgentTempExportsRepository
from business.services.export_data_service import (
    ExportDataService,
    EXPORT_COLLECTION_OLX_LISTINGS,
    EXPORT_COLLECTION_PROZORRO_AUCTIONS,
    EXPORT_COLLECTION_UNIFIED_LISTINGS,
)
from business.services.prozorro_service import ProZorroService
from utils.file_utils import ensure_directory_exists, generate_excel_in_memory
from utils.date_utils import KYIV_TZ
from datetime import datetime as dt

mcp = FastMCP("export-mcp", json_response=True)

_export_service: Optional[ExportDataService] = None
_prozorro_service: Optional[ProZorroService] = None
_temp_exports_repo: Optional[AgentTempExportsRepository] = None


def _get_export_service() -> ExportDataService:
    """Ініціалізує підключення до БД та сервіс експорту."""
    global _export_service
    if _export_service is None:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        _export_service = ExportDataService(settings)
    return _export_service


def _get_prozorro_service() -> ProZorroService:
    global _prozorro_service
    if _prozorro_service is None:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        _prozorro_service = ProZorroService(settings)
    return _prozorro_service


def _get_temp_exports_repo() -> AgentTempExportsRepository:
    global _temp_exports_repo
    if _temp_exports_repo is None:
        settings = Settings()
        MongoDBConnection.initialize(settings)
        _temp_exports_repo = AgentTempExportsRepository()
    return _temp_exports_repo


@mcp.tool()
def export_listings_to_file(
    ids: List[str],
    collection: str,
    format: str = "xlsx",
    columns: Optional[List[str]] = None,
    column_headers: Optional[Dict[str, str]] = None,
    filename_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Експортує оголошення або аукціони за списком ідентифікаторів у файл у тимчасовій папці.
    Повертає посилання на розташування файлу (url) для передачі користувачу.

    Використовуй, коли:
    - Користувач явно просить відповідь у вигляді Excel/файлу.
    - Кількість оголошень/аукціонів у відповіді перевищує 10 — тоді обов'язково віддай результат у файлі та поверни користувачу посилання.

    Args:
        ids: Список ідентифікаторів (для prozorro_auctions — auction_id; для olx_listings — url; для unified_listings — source:source_id або _id).
        collection: Колекція — "unified_listings", "prozorro_auctions" або "olx_listings".
        format: Формат файлу: "xlsx", "csv" або "json". За замовчуванням "xlsx".
        columns: Опціональний список полів (dot-notation, наприклад auction_data.value.amount). Якщо не вказано — використовуються поля за замовчуванням для колекції.
        column_headers: Опціональний словник ключ → назва колонки для заголовків.
        filename_prefix: Опціональний префікс імені файлу.

    Returns:
        Словник: success, url (шлях до файлу), filename, size, rows_count; або success=False та error.
    """
    try:
        service = _get_export_service()
        result = service.export_to_file(
            ids=ids,
            collection=collection,
            file_format=format,
            fields=columns,
            column_headers=column_headers,
            filename_prefix=filename_prefix or "export",
        )
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def export_from_temp_collection(
    temp_collection_id: str,
    format: str = "xlsx",
    filename_prefix: Optional[str] = None,
) -> Dict[str, Any]:
    """
    Експортує в файл вибірку, створену save_query_to_temp_collection.
    Викликай після save_query_to_temp_collection(temp_collection_id). Файл зберігається у тимчасовій папці
    та автоматично відправляється користувачу (file_base64). Після експорту тимчасові дані видаляються.
    """
    try:
        repo = _get_temp_exports_repo()
        prozorro = _get_prozorro_service()
        settings = Settings()
        source_collection, docs = repo.get_batch(temp_collection_id)
        if source_collection is None and not docs:
            return {"success": False, "error": "Тимчасову вибірку не знайдено або вона вже видалена."}
        sheet = prozorro.get_standard_sheet_data_for_export_from_docs(docs, source_collection or "")
        if not sheet:
            repo.delete_batch(temp_collection_id)
            return {"success": False, "error": "Не вдалося сформувати лист для експорту."}
        rows, fieldnames, column_headers = sheet
        excel_io = generate_excel_in_memory(rows, fieldnames, column_headers)
        ensure_directory_exists(str(Path(settings.temp_directory) / "exports"))
        export_dir = Path(settings.temp_directory) / "exports"
        ts = dt.now(KYIV_TZ).strftime("%Y%m%d_%H%M%S")
        prefix = (filename_prefix or "export").strip()
        safe = "".join(c if c.isalnum() or c in "._-" else "_" for c in prefix)
        filename = f"{safe}_{ts}.xlsx"
        file_path = export_dir / filename
        file_path.write_bytes(excel_io.getvalue())
        repo.delete_batch(temp_collection_id)
        result = {
            "success": True,
            "url": str(file_path),
            "filename": filename,
            "rows_count": len(rows),
            "columns_count": len(fieldnames),
            "format": "xlsx",
            "file_base64": base64.b64encode(excel_io.getvalue()).decode("ascii"),
        }
        return result
    except Exception as e:
        return {"success": False, "error": str(e)}


@mcp.tool()
def get_export_collections() -> Dict[str, Any]:
    """
    Повертає список колекцій, доступних для експорту за ідентифікаторами.
    """
    return {
        "success": True,
        "collections": [
            {"id": EXPORT_COLLECTION_UNIFIED_LISTINGS, "description": "Зведена таблиця оголошень (OLX + ProZorro)"},
            {"id": EXPORT_COLLECTION_PROZORRO_AUCTIONS, "description": "Аукціони ProZorro"},
            {"id": EXPORT_COLLECTION_OLX_LISTINGS, "description": "Оголошення OLX"},
        ],
    }


def main():
    """Головна функція для запуску MCP сервера."""
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
