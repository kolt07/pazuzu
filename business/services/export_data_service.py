# -*- coding: utf-8 -*-
"""
Сервіс уніфікованого збереження даних у файл за ідентифікаторами оголошень у БД.
Генерує файли у тимчасовій папці та повертає посилання на розташування файлу.
"""

from pathlib import Path
from typing import Any, Dict, List, Optional

from config.settings import Settings
from domain.gateways.listing_gateway import ListingGateway
from datetime import datetime

from utils.date_utils import KYIV_TZ
from utils.file_utils import (
    ensure_directory_exists,
    save_csv_to_file,
    save_excel_to_file,
    save_json_to_file,
)

# Підтримувані колекції для експорту
EXPORT_COLLECTION_PROZORRO_AUCTIONS = "prozorro_auctions"
EXPORT_COLLECTION_OLX_LISTINGS = "olx_listings"
EXPORT_COLLECTION_UNIFIED_LISTINGS = "unified_listings"
EXPORT_COLLECTIONS = (
    EXPORT_COLLECTION_PROZORRO_AUCTIONS,
    EXPORT_COLLECTION_OLX_LISTINGS,
    EXPORT_COLLECTION_UNIFIED_LISTINGS,
)

# За замовчуванням поля для експорту (dot-notation), якщо не передано columns
DEFAULT_FIELDS_PROZORRO = [
    "auction_id",
    "last_updated",
    "auction_data.dateModified",
    "auction_data.value.amount",
    "auction_data.value.currency",
    "auction_data.status",
    "auction_data.procuringEntity.name",
]
DEFAULT_FIELDS_OLX = [
    "url",
    "updated_at",
    "search_data.title",
    "search_data.price",
    "search_data.location",
]
DEFAULT_FIELDS_UNIFIED = [
    "source",
    "source_id",
    "status",
    "property_type",
    "building_area_sqm",
    "land_area_ha",
    "title",
    "description",
    "page_url",
    "price_uah",
    "price_usd",
    "addresses",
    "cadastral_numbers",
    "source_updated_at",
]


class ExportDataService:
    """
    Сервіс експорту даних за ідентифікаторами оголошень у файл.
    Зберігає файли у тимчасовій директорії та повертає шлях до файлу.
    """

    def __init__(self, settings: Optional[Settings] = None):
        self.settings = settings or Settings()
        self._temp_dir = Path(self.settings.temp_directory) / "exports"
        self._temp_dir.mkdir(parents=True, exist_ok=True)
        self._listing_gateway = ListingGateway()

    def _get_default_fields(self, collection: str) -> List[str]:
        if collection == EXPORT_COLLECTION_PROZORRO_AUCTIONS:
            return list(DEFAULT_FIELDS_PROZORRO)
        if collection == EXPORT_COLLECTION_OLX_LISTINGS:
            return list(DEFAULT_FIELDS_OLX)
        if collection == EXPORT_COLLECTION_UNIFIED_LISTINGS:
            return list(DEFAULT_FIELDS_UNIFIED)
        return []

    def export_to_file(
        self,
        ids: List[str],
        collection: str,
        file_format: str,
        fields: Optional[List[str]] = None,
        column_headers: Optional[Dict[str, str]] = None,
        filename_prefix: Optional[str] = None,
    ) -> Dict[str, Any]:
        """
        Експортує документи за списком ідентифікаторів у файл у тимчасовій папці.

        Args:
            ids: Список ідентифікаторів (для prozorro_auctions — auction_id або _id, для olx_listings — url або _id).
            collection: Назва колекції — 'prozorro_auctions' або 'olx_listings'.
            file_format: Формат файлу — 'xlsx', 'csv' або 'json'.
            fields: Опціональний список полів (ключі з dot-notation). Якщо не вказано — використовуються поля за замовчуванням для колекції.
            column_headers: Опціональний мапінг ключ → назва колонки для заголовків.
            filename_prefix: Опціональний префікс імені файлу (без розширення).

        Returns:
            Словник: success, url (шлях до файлу), filename, size, rows_count, format, mime_type; або success=False та error.
        """
        if collection not in EXPORT_COLLECTIONS:
            return {
                "success": False,
                "error": f"Непідтримувана колекція: {collection}. Допустимі: {list(EXPORT_COLLECTIONS)}",
            }
        if file_format not in ("xlsx", "csv", "json"):
            return {
                "success": False,
                "error": f"Непідтримуваний формат: {file_format}. Допустимі: xlsx, csv, json",
            }

        try:
            columns = fields if fields else self._get_default_fields(collection)
            if ids:
                coll = self._listing_gateway.get_listing_collection_by_ids(ids, collection)
                if coll.count() == 0:
                    return {
                        "success": False,
                        "error": "За вказаними ідентифікаторами документів не знайдено",
                    }
                rows = coll.to_export_rows(columns)
            else:
                # Порожній експорт: один рядок-повідомлення, щоб користувач отримав файл
                rows = [{columns[0]: "Немає даних за вказаний період"} if columns else {}]

            ensure_directory_exists(str(self._temp_dir))
            timestamp = datetime.now(KYIV_TZ).strftime("%Y%m%d_%H%M%S")
            prefix = filename_prefix or "export"
            safe_prefix = "".join(c if c.isalnum() or c in "._-" else "_" for c in prefix)
            filename = f"{safe_prefix}_{timestamp}.{file_format}"
            file_path = self._temp_dir / filename

            headers = column_headers or {col: col.replace("_", " ").title() for col in columns}

            if file_format == "xlsx":
                save_excel_to_file(rows, str(file_path), columns, headers)
                mime_type = "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
            elif file_format == "csv":
                save_csv_to_file(rows, str(file_path), columns)
                mime_type = "text/csv"
            else:
                # json: зберігаємо список рядків (вже плоских словників)
                save_json_to_file(rows, str(file_path), ensure_ascii=False)
                mime_type = "application/json"

            size = file_path.stat().st_size
            return {
                "success": True,
                "url": str(file_path),
                "filename": filename,
                "size": size,
                "rows_count": len(rows),
                "format": file_format,
                "mime_type": mime_type,
            }
        except Exception as e:
            return {
                "success": False,
                "error": f"Помилка експорту: {str(e)}",
            }
