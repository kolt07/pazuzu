# -*- coding: utf-8 -*-
"""
MCP сервер геокодування: перетворення адрес і топонімів на координати.
Перевіряє кеш і при необхідності звертається до Google Geocoding API.
"""

from mcp.server.fastmcp import FastMCP
from business.services.geocoding_service import GeocodingService

mcp = FastMCP("geocoding-mcp", json_response=True)
_geocoding_service: GeocodingService = None


def _get_service() -> GeocodingService:
    global _geocoding_service
    if _geocoding_service is None:
        _geocoding_service = GeocodingService()
    return _geocoding_service


@mcp.tool()
def geocode_address(address_or_place: str, region: str = "ua") -> dict:
    """
    Перетворює текстовий опис адреси або топоніма (назва ЖК, вулиця, місто тощо) на координати.
    Спочатку перевіряється кеш; при відсутності результату виконується запит до Google Geocoding API.
    Використовуй для будь-яких адрес або назв місць, згаданих у запитах користувача або в даних оголошень.

    Args:
        address_or_place: Текстовий запит — адреса, назва ЖК, ТРЦ, вулиця, місто тощо.
        region: Код регіону для зміщення результатів (за замовчуванням ua — Україна).

    Returns:
        Словник: query_hash (для збереження в оголошенні), query_text, results (список місць з latitude, longitude, formatted_address, place_id, types), from_cache.
    """
    service = _get_service()
    return service.geocode(query=address_or_place, region=region)


def main():
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
