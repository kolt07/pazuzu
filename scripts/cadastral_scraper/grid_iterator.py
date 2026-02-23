# -*- coding: utf-8 -*-
"""
Генерація та ітерація комірок (тайлів) для покриття території України.
Використовує XYZ tile scheme (Web Mercator).
"""

import math
from typing import Any, Dict, Generator, List

from scripts.cadastral_scraper import config as scraper_config


def _lon_to_tile_x(lon: float, zoom: int) -> int:
    """Конвертує довготу в tile x (Web Mercator)."""
    n = 2 ** zoom
    return int((lon + 180) / 360 * n)


def _lat_to_tile_y(lat: float, zoom: int) -> int:
    """Конвертує широту в tile y (Web Mercator)."""
    lat_rad = math.radians(lat)
    n = 2 ** zoom
    y = (1 - math.asinh(math.tan(lat_rad)) / math.pi) / 2 * n
    return int(max(0, min(n - 1, y)))


def bbox_to_tile_range(bbox: Dict[str, float], zoom: int) -> tuple:
    """
    Повертає (x_min, x_max, y_min, y_max) для тайлів, що покривають bbox.
    """
    x_min = _lon_to_tile_x(bbox["min_lon"], zoom)
    x_max = _lon_to_tile_x(bbox["max_lon"], zoom)
    y_min = _lat_to_tile_y(bbox["max_lat"], zoom)  # y зростає вниз
    y_max = _lat_to_tile_y(bbox["min_lat"], zoom)
    return (x_min, x_max, y_min, y_max)


def _cell_id_tile(zoom: int, x: int, y: int) -> str:
    """Формує унікальний ключ комірки для тайлу."""
    return f"{zoom}_{x}_{y}"


def _distance_from_center(lat: float, lon: float) -> float:
    """Відстань від центру України (Київ). Менше = вищий пріоритет."""
    dlat = lat - scraper_config.UKRAINE_CENTER_LAT
    dlon = lon - scraper_config.UKRAINE_CENTER_LON
    return (dlat ** 2 + dlon ** 2) ** 0.5


def generate_tile_cells(
    bbox: Dict[str, float],
    zoom: int,
) -> Generator[Dict[str, Any], None, None]:
    """
    Генерує комірки (тайли) для заданого bbox та zoom.

    Yields:
        Словік {"cell_id", "zoom", "tile_x", "tile_y", "bbox"} для кожного тайлу
    """
    x_min, x_max, y_min, y_max = bbox_to_tile_range(bbox, zoom)
    n = 2 ** zoom

    for x in range(x_min, x_max + 1):
        for y in range(y_min, y_max + 1):
            if 0 <= x < n and 0 <= y < n:
                cell_id = _cell_id_tile(zoom, x, y)
                # Приблизний bbox тайлу (для сумісності з репозиторієм)
                lon_min = x / n * 360 - 180
                lon_max = (x + 1) / n * 360 - 180
                lat_max_rad = math.atan(math.sinh(math.pi * (1 - 2 * y / n)))
                lat_min_rad = math.atan(math.sinh(math.pi * (1 - 2 * (y + 1) / n)))
                lat_max = math.degrees(lat_max_rad)
                lat_min = math.degrees(lat_min_rad)
                center_lat = (lat_min + lat_max) / 2
                center_lon = (lon_min + lon_max) / 2
                sort_priority = _distance_from_center(center_lat, center_lon)
                yield {
                    "cell_id": cell_id,
                    "zoom": zoom,
                    "tile_x": x,
                    "tile_y": y,
                    "sort_priority": round(sort_priority, 4),
                    "bbox": {
                        "min_lat": lat_min,
                        "max_lat": lat_max,
                        "min_lon": lon_min,
                        "max_lon": lon_max,
                    },
                }


def get_all_cells_for_ukraine(zoom: int = None) -> List[Dict[str, Any]]:
    """
    Повертає список усіх тайлів для території України.
    Використовується для початкового заповнення cadastral_scraper_cells.
    """
    z = zoom or scraper_config.DEFAULT_ZOOM
    z = max(scraper_config.LAND_POLYGONS_ZOOM_MIN, min(scraper_config.LAND_POLYGONS_ZOOM_MAX, z))
    return list(generate_tile_cells(scraper_config.UKRAINE_BBOX, z))


# Зворотна сумісність: старі bbox-комірки
def _cell_id(zoom: int, min_lat: float, min_lon: float) -> str:
    return f"{zoom}_{min_lat:.6f}_{min_lon:.6f}"


def generate_grid_cells(
    bbox: Dict[str, float],
    cell_size_lat: float,
    cell_size_lon: float,
    zoom: int,
) -> Generator[Dict[str, Any], None, None]:
    """Застаріло: використовуйте generate_tile_cells."""
    min_lat, max_lat = bbox["min_lat"], bbox["max_lat"]
    min_lon, max_lon = bbox["min_lon"], bbox["max_lon"]
    lat = min_lat
    while lat < max_lat:
        lon = min_lon
        while lon < max_lon:
            cell_min_lat = lat
            cell_max_lat = min(lat + cell_size_lat, max_lat)
            cell_min_lon = lon
            cell_max_lon = min(lon + cell_size_lon, max_lon)
            cell_id = _cell_id(zoom, cell_min_lat, cell_min_lon)
            yield {
                "cell_id": cell_id,
                "zoom": zoom,
                "bbox": {
                    "min_lat": cell_min_lat,
                    "max_lat": cell_max_lat,
                    "min_lon": cell_min_lon,
                    "max_lon": cell_max_lon,
                },
            }
            lon += cell_size_lon
        lat += cell_size_lat
