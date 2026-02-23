# -*- coding: utf-8 -*-
"""
Завантаження vector tiles кадастрових ділянок з kadastrova-karta.com.

URL: https://kadastrova-karta.com/tiles/maps/kadastr/land_polygons/{z}/{x}/{y}.pbf
Формат: Mapbox Vector Tile (Protobuf)
"""

import time
from pathlib import Path
from typing import Optional

import requests

_PROJECT_ROOT = Path(__file__).resolve().parent.parent.parent
import sys
if str(_PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(_PROJECT_ROOT))

from scripts.cadastral_scraper import config as scraper_config


def get_session() -> requests.Session:
    """Повертає сесію з заголовками браузера."""
    session = requests.Session()
    session.headers.update({
        "User-Agent": scraper_config.USER_AGENT,
        "Accept": "*/*",
        "Accept-Language": "uk,en;q=0.9",
        "Referer": scraper_config.BASE_URL,
    })
    return session


def fetch_tile_pbf(
    zoom: int,
    tile_x: int,
    tile_y: int,
    delay_before: bool = True,
    delay_subsequent: bool = False,
    session: Optional[requests.Session] = None,
) -> Optional[bytes]:
    """
    Завантажує один vector tile (land_polygons) у форматі PBF.

    Args:
        zoom: рівень zoom (11-16)
        tile_x: координата x тайлу
        tile_y: координата y тайлу
        delay_before: чи робити затримку перед запитом
        delay_subsequent: чи використовувати меншу затримку (для workers > 1)
        session: HTTP-сесія для повторного використання з'єднання (оптимізація)

    Returns:
        Байти PBF або None при помилці
    """
    url = f"{scraper_config.TILE_BASE_URL}/{zoom}/{tile_x}/{tile_y}.pbf"

    if delay_before:
        time.sleep(scraper_config.get_delay_seconds(subsequent=delay_subsequent))

    sess = session if session is not None else get_session()
    try:
        # stream=True: для порожніх тайлів (Content-Length: 0) не читаємо тіло — економія часу
        response = sess.get(url, timeout=scraper_config.REQUEST_TIMEOUT, stream=True)
        response.raise_for_status()
        content_length = response.headers.get("Content-Length")
        if content_length is not None:
            try:
                cl = int(content_length)
                if cl == 0:
                    response.close()
                    return b""
            except ValueError:
                pass
        return response.content
    except Exception:
        return None


def fetch_cell_data(
    cell: dict,
    delay_before: bool = True,
    delay_subsequent: bool = False,
    session: Optional[requests.Session] = None,
) -> Optional[bytes]:
    """
    Завантажує дані для комірки (тайлу).
    cell: {"zoom", "tile_x", "tile_y"}.
    session: HTTP-сесія для повторного використання з'єднання.

    Returns:
        Байти PBF або None
    """
    zoom = cell.get("zoom")
    tile_x = cell.get("tile_x")
    tile_y = cell.get("tile_y")

    if zoom is None:
        return None
    # Legacy: якщо немає tile_x/y, обчислюємо з bbox
    if tile_x is None or tile_y is None:
        bbox = cell.get("bbox") or {}
        if bbox:
            from scripts.cadastral_scraper.grid_iterator import bbox_to_tile_range
            x_min, x_max, y_min, y_max = bbox_to_tile_range(bbox, zoom)
            tile_x = x_min
            tile_y = y_min
        else:
            return None
    return fetch_tile_pbf(
        zoom, tile_x, tile_y,
        delay_before=delay_before,
        delay_subsequent=delay_subsequent,
        session=session,
    )
