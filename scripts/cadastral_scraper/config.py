# -*- coding: utf-8 -*-
"""
Налаштування скрапера кадастрової карти kadastrova-karta.com.
"""

import os
import random

# Межі України (WGS84), приблизно
UKRAINE_BBOX = {
    "min_lat": 44.38,
    "max_lat": 52.38,
    "min_lon": 22.09,
    "max_lon": 40.23,
}

# Центр України (Київ) — для center-first порядку обробки тайлів
UKRAINE_CENTER_LAT = 50.45
UKRAINE_CENTER_LON = 30.52

# Розмір комірки сітки (градуси). 0.05° ≈ 5×5 км
CELL_SIZE_LAT = float(os.getenv("CADASTRAL_CELL_SIZE_LAT", "0.05"))
CELL_SIZE_LON = float(os.getenv("CADASTRAL_CELL_SIZE_LON", "0.05"))

# Vector tiles: land_polygons (zoom 11-16)
TILE_BASE_URL = os.getenv(
    "CADASTRAL_TILE_BASE",
    "https://kadastrova-karta.com/tiles/maps/kadastr/land_polygons",
)
LAND_POLYGONS_ZOOM_MIN = 11
LAND_POLYGONS_ZOOM_MAX = 16
# Zoom 12: менше тайлів, кожен покриває більшу площу; центр (Київ) має дані
DEFAULT_ZOOM = int(os.getenv("CADASTRAL_ZOOM", "12"))

# Затримка між запитами (секунди) — зменшення навантаження на сервер
DELAY_BETWEEN_CELLS_MIN = float(os.getenv("CADASTRAL_DELAY_MIN", "1"))
DELAY_BETWEEN_CELLS_MAX = float(os.getenv("CADASTRAL_DELAY_MAX", "3"))
# При workers > 1: затримка для запитів після першого (менша, щоб не уповільнювати порожні тайли)
DELAY_SUBSEQUENT_MIN = float(os.getenv("CADASTRAL_DELAY_SUBSEQUENT_MIN", "0.2"))
DELAY_SUBSEQUENT_MAX = float(os.getenv("CADASTRAL_DELAY_SUBSEQUENT_MAX", "0.5"))

# Таймаут одного запиту (секунди)
REQUEST_TIMEOUT = int(os.getenv("CADASTRAL_REQUEST_TIMEOUT", "30"))

# URL джерела (після дослідження можна додати API endpoint)
BASE_URL = os.getenv("CADASTRAL_BASE_URL", "https://kadastrova-karta.com/")

# User-Agent
USER_AGENT = os.getenv(
    "CADASTRAL_USER_AGENT",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
)


def get_delay_seconds(subsequent: bool = False) -> float:
    """Повертає випадкову затримку між обробкою комірок.
    subsequent=True — менша затримка для запитів після першого (при workers > 1).
    """
    if subsequent:
        return random.uniform(DELAY_SUBSEQUENT_MIN, DELAY_SUBSEQUENT_MAX)
    return random.uniform(DELAY_BETWEEN_CELLS_MIN, DELAY_BETWEEN_CELLS_MAX)
