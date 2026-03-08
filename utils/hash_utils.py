# -*- coding: utf-8 -*-
"""
Утиліти для обчислення хешів.
"""

import json
import hashlib
from typing import Dict, Any, Optional


def calculate_object_version_hash(auction_data: Dict[str, Any]) -> str:
    """
    Обчислює хеш версії об'єкта (повного тексту оголошення).
    
    Використовується для визначення, чи змінився аукціон.
    
    Args:
        auction_data: Повні дані аукціону (dict)
        
    Returns:
        MD5 хеш у вигляді рядка
    """
    # Серіалізуємо весь об'єкт в JSON для хешування
    # Використовуємо sort_keys=True для консистентності
    json_str = json.dumps(auction_data, sort_keys=True, ensure_ascii=False)
    # Обчислюємо MD5 хеш
    hash_obj = hashlib.md5(json_str.encode('utf-8'))
    return hash_obj.hexdigest()


def calculate_geocode_query_hash(query: str) -> str:
    """
    Хеш текстового запиту для геокодування (нормалізація + MD5).
    Використовується як ключ кешу запитів до Google Maps Geocoding API.
    """
    normalized = (query or "").strip().lower()
    normalized = " ".join(normalized.split())
    hash_obj = hashlib.md5(normalized.encode("utf-8"))
    return hash_obj.hexdigest()


def _normalize_description_for_hash(text: str) -> str:
    """
    Нормалізує текст опису для стабільного хешу: обрізає пробіли по краях рядків,
    прибирає порожні рядки, щоб однаковий контент давав однаковий хеш.
    """
    if not text:
        return ""
    lines = [line.strip() for line in text.strip().splitlines()]
    return "\n".join(line for line in lines if line)


def calculate_description_hash(description: str) -> str:
    """
    Обчислює хеш опису (за тією ж логікою, що й в механізмі хешування відповідей LLM).
    Використовує нормалізацію тексту, щоб однаковий зміст давав однаковий хеш.

    Args:
        description: Текст опису аукціону

    Returns:
        MD5 хеш опису у вигляді рядка
    """
    normalized = _normalize_description_for_hash(description or "")
    hash_obj = hashlib.md5(normalized.encode("utf-8"))
    return hash_obj.hexdigest()


def calculate_search_data_hash(search_data: Dict[str, Any]) -> str:
    """
    Хеш даних з картки пошуку OLX (ті самі поля, що в search_data_changed).
    Якщо хеш не змінився — сторінка пошуку не змінилась, оновлення сирих даних і LLM не потрібні.
    """
    keys = ("title", "price_text", "price_value", "currency", "location", "area_m2")
    canonical = {k: search_data.get(k) for k in keys}
    json_str = json.dumps(canonical, sort_keys=True, ensure_ascii=False)
    return hashlib.md5(json_str.encode("utf-8")).hexdigest()


def extract_auction_id(auction_data: Dict[str, Any]) -> Optional[str]:
    """
    Витягує ідентифікатор аукціону з даних.
    
    Для ProZorro.Sale посилання https://prozorro.sale/auction/{id} використовує
    auctionId (формат LSE001-UA-20260112-18611), а НЕ _id (MongoDB ObjectId).
    Пріоритет: auctionId > id > _id.
    
    Args:
        auction_data: Дані аукціону
        
    Returns:
        Ідентифікатор аукціону або None
    """
    # auctionId — формат для prozorro.sale; id — альтернатива; _id — MongoDB ObjectId (не валідний для URL)
    auction_id = auction_data.get('auctionId') or auction_data.get('id') or auction_data.get('_id')
    
    # Якщо ID все ще порожній, спробуємо знайти в data
    if not auction_id and isinstance(auction_data.get('data'), dict):
        auction_id = (
            auction_data['data'].get('auctionId')
            or auction_data['data'].get('id')
            or auction_data['data'].get('_id')
        )
    
    return auction_id if auction_id else None
