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


def calculate_description_hash(description: str) -> str:
    """
    Обчислює хеш опису (за тією ж логікою, що й в механізмі хешування відповідей LLM).
    
    Args:
        description: Текст опису аукціону
        
    Returns:
        MD5 хеш опису у вигляді рядка
    """
    # Нормалізуємо опис (прибираємо зайві пробіли)
    normalized = description.strip()
    # Обчислюємо MD5 хеш
    hash_obj = hashlib.md5(normalized.encode('utf-8'))
    return hash_obj.hexdigest()


def extract_auction_id(auction_data: Dict[str, Any]) -> Optional[str]:
    """
    Витягує ідентифікатор аукціону з даних.
    
    Args:
        auction_data: Дані аукціону
        
    Returns:
        Ідентифікатор аукціону або None
    """
    # Спробуємо знайти ID в різних місцях
    auction_id = auction_data.get('id') or auction_data.get('_id') or auction_data.get('auctionId')
    
    # Якщо ID все ще порожній, спробуємо знайти в data
    if not auction_id and isinstance(auction_data.get('data'), dict):
        auction_id = auction_data['data'].get('id') or auction_data['data'].get('_id') or auction_data['data'].get('auctionId')
    
    return auction_id if auction_id else None
