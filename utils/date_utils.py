# -*- coding: utf-8 -*-
"""
Утиліти для роботи з датами.
"""

from datetime import datetime, timedelta, timezone
from typing import Tuple


def get_date_range(days: int = 1) -> Tuple[datetime, datetime]:
    """
    Отримує діапазон дат для останніх N днів.

    Args:
        days: Кількість днів для виборки (за замовчуванням 1)

    Returns:
        Tuple[datetime, datetime]: Кортеж (початок, кінець) діапазону
    """
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=days)
    return start_date, now


def format_datetime_for_api(dt: datetime) -> str:
    """
    Форматує datetime для використання в API ProZorro.

    Args:
        dt: Дата та час для форматування

    Returns:
        str: Дата у форматі ISO 8601 (YYYY-MM-DDTHH:MM:SSZ)
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    # Форматуємо у формат ISO 8601 з 'Z' замість '+00:00'
    return dt.strftime('%Y-%m-%dT%H:%M:%SZ')


def is_within_days_range(dt: datetime, days: int = 1) -> bool:
    """
    Перевіряє, чи знаходиться дата в межах останніх N днів.

    Args:
        dt: Дата для перевірки
        days: Кількість днів для перевірки (за замовчуванням 1)

    Returns:
        bool: True, якщо дата в межах останніх N днів
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    now = datetime.now(timezone.utc)
    start_date = now - timedelta(days=days)
    
    return start_date <= dt <= now


def datetime_to_timestamp_ms(dt: datetime) -> int:
    """
    Конвертує datetime в timestamp у мілісекундах для API ProZorro.Sale.

    Args:
        dt: Дата та час для конвертації

    Returns:
        int: Timestamp у мілісекундах (Unix timestamp * 1000)
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    # Конвертуємо в UTC якщо потрібно
    if dt.tzinfo != timezone.utc:
        dt = dt.astimezone(timezone.utc)
    
    # Отримуємо timestamp у секундах і множимо на 1000 для мілісекунд
    timestamp_seconds = int(dt.timestamp())
    return timestamp_seconds * 1000


def format_datetime_for_byDateModified(dt: datetime) -> str:
    """
    Форматує datetime для використання в ендпоінті /api/search/byDateModified/{date}.
    
    Використовує формат ISO 8601 з мілісекундами та 'Z' для UTC.

    Args:
        dt: Дата та час для форматування

    Returns:
        str: Дата у форматі ISO 8601 з мілісекундами (YYYY-MM-DDTHH:MM:SS.ffffffZ)
    """
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    
    # Конвертуємо в UTC якщо потрібно
    if dt.tzinfo != timezone.utc:
        dt = dt.astimezone(timezone.utc)
    
    # Форматуємо з мілісекундами та 'Z'
    return dt.strftime('%Y-%m-%dT%H:%M:%S.%fZ')