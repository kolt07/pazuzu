# -*- coding: utf-8 -*-
"""
Утиліти для роботи з файлами.
"""

import os
import json
import csv
from pathlib import Path
from typing import Any, Dict, List
from datetime import datetime


def ensure_directory_exists(directory_path: str) -> None:
    """
    Створює директорію, якщо вона не існує.

    Args:
        directory_path: Шлях до директорії
    """
    Path(directory_path).mkdir(parents=True, exist_ok=True)


def save_json_to_file(data: Any, file_path: str, ensure_ascii: bool = False, indent: int = 2) -> None:
    """
    Зберігає дані у JSON файл з кодуванням UTF-8.

    Args:
        data: Дані для збереження
        file_path: Шлях до файлу
        ensure_ascii: Чи екранувати не-ASCII символи
        indent: Відступ для форматування JSON
    """
    ensure_directory_exists(os.path.dirname(file_path) if os.path.dirname(file_path) else '.')
    
    with open(file_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=ensure_ascii, indent=indent, default=str)


def generate_json_filename(prefix: str = 'prozorro_real_estate', extension: str = 'json') -> str:
    """
    Генерує ім'я JSON файлу з поточною датою та часом.

    Args:
        prefix: Префікс імені файлу
        extension: Розширення файлу

    Returns:
        str: Ім'я файлу у форматі prefix_YYYY-MM-DD_HH-MM-SS.extension
    """
    now = datetime.now()
    timestamp = now.strftime('%Y-%m-%d_%H-%M-%S')
    return f'{prefix}_{timestamp}.{extension}'


def save_csv_to_file(data: List[Dict[str, Any]], file_path: str, fieldnames: List[str]) -> None:
    """
    Зберігає дані у CSV файл з кодуванням UTF-8 з BOM для кращої сумісності з Excel.
    Використовує крапку з комою (;) як роздільник та правильно екранує значення.

    Args:
        data: Список словників з даними для збереження
        file_path: Шлях до файлу
        fieldnames: Список назв колонок
    """
    ensure_directory_exists(os.path.dirname(file_path) if os.path.dirname(file_path) else '.')
    
    # Використовуємо UTF-8 з BOM для кращої сумісності з Excel та іншими програмами Windows
    with open(file_path, 'w', encoding='utf-8-sig', newline='') as f:
        # Використовуємо крапку з комою як роздільник
        # quoting=csv.QUOTE_MINIMAL - екранує значення, які містять роздільник, лапки або переноси рядків
        writer = csv.DictWriter(
            f, 
            fieldnames=fieldnames, 
            delimiter=';',
            quotechar='"',
            quoting=csv.QUOTE_MINIMAL,
            extrasaction='ignore'
        )
        writer.writeheader()
        writer.writerows(data)
