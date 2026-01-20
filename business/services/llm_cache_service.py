# -*- coding: utf-8 -*-
"""
Сервіс для кешування результатів парсингу описів через LLM.
"""

import json
import hashlib
from pathlib import Path
from typing import Dict, Any, Optional
from datetime import datetime, timezone


class LLMCacheService:
    """Сервіс для кешування результатів парсингу описів через LLM."""
    
    def __init__(self, cache_file_path: Optional[str] = None):
        """
        Ініціалізація сервісу кешування.
        
        Args:
            cache_file_path: Шлях до файлу кешу. Якщо не вказано, використовується data/cache/llm_cache.json
        """
        if cache_file_path is None:
            # Використовуємо стандартний шлях
            project_root = Path(__file__).parent.parent.parent
            cache_dir = project_root / 'data' / 'cache'
            cache_dir.mkdir(parents=True, exist_ok=True)
            cache_file_path = str(cache_dir / 'llm_cache.json')
        
        self.cache_file_path = Path(cache_file_path)
        self.cache_file_path.parent.mkdir(parents=True, exist_ok=True)
        self._cache: Dict[str, Dict[str, Any]] = {}
        self._load_cache()
    
    def _load_cache(self) -> None:
        """Завантажує кеш з файлу."""
        if not self.cache_file_path.exists():
            self._cache = {}
            return
        
        try:
            with open(self.cache_file_path, 'r', encoding='utf-8') as f:
                data = json.load(f)
                # Перевіряємо формат даних
                if isinstance(data, dict):
                    self._cache = data
                else:
                    self._cache = {}
        except (json.JSONDecodeError, IOError) as e:
            print(f"Помилка завантаження кешу LLM: {e}")
            self._cache = {}
    
    def _save_cache(self) -> None:
        """Зберігає кеш у файл."""
        try:
            with open(self.cache_file_path, 'w', encoding='utf-8') as f:
                json.dump(self._cache, f, ensure_ascii=False, indent=2)
        except IOError as e:
            print(f"Помилка збереження кешу LLM: {e}")
    
    def _get_description_hash(self, description: str) -> str:
        """
        Обчислює хеш опису.
        
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
    
    def get_cached_result(self, description: str) -> Optional[Dict[str, Any]]:
        """
        Отримує збережений результат парсингу для опису.
        
        Args:
            description: Текст опису аукціону
            
        Returns:
            Словник з результатами парсингу або None, якщо результат не знайдено
        """
        if not description or not description.strip():
            return None
        
        description_hash = self._get_description_hash(description)
        
        if description_hash in self._cache:
            cache_entry = self._cache[description_hash]
            # Повертаємо результат (без метаданих)
            return cache_entry.get('result')
        
        return None
    
    def save_result(self, description: str, result: Dict[str, Any]) -> None:
        """
        Зберігає результат парсингу для опису.
        
        Args:
            description: Текст опису аукціону
            result: Результат парсингу (словник з полями)
        """
        if not description or not description.strip():
            return
        
        description_hash = self._get_description_hash(description)
        
        # Зберігаємо результат з метаданими
        self._cache[description_hash] = {
            'result': result,
            'created_at': datetime.now(timezone.utc).isoformat()
        }
        
        # Зберігаємо кеш у файл
        self._save_cache()
    
    def clear_cache(self) -> None:
        """Очищає весь кеш."""
        self._cache = {}
        if self.cache_file_path.exists():
            try:
                self.cache_file_path.unlink()
            except IOError as e:
                print(f"Помилка видалення файлу кешу: {e}")
    
    def get_cache_stats(self) -> Dict[str, Any]:
        """
        Отримує статистику кешу.
        
        Returns:
            Словник зі статистикою: кількість записів, розмір файлу тощо
        """
        cache_size = 0
        if self.cache_file_path.exists():
            cache_size = self.cache_file_path.stat().st_size
        
        return {
            'entries_count': len(self._cache),
            'cache_file_size_bytes': cache_size,
            'cache_file_path': str(self.cache_file_path)
        }
