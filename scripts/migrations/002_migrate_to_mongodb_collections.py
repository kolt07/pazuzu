# -*- coding: utf-8 -*-
"""
Міграція 002: Перенос даних з файлів в MongoDB колекції.

Ця міграція:
1. Створює колекції: prozorro_auctions, logs, users, llm_cache
2. Переносить користувачів з users.yaml в колекцію users
3. Переносить LLM кеш з файлу в колекцію llm_cache
4. Видаляє файли users.yaml та users.example.yaml після успішного переносу
"""

import json
import yaml
from pathlib import Path
from datetime import datetime, timezone
from typing import Dict, Any, List

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.users_repository import UsersRepository
from data.repositories.llm_cache_repository import LLMCacheRepository
from utils.hash_utils import calculate_description_hash


def migrate_users_from_yaml(users_repo: UsersRepository, users_yaml_path: Path) -> int:
    """
    Переносить користувачів з YAML файлу в колекцію users.
    
    Args:
        users_repo: Репозиторій користувачів
        users_yaml_path: Шлях до файлу users.yaml
        
    Returns:
        Кількість перенесених користувачів
    """
    if not users_yaml_path.exists():
        print(f"Файл {users_yaml_path} не існує, пропускаємо перенос користувачів")
        return 0
    
    try:
        with open(users_yaml_path, 'r', encoding='utf-8') as f:
            config = yaml.safe_load(f)
        
        if not config or 'users' not in config:
            print("Файл users.yaml не містить користувачів")
            return 0
        
        users = config['users'] or []
        migrated_count = 0
        
        for user_data in users:
            user_id = user_data.get('user_id')
            if not user_id:
                continue
            
            # Перевіряємо, чи користувач вже існує
            existing = users_repo.find_by_user_id(user_id)
            if existing:
                print(f"Користувач {user_id} вже існує в базі, пропускаємо")
                continue
            
            # Створюємо користувача
            role = user_data.get('role', 'user')
            nickname = user_data.get('nickname', f'User_{user_id}')
            modified_by = user_data.get('last_modified_by', 'system')
            
            # Конвертуємо modified_by в int, якщо можливо
            try:
                modified_by_int = int(modified_by) if modified_by != 'system' else 0
            except:
                modified_by_int = 0
            
            success = users_repo.create_user(user_id, role, nickname, modified_by_int)
            if success:
                migrated_count += 1
                print(f"Перенесено користувача {user_id} ({nickname})")
            else:
                print(f"Помилка переносу користувача {user_id}")
        
        return migrated_count
        
    except Exception as e:
        print(f"Помилка переносу користувачів з {users_yaml_path}: {e}")
        return 0


def migrate_llm_cache_from_file(llm_cache_repo: LLMCacheRepository, cache_file_path: Path) -> int:
    """
    Переносить LLM кеш з файлу в колекцію llm_cache.
    
    Args:
        llm_cache_repo: Репозиторій LLM кешу
        cache_file_path: Шлях до файлу llm_cache.json
        
    Returns:
        Кількість перенесених записів
    """
    if not cache_file_path.exists():
        print(f"Файл {cache_file_path} не існує, пропускаємо перенос LLM кешу")
        return 0
    
    try:
        with open(cache_file_path, 'r', encoding='utf-8') as f:
            cache_data = json.load(f)
        
        if not isinstance(cache_data, dict):
            print("Файл llm_cache.json має некоректний формат")
            return 0
        
        migrated_count = 0
        
        # cache_data - це словник, де ключі - це хеші описів, а значення - записи з результатами
        for description_hash, cache_entry in cache_data.items():
            if not isinstance(cache_entry, dict):
                continue
            
            result = cache_entry.get('result')
            if not result:
                continue
            
            # Перевіряємо, чи запис вже існує
            existing = llm_cache_repo.find_by_description_hash(description_hash)
            if existing:
                print(f"Запис з хешем {description_hash[:8]}... вже існує, пропускаємо")
                continue
            
            # Зберігаємо запис
            llm_cache_repo.save_result(description_hash, result)
            migrated_count += 1
        
        print(f"Перенесено {migrated_count} записів LLM кешу")
        return migrated_count
        
    except Exception as e:
        print(f"Помилка переносу LLM кешу з {cache_file_path}: {e}")
        return 0


def run_migration():
    """Виконує міграцію."""
    print("=" * 60)
    print("Міграція 002: Перенос даних з файлів в MongoDB колекції")
    print("=" * 60)
    
    # Ініціалізуємо налаштування та підключення до MongoDB
    settings = Settings()
    
    try:
        MongoDBConnection.initialize(settings)
        print("✓ Підключення до MongoDB успішне")
    except Exception as e:
        print(f"✗ Помилка підключення до MongoDB: {e}")
        return False
    
    # Створюємо репозиторії (вони автоматично створять колекції та індекси)
    users_repo = UsersRepository()
    llm_cache_repo = LLMCacheRepository()
    
    print("\n✓ Колекції створено (або вже існують)")
    
    # Переносимо користувачів
    project_root = Path(__file__).parent.parent.parent
    users_yaml_path = project_root / 'config' / 'users.yaml'
    
    print(f"\nПеренос користувачів з {users_yaml_path}...")
    users_migrated = migrate_users_from_yaml(users_repo, users_yaml_path)
    print(f"✓ Перенесено користувачів: {users_migrated}")
    
    # Переносимо LLM кеш
    cache_file_path = project_root / 'data' / 'cache' / 'llm_cache.json'
    
    print(f"\nПеренос LLM кешу з {cache_file_path}...")
    cache_migrated = migrate_llm_cache_from_file(llm_cache_repo, cache_file_path)
    print(f"✓ Перенесено записів LLM кешу: {cache_migrated}")
    
    # Видаляємо файли після успішного переносу
    files_to_delete = []
    
    if users_migrated > 0 and users_yaml_path.exists():
        files_to_delete.append(users_yaml_path)
    
    users_example_path = project_root / 'config' / 'users.example.yaml'
    if users_example_path.exists():
        files_to_delete.append(users_example_path)
    
    if cache_migrated > 0 and cache_file_path.exists():
        files_to_delete.append(cache_file_path)
    
    if files_to_delete:
        print(f"\nВидалення файлів після переносу...")
        for file_path in files_to_delete:
            try:
                file_path.unlink()
                print(f"✓ Видалено {file_path}")
            except Exception as e:
                print(f"✗ Помилка видалення {file_path}: {e}")
    
    print("\n" + "=" * 60)
    print("Міграція завершена успішно!")
    print("=" * 60)
    
    return True


if __name__ == '__main__':
    success = run_migration()
    exit(0 if success else 1)
