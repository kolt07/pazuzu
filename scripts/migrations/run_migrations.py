# -*- coding: utf-8 -*-
"""
Скрипт для запуску всіх міграцій.

Цей скрипт автоматично знаходить та виконує всі міграції в порядку їх номерів.
Відстежує виконані міграції в колекції _migration_history (після міграції 029).
"""

import sys
import importlib.util
from datetime import datetime, timezone
from pathlib import Path
from typing import List, Tuple, Optional

# Додаємо корінь проекту до шляху для імпортів
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


def _get_migration_history_collection():
    """Повертає колекцію _migration_history або None якщо БД не ініціалізована."""
    try:
        from config.settings import Settings
        from data.database.connection import MongoDBConnection
        MongoDBConnection.initialize(Settings())
        return MongoDBConnection.get_database()["_migration_history"]
    except Exception:
        return None


def _is_migration_applied(coll, migration_name: str) -> bool:
    """Перевіряє, чи міграція вже виконана."""
    if coll is None:
        return False
    return coll.find_one({"_id": migration_name}) is not None


def _record_migration_applied(coll, migration_name: str) -> None:
    """Записує міграцію як виконану."""
    if coll is None:
        return
    try:
        coll.replace_one(
            {"_id": migration_name},
            {
                "_id": migration_name,
                "applied_at": datetime.now(timezone.utc).isoformat(),
                "config_version_at_apply": "1.0",
            },
            upsert=True,
        )
    except Exception:
        pass


def find_migrations() -> List[Tuple[int, str, Path]]:
    """
    Знаходить всі файли міграцій у директорії migrations.
    
    Returns:
        Список кортежів (номер, назва, шлях) відсортований за номером
    """
    migrations_dir = Path(__file__).parent
    migrations = []
    
    for file_path in migrations_dir.glob("*.py"):
        if file_path.name.startswith("__") or file_path.name == "run_migrations.py":
            continue
        
        # Витягуємо номер міграції з назви файлу (наприклад, 001_create_database.py -> 001)
        try:
            number_str = file_path.stem.split("_")[0]
            number = int(number_str)
            migrations.append((number, file_path.stem, file_path))
        except (ValueError, IndexError):
            print(f"Попередження: пропущено файл {file_path.name} (некоректний формат назви)")
            continue
    
    return sorted(migrations, key=lambda x: x[0])


def run_all_migrations():
    """
    Виконує всі знайдені міграції в порядку їх номерів.
    
    Returns:
        bool: True якщо всі міграції виконані успішно, False інакше
    """
    print("=" * 60)
    print("Запуск міграцій бази даних")
    print("=" * 60)
    
    migrations = find_migrations()
    
    if not migrations:
        print("\nМіграції не знайдено.")
        return True
    
    print(f"\nЗнайдено міграцій: {len(migrations)}")
    for number, name, _ in migrations:
        print(f"  {number:03d}: {name}")
    
    print("\n" + "=" * 60)

    all_success = True

    for number, name, file_path in migrations:
        history_coll = _get_migration_history_collection()
        if _is_migration_applied(history_coll, name):
            print(f"\nПропуск міграції {number:03d}: {name} (вже виконано)")
            continue

        print(f"\nВиконання міграції {number:03d}: {name}...")
        print("-" * 60)

        try:
            # Імпортуємо модуль міграції
            module_name = f"scripts.migrations.{name}"
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)

            # Виконуємо міграцію (run_migration, run або up)
            run_fn = getattr(module, 'run_migration', None) or getattr(module, 'run', None) or getattr(module, 'up', None)
            if run_fn is not None:
                result = run_fn()
                success = result is not False
                if not success:
                    print(f"\n✗ Міграція {number:03d} завершилася з помилкою")
                    all_success = False
                    break
                _record_migration_applied(_get_migration_history_collection(), name)
            else:
                print(f"✗ Міграція {number:03d} не містить функцію run_migration(), run() або up()")
                all_success = False
                break

        except Exception as e:
            print(f"\n✗ Помилка під час виконання міграції {number:03d}: {e}")
            import traceback
            traceback.print_exc()
            all_success = False
            break
    
    print("\n" + "=" * 60)
    if all_success:
        print("Всі міграції виконано успішно!")
    else:
        print("Деякі міграції завершилися з помилками.")
    print("=" * 60)
    
    return all_success


if __name__ == "__main__":
    success = run_all_migrations()
    sys.exit(0 if success else 1)
