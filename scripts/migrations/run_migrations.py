# -*- coding: utf-8 -*-
"""
Скрипт для запуску всіх міграцій.

Цей скрипт автоматично знаходить та виконує всі міграції в порядку їх номерів.
"""

import sys
import importlib.util
from pathlib import Path
from typing import List, Tuple

# Додаємо корінь проекту до шляху для імпортів
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))


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
        print(f"\nВиконання міграції {number:03d}: {name}...")
        print("-" * 60)
        
        try:
            # Імпортуємо модуль міграції
            module_name = f"scripts.migrations.{name}"
            spec = importlib.util.spec_from_file_location(module_name, file_path)
            module = importlib.util.module_from_spec(spec)
            spec.loader.exec_module(module)
            
            # Виконуємо міграцію
            if hasattr(module, 'run_migration'):
                success = module.run_migration()
                if not success:
                    print(f"\n✗ Міграція {number:03d} завершилася з помилкою")
                    all_success = False
                    break
            else:
                print(f"✗ Міграція {number:03d} не містить функцію run_migration()")
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
