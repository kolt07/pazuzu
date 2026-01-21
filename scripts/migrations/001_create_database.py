# -*- coding: utf-8 -*-
"""
Міграція 001: Створення бази даних pazuzu.

Ця міграція створює базу даних pazuzu та перевіряє підключення до MongoDB.
"""

import sys
from pathlib import Path

# Додаємо корінь проекту до шляху для імпортів
project_root = Path(__file__).parent.parent.parent
sys.path.insert(0, str(project_root))

from config.settings import Settings
from data.database.connection import MongoDBConnection


def run_migration():
    """
    Виконує міграцію для створення бази даних.
    
    Returns:
        bool: True якщо міграція виконана успішно, False інакше
    """
    print("=" * 60)
    print("Міграція 001: Створення бази даних pazuzu")
    print("=" * 60)
    
    try:
        # Ініціалізація налаштувань
        settings = Settings()
        
        print(f"\nПідключення до MongoDB:")
        print(f"  Хост: {settings.mongodb_host}")
        print(f"  Порт: {settings.mongodb_port}")
        print(f"  База даних: {settings.mongodb_database_name}")
        
        # Ініціалізація підключення
        print("\nІніціалізація підключення...")
        MongoDBConnection.initialize(settings)
        
        # Отримання бази даних (MongoDB створює базу автоматично при першому використанні)
        database = MongoDBConnection.get_database()
        
        # Перевірка підключення через виконання команди ping
        print("Перевірка підключення...")
        client = MongoDBConnection.get_client()
        client.admin.command('ping')
        
        # Створюємо тестовий документ для активації бази даних
        print(f"\nСтворення бази даних '{settings.mongodb_database_name}'...")
        test_collection = database['_migration_check']
        test_collection.insert_one({
            'migration': '001_create_database',
            'status': 'completed',
            'message': 'База даних створена успішно'
        })
        
        # Видаляємо тестовий документ
        test_collection.delete_one({'migration': '001_create_database'})
        
        # Отримуємо список колекцій для перевірки
        collections = database.list_collection_names()
        
        print(f"\n✓ База даних '{settings.mongodb_database_name}' успішно створена!")
        print(f"  Колекції в базі: {len(collections)}")
        if collections:
            print(f"  Список колекцій: {', '.join(collections)}")
        else:
            print("  База даних порожня (це нормально для нової бази)")
        
        print("\n" + "=" * 60)
        print("Міграція виконана успішно!")
        print("=" * 60)
        
        return True
        
    except ConnectionError as e:
        print(f"\n✗ Помилка підключення: {e}")
        print("\nПеревірте:")
        print("  1. Чи запущено MongoDB сервер")
        print("  2. Чи правильні налаштування підключення в config.yaml")
        return False
        
    except Exception as e:
        print(f"\n✗ Помилка під час виконання міграції: {e}")
        import traceback
        traceback.print_exc()
        return False
        
    finally:
        # Закриваємо підключення
        MongoDBConnection.close()


if __name__ == "__main__":
    success = run_migration()
    sys.exit(0 if success else 1)
