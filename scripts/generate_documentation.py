# -*- coding: utf-8 -*-
"""
Скрипт для генерації документації з Data Dictionary.
"""

import sys
from pathlib import Path

# Додаємо корінь проекту до шляху
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from utils.doc_generator import generate_documentation


def main():
    """Головна функція."""
    print("Генерація документації з Data Dictionary...")
    print("=" * 60)
    
    try:
        generate_documentation()
        print("\n✓ Документацію успішно згенеровано!")
    except Exception as e:
        print(f"\n✗ Помилка генерації документації: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()
