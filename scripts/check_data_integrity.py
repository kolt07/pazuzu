# -*- coding: utf-8 -*-
"""
Скрипт перевірки цілісності даних.
Запуск: py scripts/check_data_integrity.py
"""

import sys
from pathlib import Path

project_root = Path(__file__).resolve().parent.parent
if str(project_root) not in sys.path:
    sys.path.insert(0, str(project_root))

if hasattr(sys.stdout, "reconfigure"):
    sys.stdout.reconfigure(encoding="utf-8")


def main():
    from business.services.data_integrity_service import DataIntegrityService
    service = DataIntegrityService()
    result = service.check()
    print("Перевірка цілісності даних")
    print("=" * 50)
    print(f"Статус: {result['status']}")
    if result.get("errors"):
        print("\nПомилки:")
        for e in result["errors"]:
            print(f"  - {e}")
    if result.get("warnings"):
        print("\nПопередження:")
        for w in result["warnings"]:
            print(f"  - {w}")
    if result.get("checks"):
        print("\nКолекції:")
        for c in result["checks"]:
            print(f"  {c['collection']}: {c['count']} документів")
    return 0 if result["status"] != "errors" else 1


if __name__ == "__main__":
    sys.exit(main())
