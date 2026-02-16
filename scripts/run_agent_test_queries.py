# -*- coding: utf-8 -*-
"""
Прогон тестових запитів для відладки агента-помічника.

Використання:
  py scripts/run_agent_test_queries.py              # усі кейси (з викликом LLM)
  py scripts/run_agent_test_queries.py --quick       # лише інтерпретатор (без LLM)
  py scripts/run_agent_test_queries.py --filter analytics  # лише кейси з "analytics" в id

Очікується запуск з кореня проекту (де є config/, business/).
"""

import re
import sys
from pathlib import Path

# Додаємо корінь проекту в шлях
PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

# Кодування виводу в консоль
if sys.stdout.encoding and sys.stdout.encoding.lower() != 'utf-8':
    try:
        sys.stdout.reconfigure(encoding='utf-8')
    except Exception:
        pass


# Тестові кейси: (id, запит, expect_no_files, expect_numbers_in_text, опис)
# expect_no_files=True — для запитів, де експорт НЕ очікується (аналітика лише текстом)
TEST_CASES = [
    (
        "analytics_text_only",
        "Дай аналітику зміни цін за кв. м. в Києві та області за останній тиждень по дням",
        True,   # не очікуємо файлів
        True,    # очікуємо числа в тексті
        "Аналітика лише текстом, без експорту",
    ),
    (
        "report_last_day",
        "Звіт за добу",
        False,   # очікуємо файли
        True,
        "Звіт за добу — файли",
    ),
    (
        "export_week",
        "Експорт за тиждень",
        False,
        True,
        "Експорт за тиждень — файли",
    ),
    (
        "how_many_olx",
        "Скільки оголошень OLX за останній тиждень у Києві?",
        True,
        True,
        "Підрахунок — текст з числом, без файлів",
    ),
    (
        "metric_explain",
        "Що таке метрика average_price_per_m2?",
        True,
        False,
        "Пояснення метрики — без вибірки",
    ),
    (
        "collections",
        "Які колекції доступні?",
        True,
        False,
        "Довідка — без експорту",
    ),
]


def run_quick(settings, filter_substring: str):
    """Перевірка лише інтерпретатора (без LLM)."""
    from business.agents.interpreter_agent import InterpreterAgent

    interpreter = InterpreterAgent(settings)
    print("=== Режим --quick: лише інтерпретатор (без виклику LLM) ===\n")
    for case_id, query, _expect_no_files, _expect_numbers, desc in TEST_CASES:
        if filter_substring and filter_substring not in case_id and filter_substring not in query:
            continue
        structured = interpreter.interpret_user_query(query, None)
        print(f"[{case_id}] {desc}")
        print(f"  Запит: {query[:60]}...")
        print(f"  intent={structured.get('intent')!r} needs_data={structured.get('needs_data')} response_format={structured.get('response_format')!r}")
        print()
    print("Готово (quick).")


def run_full(settings, filter_substring: str):
    """Повний прогон через MultiAgentService (з викликом LLM)."""
    from data.database.connection import MongoDBConnection
    from business.services.multi_agent_service import MultiAgentService

    MongoDBConnection.initialize(settings)
    service = MultiAgentService(settings, user_service=None, notify_admins_fn=None)

    print("=== Повний прогон (MultiAgentService + LLM) ===\n")
    passed = 0
    failed = 0
    for case_id, query, expect_no_files, expect_numbers_in_text, desc in TEST_CASES:
        if filter_substring and filter_substring not in case_id and filter_substring not in query:
            continue
        print(f"[{case_id}] {desc}")
        print(f"  Запит: {query[:70]}{'...' if len(query) > 70 else ''}")
        try:
            response_text = service.process_query(user_query=query, user_id="test-user")
            files = service.get_last_excel_files()
            file_count = len(files)
            has_numbers = bool(re.search(r'\d+', response_text or ""))

            ok = True
            if expect_no_files and file_count > 0:
                print(f"  FAIL: очікувалось 0 файлів, отримано {file_count} (агент не повинен вивантажувати файли без запиту)")
                ok = False
            if not expect_no_files and file_count == 0:
                if "звіт" in query.lower() or "експорт" in query.lower():
                    print(f"  WARN: очікувались файли для звіту/експорту, отримано 0")
            if expect_numbers_in_text and not has_numbers:
                print(f"  WARN: очікувались числа в тексті, не знайдено")
            if ok:
                print(f"  OK: файлів={file_count}, числа в тексті={has_numbers}")
                passed += 1
            else:
                failed += 1
            print(f"  Відповідь (перші 200 символів): {(response_text or '')[:200]}...")
            print()
        except Exception as e:
            print(f"  ERROR: {e}")
            failed += 1
            print()
    print(f"Підсумок: passed={passed}, failed={failed}")


def main():
    import argparse
    parser = argparse.ArgumentParser(description="Тестові запити для агента-помічника")
    parser.add_argument("--quick", action="store_true", help="Лише інтерпретатор, без LLM")
    parser.add_argument("--filter", type=str, default="", help="Фільтр по id або тексту запиту")
    args = parser.parse_args()

    from config.settings import Settings
    settings = Settings()

    if args.quick:
        run_quick(settings, args.filter.strip().lower())
    else:
        run_full(settings, args.filter.strip().lower())


if __name__ == "__main__":
    main()
