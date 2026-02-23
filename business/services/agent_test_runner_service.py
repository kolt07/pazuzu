# -*- coding: utf-8 -*-
"""
Сервіс тестування LLM-помічника за допомогою тест-агента.

Тест-агент: генерує 5 тест-кейсів різної складності (від простої виборки до агрегацій),
відправляє запити LLM-помічнику, потім перевіряє результат через прямий доступ до БД
(усі колекції), порівнює з очікуваним та формує звіт з «ходом думок» і коротким підсумком по кожному кейсу.
"""

import json
import logging
import re
import uuid
from datetime import datetime, timezone, timedelta
from typing import Any, Dict, List, Optional, Tuple

from config.settings import Settings
from data.database.connection import MongoDBConnection
from data.repositories.agent_activity_log_repository import AgentActivityLogRepository
from data.repositories.agent_temp_exports_repository import AgentTempExportsRepository

logger = logging.getLogger(__name__)

# Резервні тест-кейси, якщо LLM не зміг згенерувати
FALLBACK_TEST_CASES = [
    {
        "id": "simple_unified_week",
        "user_query": "Скільки оголошень нерухомості за останній тиждень?",
        "complexity": "low",
        "verification_type": "count_in_text",
        "expected_collections": ["unified_listings"],
        "expected_period_days": 7,
    },
    {
        "id": "simple_count_day",
        "user_query": "Скільки аукціонів ProZorro за останню добу?",
        "complexity": "low",
        "verification_type": "count_in_text",
        "expected_collections": ["prozorro_auctions"],
        "expected_period_days": 1,
    },
    {
        "id": "simple_olx_week",
        "user_query": "Скільки оголошень OLX за останній тиждень?",
        "complexity": "low",
        "verification_type": "count_in_text",
        "expected_collections": ["olx_listings"],
        "expected_period_days": 7,
    },
    {
        "id": "report_last_day",
        "user_query": "Звіт за добу",
        "complexity": "medium",
        "verification_type": "report_with_files",
        "expected_collections": ["prozorro_auctions", "olx_listings"],
        "expected_period_days": 1,
    },
    {
        "id": "analytics_text",
        "user_query": "Дай підсумок по середній ціні за м² за останній тиждень по регіонах",
        "complexity": "high",
        "verification_type": "aggregation_or_text",
        "expected_collections": [],
        "expected_period_days": 7,
    },
    {
        "id": "no_export",
        "user_query": "Які колекції доступні?",
        "complexity": "low",
        "verification_type": "no_export",
        "expected_collections": [],
        "expected_period_days": None,
    },
]

# Поля дат для фільтра «останні N днів» по колекціях
COLLECTION_DATE_FIELDS = {
    "unified_listings": "source_updated_at",
    "prozorro_auctions": "last_updated",
    "olx_listings": "updated_at",
}

# Фрази у відповіді помічника, що означають «даних не знайдено» / невдачу розрахунку.
# Якщо в БД за період є дані, а відповідь містить одну з них — кейс вважається не пройденим.
FAILURE_PHRASES = [
    "не вдалося знайти даних",
    "не вдалось знайти даних",
    "не знайдено даних",
    "не знайдено жодних",
    "відсутність даних",
    "немає даних",
    "даних не знайдено",
    "не має даних",
    "не знайшов даних",
    "не знайшла даних",
    "не вдалося знайти",
    "не вдалось знайти",
    "не знайдено оголошень",
    "не знайдено аукціонів",
    "не знайдено записів",
    "порожній результат",
    "результат порожній",
]


class AgentTestRunnerService:
    """
    Запуск тест-кейсів для LLM-помічника з перевіркою через прямий доступ до БД.
    Агент може читати всі колекції для верифікації результатів.
    """

    def __init__(self, settings: Settings):
        self.settings = settings
        self._activity_log = AgentActivityLogRepository()
        self._temp_exports = AgentTempExportsRepository()

    def _get_db(self):
        """Прямий доступ до БД (для перевірки, незалежно від обмежень системи)."""
        return MongoDBConnection.get_database()

    @staticmethod
    def _response_indicates_no_data_found(response_text: str) -> bool:
        """Повертає True, якщо в відповіді є фраза про відсутність даних / невдачу пошуку."""
        if not (response_text or "").strip():
            return False
        lower = response_text.lower()
        return any(phrase in lower for phrase in FAILURE_PHRASES)

    def _llm_interpret_response_success(
        self,
        user_query: str,
        response_text: str,
        total_expected: int,
        expected_counts: Dict[str, int],
        verification_type: str,
        file_count: int,
        langchain_service: Optional[Any] = None,
    ) -> Tuple[bool, str]:
        """
        Запитує LLM: чи була відповідь помічника успішною з урахуванням запиту та наявних даних у БД.
        Повертає (success, reason). Якщо передано langchain_service (наприклад з MultiAgentService) —
        використовується він, інакше створюється новий екземпляр (може падати в інших потоках).
        """
        llm = None
        if langchain_service is not None:
            try:
                llm = langchain_service._get_llm()
            except Exception as e:
                logger.debug("Не вдалося взяти LLM з переданого сервісу: %s", e)
        if llm is None:
            try:
                from business.services.langchain_agent_service import LangChainAgentService
                lc = LangChainAgentService(self.settings)
                llm = lc._get_llm()
            except Exception as e:
                logger.warning("Не вдалося створити LangChainAgentService для оцінки: %s", e)

        if llm is None:
            return True, "LLM перевірку пропущено (сервіс/ключ недоступний)"

        try:
            from langchain_core.messages import HumanMessage
            response_preview = (response_text or "")[:2500]
            prompt = f"""Тобі необхідно оцінити, чи відповідь помічника була успішною, і повернути JSON з полями success та reason.

## Запит користувача: «{user_query}»

## Відповідь помічника (фрагмент):
«««
{response_preview}
»»»

## Факти з бази даних: за вказаний період у БД є {total_expected} записів (по колекціях: {expected_counts}). Тип очікуваної відповіді: {verification_type}. Файлів у відповіді: {file_count}.

## Закріплення завдання:
Визнач, чи була відповідь помічника успішною (чи помічник реально використав дані або коректно пояснив результат). Поверни ТІЛЬКИ один JSON-об'єкт без markdown та без іншого тексту:
{{"success": true або false, "reason": "одне коротке речення українською"}}"""

            response = llm.invoke([HumanMessage(content=prompt)])
            text = getattr(response, "content", "") or str(response)
            text = text.strip()
            if "```json" in text:
                text = re.sub(r"^.*?```json\s*", "", text)
            if "```" in text:
                text = re.sub(r"\s*```.*$", "", text)
            data = json.loads(text.strip())
            success = bool(data.get("success", True))
            reason = str(data.get("reason", "") or "").strip() or ("Успішно" if success else "Невдача")
            return success, reason
        except Exception as e:
            err_msg = f"{type(e).__name__}: {e}"[:120]
            logger.warning("LLM інтерпретація успішності відповіді не вдалась: %s", e, exc_info=True)
            return True, f"LLM перевірку пропущено ({err_msg})"

    def _generate_test_cases_via_llm(self) -> List[Dict[str, Any]]:
        """Генерує 5 тест-кейсів через LLM. При помилці повертає FALLBACK_TEST_CASES."""
        try:
            from langchain_core.messages import HumanMessage
            from business.services.langchain_agent_service import LangChainAgentService
            lc = LangChainAgentService(self.settings)
            llm = lc._get_llm()
            prompt = """Тобі необхідно згенерувати рівно 5 тест-кейсів для перевірки LLM-помічника, що працює з даними ProZorro (аукціони) та OLX (оголошення), у форматі JSON.

Складність: від простої виборки з одним фільтром (наприклад «за останню добу») до складних агрегацій.

## Закріплення завдання:
Поверни ТІЛЬКИ один JSON-об'єкт без markdown та без пояснень, у такому форматі:
{
  "cases": [
    {
      "id": "унікальний_ідентифікатор",
      "user_query": "Текст запиту українською, як би сказав користувач",
      "complexity": "low" | "medium" | "high",
      "verification_type": "count_in_text" | "report_with_files" | "aggregation_or_text" | "no_export",
      "expected_collections": ["unified_listings"] або ["prozorro_auctions"] або ["olx_listings"] або кілька або [],
      "expected_period_days": 1 або 7 або null
    }
  ]
}

Умови:
- id — короткий латиницею (наприклад simple_count_day, report_week).
- user_query — один природний запит українською.
- verification_type: count_in_text — очікуємо число в тексті відповіді; report_with_files — очікуємо файли звіту; aggregation_or_text — агрегація або текст з числами; no_export — без вибірки/експорту.
- expected_collections — unified_listings (основне джерело), prozorro_auctions, olx_listings або комбінація, якщо запит стосується даних.
- Рівно 5 елементів у cases."""

            response = llm.invoke([HumanMessage(content=prompt)])
            text = getattr(response, "content", "") or str(response)
            text = text.strip()
            # Прибираємо markdown code block якщо є
            if "```json" in text:
                text = re.sub(r"^.*?```json\s*", "", text)
            if "```" in text:
                text = re.sub(r"\s*```.*$", "", text)
            data = json.loads(text.strip())
            cases = data.get("cases") or []
            if len(cases) >= 5:
                return cases[:5]
            return FALLBACK_TEST_CASES
        except Exception as e:
            logger.warning("LLM генерація тест-кейсів не вдалась: %s", e)
            return FALLBACK_TEST_CASES

    def get_direct_count(
        self,
        collection_name: str,
        period_days: Optional[int],
    ) -> int:
        """
        Повертає кількість документів у колекції за період (прямий доступ до БД).
        period_days: 1 = остання доба, 7 = останній тиждень; None = всі.
        """
        db = self._get_db()
        if collection_name not in db.list_collection_names():
            return 0
        coll = db[collection_name]
        date_field = COLLECTION_DATE_FIELDS.get(collection_name)
        if not date_field or period_days is None:
            return coll.count_documents({})
        since = datetime.now(timezone.utc) - timedelta(days=period_days)
        return coll.count_documents({date_field: {"$gte": since}})

    def run_test_case(
        self,
        case: Dict[str, Any],
        multi_agent_service: Any,
        case_index: int,
    ) -> Dict[str, Any]:
        """
        Виконує один тест-кейс: запит до помічника + перевірка через БД.
        Повертає словник: steps (хід думок), summary, passed, issues.
        """
        case_id = case.get("id", f"case_{case_index}")
        user_query = case.get("user_query", "")
        verification_type = case.get("verification_type", "count_in_text")
        expected_collections = case.get("expected_collections") or []
        expected_period_days = case.get("expected_period_days")
        steps: List[str] = []
        issues: List[str] = []

        steps.append(f"Кейс: {case_id}. Запит: «{user_query[:80]}{'…' if len(user_query) > 80 else ''}»")
        steps.append(f"Тип верифікації: {verification_type}. Очікувані колекції: {expected_collections}, період: {expected_period_days} днів.")

        request_id = str(uuid.uuid4())
        steps.append(f"Request ID: {request_id}")

        # 1) Очікувані значення з БД (прямий доступ)
        expected_counts: Dict[str, int] = {}
        for coll in expected_collections:
            cnt = self.get_direct_count(coll, expected_period_days)
            expected_counts[coll] = cnt
        # Для агрегації/аналітики без явних колекцій — підраховуємо unified_listings (основне джерело)
        if verification_type == "aggregation_or_text" and not expected_collections and expected_period_days is not None:
            expected_counts["unified_listings"] = self.get_direct_count("unified_listings", expected_period_days)
        if expected_counts:
            steps.append(f"Прямий підрахунок по БД: {expected_counts}")
        total_expected = sum(expected_counts.values())

        # 2) Виклик помічника
        try:
            response_text = multi_agent_service.process_query(
                user_query=user_query,
                user_id="agent-test-runner",
                request_id=request_id,
            )
        except Exception as e:
            steps.append(f"Помилка виконання запиту: {e}")
            issues.append(f"Помилка виконання: {e}")
            return {
                "case_id": case_id,
                "steps": steps,
                "summary": f"Кейс {case_id}: помилка виконання.",
                "passed": False,
                "issues": issues,
                "response_preview": "",
            }
        excel_files = multi_agent_service.get_last_excel_files()
        file_count = len(excel_files)
        total_rows_in_files = sum(f.get("rows_count", 0) for f in excel_files)

        steps.append(f"Відповідь (довжина {len(response_text or '')} символів). Файлів: {file_count}, рядків у файлах: {total_rows_in_files}.")
        steps.append(f"Уривок відповіді: {(response_text or '')[:300]}…")

        # 3) Лог активності по request_id
        try:
            log_entries = self._activity_log.get_by_request_id(request_id)
            steps.append(f"Записів у agent_activity_log: {len(log_entries)}.")
            if log_entries:
                intents = [e for e in log_entries if e.get("step") == "intent"]
                if intents:
                    steps.append(f"Намір (intent): {intents[-1].get('payload', {})}")
        except Exception as e:
            steps.append(f"Не вдалося прочитати лог: {e}")

        # 4) Перевірка очікувань
        if verification_type == "no_export":
            if file_count > 0:
                issues.append("Очікувалось 0 файлів (запит без вибірки), отримано файлів: " + str(file_count))
            has_numbers = bool(re.search(r"\d+", response_text or ""))
            steps.append(f"Перевірка no_export: файлів {file_count}, числа в тексті: {has_numbers}")

        elif verification_type == "count_in_text":
            total_expected = sum(expected_counts.values())
            numbers_in_text = re.findall(r"\d+", response_text or "")
            if total_expected == 0 and not numbers_in_text:
                steps.append("В БД 0 записів за період — відповідь може не містити число.")
            elif total_expected > 0:
                if not numbers_in_text:
                    issues.append("Очікувалось число в тексті відповіді, числа не знайдено.")
                else:
                    # Перевіряємо, чи є в тексті число близьке до очікуваного (допуск через округлення/формулювання)
                    found = any(int(n) == total_expected for n in numbers_in_text)
                    if not found and numbers_in_text:
                        # Можливо кілька колекцій — перевіряємо окремі числа
                        for num in numbers_in_text:
                            if int(num) in expected_counts.values():
                                found = True
                                break
                    if not found:
                        issues.append(
                            f"Очікувана кількість з БД: {total_expected} ({expected_counts}). "
                            f"Знайдені числа в тексті: {numbers_in_text[:10]}."
                        )
            steps.append(f"Очікувана сума з БД: {total_expected}. Знайдені числа: {numbers_in_text[:15]}.")

        elif verification_type == "report_with_files":
            if file_count == 0:
                issues.append("Очікувались файли звіту, отримано 0 файлів.")
            total_expected = sum(expected_counts.values())
            if total_expected > 0 and total_rows_in_files != total_expected:
                # Допускаємо відмінність через заголовки або кілька аркушів
                if total_rows_in_files == 0 and file_count > 0:
                    issues.append("Файли надіслано, але рядків у файлах: 0. Очікувалось записів з БД: " + str(total_expected))
                elif abs(total_rows_in_files - total_expected) > max(10, total_expected // 10):
                    issues.append(
                        f"Кількість рядків у файлах ({total_rows_in_files}) суттєво відрізняється від підрахунку по БД ({total_expected})."
                    )
            steps.append(f"Звіт: файлів {file_count}, рядків у файлах {total_rows_in_files}, очікувано з БД {total_expected}.")

        elif verification_type == "aggregation_or_text":
            has_numbers = bool(re.search(r"\d+", response_text or ""))
            if not has_numbers and not excel_files:
                issues.append("Очікувалась агрегація або текст з числами; числа в відповіді не знайдено.")
            steps.append(f"Агрегація/текст: числа в тексті={has_numbers}, файлів={file_count}.")

        # 5) Якщо в БД є дані за період, а відповідь повідомляє про відсутність даних — кейс не пройдено
        if total_expected > 0 and self._response_indicates_no_data_found(response_text or ""):
            issues.append(
                f"Відповідь містить фразу про відсутність даних або невдачу пошуку, "
                f"тоді як в БД за період є {total_expected} записів ({expected_counts})."
            )
            steps.append(f"Виявлено фразу невдачі при наявних даних у БД: {total_expected}.")

        # 6) Інтерпретація успішності відповіді через LLM (використовуємо той самий LangChain-сервіс, що й помічник)
        langchain_svc = getattr(multi_agent_service, "langchain_service", None)
        llm_success, llm_reason = self._llm_interpret_response_success(
            user_query=user_query,
            response_text=response_text or "",
            total_expected=total_expected,
            expected_counts=expected_counts,
            verification_type=verification_type,
            file_count=file_count,
            langchain_service=langchain_svc,
        )
        steps.append(f"LLM оцінка успішності: {'успішно' if llm_success else 'невдача'}. Причина: {llm_reason}")
        if not llm_success:
            issues.append(f"LLM інтерпретація: відповідь не успішна — {llm_reason}")

        passed = len(issues) == 0
        summary = (
            f"Кейс {case_id}: {'ПРОЙДЕНО' if passed else 'НЕ ПРОЙДЕНО'}. "
            + ("; ".join(issues) if issues else "Усі перевірки пройдено.")
        )
        return {
            "case_id": case_id,
            "steps": steps,
            "summary": summary,
            "passed": passed,
            "issues": issues,
            "response_preview": (response_text or "")[:200],
        }

    def run_all(
        self,
        multi_agent_service: Any,
        generate_with_llm: bool = True,
    ) -> Dict[str, Any]:
        """
        Генерує тест-кейси (опційно через LLM), прогоняє їх, перевіряє через БД.
        Повертає повний звіт: generated_cases, results (по кейсах), total_passed, total_failed, full_report_text.
        """
        if generate_with_llm:
            cases = self._generate_test_cases_via_llm()
        else:
            cases = FALLBACK_TEST_CASES

        results: List[Dict[str, Any]] = []
        for i, case in enumerate(cases):
            result = self.run_test_case(case, multi_agent_service, i)
            results.append(result)

        total_passed = sum(1 for r in results if r.get("passed"))
        total_failed = len(results) - total_passed

        lines = [
            "=== Звіт тестування LLM-помічника ===",
            f"Тест-кейсів: {len(results)}, пройдено: {total_passed}, не пройдено: {total_failed}",
            "",
        ]
        for r in results:
            lines.append("---")
            lines.append(r.get("summary", ""))
            for step in r.get("steps", []):
                lines.append("  " + step)
            if r.get("issues"):
                lines.append("  Проблеми: " + "; ".join(r["issues"]))
            lines.append("")
        full_report_text = "\n".join(lines)

        return {
            "generated_cases": cases,
            "results": results,
            "total_passed": total_passed,
            "total_failed": total_failed,
            "full_report_text": full_report_text,
        }
