# -*- coding: utf-8 -*-
"""
Агент безпеки: верифікація запитів користувача на шкідливі/експлойтні патерни.
При виявленні — повертає відмову та сповіщення адміністраторам у Telegram.
"""

import re
import logging
from typing import Tuple, Optional, List, Callable

logger = logging.getLogger(__name__)

# Патерни, що можуть вказувати на спробу зловживання або експлойту
FORBIDDEN_PATTERNS = [
    (r"\b(db\.|collection\.|MongoClient|pymongo|find_one|\.find\s*\(|\.insert|\.update|\.delete|\.drop)\b", "прямий доступ до БД"),
    (r"\$regex|\$where|\$function", "небезпечні оператори MongoDB"),
    (r"(repeat|loop|while\s*true|for\s*;\s*;|recursive)\s*(\d{4,})?", "нескінченні цикли або надмірна рекурсія"),
    (r"limit\s*:\s*(\d{6,})|limit\s*=\s*(\d{6,})", "надмірна вибірка даних"),
    (r"(виведи|покажи|експортуй)\s*(всі|усі|кожен)\s*(записи|документи|аукціони)", "масова вибірка без обмежень"),
    (r"ignore\s*previous|ignore\s*instructions|system\s*prompt", "спроба ігнорувати інструкції"),
    (r"виконай\s*(код|script|python|bash)|eval\s*\(|exec\s*\(", "виконання коду"),
]

# Максимальні обмеження для контексту (щоб не перевантажувати)
MAX_QUERY_LENGTH = 10000


class SecurityAgent:
    """
    Перевіряє запит користувача на наявність шкідливих або експлойтних патернів.
    Якщо виявлено — викликає callback для сповіщення адміністраторів.
    """

    def __init__(
        self,
        notify_admins_fn: Optional[Callable[[str, str, Optional[str]], None]] = None,
    ):
        """
        Args:
            notify_admins_fn: Функція (message: str, user_id: Optional[str], details: Optional[str]) -> None
                              для відправки повідомлення адмінам (наприклад у Telegram).
        """
        self.notify_admins_fn = notify_admins_fn

    def check(
        self,
        user_query: str,
        user_id: Optional[str] = None,
    ) -> Tuple[bool, str]:
        """
        Верифікує запит. Повертає (дозволено, повідомлення).
        Якщо не дозволено — викликає notify_admins_fn.

        Returns:
            (True, "") якщо безпечно, (False, "причина") інакше.
        """
        if not user_query or not isinstance(user_query, str):
            return False, "Запит не може бути порожнім."

        text = user_query.strip()
        if len(text) > MAX_QUERY_LENGTH:
            self._notify(
                "Перевищено максимальну довжину запиту.",
                user_id,
                f"Довжина: {len(text)}, ліміт: {MAX_QUERY_LENGTH}",
            )
            return False, "Запит занадто довгий. Скоротьте його."

        text_lower = text.lower()
        for pattern, description in FORBIDDEN_PATTERNS:
            if re.search(pattern, text_lower, re.IGNORECASE):
                logger.warning("SecurityAgent: виявлено заборонений патерн: %s", description)
                self._notify(
                    f"Виявлено можливе небезпечне запитання: {description}",
                    user_id,
                    f"Фрагмент: {text[:500]}",
                )
                return False, f"Запит містить заборонені елементи ({description}). Використовуйте лише дозволені можливості бота."

        return True, ""

    def _notify(
        self,
        message: str,
        user_id: Optional[str],
        details: Optional[str] = None,
    ) -> None:
        if self.notify_admins_fn:
            try:
                self.notify_admins_fn(message, user_id, details)
            except Exception as e:
                logger.exception("Помилка сповіщення адмінів: %s", e)
