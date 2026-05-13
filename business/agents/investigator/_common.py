# -*- coding: utf-8 -*-
"""
Утиліти для агентів Flx: завантаження промптів, виклик LLM, безпечне
розпарсювання JSON. Жодних побічних ефектів — лише обробка тексту.
"""

import json
import logging
from typing import Any, Dict, Optional

from config.config_loader import get_config_loader
from config.settings import Settings
from business.services.llm_service import LLMService

logger = logging.getLogger(__name__)


SYSTEM_PROMPT = (
    "You are Flx, a real-estate investigator-agent. "
    "Your single job in this turn is to think and produce a structured JSON output. "
    "Do not invoke any tools yourself; the surrounding executor performs them based on your output. "
    "All natural-language string values in the JSON must be in Ukrainian. Return ONLY valid JSON."
)


def get_llm_service(settings: Settings) -> LLMService:
    """Загальна точка отримання LLMService для агентів Flx."""
    return LLMService(settings)


def get_prompt_template(name: str, fallback: str = "") -> str:
    """Завантажує промпт-шаблон з prompts.yaml. Повертає fallback при відсутності."""
    try:
        loader = get_config_loader()
        tmpl = loader.get_prompt(name, fallback=fallback) or fallback
        return str(tmpl)
    except Exception as e:
        logger.warning("Не вдалося завантажити промпт %s: %s", name, e)
        return fallback


def call_llm_json(
    llm: LLMService,
    *,
    prompt: str,
    caller: str,
    system_prompt: Optional[str] = None,
    temperature: Optional[float] = None,
) -> Dict[str, Any]:
    """Викликає LLM-провайдера інвестігейтора і парсить JSON-відповідь.

    Повертає {} у випадку помилки — викликача ця реалізація НЕ кидає виключення,
    щоб оркестратор міг зробити graceful retry або дефолтну дію.
    """
    raw = ""
    try:
        raw = llm.generate_text_investigator(
            prompt=prompt,
            system_prompt=system_prompt or SYSTEM_PROMPT,
            temperature=temperature,
            _caller=caller,
        )
    except Exception as e:
        logger.warning("[%s] LLM call failed: %s", caller, e)
        return {}
    if not raw or not raw.strip():
        return {}
    data = _parse_first_json_object(raw)
    if data is None:
        logger.warning("[%s] Invalid JSON from LLM (no parseable object); raw: %s", caller, raw[:300])
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _strip_markdown_fence(text: str) -> str:
    """Прибирає ``` … ``` оболонку, якщо вона є. Залишає лише вміст."""
    text = (text or "").strip()
    if not text.startswith("```"):
        return text
    lines = text.split("\n")
    json_lines: list = []
    in_block = False
    for line in lines:
        if line.strip().startswith("```"):
            if not in_block:
                in_block = True
                continue
            break
        if in_block:
            json_lines.append(line)
    return "\n".join(json_lines).strip()


def _parse_first_json_object(text: str) -> Optional[Any]:
    """Витягує і парсить ПЕРШИЙ top-level JSON-об'єкт з тексту.

    LLM іноді повертає два JSON-об'єкти підряд або хвіст пояснень після JSON:
    ``json.loads`` падає на 'Extra data'. Використовуємо ``raw_decode``: він
    парсить рівно один об'єкт від поточної позиції; усе, що після — ігнорується.

    Повертає Python-структуру або None, якщо парс невдалий.
    """
    body = _strip_markdown_fence(text)
    if not body:
        return None
    decoder = json.JSONDecoder()
    start = body.find("{")
    if start < 0:
        # Можливо, top-level — масив. Підтримуємо обидва варіанти на всякий випадок.
        start = body.find("[")
        if start < 0:
            return None
    body = body[start:]
    try:
        obj, _end = decoder.raw_decode(body)
        return obj
    except json.JSONDecodeError as e:
        logger.debug("raw_decode failed at first attempt: %s; body[:200]=%s", e, body[:200])
        # Спроба: оригінал міг містити trailing-кому або bare control chars — спробуємо
        # дуже консервативну санітизацію.
        cleaned = body.replace("\r", "")
        try:
            obj, _end = decoder.raw_decode(cleaned)
            return obj
        except json.JSONDecodeError:
            return None


def render_template(template: str, **kwargs: Any) -> str:
    """Безпечне форматування шаблону: відсутні плейсхолдери замінюємо на 'не вказано'."""
    if not template:
        return ""
    safe_kwargs = {k: ("не вказано" if v is None else v) for k, v in kwargs.items()}
    try:
        return template.format(**safe_kwargs)
    except KeyError as e:
        logger.warning("render_template: відсутній плейсхолдер %s", e)
        return template
    except Exception as e:
        logger.warning("render_template: %s", e)
        return template
