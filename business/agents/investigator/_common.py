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
    json_text = _extract_json(raw)
    if not json_text:
        return {}
    try:
        data = json.loads(json_text)
    except json.JSONDecodeError as e:
        logger.warning("[%s] Invalid JSON from LLM: %s; raw: %s", caller, e, raw[:200])
        return {}
    if not isinstance(data, dict):
        return {}
    return data


def _extract_json(text: str) -> str:
    """Витягує перший JSON-об'єкт з тексту LLM (підтримує markdown-код-блоки)."""
    text = (text or "").strip()
    if text.startswith("```"):
        lines = text.split("\n")
        json_lines = []
        in_json = False
        for line in lines:
            if line.strip().startswith("```"):
                if not in_json:
                    in_json = True
                else:
                    break
                continue
            if in_json:
                json_lines.append(line)
        return "\n".join(json_lines).strip()
    if "{" in text:
        start = text.find("{")
        end = text.rfind("}")
        if start != -1 and end != -1 and end > start:
            return text[start : end + 1]
    return text


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
