# -*- coding: utf-8 -*-
"""Оновлення CognitiveState та серіалізація для промпту / логів."""

from __future__ import annotations

import json
import logging
import uuid
from typing import Any, Dict, List, Optional, Tuple

from business.domain.cognitive_agent_models import (
    CS_COMPLETED_STEPS,
    CS_CONFIDENCE,
    CS_CURRENT_TASK,
    CS_GOAL,
    CS_KNOWN_FACTS,
    CS_LAST_TOOL_FAILED,
    CS_UNKNOWNS,
    new_cognitive_state,
)

logger = logging.getLogger(__name__)


def _truncate(s: str, n: int) -> str:
    s = (s or "").strip()
    if len(s) <= n:
        return s
    return s[: n - 3] + "..."


def format_cognitive_for_prompt(cognitive: Optional[Dict[str, Any]]) -> str:
    """Короткий блок українською для контексту LLM (не роздуваємо вікно)."""
    if not cognitive:
        return ""
    lines = [
        "## Внутрішній стан задачі (оновлюється після інструментів)",
        f"- Мета: {_truncate(str(cognitive.get(CS_GOAL, '')), 400)}",
        f"- Поточний фокус: {_truncate(str(cognitive.get(CS_CURRENT_TASK, '')), 300)}",
    ]
    facts = cognitive.get(CS_KNOWN_FACTS) or []
    if isinstance(facts, list) and facts:
        lines.append("- Відомі факти (скорочено):")
        for f in facts[-8:]:
            lines.append(f"  • {_truncate(str(f), 220)}")
    unk = cognitive.get(CS_UNKNOWNS) or []
    if isinstance(unk, list) and unk:
        lines.append("- Невизначеності:")
        for u in unk[-5:]:
            lines.append(f"  • {_truncate(str(u), 200)}")
    done = cognitive.get(CS_COMPLETED_STEPS) or []
    if isinstance(done, list) and done:
        lines.append("- Зроблені кроки:")
        for d in done[-6:]:
            lines.append(f"  • {_truncate(str(d), 200)}")
    if cognitive.get(CS_LAST_TOOL_FAILED):
        lines.append("- Останній виклик інструменту: **неуспішний** — перевір підхід.")
    lines.append(f"- Впевненість (оцінка): {cognitive.get(CS_CONFIDENCE, 0.5)}")
    return "\n".join(lines)


def merge_after_agent_turn(
    cognitive: Dict[str, Any],
    *,
    assistant_text_excerpt: str,
    has_tool_calls: bool,
) -> Dict[str, Any]:
    """Після відповіді LLM: оновити поточний фокус (легка евристика)."""
    out = dict(cognitive)
    excerpt = _truncate(assistant_text_excerpt, 400)
    if excerpt and not has_tool_calls:
        out[CS_CURRENT_TASK] = "Фінальна відповідь користувачу"
    elif has_tool_calls:
        out[CS_CURRENT_TASK] = "Виконання інструментів"
    return out


def merge_after_tools(
    cognitive: Dict[str, Any],
    tool_summaries: List[Tuple[str, bool, str]],
) -> Dict[str, Any]:
    """
    tool_summaries: список (tool_name, success, excerpt_result)
    """
    out = dict(cognitive)
    completed = list(out.get(CS_COMPLETED_STEPS) or [])
    facts = list(out.get(CS_KNOWN_FACTS) or [])
    unknowns = list(out.get(CS_UNKNOWNS) or [])
    any_fail = False
    for name, ok, excerpt in tool_summaries:
        line = f"{name}: {'ok' if ok else 'FAIL'} — {_truncate(excerpt, 300)}"
        completed.append(line)
        if ok and excerpt:
            facts.append(f"[{name}] {_truncate(excerpt, 350)}")
        if not ok:
            any_fail = True
            unknowns.append(f"Потрібне уточнення після {name}: {_truncate(excerpt, 200)}")
    out[CS_COMPLETED_STEPS] = completed[-40:]
    out[CS_KNOWN_FACTS] = facts[-30:]
    out[CS_UNKNOWNS] = unknowns[-15:]
    out[CS_LAST_TOOL_FAILED] = any_fail
    # Проста евристика впевненості
    base = float(out.get(CS_CONFIDENCE) or 0.5)
    if any_fail:
        out[CS_CONFIDENCE] = max(0.15, base - 0.08)
    else:
        out[CS_CONFIDENCE] = min(0.92, base + 0.03)
    return out


def summarize_tool_result_for_chain(result: Any) -> str:
    """Скорочений текст спостереження для Long Chain."""
    try:
        if isinstance(result, dict):
            if "error" in result and not result.get("success", True):
                return _truncate(str(result.get("error")), 500)
            keys = ("success", "rows_count", "filename", "message", "count")
            parts = [f"{k}={result.get(k)}" for k in keys if k in result]
            raw = json.dumps(result, ensure_ascii=False, default=str)
            if parts:
                return _truncate("; ".join(parts) + " | " + raw, 1500)
            return _truncate(raw, 1500)
        return _truncate(str(result), 1500)
    except Exception as e:
        logger.debug("summarize_tool_result_for_chain: %s", e)
        return _truncate(str(result), 500)


def make_chain_step_id() -> str:
    return f"lc_{uuid.uuid4().hex[:12]}"


def initial_cognitive_from_user_query(user_query: str) -> Dict[str, Any]:
    return new_cognitive_state(user_query)
