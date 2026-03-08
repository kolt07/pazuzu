# -*- coding: utf-8 -*-
"""
Єдина схема кроку плану (контракт Planner → Executor).

Кожен крок: step (тип), action (ім'я інструмента для виконання), params (аргументи).
Валідація перед виконанням гарантує відтворюваність і безпеку.
"""

from typing import Dict, Any, List, Tuple, Optional

# Дозволені типи кроків та відповідні action (tool name)
STEP_TYPES = ("update_data", "query", "export", "get_collections")
ACTION_BY_STEP = {
    "update_data": "trigger_data_update",
    "query": "save_query_to_temp_collection",
    "export": "export_from_temp_collection",
    "get_collections": "get_allowed_collections",
}

# Дозволені ключі в params по типу кроку (для валідації)
ALLOWED_PARAMS_BY_STEP = {
    "update_data": {"source", "days"},
    "query": {"query"},
    "export": {"format", "filename_prefix", "temp_collection_id", "temp_collection_id_from_step"},
    "get_collections": set(),
}


def validate_step(step: Dict[str, Any]) -> Tuple[bool, Optional[str]]:
    """
    Валідує один крок плану. Повертає (True, None) або (False, повідомлення про помилку).
    Приймає кроки у форматі {"step": "...", "action": "...", "params": {...}}
    або {"action": "...", "params": {...}} (legacy).
    """
    if not isinstance(step, dict):
        return False, "Крок має бути словником"
    action = step.get("action")
    step_type = step.get("step")
    params = step.get("params")
    if params is None:
        params = {}

    # Legacy: лише action без step — дозволяємо якщо action відомий
    if step_type is None:
        if action in set(ACTION_BY_STEP.values()):
            # Визначаємо step за action
            for st, act in ACTION_BY_STEP.items():
                if act == action:
                    step_type = st
                    break
        else:
            return False, f"Невідомий action: {action}"

    if step_type not in STEP_TYPES:
        return False, f"Невідомий тип кроку: {step_type}"

    if action and action != ACTION_BY_STEP.get(step_type):
        return False, f"Невідповідність step={step_type} та action={action}"

    allowed_keys = ALLOWED_PARAMS_BY_STEP.get(step_type, set())
    # temp_collection_id_from_step дозволений на рівні кроку (не в params)
    if step_type == "export":
        if not isinstance(params, dict):
            return False, "params для export має бути словником"
        # Не вважати temp_collection_id_from_step частиною params (воно на рівні step)
        extra = set(params.keys()) - allowed_keys
        if extra:
            return False, f"Заборонені ключі в params для export: {extra}"
    else:
        if not isinstance(params, dict):
            return False, "params має бути словником"
        extra = set(params.keys()) - allowed_keys
        if extra:
            return False, f"Заборонені ключі в params для {step_type}: {extra}"

    if step_type == "update_data":
        src = params.get("source")
        if src not in ("olx", "prozorro"):
            return False, "update_data вимагає source: olx або prozorro"

    if step_type == "query" and "query" in params:
        q = params["query"]
        if not isinstance(q, dict):
            return False, "query.params.query має бути словником"
        if "collection" not in q:
            return False, "query.params.query має містити collection"

    return True, None


def validate_plan(steps: List[Dict[str, Any]]) -> Tuple[bool, Optional[str]]:
    """
    Валідує список кроків плану. Повертає (True, None) або (False, повідомлення про помилку).
    """
    if not isinstance(steps, list):
        return False, "План має бути списком кроків"
    for i, step in enumerate(steps):
        ok, err = validate_step(step)
        if not ok:
            return False, f"Крок {i}: {err}"
    return True, None
