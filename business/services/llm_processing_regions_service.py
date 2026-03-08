# -*- coding: utf-8 -*-
"""
Сервіс налаштувань областей для LLM-обробки (OLX та ProZorro).
Читає/записує config/llm_processing_regions.yaml.
"""

import logging
from pathlib import Path
from typing import List, Set

import yaml

logger = logging.getLogger(__name__)

_CONFIG_PATH = Path(__file__).resolve().parent.parent.parent / "config" / "llm_processing_regions.yaml"


def get_all_region_names() -> List[str]:
    """Повертає список усіх назв областей (OLX + м. Київ) для вибору в UI."""
    try:
        from scripts.olx_scraper import config as scraper_config
        slugs = scraper_config.get_olx_region_slugs()
        if slugs:
            return list(slugs.keys())
    except Exception as e:
        logger.warning("Не вдалося завантажити olx_region_slugs: %s", e)
    return []


def get_enabled_regions() -> Set[str]:
    """
    Повертає множину назв областей, увімкнених для LLM-обробки.
    Якщо файл відсутній або enabled_regions порожній — повертаємо порожню множину,
    що інтерпретується як «усі області увімкнені» (backward compatibility).
    """
    if not _CONFIG_PATH.exists():
        return set()
    try:
        with open(_CONFIG_PATH, "r", encoding="utf-8") as f:
            data = yaml.safe_load(f)
        regions = data.get("enabled_regions")
        if not regions:
            return set()
        return set(r.strip() for r in regions if isinstance(r, str) and r.strip())
    except Exception as e:
        logger.warning("Помилка читання llm_processing_regions.yaml: %s", e)
        return set()


def set_enabled_regions(region_names: List[str]) -> None:
    """Зберігає список увімкнених областей у config/llm_processing_regions.yaml."""
    _CONFIG_PATH.parent.mkdir(parents=True, exist_ok=True)
    normalized = [r.strip() for r in region_names if isinstance(r, str) and r.strip()]
    data = {"enabled_regions": normalized}
    with open(_CONFIG_PATH, "w", encoding="utf-8") as f:
        yaml.dump(data, f, allow_unicode=True, default_flow_style=False, sort_keys=False)


def is_region_enabled_for_llm(region_name: str) -> bool:
    """
    Перевіряє, чи область увімкнена для LLM-обробки.
    Якщо enabled_regions порожній (файл порожній або відсутній) — усі області увімкнені (True).
    Регіон нормалізується (відсікається « область», « обл.») для збігу з ProZorro.
    """
    enabled = get_enabled_regions()
    if not enabled:
        return True
    n = normalize_region_name(region_name or "")
    return n in enabled if n else True


def normalize_region_name(name: str) -> str:
    """Нормалізує назву області для порівняння: відсікає « область», « обл.»."""
    if not name or not isinstance(name, str):
        return ""
    s = name.strip()
    for suffix in (" область", " обл."):
        if s.endswith(suffix):
            s = s[: -len(suffix)].strip()
            break
    return s
