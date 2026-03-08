# -*- coding: utf-8 -*-
"""
ConfigLoader — завантаження конфігураційного bundle (промпти, налаштування, схема).
Підтримує шлях з env PAZUZU_CONFIG_PATH або config/ за замовчуванням.
"""

import hashlib
import logging
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, Optional

import yaml

logger = logging.getLogger(__name__)

# Версія платформи (при відсутності pyproject.toml)
PLATFORM_VERSION = "1.0.0"


class ConfigLoader:
    """Завантажує та кешує файли конфігураційного bundle."""

    def __init__(self, config_path: Optional[Path] = None):
        """
        Args:
            config_path: Шлях до каталогу конфігу. За замовчуванням — config/ в корені проекту.
        """
        if config_path is None:
            import os
            env_path = os.getenv("PAZUZU_CONFIG_PATH")
            if env_path:
                config_path = Path(env_path)
            else:
                config_path = Path(__file__).parent
        self.config_path = Path(config_path)
        self._cache: Dict[str, Any] = {}

    def _load_yaml(self, filename: str) -> Optional[Dict[str, Any]]:
        """Завантажує YAML файл з кешуванням."""
        if filename in self._cache:
            return self._cache[filename]
        path = self.config_path / filename
        if not path.exists():
            logger.debug("Config file not found: %s", path)
            return None
        try:
            with open(path, "r", encoding="utf-8") as f:
                data = yaml.safe_load(f)
            self._cache[filename] = data
            return data
        except Exception as e:
            logger.warning("Failed to load config %s: %s", path, e)
            return None

    def _load_text(self, filename: str) -> str:
        """Завантажує текстовий файл."""
        cache_key = f"_text_{filename}"
        if cache_key in self._cache:
            return self._cache[cache_key]
        path = self.config_path / filename
        if not path.exists():
            return ""
        try:
            with open(path, "r", encoding="utf-8") as f:
                content = f.read()
            self._cache[cache_key] = content
            return content
        except Exception as e:
            logger.warning("Failed to load %s: %s", path, e)
            return ""

    def get_bundle_metadata(self) -> Optional[Dict[str, Any]]:
        """Повертає метадані bundle (config_version, min_platform_version, checksum)."""
        return self._load_yaml("bundle_metadata.yaml")

    def get_prompt(self, name: str, fallback: Optional[str] = None) -> Optional[str]:
        """
        Повертає промпт за іменем з prompts.yaml.

        Args:
            name: Ключ промпту (langchain_system, intent_detection, query_structure, parsing, llm_agent_system).
            fallback: Значення за замовчуванням, якщо файл або ключ відсутні.

        Returns:
            Текст промпту або fallback.
        """
        data = self._load_yaml("prompts.yaml")
        if not data or "prompts" not in data:
            return fallback
        prompts = data["prompts"]
        if name not in prompts:
            return fallback
        val = prompts[name]
        if isinstance(val, str):
            return val
        if isinstance(val, dict):
            if "base" in val:
                return val["base"]
            if "template" in val:
                return val["template"]
        return fallback

    def get_parsing_template(self, fallback: Optional[str] = None) -> Optional[str]:
        """Повертає шаблон промпту для парсингу (з плейсхолдером {description})."""
        return self.get_prompt("parsing", fallback=fallback)

    def get_glossary(self) -> str:
        """Завантажує глосарій з developer_glossary.md (config/ або docs/)."""
        # Спочатку config (у bundle), потім docs/
        for subpath in ("developer_glossary.md", "../docs/developer_glossary.md"):
            path = self.config_path / subpath
            if path.exists():
                try:
                    with open(path, "r", encoding="utf-8") as f:
                        return f.read()
                except Exception as e:
                    logger.warning("Failed to load glossary %s: %s", path, e)
        return ""

    def clear_cache(self) -> None:
        """Очищає кеш завантажених файлів."""
        self._cache.clear()


# Глобальний інстанс для зручності
_loader: Optional[ConfigLoader] = None


def get_config_loader() -> ConfigLoader:
    """Повертає глобальний ConfigLoader."""
    global _loader
    if _loader is None:
        _loader = ConfigLoader()
    return _loader


def compute_bundle_checksum(config_path: Path) -> str:
    """Обчислює checksum для файлів bundle (для bundle_metadata)."""
    hasher = hashlib.sha256()
    files = [
        "prompts.yaml",
        "bundle_metadata.yaml",
        "data_dictionary.yaml",
        "app_metadata.yaml",
    ]
    for name in sorted(files):
        path = config_path / name
        if path.exists():
            with open(path, "rb") as f:
                hasher.update(f.read())
    return hasher.hexdigest()[:32]
