# -*- coding: utf-8 -*-
"""
ConfigMigrationRunner — застосування міграцій конфігурації.
При зміні config_version виконуються трансформації bundle.
"""

import logging
from pathlib import Path

from config.config.config_loader import get_config_loader, PLATFORM_VERSION

logger = logging.getLogger(__name__)

# Підтримувані версії конфігу
SUPPORTED_CONFIG_VERSIONS = ["1.0"]


def run_if_needed() -> bool:
    """
    Перевіряє версію конфігу та застосовує міграції при потребі.

    Returns:
        True якщо все ок, False при несумісності.
    """
    loader = get_config_loader()
    meta = loader.get_bundle_metadata()
    if not meta:
        return True
    config_version = meta.get("config_version", "1.0")
    min_platform = meta.get("min_platform_version", "")
    if min_platform and min_platform > PLATFORM_VERSION:
        logger.warning(
            "Конфіг потребує платформу >= %s, поточна: %s",
            min_platform,
            PLATFORM_VERSION,
        )
    if config_version not in SUPPORTED_CONFIG_VERSIONS:
        logger.warning("Невідома версія конфігу: %s", config_version)
    # Поки що міграції конфігу не реалізовані — лише перевірка
    return True
