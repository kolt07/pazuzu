# -*- coding: utf-8 -*-
"""
Точка входу для Celery CLI: `celery -A business.celery_worker_entry:celery_app`.

Окремий модуль поруч із `celery_app`, щоб файл завжди був у пакеті `business/`
і потрапляв у Docker-образ разом із рештою коду (на відміну від скрипта в корені репозиторію).
"""

from __future__ import annotations

from .celery_app import celery_app

__all__ = ["celery_app"]
