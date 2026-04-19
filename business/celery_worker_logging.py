# -*- coding: utf-8 -*-
"""
Події Celery у stdout (docker logs): старт воркера, початок/кінець/помилка задачі.

Підключати один раз після створення celery_app. На воркері з `-Q llm_processing`
у логах будуть лише задачі LLM; на `-Q source_load` — лише source-load.
"""

from __future__ import annotations

import logging
import os
import time
from typing import Any, Dict

logger = logging.getLogger(__name__)

_task_start_mono: Dict[str, float] = {}


def _preview_first_arg(args: tuple, max_len: int = 200) -> str:
    if not args:
        return ""
    s = str(args[0])
    if len(s) <= max_len:
        return s
    return s[: max_len - 1] + "…"


def register_celery_worker_logging() -> None:
    """Реєструє сигнали Celery для видимості в консолі."""
    from celery.signals import task_failure, task_postrun, task_prerun, worker_ready

    @worker_ready.connect
    def _on_worker_ready(sender=None, **kwargs: Any) -> None:
        logger.info(
            "Celery worker готовий pid=%s hostname=%s",
            os.getpid(),
            getattr(sender, "hostname", "") or "",
        )

    @task_prerun.connect
    def _on_task_prerun(
        sender=None,
        task_id: str | None = None,
        task=None,
        args: tuple | None = None,
        kwargs: dict | None = None,
        **extra: Any,
    ) -> None:
        tid = task_id or ""
        if tid:
            _task_start_mono[tid] = time.monotonic()
        name = getattr(task, "name", getattr(sender, "name", "?"))
        preview = _preview_first_arg(tuple(args or ()))
        extra_kw = ""
        if kwargs:
            keys = list(kwargs.keys())[:12]
            extra_kw = f" kwargs_keys={keys}"
        logger.info(
            "Celery task → старт name=%s celery_id=%s arg0_preview=%s%s",
            name,
            tid,
            preview or "—",
            extra_kw,
        )

    @task_postrun.connect
    def _on_task_postrun(
        sender=None,
        task_id: str | None = None,
        task=None,
        retval: Any = None,
        **extra: Any,
    ) -> None:
        tid = task_id or ""
        name = getattr(task, "name", getattr(sender, "name", "?"))
        start = _task_start_mono.pop(tid, None) if tid else None
        dur = round(time.monotonic() - start, 2) if start is not None else None
        ok_hint = ""
        try:
            if isinstance(retval, dict) and "success" in retval:
                ok_hint = f" success={retval.get('success')}"
        except Exception:
            pass
        logger.info(
            "Celery task postrun name=%s celery_id=%s тривалість_с=%s%s",
            name,
            tid,
            dur,
            ok_hint,
        )

    @task_failure.connect
    def _on_task_failure(
        sender=None,
        task_id: str | None = None,
        exception: BaseException | None = None,
        **kwargs: Any,
    ) -> None:
        tid = task_id or ""
        if tid:
            _task_start_mono.pop(tid, None)
        name = getattr(sender, "name", "?") if sender else "?"
        logger.warning(
            "Celery task FAIL name=%s celery_id=%s: %s",
            name,
            tid,
            exception,
        )
