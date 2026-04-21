# -*- coding: utf-8 -*-
"""
Запуск Celery worker з concurrency із Settings/config.yaml.
"""

from __future__ import annotations

import os
import sys

from config.settings import Settings


def _resolve_queue_and_threads(argv: list[str]) -> tuple[str, int]:
    queue = str(argv[1] if len(argv) > 1 else "llm_processing").strip() or "llm_processing"
    settings = Settings()
    if queue == "llm_processing":
        threads = int(getattr(settings, "task_queue_llm_worker_threads", 3) or 3)
    elif queue == "source_load":
        threads = int(getattr(settings, "task_queue_source_worker_threads", 1) or 1)
    else:
        threads = int(os.getenv("TASK_QUEUE_WORKER_THREADS", "1") or 1)
    return queue, max(1, threads)


def main() -> int:
    queue, threads = _resolve_queue_and_threads(sys.argv)
    cmd = [
        sys.executable,
        "-m",
        "celery",
        "-A",
        "business.celery_worker_entry:celery_app",
        "worker",
        "-Q",
        queue,
        "--loglevel=info",
        f"--concurrency={threads}",
    ]
    os.execvp(cmd[0], cmd)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
