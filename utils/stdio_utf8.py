# -*- coding: utf-8 -*-
"""
Налаштування UTF-8 для stdout у CLI-скриптах.

Не викликає sys.stdout.buffer, якщо його немає (наприклад LoggingProxy під uvicorn/FastAPI).
"""

from __future__ import annotations

import sys


def ensure_stdout_utf8() -> None:
    """Намагається встановити UTF-8 для sys.stdout; безпечно для обгорток без .buffer."""
    out = sys.stdout
    if hasattr(out, "reconfigure"):
        try:
            out.reconfigure(encoding="utf-8")
            return
        except (OSError, AttributeError, ValueError, TypeError):
            pass
    enc = getattr(out, "encoding", None) or ""
    if str(enc).lower() == "utf-8":
        return
    if not hasattr(out, "buffer"):
        return
    import io

    try:
        sys.stdout = io.TextIOWrapper(out.buffer, encoding="utf-8", errors="replace")
    except (AttributeError, OSError, ValueError, TypeError):
        pass
