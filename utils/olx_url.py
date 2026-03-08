# -*- coding: utf-8 -*-
"""
Утиліти для роботи з URL оголошень OLX.
Один і той самий оголошення може мати різні URL через query-параметри
(search_reason=promoted vs organic). Канонічна форма — без query.
"""

from typing import Optional


def normalize_olx_listing_url(url: Optional[str]) -> Optional[str]:
    """
    Повертає канонічний URL оголошення OLX (без query-параметрів).
    Один і той самий оголошення з search_reason=promoted та search_reason=organic
    має зберігатися як один запис.

    Приклад:
        .../IDYF0Mb.html?search_reason=search%7Cpromoted -> .../IDYF0Mb.html
        .../IDYF0Mb.html?search_reason=search%7Corganic  -> .../IDYF0Mb.html
    """
    if not url or not isinstance(url, str):
        return None
    u = url.strip()
    if "?" in u:
        return u.split("?")[0]
    return u
