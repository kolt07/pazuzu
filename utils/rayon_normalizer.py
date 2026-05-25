# -*- coding: utf-8 -*-
"""
Нормалізація назв районів області (адміністративний рівень нижче за область).

Не плутати з районом міста (у доменній моделі — city_district / geo_circle).
"""

from __future__ import annotations

import re
from typing import Optional

import unicodedata

_MULTI = re.compile(r"\s+")
_SUFFIX = re.compile(
    r"\s*,?\s*(?:район|р-н|р\.|р\s*$)\s*$",
    re.IGNORECASE,
)


def _nfkc(value: str) -> str:
    return unicodedata.normalize("NFKC", value or "").strip()


def normalize_oblast_rayon_key(raw: Optional[str]) -> str:
    """Ключ для унікального індексу в межах області."""
    if not raw or not isinstance(raw, str):
        return ""
    v = _nfkc(raw)
    if not v:
        return ""
    v = v.lower()
    v = _SUFFIX.sub("", v)
    v = v.replace("'", "'")
    v = _MULTI.sub(" ", v).strip(" .,-/")
    return v


def format_oblast_rayon_display_name(raw: Optional[str]) -> Optional[str]:
    """Канонічна форма для збереження (Title Case, без зайвого «район» у кінці)."""
    if not raw or not isinstance(raw, str):
        return None
    v = _nfkc(raw)
    if not v:
        return None
    v = _SUFFIX.sub("", v).strip()
    if not v:
        return None
    parts = re.split(r"(\s+|-)", v)
    out: list[str] = []
    for part in parts:
        if not part or part.isspace():
            out.append(part)
            continue
        if part == "-":
            out.append(part)
            continue
        if len(part) == 1:
            out.append(part.upper())
        else:
            out.append(part[0].upper() + part[1:].lower())
    return "".join(out).strip() or None


def normalize_geo_circle_key(raw: Optional[str]) -> str:
    """Ключ для geo_circles (групування: сільрада, округ мегаполісу тощо)."""
    if not raw or not isinstance(raw, str):
        return ""
    v = _nfkc(raw).lower()
    v = _MULTI.sub(" ", v).strip(" .,-/")
    return v


def format_geo_circle_display_name(raw: Optional[str]) -> Optional[str]:
    if not raw or not isinstance(raw, str):
        return None
    v = _nfkc(raw)
    if not v:
        return None
    parts = re.split(r"(\s+|-)", v)
    out: list[str] = []
    for part in parts:
        if not part or part.isspace():
            out.append(part)
            continue
        if part == "-":
            out.append(part)
            continue
        if len(part) == 1:
            out.append(part.upper())
        else:
            out.append(part[0].upper() + part[1:].lower())
    return "".join(out).strip() or None


def build_geo_circle_scope_bucket(
    region_id: Optional[str],
    parent_city_id: Optional[str],
    parent_rayon_id: Optional[str],
) -> str:
    """
    Унікальність назви в межах «контексту»: одна й та ж назва може бути в різних містах/областях.
    """
    parts: list[str] = []
    if region_id:
        parts.append(f"r:{region_id}")
    if parent_city_id:
        parts.append(f"c:{parent_city_id}")
    if parent_rayon_id:
        parts.append(f"oar:{parent_rayon_id}")
    return "|".join(parts) if parts else "global"
