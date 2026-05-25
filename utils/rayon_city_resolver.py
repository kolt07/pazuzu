# -*- coding: utf-8 -*-
"""
Підбір цільового НП для переносу вулиць із помилкового «міста-району» (stub city).

Використовує fuzzy-порівняння назви району області з назвами НП у тій самій області
та явні fallback-и для випадків без однозначного адміністративного центру в тексті.
"""

from __future__ import annotations

import re
from difflib import SequenceMatcher
from typing import Any, Dict, List, Optional, Tuple

from business.services.settlement_matching_service import similarity_score
from utils.rayon_normalizer import normalize_oblast_rayon_key

# (точна назва області з regions.name, підрядок у назві району, канонічна назва НП)
FALLBACK_RAYON_TO_CITY: List[Tuple[str, str, str]] = [
    ("Київська", "Києво-Святошинськ", "Ірпінь"),
    ("Одеська", "Лиманськ", "Южне"),
    ("Одеська", "Комінтернівськ", "Южне"),
]

_MIN_FUZZY = 0.78


def _stem_key(rayon_key: str) -> str:
    return re.sub(r"(ський|цький|нський|зький)$", "", rayon_key).strip()


def pick_target_city_for_stub(
    cities: List[Dict[str, Any]],
    *,
    region_name: str,
    rayon_display_name: str,
) -> Tuple[Optional[Dict[str, Any]], float, str]:
    """
    Повертає (city_doc | None, score 0..1, reason).

    reason: fallback | fuzzy | stem_boost | none
    """
    r_display = rayon_display_name.strip()
    r_key = normalize_oblast_rayon_key(r_display)
    stem = _stem_key(r_key) if r_key else ""

    for oblast, needle, city_name in FALLBACK_RAYON_TO_CITY:
        if oblast != region_name:
            continue
        if needle.lower() not in r_display.lower() and needle.lower() not in (r_key or ""):
            continue
        for c in cities:
            if (c.get("name") or "") == city_name:
                return c, 1.0, "fallback"
        for c in cities:
            if normalize_oblast_rayon_key(c.get("name") or "") == normalize_oblast_rayon_key(
                city_name
            ):
                return c, 0.99, "fallback"

    best: Optional[Dict[str, Any]] = None
    best_score = 0.0
    reason = "fuzzy"

    for c in cities:
        cn = c.get("name") or ""
        s = similarity_score(r_display, cn)
        if stem and len(stem) >= 4:
            ck = c.get("name_normalized") or ""
            if ck.startswith(stem[:5]) or stem[:5] in ck:
                s = max(s, 0.86)
            sm = SequenceMatcher(None, stem, ck).ratio()
            s = max(s, sm * 0.95)
        if s > best_score:
            best_score = s
            best = c

    if best is not None and best_score >= _MIN_FUZZY:
        return best, best_score, reason

    return None, best_score, "none"
