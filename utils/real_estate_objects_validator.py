# -*- coding: utf-8 -*-
"""
Валідація та фільтрація ОНМ, витягнутих LLM з опису оголошення.

LLM може «розщепити» оголошення на зайві об'єкти (наприклад, по адресах з
попереднього парсингу OLX). Цей модуль залишає лише об'єкти, підтверджені
текстом оголошення (заголовок, опис, параметри, локація).
"""

from __future__ import annotations

import re
import unicodedata
from typing import Any, Dict, List, Optional, Set

from utils.listing_regex_extractor import extract_cadastral_numbers

_GENERIC_DESCRIPTION_RE = re.compile(
    r"^(?:"
    r"об['\u2019]?\s*єкт\s*\d+|"
    r"земельна\s+ділянка(?:\s*\d+)?|"
    r"ділянка\s*\d+|"
    r"будівля(?:\s*/\s*приміщення)?|"
    r"приміщення|"
    r"без\s+опису"
    r")\s*$",
    re.IGNORECASE,
)

_DECLARED_COUNT_PATTERNS = [
    re.compile(
        r"(?:є|єсть|має|містить|продаю|продам)\s+(\d{1,3})\s+"
        r"(?:окремих\s+)?(?:земельн(?:их|а)\s+)?ділян",
        re.IGNORECASE,
    ),
    re.compile(r"(\d{1,3})\s+(?:окремих\s+)?(?:земельн(?:их|а)\s+)?ділян", re.IGNORECASE),
    re.compile(r"(\d{1,3})\s+ділян", re.IGNORECASE),
    re.compile(
        r"(\d{1,3})\s+(?:окремих\s+)?(?:об['\u2019]?\s*єкт(?:и|ів)?|будівл(?:я|і|ь))",
        re.IGNORECASE,
    ),
]

_STREET_PREFIX_RE = re.compile(
    r"^(?:вул\.?|вулиця|просп\.?|проспект|бул\.?|бульвар|пров\.?|провулок|пл\.?|площа)\s+",
    re.IGNORECASE,
)


def _normalize_text(value: str) -> str:
    if not value:
        return ""
    text = unicodedata.normalize("NFKC", str(value))
    text = text.replace("\u2019", "'").lower()
    text = re.sub(r"\s+", " ", text)
    return text.strip()


def _normalize_street_token(value: str) -> str:
    return _STREET_PREFIX_RE.sub("", _normalize_text(value)).strip()


def extract_declared_object_count(text: str) -> Optional[int]:
    """
    Витягує заявлену кількість об'єктів з тексту (напр. «є 5 ділянок» → 5).
    """
    if not text:
        return None
    for pattern in _DECLARED_COUNT_PATTERNS:
        match = pattern.search(text)
        if match:
            try:
                count = int(match.group(1))
            except (ValueError, TypeError):
                continue
            if 1 <= count <= 100:
                return count
    return None


def _is_generic_description(description: str) -> bool:
    return bool(_GENERIC_DESCRIPTION_RE.match(_normalize_text(description)))


def _cadastral_in_evidence(cadastral: str, evidence_norm: str, cadastrals: Set[str]) -> bool:
    cad = (cadastral or "").strip()
    if not cad:
        return False
    if cad in cadastrals:
        return True
    # LLM інколи змінює формат (пробіли, регістр)
    return _normalize_text(cad) in evidence_norm


def _token_in_evidence(token: str, evidence_norm: str, min_len: int = 3) -> bool:
    token_norm = _normalize_text(token)
    if len(token_norm) < min_len:
        return False
    return token_norm in evidence_norm


def _address_grounded(address: Dict[str, Any], evidence_norm: str) -> bool:
    if not isinstance(address, dict):
        return False
    street = _normalize_street_token(address.get("street") or "")
    building = _normalize_text(address.get("building") or "")
    settlement = _normalize_text(address.get("settlement") or "")
    formatted = _normalize_text(address.get("formatted_address") or "")

    if street:
        if not _token_in_evidence(street, evidence_norm, min_len=4):
            return False
        if building and not _token_in_evidence(building, evidence_norm, min_len=1):
            return False
        return True

    if formatted and len(formatted) >= 8 and formatted in evidence_norm:
        return True

    if settlement and len(settlement) >= 4 and _token_in_evidence(settlement, evidence_norm, min_len=4):
        if building:
            return _token_in_evidence(building, evidence_norm, min_len=1)
        return False

    return False


def _description_grounded(description: str, evidence_norm: str) -> bool:
    desc = (description or "").strip()
    if not desc or _is_generic_description(desc):
        return False
    words = [
        w for w in re.findall(r"[\w\u0400-\u04FF']+", desc, re.UNICODE)
        if len(w) >= 5
    ]
    if not words:
        return False
    matched = sum(1 for w in words[:6] if _normalize_text(w) in evidence_norm)
    return matched >= 1


def object_is_grounded_in_evidence(
    obj: Dict[str, Any],
    evidence_text: str,
    cadastrals_in_evidence: Optional[Set[str]] = None,
) -> bool:
    """
    Перевіряє, чи об'єкт підтверджений текстом оголошення (не метаданими парсера).
    """
    if not isinstance(obj, dict):
        return False
    evidence_norm = _normalize_text(evidence_text)
    if not evidence_norm:
        return False

    cadastrals = cadastrals_in_evidence
    if cadastrals is None:
        cadastrals = set(extract_cadastral_numbers(evidence_text))

    cad = (obj.get("cadastral_number") or "").strip()
    if cad:
        return _cadastral_in_evidence(cad, evidence_norm, cadastrals)

    addr = obj.get("address")
    if isinstance(addr, dict) and _address_grounded(addr, evidence_norm):
        return True

    return _description_grounded((obj.get("description") or "").strip(), evidence_norm)


def _dedupe_objects(objects: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    seen: Set[str] = set()
    result: List[Dict[str, Any]] = []
    for obj in objects:
        if not isinstance(obj, dict):
            continue
        cad = (obj.get("cadastral_number") or "").strip()
        if cad:
            key = f"cad:{cad}"
        else:
            addr = obj.get("address") if isinstance(obj.get("address"), dict) else {}
            parts = [
                (obj.get("type") or "").strip().lower(),
                _normalize_street_token(addr.get("street") or ""),
                _normalize_text(addr.get("building") or ""),
                _normalize_text(addr.get("settlement") or ""),
                _normalize_text(obj.get("description") or "")[:80],
            ]
            key = "|".join(parts)
        if key in seen:
            continue
        seen.add(key)
        result.append(obj)
    return result


def _cap_objects(
    objects: List[Dict[str, Any]],
    max_count: int,
    cadastrals_in_evidence: Set[str],
) -> List[Dict[str, Any]]:
    if max_count <= 0 or len(objects) <= max_count:
        return objects

    def _priority(obj: Dict[str, Any]) -> tuple:
        cad = (obj.get("cadastral_number") or "").strip()
        obj_type = (obj.get("type") or "").strip().lower()
        has_cad = 1 if cad and cad in cadastrals_in_evidence else 0
        type_rank = {"land_plot": 3, "building": 2, "premises": 1}.get(obj_type, 0)
        desc_len = len((obj.get("description") or "").strip())
        return (has_cad, type_rank, desc_len)

    ranked = sorted(objects, key=_priority, reverse=True)
    return ranked[:max_count]


def filter_extracted_objects(
    objects: List[Dict[str, Any]],
    evidence_text: str,
) -> List[Dict[str, Any]]:
    """
    Фільтрує список ОНМ від LLM: лише підтверджені текстом, без дублів, з обмеженням за кількістю.
    """
    if not objects:
        return []

    evidence = (evidence_text or "").strip()
    if not evidence:
        return []

    cadastrals_list = extract_cadastral_numbers(evidence)
    cadastrals = set(cadastrals_list)
    declared = extract_declared_object_count(evidence)

    grounded = [
        obj for obj in objects
        if isinstance(obj, dict) and object_is_grounded_in_evidence(obj, evidence, cadastrals)
    ]
    grounded = _dedupe_objects(grounded)

    max_count: Optional[int] = None
    cad_cap = len(cadastrals) if cadastrals else None
    if declared is not None and cad_cap is not None:
        max_count = min(declared, cad_cap)
    elif declared is not None:
        max_count = declared
    elif cad_cap is not None:
        max_count = cad_cap

    if max_count is not None:
        grounded = _cap_objects(grounded, max_count, cadastrals)

    return grounded
