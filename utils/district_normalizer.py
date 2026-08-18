# -*- coding: utf-8 -*-
"""
Нормалізація назв районів міст для збігу з даними в БД.

Google Maps та різні джерела можуть повертати варіанти: "Солом'янський", "Соломянський",
"Солом'янський район". Користувач може писати "Соломянському" (давальний відмінок).

Важливо: «Києво-Святошинський» (скасований район Київської області, 2020) ≠
«Святошинський» (район м. Київ). Не плутати через substring-збіг.
"""

import re
from typing import Any, Dict, List, Optional, Tuple

# Київ: варіанти написання → канонічна назва для фільтра (без "район")
KYIV_DISTRICTS_MAP = {
    "деснянський": "Деснянський",
    "святошинський": "Святошинський",
    "дніпровський": "Дніпровський",
    "печерський": "Печерський",
    "голосіївський": "Голосіївський",
    "дарницький": "Дарницький",
    "солом'янський": "Солом'янський",
    "соломянський": "Солом'янський",
    "солом'янському": "Солом'янський",
    "соломянському": "Солом'янський",
    "оболонський": "Оболонський",
    "шевченківський": "Шевченківський",
    "подільський": "Подільський",
}

# Скасовані райони областей (реформа 2020) → сучасний район області
OBSOLETE_OBLAST_RAYON_MAP = {
    "києво-святошинський": "Бучанський",
    "києво святошинський": "Бучанський",
}

_KYIV_SVIATOSHYN_RE = re.compile(r"києво[\s\-]?святошинськ", re.IGNORECASE)


def is_obsolete_kyiv_sviatoshyn_rayon(name: Optional[str]) -> bool:
    """True якщо назва — скасований Києво-Святошинський район області."""
    if not name or not isinstance(name, str):
        return False
    return bool(_KYIV_SVIATOSHYN_RE.search(name.strip().lower()))


def normalize_district_for_kyiv(user_input: str) -> Optional[str]:
    """
    Нормалізує назву району Києва з тексту користувача.

    Args:
        user_input: Текст типу "Соломянському", "Солом'янський", "в Соломянському районі"

    Returns:
        Канонічна назва ("Солом'янський") або None
    """
    if not user_input or not isinstance(user_input, str):
        return None
    text = user_input.strip().lower()
    # Скасований район області — не район м. Київ
    if is_obsolete_kyiv_sviatoshyn_rayon(text):
        return None
    # Прибираємо "район", "районі", "в", "у"
    text = re.sub(r"\b(район[аиуі]?|в|у)\b", "", text, flags=re.IGNORECASE).strip()
    text = re.sub(r"\s+", " ", text)
    return KYIV_DISTRICTS_MAP.get(text)


def extract_district_from_query(query: str, city: str = "Київ") -> Optional[str]:
    """
    Витягує назву району з запиту користувача для заданого міста.

    Патерни: "в Соломянському районі", "Солом'янський район", "район Соломянський".
    Не плутає «Києво-Святошинський» зі «Святошинський».

    Returns:
        Канонічна назва району або None
    """
    if city != "Київ":
        return None
    q = (query or "").strip().lower()
    if not q:
        return None
    # Довші ключі першими (солом'янському перед солом'янський тощо)
    variants = sorted(KYIV_DISTRICTS_MAP.items(), key=lambda kv: len(kv[0]), reverse=True)
    for variant, canonical in variants:
        # Word-boundary-ish: не ловити «святошинський» всередині «києво-святошинський»
        if variant == "святошинський" and is_obsolete_kyiv_sviatoshyn_rayon(q):
            # Якщо в запиті є і місто Київ як контекст району — все одно не мапимо
            # скасований обласний район на міський.
            continue
        pattern = r"(?<![а-яіїєґa-z0-9])" + re.escape(variant) + r"(?![а-яіїєґa-z0-9])"
        if re.search(pattern, q, flags=re.IGNORECASE):
            return canonical
    return None


def sanitize_llm_address_districts(addr: Dict[str, Any]) -> Dict[str, Any]:
    """
    Виправляє плутанину Києво-Святошинський (скасований район області) vs
    Святошинський (район м. Київ) у полях LLM-адреси.

    - Якщо settlement=Київ і district/settlement_district містить Києво-Святошинський
      → settlement_district=Святошинський, district очищається.
    - Якщо settlement не Київ і district=Києво-Святошинський
      → district=Бучанський (сучасний район області).
    """
    if not isinstance(addr, dict):
        return addr

    def _strip(v: Any) -> str:
        return str(v).strip() if v is not None else ""

    out = dict(addr)
    settlement = _strip(out.get("settlement"))
    settlement_l = settlement.lower()
    is_kyiv_city = settlement_l in ("київ", "м. київ", "м київ") or (
        settlement_l.startswith("київ") and "київськ" not in settlement_l
    )

    district = _strip(out.get("district"))
    settlement_district = _strip(out.get("settlement_district"))

    if is_obsolete_kyiv_sviatoshyn_rayon(district) or is_obsolete_kyiv_sviatoshyn_rayon(settlement_district):
        if is_kyiv_city or not settlement:
            out["settlement_district"] = "Святошинський"
            if is_obsolete_kyiv_sviatoshyn_rayon(district):
                out["district"] = None
            if not settlement:
                out["settlement"] = "Київ"
                if not out.get("settlement_type"):
                    out["settlement_type"] = "м."
        else:
            # Населений пункт області — сучасний Бучанський район
            if is_obsolete_kyiv_sviatoshyn_rayon(district):
                out["district"] = "Бучанський"
            if is_obsolete_kyiv_sviatoshyn_rayon(settlement_district):
                out["settlement_district"] = None

    # LLM часто ставить region=Київська область для м. Київ → ламає геокод-запит
    settlement = _strip(out.get("settlement"))
    settlement_l = settlement.lower()
    is_kyiv_city = settlement_l in ("київ", "м. київ", "м київ") or (
        settlement_l.startswith("київ") and "київськ" not in settlement_l
    )
    region = _strip(out.get("region"))
    if is_kyiv_city:
        region_l = region.lower()
        if not region_l or region_l.startswith("київськ") or "київська" in region_l:
            out["region"] = "місто Київ"

    return out


def split_city_and_district(city_value: str) -> Tuple[Optional[str], Optional[str]]:
    """
    Розділяє рядок типу "Київ, Солом'янський" на city та district.
    LLM іноді повертає їх об'єднаними в одному полі city.

    Returns:
        (city, district) — окремі значення; якщо район не виявлено — (city, None)
    """
    if not city_value or not isinstance(city_value, str):
        return (city_value, None)
    s = city_value.strip()
    if ", " not in s:
        return (s or None, None)
    parts = [p.strip() for p in s.split(",", 1)]
    if len(parts) != 2:
        return (s or None, None)
    city_part, district_part = parts[0], parts[1]
    if not district_part:
        return (city_part or None, None)
    if is_obsolete_kyiv_sviatoshyn_rayon(district_part):
        if "київ" in (city_part or "").lower():
            return (city_part, "Святошинський")
        return (city_part or None, "Бучанський")
    canonical = normalize_district_for_kyiv(district_part)
    if canonical and "київ" in (city_part or "").lower():
        return (city_part, canonical)
    return (city_part or None, district_part or None)


def get_district_filter_value(normalized: str) -> dict:
    """
    Повертає MongoDB-фільтр для city_district з урахуванням варіантів написання.

    Якщо в БД може бути "Солом'янський" або "Соломянський" — regex з опціональним апострофом.
    """
    if not normalized:
        return {}
    # Для "Солом'янський" — regex Солом'?янський (апостроф опціональний)
    for apostrophe in ("'", "'", "`"):
        if apostrophe in normalized:
            parts = normalized.split(apostrophe, 1)
            # В regex '?' після символу = 0 або 1 раз
            pattern = re.escape(parts[0]) + re.escape(apostrophe) + "?" + re.escape(parts[1])
            return {"$regex": f"^{pattern}", "$options": "i"}
    return {"$regex": f"^{re.escape(normalized)}", "$options": "i"}
